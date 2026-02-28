from __future__ import annotations

import asyncio
import json
import random
import time
from typing import Any, cast

import redis.asyncio as redis

from factorial.agent import BaseAgent, ExecutionContext
from factorial.events import AgentEvent, BatchEvent, EventPublisher
from factorial.exceptions import InactiveTaskError, TaskNotFoundError
from factorial.logging import colored, get_logger
from factorial.queue.keys import PENDING_SENTINEL, RedisKeys
from factorial.queue.lua import (
    BatchPickupScript,
    BatchPickupScriptResult,
    CancelTaskScriptResult,
    QueueScripts,
    SteeringEnqueueScriptResult,
    create_child_task_completion_script,
    create_steering_enqueue_script,
)
from factorial.queue.task import (
    Batch,
    Task,
    TaskStatus,
    get_batch_data,
    get_task_agent,
    get_task_data,
)
from factorial.utils import decode, serialize_data

logger = get_logger(__name__)


_TERMINAL_CHILD_STATUSES = {
    TaskStatus.COMPLETED.value,
    TaskStatus.FAILED.value,
    TaskStatus.CANCELLED.value,
}


def _synthesize_quiescent_child_result(
    *,
    child_task_id: str,
    child_status: str | None,
    is_activity_wait: bool,
) -> dict[str, Any]:
    """Build a synthetic child result when a wait-set is quiescent but unresolved."""
    if is_activity_wait:
        return {
            "task_id": child_task_id,
            "status": TaskStatus.PAUSED.value,
            "wait_kind": "activity",
            "reason": "child_waiting_for_activity",
            "synthetic": True,
        }

    if child_status in _TERMINAL_CHILD_STATUSES:
        return {
            "task_id": child_task_id,
            "status": child_status,
            "result_missing": True,
            "reason": "child_terminal_without_parent_result",
            "synthetic": True,
        }

    return {
        "task_id": child_task_id,
        "status": child_status or "unknown",
        "result_missing": True,
        "reason": "child_unresolved_quiescent_state",
        "synthetic": True,
    }


async def cancel_batch(
    redis_client: redis.Redis,
    namespace: str,
    batch_id: str,
    agents_by_name: dict[str, BaseAgent[Any]],
    metrics_retention_duration: int,
) -> Batch:
    """Cancel all tasks in a batch.

    * Concurrently cancel all tasks in a batch.
    * Silently ignores cases where the task is already finished / cancelled.
    """

    batch = await get_batch_data(redis_client, namespace, batch_id)
    keys = RedisKeys.format(namespace=namespace)

    async def _safe_cancel(tid: str) -> None:
        try:
            await cancel_task(
                redis_client=redis_client,
                namespace=namespace,
                task_id=tid,
                agents_by_name=agents_by_name,
                metrics_retention_duration=metrics_retention_duration,
            )
        except (InactiveTaskError, TaskNotFoundError):
            # Task already in terminal state – ignore
            return
        except Exception as e:
            logger.error(f"Failed to cancel task {tid} in batch {batch_id}: {e}")

    # Run cancellations in parallel (bounded to avoid overwhelming redis)
    sem = asyncio.Semaphore(50)

    async def _bounded_cancel(tid: str) -> None:
        async with sem:
            await _safe_cancel(tid)

    await asyncio.gather(*[_bounded_cancel(tid) for tid in batch.task_ids])
    # Refresh batch stats (some tasks may have been cancelled immediately)
    batch = await get_batch_data(redis_client, namespace, batch_id)

    batch.metadata.status = "cancelled"

    pipe = redis_client.pipeline(transaction=True)
    pipe.hset(keys.batch_meta, batch_id, batch.metadata.to_json())
    pipe.zadd(keys.batch_completed, {batch_id: time.time()})
    await pipe.execute()

    owner_id = batch.metadata.owner_id
    if owner_id:
        publisher = EventPublisher(
            redis_client,
            RedisKeys.format(namespace=namespace, owner_id=owner_id).updates_channel,
        )
        await publisher.publish_event(
            BatchEvent(
                event_type="batch_cancelled",
                batch_id=batch_id,
                owner_id=owner_id,
                status="cancelled",
                progress=batch.progress,
                completed_tasks=len(batch.task_ids) - len(batch.remaining_task_ids),
                total_tasks=len(batch.task_ids),
            )
        )

    return batch

async def cancel_task(
    redis_client: redis.Redis,
    namespace: str,
    task_id: str,
    agents_by_name: dict[str, BaseAgent[Any]],
    metrics_retention_duration: int,
) -> None:
    agent_name = await get_task_agent(redis_client, namespace, task_id)
    queue_scripts = QueueScripts.for_agent(
        redis_client=redis_client,
        namespace=namespace,
        agent_name=agent_name,
        metrics_ttl=metrics_retention_duration,
    )
    result: CancelTaskScriptResult = await queue_scripts.cancel_task(task_id=task_id)

    if not result.success:
        if not result.current_status:
            raise TaskNotFoundError(task_id)
        elif result.current_status in [
            TaskStatus.COMPLETED,
            TaskStatus.FAILED,
            TaskStatus.CANCELLED,
        ]:
            raise InactiveTaskError(task_id)
        else:
            raise Exception(result.message)

    # If the task was cancelled immediately by the script (e.g. it was in backoff or
    # pending_tool_results), the worker loop will never see it.  The Lua script returns
    # owner_id in this case so we can emit the run_cancelled event right here.
    if result.owner_id is not None:
        await run_agent_cancellation(
            redis_client=redis_client,
            namespace=namespace,
            agent=agents_by_name[agent_name],
            task_id=task_id,
        )

async def steer_task(
    redis_client: redis.Redis,
    namespace: str,
    task_id: str,
    messages: list[dict[str, Any]],
) -> None:
    """Steer a task and wake activity waits atomically."""
    if not isinstance(messages, list):
        raise TypeError("steer_task messages must be a list of dict objects")
    normalized_messages: list[dict[str, Any]] = []
    for message in messages:
        if not isinstance(message, dict):
            raise TypeError("steer_task messages must be a list of dict objects")
        normalized_messages.append(dict(message))

    agent_name = await get_task_agent(redis_client, namespace, task_id)
    keys = RedisKeys.format(namespace=namespace, agent=agent_name, task_id=task_id)
    root_keys = RedisKeys.format(namespace=namespace)
    script = await create_steering_enqueue_script(redis_client)
    result: SteeringEnqueueScriptResult = await script.execute(
        queue_main_key=keys.queue_main,
        queue_orphaned_key=keys.queue_orphaned,
        queue_pending_key=keys.queue_pending,
        task_statuses_key=keys.task_status,
        task_agents_key=keys.task_agent,
        task_payloads_key=keys.task_payload,
        task_pickups_key=keys.task_pickups,
        task_retries_key=keys.task_retries,
        task_metas_key=keys.task_meta,
        steering_messages_key=keys.task_steering,
        activity_wait_meta_key=root_keys.activity_wait_meta,
        message_seq_key=root_keys.messaging_message_seq,
        task_id=task_id,
        messages_json=json.dumps(
            [json.dumps(serialize_data(message)) for message in normalized_messages]
        ),
    )
    if not result.success:
        if result.status == "missing":
            raise TaskNotFoundError(task_id)
        if result.status == "inactive":
            raise InactiveTaskError(task_id)
        if result.status == "corrupted":
            raise RuntimeError(f"Task {task_id} data is corrupted")
        raise RuntimeError(
            f"steer_task failed with unexpected status '{result.status}'"
        )

async def resume_if_no_remaining_child_tasks(
    redis_client: redis.Redis,
    namespace: str,
    agents_by_name: dict[str, BaseAgent[Any]],
    task_id: str,
) -> bool:
    """Resume the parent task if there are no remaining child tasks.

    Returns:
        * True if the task was resumed, False if cancelled or has child tasks
    """
    task_data = await get_task_data(redis_client, namespace, task_id)
    task_status = TaskStatus(task_data["status"])
    if task_status in [TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED]:
        return False

    agent_name = task_data["agent"]
    agent = agents_by_name[agent_name]
    keys = RedisKeys.format(namespace=namespace, task_id=task_id, agent=agent_name)

    try:
        task: Task = Task.from_dict(task_data, context_class=agent.context_class)
    except Exception as e:
        logger.error(
            f"Failed to process task {task_id}: Task data is invalid", exc_info=e
        )
        return False

    wait_child_ids_raw = cast(
        set[str | bytes],
        await redis_client.smembers(keys.pending_child_wait_ids),  # type: ignore
    )
    if not wait_child_ids_raw:
        return False
    wait_child_ids = sorted(decode(child_id) for child_id in wait_child_ids_raw)

    # Read only the child results for the currently awaited wait-set.
    result_values = cast(
        list[str | bytes | None],
        await redis_client.hmget(keys.pending_child_task_results, wait_child_ids),  # type: ignore[arg-type,misc]
    )

    completed_results: list[tuple[str, Any]] = []
    unresolved_child_ids: list[str] = []
    for child_task_id, result_json in zip(wait_child_ids, result_values, strict=True):
        if result_json is None:
            unresolved_child_ids.append(child_task_id)
            continue
        result_str = decode(result_json)
        if result_str == PENDING_SENTINEL:
            unresolved_child_ids.append(child_task_id)
            continue
        completed_results.append((child_task_id, json.loads(result_str)))

    if unresolved_child_ids:
        unresolved_status_values = cast(
            list[str | bytes | None],
            await redis_client.hmget(keys.task_status, unresolved_child_ids),  # type: ignore[arg-type,misc]
        )
        unresolved_activity_values = cast(
            list[str | bytes | None],
            await redis_client.hmget(keys.activity_wait_meta, unresolved_child_ids),  # type: ignore[arg-type,misc]
        )

        synthesized_results: list[tuple[str, Any]] = []
        for child_task_id, status_raw, activity_wait_raw in zip(
            unresolved_child_ids,
            unresolved_status_values,
            unresolved_activity_values,
            strict=True,
        ):
            child_status = decode(status_raw) if status_raw is not None else None
            is_activity_wait = child_status == TaskStatus.PAUSED.value and (
                activity_wait_raw is not None
            )
            is_terminal = child_status in _TERMINAL_CHILD_STATUSES
            if not (is_activity_wait or is_terminal):
                return False

            synthesized_results.append(
                (
                    child_task_id,
                    _synthesize_quiescent_child_result(
                        child_task_id=child_task_id,
                        child_status=child_status,
                        is_activity_wait=is_activity_wait,
                    ),
                )
            )

        completed_results.extend(synthesized_results)

    # Update the task context with the completed results
    updated_context = agent.process_child_task_results(
        task.payload, completed_results
    ).context

    # Move task back to queue
    child_task_completion_script = await create_child_task_completion_script(
        redis_client
    )
    success, message = await child_task_completion_script.execute(
        queue_main_key=keys.queue_main,
        queue_orphaned_key=keys.queue_orphaned,
        queue_pending_key=keys.queue_pending,
        pending_child_task_results_key=keys.pending_child_task_results,
        pending_child_wait_ids_key=keys.pending_child_wait_ids,
        task_statuses_key=keys.task_status,
        task_agents_key=keys.task_agent,
        task_payloads_key=keys.task_payload,
        task_pickups_key=keys.task_pickups,
        task_retries_key=keys.task_retries,
        task_metas_key=keys.task_meta,
        task_id=task.id,
        updated_task_context_json=updated_context.to_json(),
    )

    return success

async def run_agent_cancellation(
    redis_client: redis.Redis,
    namespace: str,
    agent: BaseAgent[Any],
    task_id: str,
) -> None:
    """Run task cancellation"""
    task_data = await get_task_data(redis_client, namespace, task_id)
    if not task_data:
        logger.error(
            f"Failed to complete task cancellation for {task_id}: Task data not found"
        )
        return

    try:
        task: Task[Any] = Task.from_dict(task_data, context_class=agent.context_class)
    except Exception as e:
        logger.error(
            f"Failed to process task {task_id}: Task data is invalid", exc_info=e
        )
        return

    keys = RedisKeys.format(namespace=namespace, owner_id=task.metadata.owner_id)
    event_publisher = EventPublisher(
        redis_client=redis_client,
        channel=keys.updates_channel,
    )

    try:
        execution_ctx = ExecutionContext(
            task_id=task.id,
            owner_id=task.metadata.owner_id,
            retries=task.retries,
            iterations=task.payload.turn,
            events=event_publisher,
        )
        # Lifecycle callback – run cancelled
        await agent._safe_call(
            agent.on_run_cancelled,
            task.payload,
            execution_ctx,
        )
        logger.info(f"🚫 Task cancelled {colored(f'[{task.id}]', 'dim')}")

        await event_publisher.publish_event(
            AgentEvent(
                event_type="run_cancelled",
                task_id=task.id,
                owner_id=task.metadata.owner_id,
                agent_name=agent.name,
            )
        )
    except Exception as e:
        logger.error(f"Error sending cancellation event for {task.id}", exc_info=e)

async def process_cancelled_tasks(
    redis_client: redis.Redis,
    namespace: str,
    cancelled_task_ids: list[str],
    agent: BaseAgent[Any],
) -> None:
    """Process cancelled tasks with agent-specific logic and publish events."""
    tasks = [
        run_agent_cancellation(redis_client, namespace, agent, task_id)
        for task_id in cancelled_task_ids
    ]
    await asyncio.gather(*tasks)

async def get_task_batch(
    batch_script: BatchPickupScript,
    namespace: str,
    agent: BaseAgent[Any],
    batch_size: int,
    metrics_ttl: int,
) -> tuple[list[str], list[str]]:
    """
    Get batch of tasks atomically

    Returns:
        * List of task ids to process
        * List of task ids to cancel
    """
    keys = RedisKeys.format(
        namespace=namespace,
        agent=agent.name,
    )
    agent_queue_templates = RedisKeys.format(
        namespace=namespace,
        agent="{agent}",
    )
    steering_template = RedisKeys.format(
        namespace=namespace,
        task_id="{task_id}",
    ).task_steering

    try:
        result: BatchPickupScriptResult = await batch_script.execute(
            queue_main_key=keys.queue_main,
            queue_cancelled_key=keys.queue_cancelled,
            queue_orphaned_key=keys.queue_orphaned,
            task_statuses_key=keys.task_status,
            task_agents_key=keys.task_agent,
            task_payloads_key=keys.task_payload,
            task_pickups_key=keys.task_pickups,
            task_retries_key=keys.task_retries,
            task_metas_key=keys.task_meta,
            task_cancellations_key=keys.task_cancellations,
            processing_heartbeats_key=keys.processing_heartbeats,
            agent_metrics_bucket_key=keys.agent_metrics_bucket,
            global_metrics_bucket_key=keys.global_metrics_bucket,
            batch_size=batch_size,
            metrics_ttl=metrics_ttl,
            activity_wait_meta_key=keys.activity_wait_meta,
            queue_pending_key_template=agent_queue_templates.queue_pending,
            queue_main_key_template=agent_queue_templates.queue_main,
            task_steering_key_template=steering_template,
            message_seq_key=keys.messaging_message_seq,
        )
        if result.orphaned_task_ids:
            logger.warning(
                f"⚠️ Found {len(result.orphaned_task_ids)} orphaned tasks: "
                f"{result.orphaned_task_ids}"
            )
        if result.corrupted_task_ids:
            logger.warning(
                f"⚠️ Found {len(result.corrupted_task_ids)} corrupted tasks: "
                f"{result.corrupted_task_ids}"
            )

        return result.tasks_to_process_ids, result.tasks_to_cancel_ids

    except Exception as e:
        logger.error(f"Failed to get task batch: {e}")
        await asyncio.sleep(0.15 + random.random() * 0.1)  # backoff with jitter
        return [], []
