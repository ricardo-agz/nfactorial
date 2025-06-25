import random
import secrets
import time
import asyncio
import json
from typing import Any, cast
import redis.asyncio as redis

from factorial.utils import decode
from factorial.agent import BaseAgent, ExecutionContext
from factorial.events import AgentEvent, EventPublisher
from factorial.logging import get_logger, colored
from factorial.queue.task import (
    Task,
    TaskStatus,
    ContextType,
    get_task_status,
    get_task_data,
    get_task_agent,
)
from factorial.utils import is_valid_task_id
from factorial.exceptions import (
    InactiveTaskError,
    TaskNotFoundError,
    InvalidTaskIdError,
)
from factorial.queue.lua import (
    BatchPickupScriptResult,
    BatchPickupScript,
    create_cancel_task_script,
    CancelTaskScriptResult,
    create_child_task_completion_script,
    create_enqueue_task_script,
    create_tool_completion_script,
)
from factorial.queue.keys import RedisKeys, PENDING_SENTINEL


logger = get_logger(__name__)


async def complete_deferred_tool(
    redis_client: redis.Redis,
    namespace: str,
    agents_by_name: dict[str, BaseAgent[Any]],
    task_id: str,
    tool_call_id: str,
    result: Any,
) -> bool:
    """Complete a deferred tool call"""
    task_data = await get_task_data(redis_client, namespace, task_id)
    task_status = TaskStatus(task_data["status"])
    if task_status in [TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED]:
        raise InactiveTaskError(task_id)

    agent_name = task_data["agent"]
    agent = agents_by_name[agent_name]

    try:
        task: Task = Task.from_dict(task_data, context_class=agent.context_class)  # type: ignore
    except Exception as e:
        logger.error(
            f"Failed to process task {task_id}: Task data is invalid", exc_info=e
        )
        return False

    keys = RedisKeys.format(namespace=namespace, task_id=task_id, agent=agent.name)

    # Set the result
    await redis_client.hset(keys.pending_tool_results, tool_call_id, json.dumps(result))  # type: ignore
    # Get all results and check if any are still pending
    all_results = cast(
        dict[str | bytes, str | bytes],
        await redis_client.hgetall(keys.pending_tool_results),  # type: ignore
    )
    if not all_results:
        return False

    # Check if any results are still pending (sentinel value)
    completed_results: list[tuple[str, Any]] = []
    for tcid, result_json in all_results.items():
        tcid_str = decode(tcid)
        result_str = decode(result_json)

        if result_str == PENDING_SENTINEL:
            return False
        else:
            completed_results.append((tcid_str, json.loads(result_str)))

    # Update the task context with the completed results
    updated_context = agent.process_deferred_tool_results(
        task.payload, completed_results
    ).context

    event_publisher = EventPublisher(
        redis_client=redis_client,
        channel=keys.updates_channel,
    )

    execution_ctx = ExecutionContext(
        task_id=task.id,
        owner_id=task.metadata.owner_id,
        retries=task.retries,
        iterations=task.payload.turn,
        events=event_publisher,
    )

    # Lifecycle callback – resume from pending tool calls
    await agent._safe_call(
        agent.on_pending_tool_results,
        updated_context,
        execution_ctx,
        completed_results,
    )

    # Move task back to queue
    tool_completion_script = await create_tool_completion_script(redis_client)
    success, message = await tool_completion_script.execute(
        queue_main_key=keys.queue_main,
        queue_orphaned_key=keys.queue_orphaned,
        queue_pending_key=keys.queue_pending,
        pending_tool_results_key=keys.pending_tool_results,
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


async def enqueue_task(
    redis_client: redis.Redis,
    namespace: str,
    agent: BaseAgent[Any],
    task: Task[ContextType],
) -> str:
    keys = RedisKeys.format(namespace=namespace, agent=agent.name)

    if not is_valid_task_id(task.id):
        raise InvalidTaskIdError(task.id)

    enqueue_script = await create_enqueue_task_script(redis_client)
    await enqueue_script.execute(
        agent_queue_key=keys.queue_main,
        task_statuses_key=keys.task_status,
        task_agents_key=keys.task_agent,
        task_payloads_key=keys.task_payload,
        task_pickups_key=keys.task_pickups,
        task_retries_key=keys.task_retries,
        task_metas_key=keys.task_meta,
        task_id=task.id,
        task_agent=agent.name,
        task_payload_json=task.payload.to_json(),
        task_pickups=0,
        task_retries=0,
        task_meta_json=task.metadata.to_json(),
    )

    return task.id


async def cancel_task(
    redis_client: redis.Redis,
    namespace: str,
    task_id: str,
    agents_by_name: dict[str, BaseAgent[Any]],
    metrics_bucket_duration: int,
    metrics_retention_duration: int,
) -> None:
    agent_name = await get_task_agent(redis_client, namespace, task_id)
    keys = RedisKeys.format(
        namespace=namespace,
        agent=agent_name,
        task_id=task_id,
        metrics_bucket_duration=metrics_bucket_duration,
    )

    cancel_script = await create_cancel_task_script(redis_client)
    result: CancelTaskScriptResult = await cancel_script.execute(
        queue_cancelled_key=keys.queue_cancelled,
        queue_backoff_key=keys.queue_backoff,
        queue_orphaned_key=keys.queue_orphaned,
        queue_pending_key=keys.queue_pending,
        pending_cancellations_key=keys.task_cancellations,
        task_statuses_key=keys.task_status,
        task_agents_key=keys.task_agent,
        task_payloads_key=keys.task_payload,
        task_pickups_key=keys.task_pickups,
        task_retries_key=keys.task_retries,
        task_metas_key=keys.task_meta,
        pending_tool_results_key=keys.pending_tool_results,
        pending_child_task_results_key=keys.pending_child_task_results,
        agent_metrics_bucket_key=keys.agent_metrics_bucket,
        global_metrics_bucket_key=keys.global_metrics_bucket,
        task_id=task_id,
        metrics_ttl=metrics_retention_duration,
    )

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
    """Steer a task"""
    task_status = await get_task_status(redis_client, namespace, task_id)
    if task_status in [TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED]:
        raise InactiveTaskError(task_id)

    keys = RedisKeys.format(namespace=namespace, task_id=task_id)
    # message id format: {timestamp_ms}_{random_hex} e.g. 1717234200000_a3f4b5c6
    message_mapping = {
        f"{int(time.time() * 1000)}_{secrets.token_hex(3)}": json.dumps(message)
        for message in messages
    }
    await redis_client.hset(keys.task_steering, mapping=message_mapping)  # type: ignore


async def resume_if_no_remaining_child_tasks(
    redis_client: redis.Redis,
    namespace: str,
    agents_by_name: dict[str, BaseAgent[Any]],
    task_id: str,
) -> bool:
    """Resume the parent task if there are no remaining child tasks.

    Returns:
        * True if the task was resumed, False if it was cancelled or still has child tasks
    """
    task_data = await get_task_data(redis_client, namespace, task_id)
    task_status = TaskStatus(task_data["status"])
    if task_status in [TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED]:
        return False

    agent_name = task_data["agent"]
    agent = agents_by_name[agent_name]
    keys = RedisKeys.format(namespace=namespace, task_id=task_id, agent=agent_name)

    try:
        task: Task = Task.from_dict(task_data, context_class=agent.context_class)  # type: ignore
    except Exception as e:
        logger.error(
            f"Failed to process task {task_id}: Task data is invalid", exc_info=e
        )
        return False

    # Get all results and check if any are still pending
    all_results = cast(
        dict[str | bytes, str | bytes],
        await redis_client.hgetall(keys.pending_child_task_results),  # type: ignore
    )
    if not all_results:
        return False

    # Check if any results are still pending (sentinel value)
    completed_results: list[tuple[str, Any]] = []

    for child_task_id, result_json in all_results.items():
        child_task_id_str = decode(child_task_id)
        result_str = decode(result_json)

        if result_str == PENDING_SENTINEL:
            return False
        else:
            completed_results.append((child_task_id_str, json.loads(result_str)))

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
        task = Task.from_dict(task_data, context_class=agent.context_class)
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
    """Process cancelled tasks. Runs agent-specific cancellation logic and publishes cancellation events."""
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
    metrics_bucket_duration: int,
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
        metrics_bucket_duration=metrics_bucket_duration,
    )

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
        )
        if result.orphaned_task_ids:
            logger.warning(
                f"⚠️ Found {len(result.orphaned_task_ids)} orphaned tasks: {result.orphaned_task_ids}"
            )
        if result.corrupted_task_ids:
            logger.warning(
                f"⚠️ Found {len(result.corrupted_task_ids)} corrupted tasks: {result.corrupted_task_ids}"
            )

        return result.tasks_to_process_ids, result.tasks_to_cancel_ids

    except Exception as e:
        logger.error(f"Failed to get task batch: {e}")
        await asyncio.sleep(0.15 + random.random() * 0.1)  # backoff with jitter
        return [], []
