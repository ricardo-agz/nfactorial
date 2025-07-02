import random
from typing import Any
from enum import Enum
import redis.asyncio as redis
import time
import json
import asyncio
from dataclasses import replace
from contextlib import asynccontextmanager, suppress
from factorial.agent import BaseAgent, ExecutionContext, RunCompletion, serialize_data
from factorial.context import ContextType
from factorial.events import QueueEvent, AgentEvent, EventPublisher
from factorial.logging import get_logger, colored
from factorial.exceptions import RETRYABLE_EXCEPTIONS, FatalAgentError
from factorial.queue.task import (
    Task,
    get_task_data,
    get_task_steering_messages,
)
from factorial.queue.lua import (
    TaskSteeringScript,
    TaskCompletionScript,
    create_batch_pickup_script,
    create_task_steering_script,
    create_task_completion_script,
)
from factorial.queue.operations import (
    resume_if_no_remaining_child_tasks,
    process_cancelled_tasks,
    get_task_batch,
    enqueue_task,
)
from factorial.queue.keys import RedisKeys, PENDING_SENTINEL
from datetime import datetime, timezone


logger = get_logger("factorial.queue")


class CompletionAction(str, Enum):
    """Canonical action names accepted by the Lua *task completion* script.

    Keeping them in one place prevents typos and enables static typing.
    """

    CONTINUE = "continue"
    PENDING_TOOL = "pending_tool_call_results"
    PENDING_CHILD = "pending_child_task_results"
    COMPLETE = "complete"
    RETRY = "retry"
    BACKOFF = "backoff"
    FAIL = "fail"


async def heartbeat_loop(
    redis_client: redis.Redis,
    namespace: str,
    task_id: str,
    agent: BaseAgent[Any],
    stop_event: asyncio.Event,
    interval: int,
) -> None:
    """Simple heartbeat loop that runs until stopped"""
    keys = RedisKeys.format(namespace=namespace, agent=agent.name)

    try:
        while not stop_event.is_set():
            try:
                await redis_client.zadd(
                    keys.processing_heartbeats, {task_id: time.time()}
                )  # type: ignore

                # Wait for next interval
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=interval)
                    break  # Stop event was set
                except asyncio.TimeoutError:
                    continue  # Normal timeout, send next heartbeat

            except Exception as e:
                # Log but don't crash - missing one heartbeat shouldn't kill the task
                logger.error(f"Heartbeat error for {task_id}: {e}")
                # Small backoff to avoid hammering Redis if it's having issues
                await asyncio.sleep(0.25 + random.random() * 0.5)
    except asyncio.CancelledError:
        # Task was cancelled, exit cleanly
        logger.debug(f"Heartbeat loop for {task_id} cancelled")
        raise


@asynccontextmanager
async def heartbeat_context(
    *,
    redis_client: redis.Redis,
    namespace: str,
    task_id: str,
    agent: BaseAgent[Any],
    interval: int,
):
    """Run ``heartbeat_loop`` in the background for the lifetime of the ``with`` block."""

    stop_event: asyncio.Event = asyncio.Event()
    hb_task = asyncio.create_task(
        heartbeat_loop(
            redis_client=redis_client,
            namespace=namespace,
            task_id=task_id,
            agent=agent,
            stop_event=stop_event,
            interval=interval,
        )
    )
    try:
        yield
    finally:
        stop_event.set()
        hb_task.cancel()
        # Swallow the CancelledError raised when the task is properly cleaned up.
        with suppress(asyncio.CancelledError):
            await hb_task


async def apply_steering_if_available(
    *,
    redis_client: redis.Redis,
    task: "Task[ContextType]",
    agent: BaseAgent[ContextType],
    execution_ctx: ExecutionContext,
    steering_script: TaskSteeringScript,
    namespace: str,
    event_publisher: EventPublisher,
) -> "Task[ContextType]":
    """Apply any queued steering messages to the task in a single, focused helper.

    The behaviour is identical to the inlined version that existed previously.
    Returns the (possibly) updated `Task` object.
    """

    steering_messages_data = await get_task_steering_messages(
        redis_client=redis_client,
        namespace=namespace,
        task_id=task.id,
    )
    # Early exit – nothing to do
    if not steering_messages_data:
        return task

    # Sort messages by their stream ID timestamp prefix so they are applied in
    # deterministic order.
    def _extract_ts(message_tuple: tuple[str, dict[str, Any]]) -> int:
        try:
            return int(message_tuple[0].split("_")[0])
        except (ValueError, IndexError):
            return 0

    steering_messages_data.sort(key=_extract_ts)
    steering_messages = [msg for _, msg in steering_messages_data]
    steering_message_ids = [mid for mid, _ in steering_messages_data]

    # Let the Agent transform its context
    steered_task = replace(task)
    steered_task.payload = await agent.steer(
        messages=steering_messages,
        agent_ctx=task.payload,
        execution_ctx=execution_ctx,
    )

    keys = RedisKeys.format(namespace=namespace, agent=agent.name, task_id=task.id)

    try:
        await steering_script.execute(
            queue_orphaned_key=keys.queue_orphaned,
            task_statuses_key=keys.task_status,
            task_agents_key=keys.task_agent,
            task_payloads_key=keys.task_payload,
            task_pickups_key=keys.task_pickups,
            task_retries_key=keys.task_retries,
            task_metas_key=keys.task_meta,
            steering_messages_key=keys.task_steering,
            task_id=task.id,
            steering_message_ids=steering_message_ids,
            updated_task_payload_json=steered_task.payload.to_json(),
        )

        await event_publisher.publish_event(
            AgentEvent(
                event_type="run_steering_applied",
                task_id=task.id,
                owner_id=task.metadata.owner_id,
                agent_name=agent.name,
            )
        )
        return steered_task
    except Exception as e:
        logger.error(f"Error updating task with steering messages: {e}")
        await event_publisher.publish_event(
            AgentEvent(
                event_type="run_steering_failed",
                task_id=task.id,
                owner_id=task.metadata.owner_id,
                agent_name=agent.name,
                error=str(e),
            )
        )
        return task


def classify_failure(
    exc: BaseException,
    retries: int,
    max_retries: int,
) -> tuple[CompletionAction, str | None]:
    """
    Map an exception -> (action, output_json)

    • asyncio.TimeoutError  → RETRY unless max retries hit
    • RETRYABLE_EXCEPTIONS  → BACKOFF unless max retries hit
    • everything else       → RETRY   unless max retries hit
    • If max retries hit    → FAIL with a JSON error message
    """
    # Immediate fail for unrecoverable errors
    if isinstance(exc, FatalAgentError):
        return CompletionAction.FAIL, json.dumps({"error": str(exc)})

    if isinstance(exc, asyncio.TimeoutError):
        base_action = CompletionAction.RETRY
        msg = {"error": f"Task timed out: {exc}"}

    elif isinstance(exc, tuple(RETRYABLE_EXCEPTIONS)):
        base_action = CompletionAction.BACKOFF
        msg = {"error": str(exc)}

    else:
        base_action = CompletionAction.RETRY
        msg = {"error": str(exc)}

    if retries >= max_retries:
        return CompletionAction.FAIL, json.dumps(msg)
    return base_action, None


async def process_task(
    redis_client: redis.Redis,
    namespace: str,
    task_id: str,
    completion_script: TaskCompletionScript,
    steering_script: TaskSteeringScript,
    agent: BaseAgent[ContextType],
    agents_by_name: dict[str, BaseAgent[Any]],
    max_retries: int,
    heartbeat_interval: int,
    task_timeout: int,
    metrics_bucket_duration: int,
    metrics_retention_duration: int,
) -> None:
    """Process a single task"""
    # logger.info(f"▶️  Task started   {colored(f'[{task_id}]', 'dim')}")
    task_data = await get_task_data(redis_client, namespace, task_id)
    if not task_data:
        logger.error(f"Failed to process task {task_id}: Task data not found")
        return

    try:
        task: Task[ContextType] = Task.from_dict(  # type: ignore
            task_data, context_class=agent.context_class
        )

        keys = RedisKeys.format(
            namespace=namespace,
            agent=agent.name,
            task_id=task_id,
            owner_id=task.metadata.owner_id,
            metrics_bucket_duration=metrics_bucket_duration,
        )
        parent_task_id = task.metadata.parent_id
        parent_keys = (
            RedisKeys.format(namespace=namespace, task_id=parent_task_id)
            if parent_task_id
            else None
        )
    except Exception as e:
        logger.error(
            f"Failed to process task {task_id}: Task data is invalid", exc_info=e
        )
        return

    async def complete(
        action: CompletionAction,
        pending_tool_call_ids: list[str] | None,
        pending_child_task_ids: list[str] | None,
        final_output: dict[str, Any] | str | None,
    ):
        try:
            return await completion_script.execute(
                queue_main_key=keys.queue_main,
                queue_completions_key=keys.queue_completions,
                queue_failed_key=keys.queue_failed,
                queue_backoff_key=keys.queue_backoff,
                queue_orphaned_key=keys.queue_orphaned,
                queue_pending_key=keys.queue_pending,
                task_statuses_key=keys.task_status,
                task_agents_key=keys.task_agent,
                task_payloads_key=keys.task_payload,
                task_pickups_key=keys.task_pickups,
                task_retries_key=keys.task_retries,
                task_metas_key=keys.task_meta,
                processing_heartbeats_key=keys.processing_heartbeats,
                pending_tool_results_key=keys.pending_tool_results,
                pending_child_task_results_key=keys.pending_child_task_results,
                agent_metrics_bucket_key=keys.agent_metrics_bucket,
                global_metrics_bucket_key=keys.global_metrics_bucket,
                parent_pending_child_task_results_key=parent_keys.pending_child_task_results
                if parent_keys
                else None,
                task_id=task.id,
                action=action.value,
                updated_task_payload_json=task.payload.to_json(),
                metrics_ttl=metrics_retention_duration,
                pending_sentinel=PENDING_SENTINEL,
                pending_tool_call_ids_json=json.dumps(pending_tool_call_ids)
                if pending_tool_call_ids
                else None,
                pending_child_task_ids_json=json.dumps(pending_child_task_ids)
                if pending_child_task_ids
                else None,
                final_output_json=json.dumps(final_output),
            )
        except Exception as e:
            logger.error(f"Error completing task: {e}", exc_info=e)
            # Re-raise the exception so the calling code knows the completion failed
            raise

    async def _enqueue_child_task(
        child_agent: BaseAgent[Any],
        child_payload: ContextType,  # type: ignore[type-var]
    ) -> str:
        """Lightweight wrapper to enqueue a child task.

        The function automatically sets the current *task.id* as the
        parent_id in the child task's metadata so the orchestrator can
        link the two tasks and handle result propagation/resume logic.
        Returns the *child task_id*.
        """

        # Create the child task and link to the parent via metadata
        child_task: Task[ContextType] = Task.create(
            owner_id=task.metadata.owner_id,
            agent=child_agent.name,
            payload=child_payload,
        )  # type: ignore[arg-type]

        child_task.metadata.parent_id = task.id  # Link parent

        await enqueue_task(
            redis_client=redis_client,
            namespace=namespace,
            agent=child_agent,
            task=child_task,
        )

        return child_task.id

    event_publisher = EventPublisher(
        redis_client=redis_client,
        channel=keys.updates_channel,
    )

    task_failed = False
    final_action: CompletionAction | None = (
        None  # records the action taken on completion when failing
    )

    async with heartbeat_context(
        redis_client=redis_client,
        namespace=namespace,
        task_id=task_id,
        agent=agent,
        interval=heartbeat_interval,
    ):
        try:
            execution_ctx = ExecutionContext(
                task_id=task.id,
                owner_id=task.metadata.owner_id,
                retries=task.retries,
                iterations=task.payload.turn,
                events=event_publisher,
                enqueue_child_task=_enqueue_child_task,
            )

            if task.payload.turn == 0 and task.retries == 0:
                await event_publisher.publish_event(
                    AgentEvent(
                        event_type="run_started",
                        task_id=task.id,
                        owner_id=task.metadata.owner_id,
                        agent_name=agent.name,
                    )
                )

                await agent._safe_call(
                    agent.on_run_start,
                    task.payload,
                    execution_ctx,
                )

            task = await apply_steering_if_available(
                redis_client=redis_client,
                task=task,
                agent=agent,
                execution_ctx=execution_ctx,
                steering_script=steering_script,
                namespace=namespace,
                event_publisher=event_publisher,
            )

            turn_completion = await asyncio.wait_for(
                agent.execute(task.payload, execution_ctx),
                timeout=task_timeout,
            )

            if turn_completion.pending_tool_call_ids:
                await complete(
                    action=CompletionAction.PENDING_TOOL,
                    pending_tool_call_ids=turn_completion.pending_tool_call_ids,
                    pending_child_task_ids=None,
                    final_output=None,
                )

                logger.info(
                    f"⏳ Task awaiting tool results {colored(f'[{task.id}]', 'dim')}"
                )
                await event_publisher.publish_event(
                    AgentEvent(
                        event_type="task_pending_tool_call_results",
                        task_id=task.id,
                        owner_id=task.metadata.owner_id,
                        agent_name=agent.name,
                        turn=task.payload.turn,
                        data=serialize_data(turn_completion),
                    )
                )

            if turn_completion.pending_child_task_ids:
                await complete(
                    action=CompletionAction.PENDING_CHILD,
                    pending_tool_call_ids=None,
                    pending_child_task_ids=turn_completion.pending_child_task_ids,
                    final_output=None,
                )

                logger.info(
                    f"⏳ Task awaiting child task results {colored(f'[{task.id}]', 'dim')}"
                )
                await event_publisher.publish_event(
                    AgentEvent(
                        event_type="task_pending_child_task_results",
                        task_id=task.id,
                        owner_id=task.metadata.owner_id,
                        agent_name=agent.name,
                        turn=task.payload.turn,
                        data=serialize_data(turn_completion),
                    )
                )

            elif turn_completion.is_done:
                await complete(
                    action=CompletionAction.COMPLETE,
                    pending_tool_call_ids=None,
                    pending_child_task_ids=None,
                    final_output=turn_completion.output,
                )

                # Lifecycle callback – run end (success)
                await agent._safe_call(
                    agent.on_run_end,
                    turn_completion.context,
                    execution_ctx,
                    RunCompletion(
                        output=turn_completion.output,
                        started_at=task.metadata.created_at,
                        finished_at=datetime.now(timezone.utc),
                    ),
                )

                if parent_task_id:
                    await resume_if_no_remaining_child_tasks(
                        redis_client=redis_client,
                        namespace=namespace,
                        agents_by_name=agents_by_name,
                        task_id=parent_task_id,
                    )

                # logger.info(f"✅ Task completed {colored(f'[{task.id}]', 'dim')}")
                await event_publisher.publish_event(
                    AgentEvent(
                        event_type="run_completed",
                        task_id=task.id,
                        owner_id=task.metadata.owner_id,
                        agent_name=agent.name,
                        turn=task.payload.turn,
                        data=serialize_data(turn_completion),
                    )
                )

            else:
                # Task needs to continue processing
                task.payload = turn_completion.context
                # logger.info(f"⏩ Task continued {colored(f'[{task.id}]', 'dim')}")
                await complete(
                    action=CompletionAction.CONTINUE,
                    pending_tool_call_ids=None,
                    pending_child_task_ids=None,
                    final_output=None,
                )

        except Exception as e:
            task_failed = True
            logger.error(f"❌ Task failed {colored(f'[{task.id}]', 'dim')}", exc_info=e)

            try:
                await event_publisher.publish_event(
                    QueueEvent(
                        event_type="task_failed",
                        task_id=task.id,
                        owner_id=task.metadata.owner_id,
                        agent_name=agent.name,
                        error=str(e),
                    )
                )
            except Exception as err:
                logger.error(f"Failed to send task failed event: {err}")

            action, output = classify_failure(e, task.retries, max_retries)
            final_action = action

            await complete(
                action=CompletionAction(action),
                pending_tool_call_ids=None,
                pending_child_task_ids=None,
                final_output=output,
            )

            # Lifecycle callback – run end (permanent failure)
            if final_action is CompletionAction.FAIL:
                await agent._safe_call(
                    agent.on_run_end,
                    task.payload,
                    execution_ctx,
                    RunCompletion(
                        output=None,
                        error=e,
                        started_at=task.metadata.created_at,
                        finished_at=datetime.now(timezone.utc),
                    ),
                )

            if parent_task_id and final_action is CompletionAction.FAIL:
                await resume_if_no_remaining_child_tasks(
                    redis_client=redis_client,
                    namespace=namespace,
                    agents_by_name=agents_by_name,
                    task_id=parent_task_id,
                )

        finally:
            # Only emit retry/failure events if the task actually failed
            if task_failed:
                # If the task was marked as a permanent failure, don't treat it as a retry.
                if final_action is CompletionAction.FAIL or task.retries >= max_retries:
                    logger.error(
                        f"❌ Task failed permanently {colored(f'[{task.id}]', 'dim')}"
                    )
                    await event_publisher.publish_event(
                        AgentEvent(
                            event_type="run_failed",
                            task_id=task.id,
                            owner_id=task.metadata.owner_id,
                            agent_name=agent.name,
                            error=f"Agent {agent.name} failed to complete task {task.id} due to max retries ({max_retries})",
                        )
                    )
                else:
                    logger.info(
                        f"🔄 Task set back for retry {colored(f'[{task.id}]', 'dim')}"
                    )
                    await event_publisher.publish_event(
                        QueueEvent(
                            event_type="task_retried",
                            task_id=task.id,
                            owner_id=task.metadata.owner_id,
                            agent_name=agent.name,
                        )
                    )


async def worker_loop(
    shutdown_event: asyncio.Event,
    redis_pool: redis.ConnectionPool,
    namespace: str,
    worker_id: str,
    agent: BaseAgent[Any],
    agents_by_name: dict[str, BaseAgent[Any]],
    batch_size: int,
    max_retries: int,
    heartbeat_interval: int,
    task_timeout: int,
    metrics_bucket_duration: int,
    metrics_retention_duration: int,
) -> None:
    """Main worker loop"""
    redis_client = redis.Redis(connection_pool=redis_pool, decode_responses=True)
    batch_script = await create_batch_pickup_script(redis_client)
    completion_script = await create_task_completion_script(redis_client)
    steering_script = await create_task_steering_script(redis_client)

    logger.info(f"Worker {worker_id} started")
    current_tasks: list[asyncio.Task[Any]] = []

    try:
        while not shutdown_event.is_set():
            task_batch: tuple[list[str], list[str]] = await get_task_batch(
                batch_script=batch_script,
                agent=agent,
                batch_size=batch_size,
                metrics_bucket_duration=metrics_bucket_duration,
                metrics_ttl=metrics_retention_duration,
                namespace=namespace,
            )
            tasks_to_process_ids: list[str] = task_batch[0]
            tasks_to_cancel_ids: list[str] = task_batch[1]

            if tasks_to_process_ids:
                logger.info(
                    f"Worker {worker_id} got {len(tasks_to_process_ids)} tasks to process"
                )

                cancellation_task = (
                    asyncio.create_task(
                        process_cancelled_tasks(
                            redis_client=redis_client,
                            namespace=namespace,
                            cancelled_task_ids=tasks_to_cancel_ids,
                            agent=agent,
                        )
                    )
                    if tasks_to_cancel_ids
                    else None
                )

                current_tasks = [
                    asyncio.create_task(
                        process_task(
                            redis_client=redis_client,
                            namespace=namespace,
                            task_id=task_id,
                            completion_script=completion_script,
                            steering_script=steering_script,
                            agent=agent,
                            agents_by_name=agents_by_name,
                            max_retries=max_retries,
                            heartbeat_interval=heartbeat_interval,
                            task_timeout=task_timeout,
                            metrics_bucket_duration=metrics_bucket_duration,
                            metrics_retention_duration=metrics_retention_duration,
                        )
                    )
                    for task_id in tasks_to_process_ids
                ]

                all_tasks = current_tasks + (
                    [cancellation_task] if cancellation_task else []
                )
                await asyncio.gather(*all_tasks, return_exceptions=True)
                current_tasks = []

                # Check if we should exit after completing batch
                if shutdown_event.is_set():
                    logger.info(
                        f"Worker {worker_id} shutting down after completing batch"
                    )
                    break
            else:
                if tasks_to_cancel_ids:
                    await process_cancelled_tasks(
                        redis_client=redis_client,
                        namespace=namespace,
                        cancelled_task_ids=tasks_to_cancel_ids,
                        agent=agent,
                    )

                # No tasks, wait a bit but check for shutdown
                sleep_time = 0.25 + random.uniform(0, 0.5)  # 0.25s to 0.75s
                try:
                    await asyncio.wait_for(shutdown_event.wait(), timeout=sleep_time)
                    break
                except asyncio.TimeoutError:
                    pass

    except asyncio.CancelledError:
        # Cancel any running tasks
        for task in current_tasks:
            if not task.done():
                task.cancel()
        if current_tasks:
            await asyncio.gather(*current_tasks, return_exceptions=True)
        logger.info(f"Worker {worker_id} cancelled")
        raise

    finally:
        await redis_client.close()
        logger.info(f"Worker {worker_id} finished")
