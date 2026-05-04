import asyncio
import random
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
from typing import Any

import redis.asyncio as redis

from factorial.agent import BaseAgent
from factorial.agent.context import ContextType
from factorial.core.events import EventPublisher, StartEvent
from factorial.execution.context import (
    ExecutionContext,
    HooksExecutionNamespace,
    InboxDirectExecutionNamespace,
    InboxExecutionNamespace,
    InboxGroupExecutionNamespace,
    InboxReceiptsExecutionNamespace,
    MessagingExecutionNamespace,
    MessagingGroupsExecutionNamespace,
    SignalsExecutionNamespace,
    SubagentsExecutionNamespace,
)
from factorial.queue.keys import RedisKeys
from factorial.queue.lua import (
    ActivityWaitScript,
    QueueScripts,
    SignalWaitScript,
    TaskCompletionScript,
    TaskSteeringScript,
    WaitScheduleScript,
    create_activity_wait_script,
    create_signal_wait_script,
    create_wait_schedule_script,
)
from factorial.queue.task import Task, get_task_data
from factorial.resources import (
    RedisResourceBindingStore,
    ResourceLease,
    ResourceManager,
    ResourcesExecutionNamespace,
)

from .common import apply_steering_if_available, classify_failure, logger
from .runtime_ops import TaskRuntimeOps
from .state_machine import (
    emit_failure_outcome_events,
    handle_failure_state,
    run_task_state_machine,
)


async def heartbeat_loop(
    redis_client: redis.Redis,
    namespace: str,
    task_id: str,
    agent: BaseAgent[Any],
    stop_event: asyncio.Event,
    interval: int,
) -> None:
    """Simple heartbeat loop that runs until stopped."""
    keys = RedisKeys.format(namespace=namespace, agent=agent.name)

    try:
        while not stop_event.is_set():
            try:
                await redis_client.zadd(
                    keys.processing_heartbeats,
                    {task_id: time.time()},
                )
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=interval)
                    break
                except asyncio.TimeoutError:
                    continue
            except Exception as e:
                logger.error(f"Heartbeat error for {task_id}: {e}")
                await asyncio.sleep(0.25 + random.random() * 0.5)
    except asyncio.CancelledError:
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
) -> AsyncIterator[None]:
    """Run ``heartbeat_loop`` in the background for the ``with`` block."""
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
        with suppress(asyncio.CancelledError):
            await hb_task


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
    metrics_retention_duration: int,
    wait_schedule_script: WaitScheduleScript | None = None,
    activity_wait_script: ActivityWaitScript | None = None,
    signal_wait_script: SignalWaitScript | None = None,
) -> None:
    """Process a single task."""
    task_data = await get_task_data(redis_client, namespace, task_id)
    if not task_data:
        logger.error(f"Failed to process task {task_id}: Task data not found")
        return

    try:
        task: Task[ContextType] = Task.from_dict(
            task_data,
            payload_parser=agent.context_from_dict,
        )
        keys = RedisKeys.format(
            namespace=namespace,
            agent=agent.name,
            task_id=task_id,
            owner_id=task.metadata.owner_id,
        )
        parent_task_id = task.metadata.parent_id
    except Exception as e:
        logger.error(
            f"Failed to process task {task_id}: Task data is invalid",
            exc_info=e,
        )
        return

    event_publisher = EventPublisher(
        redis_client=redis_client,
        channel=keys.updates_channel,
    )
    if wait_schedule_script is None:
        wait_schedule_script = await create_wait_schedule_script(redis_client)
    if activity_wait_script is None:
        activity_wait_script = await create_activity_wait_script(redis_client)
    if signal_wait_script is None:
        signal_wait_script = await create_signal_wait_script(redis_client)

    queue_scripts = QueueScripts.for_agent(
        redis_client=redis_client,
        namespace=namespace,
        agent_name=agent.name,
        metrics_ttl=metrics_retention_duration,
        completion_script=completion_script,
    )

    runtime = TaskRuntimeOps(
        redis_client=redis_client,
        namespace=namespace,
        agent=agent,
        agents_by_name=agents_by_name,
        task=task,
        keys=keys,
        event_publisher=event_publisher,
        queue_scripts=queue_scripts,
        wait_schedule_script=wait_schedule_script,
        activity_wait_script=activity_wait_script,
        signal_wait_script=signal_wait_script,
        metrics_retention_duration=metrics_retention_duration,
    )

    task_failed = False
    final_action = None
    execution_ctx = ExecutionContext(
        task_id=task.id,
        owner_id=task.metadata.owner_id,
        agent_name=agent.name,
        retry_count=task.retries,
        events=event_publisher,
    )

    async with heartbeat_context(
        redis_client=redis_client,
        namespace=namespace,
        task_id=task_id,
        agent=agent,
        interval=heartbeat_interval,
    ):
        try:
            execution_ctx.subagents = SubagentsExecutionNamespace(
                enqueue_callback=runtime.enqueue_child_task,
                enqueue_batch_callback=runtime.enqueue_batch,
                cancel_callback=runtime.cancel_child_task,
                cancel_many_callback=runtime.cancel_child_tasks,
                signal_callback=runtime.signal_child_task,
                signal_many_callback=runtime.signal_child_tasks,
            )
            execution_ctx.hooks = HooksExecutionNamespace(
                persist_runtime_callback=runtime.persist_hook_runtime
            )
            execution_ctx.messaging = MessagingExecutionNamespace(
                send_callback=runtime.messaging_send_direct,
                groups=MessagingGroupsExecutionNamespace(
                    create_callback=runtime.messaging_create_group,
                    get_callback=runtime.messaging_get_group,
                    list_callback=runtime.messaging_list_groups,
                    find_callback=runtime.messaging_find_groups,
                    add_members_callback=runtime.messaging_add_group_members,
                    remove_members_callback=runtime.messaging_remove_group_members,
                    leave_callback=runtime.messaging_leave_group,
                    send_callback=runtime.messaging_send_group,
                ),
            )
            execution_ctx.inbox = InboxExecutionNamespace(
                direct=InboxDirectExecutionNamespace(
                    peek_callback=runtime.inbox_direct_peek,
                    mark_read_callback=runtime.inbox_direct_mark_read,
                ),
                group=InboxGroupExecutionNamespace(
                    peek_callback=runtime.inbox_group_peek,
                    mark_read_callback=runtime.inbox_group_mark_read,
                ),
                receipts=InboxReceiptsExecutionNamespace(
                    peek_callback=runtime.inbox_receipts_peek,
                    mark_read_callback=runtime.inbox_receipts_mark_read,
                ),
            )
            execution_ctx.signals = SignalsExecutionNamespace()
            execution_ctx.resources = ResourcesExecutionNamespace(
                manager=ResourceManager(
                    store=RedisResourceBindingStore(
                        redis_client=redis_client,
                        namespace=namespace,
                        task_id=task.id,
                    ),
                    task_id=task.id,
                    owner_id=task.metadata.owner_id,
                    agent_name=agent.name,
                    lease=ResourceLease.worker(task.pickups),
                ),
                default_sandbox_provider=agent.default_sandbox_provider,
            )

            if task.payload.turn_number == 1 and task.retries == 0:
                await agent._emit_event(
                    StartEvent(
                        task_id=task.id,
                        owner_id=task.metadata.owner_id,
                        agent_name=agent.name,
                    ),
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
            runtime.task = task

            signal_wake_context = await runtime.pop_signal_wake_context()
            if (
                isinstance(signal_wake_context, dict)
                and str(signal_wake_context.get("wake_reason", "")).lower() == "timeout"
            ):
                execution_ctx.signals.current_signal = None
                execution_ctx.signals.wake_reason_value = "timeout"
            elif isinstance(signal_wake_context, dict):
                execution_ctx.signals.current_signal = signal_wake_context
                wake_reason = signal_wake_context.get("wake_reason")
                if isinstance(wake_reason, str) and wake_reason:
                    execution_ctx.signals.wake_reason_value = wake_reason
                else:
                    execution_ctx.signals.wake_reason_value = "signal"
            else:
                execution_ctx.signals.current_signal = None
                execution_ctx.signals.wake_reason_value = None

            await run_task_state_machine(
                redis_client=redis_client,
                namespace=namespace,
                task=task,
                task_timeout=task_timeout,
                parent_task_id=parent_task_id,
                agent=agent,
                agents_by_name=agents_by_name,
                execution_ctx=execution_ctx,
                event_publisher=event_publisher,
                complete=runtime.complete,
                park_or_resume_child_wait=runtime.park_or_resume_child_wait,
                park_activity_wait=runtime.park_activity_wait,
                park_signal_wait=runtime.park_signal_wait,
                park_scheduled_wait=runtime.park_scheduled_wait,
                publish_batch_progress=runtime.publish_batch_progress,
            )
            return

        except Exception as e:
            task_failed = True
            action, output = classify_failure(e, task.retries, max_retries)
            final_action = action
            await handle_failure_state(
                redis_client=redis_client,
                namespace=namespace,
                task=task,
                parent_task_id=parent_task_id,
                agent=agent,
                agents_by_name=agents_by_name,
                execution_ctx=execution_ctx,
                error=e,
                failure_action=action,
                failure_output=output,
                event_publisher=event_publisher,
                complete=runtime.complete,
                park_or_resume_child_wait=runtime.park_or_resume_child_wait,
                park_activity_wait=runtime.park_activity_wait,
                park_signal_wait=runtime.park_signal_wait,
                park_scheduled_wait=runtime.park_scheduled_wait,
                publish_batch_progress=runtime.publish_batch_progress,
            )

        finally:
            if task_failed and final_action is not None:
                await emit_failure_outcome_events(
                    task=task,
                    agent=agent,
                    max_retries=max_retries,
                    failure_action=final_action,
                    event_publisher=event_publisher,
                    publish_batch_progress=runtime.publish_batch_progress,
                )

