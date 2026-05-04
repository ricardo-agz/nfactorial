import asyncio
from enum import Enum
from typing import Any

import redis.asyncio as redis

from factorial._internal.agent.tools.runtime import process_deferred_tool_results
from factorial._internal.agent.tools.types import _ToolResultInternal
from factorial._internal.queue.operations import (
    process_hook_runtime_wake_requests,
)
from factorial.agent import BaseAgent
from factorial.agent.context import ContextType
from factorial.core.events import (
    EventPublisher,
    FinishEvent,
    QueueEvent,
    ToolFinishEvent,
)
from factorial.core.logging import colored
from factorial.core.run_types import RunError, RunStatus
from factorial.execution.context import ExecutionContext, execution_context
from factorial.execution.waits import WaitInstruction
from factorial.queue.task import Task

from .common import CompletionAction, extract_wait_instructions, logger
from .parking import (
    ParkChildren,
    ParkPendingTools,
    compile_wait_command,
)
from .transitions import (
    ParkTask,
    TaskTransitionContext,
    execute_transition,
    transition_from_failure,
    transition_from_turn_completion,
)


class TaskProcessingState(str, Enum):
    HOOKS = "hooks"
    EXECUTE = "execute"
    COMPLETE = "complete"
    DONE = "done"


def _transition_context(
    *,
    redis_client: redis.Redis,
    namespace: str,
    task: Task[ContextType],
    parent_task_id: str | None,
    agent: BaseAgent[ContextType],
    agents_by_name: dict[str, BaseAgent[Any]],
    execution_ctx: ExecutionContext,
    event_publisher: EventPublisher,
    complete: Any,
    park_or_resume_child_wait: Any,
    park_activity_wait: Any,
    park_signal_wait: Any,
    park_scheduled_wait: Any,
    publish_batch_progress: Any,
) -> TaskTransitionContext:
    return TaskTransitionContext(
        redis_client=redis_client,
        namespace=namespace,
        task=task,
        parent_task_id=parent_task_id,
        agent=agent,
        agents_by_name=agents_by_name,
        execution_ctx=execution_ctx,
        event_publisher=event_publisher,
        complete=complete,
        park_or_resume_child_wait=park_or_resume_child_wait,
        park_activity_wait=park_activity_wait,
        park_signal_wait=park_signal_wait,
        park_scheduled_wait=park_scheduled_wait,
        publish_batch_progress=publish_batch_progress,
    )


async def handle_hook_state(
    *,
    redis_client: redis.Redis,
    namespace: str,
    agent: BaseAgent[ContextType],
    task: Task[ContextType],
    parent_task_id: str | None,
    agents_by_name: dict[str, BaseAgent[Any]],
    execution_ctx: ExecutionContext,
    event_publisher: EventPublisher,
    complete: Any,
    park_or_resume_child_wait: Any,
    park_activity_wait: Any,
    park_signal_wait: Any,
    park_scheduled_wait: Any,
    publish_batch_progress: Any,
) -> bool:
    """Handle hook-runtime progression before normal turn execution."""
    hook_tick = await process_hook_runtime_wake_requests(
        redis_client=redis_client,
        namespace=namespace,
        agent=agent,
        task=task,
        execution_ctx=execution_ctx,
    )
    hook_pending_child_task_ids: list[str] = []
    if hook_tick.completed_results:
        for _, hook_result in hook_tick.completed_results:
            if (
                isinstance(hook_result, _ToolResultInternal)
                and hook_result.pending_child_task_ids
            ):
                hook_pending_child_task_ids.extend(hook_result.pending_child_task_ids)

        task.payload = process_deferred_tool_results(
            task.payload,
            hook_tick.completed_results,
        ).context
        for tool_call_id, hook_result in hook_tick.completed_results:
            client_output = (
                hook_result.client_output
                if isinstance(hook_result, _ToolResultInternal)
                else hook_result
            )
            await agent._emit_event(
                ToolFinishEvent(
                    task_id=task.id,
                    owner_id=task.metadata.owner_id,
                    agent_name=agent.name,
                    turn=task.payload.turn_number,
                    tool_call_id=tool_call_id,
                    output=client_output,
                    is_error=False,
                ),
                task.payload,
                execution_ctx,
            )

    if hook_tick.should_repark:
        if hook_pending_child_task_ids:
            raise RuntimeError(
                "Hook runtime produced both pending child tasks and "
                "still-pending hook sessions in the same tick."
            )
        if not hook_tick.pending_tool_call_ids:
            raise RuntimeError(
                "Hook runtime requested re-park with no pending tool calls."
            )
        await execute_transition(
            ParkTask(ParkPendingTools(tuple(hook_tick.pending_tool_call_ids))),
            _transition_context(
                redis_client=redis_client,
                namespace=namespace,
                task=task,
                parent_task_id=parent_task_id,
                agent=agent,
                agents_by_name=agents_by_name,
                execution_ctx=execution_ctx,
                event_publisher=event_publisher,
                complete=complete,
                park_or_resume_child_wait=park_or_resume_child_wait,
                park_activity_wait=park_activity_wait,
                park_signal_wait=park_signal_wait,
                park_scheduled_wait=park_scheduled_wait,
                publish_batch_progress=publish_batch_progress,
            ),
        )
        return True

    if hook_pending_child_task_ids:
        await execute_transition(
            ParkTask(ParkChildren(tuple(hook_pending_child_task_ids))),
            _transition_context(
                redis_client=redis_client,
                namespace=namespace,
                task=task,
                parent_task_id=parent_task_id,
                agent=agent,
                agents_by_name=agents_by_name,
                execution_ctx=execution_ctx,
                event_publisher=event_publisher,
                complete=complete,
                park_or_resume_child_wait=park_or_resume_child_wait,
                park_activity_wait=park_activity_wait,
                park_signal_wait=park_signal_wait,
                park_scheduled_wait=park_scheduled_wait,
                publish_batch_progress=publish_batch_progress,
            ),
        )
        return True

    return False


async def handle_wait_state(
    *,
    redis_client: redis.Redis,
    namespace: str,
    task: Task[ContextType],
    agent: BaseAgent[ContextType],
    parent_task_id: str | None,
    agents_by_name: dict[str, BaseAgent[Any]],
    execution_ctx: ExecutionContext,
    turn_completion: Any,
    wait_instructions: list[tuple[str, WaitInstruction]],
    event_publisher: EventPublisher,
    complete: Any,
    park_or_resume_child_wait: Any,
    park_activity_wait: Any,
    park_signal_wait: Any,
    park_scheduled_wait: Any,
    publish_batch_progress: Any,
) -> bool:
    """Handle all wait instructions for the current turn."""
    await execute_transition(
        ParkTask(
            compile_wait_command(
                task_id=task.id,
                turn_completion=turn_completion,
                wait_instructions=wait_instructions,
            )
        ),
        _transition_context(
            redis_client=redis_client,
            namespace=namespace,
            task=task,
            parent_task_id=parent_task_id,
            agent=agent,
            agents_by_name=agents_by_name,
            execution_ctx=execution_ctx,
            event_publisher=event_publisher,
            complete=complete,
            park_or_resume_child_wait=park_or_resume_child_wait,
            park_activity_wait=park_activity_wait,
            park_signal_wait=park_signal_wait,
            park_scheduled_wait=park_scheduled_wait,
            publish_batch_progress=publish_batch_progress,
        ),
    )
    return True


async def handle_completion_state(
    *,
    redis_client: redis.Redis,
    namespace: str,
    task: Task[ContextType],
    parent_task_id: str | None,
    agent: BaseAgent[ContextType],
    agents_by_name: dict[str, BaseAgent[Any]],
    execution_ctx: ExecutionContext,
    turn_completion: Any,
    event_publisher: EventPublisher,
    complete: Any,
    park_or_resume_child_wait: Any,
    park_activity_wait: Any,
    park_signal_wait: Any,
    park_scheduled_wait: Any,
    publish_batch_progress: Any,
) -> None:
    """Handle non-wait turn completion transitions."""
    await execute_transition(
        transition_from_turn_completion(turn_completion),
        _transition_context(
            redis_client=redis_client,
            namespace=namespace,
            task=task,
            parent_task_id=parent_task_id,
            agent=agent,
            agents_by_name=agents_by_name,
            execution_ctx=execution_ctx,
            event_publisher=event_publisher,
            complete=complete,
            park_or_resume_child_wait=park_or_resume_child_wait,
            park_activity_wait=park_activity_wait,
            park_signal_wait=park_signal_wait,
            park_scheduled_wait=park_scheduled_wait,
            publish_batch_progress=publish_batch_progress,
        ),
    )


async def run_task_state_machine(
    *,
    redis_client: redis.Redis,
    namespace: str,
    task: Task[ContextType],
    task_timeout: int,
    parent_task_id: str | None,
    agent: BaseAgent[ContextType],
    agents_by_name: dict[str, BaseAgent[Any]],
    execution_ctx: ExecutionContext,
    event_publisher: EventPublisher,
    complete: Any,
    park_or_resume_child_wait: Any,
    park_activity_wait: Any,
    park_signal_wait: Any,
    park_scheduled_wait: Any,
    publish_batch_progress: Any,
) -> None:
    """Small coordinator for hook -> execute -> wait/completion transitions."""
    state = TaskProcessingState.HOOKS
    turn_completion: Any | None = None

    while state is not TaskProcessingState.DONE:
        if state is TaskProcessingState.HOOKS:
            should_stop = await handle_hook_state(
                redis_client=redis_client,
                namespace=namespace,
                agent=agent,
                task=task,
                parent_task_id=parent_task_id,
                agents_by_name=agents_by_name,
                execution_ctx=execution_ctx,
                event_publisher=event_publisher,
                complete=complete,
                park_or_resume_child_wait=park_or_resume_child_wait,
                park_activity_wait=park_activity_wait,
                park_signal_wait=park_signal_wait,
                park_scheduled_wait=park_scheduled_wait,
                publish_batch_progress=publish_batch_progress,
            )
            state = (
                TaskProcessingState.DONE if should_stop else TaskProcessingState.EXECUTE
            )
            continue

        if state is TaskProcessingState.EXECUTE:
            token = execution_context.set(execution_ctx)
            try:
                execution_result = await asyncio.wait_for(
                    agent.run_turn(task.payload),
                    timeout=task_timeout,
                )
            finally:
                execution_context.reset(token)
            turn_completion = execution_result
            task.payload = execution_result.context
            wait_instructions = extract_wait_instructions(
                execution_result.tool_call_results
            )
            if wait_instructions:
                should_stop = await handle_wait_state(
                    redis_client=redis_client,
                    namespace=namespace,
                    task=task,
                    agent=agent,
                    parent_task_id=parent_task_id,
                    agents_by_name=agents_by_name,
                    execution_ctx=execution_ctx,
                    turn_completion=turn_completion,
                    wait_instructions=wait_instructions,
                    event_publisher=event_publisher,
                    complete=complete,
                    park_or_resume_child_wait=park_or_resume_child_wait,
                    park_activity_wait=park_activity_wait,
                    park_signal_wait=park_signal_wait,
                    park_scheduled_wait=park_scheduled_wait,
                    publish_batch_progress=publish_batch_progress,
                )
                state = (
                    TaskProcessingState.DONE
                    if should_stop
                    else TaskProcessingState.COMPLETE
                )
            else:
                state = TaskProcessingState.COMPLETE
            continue

        if state is TaskProcessingState.COMPLETE:
            if turn_completion is None:
                raise RuntimeError(
                    "Task state machine reached completion without turn data"
                )
            await handle_completion_state(
                redis_client=redis_client,
                namespace=namespace,
                task=task,
                parent_task_id=parent_task_id,
                agent=agent,
                agents_by_name=agents_by_name,
                execution_ctx=execution_ctx,
                turn_completion=turn_completion,
                event_publisher=event_publisher,
                complete=complete,
                park_or_resume_child_wait=park_or_resume_child_wait,
                park_activity_wait=park_activity_wait,
                park_signal_wait=park_signal_wait,
                park_scheduled_wait=park_scheduled_wait,
                publish_batch_progress=publish_batch_progress,
            )
            state = TaskProcessingState.DONE
            continue

        raise RuntimeError(f"Unknown task processing state '{state}'")


async def handle_failure_state(
    *,
    redis_client: redis.Redis,
    namespace: str,
    task: Task[ContextType],
    parent_task_id: str | None,
    agent: BaseAgent[ContextType],
    agents_by_name: dict[str, BaseAgent[Any]],
    execution_ctx: ExecutionContext,
    error: Exception,
    failure_action: CompletionAction,
    failure_output: str | None,
    event_publisher: EventPublisher,
    complete: Any,
    park_or_resume_child_wait: Any,
    park_activity_wait: Any,
    park_signal_wait: Any,
    park_scheduled_wait: Any,
    publish_batch_progress: Any,
) -> None:
    """Handle task failure transition + callback side effects."""
    logger.error(f"❌ Task failed {colored(f'[{task.id}]', 'dim')}", exc_info=error)

    try:
        await event_publisher.publish_event(
            QueueEvent(
                event_type="task_failed",
                task_id=task.id,
                owner_id=task.metadata.owner_id,
                agent_name=agent.name,
                error=str(error),
            )
        )
    except Exception as publish_err:
        logger.error(f"Failed to send task failed event: {publish_err}")

    await execute_transition(
        transition_from_failure(
            action=failure_action,
            output=failure_output,
            error=error,
        ),
        _transition_context(
            redis_client=redis_client,
            namespace=namespace,
            task=task,
            parent_task_id=parent_task_id,
            agent=agent,
            agents_by_name=agents_by_name,
            execution_ctx=execution_ctx,
            event_publisher=event_publisher,
            complete=complete,
            park_or_resume_child_wait=park_or_resume_child_wait,
            park_activity_wait=park_activity_wait,
            park_signal_wait=park_signal_wait,
            park_scheduled_wait=park_scheduled_wait,
            publish_batch_progress=publish_batch_progress,
        ),
    )


async def emit_failure_outcome_events(
    *,
    task: Task[ContextType],
    agent: BaseAgent[ContextType],
    max_retries: int,
    failure_action: CompletionAction,
    event_publisher: EventPublisher,
    publish_batch_progress: Any,
) -> None:
    """Emit post-failure terminal/retry events."""
    if failure_action is CompletionAction.FAIL or task.retries >= max_retries:
        logger.error(f"❌ Task failed permanently {colored(f'[{task.id}]', 'dim')}")
        await event_publisher.publish_event(
            FinishEvent(
                task_id=task.id,
                owner_id=task.metadata.owner_id,
                agent_name=agent.name,
                status=RunStatus.FAILED,
                run_error=RunError(
                    type="TaskFailure",
                    message=(
                        f"Agent {agent.name} failed to complete "
                        f"task {task.id} (max retries: {max_retries})"
                    ),
                ),
            )
        )

        if task.metadata.batch_id:
            await publish_batch_progress(task.metadata.batch_id)
        return

    logger.info(f"🔄 Task set back for retry {colored(f'[{task.id}]', 'dim')}")
    await event_publisher.publish_event(
        QueueEvent(
            event_type="task_retried",
            task_id=task.id,
            owner_id=task.metadata.owner_id,
            agent_name=agent.name,
        )
    )
