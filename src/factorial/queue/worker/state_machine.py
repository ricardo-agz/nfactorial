import asyncio
import time
from datetime import datetime, timezone
from enum import Enum
from typing import Any

import redis.asyncio as redis

from factorial.agent import BaseAgent, ExecutionContext, RunCompletion, serialize_data
from factorial.context import ContextType
from factorial.events import AgentEvent, EventPublisher, QueueEvent
from factorial.logging import colored
from factorial.queue.operations import (
    process_hook_runtime_wake_requests,
    resume_if_no_remaining_child_tasks,
)
from factorial.queue.task import Task
from factorial.tools import _ToolResultInternal
from factorial.waits import WaitInstruction, next_cron_wake_timestamp

from .common import CompletionAction, extract_wait_instructions, logger


class TaskProcessingState(str, Enum):
    HOOKS = "hooks"
    EXECUTE = "execute"
    COMPLETE = "complete"
    DONE = "done"


async def handle_hook_state(
    *,
    redis_client: redis.Redis,
    namespace: str,
    agent: BaseAgent[ContextType],
    task: Task[ContextType],
    execution_ctx: ExecutionContext,
    event_publisher: EventPublisher,
    complete: Any,
    park_or_resume_child_wait: Any,
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

        task.payload = agent.process_deferred_tool_results(
            task.payload,
            hook_tick.completed_results,
        ).context
        await agent._safe_call(
            agent.on_pending_tool_results,
            task.payload,
            execution_ctx,
            hook_tick.completed_results,
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
        await complete(
            action=CompletionAction.PENDING_TOOL,
            pending_tool_call_ids=hook_tick.pending_tool_call_ids,
            pending_child_task_ids=None,
            final_output=None,
        )
        await event_publisher.publish_event(
            AgentEvent(
                event_type="task_pending_tool_call_results",
                task_id=task.id,
                owner_id=task.metadata.owner_id,
                agent_name=agent.name,
                turn=task.payload.turn,
            )
        )
        return True

    if hook_pending_child_task_ids:
        await park_or_resume_child_wait(
            child_task_ids=hook_pending_child_task_ids,
            event_data=None,
        )
        return True

    return False


async def handle_wait_state(
    *,
    task: Task[ContextType],
    agent: BaseAgent[ContextType],
    turn_completion: Any,
    wait_instructions: list[tuple[str, WaitInstruction]],
    event_publisher: EventPublisher,
    park_or_resume_child_wait: Any,
    park_activity_wait: Any,
    park_scheduled_wait: Any,
) -> bool:
    """Handle all wait instructions for the current turn."""
    if (
        turn_completion.pending_tool_call_ids
        or turn_completion.pending_child_task_ids
    ):
        raise RuntimeError(
            "Turn cannot combine wait instructions with pending tool "
            "or child-task continuations."
        )

    source_tool_call_ids = [tool_call_id for tool_call_id, _ in wait_instructions]
    wait_kinds = {wait_instruction.kind for _, wait_instruction in wait_instructions}
    if len(wait_kinds) != 1:
        raise RuntimeError(
            "All wait instructions in a single turn must have "
            "the same kind."
        )

    wait_kind = next(iter(wait_kinds))
    if wait_kind == "jobs":
        child_task_ids: list[str] = []
        for _, wait_instruction in wait_instructions:
            for job_ref in wait_instruction.job_refs or []:
                ref_parent_task_id = job_ref.get("parent_task_id")
                if ref_parent_task_id is not None and ref_parent_task_id != task.id:
                    raise ValueError(
                        "wait.jobs received job refs from a different "
                        f"parent task. Expected '{task.id}', got "
                        f"'{ref_parent_task_id}'."
                    )
                ref_task_id = job_ref.get("task_id")
                if not isinstance(ref_task_id, str) or not ref_task_id:
                    raise ValueError(
                        "wait.jobs received a job ref with invalid task_id."
                    )
                child_task_ids.append(ref_task_id)

            if wait_instruction.child_task_ids:
                child_task_ids.extend(wait_instruction.child_task_ids)

        if not child_task_ids:
            raise ValueError("wait.jobs requires at least one job/task reference.")

        await park_or_resume_child_wait(
            child_task_ids=child_task_ids,
            event_data=serialize_data(turn_completion),
        )
        return True

    if wait_kind == "activity":
        wait_data: Any = None
        for _, wait_instruction in wait_instructions:
            if wait_data is None and wait_instruction.data is not None:
                wait_data = wait_instruction.data
        await park_activity_wait(
            source_tool_call_ids=source_tool_call_ids,
            data=wait_data,
        )
        await event_publisher.publish_event(
            AgentEvent(
                event_type="task_activity_waiting",
                task_id=task.id,
                owner_id=task.metadata.owner_id,
                agent_name=agent.name,
                turn=task.payload.turn,
                data={
                    "wait_kind": "activity",
                    "source_tool_call_ids": source_tool_call_ids,
                },
            )
        )
        return True

    wake_timestamps: list[float] = []
    scheduled_wait_data: Any = None
    for _, wait_instruction in wait_instructions:
        if scheduled_wait_data is None and wait_instruction.data is not None:
            scheduled_wait_data = wait_instruction.data

        if wait_instruction.kind == "sleep":
            if wait_instruction.sleep_s is None:
                raise ValueError("wait.sleep requires a sleep duration.")
            if wait_instruction.sleep_s < 0:
                raise ValueError("wait.sleep requires a non-negative duration.")
            wake_timestamps.append(time.time() + wait_instruction.sleep_s)
        elif wait_instruction.kind == "cron":
            if not wait_instruction.cron:
                raise ValueError("wait.cron requires a non-empty cron expression.")
            wake_timestamps.append(
                next_cron_wake_timestamp(
                    wait_instruction.cron,
                    wait_instruction.timezone or "UTC",
                )
            )
        else:
            raise RuntimeError(f"Unsupported wait kind '{wait_instruction.kind}'.")

    wake_timestamp = min(wake_timestamps)
    representative = wait_instructions[0][1]
    if wait_kind == "cron":
        cron_signatures = {
            (
                wait_instruction.cron,
                wait_instruction.timezone or "UTC",
            )
            for _, wait_instruction in wait_instructions
        }
        if len(cron_signatures) != 1:
            raise RuntimeError(
                "Multiple wait.cron instructions in a single turn must "
                "use the same expression and timezone."
            )

    await park_scheduled_wait(
        wait_kind=wait_kind,
        wake_timestamp=wake_timestamp,
        source_tool_call_ids=source_tool_call_ids,
        data=scheduled_wait_data,
        cron_expression=(representative.cron if wait_kind == "cron" else None),
        cron_timezone=(representative.timezone if wait_kind == "cron" else None),
    )
    await event_publisher.publish_event(
        AgentEvent(
            event_type="task_paused",
            task_id=task.id,
            owner_id=task.metadata.owner_id,
            agent_name=agent.name,
            turn=task.payload.turn,
            data={
                "wait_kind": wait_kind,
                "wake_timestamp": wake_timestamp,
                "source_tool_call_ids": source_tool_call_ids,
            },
        )
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
    publish_batch_progress: Any,
) -> None:
    """Handle non-wait turn completion transitions."""
    if (
        turn_completion.pending_tool_call_ids
        and turn_completion.pending_child_task_ids
    ):
        raise RuntimeError(
            "Turn cannot simultaneously park on pending tool results and "
            "pending child task results."
        )

    if turn_completion.pending_tool_call_ids:
        await complete(
            action=CompletionAction.PENDING_TOOL,
            pending_tool_call_ids=turn_completion.pending_tool_call_ids,
            pending_child_task_ids=None,
            final_output=None,
        )

        logger.info(f"⏳ Task awaiting tool results {colored(f'[{task.id}]', 'dim')}")
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
        return

    if turn_completion.pending_child_task_ids:
        logger.info(
            f"⏳ Task awaiting child task results {colored(f'[{task.id}]', 'dim')}"
        )
        await park_or_resume_child_wait(
            child_task_ids=turn_completion.pending_child_task_ids,
            event_data=serialize_data(turn_completion),
        )
        return

    if turn_completion.is_done:
        await complete(
            action=CompletionAction.COMPLETE,
            pending_tool_call_ids=None,
            pending_child_task_ids=None,
            final_output=turn_completion.output,
        )

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

        if task.metadata.batch_id:
            await publish_batch_progress(task.metadata.batch_id)
        return

    await complete(
        action=CompletionAction.CONTINUE,
        pending_tool_call_ids=None,
        pending_child_task_ids=None,
        final_output=None,
    )
    if task.metadata.batch_id and agent.max_turns:
        await publish_batch_progress(task.metadata.batch_id)


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
                execution_ctx=execution_ctx,
                event_publisher=event_publisher,
                complete=complete,
                park_or_resume_child_wait=park_or_resume_child_wait,
            )
            state = (
                TaskProcessingState.DONE
                if should_stop
                else TaskProcessingState.EXECUTE
            )
            continue

        if state is TaskProcessingState.EXECUTE:
            execution_result = await asyncio.wait_for(
                agent.execute(task.payload, execution_ctx),
                timeout=task_timeout,
            )
            turn_completion = execution_result
            task.payload = execution_result.context
            wait_instructions = extract_wait_instructions(
                execution_result.tool_call_results
            )
            if wait_instructions:
                should_stop = await handle_wait_state(
                    task=task,
                    agent=agent,
                    turn_completion=turn_completion,
                    wait_instructions=wait_instructions,
                    event_publisher=event_publisher,
                    park_or_resume_child_wait=park_or_resume_child_wait,
                    park_activity_wait=park_activity_wait,
                    park_scheduled_wait=park_scheduled_wait,
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

    await complete(
        action=failure_action,
        pending_tool_call_ids=None,
        pending_child_task_ids=None,
        final_output=failure_output,
    )

    if failure_action is CompletionAction.FAIL:
        await agent._safe_call(
            agent.on_run_end,
            task.payload,
            execution_ctx,
            RunCompletion(
                output=None,
                error=error,
                started_at=task.metadata.created_at,
                finished_at=datetime.now(timezone.utc),
            ),
        )

    if parent_task_id and failure_action is CompletionAction.FAIL:
        await resume_if_no_remaining_child_tasks(
            redis_client=redis_client,
            namespace=namespace,
            agents_by_name=agents_by_name,
            task_id=parent_task_id,
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
            AgentEvent(
                event_type="run_failed",
                task_id=task.id,
                owner_id=task.metadata.owner_id,
                agent_name=agent.name,
                error=(
                    f"Agent {agent.name} failed to complete "
                    f"task {task.id} (max retries: {max_retries})"
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

