from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

import redis.asyncio as redis

from factorial.agent import BaseAgent
from factorial.core.events import EventPublisher, FinishEvent
from factorial.core.run_types import RunError, RunStatus, UsageSummary
from factorial.execution.context import ExecutionContext
from factorial.queue.operations import resume_if_no_remaining_child_tasks
from factorial.queue.task import Task

from .common import CompletionAction, logger
from .parking import (
    ParkCommand,
    compile_pending_command,
    park_command,
)

CompleteCallback = Callable[..., Awaitable[None]]
ParkChildCallback = Callable[..., Awaitable[None]]
ParkActivityCallback = Callable[..., Awaitable[None]]
ParkSignalCallback = Callable[..., Awaitable[bool]]
ParkScheduledCallback = Callable[..., Awaitable[None]]
PublishBatchProgressCallback = Callable[[str], Awaitable[None]]


@dataclass(frozen=True)
class ParkTask:
    command: ParkCommand


@dataclass(frozen=True)
class ContinueTask:
    pass


@dataclass(frozen=True)
class CompleteTask:
    output: Any
    context: Any
    turn_count: int
    usage: UsageSummary


@dataclass(frozen=True)
class RetryTask:
    action: CompletionAction
    output: str | None = None

    def __post_init__(self) -> None:
        if self.action not in {CompletionAction.RETRY, CompletionAction.BACKOFF}:
            raise ValueError(
                "RetryTask action must be CompletionAction.RETRY or "
                "CompletionAction.BACKOFF"
            )


@dataclass(frozen=True)
class FailTask:
    error: Exception
    output: str | None


TaskTransition = ParkTask | ContinueTask | CompleteTask | RetryTask | FailTask


@dataclass(frozen=True)
class TaskTransitionContext:
    redis_client: redis.Redis
    namespace: str
    task: Task[Any]
    parent_task_id: str | None
    agent: BaseAgent[Any]
    agents_by_name: dict[str, BaseAgent[Any]]
    execution_ctx: ExecutionContext
    event_publisher: EventPublisher
    complete: CompleteCallback
    park_or_resume_child_wait: ParkChildCallback
    park_activity_wait: ParkActivityCallback
    park_signal_wait: ParkSignalCallback
    park_scheduled_wait: ParkScheduledCallback
    publish_batch_progress: PublishBatchProgressCallback


def transition_from_turn_completion(turn_completion: Any) -> TaskTransition:
    pending_command = compile_pending_command(turn_completion)
    if pending_command is not None:
        return ParkTask(pending_command)

    if turn_completion.is_done:
        turn_count = (
            turn_completion.turn_summary.turn_number
            if turn_completion.turn_summary is not None
            else turn_completion.context.turn_number
        )
        return CompleteTask(
            output=turn_completion.output,
            context=turn_completion.context,
            turn_count=turn_count,
            usage=turn_completion.usage,
        )

    return ContinueTask()


def transition_from_failure(
    *,
    action: CompletionAction,
    output: str | None,
    error: Exception,
) -> TaskTransition:
    if action is CompletionAction.FAIL:
        return FailTask(error=error, output=output)
    if action in {CompletionAction.RETRY, CompletionAction.BACKOFF}:
        return RetryTask(action=action, output=output)
    raise ValueError(f"Unsupported failure transition action: {action.value}")


async def _checkpoint_resources(execution_ctx: ExecutionContext) -> None:
    if execution_ctx.resources.manager is None:
        logger.warning(
            "Skipping resource checkpoint for task %s because resources were "
            "not fully initialized",
            execution_ctx.task_id,
        )
        return
    await execution_ctx.resources.checkpoint_all()


async def _destroy_resources(execution_ctx: ExecutionContext) -> None:
    if execution_ctx.resources.manager is None:
        logger.warning(
            "Skipping resource destroy for task %s because resources were "
            "not fully initialized",
            execution_ctx.task_id,
        )
        return
    await execution_ctx.resources.destroy_all()


async def execute_transition(
    transition: TaskTransition,
    context: TaskTransitionContext,
) -> None:
    if isinstance(transition, ParkTask):
        await park_command(
            command=transition.command,
            task=context.task,
            agent=context.agent,
            execution_ctx=context.execution_ctx,
            event_publisher=context.event_publisher,
            complete=context.complete,
            park_or_resume_child_wait=context.park_or_resume_child_wait,
            park_activity_wait=context.park_activity_wait,
            park_signal_wait=context.park_signal_wait,
            park_scheduled_wait=context.park_scheduled_wait,
        )
        return

    if isinstance(transition, ContinueTask):
        await _checkpoint_resources(context.execution_ctx)
        await context.complete(
            action=CompletionAction.CONTINUE,
            pending_tool_call_ids=None,
            pending_child_task_ids=None,
            final_output=None,
        )
        if context.task.metadata.batch_id and context.agent.max_turns:
            await context.publish_batch_progress(context.task.metadata.batch_id)
        return

    if isinstance(transition, CompleteTask):
        await _destroy_resources(context.execution_ctx)
        await context.complete(
            action=CompletionAction.COMPLETE,
            pending_tool_call_ids=None,
            pending_child_task_ids=None,
            final_output=transition.output,
        )

        if context.parent_task_id:
            await resume_if_no_remaining_child_tasks(
                redis_client=context.redis_client,
                namespace=context.namespace,
                agents_by_name=context.agents_by_name,
                task_id=context.parent_task_id,
            )

        await context.agent._emit_event(
            FinishEvent(
                task_id=context.task.id,
                owner_id=context.task.metadata.owner_id,
                agent_name=context.agent.name,
                status=RunStatus.COMPLETED,
                output=transition.output,
                turn_count=transition.turn_count,
                usage=transition.usage,
            ),
            transition.context,
            context.execution_ctx,
        )

        if context.task.metadata.batch_id:
            await context.publish_batch_progress(context.task.metadata.batch_id)
        return

    if isinstance(transition, RetryTask):
        await _checkpoint_resources(context.execution_ctx)
        await context.complete(
            action=transition.action,
            pending_tool_call_ids=None,
            pending_child_task_ids=None,
            final_output=transition.output,
        )
        return

    if isinstance(transition, FailTask):
        await _destroy_resources(context.execution_ctx)
        await context.complete(
            action=CompletionAction.FAIL,
            pending_tool_call_ids=None,
            pending_child_task_ids=None,
            final_output=transition.output,
        )
        await context.agent._emit_event(
            FinishEvent(
                task_id=context.task.id,
                owner_id=context.task.metadata.owner_id,
                agent_name=context.agent.name,
                status=RunStatus.FAILED,
                run_error=RunError.from_exception(transition.error),
                turn_count=(
                    context.execution_ctx.last_turn.turn_number
                    if context.execution_ctx.last_turn is not None
                    else max(context.task.payload.turn_number - 1, 0)
                ),
                usage=context.execution_ctx.usage,
            ),
            context.task.payload,
            context.execution_ctx,
        )

        if context.parent_task_id:
            await resume_if_no_remaining_child_tasks(
                redis_client=context.redis_client,
                namespace=context.namespace,
                agents_by_name=context.agents_by_name,
                task_id=context.parent_task_id,
            )
        return


__all__ = [
    "CompleteTask",
    "ContinueTask",
    "FailTask",
    "ParkTask",
    "RetryTask",
    "TaskTransition",
    "TaskTransitionContext",
    "execute_transition",
    "transition_from_failure",
    "transition_from_turn_completion",
]
