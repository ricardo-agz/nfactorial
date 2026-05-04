from __future__ import annotations

import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, Literal, cast

from factorial._internal.serialization import serialize_data
from factorial.agent import BaseAgent
from factorial.agent.context import ContextType
from factorial.core.events import AgentEvent, EventPublisher
from factorial.core.logging import colored
from factorial.execution.context import ExecutionContext
from factorial.execution.waits import WaitInstruction, next_cron_wake_timestamp
from factorial.queue.task import Task

from .common import CompletionAction, logger

TimeoutKind = Literal["sleep", "cron"]
ScheduledWaitKind = Literal["sleep", "cron"]

CompleteCallback = Callable[..., Awaitable[None]]
ParkChildCallback = Callable[..., Awaitable[None]]
ParkActivityCallback = Callable[..., Awaitable[None]]
ParkSignalCallback = Callable[..., Awaitable[bool]]
ParkScheduledCallback = Callable[..., Awaitable[None]]


@dataclass(frozen=True)
class ParkPendingTools:
    pending_tool_call_ids: tuple[str, ...]
    event_data: dict[str, Any] | None = None


@dataclass(frozen=True)
class ParkChildren:
    child_task_ids: tuple[str, ...]
    event_data: dict[str, Any] | None = None


@dataclass(frozen=True)
class ParkScheduled:
    wait_kind: ScheduledWaitKind
    wake_timestamp: float
    source_tool_call_ids: tuple[str, ...]
    data: Any = None
    cron_expression: str | None = None
    cron_timezone: str | None = None


@dataclass(frozen=True)
class ParkActivity:
    source_tool_call_ids: tuple[str, ...]
    data: Any = None
    timeout_wake_timestamp: float | None = None
    timeout_kind: TimeoutKind | None = None
    timeout_cron_expression: str | None = None
    timeout_cron_timezone: str | None = None


@dataclass(frozen=True)
class ParkSignal:
    signal_id: str
    source_tool_call_ids: tuple[str, ...]
    data: Any = None
    timeout_wake_timestamp: float | None = None
    timeout_kind: TimeoutKind | None = None
    timeout_cron_expression: str | None = None
    timeout_cron_timezone: str | None = None


ParkCommand = (
    ParkPendingTools
    | ParkChildren
    | ParkScheduled
    | ParkActivity
    | ParkSignal
)


def compile_wait_command(
    *,
    task_id: str,
    turn_completion: Any,
    wait_instructions: list[tuple[str, WaitInstruction]],
) -> ParkCommand:
    """Normalize tool-produced wait instructions into one runtime parking command."""
    if turn_completion.pending_tool_call_ids or turn_completion.pending_child_task_ids:
        raise RuntimeError(
            "Turn cannot combine wait instructions with pending tool "
            "or child-task continuations."
        )

    source_tool_call_ids = tuple(tool_call_id for tool_call_id, _ in wait_instructions)
    wait_kinds = {wait_instruction.kind for _, wait_instruction in wait_instructions}
    if len(wait_kinds) != 1:
        raise RuntimeError(
            "All wait instructions in a single turn must have the same kind."
        )

    wait_kind = next(iter(wait_kinds))
    if wait_kind == "jobs":
        return _compile_child_wait(
            task_id=task_id,
            wait_instructions=wait_instructions,
            event_data=serialize_data(turn_completion),
        )
    if wait_kind == "activity":
        return _compile_activity_wait(
            source_tool_call_ids=source_tool_call_ids,
            wait_instructions=wait_instructions,
        )
    if wait_kind == "signal":
        return _compile_signal_wait(
            source_tool_call_ids=source_tool_call_ids,
            wait_instructions=wait_instructions,
        )
    if wait_kind in {"sleep", "cron"}:
        return _compile_scheduled_wait(
            wait_kind=wait_kind,
            source_tool_call_ids=source_tool_call_ids,
            wait_instructions=wait_instructions,
        )

    raise RuntimeError(f"Unsupported wait kind '{wait_kind}'.")


def compile_pending_command(turn_completion: Any) -> ParkCommand | None:
    if turn_completion.pending_tool_call_ids and turn_completion.pending_child_task_ids:
        raise RuntimeError(
            "Turn cannot simultaneously park on pending tool results and "
            "pending child task results."
        )

    if turn_completion.pending_tool_call_ids:
        return ParkPendingTools(
            pending_tool_call_ids=tuple(turn_completion.pending_tool_call_ids),
            event_data=serialize_data(turn_completion),
        )
    if turn_completion.pending_child_task_ids:
        return ParkChildren(
            child_task_ids=tuple(turn_completion.pending_child_task_ids),
            event_data=serialize_data(turn_completion),
        )
    return None


async def park_command(
    *,
    command: ParkCommand,
    task: Task[ContextType],
    agent: BaseAgent[ContextType],
    execution_ctx: ExecutionContext,
    event_publisher: EventPublisher,
    complete: CompleteCallback,
    park_or_resume_child_wait: ParkChildCallback,
    park_activity_wait: ParkActivityCallback,
    park_signal_wait: ParkSignalCallback,
    park_scheduled_wait: ParkScheduledCallback,
) -> None:
    await execution_ctx.resources.checkpoint_all()

    if isinstance(command, ParkPendingTools):
        await complete(
            action=CompletionAction.PENDING_TOOL,
            pending_tool_call_ids=list(command.pending_tool_call_ids),
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
                turn=task.payload.turn_number,
                data=command.event_data,
            )
        )
        return

    if isinstance(command, ParkChildren):
        logger.info(
            f"⏳ Task awaiting child task results {colored(f'[{task.id}]', 'dim')}"
        )
        await park_or_resume_child_wait(
            child_task_ids=list(command.child_task_ids),
            event_data=command.event_data,
        )
        return

    if isinstance(command, ParkActivity):
        await park_activity_wait(
            source_tool_call_ids=list(command.source_tool_call_ids),
            data=command.data,
            timeout_wake_timestamp=command.timeout_wake_timestamp,
            timeout_kind=command.timeout_kind,
            timeout_cron_expression=command.timeout_cron_expression,
            timeout_cron_timezone=command.timeout_cron_timezone,
        )
        event_data: dict[str, Any] = {
            "wait_kind": "activity",
            "source_tool_call_ids": list(command.source_tool_call_ids),
        }
        if command.timeout_wake_timestamp is not None:
            event_data["timeout_kind"] = command.timeout_kind
            event_data["wake_timestamp"] = command.timeout_wake_timestamp
            if command.timeout_cron_expression is not None:
                event_data["timeout_cron"] = command.timeout_cron_expression
            if command.timeout_cron_timezone is not None:
                event_data["timeout_timezone"] = command.timeout_cron_timezone
        await event_publisher.publish_event(
            AgentEvent(
                event_type="task_activity_waiting",
                task_id=task.id,
                owner_id=task.metadata.owner_id,
                agent_name=agent.name,
                turn=task.payload.turn_number,
                data=event_data,
            )
        )
        return

    if isinstance(command, ParkSignal):
        woken_immediately = await park_signal_wait(
            signal_id=command.signal_id,
            source_tool_call_ids=list(command.source_tool_call_ids),
            data=command.data,
            timeout_wake_timestamp=command.timeout_wake_timestamp,
            timeout_kind=command.timeout_kind,
            timeout_cron_expression=command.timeout_cron_expression,
            timeout_cron_timezone=command.timeout_cron_timezone,
        )
        event_data: dict[str, Any] = {
            "wait_kind": "signal",
            "signal_id": command.signal_id,
            "source_tool_call_ids": list(command.source_tool_call_ids),
        }
        if command.timeout_wake_timestamp is not None:
            event_data["timeout_kind"] = command.timeout_kind
            event_data["wake_timestamp"] = command.timeout_wake_timestamp
            if command.timeout_cron_expression is not None:
                event_data["timeout_cron"] = command.timeout_cron_expression
            if command.timeout_cron_timezone is not None:
                event_data["timeout_timezone"] = command.timeout_cron_timezone
        await event_publisher.publish_event(
            AgentEvent(
                event_type=(
                    "task_signal_wait_satisfied"
                    if woken_immediately
                    else "task_signal_waiting"
                ),
                task_id=task.id,
                owner_id=task.metadata.owner_id,
                agent_name=agent.name,
                turn=task.payload.turn_number,
                data=event_data,
            )
        )
        return

    await park_scheduled_wait(
        wait_kind=command.wait_kind,
        wake_timestamp=command.wake_timestamp,
        source_tool_call_ids=list(command.source_tool_call_ids),
        data=command.data,
        cron_expression=command.cron_expression,
        cron_timezone=command.cron_timezone,
    )
    await event_publisher.publish_event(
        AgentEvent(
            event_type="task_paused",
            task_id=task.id,
            owner_id=task.metadata.owner_id,
            agent_name=agent.name,
            turn=task.payload.turn_number,
            data={
                "wait_kind": command.wait_kind,
                "wake_timestamp": command.wake_timestamp,
                "source_tool_call_ids": list(command.source_tool_call_ids),
            },
        )
    )


def _compile_child_wait(
    *,
    task_id: str,
    wait_instructions: list[tuple[str, WaitInstruction]],
    event_data: dict[str, Any] | None,
) -> ParkChildren:
    child_task_ids: list[str] = []
    for _, wait_instruction in wait_instructions:
        for job_ref in wait_instruction.job_refs or []:
            ref_parent_task_id = job_ref.get("parent_task_id")
            if ref_parent_task_id is not None and ref_parent_task_id != task_id:
                raise ValueError(
                    "wait.jobs received job refs from a different "
                    f"parent task. Expected '{task_id}', got "
                    f"'{ref_parent_task_id}'."
                )
            ref_task_id = job_ref.get("task_id")
            if not isinstance(ref_task_id, str) or not ref_task_id:
                raise ValueError("wait.jobs received a job ref with invalid task_id.")
            child_task_ids.append(ref_task_id)

        if wait_instruction.child_task_ids:
            child_task_ids.extend(wait_instruction.child_task_ids)

    if not child_task_ids:
        raise ValueError("wait.jobs requires at least one job/task reference.")

    return ParkChildren(
        child_task_ids=tuple(child_task_ids),
        event_data=event_data,
    )


def _compile_activity_wait(
    *,
    source_tool_call_ids: tuple[str, ...],
    wait_instructions: list[tuple[str, WaitInstruction]],
) -> ParkActivity:
    wait_data: Any = None
    timeout = _select_timeout(
        wait_instructions=wait_instructions,
        data_prefix="activity",
    )
    for _, wait_instruction in wait_instructions:
        if wait_data is None and wait_instruction.data is not None:
            wait_data = wait_instruction.data

    return ParkActivity(
        source_tool_call_ids=source_tool_call_ids,
        data=wait_data,
        timeout_wake_timestamp=timeout[0],
        timeout_kind=timeout[1],
        timeout_cron_expression=timeout[2],
        timeout_cron_timezone=timeout[3],
    )


def _compile_signal_wait(
    *,
    source_tool_call_ids: tuple[str, ...],
    wait_instructions: list[tuple[str, WaitInstruction]],
) -> ParkSignal:
    signal_ids: set[str] = set()
    wait_data: Any = None
    timeout = _select_timeout(
        wait_instructions=wait_instructions,
        data_prefix="signal",
    )
    for _, wait_instruction in wait_instructions:
        signal_id = wait_instruction.signal_id
        if not isinstance(signal_id, str) or not signal_id:
            raise ValueError("wait.until_signal requires a non-empty signal_id.")
        signal_ids.add(signal_id)
        if wait_data is None and wait_instruction.data is not None:
            wait_data = wait_instruction.data

    if len(signal_ids) != 1:
        raise RuntimeError(
            "All wait.until_signal instructions in a single turn must "
            "use the same signal_id."
        )

    return ParkSignal(
        signal_id=next(iter(signal_ids)),
        source_tool_call_ids=source_tool_call_ids,
        data=wait_data,
        timeout_wake_timestamp=timeout[0],
        timeout_kind=timeout[1],
        timeout_cron_expression=timeout[2],
        timeout_cron_timezone=timeout[3],
    )


def _compile_scheduled_wait(
    *,
    wait_kind: str,
    source_tool_call_ids: tuple[str, ...],
    wait_instructions: list[tuple[str, WaitInstruction]],
) -> ParkScheduled:
    wake_timestamps: list[float] = []
    wait_data: Any = None
    for _, wait_instruction in wait_instructions:
        if wait_data is None and wait_instruction.data is not None:
            wait_data = wait_instruction.data

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

    return ParkScheduled(
        wait_kind=cast(ScheduledWaitKind, wait_kind),
        wake_timestamp=min(wake_timestamps),
        source_tool_call_ids=source_tool_call_ids,
        data=wait_data,
        cron_expression=(representative.cron if wait_kind == "cron" else None),
        cron_timezone=(representative.timezone if wait_kind == "cron" else None),
    )


def _select_timeout(
    *,
    wait_instructions: list[tuple[str, WaitInstruction]],
    data_prefix: Literal["activity", "signal"],
) -> tuple[float | None, TimeoutKind | None, str | None, str | None]:
    candidates: list[tuple[float, TimeoutKind, str | None, str | None]] = []
    for _, wait_instruction in wait_instructions:
        timeout_kind: TimeoutKind | None = getattr(
            wait_instruction,
            f"{data_prefix}_timeout_kind",
        )
        if timeout_kind is None:
            continue

        if timeout_kind == "sleep":
            timeout_s: float | None = getattr(
                wait_instruction,
                f"{data_prefix}_timeout_s",
            )
            if timeout_s is None:
                raise ValueError(
                    f"wait.{_timeout_api_name(data_prefix)}"
                    "(timeout=wait.sleep(...)) requires a sleep duration."
                )
            if timeout_s < 0:
                raise ValueError(
                    f"wait.{_timeout_api_name(data_prefix)}"
                    "(timeout=wait.sleep(...)) requires a non-negative duration."
                )
            candidates.append((time.time() + timeout_s, "sleep", None, None))
            continue

        cron_expression: str | None = getattr(
            wait_instruction,
            f"{data_prefix}_timeout_cron",
        )
        if not cron_expression:
            raise ValueError(
                f"wait.{_timeout_api_name(data_prefix)}"
                "(timeout=wait.cron(...)) requires a non-empty cron expression."
            )
        cron_timezone = (
            getattr(wait_instruction, f"{data_prefix}_timeout_timezone")
            or "UTC"
        )
        candidates.append(
            (
                next_cron_wake_timestamp(cron_expression, cron_timezone),
                "cron",
                cron_expression,
                cron_timezone,
            )
        )

    if not candidates:
        return None, None, None, None
    return min(candidates, key=lambda item: item[0])


def _timeout_api_name(data_prefix: Literal["activity", "signal"]) -> str:
    if data_prefix == "activity":
        return "activity"
    return "until_signal"

