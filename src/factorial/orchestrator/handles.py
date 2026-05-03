from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator, AsyncIterator
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from factorial.core.events import BaseEvent, FinishEvent, parse_event
from factorial.core.run_types import RunResult, TurnSummary

if TYPE_CHECKING:
    from factorial.orchestrator.core import Orchestrator


OutputT = TypeVar("OutputT")
StateT = TypeVar("StateT")
MetadataT = TypeVar("MetadataT")
HookPayloadT = TypeVar("HookPayloadT")


class TaskSnapshotStatus(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    WAITING = "waiting"
    BACKOFF = "backoff"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class WaitKind(str, Enum):
    SLEEP = "sleep"
    CRON = "cron"
    SIGNAL = "signal"
    ACTIVITY = "activity"


class HookMode(str, Enum):
    REQUIRES = "requires"
    AWAITS = "awaits"


class HookCompletionStatus(str, Enum):
    RESOLVED = "resolved"
    IDEMPOTENT = "idempotent"


@dataclass(frozen=True)
class WaitSnapshot:
    kind: WaitKind
    wake_at: datetime | None = None
    signal_id: str | None = None
    source_tool_call_ids: tuple[str, ...] = ()
    data: Any = None


@dataclass(frozen=True)
class PendingHookSnapshot:
    id: str
    hook_type: str
    mode: HookMode
    title: str | None
    tool_name: str | None
    param_name: str | None
    expires_at: datetime
    metadata: dict[str, Any]


@dataclass(frozen=True)
class HookCompletionResult:
    status: HookCompletionStatus
    task_resumed: bool


@dataclass(frozen=True)
class TaskSnapshot(Generic[StateT, MetadataT]):
    id: str
    agent_name: str
    owner_id: str
    batch_id: str | None
    status: TaskSnapshotStatus
    state: StateT
    metadata: MetadataT
    output: object | None
    retry_count: int
    turn_number: int
    last_turn: TurnSummary | None = None
    wait: WaitSnapshot | None = None
    pending_hooks: tuple[PendingHookSnapshot, ...] = ()
    pending_child_task_ids: tuple[str, ...] = ()
    backoff_until: datetime | None = None


@dataclass(frozen=True)
class BatchSnapshot:
    id: str
    owner_id: str
    total_tasks: int
    remaining_tasks: int
    progress: float
    is_finished: bool


@dataclass(frozen=True)
class InputWithContext(Generic[StateT, MetadataT]):
    input: str | list[dict[str, Any]]
    state: StateT | None = None
    metadata: MetadataT | None = None


def with_context(
    input: str | list[dict[str, Any]],
    *,
    state: StateT | None = None,
    metadata: MetadataT | None = None,
) -> InputWithContext[StateT, MetadataT]:
    return InputWithContext(input=input, state=state, metadata=metadata)


class PendingHookHandle(Generic[HookPayloadT]):
    def __init__(
        self,
        *,
        orchestrator: Orchestrator,
        task_id: str,
        snapshot: PendingHookSnapshot,
    ) -> None:
        self._orchestrator = orchestrator
        self._task_id = task_id
        self._snapshot = snapshot

    @property
    def snapshot(self) -> PendingHookSnapshot:
        return self._snapshot

    async def complete(
        self,
        payload: HookPayloadT | dict[str, Any],
    ) -> HookCompletionResult:
        token = await self._orchestrator.rotate_hook_token(
            hook_id=self._snapshot.id,
            revoke_previous=False,
        )
        resolution = await self._orchestrator.resolve_hook(
            hook_id=self._snapshot.id,
            payload=payload,
            token=token,
        )
        return HookCompletionResult(
            status=HookCompletionStatus(resolution.status),
            task_resumed=resolution.task_resumed,
        )


class TaskHandle(Generic[OutputT, StateT, MetadataT]):
    def __init__(
        self,
        *,
        orchestrator: Orchestrator,
        task_id: str,
        agent_name: str,
        owner_id: str,
        batch_id: str | None = None,
    ) -> None:
        self._orchestrator = orchestrator
        self.id = task_id
        self.agent_name = agent_name
        self.owner_id = owner_id
        self.batch_id = batch_id

    async def snapshot(self) -> TaskSnapshot[StateT, MetadataT]:
        return await self._orchestrator.snapshot_task(self.id)

    async def hooks(self) -> tuple[PendingHookHandle[Any], ...]:
        snapshot = await self.snapshot()
        return tuple(
            PendingHookHandle(
                orchestrator=self._orchestrator,
                task_id=self.id,
                snapshot=hook_snapshot,
            )
            for hook_snapshot in snapshot.pending_hooks
        )

    async def hook(self, hook_id: str) -> PendingHookHandle[Any]:
        for hook_handle in await self.hooks():
            if hook_handle.snapshot.id == hook_id:
                return hook_handle
        raise ValueError(f"Hook '{hook_id}' is not pending for task '{self.id}'")

    async def wait(
        self,
        *,
        timeout: float | None = None,
    ) -> RunResult[OutputT, StateT, MetadataT]:
        snapshot = await self.snapshot()
        if snapshot.status in {
            TaskSnapshotStatus.COMPLETED,
            TaskSnapshotStatus.FAILED,
            TaskSnapshotStatus.CANCELLED,
        }:
            return await self._orchestrator.task_result(self.id)

        updates = self.updates(types=(FinishEvent,))
        try:
            if timeout is None:
                async for _event in updates:
                    return await self._orchestrator.task_result(self.id)
            else:
                await asyncio.wait_for(anext(updates), timeout=timeout)
                return await self._orchestrator.task_result(self.id)
        finally:
            await updates.aclose()

        raise RuntimeError("task.wait() exited unexpectedly")

    async def updates(
        self,
        *,
        types: tuple[type[BaseEvent], ...] | None = None,
    ) -> AsyncGenerator[BaseEvent, None]:
        async for update in self._orchestrator.subscribe_to_updates(
            owner_id=self.owner_id,
            task_ids=[self.id],
        ):
            event = parse_event(update)
            if types is None or isinstance(event, types):
                yield event

    def __aiter__(self) -> AsyncIterator[BaseEvent]:
        return self.updates()

    async def cancel(self) -> None:
        await self._orchestrator.cancel_task(task_id=self.id)

    async def steer(self, input: str | list[dict[str, Any]]) -> None:
        await self._orchestrator.steer_task_input(task_id=self.id, input=input)

    async def wake(
        self,
        input: str | list[dict[str, Any]] | None = None,
    ) -> bool:
        return await self._orchestrator.wake_task(task_id=self.id, input=input)

    async def branch(
        self,
        input: str | list[dict[str, Any]],
        *,
        state: StateT | None = None,
        metadata: MetadataT | None = None,
    ) -> TaskHandle[OutputT, StateT, MetadataT]:
        return await self._orchestrator.branch_task(
            task_id=self.id,
            input=input,
            state=state,
            metadata=metadata,
        )


class BatchHandle(Generic[OutputT, StateT, MetadataT]):
    def __init__(
        self,
        *,
        orchestrator: Orchestrator,
        batch_id: str,
        agent_name: str,
        owner_id: str,
        task_ids: tuple[str, ...],
    ) -> None:
        self._orchestrator = orchestrator
        self.id = batch_id
        self.agent_name = agent_name
        self.owner_id = owner_id
        self.task_ids = task_ids

    @property
    def tasks(self) -> tuple[TaskHandle[OutputT, StateT, MetadataT], ...]:
        return tuple(
            TaskHandle(
                orchestrator=self._orchestrator,
                task_id=task_id,
                agent_name=self.agent_name,
                owner_id=self.owner_id,
                batch_id=self.id,
            )
            for task_id in self.task_ids
        )

    async def snapshot(self) -> BatchSnapshot:
        return await self._orchestrator.snapshot_batch(self.id)

    async def wait(
        self,
        *,
        timeout: float | None = None,
    ) -> tuple[RunResult[OutputT, StateT, MetadataT], ...]:
        results = await asyncio.gather(
            *[task.wait(timeout=timeout) for task in self.tasks]
        )
        return tuple(results)

    async def updates(
        self,
        *,
        types: tuple[type[BaseEvent], ...] | None = None,
    ) -> AsyncGenerator[BaseEvent, None]:
        async for update in self._orchestrator.subscribe_to_updates(
            owner_id=self.owner_id,
            task_ids=list(self.task_ids),
        ):
            event = parse_event(update)
            if types is None or isinstance(event, types):
                yield event

    def __aiter__(self) -> AsyncIterator[BaseEvent]:
        return self.updates()

    async def cancel(self) -> None:
        await self._orchestrator.cancel_batch(self.id)


__all__ = [
    "BatchHandle",
    "BatchSnapshot",
    "HookCompletionResult",
    "HookCompletionStatus",
    "HookMode",
    "InputWithContext",
    "PendingHookHandle",
    "PendingHookSnapshot",
    "TaskHandle",
    "TaskSnapshot",
    "TaskSnapshotStatus",
    "WaitKind",
    "WaitSnapshot",
    "with_context",
]
