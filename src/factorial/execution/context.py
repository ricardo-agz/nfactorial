from __future__ import annotations

import builtins
from collections.abc import Awaitable, Callable
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from factorial.agent.context import ContextType
from factorial.core.events import EventPublisher
from factorial.core.run_types import TurnSummary, UsageSummary

if TYPE_CHECKING:
    from factorial.agent import BaseAgent  # pragma: no cover
    from factorial.queue.task import Batch  # pragma: no cover

execution_context: ContextVar[ExecutionContext] = ContextVar("execution_context")


EnqueueChildTaskCallback = Callable[
    ["BaseAgent[Any]", Any, str | None],
    Awaitable[str],
]
EnqueueBatchCallback = Callable[
    ["BaseAgent[Any]", list[Any], list[str] | None, str | None],
    Awaitable["Batch"],
]
CancelChildTaskCallback = Callable[[str], Awaitable[None]]
CancelChildTasksCallback = Callable[[list[str]], Awaitable[None]]
SignalChildTaskCallback = Callable[[str, str, Any], Awaitable[dict[str, Any]]]
SignalChildTasksCallback = Callable[
    [list[str], str, Any],
    Awaitable[dict[str, Any]],
]
PersistHookRuntimeCallback = Callable[[dict[str, Any]], Awaitable[None]]
MessagingCreateGroupCallback = Callable[
    [str, list[str] | None],
    Awaitable[dict[str, Any]],
]
MessagingGetGroupCallback = Callable[[str], Awaitable[dict[str, Any]]]
MessagingListGroupsCallback = Callable[[], Awaitable[list[dict[str, Any]]]]
MessagingFindGroupsCallback = Callable[[str], Awaitable[list[dict[str, Any]]]]
MessagingAddGroupMembersCallback = Callable[[str, list[str]], Awaitable[list[str]]]
MessagingRemoveGroupMembersCallback = Callable[[str, list[str]], Awaitable[list[str]]]
MessagingLeaveGroupCallback = Callable[[str], Awaitable[bool]]
MessagingSendGroupCallback = Callable[
    [str, str, Any, dict[str, Any] | None],
    Awaitable[dict[str, Any]],
]
MessagingSendDirectCallback = Callable[
    [str, str, Any, dict[str, Any] | None],
    Awaitable[dict[str, Any]],
]
InboxDirectPeekCallback = Callable[
    [bool, int, str | None],
    Awaitable[dict[str, Any]],
]
InboxDirectMarkReadCallback = Callable[
    [list[str], bool, Any],
    Awaitable[dict[str, Any]],
]
InboxGroupPeekCallback = Callable[
    [str, bool, int, str | None],
    Awaitable[dict[str, Any]],
]
InboxGroupMarkReadCallback = Callable[
    [str, list[str], bool, Any],
    Awaitable[dict[str, Any]],
]
InboxReceiptsPeekCallback = Callable[
    [bool, int, str | None],
    Awaitable[dict[str, Any]],
]
InboxReceiptsMarkReadCallback = Callable[[list[str]], Awaitable[dict[str, Any]]]


@dataclass
class SubagentsExecutionNamespace:
    """Namespaced child-task operations available at runtime."""

    enqueue_callback: EnqueueChildTaskCallback | None = None
    enqueue_batch_callback: EnqueueBatchCallback | None = None
    cancel_callback: CancelChildTaskCallback | None = None
    cancel_many_callback: CancelChildTasksCallback | None = None
    signal_callback: SignalChildTaskCallback | None = None
    signal_many_callback: SignalChildTasksCallback | None = None

    @property
    def has_enqueue_batch(self) -> bool:
        return self.enqueue_batch_callback is not None

    async def enqueue(
        self,
        agent: BaseAgent[ContextType],
        payload: ContextType,
        *,
        task_id: str | None = None,
    ) -> str:
        callback = self.enqueue_callback
        if callback is None:
            raise RuntimeError(
                "subagents.enqueue is not configured for this execution context"
            )
        return await callback(agent, payload, task_id)

    async def enqueue_batch(
        self,
        agent: BaseAgent[ContextType],
        payloads: list[ContextType],
        *,
        task_ids: list[str] | None = None,
        batch_id: str | None = None,
    ) -> Batch:
        callback = self.enqueue_batch_callback
        if callback is None:
            raise RuntimeError(
                "subagents.enqueue_batch is not configured for this execution context"
            )
        return await callback(agent, payloads, task_ids, batch_id)

    async def cancel(self, task_id: str) -> None:
        callback = self.cancel_callback
        if callback is not None:
            await callback(task_id)
            return

        cancel_many_callback = self.cancel_many_callback
        if cancel_many_callback is not None:
            await cancel_many_callback([task_id])
            return

        raise RuntimeError(
            "subagents.cancel is not configured for this execution context"
        )

    async def cancel_many(self, task_ids: list[str]) -> None:
        if not task_ids:
            return

        deduped_task_ids = list(dict.fromkeys(task_ids))
        callback = self.cancel_many_callback
        if callback is not None:
            await callback(deduped_task_ids)
            return

        cancel_callback = self.cancel_callback
        if cancel_callback is None:
            raise RuntimeError(
                "subagents.cancel_many is not configured for this execution context"
            )
        for task_id in deduped_task_ids:
            await cancel_callback(task_id)

    async def signal(
        self,
        task_id: str,
        signal_id: str,
        payload: Any = None,
    ) -> dict[str, Any]:
        signal_callback = self.signal_callback
        if signal_callback is not None:
            return await signal_callback(task_id, signal_id, payload)

        signal_many_callback = self.signal_many_callback
        if signal_many_callback is not None:
            return await signal_many_callback([task_id], signal_id, payload)

        raise RuntimeError(
            "subagents.signal is not configured for this execution context"
        )

    async def signal_many(
        self,
        task_ids: list[str],
        signal_id: str,
        payload: Any = None,
    ) -> dict[str, Any]:
        if not task_ids:
            return {
                "signal_id": signal_id,
                "target_task_ids": [],
                "signaled_task_ids": [],
                "woken_task_ids": [],
                "skipped_inactive_task_ids": [],
                "failed_task_ids": [],
            }
        deduped_task_ids = list(dict.fromkeys(task_ids))
        signal_many_callback = self.signal_many_callback
        if signal_many_callback is not None:
            return await signal_many_callback(deduped_task_ids, signal_id, payload)

        signal_callback = self.signal_callback
        if signal_callback is None:
            raise RuntimeError(
                "subagents.signal_many is not configured for this execution context"
            )

        aggregate: dict[str, Any] = {
            "signal_id": signal_id,
            "target_task_ids": list(deduped_task_ids),
            "signaled_task_ids": [],
            "woken_task_ids": [],
            "skipped_inactive_task_ids": [],
            "failed_task_ids": [],
        }
        signaled_set: set[str] = set()
        woken_set: set[str] = set()
        skipped_set: set[str] = set()
        failed_set: set[str] = set()
        for task_id in deduped_task_ids:
            try:
                result = await signal_callback(task_id, signal_id, payload)
            except Exception:
                if task_id not in failed_set:
                    failed_set.add(task_id)
                    aggregate["failed_task_ids"].append(task_id)
                continue
            for value in result.get("signaled_task_ids", []):
                if isinstance(value, str) and value not in signaled_set:
                    signaled_set.add(value)
                    aggregate["signaled_task_ids"].append(value)
            for value in result.get("woken_task_ids", []):
                if isinstance(value, str) and value not in woken_set:
                    woken_set.add(value)
                    aggregate["woken_task_ids"].append(value)
            for value in result.get("skipped_inactive_task_ids", []):
                if isinstance(value, str) and value not in skipped_set:
                    skipped_set.add(value)
                    aggregate["skipped_inactive_task_ids"].append(value)
            for value in result.get("failed_task_ids", []):
                if isinstance(value, str) and value not in failed_set:
                    failed_set.add(value)
                    aggregate["failed_task_ids"].append(value)
        return aggregate


@dataclass
class HooksExecutionNamespace:
    """Namespaced hook-session operations available at runtime."""

    persist_runtime_callback: PersistHookRuntimeCallback | None = None

    async def persist_runtime(self, runtime_payload: dict[str, Any]) -> None:
        callback = self.persist_runtime_callback
        if callback is None:
            raise RuntimeError(
                "hooks.persist_runtime is not configured for this execution context"
            )
        await callback(runtime_payload)


@dataclass
class MessagingGroupsExecutionNamespace:
    """Namespaced group-messaging operations available at runtime."""

    create_callback: MessagingCreateGroupCallback | None = None
    get_callback: MessagingGetGroupCallback | None = None
    list_callback: MessagingListGroupsCallback | None = None
    find_callback: MessagingFindGroupsCallback | None = None
    add_members_callback: MessagingAddGroupMembersCallback | None = None
    remove_members_callback: MessagingRemoveGroupMembersCallback | None = None
    leave_callback: MessagingLeaveGroupCallback | None = None
    send_callback: MessagingSendGroupCallback | None = None

    async def create(
        self,
        group_name: str,
        member_task_ids: builtins.list[str] | None,
    ) -> dict[str, Any]:
        callback = self.create_callback
        if callback is None:
            raise RuntimeError(
                "messaging.groups.create is not configured for this execution context"
            )
        return await callback(group_name, member_task_ids)

    async def get(self, group_name: str) -> dict[str, Any]:
        callback = self.get_callback
        if callback is None:
            raise RuntimeError(
                "messaging.groups.get is not configured for this execution context"
            )
        return await callback(group_name)

    async def list(self) -> builtins.list[dict[str, Any]]:
        callback = self.list_callback
        if callback is None:
            raise RuntimeError(
                "messaging.groups.list is not configured for this execution context"
            )
        return await callback()

    async def find(
        self, group_name: str
    ) -> builtins.list[dict[str, Any]]:
        callback = self.find_callback
        if callback is None:
            raise RuntimeError(
                "messaging.groups.find is not configured for this execution context"
            )
        return await callback(group_name)

    async def add_members(
        self,
        group_name: str,
        member_task_ids: builtins.list[str],
    ) -> builtins.list[str]:
        callback = self.add_members_callback
        if callback is None:
            raise RuntimeError(
                "messaging.groups.add_members is not configured for this "
                "execution context"
            )
        return await callback(group_name, member_task_ids)

    async def remove_members(
        self,
        group_name: str,
        member_task_ids: builtins.list[str],
    ) -> builtins.list[str]:
        callback = self.remove_members_callback
        if callback is None:
            raise RuntimeError(
                "messaging.groups.remove_members is not configured for this "
                "execution context"
            )
        return await callback(group_name, member_task_ids)

    async def leave(self, group_name: str) -> bool:
        callback = self.leave_callback
        if callback is None:
            raise RuntimeError(
                "messaging.groups.leave is not configured for this execution "
                "context"
            )
        return await callback(group_name)

    async def send(
        self,
        group_name: str,
        content: str,
        data: Any = None,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        callback = self.send_callback
        if callback is None:
            raise RuntimeError(
                "messaging.groups.send is not configured for this execution context"
            )
        return await callback(group_name, content, data, metadata)


@dataclass
class MessagingExecutionNamespace:
    """Namespaced direct and group messaging operations available at runtime."""

    send_callback: MessagingSendDirectCallback | None = None
    groups: MessagingGroupsExecutionNamespace = field(
        default_factory=MessagingGroupsExecutionNamespace
    )

    async def send(
        self,
        to_task_id: str,
        content: str,
        data: Any = None,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        callback = self.send_callback
        if callback is None:
            raise RuntimeError(
                "messaging.send is not configured for this execution context"
            )
        return await callback(to_task_id, content, data, metadata)


@dataclass
class InboxDirectExecutionNamespace:
    """Direct-message inbox operations available during runtime."""

    peek_callback: InboxDirectPeekCallback | None = None
    mark_read_callback: InboxDirectMarkReadCallback | None = None

    async def peek(
        self,
        *,
        unread_only: bool,
        limit: int,
        cursor: str | None,
    ) -> dict[str, Any]:
        callback = self.peek_callback
        if callback is None:
            raise RuntimeError(
                "inbox.direct.peek is not configured for this execution context"
            )
        return await callback(unread_only, limit, cursor)

    async def mark_read(
        self,
        *,
        message_ids: list[str],
        notify_sender: bool = False,
        data: Any = None,
    ) -> dict[str, Any]:
        callback = self.mark_read_callback
        if callback is None:
            raise RuntimeError(
                "inbox.direct.mark_read is not configured for this execution context"
            )
        return await callback(message_ids, notify_sender, data)


@dataclass
class InboxGroupExecutionNamespace:
    """Group-message inbox operations available during runtime."""

    peek_callback: InboxGroupPeekCallback | None = None
    mark_read_callback: InboxGroupMarkReadCallback | None = None

    async def peek(
        self,
        *,
        group_name: str,
        unread_only: bool,
        limit: int,
        cursor: str | None,
    ) -> dict[str, Any]:
        callback = self.peek_callback
        if callback is None:
            raise RuntimeError(
                "inbox.group.peek is not configured for this execution context"
            )
        return await callback(group_name, unread_only, limit, cursor)

    async def mark_read(
        self,
        *,
        group_name: str,
        message_ids: list[str],
        notify_sender: bool = False,
        data: Any = None,
    ) -> dict[str, Any]:
        callback = self.mark_read_callback
        if callback is None:
            raise RuntimeError(
                "inbox.group.mark_read is not configured for this execution context"
            )
        return await callback(group_name, message_ids, notify_sender, data)


@dataclass
class InboxReceiptsExecutionNamespace:
    """Read-receipt inbox operations available during runtime."""

    peek_callback: InboxReceiptsPeekCallback | None = None
    mark_read_callback: InboxReceiptsMarkReadCallback | None = None

    async def peek(
        self,
        *,
        unread_only: bool,
        limit: int,
        cursor: str | None,
    ) -> dict[str, Any]:
        callback = self.peek_callback
        if callback is None:
            raise RuntimeError(
                "inbox.receipts.peek is not configured for this execution context"
            )
        return await callback(unread_only, limit, cursor)

    async def mark_read(
        self,
        *,
        receipt_ids: list[str],
    ) -> dict[str, Any]:
        callback = self.mark_read_callback
        if callback is None:
            raise RuntimeError(
                "inbox.receipts.mark_read is not configured for this execution context"
            )
        return await callback(receipt_ids)


@dataclass
class InboxExecutionNamespace:
    """Inbox operations available during active task execution."""

    direct: InboxDirectExecutionNamespace = field(
        default_factory=InboxDirectExecutionNamespace
    )
    group: InboxGroupExecutionNamespace = field(
        default_factory=InboxGroupExecutionNamespace
    )
    receipts: InboxReceiptsExecutionNamespace = field(
        default_factory=InboxReceiptsExecutionNamespace
    )


@dataclass
class SignalsExecutionNamespace:
    """Signal wait wake context available during active task execution."""

    current_signal: dict[str, Any] | None = None
    wake_reason_value: str | None = None

    def current(self) -> dict[str, Any] | None:
        if self.current_signal is None:
            return None
        return dict(self.current_signal)

    def wake_reason(self) -> str | None:
        if self.wake_reason_value is None:
            return None
        return str(self.wake_reason_value)


@dataclass
class ExecutionContext:
    """Runtime-owned context for one active execution."""

    task_id: str
    owner_id: str
    retry_count: int = 0
    wake_reason: str | None = None
    usage: UsageSummary = field(default_factory=UsageSummary.zero)
    last_turn: TurnSummary | None = None
    events: EventPublisher | None = None
    subagents: SubagentsExecutionNamespace = field(
        default_factory=SubagentsExecutionNamespace
    )
    hooks: HooksExecutionNamespace = field(default_factory=HooksExecutionNamespace)
    messaging: MessagingExecutionNamespace = field(
        default_factory=MessagingExecutionNamespace
    )
    inbox: InboxExecutionNamespace = field(default_factory=InboxExecutionNamespace)
    signals: SignalsExecutionNamespace = field(
        default_factory=SignalsExecutionNamespace
    )

    @classmethod
    def current(cls) -> ExecutionContext:
        """Get the runtime context for the current executing task."""
        return execution_context.get()

    async def spawn_child_task(
        self,
        agent: BaseAgent[ContextType],
        payload: ContextType,
        *,
        task_id: str | None = None,
    ) -> str:
        """Enqueue a child task for *agent* with *payload*.

        This is a thin wrapper around ``execution_ctx.subagents.enqueue``.
        It ensures runtime enqueue wiring has been configured and forwards the
        call.  Returns the **task_id** of the created child task.
        """

        return await self.subagents.enqueue(
            agent,
            payload,
            task_id=task_id,
        )

    async def spawn_child_tasks(
        self,
        agent: BaseAgent[ContextType],
        payloads: list[ContextType],
        *,
        task_ids: list[str] | None = None,
        batch_id: str | None = None,
    ) -> Batch:
        """Spawn multiple child tasks in a batch.

        This is a thin wrapper around ``execution_ctx.subagents.enqueue_batch``.
        It ensures runtime enqueue wiring has been configured and forwards the
        call.  Returns the batch object.
        """

        return await self.subagents.enqueue_batch(
            agent,
            payloads,
            task_ids=task_ids,
            batch_id=batch_id,
        )

    async def cancel_child_task(self, task_id: str) -> None:
        """Cancel a direct child task owned by the current execution."""
        await self.subagents.cancel(task_id)

    async def cancel_child_tasks(self, task_ids: list[str]) -> None:
        """Cancel direct child tasks owned by the current execution."""
        await self.subagents.cancel_many(task_ids)

    async def persist_hook_session(self, runtime_payload: dict[str, Any]) -> None:
        """Persist hook-session runtime metadata for staged continuation."""
        await self.hooks.persist_runtime(runtime_payload)


__all__ = [
    "ExecutionContext",
    "HooksExecutionNamespace",
    "InboxDirectExecutionNamespace",
    "InboxExecutionNamespace",
    "InboxGroupExecutionNamespace",
    "InboxReceiptsExecutionNamespace",
    "MessagingExecutionNamespace",
    "MessagingGroupsExecutionNamespace",
    "SignalsExecutionNamespace",
    "SubagentsExecutionNamespace",
    "execution_context",
]
