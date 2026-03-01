from __future__ import annotations

import builtins
from collections.abc import Awaitable, Callable
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, TypeVar

from pydantic import BaseModel, Field

from factorial.events import EventPublisher

if TYPE_CHECKING:
    from factorial.agent import BaseAgent  # pragma: no cover
    from factorial.queue.task import Batch  # pragma: no cover

execution_context: ContextVar[ExecutionContext] = ContextVar("execution_context")

ContextType = TypeVar("ContextType", bound="AgentContext")


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
    [str, str, dict[str, Any] | None],
    Awaitable[dict[str, Any]],
]
MessagingSendDirectCallback = Callable[
    [str, str, dict[str, Any] | None],
    Awaitable[dict[str, Any]],
]


@dataclass
class SubagentsExecutionNamespace:
    """Namespaced child-task operations available at runtime."""

    enqueue_callback: EnqueueChildTaskCallback | None = None
    enqueue_batch_callback: EnqueueBatchCallback | None = None
    cancel_callback: CancelChildTaskCallback | None = None
    cancel_many_callback: CancelChildTasksCallback | None = None

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
        member_task_ids: list[str] | None,
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

    async def list(self) -> list[dict[str, Any]]:
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
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        callback = self.send_callback
        if callback is None:
            raise RuntimeError(
                "messaging.groups.send is not configured for this execution context"
            )
        return await callback(group_name, content, metadata)


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
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        callback = self.send_callback
        if callback is None:
            raise RuntimeError(
                "messaging.send is not configured for this execution context"
            )
        return await callback(to_task_id, content, metadata)


@dataclass
class ExecutionContext:
    """Per-request context (not stored on agent)"""

    task_id: str
    owner_id: str
    retries: int
    iterations: int
    events: EventPublisher
    subagents: SubagentsExecutionNamespace = field(
        default_factory=SubagentsExecutionNamespace
    )
    hooks: HooksExecutionNamespace = field(default_factory=HooksExecutionNamespace)
    messaging: MessagingExecutionNamespace = field(
        default_factory=MessagingExecutionNamespace
    )

    @classmethod
    def current(cls) -> ExecutionContext:
        """Get current execution context"""
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


class VerificationState(BaseModel):
    attempts_used: int = 0
    last_candidate_hash: str | None = None
    last_outcome: str | None = None


class AgentContext(BaseModel):
    """
    Agent state passed to the agent for turn execution.

    Base Fields:
    - query: str
    - messages: list[dict[str, Any]] = []
    - turn: int = 0
    - output: Any = None
    """

    query: str
    messages: list[dict[str, Any]] = []
    turn: int = 0
    output: Any = None
    attempt: int = 0
    verification: VerificationState = Field(default_factory=VerificationState)

    class Config:
        extra = "allow"  # Users can add extra fields
        arbitrary_types_allowed = True  # For Any type flexibility

    def to_dict(self) -> dict[str, Any]:
        return self.model_dump()

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> AgentContext:
        return cls(**data)

    def to_json(self) -> str:
        return self.model_dump_json()

    @classmethod
    def from_json(cls, json_str: str) -> AgentContext:
        return cls.model_validate_json(json_str)
