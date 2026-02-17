from __future__ import annotations

from dataclasses import dataclass
from typing import Any, TypeVar, cast

from factorial.context import ExecutionContext

CallbackT = TypeVar("CallbackT")


@dataclass(frozen=True)
class MessageDeliveryReport:
    """Delivery outcome for direct and group messaging operations."""

    thread_message_id: str | None
    global_message_id: str | None
    delivered_task_ids: list[str]
    skipped_inactive_task_ids: list[str]
    failed_task_ids: list[str]


def _current_execution_context() -> ExecutionContext:
    try:
        return ExecutionContext.current()
    except LookupError as exc:  # pragma: no cover - defensive runtime guard
        raise RuntimeError(
            "messaging can only be used during active task execution"
        ) from exc


def _require_callback(name: str, callback: CallbackT | None) -> CallbackT:
    if callback is None:
        raise RuntimeError(f"{name} is not configured for this execution context")
    return callback


def _normalize_group_name(group_name: str) -> str:
    if not isinstance(group_name, str) or not group_name.strip():
        raise ValueError("group_name must be a non-empty string")
    return group_name.strip()


def _normalize_content(content: str) -> str:
    if not isinstance(content, str) or not content.strip():
        raise ValueError("content must be a non-empty string")
    return content.strip()


def _coerce_task_id(task_or_ref: Any) -> str:
    if isinstance(task_or_ref, str) and task_or_ref:
        return task_or_ref

    if isinstance(task_or_ref, dict):
        candidate = task_or_ref.get("task_id")
        if isinstance(candidate, str) and candidate:
            return candidate

    candidate = getattr(task_or_ref, "task_id", None)
    if isinstance(candidate, str) and candidate:
        return candidate

    raise TypeError(
        "Expected a task_id string or JobRef-like object with task_id"
    )


def _coerce_member_task_ids(members: list[Any] | None) -> list[str]:
    if members is None:
        return []
    if not isinstance(members, list):
        raise TypeError("members must be a list")
    task_ids = [_coerce_task_id(member) for member in members]
    return list(dict.fromkeys(task_ids))


def _coerce_metadata(metadata: dict[str, Any] | None) -> dict[str, Any] | None:
    if metadata is None:
        return None
    if not isinstance(metadata, dict):
        raise TypeError("metadata must be a dict when provided")
    return dict(metadata)


def _delivery_from_dict(data: dict[str, Any]) -> MessageDeliveryReport:
    return MessageDeliveryReport(
        thread_message_id=cast(str | None, data.get("thread_message_id")),
        global_message_id=cast(str | None, data.get("global_message_id")),
        delivered_task_ids=list(cast(list[str], data.get("delivered_task_ids", []))),
        skipped_inactive_task_ids=list(
            cast(list[str], data.get("skipped_inactive_task_ids", []))
        ),
        failed_task_ids=list(cast(list[str], data.get("failed_task_ids", []))),
    )


@dataclass(frozen=True)
class MessagingGroupHandle:
    """Handle for a team-scoped messaging group."""

    name: str
    team_id: str

    async def send(
        self,
        content: str,
        *,
        metadata: dict[str, Any] | None = None,
    ) -> MessageDeliveryReport:
        return await messaging.groups.send(
            self.name,
            content,
            metadata=metadata,
        )

    async def add_members(self, members: list[Any]) -> list[str]:
        ctx = _current_execution_context()
        callback = _require_callback(
            "messaging_add_group_members",
            ctx.messaging_add_group_members,
        )
        return await callback(self.name, _coerce_member_task_ids(members))


class MessagingGroupsNamespace:
    """Namespace for team-scoped messaging group operations."""

    async def create(
        self,
        group_name: str,
        *,
        members: list[Any] | None = None,
    ) -> MessagingGroupHandle:
        ctx = _current_execution_context()
        callback = _require_callback(
            "messaging_create_group",
            ctx.messaging_create_group,
        )
        data = await callback(
            _normalize_group_name(group_name),
            _coerce_member_task_ids(members),
        )
        return MessagingGroupHandle(
            name=str(data["group_name"]),
            team_id=str(data["team_id"]),
        )

    async def get(self, group_name: str) -> MessagingGroupHandle:
        ctx = _current_execution_context()
        callback = _require_callback(
            "messaging_get_group",
            ctx.messaging_get_group,
        )
        data = await callback(_normalize_group_name(group_name))
        return MessagingGroupHandle(
            name=str(data["group_name"]),
            team_id=str(data["team_id"]),
        )

    async def list(self) -> list[MessagingGroupHandle]:
        ctx = _current_execution_context()
        callback = _require_callback(
            "messaging_list_groups",
            ctx.messaging_list_groups,
        )
        results = await callback()
        return [
            MessagingGroupHandle(
                name=str(entry["group_name"]),
                team_id=str(entry["team_id"]),
            )
            for entry in results
        ]

    async def find(self, group_name: str) -> list[MessagingGroupHandle]:
        ctx = _current_execution_context()
        callback = _require_callback(
            "messaging_find_groups",
            ctx.messaging_find_groups,
        )
        results = await callback(_normalize_group_name(group_name))
        return [
            MessagingGroupHandle(
                name=str(entry["group_name"]),
                team_id=str(entry["team_id"]),
            )
            for entry in results
        ]

    async def send(
        self,
        group_name: str,
        content: str,
        *,
        metadata: dict[str, Any] | None = None,
    ) -> MessageDeliveryReport:
        ctx = _current_execution_context()
        callback = _require_callback(
            "messaging_send_group",
            ctx.messaging_send_group,
        )
        result = await callback(
            _normalize_group_name(group_name),
            _normalize_content(content),
            _coerce_metadata(metadata),
        )
        return _delivery_from_dict(result)

    async def add_members(self, group_name: str, members: list[Any]) -> list[str]:
        ctx = _current_execution_context()
        callback = _require_callback(
            "messaging_add_group_members",
            ctx.messaging_add_group_members,
        )
        return await callback(
            _normalize_group_name(group_name),
            _coerce_member_task_ids(members),
        )


class MessagingNamespace:
    """Top-level runtime messaging namespace (`from factorial import messaging`)."""

    def __init__(self) -> None:
        self.groups = MessagingGroupsNamespace()

    async def send(
        self,
        to_task_id: Any,
        content: str,
        *,
        metadata: dict[str, Any] | None = None,
    ) -> MessageDeliveryReport:
        ctx = _current_execution_context()
        callback = _require_callback(
            "messaging_send_direct",
            ctx.messaging_send_direct,
        )
        result = await callback(
            _coerce_task_id(to_task_id),
            _normalize_content(content),
            _coerce_metadata(metadata),
        )
        return _delivery_from_dict(result)


messaging = MessagingNamespace()

__all__ = [
    "MessageDeliveryReport",
    "MessagingGroupHandle",
    "MessagingGroupsNamespace",
    "MessagingNamespace",
    "messaging",
]
