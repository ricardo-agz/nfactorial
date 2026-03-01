from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal, cast

if TYPE_CHECKING:
    from factorial.orchestrator import Orchestrator


@dataclass(frozen=True)
class GroupConversationSummary:
    team_id: str
    group_id: str
    group_name: str
    thread_id: str
    last_message_id: str | None
    last_message_at: float | None
    last_message_preview: str | None


@dataclass(frozen=True)
class GroupConversationListPage:
    conversations: list[GroupConversationSummary]
    next_cursor: str | None
    has_more: bool


@dataclass(frozen=True)
class GroupMessageRecord:
    message_id: str
    thread_id: str
    team_id: str
    group_id: str
    group_name: str
    from_task_id: str | None
    from_owner_id: str | None
    to_task_ids: list[str]
    delivered_task_ids: list[str]
    skipped_inactive_task_ids: list[str]
    failed_task_ids: list[str]
    content: str
    metadata: dict[str, Any]
    created_at: float | None


@dataclass(frozen=True)
class GroupMessageHistoryPage:
    thread_id: str
    team_id: str
    group_id: str
    group_name: str
    messages: list[GroupMessageRecord]
    next_before: str | None
    next_after: str | None
    has_more: bool


@dataclass(frozen=True)
class DirectConversationSummary:
    team_id: str
    task_a_id: str
    task_b_id: str
    thread_id: str
    last_message_id: str | None
    last_message_at: float | None
    last_message_preview: str | None


@dataclass(frozen=True)
class DirectConversationListPage:
    conversations: list[DirectConversationSummary]
    next_cursor: str | None
    has_more: bool


@dataclass(frozen=True)
class DirectMessageRecord:
    message_id: str
    thread_id: str
    team_id: str
    task_a_id: str
    task_b_id: str
    from_task_id: str | None
    to_task_ids: list[str]
    delivered_task_ids: list[str]
    skipped_inactive_task_ids: list[str]
    failed_task_ids: list[str]
    content: str
    metadata: dict[str, Any]
    created_at: float | None


@dataclass(frozen=True)
class DirectMessageHistoryPage:
    thread_id: str
    team_id: str
    task_a_id: str
    task_b_id: str
    messages: list[DirectMessageRecord]
    next_before: str | None
    next_after: str | None
    has_more: bool


def _group_message_from_dict(data: dict[str, Any]) -> GroupMessageRecord:
    return GroupMessageRecord(
        message_id=str(data["message_id"]),
        thread_id=str(data["thread_id"]),
        team_id=str(data["team_id"]),
        group_id=str(data["group_id"]),
        group_name=str(data["group_name"]),
        from_task_id=cast(str | None, data.get("from_task_id")),
        from_owner_id=cast(str | None, data.get("from_owner_id")),
        to_task_ids=list(cast(list[str], data.get("to_task_ids", []))),
        delivered_task_ids=list(cast(list[str], data.get("delivered_task_ids", []))),
        skipped_inactive_task_ids=list(
            cast(list[str], data.get("skipped_inactive_task_ids", []))
        ),
        failed_task_ids=list(cast(list[str], data.get("failed_task_ids", []))),
        content=str(data.get("content", "")),
        metadata=dict(cast(dict[str, Any], data.get("metadata", {}))),
        created_at=cast(float | None, data.get("created_at")),
    )


def _direct_message_from_dict(data: dict[str, Any]) -> DirectMessageRecord:
    return DirectMessageRecord(
        message_id=str(data["message_id"]),
        thread_id=str(data["thread_id"]),
        team_id=str(data["team_id"]),
        task_a_id=str(data["task_a_id"]),
        task_b_id=str(data["task_b_id"]),
        from_task_id=cast(str | None, data.get("from_task_id")),
        to_task_ids=list(cast(list[str], data.get("to_task_ids", []))),
        delivered_task_ids=list(cast(list[str], data.get("delivered_task_ids", []))),
        skipped_inactive_task_ids=list(
            cast(list[str], data.get("skipped_inactive_task_ids", []))
        ),
        failed_task_ids=list(cast(list[str], data.get("failed_task_ids", []))),
        content=str(data.get("content", "")),
        metadata=dict(cast(dict[str, Any], data.get("metadata", {}))),
        created_at=cast(float | None, data.get("created_at")),
    )


class OrchestratorMessagingGroupsNamespace:
    def __init__(self, orchestrator: Orchestrator):
        self._orchestrator = orchestrator

    async def history(
        self,
        *,
        group_id: str | None = None,
        team_id: str | None = None,
        group_name: str | None = None,
        limit: int = 50,
        before: str | None = None,
        after: str | None = None,
        order: Literal["asc", "desc"] = "desc",
    ) -> GroupMessageHistoryPage:
        from factorial.queue import (
            messaging_groups_history as q_messaging_groups_history,
        )

        async with self._orchestrator.redis_client_context() as redis_client:
            payload = await q_messaging_groups_history(
                redis_client=redis_client,
                namespace=self._orchestrator.namespace,
                group_id=group_id,
                team_id=team_id,
                group_name=group_name,
                limit=limit,
                before=before,
                after=after,
                order=order,
            )
        messages = [
            _group_message_from_dict(entry)
            for entry in cast(list[dict[str, Any]], payload["messages"])
        ]
        return GroupMessageHistoryPage(
            thread_id=str(payload["thread_id"]),
            team_id=str(payload["team_id"]),
            group_id=str(payload["group_id"]),
            group_name=str(payload["group_name"]),
            messages=messages,
            next_before=cast(str | None, payload.get("next_before")),
            next_after=cast(str | None, payload.get("next_after")),
            has_more=bool(payload.get("has_more")),
        )

    async def list(
        self,
        *,
        team_id: str,
        limit: int = 50,
        cursor: str | None = None,
    ) -> GroupConversationListPage:
        from factorial.queue import (
            messaging_groups_list_threads as q_messaging_groups_list_threads,
        )

        async with self._orchestrator.redis_client_context() as redis_client:
            payload = await q_messaging_groups_list_threads(
                redis_client=redis_client,
                namespace=self._orchestrator.namespace,
                team_id=team_id,
                limit=limit,
                cursor=cursor,
            )
        conversations = [
            GroupConversationSummary(
                team_id=str(entry["team_id"]),
                group_id=str(entry["group_id"]),
                group_name=str(entry["group_name"]),
                thread_id=str(entry["thread_id"]),
                last_message_id=cast(str | None, entry.get("last_message_id")),
                last_message_at=cast(float | None, entry.get("last_message_at")),
                last_message_preview=cast(
                    str | None, entry.get("last_message_preview")
                ),
            )
            for entry in cast(list[dict[str, Any]], payload["conversations"])
        ]
        return GroupConversationListPage(
            conversations=conversations,
            next_cursor=cast(str | None, payload.get("next_cursor")),
            has_more=bool(payload.get("has_more")),
        )


class OrchestratorMessagingDirectNamespace:
    def __init__(self, orchestrator: Orchestrator):
        self._orchestrator = orchestrator

    async def history(
        self,
        *,
        task_a_id: str,
        task_b_id: str,
        limit: int = 50,
        before: str | None = None,
        after: str | None = None,
        order: Literal["asc", "desc"] = "desc",
    ) -> DirectMessageHistoryPage:
        from factorial.queue import (
            messaging_direct_history as q_messaging_direct_history,
        )

        async with self._orchestrator.redis_client_context() as redis_client:
            payload = await q_messaging_direct_history(
                redis_client=redis_client,
                namespace=self._orchestrator.namespace,
                task_a_id=task_a_id,
                task_b_id=task_b_id,
                limit=limit,
                before=before,
                after=after,
                order=order,
            )
        messages = [
            _direct_message_from_dict(entry)
            for entry in cast(list[dict[str, Any]], payload["messages"])
        ]
        return DirectMessageHistoryPage(
            thread_id=str(payload["thread_id"]),
            team_id=str(payload["team_id"]),
            task_a_id=str(payload["task_a_id"]),
            task_b_id=str(payload["task_b_id"]),
            messages=messages,
            next_before=cast(str | None, payload.get("next_before")),
            next_after=cast(str | None, payload.get("next_after")),
            has_more=bool(payload.get("has_more")),
        )

    async def list(
        self,
        *,
        team_id: str,
        limit: int = 50,
        cursor: str | None = None,
    ) -> DirectConversationListPage:
        from factorial.queue import (
            messaging_direct_list_threads as q_messaging_direct_list_threads,
        )

        async with self._orchestrator.redis_client_context() as redis_client:
            payload = await q_messaging_direct_list_threads(
                redis_client=redis_client,
                namespace=self._orchestrator.namespace,
                team_id=team_id,
                limit=limit,
                cursor=cursor,
            )
        conversations = [
            DirectConversationSummary(
                team_id=str(entry["team_id"]),
                task_a_id=str(entry["task_a_id"]),
                task_b_id=str(entry["task_b_id"]),
                thread_id=str(entry["thread_id"]),
                last_message_id=cast(str | None, entry.get("last_message_id")),
                last_message_at=cast(float | None, entry.get("last_message_at")),
                last_message_preview=cast(
                    str | None, entry.get("last_message_preview")
                ),
            )
            for entry in cast(list[dict[str, Any]], payload["conversations"])
        ]
        return DirectConversationListPage(
            conversations=conversations,
            next_cursor=cast(str | None, payload.get("next_cursor")),
            has_more=bool(payload.get("has_more")),
        )


class OrchestratorMessagingNamespace:
    def __init__(self, orchestrator: Orchestrator):
        self.groups = OrchestratorMessagingGroupsNamespace(orchestrator)
        self.direct = OrchestratorMessagingDirectNamespace(orchestrator)


__all__ = [
    "GroupConversationSummary",
    "GroupConversationListPage",
    "GroupMessageRecord",
    "GroupMessageHistoryPage",
    "DirectConversationSummary",
    "DirectConversationListPage",
    "DirectMessageRecord",
    "DirectMessageHistoryPage",
    "OrchestratorMessagingNamespace",
]
