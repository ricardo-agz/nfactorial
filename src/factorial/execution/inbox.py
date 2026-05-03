from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, TypeVar, cast

from pydantic import BaseModel

from factorial.execution.context import ExecutionContext

T = TypeVar("T")


def _current_execution_context() -> ExecutionContext:
    try:
        return ExecutionContext.current()
    except LookupError as exc:  # pragma: no cover - defensive runtime guard
        raise RuntimeError(
            "inbox can only be used during active task execution"
        ) from exc


def _normalize_limit(limit: int) -> int:
    if not isinstance(limit, int):
        raise TypeError("limit must be an integer")
    if limit < 1:
        raise ValueError("limit must be >= 1")
    return limit


def _normalize_cursor(cursor: str | None) -> str | None:
    if cursor is None:
        return None
    if not isinstance(cursor, str):
        raise TypeError("cursor must be a string when provided")
    normalized = cursor.strip()
    if not normalized:
        raise ValueError("cursor must be a non-empty string when provided")
    return normalized


def _normalize_group_name(group_name: str) -> str:
    if not isinstance(group_name, str) or not group_name.strip():
        raise ValueError("group_name must be a non-empty string")
    return group_name.strip()


def _normalize_message_ids(message_ids: list[str]) -> list[str]:
    if not isinstance(message_ids, list):
        raise TypeError("message_ids must be a list of strings")
    normalized: list[str] = []
    for message_id in message_ids:
        if not isinstance(message_id, str) or not message_id.strip():
            raise ValueError("message_ids must contain non-empty strings")
        normalized.append(message_id.strip())
    return list(dict.fromkeys(normalized))


def _normalize_receipt_ids(receipt_ids: list[str]) -> list[str]:
    if not isinstance(receipt_ids, list):
        raise TypeError("receipt_ids must be a list of strings")
    normalized: list[str] = []
    for receipt_id in receipt_ids:
        if not isinstance(receipt_id, str) or not receipt_id.strip():
            raise ValueError("receipt_ids must contain non-empty strings")
        normalized.append(receipt_id.strip())
    return list(dict.fromkeys(normalized))


@dataclass
class InboxMessage:
    message_id: str
    thread_id: str | None
    team_id: str | None
    group_name: str | None
    from_task_id: str | None
    from_owner_id: str | None
    content: str
    data: Any
    metadata: dict[str, Any]
    created_at: float | None
    is_read: bool
    _mark_read_callback: Callable[[str, bool, Any], Any] = field(
        repr=False,
        compare=False,
    )

    def data_as(self, model: type[T]) -> T:
        if self.data is None:
            raise ValueError("message data is empty")
        if isinstance(model, type) and issubclass(model, BaseModel):
            return cast(T, model.model_validate(self.data))
        return cast(T, self.data)

    async def mark_read(
        self,
        *,
        notify_sender: bool = False,
        data: Any = None,
    ) -> dict[str, Any]:
        result = await self._mark_read_callback(self.message_id, notify_sender, data)
        self.is_read = True
        return cast(dict[str, Any], result)


@dataclass
class InboxMessagePage:
    messages: list[InboxMessage]
    next_cursor: str | None
    has_more: bool
    _mark_read_callback: Callable[[list[str], bool, Any], Any] = field(
        repr=False,
        compare=False,
    )

    async def mark_read(
        self,
        *,
        notify_sender: bool = False,
        data: Any = None,
    ) -> dict[str, Any]:
        message_ids = [message.message_id for message in self.messages]
        if not message_ids:
            return {
                "marked_message_ids": [],
                "already_marked_message_ids": [],
                "ignored_message_ids": [],
                "receipt_ids": [],
            }
        result = await self._mark_read_callback(message_ids, notify_sender, data)
        for message in self.messages:
            message.is_read = True
        return cast(dict[str, Any], result)


@dataclass
class InboxReceipt:
    receipt_id: str
    source_message_id: str | None
    source_kind: str | None
    source_group_name: str | None
    reader_task_id: str | None
    sender_task_id: str | None
    data: Any
    created_at: float | None
    is_read: bool
    _mark_read_callback: Callable[[str], Any] = field(
        repr=False,
        compare=False,
    )

    async def mark_read(self) -> dict[str, Any]:
        result = await self._mark_read_callback(self.receipt_id)
        self.is_read = True
        return cast(dict[str, Any], result)


@dataclass
class InboxReceiptPage:
    messages: list[InboxReceipt]
    next_cursor: str | None
    has_more: bool
    _mark_read_callback: Callable[[list[str]], Any] = field(
        repr=False,
        compare=False,
    )

    async def mark_read(self) -> dict[str, Any]:
        receipt_ids = [receipt.receipt_id for receipt in self.messages]
        if not receipt_ids:
            return {
                "marked_receipt_ids": [],
                "already_marked_receipt_ids": [],
                "ignored_receipt_ids": [],
            }
        result = await self._mark_read_callback(receipt_ids)
        for receipt in self.messages:
            receipt.is_read = True
        return cast(dict[str, Any], result)


class InboxDirectNamespace:
    async def peek(
        self,
        *,
        unread_only: bool = True,
        limit: int = 50,
        cursor: str | None = None,
    ) -> InboxMessagePage:
        ctx = _current_execution_context()
        payload = await ctx.inbox.direct.peek(
            unread_only=unread_only,
            limit=_normalize_limit(limit),
            cursor=_normalize_cursor(cursor),
        )
        records = cast(list[dict[str, Any]], payload.get("messages", []))
        messages = [
            InboxMessage(
                message_id=str(record["message_id"]),
                thread_id=cast(str | None, record.get("thread_id")),
                team_id=cast(str | None, record.get("team_id")),
                group_name=cast(str | None, record.get("group_name")),
                from_task_id=cast(str | None, record.get("from_task_id")),
                from_owner_id=cast(str | None, record.get("from_owner_id")),
                content=str(record.get("content", "")),
                data=record.get("data"),
                metadata=dict(cast(dict[str, Any], record.get("metadata", {}))),
                created_at=cast(float | None, record.get("created_at")),
                is_read=bool(record.get("is_read")),
                _mark_read_callback=self._mark_single,
            )
            for record in records
        ]
        return InboxMessagePage(
            messages=messages,
            next_cursor=cast(str | None, payload.get("next_cursor")),
            has_more=bool(payload.get("has_more")),
            _mark_read_callback=self._mark_many,
        )

    async def mark_read(
        self,
        *,
        message_ids: list[str],
        notify_sender: bool = False,
        data: Any = None,
    ) -> dict[str, Any]:
        ctx = _current_execution_context()
        return await ctx.inbox.direct.mark_read(
            message_ids=_normalize_message_ids(message_ids),
            notify_sender=notify_sender,
            data=data,
        )

    async def _mark_single(
        self,
        message_id: str,
        notify_sender: bool,
        data: Any,
    ) -> dict[str, Any]:
        return await self.mark_read(
            message_ids=[message_id],
            notify_sender=notify_sender,
            data=data,
        )

    async def _mark_many(
        self,
        message_ids: list[str],
        notify_sender: bool,
        data: Any,
    ) -> dict[str, Any]:
        return await self.mark_read(
            message_ids=message_ids,
            notify_sender=notify_sender,
            data=data,
        )


class InboxGroupNamespace:
    async def peek(
        self,
        group_name: str,
        *,
        unread_only: bool = True,
        limit: int = 50,
        cursor: str | None = None,
    ) -> InboxMessagePage:
        ctx = _current_execution_context()
        normalized_group_name = _normalize_group_name(group_name)
        payload = await ctx.inbox.group.peek(
            group_name=normalized_group_name,
            unread_only=unread_only,
            limit=_normalize_limit(limit),
            cursor=_normalize_cursor(cursor),
        )
        records = cast(list[dict[str, Any]], payload.get("messages", []))
        messages = [
            InboxMessage(
                message_id=str(record["message_id"]),
                thread_id=cast(str | None, record.get("thread_id")),
                team_id=cast(str | None, record.get("team_id")),
                group_name=cast(str | None, record.get("group_name")),
                from_task_id=cast(str | None, record.get("from_task_id")),
                from_owner_id=cast(str | None, record.get("from_owner_id")),
                content=str(record.get("content", "")),
                data=record.get("data"),
                metadata=dict(cast(dict[str, Any], record.get("metadata", {}))),
                created_at=cast(float | None, record.get("created_at")),
                is_read=bool(record.get("is_read")),
                _mark_read_callback=lambda message_id, notify_sender, marker_data: (
                    self._mark_single(
                        normalized_group_name,
                        message_id,
                        notify_sender,
                        marker_data,
                    )
                ),
            )
            for record in records
        ]
        return InboxMessagePage(
            messages=messages,
            next_cursor=cast(str | None, payload.get("next_cursor")),
            has_more=bool(payload.get("has_more")),
            _mark_read_callback=lambda message_ids, notify_sender, marker_data: (
                self._mark_many(
                    normalized_group_name,
                    message_ids,
                    notify_sender,
                    marker_data,
                )
            ),
        )

    async def mark_read(
        self,
        group_name: str,
        *,
        message_ids: list[str],
        notify_sender: bool = False,
        data: Any = None,
    ) -> dict[str, Any]:
        ctx = _current_execution_context()
        return await ctx.inbox.group.mark_read(
            group_name=_normalize_group_name(group_name),
            message_ids=_normalize_message_ids(message_ids),
            notify_sender=notify_sender,
            data=data,
        )

    async def _mark_single(
        self,
        group_name: str,
        message_id: str,
        notify_sender: bool,
        data: Any,
    ) -> dict[str, Any]:
        return await self.mark_read(
            group_name,
            message_ids=[message_id],
            notify_sender=notify_sender,
            data=data,
        )

    async def _mark_many(
        self,
        group_name: str,
        message_ids: list[str],
        notify_sender: bool,
        data: Any,
    ) -> dict[str, Any]:
        return await self.mark_read(
            group_name,
            message_ids=message_ids,
            notify_sender=notify_sender,
            data=data,
        )


class InboxReceiptsNamespace:
    async def peek(
        self,
        *,
        unread_only: bool = True,
        limit: int = 50,
        cursor: str | None = None,
    ) -> InboxReceiptPage:
        ctx = _current_execution_context()
        payload = await ctx.inbox.receipts.peek(
            unread_only=unread_only,
            limit=_normalize_limit(limit),
            cursor=_normalize_cursor(cursor),
        )
        records = cast(list[dict[str, Any]], payload.get("messages", []))
        receipts = [
            InboxReceipt(
                receipt_id=str(record["receipt_id"]),
                source_message_id=cast(str | None, record.get("source_message_id")),
                source_kind=cast(str | None, record.get("source_kind")),
                source_group_name=cast(str | None, record.get("source_group_name")),
                reader_task_id=cast(str | None, record.get("reader_task_id")),
                sender_task_id=cast(str | None, record.get("sender_task_id")),
                data=record.get("data"),
                created_at=cast(float | None, record.get("created_at")),
                is_read=bool(record.get("is_read")),
                _mark_read_callback=self._mark_single,
            )
            for record in records
        ]
        return InboxReceiptPage(
            messages=receipts,
            next_cursor=cast(str | None, payload.get("next_cursor")),
            has_more=bool(payload.get("has_more")),
            _mark_read_callback=self._mark_many,
        )

    async def mark_read(
        self,
        *,
        receipt_ids: list[str],
    ) -> dict[str, Any]:
        ctx = _current_execution_context()
        return await ctx.inbox.receipts.mark_read(
            receipt_ids=_normalize_receipt_ids(receipt_ids),
        )

    async def _mark_single(self, receipt_id: str) -> dict[str, Any]:
        return await self.mark_read(receipt_ids=[receipt_id])

    async def _mark_many(self, receipt_ids: list[str]) -> dict[str, Any]:
        return await self.mark_read(receipt_ids=receipt_ids)


class InboxNamespace:
    """Top-level inbox runtime namespace (`from factorial import inbox`)."""

    def __init__(self) -> None:
        self.direct = InboxDirectNamespace()
        self.group = InboxGroupNamespace()
        self.receipts = InboxReceiptsNamespace()


inbox = InboxNamespace()

__all__ = [
    "InboxMessage",
    "InboxMessagePage",
    "InboxReceipt",
    "InboxReceiptPage",
    "InboxDirectNamespace",
    "InboxGroupNamespace",
    "InboxReceiptsNamespace",
    "InboxNamespace",
    "inbox",
]
