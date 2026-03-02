"""Contracts for runtime inbox namespace APIs."""

from __future__ import annotations

from typing import Any, cast

import pytest
from pydantic import BaseModel

from factorial.core.events import EventPublisher
from factorial.execution.context import ExecutionContext, execution_context
from factorial.execution.inbox import inbox


class _NoopEvents:
    async def publish_event(self, _event: Any) -> None:
        return None


def _base_ctx() -> ExecutionContext:
    return ExecutionContext(
        task_id="task-reader",
        owner_id="owner-1",
        retries=0,
        iterations=0,
        events=cast(EventPublisher, _NoopEvents()),
    )


class _Vote(BaseModel):
    player_id: str
    choice: str


@pytest.mark.asyncio
async def test_direct_inbox_peek_and_message_mark_read_route_callbacks() -> None:
    captured: dict[str, Any] = {}

    async def _peek(
        unread_only: bool,
        limit: int,
        cursor: str | None,
    ) -> dict[str, Any]:
        captured["peek"] = {
            "unread_only": unread_only,
            "limit": limit,
            "cursor": cursor,
        }
        return {
            "messages": [
                {
                    "message_id": "1-0",
                    "thread_id": "dm:team-1:task-a:task-reader",
                    "team_id": "team-1",
                    "from_task_id": "task-a",
                    "from_owner_id": None,
                    "content": "vote",
                    "data": {"player_id": "task-a", "choice": "task-z"},
                    "metadata": {"round": 2},
                    "created_at": 123.0,
                    "is_read": False,
                }
            ],
            "next_cursor": "1-0",
            "has_more": False,
        }

    async def _mark_read(
        message_ids: list[str],
        notify_sender: bool,
        data: Any,
    ) -> dict[str, Any]:
        captured["mark_read"] = {
            "message_ids": message_ids,
            "notify_sender": notify_sender,
            "data": data,
        }
        return {"marked_message_ids": message_ids, "receipt_ids": ["r-1"]}

    ctx = _base_ctx()
    ctx.inbox.direct.peek_callback = _peek
    ctx.inbox.direct.mark_read_callback = _mark_read
    token = execution_context.set(ctx)
    try:
        page = await inbox.direct.peek(unread_only=True, limit=10)
        assert len(page.messages) == 1
        message = page.messages[0]
        vote = message.data_as(_Vote)
        assert vote.player_id == "task-a"
        assert vote.choice == "task-z"

        result = await message.mark_read(
            notify_sender=True,
            data={"accepted": True},
        )
    finally:
        execution_context.reset(token)

    assert captured["peek"] == {"unread_only": True, "limit": 10, "cursor": None}
    assert captured["mark_read"] == {
        "message_ids": ["1-0"],
        "notify_sender": True,
        "data": {"accepted": True},
    }
    assert result["receipt_ids"] == ["r-1"]


@pytest.mark.asyncio
async def test_group_inbox_page_mark_read_routes_group_callback() -> None:
    captured: dict[str, Any] = {}

    async def _peek(
        group_name: str,
        unread_only: bool,
        limit: int,
        cursor: str | None,
    ) -> dict[str, Any]:
        captured["peek"] = {
            "group_name": group_name,
            "unread_only": unread_only,
            "limit": limit,
            "cursor": cursor,
        }
        return {
            "messages": [
                {
                    "message_id": "2-0",
                    "thread_id": "group:team-1:village",
                    "team_id": "team-1",
                    "group_name": "village",
                    "from_task_id": "task-b",
                    "from_owner_id": None,
                    "content": "accuse",
                    "data": {"target": "task-c"},
                    "metadata": {},
                    "created_at": 456.0,
                    "is_read": False,
                }
            ],
            "next_cursor": "2-0",
            "has_more": False,
        }

    async def _mark_read(
        group_name: str,
        message_ids: list[str],
        notify_sender: bool,
        data: Any,
    ) -> dict[str, Any]:
        captured["mark_read"] = {
            "group_name": group_name,
            "message_ids": message_ids,
            "notify_sender": notify_sender,
            "data": data,
        }
        return {"marked_message_ids": message_ids}

    ctx = _base_ctx()
    ctx.inbox.group.peek_callback = _peek
    ctx.inbox.group.mark_read_callback = _mark_read
    token = execution_context.set(ctx)
    try:
        page = await inbox.group.peek("village", unread_only=True, limit=5)
        result = await page.mark_read(
            notify_sender=False,
            data={"validated": True},
        )
    finally:
        execution_context.reset(token)

    assert captured["peek"] == {
        "group_name": "village",
        "unread_only": True,
        "limit": 5,
        "cursor": None,
    }
    assert captured["mark_read"] == {
        "group_name": "village",
        "message_ids": ["2-0"],
        "notify_sender": False,
        "data": {"validated": True},
    }
    assert result["marked_message_ids"] == ["2-0"]


@pytest.mark.asyncio
async def test_receipts_inbox_peek_and_mark_read_route_callbacks() -> None:
    captured: dict[str, Any] = {}

    async def _peek(
        unread_only: bool,
        limit: int,
        cursor: str | None,
    ) -> dict[str, Any]:
        captured["peek"] = {
            "unread_only": unread_only,
            "limit": limit,
            "cursor": cursor,
        }
        return {
            "messages": [
                {
                    "receipt_id": "7-0",
                    "source_message_id": "1-0",
                    "source_kind": "direct",
                    "source_group_name": None,
                    "reader_task_id": "task-reader-2",
                    "sender_task_id": "task-reader",
                    "data": {"accepted": True},
                    "created_at": 789.0,
                    "is_read": False,
                }
            ],
            "next_cursor": "7-0",
            "has_more": False,
        }

    async def _mark_read(receipt_ids: list[str]) -> dict[str, Any]:
        captured["mark_read"] = receipt_ids
        return {"marked_receipt_ids": receipt_ids}

    ctx = _base_ctx()
    ctx.inbox.receipts.peek_callback = _peek
    ctx.inbox.receipts.mark_read_callback = _mark_read
    token = execution_context.set(ctx)
    try:
        page = await inbox.receipts.peek(unread_only=True, limit=10)
        result = await page.mark_read()
    finally:
        execution_context.reset(token)

    assert captured["peek"] == {"unread_only": True, "limit": 10, "cursor": None}
    assert captured["mark_read"] == ["7-0"]
    assert result["marked_receipt_ids"] == ["7-0"]
