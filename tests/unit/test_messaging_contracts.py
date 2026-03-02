"""Contracts for runtime messaging namespace APIs."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

import pytest

from factorial.core.events import EventPublisher
from factorial.execution.context import ExecutionContext, execution_context
from factorial.execution.messaging import (
    MessageDeliveryReport,
    MessagingGroupHandle,
    messaging,
)


class _NoopEvents:
    async def publish_event(self, _event: Any) -> None:
        return None


def _base_ctx() -> ExecutionContext:
    return ExecutionContext(
        task_id="parent-task",
        owner_id="owner-1",
        retries=0,
        iterations=0,
        events=cast(EventPublisher, _NoopEvents()),
    )


@pytest.mark.asyncio
async def test_groups_create_normalizes_members() -> None:
    captured: dict[str, Any] = {}

    async def _create_group(
        group_name: str,
        member_task_ids: list[str] | None,
    ) -> dict[str, Any]:
        captured["group_name"] = group_name
        captured["member_task_ids"] = member_task_ids
        return {"team_id": "team-1", "group_name": group_name}

    ctx = _base_ctx()
    ctx.messaging.groups.create_callback = _create_group
    token = execution_context.set(ctx)
    try:
        group = await messaging.groups.create(
            "research",
            members=[
                {"task_id": "task-a"},
                SimpleNamespace(task_id="task-b"),
                "task-a",
            ],
        )
    finally:
        execution_context.reset(token)

    assert captured["group_name"] == "research"
    assert captured["member_task_ids"] == ["task-a", "task-b"]
    assert group == MessagingGroupHandle(name="research", team_id="team-1")


@pytest.mark.asyncio
async def test_groups_get_exposes_member_task_ids() -> None:
    async def _get_group(group_name: str) -> dict[str, Any]:
        return {
            "team_id": "team-1",
            "group_name": group_name,
            "member_task_ids": ["task-a", "task-b"],
        }

    ctx = _base_ctx()
    ctx.messaging.groups.get_callback = _get_group
    token = execution_context.set(ctx)
    try:
        group = await messaging.groups.get("research")
    finally:
        execution_context.reset(token)

    assert group == MessagingGroupHandle(
        name="research",
        team_id="team-1",
        member_task_ids=["task-a", "task-b"],
    )
    assert group.member_task_ids == ["task-a", "task-b"]


@pytest.mark.asyncio
async def test_group_handle_send_uses_group_callback() -> None:
    captured: dict[str, Any] = {}

    async def _send_group(
        group_name: str,
        content: str,
        data: Any,
        metadata: dict[str, Any] | None,
    ) -> dict[str, Any]:
        captured["group_name"] = group_name
        captured["content"] = content
        captured["data"] = data
        captured["metadata"] = metadata
        return {
            "thread_message_id": "1-0",
            "global_message_id": "2-0",
            "delivered_task_ids": ["task-a"],
            "skipped_inactive_task_ids": [],
            "failed_task_ids": [],
        }

    ctx = _base_ctx()
    ctx.messaging.groups.send_callback = _send_group
    token = execution_context.set(ctx)
    try:
        report = await MessagingGroupHandle(name="research", team_id="team-1").send(
            "kickoff",
            metadata={"priority": "high"},
        )
    finally:
        execution_context.reset(token)

    assert captured == {
        "group_name": "research",
        "content": "kickoff",
        "data": None,
        "metadata": {"priority": "high"},
    }
    assert report == MessageDeliveryReport(
        thread_message_id="1-0",
        global_message_id="2-0",
        delivered_task_ids=["task-a"],
        skipped_inactive_task_ids=[],
        failed_task_ids=[],
    )


@pytest.mark.asyncio
async def test_group_handle_member_mutations_use_group_callbacks() -> None:
    captured: dict[str, Any] = {}

    async def _remove_members(
        group_name: str,
        member_task_ids: list[str],
    ) -> list[str]:
        captured["remove_group_name"] = group_name
        captured["remove_member_task_ids"] = member_task_ids
        return member_task_ids

    async def _leave(group_name: str) -> bool:
        captured["leave_group_name"] = group_name
        return True

    ctx = _base_ctx()
    ctx.messaging.groups.remove_members_callback = _remove_members
    ctx.messaging.groups.leave_callback = _leave
    token = execution_context.set(ctx)
    try:
        handle = MessagingGroupHandle(name="research", team_id="team-1")
        removed = await handle.remove_members(
            [SimpleNamespace(task_id="task-a"), "task-b", "task-a"]
        )
        left = await handle.leave()
    finally:
        execution_context.reset(token)

    assert removed == ["task-a", "task-b"]
    assert left is True
    assert captured == {
        "remove_group_name": "research",
        "remove_member_task_ids": ["task-a", "task-b"],
        "leave_group_name": "research",
    }


@pytest.mark.asyncio
async def test_direct_send_accepts_jobref_like_target() -> None:
    captured: dict[str, Any] = {}

    async def _send_direct(
        to_task_id: str,
        content: str,
        data: Any,
        metadata: dict[str, Any] | None,
    ) -> dict[str, Any]:
        captured["to_task_id"] = to_task_id
        captured["content"] = content
        captured["data"] = data
        captured["metadata"] = metadata
        return {
            "thread_message_id": "3-0",
            "global_message_id": "4-0",
            "delivered_task_ids": [to_task_id],
            "skipped_inactive_task_ids": [],
            "failed_task_ids": [],
        }

    ctx = _base_ctx()
    ctx.messaging.send_callback = _send_direct
    token = execution_context.set(ctx)
    try:
        report = await messaging.send(
            SimpleNamespace(task_id="task-x"),
            "hello",
            metadata={"reason": "coordination"},
        )
    finally:
        execution_context.reset(token)

    assert captured == {
        "to_task_id": "task-x",
        "content": "hello",
        "data": None,
        "metadata": {"reason": "coordination"},
    }
    assert report.delivered_task_ids == ["task-x"]


@pytest.mark.asyncio
async def test_groups_remove_members_normalizes_members() -> None:
    captured: dict[str, Any] = {}

    async def _remove_members(
        group_name: str,
        member_task_ids: list[str],
    ) -> list[str]:
        captured["group_name"] = group_name
        captured["member_task_ids"] = member_task_ids
        return member_task_ids

    ctx = _base_ctx()
    ctx.messaging.groups.remove_members_callback = _remove_members
    token = execution_context.set(ctx)
    try:
        removed = await messaging.groups.remove_members(
            "research",
            [
                {"task_id": "task-a"},
                SimpleNamespace(task_id="task-b"),
                "task-a",
            ],
        )
    finally:
        execution_context.reset(token)

    assert removed == ["task-a", "task-b"]
    assert captured == {
        "group_name": "research",
        "member_task_ids": ["task-a", "task-b"],
    }


@pytest.mark.asyncio
async def test_execution_context_messaging_namespace_routes_callbacks() -> None:
    async def _list_groups() -> list[dict[str, Any]]:
        return [{"team_id": "team-1", "group_name": "research"}]

    async def _send_group(
        group_name: str,
        content: str,
        _data: Any,
        metadata: dict[str, Any] | None,
    ) -> dict[str, Any]:
        return {
            "thread_message_id": "group-1",
            "global_message_id": "global-1",
            "delivered_task_ids": [group_name, content],
            "skipped_inactive_task_ids": [],
            "failed_task_ids": [],
            "metadata": metadata,
        }

    async def _send_direct(
        to_task_id: str,
        _content: str,
        _data: Any,
        _metadata: dict[str, Any] | None,
    ) -> dict[str, Any]:
        return {
            "thread_message_id": "direct-1",
            "global_message_id": "global-2",
            "delivered_task_ids": [to_task_id],
            "skipped_inactive_task_ids": [],
            "failed_task_ids": [],
        }

    async def _remove_members(
        group_name: str,
        member_task_ids: list[str],
    ) -> list[str]:
        return [group_name, *member_task_ids]

    async def _leave_group(_group_name: str) -> bool:
        return True

    ctx = _base_ctx()
    ctx.messaging.groups.list_callback = _list_groups
    ctx.messaging.groups.send_callback = _send_group
    ctx.messaging.groups.remove_members_callback = _remove_members
    ctx.messaging.groups.leave_callback = _leave_group
    ctx.messaging.send_callback = _send_direct

    groups = await ctx.messaging.groups.list()
    removed = await ctx.messaging.groups.remove_members("research", ["task-z"])
    left = await ctx.messaging.groups.leave("research")
    group_report = await ctx.messaging.groups.send("research", "kickoff")
    direct_report = await ctx.messaging.send("task-z", "ping")

    assert groups == [{"team_id": "team-1", "group_name": "research"}]
    assert removed == ["research", "task-z"]
    assert left is True
    assert group_report["thread_message_id"] == "group-1"
    assert direct_report["delivered_task_ids"] == ["task-z"]


@pytest.mark.asyncio
async def test_messaging_requires_active_execution_context() -> None:
    with pytest.raises(RuntimeError, match="active task execution"):
        await messaging.send("task-x", "hello")
