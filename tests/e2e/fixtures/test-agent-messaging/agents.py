from __future__ import annotations

from typing import Any

from factorial import WaitInstruction, inbox, messaging, subagents, tool, wait
from factorial.testing import MockAgent, tool_call


@tool
async def wait_for_direct_read_signal() -> WaitInstruction:
    return wait.until_signal(
        "fixture.read_direct",
        data={"reason": "awaiting_direct_message"},
    )


@tool
async def wait_for_group_read_signal() -> WaitInstruction:
    return wait.until_signal(
        "fixture.read_group",
        data={"reason": "awaiting_group_message"},
    )


@tool
async def read_direct_inbox() -> dict[str, Any]:
    page = await inbox.direct.peek(unread_only=True, limit=10)
    if page.messages:
        first = page.messages[0]
        await page.mark_read(
            notify_sender=True,
            data={"ack": "direct"},
        )
        return {
            "message_count": len(page.messages),
            "first_content": first.content,
            "from_task_id": first.from_task_id,
        }
    return {
        "message_count": 0,
        "first_content": None,
        "from_task_id": None,
    }


@tool
async def read_group_inbox(group_name: str) -> dict[str, Any]:
    page = await inbox.group.peek(group_name, unread_only=True, limit=10)
    if page.messages:
        first = page.messages[0]
        await page.mark_read(
            notify_sender=True,
            data={"ack": "group"},
        )
        return {
            "message_count": len(page.messages),
            "group_name": group_name,
            "first_content": first.content,
            "from_task_id": first.from_task_id,
        }
    return {
        "message_count": 0,
        "group_name": group_name,
        "first_content": None,
        "from_task_id": None,
    }


@tool
async def read_sender_receipts() -> dict[str, Any]:
    page = await inbox.receipts.peek(unread_only=True, limit=10)
    reader_task_ids = sorted(
        receipt.reader_task_id
        for receipt in page.messages
        if receipt.reader_task_id is not None
    )
    receipt_data = [receipt.data for receipt in page.messages]
    await page.mark_read()
    return {
        "receipt_count": len(page.messages),
        "reader_task_ids": reader_task_ids,
        "receipt_data": receipt_data,
    }


@tool
async def spawn_send_direct_and_wait() -> WaitInstruction:
    jobs = await subagents.spawn(
        agent=direct_listener_agent,
        inputs=["Listen for a direct message from the parent task."],
        key="direct_listener",
    )
    await messaging.send(
        jobs[0],
        "hello from parent",
    )
    await subagents.signal(
        jobs[0],
        signal_id="fixture.read_direct",
        payload={"reason": "ready_to_read_direct_message"},
    )
    return wait.jobs(
        jobs,
        data={"reason": "awaiting_direct_listener"},
    )


@tool
async def spawn_group_broadcast_and_wait() -> WaitInstruction:
    jobs = await subagents.spawn(
        agent=group_listener_agent,
        inputs=[
            "Listen for the first group broadcast.",
            "Listen for the first group broadcast.",
        ],
        key="group_listeners",
    )
    group = await messaging.groups.create("research", members=jobs)
    await group.send("kickoff")
    await subagents.signal(
        jobs,
        signal_id="fixture.read_group",
        payload={"reason": "ready_to_read_group_message"},
    )
    return wait.jobs(
        jobs,
        data={
            "reason": "awaiting_group_listeners",
            "group_name": "research",
        },
    )


direct_listener_agent = MockAgent(
    name="direct_listener_agent",
    instructions="Wait for a signal, then read the direct inbox.",
    tools=[wait_for_direct_read_signal, read_direct_inbox],
    responses=[
        tool_call("wait_for_direct_read_signal"),
        tool_call("read_direct_inbox"),
        "direct inbox processed",
    ],
)


group_listener_agent = MockAgent(
    name="group_listener_agent",
    instructions="Wait for a signal, then read the group inbox.",
    tools=[wait_for_group_read_signal, read_group_inbox],
    responses=[
        tool_call("wait_for_group_read_signal"),
        tool_call("read_group_inbox", group_name="research"),
        "group inbox processed",
    ],
)


direct_messaging_parent_agent = MockAgent(
    name="direct_messaging_parent_agent",
    instructions="Spawn one listener, send a direct message, then read receipts.",
    tools=[spawn_send_direct_and_wait, read_sender_receipts],
    responses=[
        tool_call("spawn_send_direct_and_wait"),
        tool_call("read_sender_receipts"),
        "direct messaging complete",
    ],
)


group_messaging_parent_agent = MockAgent(
    name="group_messaging_parent_agent",
    instructions="Spawn listeners, broadcast to a group, then read receipts.",
    tools=[spawn_group_broadcast_and_wait, read_sender_receipts],
    responses=[
        tool_call("spawn_group_broadcast_and_wait"),
        tool_call("read_sender_receipts"),
        "group messaging complete",
    ],
)
