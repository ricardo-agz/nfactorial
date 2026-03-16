from __future__ import annotations

from typing import cast

from factorial import TurnFinishEvent
from tests.e2e import (
    ProbeContext,
    pending_children,
    probe,
    status_is,
    tool_output,
)


@probe(timeout_s=20.0)
async def direct_message_delivery_and_receipts_round_trip(ctx: ProbeContext) -> None:
    run = await ctx.run(
        "direct_messaging_parent_agent",
        input="Spawn a listener, deliver a direct message, then read receipts.",
    )
    turn_finish: TurnFinishEvent = await run.wait_for_event(
        TurnFinishEvent,
        pending_children(1),
        timeout_s=6.0,
    )

    assert len(turn_finish.pending_child_task_ids) == 1
    child = ctx.handle(
        turn_finish.pending_child_task_ids[0],
        agent_name="direct_listener_agent",
        owner_id=run.owner_id,
    )
    child_result = await child.wait_for_result(
        status_is("completed"),
        timeout_s=8.0,
    )
    child_inbox = tool_output(child_result.messages, "read_direct_inbox")
    assert child_inbox == {
        "message_count": 1,
        "first_content": "hello from parent",
        "from_task_id": run.task_id,
    }

    result = await run.wait_for_result(
        status_is("completed"),
        timeout_s=8.0,
    )
    assert result.output == "direct messaging complete"
    receipts = tool_output(result.messages, "read_sender_receipts")
    assert receipts["receipt_count"] == 1
    assert receipts["reader_task_ids"] == [child.task_id]
    assert receipts["receipt_data"] == [{"ack": "direct"}]


@probe(timeout_s=25.0)
async def group_broadcast_delivers_to_all_members_and_emits_receipts(
    ctx: ProbeContext,
) -> None:
    run = await ctx.run(
        "group_messaging_parent_agent",
        input="Spawn listeners, broadcast to the group, then read receipts.",
    )
    turn_finish: TurnFinishEvent = await run.wait_for_event(
        TurnFinishEvent,
        pending_children(2),
        timeout_s=6.0,
    )

    child_ids = sorted(turn_finish.pending_child_task_ids)
    child_results = []
    for child_id in child_ids:
        child = ctx.handle(
            child_id,
            agent_name="group_listener_agent",
            owner_id=run.owner_id,
        )
        child_results.append(
            await child.wait_for_result(
                status_is("completed"),
                timeout_s=8.0,
            )
        )

    for child_result in child_results:
        child_inbox = tool_output(child_result.messages, "read_group_inbox")
        assert child_inbox == {
            "message_count": 1,
            "group_name": "research",
            "first_content": "kickoff",
            "from_task_id": run.task_id,
        }

    result = await run.wait_for_result(
        status_is("completed"),
        timeout_s=8.0,
    )
    assert result.output == "group messaging complete"
    receipts = tool_output(result.messages, "read_sender_receipts")
    assert receipts["receipt_count"] == 2
    assert receipts["reader_task_ids"] == child_ids
    receipt_data = cast(list[object], receipts["receipt_data"])
    assert sorted(receipt_data, key=str) == [
        {"ack": "group"},
        {"ack": "group"},
    ]
