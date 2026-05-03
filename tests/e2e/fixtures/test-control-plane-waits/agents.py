from __future__ import annotations

from typing import Annotated, Any

from factorial import (
    AgentContext,
    Hook,
    HookRequestContext,
    PendingHook,
    WaitInstruction,
    hook,
    inbox,
    tool,
    wait,
)
from factorial.testing import MockAgent, tool_call


def _state_dict(agent_ctx: AgentContext[Any, Any]) -> dict[str, Any]:
    if not isinstance(agent_ctx.state, dict):
        agent_ctx.state = {}
    return agent_ctx.state


@tool
async def pause_for_inbound_message() -> WaitInstruction:
    return wait.sleep(
        2.0,
        data={"reason": "awaiting_inbound_message"},
    )


@tool
async def read_latest_inbound_message(
    agent_ctx: AgentContext[Any, Any],
) -> dict[str, Any]:
    page = await inbox.direct.peek(unread_only=True, limit=10)
    latest_message = page.messages[0] if page.messages else None
    state = _state_dict(agent_ctx)
    state["received_message_count"] = len(page.messages)
    state["last_received_message"] = (
        latest_message.content if latest_message is not None else None
    )

    if page.messages:
        await page.mark_read(
            notify_sender=False,
            data={"read_by": "test-control-plane-waits"},
        )

    return {
        "received_message_count": state["received_message_count"],
        "last_received_message": state["last_received_message"],
    }


@tool
async def wait_for_operator_steer() -> WaitInstruction:
    return wait.activity(data={"reason": "awaiting_operator_steer"})


@tool
async def wait_for_fixture_cron_tick() -> WaitInstruction:
    return wait.cron(
        "* * * * *",
        timezone="UTC",
        data={"reason": "awaiting_fixture_cron_tick"},
    )


class ApprovalHook(Hook):
    approved: bool


def _request_fixture_approval(
    ctx: HookRequestContext,
) -> PendingHook[ApprovalHook]:
    return ApprovalHook.pending(
        ctx=ctx,
        title="Approve fixture action",
        timeout_s=120.0,
        metadata={
            "channel": "fixture",
            "title": "Approve fixture action",
        },
    )


@tool
def request_fixture_approval(
    approval: Annotated[ApprovalHook, hook.requires(_request_fixture_approval)],
) -> str:
    return f"approved:{approval.approved}"


message_receiver_agent = MockAgent(
    name="message_receiver",
    instructions=(
        "Pause briefly so a human can send a direct message, then read the latest "
        "inbound message from the direct inbox."
    ),
    tools=[pause_for_inbound_message, read_latest_inbound_message],
    responses=[
        tool_call("pause_for_inbound_message"),
        tool_call("read_latest_inbound_message"),
        "captured inbound human message",
    ],
)


activity_wait_agent = MockAgent(
    name="activity_waiter",
    instructions="Pause on an activity wait until the operator steers the task.",
    tools=[wait_for_operator_steer],
    responses=[
        tool_call("wait_for_operator_steer"),
        "activity wait resumed",
    ],
)


cron_wait_agent = MockAgent(
    name="cron_waiter",
    instructions="Pause until the next cron tick.",
    tools=[wait_for_fixture_cron_tick],
    responses=[
        tool_call("wait_for_fixture_cron_tick"),
        "cron wait resumed",
    ],
)


approval_wait_agent = MockAgent(
    name="approval_waiter",
    instructions="Request a human approval hook before completing.",
    tools=[request_fixture_approval],
    responses=[
        tool_call("request_fixture_approval"),
        "approval completed",
    ],
)
