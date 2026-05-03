from __future__ import annotations

from factorial import WaitInstruction, tool, wait
from factorial.testing import MockAgent, tool_call


@tool
async def wait_for_fixture_signal() -> WaitInstruction:
    return wait.until_signal(
        "fixture.launch",
        data={"reason": "awaiting_fixture_signal"},
    )


@tool
async def wait_for_fixture_signal_with_timeout() -> WaitInstruction:
    return wait.until_signal(
        "fixture.timeout",
        timeout=wait.sleep(1.0),
        data={"reason": "awaiting_fixture_signal_timeout"},
    )


signal_wait_agent = MockAgent(
    name="signal_wait_agent",
    instructions="Wait for a manual signal before continuing.",
    tools=[wait_for_fixture_signal],
    responses=[
        tool_call("wait_for_fixture_signal"),
        "manual signal received",
    ],
)


signal_timeout_agent = MockAgent(
    name="signal_timeout_agent",
    instructions="Wait for a signal, but continue once the timeout fires.",
    tools=[wait_for_fixture_signal_with_timeout],
    responses=[
        tool_call("wait_for_fixture_signal_with_timeout"),
        "signal wait timed out",
    ],
)
