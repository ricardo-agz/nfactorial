from __future__ import annotations

from factorial import all_of, no_tool_calls, tool, tool_called, turn_count_is
from factorial.testing import MockAgent, tool_call


@tool
def done(summary: str) -> dict[str, str]:
    return {"summary": summary}


@tool
def noop() -> None:
    return None


finish_tool_stop_agent = MockAgent(
    name="finish_tool_stop_agent",
    instructions="Finish by calling the done tool.",
    tools=[done],
    stop_when=tool_called("done"),
    responses=[
        tool_call("done", summary="Ship it."),
        "should not be reached",
    ],
)


turn_limit_failure_agent = MockAgent(
    name="turn_limit_failure_agent",
    instructions="Make one non-final tool call so the turn limit fails the run.",
    tools=[noop],
    stop_when=turn_count_is(1),
    responses=[
        tool_call("noop"),
        "should not be reached",
    ],
)


composite_all_of_stop_agent = MockAgent(
    name="composite_all_of_stop_agent",
    instructions="Only stop once a natural-language response happens on turn two.",
    stop_when=all_of(no_tool_calls(), turn_count_is(2)),
    responses=[
        "draft answer",
        "final answer on turn two",
        "should not be reached",
    ],
)
