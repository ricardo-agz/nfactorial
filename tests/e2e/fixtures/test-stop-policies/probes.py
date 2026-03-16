from __future__ import annotations

from tests.e2e import (
    ProbeContext,
    field_equals,
    output_contains,
    output_field_equals,
    probe,
    status_is,
)


@probe(timeout_s=12.0)
async def tool_called_stop_returns_tool_output(ctx: ProbeContext) -> None:
    run = await ctx.run(
        "finish_tool_stop_agent",
        input="Finish using the done tool.",
    )
    result = await run.wait_for_result(
        status_is("completed"),
        output_field_equals("summary", "Ship it."),
        field_equals("turn_count", 1),
        timeout_s=4.0,
    )

    assert result.output == {"summary": "Ship it."}
    assert result.messages[-2]["role"] == "assistant_tool_calls"
    assert result.messages[-1]["role"] == "tool"


@probe(timeout_s=12.0)
async def turn_limit_stop_fails_without_final_output(ctx: ProbeContext) -> None:
    run = await ctx.run(
        "turn_limit_failure_agent",
        input="Trigger a tool call without a finalized output.",
    )
    result = await run.wait_for_result(
        status_is("failed"),
        field_equals("turn_count", 1),
        timeout_s=4.0,
    )

    assert result.output is None


@probe(timeout_s=12.0)
async def composite_stop_waits_for_second_natural_language_turn(
    ctx: ProbeContext,
) -> None:
    run = await ctx.run(
        "composite_all_of_stop_agent",
        input="Do not stop on the first natural-language answer.",
    )
    result = await run.wait_for_result(
        status_is("completed"),
        output_contains("final answer on turn two"),
        field_equals("turn_count", 2),
        timeout_s=4.0,
    )

    assert result.output == "final answer on turn two"
    assert [
        message.get("content")
        for message in result.messages
        if message.get("role") == "assistant"
    ] == [
        "draft answer",
        "final answer on turn two",
    ]
