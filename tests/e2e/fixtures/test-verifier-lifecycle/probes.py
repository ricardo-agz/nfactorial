from __future__ import annotations

import json

from tests.e2e import ProbeContext, field_equals, probe, status_is


@probe(timeout_s=15.0)
async def verifier_retries_then_accepts(ctx: ProbeContext) -> None:
    run = await ctx.run(
        "verification_retry_agent",
        input="Generate a verified answer.",
    )
    result = await run.wait_for_result(
        status_is("completed"),
        field_equals("turn_count", 2),
        timeout_s=8.0,
    )

    assert result.output == json.dumps(
        {"summary": "second attempt", "score": 10},
        separators=(",", ":"),
        sort_keys=True,
    )
    assert any(
        message.get("role") == "system"
        and "Verifier feedback [score_low]" in str(message.get("content", ""))
        for message in result.messages
    )
    assert sum(message.get("role") == "assistant" for message in result.messages) == 2


@probe(timeout_s=15.0)
async def verifier_failure_surfaces_terminal_error(ctx: ProbeContext) -> None:
    run = await ctx.run(
        "verification_failure_agent",
        input="Reject this answer until verification fails.",
    )
    result = await run.wait_for_result(
        status_is("failed"),
        field_equals("turn_count", 2),
        timeout_s=8.0,
    )

    assert result.output is None
    assert any(
        message.get("role") == "system"
        and "Verifier feedback [tests_failed]" in str(message.get("content", ""))
        for message in result.messages
    )
    assert sum(message.get("role") == "assistant" for message in result.messages) == 1
