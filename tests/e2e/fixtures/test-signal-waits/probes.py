from __future__ import annotations

from tests.e2e import ProbeContext, output_contains, probe, status_is, wait_kind_is


@probe(timeout_s=15.0)
async def signal_wait_resumes_when_woken(ctx: ProbeContext) -> None:
    run = await ctx.run(
        "signal_wait_agent",
        input="Wait for the launch signal before continuing.",
    )
    snapshot = await run.wait_for(
        status_is("waiting"),
        wait_kind_is("signal"),
        timeout_s=4.0,
    )

    assert snapshot.wait is not None
    assert snapshot.wait.signal_id == "fixture.launch"
    assert snapshot.wait.data == {"reason": "awaiting_fixture_signal"}

    woke = await run.wake("Launch approved.")
    assert woke is True

    result = await run.wait_for_result(
        status_is("completed"),
        output_contains("manual signal received"),
        timeout_s=6.0,
    )
    assert result.output == "manual signal received"
    assert any(
        message.get("role") == "system"
        and "interrupted a signal wait" in str(message.get("content", ""))
        for message in result.messages
    )
    assert any(
        message.get("role") == "user" and message.get("content") == "Launch approved."
        for message in result.messages
    )


@probe(timeout_s=15.0)
async def signal_wait_timeout_resumes_without_manual_input(ctx: ProbeContext) -> None:
    run = await ctx.run(
        "signal_timeout_agent",
        input="Wait for a signal but continue once the timeout fires.",
    )
    snapshot = await run.wait_for(
        status_is("waiting"),
        wait_kind_is("signal"),
        timeout_s=4.0,
    )

    assert snapshot.wait is not None
    assert snapshot.wait.signal_id == "fixture.timeout"
    assert snapshot.wait.data == {"reason": "awaiting_fixture_signal_timeout"}

    result = await run.wait_for_result(
        status_is("completed"),
        output_contains("signal wait timed out"),
        timeout_s=8.0,
    )
    assert result.output == "signal wait timed out"
    assert result.turn_count == 2
