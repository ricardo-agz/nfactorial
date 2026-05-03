from __future__ import annotations

from tests.e2e import ProbeContext, output_contains, probe, status_is


@probe(timeout_s=20.0)
async def retryable_failure_enters_backoff_then_recovers(ctx: ProbeContext) -> None:
    run = await ctx.run(
        "backoff_recovery_agent",
        input="Recover after one retryable failure.",
    )
    snapshot = await run.wait_for(
        status_is("backoff"),
        timeout_s=4.0,
    )

    assert snapshot.backoff_until is not None

    result = await run.wait_for_result(
        status_is("completed"),
        output_contains("recovered after backoff"),
        timeout_s=15.0,
    )
    assert result.output == "recovered after backoff"


@probe(timeout_s=20.0)
async def retryable_failure_fails_after_retries_exhaust(ctx: ProbeContext) -> None:
    run = await ctx.run(
        "backoff_exhaustion_agent",
        input="Keep retrying until backoff retries are exhausted.",
    )
    snapshot = await run.wait_for(
        status_is("backoff"),
        timeout_s=4.0,
    )

    assert snapshot.backoff_until is not None

    result = await run.wait_for_result(
        status_is("failed"),
        timeout_s=15.0,
    )
    assert result.output is None
