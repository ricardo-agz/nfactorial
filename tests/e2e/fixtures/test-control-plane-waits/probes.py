from __future__ import annotations

from factorial import HookCompletionStatus, RunStatus

from tests.e2e import (
    ProbeContext,
    output_contains,
    pending_hooks,
    probe,
    status_is,
    wait_kind_is,
)


@probe(timeout_s=15.0)
async def sleep_wait_captures_direct_human_message(ctx: ProbeContext) -> None:
    run = await ctx.run(
        "message_receiver",
        input="pause briefly, then read the latest direct human message",
    )
    snapshot = await run.wait_for(
        status_is("waiting"),
        wait_kind_is("sleep"),
        timeout_s=4.0,
    )

    assert snapshot.wait is not None
    assert snapshot.wait.data == {"reason": "awaiting_inbound_message"}

    delivery = await run.message("hello from operator")
    assert delivery.to_task_id == run.task_id
    assert delivery.delivered_task_ids == (run.task_id,)
    assert delivery.failed_task_ids == ()

    result = await run.wait_for_result(
        status_is("completed"),
        output_contains("captured inbound human message"),
        timeout_s=8.0,
    )
    assert result.output == "captured inbound human message"
    assert any(
        message.get("role") == "tool"
        and message.get("tool_name") == "read_latest_inbound_message"
        and message.get("output", {}).get("last_received_message")
        == "hello from operator"
        and message.get("output", {}).get("received_message_count") == 1
        for message in result.messages
    )


@probe(timeout_s=12.0)
async def activity_wait_resumes_on_steer(ctx: ProbeContext) -> None:
    run = await ctx.run(
        "activity_waiter",
        input="wait until the operator steers this task forward",
    )
    snapshot = await run.wait_for(
        status_is("waiting"),
        wait_kind_is("activity"),
        timeout_s=4.0,
    )

    assert snapshot.wait is not None
    assert snapshot.wait.data == {"reason": "awaiting_operator_steer"}

    await run.steer("Continue now.")

    result = await run.wait_for_result(
        status_is("completed"),
        output_contains("activity wait resumed"),
        timeout_s=6.0,
    )
    assert result.output == "activity wait resumed"
    assert any(
        message.get("role") == "user" and message.get("content") == "Continue now."
        for message in result.messages
    )


@probe(timeout_s=12.0)
async def cron_wait_enters_scheduled_state(ctx: ProbeContext) -> None:
    run = await ctx.run(
        "cron_waiter",
        input="pause until the next cron tick",
    )
    snapshot = await run.wait_for(
        status_is("waiting"),
        wait_kind_is("cron"),
        timeout_s=4.0,
    )

    assert snapshot.wait is not None
    assert snapshot.wait.data == {"reason": "awaiting_fixture_cron_tick"}

    await run.cancel()
    result = await run.wait_for_result(
        status_is("cancelled"),
        timeout_s=4.0,
    )
    assert result.status is RunStatus.CANCELLED


@probe(timeout_s=15.0)
async def hook_approval_resumes_task(ctx: ProbeContext) -> None:
    run = await ctx.run(
        "approval_waiter",
        input="request approval before you finish",
    )
    snapshot = await run.wait_for(
        status_is("waiting"),
        pending_hooks(1),
        timeout_s=4.0,
    )

    assert len(snapshot.pending_hooks) == 1

    hook_handle = await run.wait_for_hook(
        tool_name="request_fixture_approval",
        timeout_s=4.0,
    )
    assert hook_handle.snapshot.title == "Approve fixture action"
    assert hook_handle.snapshot.metadata == {
        "channel": "fixture",
        "title": "Approve fixture action",
    }

    resolution = await hook_handle.resolve({"approved": True})
    assert resolution.status is HookCompletionStatus.RESOLVED
    assert resolution.task_resumed is True

    result = await run.wait_for_result(
        status_is("completed"),
        output_contains("approval completed"),
        timeout_s=6.0,
    )
    assert result.output == "approval completed"
