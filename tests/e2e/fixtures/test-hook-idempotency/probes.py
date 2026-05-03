from __future__ import annotations

from factorial import HookCompletionStatus
from tests.e2e import (
    ProbeContext,
    output_contains,
    pending_hooks,
    probe,
    status_is,
    tool_client_output,
)


@probe(timeout_s=15.0)
async def hook_resolution_is_idempotent_when_key_is_reused(
    ctx: ProbeContext,
) -> None:
    run = await ctx.run(
        "idempotent_hook_agent",
        input="Approve this idempotently.",
    )
    snapshot = await run.wait_for(
        status_is("waiting"),
        pending_hooks(1),
        timeout_s=4.0,
    )

    assert len(snapshot.pending_hooks) == 1
    hook_handle = await run.wait_for_hook(
        tool_name="finalize_idempotent_approval",
        param_name="approval",
        timeout_s=4.0,
    )
    assert hook_handle.snapshot.metadata == {
        "title": "Idempotent approval",
        "channel": "finance",
    }

    first_resolution = await hook_handle.resolve(
        {"approved": True, "reviewer": "Nora"},
        idempotency_key="approval-event-1",
    )
    second_resolution = await hook_handle.resolve(
        {"approved": True, "reviewer": "Nora"},
        idempotency_key="approval-event-1",
    )
    assert first_resolution.status is HookCompletionStatus.RESOLVED
    assert second_resolution.status is HookCompletionStatus.IDEMPOTENT

    result = await run.wait_for_result(
        status_is("completed"),
        output_contains("idempotent approval completed"),
        timeout_s=6.0,
    )
    assert result.output == "idempotent approval completed"
    assert tool_client_output(result.messages, "finalize_idempotent_approval") == {
        "approved": True,
        "reviewer": "Nora",
    }
