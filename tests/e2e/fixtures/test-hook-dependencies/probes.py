from __future__ import annotations

from factorial import HookCompletionStatus, HookMode
from tests.e2e import (
    ProbeContext,
    output_contains,
    pending_hooks,
    probe,
    status_is,
    tool_client_output,
)


@probe(timeout_s=20.0)
async def staged_hooks_request_parallel_then_dependent_follow_up(
    ctx: ProbeContext,
) -> None:
    run = await ctx.run(
        "staged_hook_agent",
        input="Approve the procurement request.",
    )
    first_snapshot = await run.wait_for(
        status_is("waiting"),
        pending_hooks(2),
        timeout_s=4.0,
    )

    first_hooks = {hook.param_name: hook for hook in first_snapshot.pending_hooks}
    assert set(first_hooks) == {"manager", "legal"}
    assert first_hooks["manager"].mode is HookMode.REQUIRES
    assert first_hooks["legal"].mode is HookMode.AWAITS
    assert first_hooks["manager"].metadata == {
        "title": "Manager approval",
        "stage": "initial",
        "requested_amount": 1250,
    }
    assert first_hooks["legal"].metadata == {
        "title": "Legal review",
        "stage": "initial",
        "requested_amount": 1250,
    }

    manager_hook = await run.wait_for_hook(
        tool_name="approve_procurement",
        param_name="manager",
        timeout_s=4.0,
    )
    manager_resolution = await manager_hook.resolve(
        {"approved": True, "approver": "Mina"},
    )
    assert manager_resolution.status is HookCompletionStatus.RESOLVED
    assert manager_resolution.task_resumed is False

    second_snapshot = await run.wait_for(
        status_is("waiting"),
        pending_hooks(1),
        timeout_s=4.0,
    )
    assert second_snapshot.pending_hooks[0].param_name == "legal"

    legal_hook = await run.wait_for_hook(
        tool_name="approve_procurement",
        param_name="legal",
        timeout_s=4.0,
    )
    legal_resolution = await legal_hook.resolve(
        {"approved": True, "reviewer": "Leo"},
    )
    assert legal_resolution.status is HookCompletionStatus.RESOLVED
    assert legal_resolution.task_resumed is True

    third_snapshot = await run.wait_for(
        status_is("waiting"),
        pending_hooks(1),
        timeout_s=4.0,
    )
    assert len(third_snapshot.pending_hooks) == 1
    finance_hook = third_snapshot.pending_hooks[0]
    assert finance_hook.param_name == "finance"
    assert finance_hook.mode is HookMode.REQUIRES
    assert finance_hook.metadata == {
        "title": "Finance approval",
        "stage": "finance",
        "requested_amount": 1250,
        "manager_approver": "Mina",
        "legal_reviewer": "Leo",
    }

    finance_handle = await run.wait_for_hook(
        tool_name="approve_procurement",
        param_name="finance",
        timeout_s=4.0,
    )
    finance_resolution = await finance_handle.resolve(
        {"approved": True, "budget_code": "CAP-42"},
    )
    assert finance_resolution.status is HookCompletionStatus.RESOLVED
    assert finance_resolution.task_resumed is True

    result = await run.wait_for_result(
        status_is("completed"),
        output_contains("procurement approvals complete"),
        timeout_s=6.0,
    )
    assert result.output == "procurement approvals complete"
    assert tool_client_output(result.messages, "approve_procurement") == {
        "amount": 1250,
        "approved": True,
        "manager_approver": "Mina",
        "legal_reviewer": "Leo",
        "budget_code": "CAP-42",
    }
