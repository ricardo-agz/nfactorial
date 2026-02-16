"""Contracts for hook dependency inference and validation."""

from typing import Annotated

import pytest

from factorial import Hook, HookRequestContext, PendingHook, hook, tool
from factorial.hooks import (
    HookDependencyCycleError,
    HookDependencyResolutionError,
    HookTypeMismatchError,
)


class ManagerApprovalHook(Hook):
    approved: bool


class FinanceApprovalHook(Hook):
    approved: bool


class LedgerReadyHook(Hook):
    ready: bool


def test_compiler_infers_dependency_graph_and_stages() -> None:
    def request_manager(
        ctx: HookRequestContext,
        amount: int,
    ) -> PendingHook[ManagerApprovalHook]:
        return ManagerApprovalHook.pending(ctx=ctx, amount=amount)

    def request_finance(
        ctx: HookRequestContext,
        amount: int,
        manager: ManagerApprovalHook,
    ) -> PendingHook[FinanceApprovalHook]:
        return FinanceApprovalHook.pending(
            ctx=ctx,
            amount=amount,
            manager_approved=manager.approved,
        )

    def request_ledger(
        ctx: HookRequestContext,
        manager: ManagerApprovalHook,
        finance: FinanceApprovalHook,
    ) -> PendingHook[LedgerReadyHook]:
        return LedgerReadyHook.pending(
            ctx=ctx,
            manager_approved=manager.approved,
            finance_approved=finance.approved,
        )

    @tool
    def approve_expense(
        amount: int,
        manager: Annotated[ManagerApprovalHook, hook.requires(request_manager)],
        finance: Annotated[FinanceApprovalHook, hook.requires(request_finance)],
        ledger: Annotated[LedgerReadyHook, hook.awaits(request_ledger)],
    ) -> str:
        return f"approved amount={amount}"

    assert approve_expense.hook_plan is not None
    assert approve_expense.hook_plan.tool_args == ("amount",)
    assert approve_expense.hook_plan.hook_order == ("manager", "finance", "ledger")
    assert approve_expense.hook_plan.stages == (
        ("manager",),
        ("finance",),
        ("ledger",),
    )
    assert approve_expense.hook_plan.nodes["manager"].depends_on == ()
    assert approve_expense.hook_plan.nodes["finance"].depends_on == ("manager",)
    ledger_deps = approve_expense.hook_plan.nodes["ledger"].depends_on
    assert ledger_deps == ("manager", "finance")
    assert approve_expense.hook_plan.nodes["ledger"].mode == "awaits"


def test_compiler_groups_independent_hooks_in_same_stage() -> None:
    def request_manager(
        ctx: HookRequestContext,
        amount: int,
    ) -> PendingHook[ManagerApprovalHook]:
        return ManagerApprovalHook.pending(ctx=ctx, amount=amount)

    def request_finance(
        ctx: HookRequestContext,
        amount: int,
    ) -> PendingHook[FinanceApprovalHook]:
        return FinanceApprovalHook.pending(ctx=ctx, amount=amount)

    @tool
    def approve_expense(
        amount: int,
        manager: Annotated[ManagerApprovalHook, hook.requires(request_manager)],
        finance: Annotated[FinanceApprovalHook, hook.requires(request_finance)],
    ) -> str:
        return f"approved amount={amount}"

    assert approve_expense.hook_plan is not None
    assert approve_expense.hook_plan.stages == (("manager", "finance"),)


def test_tool_schema_excludes_hook_injected_params() -> None:
    def request_manager(
        ctx: HookRequestContext,
        amount: int,
    ) -> PendingHook[ManagerApprovalHook]:
        return ManagerApprovalHook.pending(ctx=ctx, amount=amount)

    @tool
    def approve_expense(
        amount: int,
        manager: Annotated[ManagerApprovalHook, hook.requires(request_manager)],
    ) -> str:
        return f"approved amount={amount}"

    assert approve_expense.params_json_schema["properties"] == {
        "amount": {"type": "integer"}
    }
    assert approve_expense.params_json_schema["required"] == ["amount"]


def test_compiler_rejects_unresolved_required_builder_param() -> None:
    def request_manager(
        ctx: HookRequestContext,
        unknown: str,
    ) -> PendingHook[ManagerApprovalHook]:
        return ManagerApprovalHook.pending(ctx=ctx, unknown=unknown)

    with pytest.raises(
        HookDependencyResolutionError,
        match="Cannot resolve required parameter 'unknown'",
    ):

        @tool
        def approve_expense(
            amount: int,
            manager: Annotated[ManagerApprovalHook, hook.requires(request_manager)],
        ) -> str:
            return f"approved amount={amount}"


def test_compiler_rejects_type_only_hook_reference() -> None:
    def request_manager(
        ctx: HookRequestContext,
        amount: int,
    ) -> PendingHook[ManagerApprovalHook]:
        return ManagerApprovalHook.pending(ctx=ctx, amount=amount)

    def request_finance(
        ctx: HookRequestContext,
        approval: ManagerApprovalHook,
    ) -> PendingHook[FinanceApprovalHook]:
        return FinanceApprovalHook.pending(ctx=ctx, manager_approved=approval.approved)

    with pytest.raises(
        HookDependencyResolutionError,
        match="must reference other hook parameters by name",
    ):

        @tool
        def approve_expense(
            amount: int,
            manager: Annotated[ManagerApprovalHook, hook.requires(request_manager)],
            finance: Annotated[FinanceApprovalHook, hook.requires(request_finance)],
        ) -> str:
            return f"approved amount={amount}"


def test_compiler_rejects_hook_reference_type_mismatch() -> None:
    def request_manager(
        ctx: HookRequestContext,
        amount: int,
    ) -> PendingHook[ManagerApprovalHook]:
        return ManagerApprovalHook.pending(ctx=ctx, amount=amount)

    def request_finance(
        ctx: HookRequestContext,
        manager: FinanceApprovalHook,
    ) -> PendingHook[FinanceApprovalHook]:
        return FinanceApprovalHook.pending(ctx=ctx, manager_approved=manager.approved)

    with pytest.raises(HookTypeMismatchError, match="expects 'FinanceApprovalHook'"):

        @tool
        def approve_expense(
            amount: int,
            manager: Annotated[ManagerApprovalHook, hook.requires(request_manager)],
            finance: Annotated[FinanceApprovalHook, hook.requires(request_finance)],
        ) -> str:
            return f"approved amount={amount}"


def test_compiler_rejects_request_builder_return_type_mismatch() -> None:
    def request_manager(
        ctx: HookRequestContext,
        amount: int,
    ) -> PendingHook[FinanceApprovalHook]:
        return FinanceApprovalHook.pending(ctx=ctx, amount=amount)

    with pytest.raises(
        HookTypeMismatchError,
        match=r"must return PendingHook\[ManagerApprovalHook\]",
    ):

        @tool
        def approve_expense(
            amount: int,
            manager: Annotated[ManagerApprovalHook, hook.requires(request_manager)],
        ) -> str:
            return f"approved amount={amount}"


def test_compiler_detects_dependency_cycles() -> None:
    class AHook(Hook):
        ok: bool

    class BHook(Hook):
        ok: bool

    def request_a(ctx: HookRequestContext, b: BHook) -> PendingHook[AHook]:
        return AHook.pending(ctx=ctx, b_ok=b.ok)

    def request_b(ctx: HookRequestContext, a: AHook) -> PendingHook[BHook]:
        return BHook.pending(ctx=ctx, a_ok=a.ok)

    match_msg = "Cyclic hook dependencies detected"
    with pytest.raises(HookDependencyCycleError, match=match_msg):

        @tool
        def cyclic(
            amount: int,
            a: Annotated[AHook, hook.requires(request_a)],
            b: Annotated[BHook, hook.requires(request_b)],
        ) -> str:
            return f"value={amount}"


def test_compiler_allows_optional_unresolved_builder_params() -> None:
    def request_manager(
        ctx: HookRequestContext,
        amount: int,
        urgency: str = "normal",
    ) -> PendingHook[ManagerApprovalHook]:
        return ManagerApprovalHook.pending(ctx=ctx, amount=amount, urgency=urgency)

    @tool
    def approve_expense(
        amount: int,
        manager: Annotated[ManagerApprovalHook, hook.requires(request_manager)],
    ) -> str:
        return f"approved amount={amount}"

    assert approve_expense.hook_plan is not None
    assert approve_expense.hook_plan.nodes["manager"].depends_on == ()

