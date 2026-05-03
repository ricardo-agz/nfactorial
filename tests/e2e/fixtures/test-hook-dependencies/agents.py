from __future__ import annotations

from typing import Annotated, Any

from factorial import AgentContext, Hook, HookRequestContext, PendingHook, hook, tool
from factorial.testing import MockAgent, tool_call


class ManagerApprovalHook(Hook):
    approved: bool
    approver: str


class LegalReviewHook(Hook):
    approved: bool
    reviewer: str


class FinanceApprovalHook(Hook):
    approved: bool
    budget_code: str


def _state_dict(agent_ctx: AgentContext[Any, Any]) -> dict[str, Any]:
    if not isinstance(agent_ctx.state, dict):
        agent_ctx.state = {}
    return agent_ctx.state


def _request_manager_approval(
    ctx: HookRequestContext,
    amount: int,
) -> PendingHook[ManagerApprovalHook]:
    return ManagerApprovalHook.pending(
        ctx=ctx,
        title="Manager approval",
        timeout_s=120.0,
        metadata={
            "title": "Manager approval",
            "stage": "initial",
            "requested_amount": amount,
        },
    )


def _request_legal_review(
    ctx: HookRequestContext,
    amount: int,
) -> PendingHook[LegalReviewHook]:
    return LegalReviewHook.pending(
        ctx=ctx,
        title="Legal review",
        timeout_s=120.0,
        metadata={
            "title": "Legal review",
            "stage": "initial",
            "requested_amount": amount,
        },
    )


def _request_finance_approval(
    ctx: HookRequestContext,
    amount: int,
    manager: ManagerApprovalHook,
    legal: LegalReviewHook,
) -> PendingHook[FinanceApprovalHook]:
    return FinanceApprovalHook.pending(
        ctx=ctx,
        title="Finance approval",
        timeout_s=120.0,
        metadata={
            "title": "Finance approval",
            "stage": "finance",
            "requested_amount": amount,
            "manager_approver": manager.approver,
            "legal_reviewer": legal.reviewer,
        },
    )


@tool
def approve_procurement(
    agent_ctx: AgentContext[Any, Any],
    amount: int,
    manager: Annotated[ManagerApprovalHook, hook.requires(_request_manager_approval)],
    legal: Annotated[LegalReviewHook, hook.awaits(_request_legal_review)],
    finance: Annotated[FinanceApprovalHook, hook.requires(_request_finance_approval)],
) -> dict[str, Any]:
    summary = {
        "amount": amount,
        "approved": manager.approved and legal.approved and finance.approved,
        "manager_approver": manager.approver,
        "legal_reviewer": legal.reviewer,
        "budget_code": finance.budget_code,
    }
    _state_dict(agent_ctx)["approval_summary"] = summary
    return summary


staged_hook_agent = MockAgent(
    name="staged_hook_agent",
    instructions="Request staged approvals before finalizing the procurement tool.",
    tools=[approve_procurement],
    responses=[
        tool_call("approve_procurement", amount=1250),
        "procurement approvals complete",
    ],
)
