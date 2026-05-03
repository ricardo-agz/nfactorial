from __future__ import annotations

from typing import Annotated, Any

from factorial import AgentContext, Hook, HookRequestContext, PendingHook, hook, tool
from factorial.testing import MockAgent, tool_call


class IdempotentApprovalHook(Hook):
    approved: bool
    reviewer: str


def _state_dict(agent_ctx: AgentContext[Any, Any]) -> dict[str, Any]:
    if not isinstance(agent_ctx.state, dict):
        agent_ctx.state = {}
    return agent_ctx.state


def _request_idempotent_approval(
    ctx: HookRequestContext,
) -> PendingHook[IdempotentApprovalHook]:
    return IdempotentApprovalHook.pending(
        ctx=ctx,
        title="Idempotent approval",
        timeout_s=120.0,
        metadata={
            "title": "Idempotent approval",
            "channel": "finance",
        },
    )


@tool
def finalize_idempotent_approval(
    agent_ctx: AgentContext[Any, Any],
    approval: Annotated[
        IdempotentApprovalHook,
        hook.requires(_request_idempotent_approval),
    ],
) -> dict[str, Any]:
    summary = {
        "approved": approval.approved,
        "reviewer": approval.reviewer,
    }
    _state_dict(agent_ctx)["approval_summary"] = summary
    return summary


idempotent_hook_agent = MockAgent(
    name="idempotent_hook_agent",
    instructions="Request one approval and finalize from the tool output.",
    tools=[finalize_idempotent_approval],
    responses=[
        tool_call("finalize_idempotent_approval"),
        "idempotent approval completed",
    ],
)
