"""Contracts for hook namespace helpers and pending tickets."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, cast

import pytest

from factorial import Hook, HookDependency, HookRequestContext, PendingHook, hook


class ApprovalHook(Hook):
    approved: bool
    reason: str | None = None


def test_pending_builder_merges_metadata_with_correct_precedence() -> None:
    ctx = HookRequestContext(
        task_id="task-1",
        owner_id="owner-1",
        agent_name="agent-A",
        tool_name="request_approval",
        tool_call_id="call-1",
        args={"change": "deploy"},
        hooks_public_base_url="https://example.test/",
    )
    start = datetime.now(timezone.utc)

    pending = ApprovalHook.pending(
        ctx=ctx,
        hook_id="hook-1",
        token="token-1",
        timeout_s=90,
        body="default-body",
        channel="default-channel",
        metadata={
            "priority": "high",
            "body": "custom-body",
            "channel": "email",
            "request_id": "old-id",
        },
        request_id="req-123",
    )

    assert isinstance(pending, PendingHook)
    assert pending.hook_type is ApprovalHook
    assert pending.submit_url == "https://example.test/hooks/hook-1/submit"
    assert pending.metadata == {
        "priority": "high",
        "body": "custom-body",
        "channel": "email",
        "request_id": "req-123",
    }
    assert pending.auth_headers() == {"X-NFactorial-Hook-Token": "token-1"}
    assert pending.auth_query() == {"token": "token-1"}
    expires_in_s = (pending.expires_at - start).total_seconds()
    assert 89.0 <= expires_in_s <= 91.0


def test_pending_builder_respects_explicit_submit_url_and_optional_public_base(
) -> None:
    explicit_ctx = HookRequestContext(
        task_id="task-1",
        owner_id="owner-1",
        agent_name="agent-A",
        tool_name="request_approval",
        tool_call_id="call-1",
        hooks_public_base_url="https://ignored.example",
    )
    explicit = ApprovalHook.pending(
        ctx=explicit_ctx,
        hook_id="hook-explicit",
        submit_url="https://hooks.example/custom/submit",
    )
    assert explicit.submit_url == "https://hooks.example/custom/submit"

    no_base_ctx = HookRequestContext(
        task_id="task-1",
        owner_id="owner-1",
        agent_name="agent-A",
        tool_name="request_approval",
        tool_call_id="call-1",
    )
    without_base = ApprovalHook.pending(ctx=no_base_ctx, hook_id="hook-no-base")
    assert without_base.submit_url is None


def test_hook_namespace_requires_and_awaits_validate_callables() -> None:
    def request_builder(ctx: HookRequestContext) -> PendingHook[ApprovalHook]:
        return ApprovalHook.pending(ctx=ctx, timeout_s=30)

    requires_dep = hook.requires(request_builder)
    awaits_dep = hook.awaits(request_builder)

    assert isinstance(requires_dep, HookDependency)
    assert requires_dep.mode == "requires"
    assert requires_dep.request is request_builder
    assert isinstance(awaits_dep, HookDependency)
    assert awaits_dep.mode == "awaits"
    assert awaits_dep.request is request_builder

    with pytest.raises(TypeError, match="expects a callable request builder"):
        hook.requires(cast(Any, "not-callable"))

    with pytest.raises(TypeError, match="expects a callable request builder"):
        hook.awaits(cast(Any, None))

