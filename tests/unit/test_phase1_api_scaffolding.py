"""Phase 1 tests for namespace-first API scaffolding."""

from __future__ import annotations

from datetime import datetime, timezone

from factorial import (
    FunctionTool,
    Hook,
    HookDependency,
    HookRequestContext,
    PendingHook,
    ToolResult,
    hook,
    tool,
    wait,
)
from factorial.waits import WaitInstruction


class ApprovalHook(Hook):
    approved: bool
    reason: str | None = None


def test_tool_namespace_as_decorator_returns_function_tool() -> None:
    @tool
    def greet(name: str) -> str:
        return f"hi {name}"

    assert isinstance(greet, FunctionTool)
    assert greet.name == "greet"
    assert greet.description == "Execute greet"


def test_tool_namespace_accepts_configuration_arguments() -> None:
    @tool(name="lookup", description="Lookup data")
    def query(term: str) -> str:
        return term

    assert isinstance(query, FunctionTool)
    assert query.name == "lookup"
    assert query.description == "Lookup data"


def test_tool_result_constructors_expose_status() -> None:
    ok = tool.ok("completed", {"id": 1})
    failed = tool.fail("needs action", {"code": "REVIEW"})
    errored = tool.error("unexpected", {"code": "E_GENERIC"})

    assert isinstance(ok, ToolResult)
    assert ok.status == "ok"
    assert ok.output_str == "completed"
    assert ok.output_data == {"id": 1}

    assert failed.status == "fail"
    assert failed.output_str == "needs action"
    assert failed.output_data == {"code": "REVIEW"}

    assert errored.status == "error"
    assert errored.output_str == "unexpected"
    assert errored.output_data == {"code": "E_GENERIC"}


def test_hook_pending_builder_returns_ticket_with_auth_helpers() -> None:
    ctx = HookRequestContext(
        task_id="task-1",
        owner_id="owner-1",
        agent_name="agent-A",
        tool_name="request_approval",
        tool_call_id="call-1",
        args={"change": "deploy"},
        hooks_public_base_url="https://example.test",
    )

    pending = ApprovalHook.pending(
        ctx=ctx,
        title="Approve deploy",
        body="Approve this deployment?",
        channel="slack",
        timeout_s=90,
        metadata={"priority": "high"},
        request_id="req-123",
    )

    assert isinstance(pending, PendingHook)
    assert pending.hook_type is ApprovalHook
    assert pending.title == "Approve deploy"
    assert pending.submit_url == f"https://example.test/hooks/{pending.hook_id}/submit"
    assert pending.metadata["priority"] == "high"
    assert pending.metadata["body"] == "Approve this deployment?"
    assert pending.metadata["channel"] == "slack"
    assert pending.metadata["request_id"] == "req-123"
    assert pending.auth_headers() == {"X-NFactorial-Hook-Token": pending.token}
    assert pending.auth_query() == {"token": pending.token}
    assert pending.expires_at > datetime.now(timezone.utc)


def test_hook_namespace_dependency_metadata() -> None:
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


def test_wait_namespace_builders_return_serializable_instructions() -> None:
    class DummyAgent:
        name = "child-research-agent"

    sleep_wait = wait.sleep(12.5, message="retry shortly")
    cron_wait = wait.cron("0 * * * *", timezone="UTC", message="hourly sync")
    children_wait = wait.children(
        agent=DummyAgent(),
        inputs=[{"query": "redis hooks"}],
        timeout_s=300,
        message="waiting for child tasks",
    )

    assert isinstance(sleep_wait, WaitInstruction)
    assert sleep_wait.kind == "sleep"
    assert sleep_wait.sleep_s == 12.5
    assert sleep_wait.message == "retry shortly"

    assert isinstance(cron_wait, WaitInstruction)
    assert cron_wait.kind == "cron"
    assert cron_wait.cron == "0 * * * *"
    assert cron_wait.timezone == "UTC"
    assert cron_wait.message == "hourly sync"

    assert isinstance(children_wait, WaitInstruction)
    assert children_wait.kind == "children"
    assert children_wait.child_agent_name == "child-research-agent"
    assert children_wait.child_inputs == [{"query": "redis hooks"}]
    assert children_wait.timeout_s == 300
    assert children_wait.message == "waiting for child tasks"
