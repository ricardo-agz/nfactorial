"""Contracts for subagent spawning, signaling, and cancellation."""

from __future__ import annotations

import uuid
from types import SimpleNamespace
from typing import Any, cast

import pytest

from factorial.agent.context import AgentContext
from factorial.core.events import EventPublisher
from factorial.execution.context import (
    ExecutionContext,
    SubagentsExecutionNamespace,
    execution_context,
)
from factorial.execution.subagents import JobRef, SignalDeliveryReport, subagents


class _NoopEvents:
    async def publish_event(self, _event: Any) -> None:
        return None


class _DummyChildAgent:
    name = "child-agent"

    def context_from_dict(self, data: dict) -> AgentContext:
        return AgentContext.from_dict(data)

    def build_context(self, input: str | list) -> AgentContext:
        from factorial.ai.messages import normalize_messages_input

        return AgentContext(messages=normalize_messages_input(input))


class _InvalidContextChildAgent:
    """Agent without context_from_dict/build_context for error-path testing."""

    name = "child-agent"


@pytest.mark.asyncio
async def test_spawn_enqueues_children_and_returns_job_refs() -> None:
    captured_payloads: list[AgentContext] = []
    captured_task_ids: list[str] = []

    async def _enqueue_child_task(
        _agent: Any,
        payload: Any,
        task_id: str | None,
    ) -> str:
        assert task_id is not None
        captured_payloads.append(cast(AgentContext, payload))
        captured_task_ids.append(task_id)
        return task_id

    ctx = ExecutionContext(
        task_id="parent-1",
        owner_id="owner-1",
        retry_count=0,
        events=cast(EventPublisher, _NoopEvents()),
        subagents=SubagentsExecutionNamespace(
            enqueue_callback=_enqueue_child_task,
        ),
    )
    token = execution_context.set(ctx)
    try:
        jobs = await subagents.spawn(
            agent=_DummyChildAgent(),
            inputs=[
                {"messages": [{"role": "user", "content": "q1"}]},
                {"messages": [{"role": "user", "content": "q2"}]},
            ],
            key="research",
        )
    finally:
        execution_context.reset(token)

    assert len(jobs) == 2
    assert jobs[0] == JobRef(
        task_id=captured_task_ids[0],
        agent_name="child-agent",
        parent_task_id="parent-1",
        key="research",
    )
    assert jobs[1] == JobRef(
        task_id=captured_task_ids[1],
        agent_name="child-agent",
        parent_task_id="parent-1",
        key="research",
    )
    assert jobs[0].task_id != jobs[1].task_id
    uuid.UUID(jobs[0].task_id)
    uuid.UUID(jobs[1].task_id)

    def _first_user_content(p: AgentContext) -> str:
        for m in p.messages:
            if m.get("role") == "user" and isinstance(m.get("content"), str):
                return m["content"]
        return ""

    assert [_first_user_content(p) for p in captured_payloads] == ["q1", "q2"]


@pytest.mark.asyncio
async def test_run_returns_wait_jobs_instruction() -> None:
    captured_task_ids: list[str] = []

    async def _enqueue_child_task(
        _agent: Any,
        _payload: Any,
        task_id: str | None,
    ) -> str:
        assert task_id is not None
        captured_task_ids.append(task_id)
        return task_id

    ctx = ExecutionContext(
        task_id="parent-1",
        owner_id="owner-1",
        retry_count=0,
        events=cast(EventPublisher, _NoopEvents()),
        subagents=SubagentsExecutionNamespace(
            enqueue_callback=_enqueue_child_task,
        ),
    )
    token = execution_context.set(ctx)
    try:
        instruction = await subagents.run(
            agent=_DummyChildAgent(),
            inputs=[{"messages": [{"role": "user", "content": "q1"}]}],
            key="research",
            data="waiting for research",
        )
    finally:
        execution_context.reset(token)

    assert instruction.kind == "jobs"
    assert instruction.child_task_ids == captured_task_ids
    assert instruction.data == "waiting for research"


@pytest.mark.asyncio
async def test_spawn_is_deterministic_for_repeated_call_with_same_key() -> None:
    seen_task_ids: list[str] = []

    async def _enqueue_child_task(
        _agent: Any,
        _payload: Any,
        task_id: str | None,
    ) -> str:
        assert task_id is not None
        seen_task_ids.append(task_id)
        return task_id

    ctx = ExecutionContext(
        task_id="parent-1",
        owner_id="owner-1",
        retry_count=0,
        events=cast(EventPublisher, _NoopEvents()),
        subagents=SubagentsExecutionNamespace(
            enqueue_callback=_enqueue_child_task,
        ),
    )
    token = execution_context.set(ctx)
    try:
        first = await subagents.spawn(
            agent=_DummyChildAgent(),
            inputs=[
                {"messages": [{"role": "user", "content": "q1"}]},
                {"messages": [{"role": "user", "content": "q2"}]},
            ],
            key="research",
        )
        second = await subagents.spawn(
            agent=_DummyChildAgent(),
            inputs=[
                {"messages": [{"role": "user", "content": "q1"}]},
                {"messages": [{"role": "user", "content": "q2"}]},
            ],
            key="research",
        )
    finally:
        execution_context.reset(token)

    assert [job.task_id for job in first] == [job.task_id for job in second]
    assert seen_task_ids[:2] == seen_task_ids[2:]


@pytest.mark.asyncio
async def test_spawn_requires_non_empty_key() -> None:
    async def _enqueue_child_task(
        _agent: Any,
        _payload: Any,
        task_id: str | None,
    ) -> str:
        return task_id or ""

    ctx = ExecutionContext(
        task_id="parent-1",
        owner_id="owner-1",
        retry_count=0,
        events=cast(EventPublisher, _NoopEvents()),
        subagents=SubagentsExecutionNamespace(
            enqueue_callback=_enqueue_child_task,
        ),
    )
    token = execution_context.set(ctx)
    try:
        with pytest.raises(ValueError, match="non-empty key"):
            await subagents.spawn(
                agent=_DummyChildAgent(),
                inputs=[{"messages": [{"role": "user", "content": "q1"}]}],
                key="",
            )
    finally:
        execution_context.reset(token)


@pytest.mark.asyncio
async def test_spawn_rejects_inputs_that_cannot_be_coerced_to_agent_context() -> None:
    async def _enqueue_child_task(
        _agent: Any,
        _payload: Any,
        task_id: str | None,
    ) -> str:
        return task_id or ""

    ctx = ExecutionContext(
        task_id="parent-1",
        owner_id="owner-1",
        retry_count=0,
        events=cast(EventPublisher, _NoopEvents()),
        subagents=SubagentsExecutionNamespace(
            enqueue_callback=_enqueue_child_task,
        ),
    )
    token = execution_context.set(ctx)
    try:
        with pytest.raises(TypeError, match="inputs must be context instances"):
            await subagents.spawn(
                agent=_DummyChildAgent(),
                inputs=[object()],
                key="research",
            )
    finally:
        execution_context.reset(token)


@pytest.mark.asyncio
async def test_spawn_rejects_agent_with_invalid_input_coercion() -> None:
    async def _enqueue_child_task(
        _agent: Any,
        _payload: Any,
        task_id: str | None,
    ) -> str:
        return task_id or ""

    ctx = ExecutionContext(
        task_id="parent-1",
        owner_id="owner-1",
        retry_count=0,
        events=cast(EventPublisher, _NoopEvents()),
        subagents=SubagentsExecutionNamespace(
            enqueue_callback=_enqueue_child_task,
        ),
    )
    token = execution_context.set(ctx)
    try:
        with pytest.raises(TypeError, match="cannot coerce"):
            await subagents.spawn(
                agent=_InvalidContextChildAgent(),
                inputs=[{"messages": [{"role": "user", "content": "q1"}]}],
                key="research",
            )
    finally:
        execution_context.reset(token)


@pytest.mark.asyncio
async def test_subagents_cancel_accepts_job_ref_like_target() -> None:
    cancelled_task_ids: list[str] = []

    async def _cancel_child(task_id: str) -> None:
        cancelled_task_ids.append(task_id)

    ctx = ExecutionContext(
        task_id="parent-1",
        owner_id="owner-1",
        retry_count=0,
        events=cast(EventPublisher, _NoopEvents()),
        subagents=SubagentsExecutionNamespace(cancel_callback=_cancel_child),
    )
    token = execution_context.set(ctx)
    try:
        await subagents.cancel(
            JobRef(
                task_id="child-2",
                agent_name="child-agent",
                parent_task_id="parent-1",
            )
        )
    finally:
        execution_context.reset(token)

    assert cancelled_task_ids == ["child-2"]


@pytest.mark.asyncio
async def test_subagents_cancel_accepts_list_and_uses_batch_callback() -> None:
    cancelled_task_batches: list[list[str]] = []

    async def _cancel_children(task_ids: list[str]) -> None:
        cancelled_task_batches.append(list(task_ids))

    ctx = ExecutionContext(
        task_id="parent-1",
        owner_id="owner-1",
        retry_count=0,
        events=cast(EventPublisher, _NoopEvents()),
        subagents=SubagentsExecutionNamespace(cancel_many_callback=_cancel_children),
    )
    token = execution_context.set(ctx)
    try:
        cancelled = await subagents.cancel(
            [
                "child-1",
                JobRef(
                    task_id="child-2",
                    agent_name="child-agent",
                    parent_task_id="parent-1",
                ),
                {"task_id": "child-1"},
            ]
        )
    finally:
        execution_context.reset(token)

    assert cancelled == ["child-1", "child-2"]
    assert cancelled_task_batches == [["child-1", "child-2"]]


@pytest.mark.asyncio
async def test_subagents_cancel_rejects_invalid_target() -> None:
    ctx = ExecutionContext(
        task_id="parent-1",
        owner_id="owner-1",
        retry_count=0,
        events=cast(EventPublisher, _NoopEvents()),
    )
    token = execution_context.set(ctx)
    try:
        with pytest.raises(TypeError, match="expects a task_id string"):
            await subagents.cancel(object())
        with pytest.raises(TypeError, match="expects a task_id string"):
            await subagents.cancel(["child-1", object()])
    finally:
        execution_context.reset(token)


@pytest.mark.asyncio
async def test_subagents_signal_routes_single_target_callback() -> None:
    seen: list[tuple[str, str, Any]] = []

    async def _signal_child(task_id: str, signal_id: str, payload: Any) -> dict[str, Any]:
        seen.append((task_id, signal_id, payload))
        return {
            "signal_id": signal_id,
            "target_task_ids": [task_id],
            "signaled_task_ids": [task_id],
            "woken_task_ids": [task_id],
            "skipped_inactive_task_ids": [],
            "failed_task_ids": [],
        }

    ctx = ExecutionContext(
        task_id="parent-task",
        owner_id="owner",
        retry_count=0,
        events=cast(EventPublisher, _NoopEvents()),
        subagents=SubagentsExecutionNamespace(signal_callback=_signal_child),
    )
    token = execution_context.set(ctx)
    try:
        report = await subagents.signal(
            "child-task-1",
            signal_id="day_vote_open:1",
            payload={"round": 1},
        )
    finally:
        execution_context.reset(token)

    assert isinstance(report, SignalDeliveryReport)
    assert report.signal_id == "day_vote_open:1"
    assert report.target_task_ids == ["child-task-1"]
    assert report.signaled_task_ids == ["child-task-1"]
    assert report.woken_task_ids == ["child-task-1"]
    assert report.failed_task_ids == []
    assert seen == [("child-task-1", "day_vote_open:1", {"round": 1})]


@pytest.mark.asyncio
async def test_subagents_signal_routes_batch_callback_with_dedupe() -> None:
    seen_batches: list[tuple[list[str], str, Any]] = []

    async def _signal_children(
        task_ids: list[str],
        signal_id: str,
        payload: Any,
    ) -> dict[str, Any]:
        seen_batches.append((list(task_ids), signal_id, payload))
        return {
            "signal_id": signal_id,
            "target_task_ids": list(task_ids),
            "signaled_task_ids": list(task_ids),
            "woken_task_ids": [task_ids[0]],
            "skipped_inactive_task_ids": [task_ids[-1]],
            "failed_task_ids": [],
        }

    ctx = ExecutionContext(
        task_id="parent-task",
        owner_id="owner",
        retry_count=0,
        events=cast(EventPublisher, _NoopEvents()),
        subagents=SubagentsExecutionNamespace(signal_many_callback=_signal_children),
    )
    token = execution_context.set(ctx)
    try:
        report = await subagents.signal(
            ["child-a", "child-b", "child-a"],
            signal_id="night_action_open:5",
            payload={"round": 5},
        )
    finally:
        execution_context.reset(token)

    assert report.target_task_ids == ["child-a", "child-b"]
    assert report.signaled_task_ids == ["child-a", "child-b"]
    assert report.woken_task_ids == ["child-a"]
    assert report.skipped_inactive_task_ids == ["child-b"]
    assert seen_batches == [
        (["child-a", "child-b"], "night_action_open:5", {"round": 5})
    ]

