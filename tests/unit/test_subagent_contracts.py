"""Contracts for subagent spawning and wait-join orchestration."""

from __future__ import annotations

import uuid
from types import SimpleNamespace
from typing import Any, cast

import pytest

from factorial.core.events import EventPublisher
from factorial.execution.context import (
    AgentContext,
    ExecutionContext,
    SubagentsExecutionNamespace,
    execution_context,
)
from factorial.execution.subagents import JobRef, subagents


class _NoopEvents:
    async def publish_event(self, _event: Any) -> None:
        return None


class _DummyChildAgent:
    name = "child-agent"
    context_class = AgentContext


class _InvalidContextChildAgent:
    name = "child-agent"
    context_class = object


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
        retries=0,
        iterations=0,
        events=cast(EventPublisher, _NoopEvents()),
        subagents=SubagentsExecutionNamespace(
            enqueue_callback=_enqueue_child_task,
        ),
    )
    token = execution_context.set(ctx)
    try:
        jobs = await subagents.spawn(
            agent=_DummyChildAgent(),
            inputs=[{"query": "q1"}, {"query": "q2"}],
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
    assert [payload.query for payload in captured_payloads] == ["q1", "q2"]


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
        retries=0,
        iterations=0,
        events=cast(EventPublisher, _NoopEvents()),
        subagents=SubagentsExecutionNamespace(
            enqueue_callback=_enqueue_child_task,
        ),
    )
    token = execution_context.set(ctx)
    try:
        instruction = await subagents.run(
            agent=_DummyChildAgent(),
            inputs=[{"query": "q1"}],
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
        retries=0,
        iterations=2,
        events=cast(EventPublisher, _NoopEvents()),
        subagents=SubagentsExecutionNamespace(
            enqueue_callback=_enqueue_child_task,
        ),
    )
    token = execution_context.set(ctx)
    try:
        first = await subagents.spawn(
            agent=_DummyChildAgent(),
            inputs=[{"query": "q1"}, {"query": "q2"}],
            key="research",
        )
        second = await subagents.spawn(
            agent=_DummyChildAgent(),
            inputs=[{"query": "q1"}, {"query": "q2"}],
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
        retries=0,
        iterations=0,
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
                inputs=[{"query": "q1"}],
                key="",
            )
    finally:
        execution_context.reset(token)


@pytest.mark.asyncio
async def test_spawn_strips_key_before_persisting_job_refs() -> None:
    async def _enqueue_child_task(
        _agent: Any,
        _payload: Any,
        task_id: str | None,
    ) -> str:
        assert task_id is not None
        return task_id

    ctx = ExecutionContext(
        task_id="parent-1",
        owner_id="owner-1",
        retries=0,
        iterations=0,
        events=cast(EventPublisher, _NoopEvents()),
        subagents=SubagentsExecutionNamespace(
            enqueue_callback=_enqueue_child_task,
        ),
    )
    token = execution_context.set(ctx)
    try:
        jobs = await subagents.spawn(
            agent=_DummyChildAgent(),
            inputs=[{"query": "q1"}],
            key="  research  ",
        )
    finally:
        execution_context.reset(token)

    assert jobs[0].key == "research"


@pytest.mark.asyncio
async def test_spawn_returns_empty_list_for_empty_inputs() -> None:
    ctx = ExecutionContext(
        task_id="parent-1",
        owner_id="owner-1",
        retries=0,
        iterations=0,
        events=cast(EventPublisher, _NoopEvents()),
    )
    token = execution_context.set(ctx)
    try:
        jobs = await subagents.spawn(
            agent=_DummyChildAgent(), inputs=[], key="research"
        )
    finally:
        execution_context.reset(token)

    assert jobs == []


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
        retries=0,
        iterations=0,
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
async def test_spawn_rejects_agent_with_invalid_context_class() -> None:
    async def _enqueue_child_task(
        _agent: Any,
        _payload: Any,
        task_id: str | None,
    ) -> str:
        return task_id or ""

    ctx = ExecutionContext(
        task_id="parent-1",
        owner_id="owner-1",
        retries=0,
        iterations=0,
        events=cast(EventPublisher, _NoopEvents()),
        subagents=SubagentsExecutionNamespace(
            enqueue_callback=_enqueue_child_task,
        ),
    )
    token = execution_context.set(ctx)
    try:
        with pytest.raises(TypeError, match="invalid context_class"):
            await subagents.spawn(
                agent=_InvalidContextChildAgent(),
                inputs=[{"query": "q1"}],
                key="research",
            )
    finally:
        execution_context.reset(token)


@pytest.mark.asyncio
async def test_spawn_uses_batch_enqueue_with_deterministic_ids() -> None:
    seen_batch_ids: list[str] = []
    seen_task_ids: list[list[str]] = []

    async def _enqueue_batch(
        _agent: Any,
        _payloads: list[Any],
        task_ids: list[str] | None,
        batch_id: str | None,
    ) -> Any:
        assert task_ids is not None
        assert batch_id is not None
        seen_batch_ids.append(batch_id)
        seen_task_ids.append(list(task_ids))
        return SimpleNamespace(task_ids=list(task_ids))

    ctx = ExecutionContext(
        task_id="parent-1",
        owner_id="owner-1",
        retries=0,
        iterations=0,
        events=cast(EventPublisher, _NoopEvents()),
        subagents=SubagentsExecutionNamespace(
            enqueue_batch_callback=_enqueue_batch,
        ),
    )
    token = execution_context.set(ctx)
    try:
        first = await subagents.spawn(
            agent=_DummyChildAgent(),
            inputs=[{"query": "q1"}, {"query": "q2"}],
            key="research",
        )
        second = await subagents.spawn(
            agent=_DummyChildAgent(),
            inputs=[{"query": "q1"}, {"query": "q2"}],
            key="research",
        )
    finally:
        execution_context.reset(token)

    assert len(seen_batch_ids) == 2
    assert seen_batch_ids[0] == seen_batch_ids[1]
    uuid.UUID(seen_batch_ids[0])
    assert seen_task_ids[0] == seen_task_ids[1]
    assert [job.task_id for job in first] == seen_task_ids[0]
    assert [job.task_id for job in second] == seen_task_ids[1]


@pytest.mark.asyncio
async def test_execution_context_subagents_namespace_routes_callbacks() -> None:
    cancelled_task_ids: list[str] = []
    cancelled_task_batches: list[list[str]] = []

    async def _enqueue_child_task(
        _agent: Any,
        _payload: Any,
        task_id: str | None,
    ) -> str:
        return task_id or "generated-child"

    async def _enqueue_batch(
        _agent: Any,
        _payloads: list[Any],
        task_ids: list[str] | None,
        _batch_id: str | None,
    ) -> Any:
        return SimpleNamespace(task_ids=task_ids or [])

    async def _cancel_child(task_id: str) -> None:
        cancelled_task_ids.append(task_id)

    async def _cancel_children(task_ids: list[str]) -> None:
        cancelled_task_batches.append(list(task_ids))

    ctx = ExecutionContext(
        task_id="parent-1",
        owner_id="owner-1",
        retries=0,
        iterations=0,
        events=cast(EventPublisher, _NoopEvents()),
        subagents=SubagentsExecutionNamespace(
            enqueue_callback=_enqueue_child_task,
            enqueue_batch_callback=_enqueue_batch,
            cancel_callback=_cancel_child,
            cancel_many_callback=_cancel_children,
        ),
    )

    child_task_id = await ctx.subagents.enqueue(
        _DummyChildAgent(),
        AgentContext(query="q1"),
        task_id="child-1",
    )
    batch = await ctx.subagents.enqueue_batch(
        _DummyChildAgent(),
        [AgentContext(query="q1"), AgentContext(query="q2")],
        task_ids=["child-1", "child-2"],
        batch_id="batch-1",
    )
    await ctx.subagents.cancel("child-3")
    await ctx.subagents.cancel_many(["child-3", "child-4", "child-3"])

    assert child_task_id == "child-1"
    assert batch.task_ids == ["child-1", "child-2"]
    assert cancelled_task_ids == ["child-3"]
    assert cancelled_task_batches == [["child-3", "child-4"]]


@pytest.mark.asyncio
async def test_subagents_cancel_routes_to_execution_context_callback() -> None:
    cancelled_task_ids: list[str] = []

    async def _cancel_child(task_id: str) -> None:
        cancelled_task_ids.append(task_id)

    ctx = ExecutionContext(
        task_id="parent-1",
        owner_id="owner-1",
        retries=0,
        iterations=0,
        events=cast(EventPublisher, _NoopEvents()),
        subagents=SubagentsExecutionNamespace(cancel_callback=_cancel_child),
    )
    token = execution_context.set(ctx)
    try:
        cancelled = await subagents.cancel("child-1")
    finally:
        execution_context.reset(token)

    assert cancelled == "child-1"
    assert cancelled_task_ids == ["child-1"]


@pytest.mark.asyncio
async def test_subagents_cancel_accepts_job_ref_like_target() -> None:
    cancelled_task_ids: list[str] = []

    async def _cancel_child(task_id: str) -> None:
        cancelled_task_ids.append(task_id)

    ctx = ExecutionContext(
        task_id="parent-1",
        owner_id="owner-1",
        retries=0,
        iterations=0,
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
        retries=0,
        iterations=0,
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
async def test_subagents_cancel_returns_empty_list_for_empty_targets() -> None:
    ctx = ExecutionContext(
        task_id="parent-1",
        owner_id="owner-1",
        retries=0,
        iterations=0,
        events=cast(EventPublisher, _NoopEvents()),
    )
    token = execution_context.set(ctx)
    try:
        cancelled = await subagents.cancel([])
    finally:
        execution_context.reset(token)

    assert cancelled == []


@pytest.mark.asyncio
async def test_subagents_cancel_rejects_invalid_target() -> None:
    ctx = ExecutionContext(
        task_id="parent-1",
        owner_id="owner-1",
        retries=0,
        iterations=0,
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

