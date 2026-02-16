"""Contracts for subagent spawning and wait-join orchestration."""

from __future__ import annotations

import uuid
from types import SimpleNamespace
from typing import Any, cast

import pytest

from factorial.context import AgentContext, ExecutionContext, execution_context
from factorial.events import EventPublisher
from factorial.subagents import JobRef, subagents


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
        enqueue_child_task=_enqueue_child_task,
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
        enqueue_child_task=_enqueue_child_task,
    )
    token = execution_context.set(ctx)
    try:
        instruction = await subagents.run(
            agent=_DummyChildAgent(),
            inputs=[{"query": "q1"}],
            key="research",
            message="waiting for research",
        )
    finally:
        execution_context.reset(token)

    assert instruction.kind == "jobs"
    assert instruction.child_task_ids == captured_task_ids
    assert instruction.message == "waiting for research"


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
        enqueue_child_task=_enqueue_child_task,
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
        enqueue_child_task=_enqueue_child_task,
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
        enqueue_child_task=_enqueue_child_task,
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
        enqueue_child_task=_enqueue_child_task,
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
        enqueue_child_task=_enqueue_child_task,
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
        enqueue_batch=_enqueue_batch,
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

