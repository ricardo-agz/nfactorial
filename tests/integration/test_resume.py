"""Integration tests for task resumption."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
import redis.asyncio as redis

from factorial.context import AgentContext, VerificationState
from factorial.queue.keys import RedisKeys
from factorial.queue.operations import resume_task
from factorial.queue.task import Task, TaskStatus, get_task_data, get_task_status

from .conftest import SimpleTestAgent


async def _store_task_with_status(
    *,
    redis_client: redis.Redis,
    namespace: str,
    task: Task[AgentContext],
    status: TaskStatus,
) -> None:
    keys = RedisKeys.format(namespace=namespace, agent=task.agent, task_id=task.id)
    await redis_client.hset(keys.task_status, task.id, status.value)
    await redis_client.hset(keys.task_agent, task.id, task.agent)
    await redis_client.hset(keys.task_payload, task.id, task.payload.to_json())
    await redis_client.hset(keys.task_pickups, task.id, task.pickups)
    await redis_client.hset(keys.task_retries, task.id, task.retries)
    await redis_client.hset(keys.task_meta, task.id, task.metadata.to_json())


@pytest.mark.asyncio
class TestResumeTask:
    async def test_resume_task_clones_context_and_enqueues_new_task(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_owner_id: str,
        test_agent: SimpleTestAgent,
    ) -> None:
        source_messages = [
            {"role": "user", "content": "Research the market"},
            {"role": "assistant", "content": "Here is the first draft."},
        ]
        feedback_messages = [
            {
                "role": "user",
                "content": "Revise with clearer citations and risk analysis.",
            }
        ]
        source_ctx = AgentContext(
            query="Market research",
            messages=list(source_messages),
            turn=5,
            output={"summary": "first pass"},
            attempt=2,
            verification=VerificationState(
                attempts_used=2,
                last_candidate_hash="abc123",
                last_outcome="rejected",
            ),
        )
        source_task = Task.create(
            owner_id=test_owner_id,
            agent=test_agent.name,
            payload=source_ctx,
            max_turns=10,
        )
        source_task.metadata.parent_id = "parent-task-1"

        await _store_task_with_status(
            redis_client=redis_client,
            namespace=test_namespace,
            task=source_task,
            status=TaskStatus.COMPLETED,
        )

        resumed_task = await resume_task(
            redis_client=redis_client,
            namespace=test_namespace,
            task_id=source_task.id,
            agent=test_agent,
            messages=feedback_messages,
        )

        assert resumed_task.id != source_task.id
        assert resumed_task.agent == test_agent.name

        source_status = await get_task_status(
            redis_client, test_namespace, source_task.id
        )
        resumed_status = await get_task_status(
            redis_client, test_namespace, resumed_task.id
        )
        assert source_status == TaskStatus.COMPLETED
        assert resumed_status == TaskStatus.QUEUED

        resumed_data = await get_task_data(
            redis_client, test_namespace, resumed_task.id
        )
        resumed_payload = resumed_data["payload"]
        assert resumed_payload["query"] == source_ctx.query
        assert resumed_payload["messages"] == [*source_messages, *feedback_messages]
        assert resumed_payload["turn"] == 0
        assert resumed_payload["output"] is None
        assert resumed_payload["attempt"] == 0
        assert resumed_payload["verification"]["attempts_used"] == 0
        assert resumed_payload["verification"]["last_candidate_hash"] is None
        assert resumed_payload["verification"]["last_outcome"] is None
        assert resumed_data["metadata"]["owner_id"] == test_owner_id
        assert resumed_data["metadata"]["parent_id"] == "parent-task-1"
        assert resumed_data["metadata"]["resumed_from_task_id"] == source_task.id

        resumed_keys = RedisKeys.format(namespace=test_namespace, agent=test_agent.name)
        queued_ids = await redis_client.lrange(resumed_keys.queue_main, 0, -1)
        assert queued_ids == [resumed_task.id]

    async def test_resume_task_idempotency_replays_to_same_task(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_owner_id: str,
        test_agent: SimpleTestAgent,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        captured_events: list[Any] = []

        async def _capture_event(_self: Any, event: Any) -> None:
            captured_events.append(event)

        monkeypatch.setattr(
            "factorial.queue.operations.EventPublisher.publish_event",
            _capture_event,
        )

        source_task = Task.create(
            owner_id=test_owner_id,
            agent=test_agent.name,
            payload=AgentContext(query="idempotent resume"),
        )
        await _store_task_with_status(
            redis_client=redis_client,
            namespace=test_namespace,
            task=source_task,
            status=TaskStatus.COMPLETED,
        )

        first = await resume_task(
            redis_client=redis_client,
            namespace=test_namespace,
            task_id=source_task.id,
            agent=test_agent,
            messages=[{"role": "user", "content": "revise"}],
            idempotency_key="resume-1",
        )
        second = await resume_task(
            redis_client=redis_client,
            namespace=test_namespace,
            task_id=source_task.id,
            agent=test_agent,
            messages=[{"role": "user", "content": "revise"}],
            idempotency_key="resume-1",
        )

        assert first.id == second.id
        keys = RedisKeys.format(namespace=test_namespace, agent=test_agent.name)
        queued_ids = await redis_client.lrange(keys.queue_main, 0, -1)
        assert queued_ids == [first.id]

        assert len(captured_events) == 1
        event = captured_events[0]
        assert event.event_type == "task_resumed"
        assert event.task_id == first.id
        assert event.data is not None
        assert event.data["source_task_id"] == source_task.id
        assert event.data["resumed_task_id"] == first.id
        assert event.data["idempotent_replay"] is False

    async def test_resume_task_idempotency_rejects_conflicting_payload(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_owner_id: str,
        test_agent: SimpleTestAgent,
    ) -> None:
        source_task = Task.create(
            owner_id=test_owner_id,
            agent=test_agent.name,
            payload=AgentContext(query="conflict"),
        )
        await _store_task_with_status(
            redis_client=redis_client,
            namespace=test_namespace,
            task=source_task,
            status=TaskStatus.COMPLETED,
        )

        await resume_task(
            redis_client=redis_client,
            namespace=test_namespace,
            task_id=source_task.id,
            agent=test_agent,
            messages=[{"role": "user", "content": "first"}],
            idempotency_key="resume-conflict",
        )

        with pytest.raises(ValueError, match="idempotency_key conflict"):
            await resume_task(
                redis_client=redis_client,
                namespace=test_namespace,
                task_id=source_task.id,
                agent=test_agent,
                messages=[{"role": "user", "content": "different"}],
                idempotency_key="resume-conflict",
            )

    async def test_resume_task_idempotency_is_concurrency_safe(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_owner_id: str,
        test_agent: SimpleTestAgent,
    ) -> None:
        source_task = Task.create(
            owner_id=test_owner_id,
            agent=test_agent.name,
            payload=AgentContext(query="concurrent resume"),
        )
        await _store_task_with_status(
            redis_client=redis_client,
            namespace=test_namespace,
            task=source_task,
            status=TaskStatus.COMPLETED,
        )

        async def _resume_once() -> Task[Any]:
            return await resume_task(
                redis_client=redis_client,
                namespace=test_namespace,
                task_id=source_task.id,
                agent=test_agent,
                messages=[{"role": "user", "content": "revise concurrently"}],
                idempotency_key="resume-concurrent",
            )

        resumed_tasks = await asyncio.gather(*[_resume_once() for _ in range(5)])
        resumed_ids = {task.id for task in resumed_tasks}
        assert len(resumed_ids) == 1

        keys = RedisKeys.format(namespace=test_namespace, agent=test_agent.name)
        queued_ids = await redis_client.lrange(keys.queue_main, 0, -1)
        assert queued_ids == [resumed_tasks[0].id]

    async def test_resume_task_rejects_non_terminal_source_task(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_owner_id: str,
        test_agent: SimpleTestAgent,
    ) -> None:
        source_task = Task.create(
            owner_id=test_owner_id,
            agent=test_agent.name,
            payload=AgentContext(query="still running"),
        )
        await _store_task_with_status(
            redis_client=redis_client,
            namespace=test_namespace,
            task=source_task,
            status=TaskStatus.ACTIVE,
        )

        with pytest.raises(ValueError, match="terminal"):
            await resume_task(
                redis_client=redis_client,
                namespace=test_namespace,
                task_id=source_task.id,
                agent=test_agent,
                messages=[{"role": "user", "content": "retry"}],
            )

    async def test_resume_task_rejects_agent_mismatch(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_owner_id: str,
        test_agent: SimpleTestAgent,
    ) -> None:
        source_task = Task.create(
            owner_id=test_owner_id,
            agent=test_agent.name,
            payload=AgentContext(query="done"),
        )
        await _store_task_with_status(
            redis_client=redis_client,
            namespace=test_namespace,
            task=source_task,
            status=TaskStatus.FAILED,
        )

        other_agent = SimpleTestAgent(name="other_agent")
        with pytest.raises(ValueError, match="different agent"):
            await resume_task(
                redis_client=redis_client,
                namespace=test_namespace,
                task_id=source_task.id,
                agent=other_agent,
                messages=[{"role": "user", "content": "try again"}],
            )
