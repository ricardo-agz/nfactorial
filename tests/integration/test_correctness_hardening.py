from __future__ import annotations

import json
from typing import Any

import pytest
import redis.asyncio as redis

import factorial._internal.queue.operations.control as control_module
import factorial._internal.queue.worker.processor as processor_module
from factorial._internal.lua.queue import (
    BatchPickupScript,
    TaskCompletionScript,
    TaskSteeringScript,
)
from factorial._internal.queue.keys import RedisKeys
from factorial._internal.queue.operations import (
    cancel_task,
    enqueue_task,
    get_task_batch,
    run_agent_cancellation,
)
from factorial._internal.queue.task_store import get_task_status
from factorial._internal.queue.worker import process_task
from factorial.agent.context import AgentContext
from factorial.core.events import FinishEvent
from factorial.core.run_types import RunStatus
from factorial.queue.task import Task, TaskStatus

from .conftest import SimpleTestAgent


@pytest.mark.asyncio
async def test_parent_resume_rejects_stale_child_result_commit(
    redis_client: redis.Redis,
    test_namespace: str,
    test_owner_id: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    parent_agent = SimpleTestAgent(name="parent_agent")
    parent_task = Task.create(
        owner_id=test_owner_id,
        agent=parent_agent.name,
        payload=AgentContext(messages=[{"role": "user", "content": "wait"}]),
    )
    parent_task_id = await enqueue_task(
        redis_client=redis_client,
        namespace=test_namespace,
        agent=parent_agent,
        task=parent_task,
    )

    parent_keys = RedisKeys.format(
        namespace=test_namespace,
        agent=parent_agent.name,
        task_id=parent_task_id,
    )
    child_task_id = "child-race"
    original_result = json.dumps({"result": "original"})
    raced_result = json.dumps({"result": "raced"})

    await redis_client.hset(
        parent_keys.task_status,
        parent_task_id,
        TaskStatus.PENDING_CHILD_TASKS.value,
    )
    await redis_client.lrem(parent_keys.queue_main, 0, parent_task_id)
    await redis_client.zadd(parent_keys.queue_pending, {parent_task_id: 1.0})
    await redis_client.sadd(parent_keys.pending_child_wait_ids, child_task_id)
    await redis_client.hset(
        parent_keys.pending_child_task_results,
        child_task_id,
        original_result,
    )
    await redis_client.hset(
        parent_keys.task_status,
        child_task_id,
        TaskStatus.COMPLETED.value,
    )

    real_script = await control_module.create_child_task_completion_script(redis_client)

    class RacingChildCompletionScript:
        async def execute(self, **kwargs: Any) -> tuple[bool, str]:
            await redis_client.hset(
                parent_keys.pending_child_task_results,
                child_task_id,
                raced_result,
            )
            return await real_script.execute(**kwargs)

    async def racing_script_factory(_: redis.Redis) -> RacingChildCompletionScript:
        return RacingChildCompletionScript()

    monkeypatch.setattr(
        control_module,
        "create_child_task_completion_script",
        racing_script_factory,
    )

    resumed = await control_module.resume_if_no_remaining_child_tasks(
        redis_client=redis_client,
        namespace=test_namespace,
        agents_by_name={parent_agent.name: parent_agent},
        task_id=parent_task_id,
    )

    assert resumed is False
    assert await get_task_status(
        redis_client,
        test_namespace,
        parent_task_id,
    ) == TaskStatus.PENDING_CHILD_TASKS
    assert await redis_client.hget(
        parent_keys.pending_child_task_results,
        child_task_id,
    ) == raced_result
    assert await redis_client.smembers(parent_keys.pending_child_wait_ids) == {
        child_task_id
    }
    assert await redis_client.llen(parent_keys.queue_main) == 0


@pytest.mark.asyncio
async def test_resume_with_unregistered_parent_agent_is_controlled(
    redis_client: redis.Redis,
    test_namespace: str,
    test_owner_id: str,
) -> None:
    parent_agent = SimpleTestAgent(name="removed_parent_agent")
    parent_task = Task.create(
        owner_id=test_owner_id,
        agent=parent_agent.name,
        payload=AgentContext(messages=[{"role": "user", "content": "wait"}]),
    )
    parent_task_id = await enqueue_task(
        redis_client=redis_client,
        namespace=test_namespace,
        agent=parent_agent,
        task=parent_task,
    )
    parent_keys = RedisKeys.format(
        namespace=test_namespace,
        agent=parent_agent.name,
        task_id=parent_task_id,
    )
    await redis_client.hset(
        parent_keys.task_status,
        parent_task_id,
        TaskStatus.PENDING_CHILD_TASKS.value,
    )
    await redis_client.sadd(parent_keys.pending_child_wait_ids, "child-1")

    assert await control_module.resume_if_no_remaining_child_tasks(
        redis_client=redis_client,
        namespace=test_namespace,
        agents_by_name={},
        task_id=parent_task_id,
    ) is False


@pytest.mark.asyncio
async def test_immediate_cancel_with_unregistered_agent_does_not_crash(
    redis_client: redis.Redis,
    test_namespace: str,
    test_owner_id: str,
) -> None:
    agent = SimpleTestAgent(name="removed_cancel_agent")
    task = Task.create(
        owner_id=test_owner_id,
        agent=agent.name,
        payload=AgentContext(messages=[{"role": "user", "content": "cancel"}]),
    )
    task_id = await enqueue_task(
        redis_client=redis_client,
        namespace=test_namespace,
        agent=agent,
        task=task,
    )
    keys = RedisKeys.format(namespace=test_namespace, agent=agent.name, task_id=task_id)
    await redis_client.hset(
        keys.task_status,
        task_id,
        TaskStatus.PENDING_CHILD_TASKS.value,
    )

    await cancel_task(
        redis_client=redis_client,
        namespace=test_namespace,
        task_id=task_id,
        agents_by_name={},
        metrics_retention_duration=3600,
    )

    assert await get_task_status(redis_client, test_namespace, task_id) == (
        TaskStatus.CANCELLED
    )


@pytest.mark.asyncio
async def test_cancellation_emits_finish_event_when_cleanup_fails(
    redis_client: redis.Redis,
    test_namespace: str,
    test_owner_id: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    agent = SimpleTestAgent(name="cancel_cleanup_agent")
    task = Task.create(
        owner_id=test_owner_id,
        agent=agent.name,
        payload=AgentContext(messages=[{"role": "user", "content": "cancel"}]),
    )
    task_id = await enqueue_task(
        redis_client=redis_client,
        namespace=test_namespace,
        agent=agent,
        task=task,
    )
    emitted_events: list[FinishEvent] = []

    async def fail_destroy_all(_: Any) -> None:
        raise RuntimeError("cleanup failed")

    async def record_event(
        event: Any,
        payload: AgentContext,
        execution_ctx: Any,
    ) -> None:
        del payload, execution_ctx
        if isinstance(event, FinishEvent):
            emitted_events.append(event)

    monkeypatch.setattr(
        control_module.ResourcesExecutionNamespace,
        "destroy_all",
        fail_destroy_all,
    )
    monkeypatch.setattr(agent, "_emit_event", record_event)

    await run_agent_cancellation(
        redis_client=redis_client,
        namespace=test_namespace,
        agent=agent,
        task_id=task_id,
    )

    assert len(emitted_events) == 1
    assert emitted_events[0].status == RunStatus.CANCELLED


@pytest.mark.asyncio
async def test_process_task_handles_resource_context_setup_failure(
    redis_client: redis.Redis,
    test_namespace: str,
    test_owner_id: str,
    pickup_script: BatchPickupScript,
    completion_script: TaskCompletionScript,
    steering_script: TaskSteeringScript,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    agent = SimpleTestAgent(name="resource_setup_agent")
    task = Task.create(
        owner_id=test_owner_id,
        agent=agent.name,
        payload=AgentContext(messages=[{"role": "user", "content": "run"}]),
    )
    task_id = await enqueue_task(
        redis_client=redis_client,
        namespace=test_namespace,
        agent=agent,
        task=task,
    )
    picked, cancelled = await get_task_batch(
        batch_script=pickup_script,
        namespace=test_namespace,
        agent=agent,
        batch_size=1,
        metrics_ttl=3600,
    )
    assert picked == [task_id]
    assert cancelled == []

    class FailingResourceManager:
        def __init__(self, **_: Any) -> None:
            raise RuntimeError("resource manager setup failed")

    monkeypatch.setattr(processor_module, "ResourceManager", FailingResourceManager)

    await process_task(
        redis_client=redis_client,
        namespace=test_namespace,
        task_id=task_id,
        completion_script=completion_script,
        steering_script=steering_script,
        agent=agent,
        agents_by_name={agent.name: agent},
        max_retries=0,
        heartbeat_interval=30,
        task_timeout=30,
        metrics_retention_duration=3600,
    )

    assert await get_task_status(redis_client, test_namespace, task_id) == (
        TaskStatus.FAILED
    )
