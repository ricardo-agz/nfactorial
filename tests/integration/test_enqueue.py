import json
import uuid
from typing import Any, cast

import pytest
import redis.asyncio as redis

from factorial.context import AgentContext
from factorial.queue.keys import RedisKeys
from factorial.queue.lua import EnqueueTaskScript
from factorial.queue.operations import create_batch_and_enqueue, enqueue_task
from factorial.queue.task import Task, TaskStatus, get_task_data, get_task_status

from .conftest import SimpleTestAgent


@pytest.mark.asyncio
class TestEnqueueTask:
    """Tests for single task enqueue operations."""

    async def test_enqueue_task_creates_queued_status(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_agent: SimpleTestAgent,
        sample_task: Task[AgentContext],
    ) -> None:
        """Test that enqueuing a task sets status to QUEUED."""
        task_id = await enqueue_task(
            redis_client=redis_client,
            namespace=test_namespace,
            agent=test_agent,
            task=sample_task,
        )

        status = await get_task_status(redis_client, test_namespace, task_id)
        assert status == TaskStatus.QUEUED

    async def test_enqueue_task_adds_to_queue(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_agent: SimpleTestAgent,
        sample_task: Task[AgentContext],
    ) -> None:
        """Test that enqueuing a task adds it to the agent queue."""
        keys = RedisKeys.format(namespace=test_namespace, agent=test_agent.name)

        task_id = await enqueue_task(
            redis_client=redis_client,
            namespace=test_namespace,
            agent=test_agent,
            task=sample_task,
        )

        # Check task is in the main queue
        queue_length = await cast(Any, redis_client.llen(keys.queue_main))
        assert queue_length == 1

        queued_task_id = await cast(Any, redis_client.lindex(keys.queue_main, 0))
        assert queued_task_id == task_id

    async def test_enqueue_task_stores_payload(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_agent: SimpleTestAgent,
        sample_task: Task[AgentContext],
    ) -> None:
        """Test that enqueuing a task stores the payload."""
        task_id = await enqueue_task(
            redis_client=redis_client,
            namespace=test_namespace,
            agent=test_agent,
            task=sample_task,
        )

        task_data = await get_task_data(redis_client, test_namespace, task_id)

        assert task_data["payload"]["query"] == sample_task.payload.query
        assert task_data["agent"] == test_agent.name

    async def test_enqueue_task_stores_metadata(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_agent: SimpleTestAgent,
        sample_task: Task[AgentContext],
        test_owner_id: str,
    ) -> None:
        """Test that enqueuing a task stores metadata correctly."""
        task_id = await enqueue_task(
            redis_client=redis_client,
            namespace=test_namespace,
            agent=test_agent,
            task=sample_task,
        )

        task_data = await get_task_data(redis_client, test_namespace, task_id)

        assert task_data["metadata"]["owner_id"] == test_owner_id
        assert task_data["pickups"] == 0
        assert task_data["retries"] == 0

    async def test_enqueue_multiple_tasks(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_agent: SimpleTestAgent,
        test_owner_id: str,
    ) -> None:
        """Test enqueuing multiple tasks."""
        keys = RedisKeys.format(namespace=test_namespace, agent=test_agent.name)

        task_ids = []
        for i in range(5):
            ctx = AgentContext(query=f"Query {i}")
            task = Task.create(
                owner_id=test_owner_id,
                agent=test_agent.name,
                payload=ctx,
            )
            task_id = await enqueue_task(
                redis_client=redis_client,
                namespace=test_namespace,
                agent=test_agent,
                task=task,
            )
            task_ids.append(task_id)

        # Check all tasks are in queue
        queue_length = await cast(Any, redis_client.llen(keys.queue_main))
        assert queue_length == 5

        # Verify each task exists
        for task_id in task_ids:
            status = await get_task_status(redis_client, test_namespace, task_id)
            assert status == TaskStatus.QUEUED

    async def test_enqueue_preserves_task_id(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_agent: SimpleTestAgent,
        sample_task: Task[AgentContext],
    ) -> None:
        """Test that the original task ID is preserved."""
        original_id = sample_task.id

        returned_id = await enqueue_task(
            redis_client=redis_client,
            namespace=test_namespace,
            agent=test_agent,
            task=sample_task,
        )

        assert returned_id == original_id

    async def test_enqueue_fifo_order(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_agent: SimpleTestAgent,
        test_owner_id: str,
    ) -> None:
        """Test that tasks are enqueued in FIFO order (RPUSH)."""
        keys = RedisKeys.format(namespace=test_namespace, agent=test_agent.name)

        task_ids = []
        for i in range(3):
            ctx = AgentContext(query=f"Query {i}")
            task = Task.create(
                owner_id=test_owner_id,
                agent=test_agent.name,
                payload=ctx,
            )
            task_id = await enqueue_task(
                redis_client=redis_client,
                namespace=test_namespace,
                agent=test_agent,
                task=task,
            )
            task_ids.append(task_id)

        # First task should be at the front (index 0 from left)
        first_in_queue = await cast(Any, redis_client.lindex(keys.queue_main, 0))
        assert first_in_queue == task_ids[0]


@pytest.mark.asyncio
class TestEnqueueBatch:
    """Tests for batch task enqueue operations."""

    async def test_batch_enqueue_creates_all_tasks(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_agent: SimpleTestAgent,
        test_owner_id: str,
    ) -> None:
        """Test that batch enqueue creates all tasks."""
        payloads = [
            AgentContext(query=f"Batch query {i}")
            for i in range(5)
        ]

        batch = await create_batch_and_enqueue(
            redis_client=redis_client,
            namespace=test_namespace,
            agent=test_agent,
            payloads=payloads,
            owner_id=test_owner_id,
        )

        assert len(batch.task_ids) == 5

        # Verify all tasks are queued
        for task_id in batch.task_ids:
            status = await get_task_status(redis_client, test_namespace, task_id)
            assert status == TaskStatus.QUEUED

    async def test_batch_enqueue_creates_batch_metadata(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_agent: SimpleTestAgent,
        test_owner_id: str,
    ) -> None:
        """Test that batch enqueue creates batch metadata."""
        payloads = [AgentContext(query=f"Query {i}") for i in range(3)]

        batch = await create_batch_and_enqueue(
            redis_client=redis_client,
            namespace=test_namespace,
            agent=test_agent,
            payloads=payloads,
            owner_id=test_owner_id,
        )

        assert batch.id is not None
        assert batch.metadata.owner_id == test_owner_id
        assert batch.metadata.total_tasks == 3
        assert batch.metadata.status == "active"
        assert batch.progress == 0.0

    async def test_batch_tasks_reference_batch_id(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_agent: SimpleTestAgent,
        test_owner_id: str,
    ) -> None:
        """Test that batch tasks reference the batch ID."""
        payloads = [AgentContext(query="Test query")]

        batch = await create_batch_and_enqueue(
            redis_client=redis_client,
            namespace=test_namespace,
            agent=test_agent,
            payloads=payloads,
            owner_id=test_owner_id,
        )

        task_data = await get_task_data(
            redis_client, test_namespace, batch.task_ids[0]
        )
        assert task_data["metadata"]["batch_id"] == batch.id

    async def test_batch_with_parent_id(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_agent: SimpleTestAgent,
        test_owner_id: str,
    ) -> None:
        """Test batch enqueue with parent task ID."""
        parent_id = str(uuid.uuid4())
        payloads = [AgentContext(query="Child query")]

        batch = await create_batch_and_enqueue(
            redis_client=redis_client,
            namespace=test_namespace,
            agent=test_agent,
            payloads=payloads,
            owner_id=test_owner_id,
            parent_id=parent_id,
        )

        # Child tasks should reference the parent
        task_data = await get_task_data(
            redis_client, test_namespace, batch.task_ids[0]
        )
        assert task_data["metadata"]["parent_id"] == parent_id

    async def test_batch_remaining_tasks_initialized(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_agent: SimpleTestAgent,
        test_owner_id: str,
    ) -> None:
        """Test that batch remaining tasks list is properly initialized."""
        payloads = [AgentContext(query=f"Query {i}") for i in range(3)]

        batch = await create_batch_and_enqueue(
            redis_client=redis_client,
            namespace=test_namespace,
            agent=test_agent,
            payloads=payloads,
            owner_id=test_owner_id,
        )

        assert len(batch.remaining_task_ids) == 3
        assert set(batch.remaining_task_ids) == set(batch.task_ids)

    async def test_batch_retry_does_not_reset_remaining_tasks(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_agent: SimpleTestAgent,
        test_owner_id: str,
    ) -> None:
        """Re-enqueueing an existing deterministic batch preserves progress state."""
        payloads = [AgentContext(query=f"Query {i}") for i in range(2)]
        task_ids = [str(uuid.uuid4()), str(uuid.uuid4())]
        batch_id = str(uuid.uuid4())

        await create_batch_and_enqueue(
            redis_client=redis_client,
            namespace=test_namespace,
            agent=test_agent,
            payloads=payloads,
            owner_id=test_owner_id,
            task_ids=task_ids,
            batch_id=batch_id,
        )

        keys = RedisKeys.format(namespace=test_namespace, agent=test_agent.name)
        await cast(
            Any,
            redis_client.hset(
                keys.batch_remaining_tasks,
                batch_id,
                json.dumps([task_ids[1]]),
            ),
        )

        await create_batch_and_enqueue(
            redis_client=redis_client,
            namespace=test_namespace,
            agent=test_agent,
            payloads=payloads,
            owner_id=test_owner_id,
            task_ids=task_ids,
            batch_id=batch_id,
        )

        remaining_json = await cast(
            Any,
            redis_client.hget(keys.batch_remaining_tasks, batch_id),
        )
        assert remaining_json is not None
        assert json.loads(remaining_json) == [task_ids[1]]

        queue_ids = await cast(Any, redis_client.lrange(keys.queue_main, 0, -1))
        assert queue_ids.count(task_ids[0]) == 1
        assert queue_ids.count(task_ids[1]) == 1


@pytest.mark.asyncio
class TestEnqueueScript:
    """Direct tests for the enqueue Lua script."""

    async def test_lua_enqueue_sets_all_fields(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_agent: SimpleTestAgent,
        enqueue_script: EnqueueTaskScript,
        test_owner_id: str,
    ) -> None:
        """Test that the Lua enqueue script sets all required fields."""
        keys = RedisKeys.format(namespace=test_namespace, agent=test_agent.name)
        task_id = str(uuid.uuid4())
        payload = AgentContext(query="Test query")
        metadata = {
            "owner_id": test_owner_id,
            "parent_id": None,
            "batch_id": None,
            "created_at": 1234567890.0,
            "max_turns": 10,
        }

        await enqueue_script.execute(
            agent_queue_key=keys.queue_main,
            task_statuses_key=keys.task_status,
            task_agents_key=keys.task_agent,
            task_payloads_key=keys.task_payload,
            task_pickups_key=keys.task_pickups,
            task_retries_key=keys.task_retries,
            task_metas_key=keys.task_meta,
            task_id=task_id,
            task_agent=test_agent.name,
            task_payload_json=payload.to_json(),
            task_pickups=0,
            task_retries=0,
            task_meta_json=json.dumps(metadata),
        )

        # Verify all fields were set
        status = await cast(Any, redis_client.hget(keys.task_status, task_id))
        assert status == "queued"

        agent = await cast(Any, redis_client.hget(keys.task_agent, task_id))
        assert agent == test_agent.name

        pickups = await cast(Any, redis_client.hget(keys.task_pickups, task_id))
        assert int(pickups) == 0

        retries = await cast(Any, redis_client.hget(keys.task_retries, task_id))
        assert int(retries) == 0
