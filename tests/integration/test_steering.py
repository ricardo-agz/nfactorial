"""Integration tests for task steering operations."""

import json

import pytest
import redis.asyncio as redis

from factorial.context import AgentContext
from factorial.queue.keys import RedisKeys
from factorial.queue.operations import enqueue_task
from factorial.queue.task import Task, TaskStatus, get_task_status

from .conftest import ScriptRunner, SimpleTestAgent


@pytest.mark.asyncio
class TestSteeringMessageApplication:
    """Tests for steering message application (steering.lua)."""

    async def test_applies_steering_to_processing_task(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_agent: SimpleTestAgent,
        sample_task: Task[AgentContext],
        script_runner: ScriptRunner,
    ) -> None:
        """Test applying steering messages to a processing task."""
        keys = script_runner.keys

        # Enqueue and pickup task
        task_id = await script_runner.enqueue_and_pickup(test_agent, sample_task)

        # Add steering messages
        message_id_1 = "1234567890_abc123"
        message_id_2 = "1234567891_def456"
        task_keys = RedisKeys.format(namespace=test_namespace, task_id=task_id)

        await redis_client.hset(            task_keys.task_steering,
            message_id_1,
            json.dumps({"role": "user", "content": "Please focus on X"}),
        )
        await redis_client.hset(            task_keys.task_steering,
            message_id_2,
            json.dumps({"role": "user", "content": "Also consider Y"}),
        )

        # Apply steering with updated payload
        updated_payload = AgentContext(
            query="Updated query with steering",
            messages=[
                {"role": "user", "content": "Please focus on X"},
                {"role": "user", "content": "Also consider Y"},
            ],
        )

        result = await script_runner.steer(
            task_id=task_id,
            steering_message_ids=[message_id_1, message_id_2],
            payload_json=updated_payload.to_json(),
        )

        assert result.success
        assert result.status == "ok"

        # Verify payload was updated
        payload_json = await redis_client.hget(keys.task_payload, task_id)
        assert payload_json is not None
        payload_data = json.loads(payload_json)
        assert payload_data["query"] == "Updated query with steering"
        assert len(payload_data["messages"]) == 2

        # Verify steering messages were deleted
        remaining_messages = await redis_client.hgetall(task_keys.task_steering)
        assert len(remaining_messages) == 0

    async def test_applies_steering_to_active_task(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_agent: SimpleTestAgent,
        sample_task: Task[AgentContext],
        script_runner: ScriptRunner,
    ) -> None:
        """Test applying steering messages to an active (requeued) task."""
        # Enqueue and pickup task
        task_id = await script_runner.enqueue_and_pickup(test_agent, sample_task)

        # Continue the task (puts it back in queue as ACTIVE)
        from factorial.queue.worker import CompletionAction

        await script_runner.complete(
            task_id=task_id,
            action=CompletionAction.CONTINUE,
            payload_json=sample_task.payload.to_json(),
        )

        # Verify task is active
        status = await get_task_status(redis_client, test_namespace, task_id)
        assert status == TaskStatus.ACTIVE

        # Add and apply steering
        task_keys = RedisKeys.format(namespace=test_namespace, task_id=task_id)
        message_id = "1234567890_abc123"
        await redis_client.hset(            task_keys.task_steering,
            message_id,
            json.dumps({"role": "user", "content": "New direction"}),
        )

        updated_payload = AgentContext(query="Steered query")
        result = await script_runner.steer(
            task_id=task_id,
            steering_message_ids=[message_id],
            payload_json=updated_payload.to_json(),
        )

        assert result.success

    async def test_applies_steering_to_pending_tool_task(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_agent: SimpleTestAgent,
        sample_task: Task[AgentContext],
        script_runner: ScriptRunner,
    ) -> None:
        """Test applying steering messages to a task pending tool results."""
        from factorial.queue.worker import CompletionAction

        # Enqueue and pickup task
        task_id = await script_runner.enqueue_and_pickup(test_agent, sample_task)

        # Put task in pending tool results state
        await script_runner.complete(
            task_id=task_id,
            action=CompletionAction.PENDING_TOOL,
            payload_json=sample_task.payload.to_json(),
            pending_tool_call_ids=["tool_call_1"],
        )

        status = await get_task_status(redis_client, test_namespace, task_id)
        assert status == TaskStatus.PENDING_TOOL_RESULTS

        # Add and apply steering
        task_keys = RedisKeys.format(namespace=test_namespace, task_id=task_id)
        message_id = "1234567890_steering"
        await redis_client.hset(            task_keys.task_steering,
            message_id,
            json.dumps({"role": "user", "content": "Urgent update"}),
        )

        updated_payload = AgentContext(query="Steered while pending")
        result = await script_runner.steer(
            task_id=task_id,
            steering_message_ids=[message_id],
            payload_json=updated_payload.to_json(),
        )

        assert result.success

    async def test_steering_deletes_messages_even_on_failure(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        script_runner: ScriptRunner,
    ) -> None:
        """Test that steering messages are deleted even when task is missing."""
        keys = script_runner.keys
        fake_task_id = "nonexistent-task"
        task_keys = RedisKeys.format(namespace=test_namespace, task_id=fake_task_id)

        # Add steering messages
        message_id = "1234567890_msg"
        await redis_client.hset(            task_keys.task_steering,
            message_id,
            json.dumps({"role": "user", "content": "Message for missing task"}),
        )

        # Apply steering to missing task
        result = await script_runner.steer(
            task_id=fake_task_id,
            steering_message_ids=[message_id],
            payload_json=AgentContext(query="test").to_json(),
        )

        assert not result.success
        assert result.status == "missing"

        # Messages should still be deleted
        remaining = await redis_client.hgetall(task_keys.task_steering)
        assert len(remaining) == 0

        # Task should be in orphaned queue
        orphaned_score = await redis_client.zscore(keys.queue_orphaned, fake_task_id)
        assert orphaned_score is not None

    async def test_steering_multiple_messages_atomically(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_agent: SimpleTestAgent,
        sample_task: Task[AgentContext],
        script_runner: ScriptRunner,
    ) -> None:
        """Test that multiple steering messages are processed atomically."""
        task_id = await script_runner.enqueue_and_pickup(test_agent, sample_task)

        # Add multiple steering messages
        task_keys = RedisKeys.format(namespace=test_namespace, task_id=task_id)
        message_ids = []
        for i in range(10):
            msg_id = f"1234567890_{i:03d}"
            message_ids.append(msg_id)
            await redis_client.hset(                task_keys.task_steering,
                msg_id,
                json.dumps({"role": "user", "content": f"Message {i}"}),
            )

        # Apply all steering messages
        updated_payload = AgentContext(query="All messages applied")
        result = await script_runner.steer(
            task_id=task_id,
            steering_message_ids=message_ids,
            payload_json=updated_payload.to_json(),
        )

        assert result.success

        # All messages should be deleted
        remaining = await redis_client.hgetall(task_keys.task_steering)
        assert len(remaining) == 0

    async def test_steering_partial_message_ids(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_agent: SimpleTestAgent,
        sample_task: Task[AgentContext],
        script_runner: ScriptRunner,
    ) -> None:
        """Test steering with only some of the queued message IDs."""
        task_id = await script_runner.enqueue_and_pickup(test_agent, sample_task)

        # Add multiple steering messages
        task_keys = RedisKeys.format(namespace=test_namespace, task_id=task_id)
        await redis_client.hset(            task_keys.task_steering,
            "msg_1",
            json.dumps({"role": "user", "content": "First message"}),
        )
        await redis_client.hset(            task_keys.task_steering,
            "msg_2",
            json.dumps({"role": "user", "content": "Second message"}),
        )
        await redis_client.hset(            task_keys.task_steering,
            "msg_3",
            json.dumps({"role": "user", "content": "Third message"}),
        )

        # Apply only first two messages
        result = await script_runner.steer(
            task_id=task_id,
            steering_message_ids=["msg_1", "msg_2"],
            payload_json=AgentContext(query="Partial steering").to_json(),
        )

        assert result.success

        # Only specified messages should be deleted
        remaining = await redis_client.hgetall(task_keys.task_steering)
        assert len(remaining) == 1
        assert "msg_3" in remaining

    async def test_steering_empty_message_ids_list(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_agent: SimpleTestAgent,
        sample_task: Task[AgentContext],
        script_runner: ScriptRunner,
    ) -> None:
        """Test steering with empty message IDs list still updates payload."""
        keys = script_runner.keys
        task_id = await script_runner.enqueue_and_pickup(test_agent, sample_task)

        # Apply steering with no message IDs
        updated_payload = AgentContext(query="Updated without messages")
        result = await script_runner.steer(
            task_id=task_id,
            steering_message_ids=[],
            payload_json=updated_payload.to_json(),
        )

        assert result.success

        # Payload should still be updated
        payload_json = await redis_client.hget(keys.task_payload, task_id)
        payload_data = json.loads(payload_json)
        assert payload_data["query"] == "Updated without messages"


@pytest.mark.asyncio
class TestSteeringWithTaskStateValidation:
    """Tests for steering with different task states."""

    async def test_steering_queued_task(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_agent: SimpleTestAgent,
        sample_task: Task[AgentContext],
        script_runner: ScriptRunner,
    ) -> None:
        """Test steering a queued (not yet picked up) task."""
        # Enqueue but don't pickup
        task_id = await enqueue_task(
            redis_client=redis_client,
            namespace=test_namespace,
            agent=test_agent,
            task=sample_task,
        )

        task_keys = RedisKeys.format(namespace=test_namespace, task_id=task_id)
        message_id = "steering_queued"
        await redis_client.hset(            task_keys.task_steering,
            message_id,
            json.dumps({"role": "user", "content": "Steer before pickup"}),
        )

        result = await script_runner.steer(
            task_id=task_id,
            steering_message_ids=[message_id],
            payload_json=AgentContext(query="Steered while queued").to_json(),
        )

        assert result.success

    async def test_steering_backoff_task(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_agent: SimpleTestAgent,
        sample_task: Task[AgentContext],
        script_runner: ScriptRunner,
    ) -> None:
        """Test steering a task in backoff state."""
        from factorial.queue.worker import CompletionAction

        task_id = await script_runner.enqueue_and_pickup(test_agent, sample_task)

        # Put in backoff
        await script_runner.complete(
            task_id=task_id,
            action=CompletionAction.BACKOFF,
            payload_json=sample_task.payload.to_json(),
        )

        status = await get_task_status(redis_client, test_namespace, task_id)
        assert status == TaskStatus.BACKOFF

        # Add and apply steering
        task_keys = RedisKeys.format(namespace=test_namespace, task_id=task_id)
        message_id = "steer_backoff"
        await redis_client.hset(            task_keys.task_steering,
            message_id,
            json.dumps({"role": "user", "content": "Steer during backoff"}),
        )

        result = await script_runner.steer(
            task_id=task_id,
            steering_message_ids=[message_id],
            payload_json=AgentContext(query="Steered in backoff").to_json(),
        )

        assert result.success

    async def test_steering_completed_task(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_agent: SimpleTestAgent,
        sample_task: Task[AgentContext],
        script_runner: ScriptRunner,
    ) -> None:
        """Test steering a completed task - succeeds but has no effect."""
        from factorial.queue.worker import CompletionAction

        task_id = await script_runner.enqueue_and_pickup(test_agent, sample_task)

        # Complete the task
        await script_runner.complete(
            task_id=task_id,
            action=CompletionAction.COMPLETE,
            payload_json=sample_task.payload.to_json(),
            final_output={"result": "done"},
        )

        status = await get_task_status(redis_client, test_namespace, task_id)
        assert status == TaskStatus.COMPLETED

        # Steering a completed task should still work (updates payload)
        task_keys = RedisKeys.format(namespace=test_namespace, task_id=task_id)
        message_id = "steer_completed"
        await redis_client.hset(            task_keys.task_steering,
            message_id,
            json.dumps({"role": "user", "content": "Steer completed task"}),
        )

        result = await script_runner.steer(
            task_id=task_id,
            steering_message_ids=[message_id],
            payload_json=AgentContext(query="Steered completed").to_json(),
        )

        # Script succeeds - it's up to the caller to check if steering makes sense
        assert result.success
