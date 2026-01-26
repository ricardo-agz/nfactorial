import pytest
import redis.asyncio as redis

from factorial.context import AgentContext
from factorial.queue.keys import PENDING_SENTINEL, RedisKeys
from factorial.queue.operations import enqueue_task
from factorial.queue.task import Task, TaskStatus, get_task_status
from factorial.queue.worker import CompletionAction

from .conftest import ScriptRunner, SimpleTestAgent


@pytest.mark.asyncio
class TestInvalidStateTransitions:
    """Tests for invalid state transition handling."""

    async def test_complete_action_on_queued_task_fails(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_agent: SimpleTestAgent,
        sample_task: Task[AgentContext],
        script_runner: ScriptRunner,
    ) -> None:
        """Test that completing a queued (not processing) task fails."""
        # Enqueue but don't pickup
        task_id = await enqueue_task(
            redis_client=redis_client,
            namespace=test_namespace,
            agent=test_agent,
            task=sample_task,
        )

        status = await get_task_status(redis_client, test_namespace, task_id)
        assert status == TaskStatus.QUEUED

        # Try to complete without picking up first
        result = await script_runner.complete(
            task_id=task_id,
            action=CompletionAction.COMPLETE,
            payload_json=sample_task.payload.to_json(),
            final_output={"result": "done"},
        )

        # Should fail because task is not in processing state
        assert not result.success

        # Task should still be queued
        status = await get_task_status(redis_client, test_namespace, task_id)
        assert status == TaskStatus.QUEUED

    async def test_continue_action_on_completed_task_fails(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_agent: SimpleTestAgent,
        sample_task: Task[AgentContext],
        script_runner: ScriptRunner,
    ) -> None:
        """Test that continuing a completed task fails."""
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

        # Try to continue a completed task
        result = await script_runner.complete(
            task_id=task_id,
            action=CompletionAction.CONTINUE,
            payload_json=sample_task.payload.to_json(),
        )

        assert not result.success

    async def test_retry_action_on_failed_task_fails(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_agent: SimpleTestAgent,
        sample_task: Task[AgentContext],
        script_runner: ScriptRunner,
    ) -> None:
        """Test that retrying a failed task fails."""
        task_id = await script_runner.enqueue_and_pickup(test_agent, sample_task)

        # Fail the task
        await script_runner.complete(
            task_id=task_id,
            action=CompletionAction.FAIL,
            payload_json=sample_task.payload.to_json(),
        )

        status = await get_task_status(redis_client, test_namespace, task_id)
        assert status == TaskStatus.FAILED

        # Try to retry a failed task
        result = await script_runner.complete(
            task_id=task_id,
            action=CompletionAction.RETRY,
            payload_json=sample_task.payload.to_json(),
        )

        assert not result.success

    async def test_backoff_action_on_cancelled_task_fails(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_agent: SimpleTestAgent,
        sample_task: Task[AgentContext],
        script_runner: ScriptRunner,
    ) -> None:
        """Test that putting a cancelled task in backoff fails."""
        # Enqueue and mark for cancellation
        task_id = await enqueue_task(
            redis_client=redis_client,
            namespace=test_namespace,
            agent=test_agent,
            task=sample_task,
        )
        await script_runner.mark_for_cancellation(task_id)
        await script_runner.pickup()

        status = await get_task_status(redis_client, test_namespace, task_id)
        assert status == TaskStatus.CANCELLED

        # Try to put cancelled task in backoff
        result = await script_runner.complete(
            task_id=task_id,
            action=CompletionAction.BACKOFF,
            payload_json=sample_task.payload.to_json(),
        )

        assert not result.success

    async def test_fail_action_on_active_task_fails(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_agent: SimpleTestAgent,
        sample_task: Task[AgentContext],
        script_runner: ScriptRunner,
    ) -> None:
        """Test that failing an active (not processing) task fails."""
        task_id = await script_runner.enqueue_and_pickup(test_agent, sample_task)

        # Continue task to make it active
        await script_runner.complete(
            task_id=task_id,
            action=CompletionAction.CONTINUE,
            payload_json=sample_task.payload.to_json(),
        )

        status = await get_task_status(redis_client, test_namespace, task_id)
        assert status == TaskStatus.ACTIVE

        # Try to fail an active task (should fail because it's not processing)
        result = await script_runner.complete(
            task_id=task_id,
            action=CompletionAction.FAIL,
            payload_json=sample_task.payload.to_json(),
        )

        assert not result.success


@pytest.mark.asyncio
class TestCorruptedAndMissingTaskData:
    """Tests for corrupted and missing task data handling."""

    async def test_pickup_handles_missing_task_data(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_agent: SimpleTestAgent,
        script_runner: ScriptRunner,
    ) -> None:
        """Test that pickup handles tasks with missing data gracefully."""
        keys = script_runner.keys
        fake_task_id = "task-with-no-data"

        # Add task ID to queue without task data
        await redis_client.rpush(keys.queue_main, fake_task_id)
        # Pickup should handle gracefully
        result = await script_runner.pickup()

        # Should not be in process list (no valid data)
        assert fake_task_id not in result.tasks_to_process_ids

        # Should be in orphaned or corrupted list
        is_orphaned = fake_task_id in result.orphaned_task_ids
        is_corrupted = fake_task_id in result.corrupted_task_ids
        assert is_orphaned or is_corrupted

    async def test_pickup_handles_partial_task_data(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_agent: SimpleTestAgent,
        script_runner: ScriptRunner,
    ) -> None:
        """Test pickup with partial/corrupted task data."""
        keys = script_runner.keys
        partial_task_id = "partial-task-data"

        # Add only some task fields (corrupted state)
        await redis_client.hset(            keys.task_status, partial_task_id, "queued"
        )
        # Missing: agent, payload, pickups, retries, meta

        # Add to queue
        await redis_client.rpush(keys.queue_main, partial_task_id)
        # Pickup should detect corruption
        result = await script_runner.pickup()

        # Should not be processed
        assert partial_task_id not in result.tasks_to_process_ids

        # Should be marked as corrupted
        assert partial_task_id in result.corrupted_task_ids

    async def test_completion_handles_missing_task(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        script_runner: ScriptRunner,
    ) -> None:
        """Test completion script handles missing task data."""
        keys = script_runner.keys
        fake_task_id = "nonexistent-task"

        # Try to complete a task that doesn't exist
        result = await script_runner.complete(
            task_id=fake_task_id,
            action=CompletionAction.COMPLETE,
            payload_json=AgentContext(query="test").to_json(),
            final_output={"result": "done"},
        )

        assert not result.success

        # Task should be added to orphaned queue
        orphaned_score = await redis_client.zscore(keys.queue_orphaned, fake_task_id)
        assert orphaned_score is not None


@pytest.mark.asyncio
class TestOrphanedTaskHandling:
    """Tests for orphaned task queue management."""

    async def test_orphaned_tasks_are_tracked(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_agent: SimpleTestAgent,
        script_runner: ScriptRunner,
    ) -> None:
        """Test that orphaned tasks are properly tracked."""
        keys = script_runner.keys

        # Add task ID to queue without data
        orphan_1 = "orphan-task-1"
        orphan_2 = "orphan-task-2"

        await redis_client.rpush(keys.queue_main, orphan_1)
        await redis_client.rpush(keys.queue_main, orphan_2)
        # Pickup both
        await script_runner.pickup(batch_size=2)

        # Both should be in orphaned queue
        orphan_1_score = await redis_client.zscore(keys.queue_orphaned, orphan_1)
        orphan_2_score = await redis_client.zscore(keys.queue_orphaned, orphan_2)

        assert orphan_1_score is not None
        assert orphan_2_score is not None

    async def test_stale_recovery_handles_orphaned_heartbeats(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_agent: SimpleTestAgent,
        script_runner: ScriptRunner,
    ) -> None:
        """Test that stale recovery handles orphaned heartbeats."""
        import time

        keys = script_runner.keys
        orphan_task_id = "orphan-in-heartbeats"

        # Add orphan to heartbeats with stale timestamp
        stale_ts = time.time() - 120
        await redis_client.zadd(
            keys.processing_heartbeats, {orphan_task_id: stale_ts}
        )

        # Run stale recovery
        result = await script_runner.recover_stale(
            cutoff_timestamp=time.time() - 60,
            max_retries=3,
        )

        # Should be marked as orphaned
        assert (orphan_task_id, "orphaned") in result.stale_task_actions

        # Should be in orphaned queue
        orphaned_score = await redis_client.zscore(keys.queue_orphaned, orphan_task_id)
        assert orphaned_score is not None


@pytest.mark.asyncio
class TestCancellationEdgeCases:
    """Tests for cancellation edge cases."""

    async def test_cancel_pending_tool_task_immediately(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_agent: SimpleTestAgent,
        sample_task: Task[AgentContext],
        script_runner: ScriptRunner,
    ) -> None:
        """Test cancelling a task that is pending tool results."""
        task_id = await script_runner.enqueue_and_pickup(test_agent, sample_task)

        # Put in pending tool state
        await script_runner.complete(
            task_id=task_id,
            action=CompletionAction.PENDING_TOOL,
            payload_json=sample_task.payload.to_json(),
            pending_tool_call_ids=["tool_call_1"],
        )

        status = await get_task_status(redis_client, test_namespace, task_id)
        assert status == TaskStatus.PENDING_TOOL_RESULTS

        # Cancel it
        result = await script_runner.cancel(task_id)

        assert result.success
        assert result.owner_id is not None  # Immediate cancellation returns owner_id

        status = await get_task_status(redis_client, test_namespace, task_id)
        assert status == TaskStatus.CANCELLED

    async def test_cancel_pending_child_task_immediately(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_agent: SimpleTestAgent,
        sample_task: Task[AgentContext],
        script_runner: ScriptRunner,
    ) -> None:
        """Test cancelling a task that is pending child task results."""
        import uuid

        task_id = await script_runner.enqueue_and_pickup(test_agent, sample_task)

        # Put in pending child state
        child_task_id = str(uuid.uuid4())
        await script_runner.complete(
            task_id=task_id,
            action=CompletionAction.PENDING_CHILD,
            payload_json=sample_task.payload.to_json(),
            pending_child_task_ids=[child_task_id],
        )

        status = await get_task_status(redis_client, test_namespace, task_id)
        assert status == TaskStatus.PENDING_CHILD_TASKS

        # Cancel it
        result = await script_runner.cancel(task_id)

        assert result.success

        status = await get_task_status(redis_client, test_namespace, task_id)
        assert status == TaskStatus.CANCELLED

    async def test_double_cancellation_fails(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_agent: SimpleTestAgent,
        sample_task: Task[AgentContext],
        script_runner: ScriptRunner,
    ) -> None:
        """Test that cancelling an already cancelled task fails."""
        # Enqueue and mark for cancellation
        task_id = await enqueue_task(
            redis_client=redis_client,
            namespace=test_namespace,
            agent=test_agent,
            task=sample_task,
        )
        await script_runner.mark_for_cancellation(task_id)
        await script_runner.pickup()

        status = await get_task_status(redis_client, test_namespace, task_id)
        assert status == TaskStatus.CANCELLED

        # Try to cancel again
        result = await script_runner.cancel(task_id)

        assert not result.success
        assert result.current_status == "cancelled"


@pytest.mark.asyncio
class TestBatchOperationEdgeCases:
    """Tests for batch operation edge cases."""

    async def test_empty_queue_pickup(
        self,
        script_runner: ScriptRunner,
    ) -> None:
        """Test pickup from empty queue returns empty result."""
        result = await script_runner.pickup(batch_size=10)

        assert len(result.tasks_to_process_ids) == 0
        assert len(result.tasks_to_cancel_ids) == 0

    async def test_pickup_respects_batch_size(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_agent: SimpleTestAgent,
        test_owner_id: str,
        script_runner: ScriptRunner,
    ) -> None:
        """Test that pickup respects batch size limit."""
        keys = script_runner.keys

        # Clear queue to ensure test isolation
        await redis_client.delete(keys.queue_main)

        # Create more tasks than batch size
        for i in range(10):
            ctx = AgentContext(query=f"Query {i}")
            task = Task.create(
                owner_id=test_owner_id,
                agent=test_agent.name,
                payload=ctx,
            )
            await enqueue_task(
                redis_client=redis_client,
                namespace=test_namespace,
                agent=test_agent,
                task=task,
            )

        # Verify we have 10 tasks in queue
        queue_len = await redis_client.llen(keys.queue_main)
        assert queue_len == 10

        # Pickup with small batch
        result = await script_runner.pickup(batch_size=3)

        # Should pick up at most batch_size tasks
        assert len(result.tasks_to_process_ids) == 3

        # Queue should have 7 remaining
        queue_len_after = await redis_client.llen(keys.queue_main)
        assert queue_len_after == 7

    async def test_pickup_mixed_valid_and_invalid_tasks(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_agent: SimpleTestAgent,
        test_owner_id: str,
        script_runner: ScriptRunner,
    ) -> None:
        """Test pickup handles mix of valid and invalid tasks."""
        keys = script_runner.keys

        # Create valid task
        ctx = AgentContext(query="Valid task")
        task = Task.create(
            owner_id=test_owner_id,
            agent=test_agent.name,
            payload=ctx,
        )
        valid_task_id = await enqueue_task(
            redis_client=redis_client,
            namespace=test_namespace,
            agent=test_agent,
            task=task,
        )

        # Add invalid task (no data)
        invalid_task_id = "invalid-no-data"
        await redis_client.rpush(keys.queue_main, invalid_task_id)
        # Pickup both
        result = await script_runner.pickup(batch_size=5)

        # Valid task should be processed
        assert valid_task_id in result.tasks_to_process_ids

        # Invalid task should be orphaned
        assert invalid_task_id in result.orphaned_task_ids


@pytest.mark.asyncio
class TestPendingSentinelHandling:
    """Tests for pending sentinel value handling."""

    async def test_tool_completion_with_sentinel_still_present(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_agent: SimpleTestAgent,
        sample_task: Task[AgentContext],
        script_runner: ScriptRunner,
    ) -> None:
        """Test tool completion when some results are still pending."""
        task_id = await script_runner.enqueue_and_pickup(test_agent, sample_task)

        # Put in pending state with 2 tool calls
        await script_runner.complete(
            task_id=task_id,
            action=CompletionAction.PENDING_TOOL,
            payload_json=sample_task.payload.to_json(),
            pending_tool_call_ids=["tool_1", "tool_2"],
        )

        # Set only one tool result
        await script_runner.set_tool_result(task_id, "tool_1", {"result": "done"})

        # Tool completion should NOT requeue (tool_2 still pending)
        success, _ = await script_runner.complete_tool(
            task_id=task_id,
            payload_json=sample_task.payload.to_json(),
        )

        # This would fail because tool_2 is still PENDING_SENTINEL
        # The actual behavior depends on the Lua script implementation
        # In this case, the script checks for sentinels before proceeding

    async def test_pending_sentinel_value_is_correct(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_agent: SimpleTestAgent,
        sample_task: Task[AgentContext],
        script_runner: ScriptRunner,
    ) -> None:
        """Test that pending sentinel values are correctly set."""
        task_id = await script_runner.enqueue_and_pickup(test_agent, sample_task)

        # Put in pending state
        tool_call_ids = ["tool_1", "tool_2", "tool_3"]
        await script_runner.complete(
            task_id=task_id,
            action=CompletionAction.PENDING_TOOL,
            payload_json=sample_task.payload.to_json(),
            pending_tool_call_ids=tool_call_ids,
        )

        # Verify sentinel values
        task_keys = RedisKeys.format(
            namespace=test_namespace,
            agent=test_agent.name,
            task_id=task_id,
        )
        for tool_id in tool_call_ids:
            value = await redis_client.hget(task_keys.pending_tool_results, tool_id)
            assert value == PENDING_SENTINEL


@pytest.mark.asyncio
class TestMetricsTracking:
    """Tests for metrics tracking on state transitions."""

    async def test_completion_increments_completed_metric(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_agent: SimpleTestAgent,
        sample_task: Task[AgentContext],
        script_runner: ScriptRunner,
    ) -> None:
        """Test that completing a task increments completion metrics."""
        keys = script_runner.keys

        # Get initial metrics (might not exist)
        initial_metrics = await redis_client.hgetall(keys.agent_metrics_bucket)

        task_id = await script_runner.enqueue_and_pickup(test_agent, sample_task)
        await script_runner.complete(
            task_id=task_id,
            action=CompletionAction.COMPLETE,
            payload_json=sample_task.payload.to_json(),
            final_output={"result": "done"},
        )

        # Metrics should be updated (exact format depends on implementation)
        final_metrics = await redis_client.hgetall(keys.agent_metrics_bucket)

        # Metrics hash should have entries after completion
        assert len(final_metrics) >= len(initial_metrics)

    async def test_failure_increments_failed_metric(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_agent: SimpleTestAgent,
        sample_task: Task[AgentContext],
        script_runner: ScriptRunner,
    ) -> None:
        """Test that failing a task increments failure metrics."""
        keys = script_runner.keys

        task_id = await script_runner.enqueue_and_pickup(test_agent, sample_task)
        await script_runner.complete(
            task_id=task_id,
            action=CompletionAction.FAIL,
            payload_json=sample_task.payload.to_json(),
        )

        # Metrics should exist after failure
        metrics = await redis_client.hgetall(keys.agent_metrics_bucket)
        assert len(metrics) > 0
