import time

import pytest
import redis.asyncio as redis

from factorial.context import AgentContext
from factorial.queue.operations import enqueue_task
from factorial.queue.task import Task, TaskStatus, get_task_status
from factorial.queue.worker import CompletionAction

from .conftest import ScriptRunner, SimpleTestAgent


@pytest.mark.asyncio
class TestBackoffRecovery:
    """Tests for backoff queue recovery (backoff.lua)."""

    async def test_recovers_task_after_backoff_expires(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_agent: SimpleTestAgent,
        sample_task: Task[AgentContext],
        script_runner: ScriptRunner,
    ) -> None:
        """Test that tasks in backoff are recovered when their time expires."""
        keys = script_runner.keys

        # Enqueue and pickup task
        task_id = await enqueue_task(
            redis_client=redis_client,
            namespace=test_namespace,
            agent=test_agent,
            task=sample_task,
        )
        await script_runner.pickup()

        # Put task into backoff state
        await script_runner.complete(
            task_id=task_id,
            action=CompletionAction.BACKOFF,
            payload_json=sample_task.payload.to_json(),
        )

        # Verify task is in backoff
        status = await get_task_status(redis_client, test_namespace, task_id)
        assert status == TaskStatus.BACKOFF

        # Simulate backoff time expiring by setting score to past
        await redis_client.zadd(keys.queue_backoff, {task_id: time.time() - 10})
        # Run backoff recovery
        recovered_ids = await script_runner.recover_backoff(max_batch_size=10)

        assert task_id in recovered_ids

        # Verify task is now active and in main queue
        status = await get_task_status(redis_client, test_namespace, task_id)
        assert status == TaskStatus.ACTIVE

        queue_length = await redis_client.llen(keys.queue_main)
        assert queue_length == 1

    async def test_does_not_recover_task_still_in_backoff(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_agent: SimpleTestAgent,
        sample_task: Task[AgentContext],
        script_runner: ScriptRunner,
    ) -> None:
        """Test that tasks with future backoff time are not recovered."""
        # Enqueue and pickup task
        task_id = await enqueue_task(
            redis_client=redis_client,
            namespace=test_namespace,
            agent=test_agent,
            task=sample_task,
        )
        await script_runner.pickup()

        # Put task into backoff state
        await script_runner.complete(
            task_id=task_id,
            action=CompletionAction.BACKOFF,
            payload_json=sample_task.payload.to_json(),
        )

        # Backoff score is in the future by default, so recovery should return empty
        recovered_ids = await script_runner.recover_backoff(max_batch_size=10)

        assert task_id not in recovered_ids

        # Task should still be in backoff
        status = await get_task_status(redis_client, test_namespace, task_id)
        assert status == TaskStatus.BACKOFF

    async def test_recovers_multiple_tasks_in_batch(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_agent: SimpleTestAgent,
        test_owner_id: str,
        script_runner: ScriptRunner,
    ) -> None:
        """Test recovering multiple tasks in one batch."""
        keys = script_runner.keys
        task_ids = []

        # Create and backoff multiple tasks
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
            await script_runner.pickup()
            await script_runner.complete(
                task_id=task_id,
                action=CompletionAction.BACKOFF,
                payload_json=ctx.to_json(),
            )
            task_ids.append(task_id)

        # Set all backoff scores to past
        for task_id in task_ids:
            await redis_client.zadd(keys.queue_backoff, {task_id: time.time() - 10})
        # Recover all
        recovered_ids = await script_runner.recover_backoff(max_batch_size=10)

        assert len(recovered_ids) == 5
        for task_id in task_ids:
            assert task_id in recovered_ids

    async def test_respects_batch_size_limit(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_agent: SimpleTestAgent,
        test_owner_id: str,
        script_runner: ScriptRunner,
    ) -> None:
        """Test that recovery respects the batch size limit."""
        keys = script_runner.keys

        # Create more tasks than batch size
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
            await script_runner.pickup()
            await script_runner.complete(
                task_id=task_id,
                action=CompletionAction.BACKOFF,
                payload_json=ctx.to_json(),
            )
            await redis_client.zadd(keys.queue_backoff, {task_id: time.time() - 10})
        # Recover with small batch size
        recovered_ids = await script_runner.recover_backoff(max_batch_size=2)

        assert len(recovered_ids) == 2

    async def test_handles_missing_task_data(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_agent: SimpleTestAgent,
        script_runner: ScriptRunner,
    ) -> None:
        """Test that missing task data results in orphaned queue entry."""
        keys = script_runner.keys
        fake_task_id = "nonexistent-task-id"

        # Add a fake task to backoff queue with expired time
        await redis_client.zadd(keys.queue_backoff, {fake_task_id: time.time() - 10})
        # Run recovery - should handle gracefully
        recovered_ids = await script_runner.recover_backoff(max_batch_size=10)

        # Fake task should not be recovered
        assert fake_task_id not in recovered_ids

        # Should be in orphaned queue
        orphaned_score = await redis_client.zscore(keys.queue_orphaned, fake_task_id)
        assert orphaned_score is not None


@pytest.mark.asyncio
class TestStaleTaskRecovery:
    """Tests for stale task recovery (recovery.lua)."""

    async def test_recovers_stale_processing_task(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_agent: SimpleTestAgent,
        sample_task: Task[AgentContext],
        script_runner: ScriptRunner,
    ) -> None:
        """Test recovering a task with stale heartbeat."""
        keys = script_runner.keys

        # Enqueue and pickup task
        task_id = await enqueue_task(
            redis_client=redis_client,
            namespace=test_namespace,
            agent=test_agent,
            task=sample_task,
        )
        await script_runner.pickup()

        # Verify task is processing with a heartbeat
        status = await get_task_status(redis_client, test_namespace, task_id)
        assert status == TaskStatus.PROCESSING

        # Simulate stale heartbeat by setting it to past
        stale_ts = time.time() - 120
        await redis_client.zadd(
            keys.processing_heartbeats, {task_id: stale_ts}
        )

        # Run stale recovery with cutoff 60 seconds ago
        result = await script_runner.recover_stale(
            cutoff_timestamp=time.time() - 60,
            max_retries=3,
        )

        assert result.recovered_count == 1
        assert result.failed_count == 0
        assert (task_id, "recovered") in result.stale_task_actions

        # Task should be active and back in queue
        status = await get_task_status(redis_client, test_namespace, task_id)
        assert status == TaskStatus.ACTIVE

        # Retry count should be incremented
        retries = await redis_client.hget(keys.task_retries, task_id)
        assert int(retries) == 1
    async def test_fails_task_after_max_retries(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_agent: SimpleTestAgent,
        sample_task: Task[AgentContext],
        script_runner: ScriptRunner,
    ) -> None:
        """Test that stale tasks fail when max retries exceeded."""
        keys = script_runner.keys

        # Enqueue and pickup task
        task_id = await enqueue_task(
            redis_client=redis_client,
            namespace=test_namespace,
            agent=test_agent,
            task=sample_task,
        )
        await script_runner.pickup()

        # Set retries to max
        await redis_client.hset(keys.task_retries, task_id, "3")
        # Simulate stale heartbeat
        stale_ts = time.time() - 120
        await redis_client.zadd(
            keys.processing_heartbeats, {task_id: stale_ts}
        )

        # Run recovery with max_retries=3
        result = await script_runner.recover_stale(
            cutoff_timestamp=time.time() - 60,
            max_retries=3,
        )

        assert result.recovered_count == 0
        assert result.failed_count == 1
        assert (task_id, "failed") in result.stale_task_actions

        # Task should be failed
        status = await get_task_status(redis_client, test_namespace, task_id)
        assert status == TaskStatus.FAILED

    async def test_does_not_recover_fresh_heartbeat(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_agent: SimpleTestAgent,
        sample_task: Task[AgentContext],
        script_runner: ScriptRunner,
    ) -> None:
        """Test that tasks with fresh heartbeats are not recovered."""
        # Enqueue and pickup task
        task_id = await enqueue_task(
            redis_client=redis_client,
            namespace=test_namespace,
            agent=test_agent,
            task=sample_task,
        )
        await script_runner.pickup()

        # Heartbeat is fresh (just set by pickup)
        # Run recovery with cutoff in the past - should find nothing
        result = await script_runner.recover_stale(
            cutoff_timestamp=time.time() - 60,
            max_retries=3,
        )

        assert result.recovered_count == 0
        assert result.failed_count == 0

        # Task should still be processing
        status = await get_task_status(redis_client, test_namespace, task_id)
        assert status == TaskStatus.PROCESSING

    async def test_handles_orphaned_task_in_heartbeats(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        script_runner: ScriptRunner,
    ) -> None:
        """Test handling of tasks in heartbeats with no data."""
        keys = script_runner.keys
        fake_task_id = "missing-task-data"

        # Add fake task to heartbeats with stale timestamp
        stale_ts = time.time() - 120
        await redis_client.zadd(
            keys.processing_heartbeats, {fake_task_id: stale_ts}
        )

        # Run recovery
        result = await script_runner.recover_stale(
            cutoff_timestamp=time.time() - 60,
            max_retries=3,
        )

        assert result.recovered_count == 0
        assert (fake_task_id, "orphaned") in result.stale_task_actions

        # Should be in orphaned queue
        orphaned_score = await redis_client.zscore(keys.queue_orphaned, fake_task_id)
        assert orphaned_score is not None

    async def test_respects_batch_limit(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_agent: SimpleTestAgent,
        test_owner_id: str,
        script_runner: ScriptRunner,
    ) -> None:
        """Test that stale recovery respects batch size limit."""
        keys = script_runner.keys

        # Create multiple stale tasks
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
            await script_runner.pickup()
            # Make heartbeat stale
            stale_ts = time.time() - 120
            await redis_client.zadd(
                keys.processing_heartbeats, {task_id: stale_ts}
            )

        # Recover with small batch
        result = await script_runner.recover_stale(
            cutoff_timestamp=time.time() - 60,
            max_recovery_batch=2,
            max_retries=3,
        )

        assert result.recovered_count == 2


@pytest.mark.asyncio
class TestTaskExpiration:
    """Tests for task expiration (expiration.lua)."""

    async def test_expires_old_completed_task(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_agent: SimpleTestAgent,
        sample_task: Task[AgentContext],
        script_runner: ScriptRunner,
    ) -> None:
        """Test that old completed tasks are expired and cleaned up."""
        keys = script_runner.keys

        # Complete a task
        task_id = await script_runner.enqueue_and_pickup(test_agent, sample_task)
        await script_runner.complete(
            task_id=task_id,
            action=CompletionAction.COMPLETE,
            payload_json=sample_task.payload.to_json(),
            final_output={"result": "done"},
        )

        # Set completion timestamp to past
        await redis_client.zadd(keys.queue_completions, {task_id: time.time() - 86400})
        # Run expiration with 1 hour cutoff
        result = await script_runner.expire_tasks(
            completed_cutoff=time.time() - 3600,
            failed_cutoff=time.time() - 3600,
            cancelled_cutoff=time.time() - 3600,
        )

        assert result.completed_cleaned == 1
        assert ("completed", task_id) in result.cleaned_task_details

        # Task data should be deleted
        status = await redis_client.hget(keys.task_status, task_id)
        assert status is None

    async def test_expires_old_failed_task(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_agent: SimpleTestAgent,
        sample_task: Task[AgentContext],
        script_runner: ScriptRunner,
    ) -> None:
        """Test that old failed tasks are expired and cleaned up."""
        keys = script_runner.keys

        # Fail a task
        task_id = await script_runner.enqueue_and_pickup(test_agent, sample_task)
        await script_runner.complete(
            task_id=task_id,
            action=CompletionAction.FAIL,
            payload_json=sample_task.payload.to_json(),
        )

        # Set failure timestamp to past
        await redis_client.zadd(keys.queue_failed, {task_id: time.time() - 86400})
        # Run expiration
        result = await script_runner.expire_tasks(
            completed_cutoff=time.time() - 3600,
            failed_cutoff=time.time() - 3600,
            cancelled_cutoff=time.time() - 3600,
        )

        assert result.failed_cleaned == 1
        assert ("failed", task_id) in result.cleaned_task_details

    async def test_expires_old_cancelled_task(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_agent: SimpleTestAgent,
        sample_task: Task[AgentContext],
        script_runner: ScriptRunner,
    ) -> None:
        """Test that old cancelled tasks are expired and cleaned up."""
        keys = script_runner.keys

        # Enqueue and mark for cancellation
        task_id = await enqueue_task(
            redis_client=redis_client,
            namespace=test_namespace,
            agent=test_agent,
            task=sample_task,
        )
        await script_runner.mark_for_cancellation(task_id)

        # Pickup will cancel it
        await script_runner.pickup()

        # Set cancellation timestamp to past
        await redis_client.zadd(keys.queue_cancelled, {task_id: time.time() - 86400})
        # Run expiration
        result = await script_runner.expire_tasks(
            completed_cutoff=time.time() - 3600,
            failed_cutoff=time.time() - 3600,
            cancelled_cutoff=time.time() - 3600,
        )

        assert result.cancelled_cleaned == 1
        assert ("cancelled", task_id) in result.cleaned_task_details

    async def test_does_not_expire_recent_tasks(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_agent: SimpleTestAgent,
        sample_task: Task[AgentContext],
        script_runner: ScriptRunner,
    ) -> None:
        """Test that recent tasks are not expired."""
        keys = script_runner.keys

        # Complete a task (timestamp will be current)
        task_id = await script_runner.enqueue_and_pickup(test_agent, sample_task)
        await script_runner.complete(
            task_id=task_id,
            action=CompletionAction.COMPLETE,
            payload_json=sample_task.payload.to_json(),
            final_output={"result": "done"},
        )

        # Run expiration with cutoff in the past
        result = await script_runner.expire_tasks(
            completed_cutoff=time.time() - 3600,
            failed_cutoff=time.time() - 3600,
            cancelled_cutoff=time.time() - 3600,
        )

        assert result.completed_cleaned == 0

        # Task data should still exist
        status = await redis_client.hget(keys.task_status, task_id)
        assert status == "completed"

    async def test_respects_different_cutoffs_per_queue(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_agent: SimpleTestAgent,
        test_owner_id: str,
        script_runner: ScriptRunner,
    ) -> None:
        """Test that different cutoffs are respected for each queue type."""
        keys = script_runner.keys

        # Create completed task
        ctx1 = AgentContext(query="Completed task")
        task1 = Task.create(owner_id=test_owner_id, agent=test_agent.name, payload=ctx1)
        task_id_1 = await script_runner.enqueue_and_pickup(test_agent, task1)
        await script_runner.complete(
            task_id=task_id_1,
            action=CompletionAction.COMPLETE,
            payload_json=ctx1.to_json(),
            final_output={"result": "done"},
        )
        await redis_client.zadd(keys.queue_completions, {task_id_1: time.time() - 7200})
        # Create failed task
        ctx2 = AgentContext(query="Failed task")
        task2 = Task.create(owner_id=test_owner_id, agent=test_agent.name, payload=ctx2)
        task_id_2 = await script_runner.enqueue_and_pickup(test_agent, task2)
        await script_runner.complete(
            task_id=task_id_2,
            action=CompletionAction.FAIL,
            payload_json=ctx2.to_json(),
        )
        await redis_client.zadd(keys.queue_failed, {task_id_2: time.time() - 7200})
        # Run expiration with different cutoffs:
        # - completed: expire tasks older than 1 hour (will expire)
        # - failed: expire tasks older than 24 hours (won't expire)
        # - cancelled: expire tasks older than 1 hour
        result = await script_runner.expire_tasks(
            completed_cutoff=time.time() - 3600,  # 1 hour
            failed_cutoff=time.time() - 86400,  # 24 hours
            cancelled_cutoff=time.time() - 3600,
        )

        assert result.completed_cleaned == 1
        assert result.failed_cleaned == 0

    async def test_respects_batch_limit(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_agent: SimpleTestAgent,
        test_owner_id: str,
        script_runner: ScriptRunner,
    ) -> None:
        """Test that expiration respects batch size limit."""
        keys = script_runner.keys

        # Create multiple completed tasks
        for i in range(5):
            ctx = AgentContext(query=f"Query {i}")
            task = Task.create(
                owner_id=test_owner_id, agent=test_agent.name, payload=ctx
            )
            task_id = await script_runner.enqueue_and_pickup(test_agent, task)
            await script_runner.complete(
                task_id=task_id,
                action=CompletionAction.COMPLETE,
                payload_json=ctx.to_json(),
                final_output={"result": "done"},
            )
            old_ts = time.time() - 86400
            await redis_client.zadd(
                keys.queue_completions, {task_id: old_ts}
            )

        # Expire with small batch
        result = await script_runner.expire_tasks(
            completed_cutoff=time.time() - 3600,
            failed_cutoff=time.time() - 3600,
            cancelled_cutoff=time.time() - 3600,
            max_cleanup_batch=2,
        )

        assert result.completed_cleaned == 2

    async def test_cleans_all_task_data_fields(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_agent: SimpleTestAgent,
        sample_task: Task[AgentContext],
        script_runner: ScriptRunner,
    ) -> None:
        """Test that all task data fields are cleaned on expiration."""
        keys = script_runner.keys

        # Complete a task
        task_id = await script_runner.enqueue_and_pickup(test_agent, sample_task)
        await script_runner.complete(
            task_id=task_id,
            action=CompletionAction.COMPLETE,
            payload_json=sample_task.payload.to_json(),
            final_output={"result": "done"},
        )

        # Verify data exists
        assert await redis_client.hget(keys.task_status, task_id) is not None
        assert await redis_client.hget(keys.task_agent, task_id) is not None
        assert await redis_client.hget(keys.task_payload, task_id) is not None
        assert await redis_client.hget(keys.task_pickups, task_id) is not None
        assert await redis_client.hget(keys.task_retries, task_id) is not None
        assert await redis_client.hget(keys.task_meta, task_id) is not None

        # Expire
        await redis_client.zadd(keys.queue_completions, {task_id: time.time() - 86400})

        await script_runner.expire_tasks(
            completed_cutoff=time.time() - 3600,
            failed_cutoff=time.time() - 3600,
            cancelled_cutoff=time.time() - 3600,
        )

        # All data should be deleted
        assert await redis_client.hget(keys.task_status, task_id) is None
        assert await redis_client.hget(keys.task_agent, task_id) is None
        assert await redis_client.hget(keys.task_payload, task_id) is None
        assert await redis_client.hget(keys.task_pickups, task_id) is None
        assert await redis_client.hget(keys.task_retries, task_id) is None
        assert await redis_client.hget(keys.task_meta, task_id) is None
