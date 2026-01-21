"""Integration tests for task completion state transitions."""

import json
import time

import pytest
import redis.asyncio as redis

from factorial.queue.keys import PENDING_SENTINEL, RedisKeys
from factorial.queue.lua import (
    BatchPickupScript,
    TaskCompletionScript,
)
from factorial.queue.operations import enqueue_task
from factorial.queue.task import TaskStatus, get_task_status
from factorial.queue.worker import CompletionAction

from .conftest import SimpleTestAgent


@pytest.mark.asyncio
class TestTaskPickup:
    """Tests for task pickup from queue."""

    async def test_pickup_transitions_to_processing(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_agent: SimpleTestAgent,
        sample_task,
        pickup_script: BatchPickupScript,
    ) -> None:
        """Test that picking up a task transitions it to PROCESSING."""
        keys = RedisKeys.format(namespace=test_namespace, agent=test_agent.name)

        task_id = await enqueue_task(
            redis_client=redis_client,
            namespace=test_namespace,
            agent=test_agent,
            task=sample_task,
        )

        # Pick up the task
        result = await pickup_script.execute(
            queue_main_key=keys.queue_main,
            queue_cancelled_key=keys.queue_cancelled,
            queue_orphaned_key=keys.queue_orphaned,
            task_statuses_key=keys.task_status,
            task_agents_key=keys.task_agent,
            task_payloads_key=keys.task_payload,
            task_pickups_key=keys.task_pickups,
            task_retries_key=keys.task_retries,
            task_metas_key=keys.task_meta,
            task_cancellations_key=keys.task_cancellations,
            processing_heartbeats_key=keys.processing_heartbeats,
            agent_metrics_bucket_key=keys.agent_metrics_bucket,
            global_metrics_bucket_key=keys.global_metrics_bucket,
            batch_size=1,
            metrics_ttl=3600,
        )

        assert task_id in result.tasks_to_process_ids
        status = await get_task_status(redis_client, test_namespace, task_id)
        assert status == TaskStatus.PROCESSING

    async def test_pickup_increments_pickups(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_agent: SimpleTestAgent,
        sample_task,
        pickup_script: BatchPickupScript,
    ) -> None:
        """Test that pickup increments the pickups counter."""
        keys = RedisKeys.format(namespace=test_namespace, agent=test_agent.name)

        task_id = await enqueue_task(
            redis_client=redis_client,
            namespace=test_namespace,
            agent=test_agent,
            task=sample_task,
        )

        await pickup_script.execute(
            queue_main_key=keys.queue_main,
            queue_cancelled_key=keys.queue_cancelled,
            queue_orphaned_key=keys.queue_orphaned,
            task_statuses_key=keys.task_status,
            task_agents_key=keys.task_agent,
            task_payloads_key=keys.task_payload,
            task_pickups_key=keys.task_pickups,
            task_retries_key=keys.task_retries,
            task_metas_key=keys.task_meta,
            task_cancellations_key=keys.task_cancellations,
            processing_heartbeats_key=keys.processing_heartbeats,
            agent_metrics_bucket_key=keys.agent_metrics_bucket,
            global_metrics_bucket_key=keys.global_metrics_bucket,
            batch_size=1,
            metrics_ttl=3600,
        )

        pickups = await redis_client.hget(keys.task_pickups, task_id)
        assert int(pickups) == 1

    async def test_pickup_adds_to_heartbeats(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_agent: SimpleTestAgent,
        sample_task,
        pickup_script: BatchPickupScript,
    ) -> None:
        """Test that picked up task is added to processing heartbeats."""
        keys = RedisKeys.format(namespace=test_namespace, agent=test_agent.name)

        task_id = await enqueue_task(
            redis_client=redis_client,
            namespace=test_namespace,
            agent=test_agent,
            task=sample_task,
        )

        await pickup_script.execute(
            queue_main_key=keys.queue_main,
            queue_cancelled_key=keys.queue_cancelled,
            queue_orphaned_key=keys.queue_orphaned,
            task_statuses_key=keys.task_status,
            task_agents_key=keys.task_agent,
            task_payloads_key=keys.task_payload,
            task_pickups_key=keys.task_pickups,
            task_retries_key=keys.task_retries,
            task_metas_key=keys.task_meta,
            task_cancellations_key=keys.task_cancellations,
            processing_heartbeats_key=keys.processing_heartbeats,
            agent_metrics_bucket_key=keys.agent_metrics_bucket,
            global_metrics_bucket_key=keys.global_metrics_bucket,
            batch_size=1,
            metrics_ttl=3600,
        )

        # Task should be in heartbeats set
        score = await redis_client.zscore(keys.processing_heartbeats, task_id)
        assert score is not None


@pytest.mark.asyncio
class TestTaskCompletion:
    """Tests for task completion transitions."""

    async def _setup_processing_task(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_agent: SimpleTestAgent,
        sample_task,
        pickup_script: BatchPickupScript,
    ) -> str:
        """Helper to create and pick up a task for processing."""
        keys = RedisKeys.format(namespace=test_namespace, agent=test_agent.name)

        task_id = await enqueue_task(
            redis_client=redis_client,
            namespace=test_namespace,
            agent=test_agent,
            task=sample_task,
        )

        await pickup_script.execute(
            queue_main_key=keys.queue_main,
            queue_cancelled_key=keys.queue_cancelled,
            queue_orphaned_key=keys.queue_orphaned,
            task_statuses_key=keys.task_status,
            task_agents_key=keys.task_agent,
            task_payloads_key=keys.task_payload,
            task_pickups_key=keys.task_pickups,
            task_retries_key=keys.task_retries,
            task_metas_key=keys.task_meta,
            task_cancellations_key=keys.task_cancellations,
            processing_heartbeats_key=keys.processing_heartbeats,
            agent_metrics_bucket_key=keys.agent_metrics_bucket,
            global_metrics_bucket_key=keys.global_metrics_bucket,
            batch_size=1,
            metrics_ttl=3600,
        )

        return task_id

    async def test_complete_action_transitions_to_completed(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_agent: SimpleTestAgent,
        sample_task,
        pickup_script: BatchPickupScript,
        completion_script: TaskCompletionScript,
        test_owner_id: str,
    ) -> None:
        """Test that complete action transitions task to COMPLETED."""
        keys = RedisKeys.format(
            namespace=test_namespace,
            agent=test_agent.name,
            task_id=sample_task.id,
        )

        task_id = await self._setup_processing_task(
            redis_client, test_namespace, test_agent, sample_task, pickup_script
        )

        # Complete the task
        result = await completion_script.execute(
            queue_main_key=keys.queue_main,
            queue_completions_key=keys.queue_completions,
            queue_failed_key=keys.queue_failed,
            queue_backoff_key=keys.queue_backoff,
            queue_orphaned_key=keys.queue_orphaned,
            queue_pending_key=keys.queue_pending,
            task_statuses_key=keys.task_status,
            task_agents_key=keys.task_agent,
            task_payloads_key=keys.task_payload,
            task_pickups_key=keys.task_pickups,
            task_retries_key=keys.task_retries,
            task_metas_key=keys.task_meta,
            processing_heartbeats_key=keys.processing_heartbeats,
            pending_tool_results_key=keys.pending_tool_results,
            pending_child_task_results_key=keys.pending_child_task_results,
            agent_metrics_bucket_key=keys.agent_metrics_bucket,
            global_metrics_bucket_key=keys.global_metrics_bucket,
            batch_meta_key=keys.batch_meta,
            batch_progress_key=keys.batch_progress,
            batch_remaining_tasks_key=keys.batch_remaining_tasks,
            batch_completed_key=keys.batch_completed,
            task_id=task_id,
            action=CompletionAction.COMPLETE.value,
            updated_task_payload_json=sample_task.payload.to_json(),
            metrics_ttl=3600,
            pending_sentinel=PENDING_SENTINEL,
            current_turn=0,
            final_output_json=json.dumps({"result": "done"}),
        )

        assert result.success
        status = await get_task_status(redis_client, test_namespace, task_id)
        assert status == TaskStatus.COMPLETED

    async def test_continue_action_transitions_to_active(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_agent: SimpleTestAgent,
        sample_task,
        pickup_script: BatchPickupScript,
        completion_script: TaskCompletionScript,
    ) -> None:
        """Test that continue action transitions task to ACTIVE and requeues."""
        keys = RedisKeys.format(
            namespace=test_namespace,
            agent=test_agent.name,
            task_id=sample_task.id,
        )

        task_id = await self._setup_processing_task(
            redis_client, test_namespace, test_agent, sample_task, pickup_script
        )

        # Continue the task
        result = await completion_script.execute(
            queue_main_key=keys.queue_main,
            queue_completions_key=keys.queue_completions,
            queue_failed_key=keys.queue_failed,
            queue_backoff_key=keys.queue_backoff,
            queue_orphaned_key=keys.queue_orphaned,
            queue_pending_key=keys.queue_pending,
            task_statuses_key=keys.task_status,
            task_agents_key=keys.task_agent,
            task_payloads_key=keys.task_payload,
            task_pickups_key=keys.task_pickups,
            task_retries_key=keys.task_retries,
            task_metas_key=keys.task_meta,
            processing_heartbeats_key=keys.processing_heartbeats,
            pending_tool_results_key=keys.pending_tool_results,
            pending_child_task_results_key=keys.pending_child_task_results,
            agent_metrics_bucket_key=keys.agent_metrics_bucket,
            global_metrics_bucket_key=keys.global_metrics_bucket,
            batch_meta_key=keys.batch_meta,
            batch_progress_key=keys.batch_progress,
            batch_remaining_tasks_key=keys.batch_remaining_tasks,
            batch_completed_key=keys.batch_completed,
            task_id=task_id,
            action=CompletionAction.CONTINUE.value,
            updated_task_payload_json=sample_task.payload.to_json(),
            metrics_ttl=3600,
            pending_sentinel=PENDING_SENTINEL,
            current_turn=0,
        )

        assert result.success
        status = await get_task_status(redis_client, test_namespace, task_id)
        assert status == TaskStatus.ACTIVE

        # Task should be back in queue
        queue_length = await redis_client.llen(keys.queue_main)
        assert queue_length == 1

    async def test_retry_action_increments_retries(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_agent: SimpleTestAgent,
        sample_task,
        pickup_script: BatchPickupScript,
        completion_script: TaskCompletionScript,
    ) -> None:
        """Test that retry action increments retry count."""
        keys = RedisKeys.format(
            namespace=test_namespace,
            agent=test_agent.name,
            task_id=sample_task.id,
        )

        task_id = await self._setup_processing_task(
            redis_client, test_namespace, test_agent, sample_task, pickup_script
        )

        # Retry the task
        await completion_script.execute(
            queue_main_key=keys.queue_main,
            queue_completions_key=keys.queue_completions,
            queue_failed_key=keys.queue_failed,
            queue_backoff_key=keys.queue_backoff,
            queue_orphaned_key=keys.queue_orphaned,
            queue_pending_key=keys.queue_pending,
            task_statuses_key=keys.task_status,
            task_agents_key=keys.task_agent,
            task_payloads_key=keys.task_payload,
            task_pickups_key=keys.task_pickups,
            task_retries_key=keys.task_retries,
            task_metas_key=keys.task_meta,
            processing_heartbeats_key=keys.processing_heartbeats,
            pending_tool_results_key=keys.pending_tool_results,
            pending_child_task_results_key=keys.pending_child_task_results,
            agent_metrics_bucket_key=keys.agent_metrics_bucket,
            global_metrics_bucket_key=keys.global_metrics_bucket,
            batch_meta_key=keys.batch_meta,
            batch_progress_key=keys.batch_progress,
            batch_remaining_tasks_key=keys.batch_remaining_tasks,
            batch_completed_key=keys.batch_completed,
            task_id=task_id,
            action=CompletionAction.RETRY.value,
            updated_task_payload_json=sample_task.payload.to_json(),
            metrics_ttl=3600,
            pending_sentinel=PENDING_SENTINEL,
            current_turn=0,
        )

        retries = await redis_client.hget(keys.task_retries, task_id)
        assert int(retries) == 1

        # Status should be ACTIVE (requeued)
        status = await get_task_status(redis_client, test_namespace, task_id)
        assert status == TaskStatus.ACTIVE

    async def test_backoff_action_adds_to_backoff_queue(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_agent: SimpleTestAgent,
        sample_task,
        pickup_script: BatchPickupScript,
        completion_script: TaskCompletionScript,
    ) -> None:
        """Test that backoff action adds task to backoff queue."""
        keys = RedisKeys.format(
            namespace=test_namespace,
            agent=test_agent.name,
            task_id=sample_task.id,
        )

        task_id = await self._setup_processing_task(
            redis_client, test_namespace, test_agent, sample_task, pickup_script
        )

        # Backoff the task
        await completion_script.execute(
            queue_main_key=keys.queue_main,
            queue_completions_key=keys.queue_completions,
            queue_failed_key=keys.queue_failed,
            queue_backoff_key=keys.queue_backoff,
            queue_orphaned_key=keys.queue_orphaned,
            queue_pending_key=keys.queue_pending,
            task_statuses_key=keys.task_status,
            task_agents_key=keys.task_agent,
            task_payloads_key=keys.task_payload,
            task_pickups_key=keys.task_pickups,
            task_retries_key=keys.task_retries,
            task_metas_key=keys.task_meta,
            processing_heartbeats_key=keys.processing_heartbeats,
            pending_tool_results_key=keys.pending_tool_results,
            pending_child_task_results_key=keys.pending_child_task_results,
            agent_metrics_bucket_key=keys.agent_metrics_bucket,
            global_metrics_bucket_key=keys.global_metrics_bucket,
            batch_meta_key=keys.batch_meta,
            batch_progress_key=keys.batch_progress,
            batch_remaining_tasks_key=keys.batch_remaining_tasks,
            batch_completed_key=keys.batch_completed,
            task_id=task_id,
            action=CompletionAction.BACKOFF.value,
            updated_task_payload_json=sample_task.payload.to_json(),
            metrics_ttl=3600,
            pending_sentinel=PENDING_SENTINEL,
            current_turn=0,
        )

        status = await get_task_status(redis_client, test_namespace, task_id)
        assert status == TaskStatus.BACKOFF

        # Task should be in backoff queue with future timestamp
        score = await redis_client.zscore(keys.queue_backoff, task_id)
        assert score is not None
        assert score > time.time()  # Future timestamp

    async def test_fail_action_transitions_to_failed(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_agent: SimpleTestAgent,
        sample_task,
        pickup_script: BatchPickupScript,
        completion_script: TaskCompletionScript,
    ) -> None:
        """Test that fail action transitions task to FAILED."""
        keys = RedisKeys.format(
            namespace=test_namespace,
            agent=test_agent.name,
            task_id=sample_task.id,
        )

        task_id = await self._setup_processing_task(
            redis_client, test_namespace, test_agent, sample_task, pickup_script
        )

        # Fail the task
        result = await completion_script.execute(
            queue_main_key=keys.queue_main,
            queue_completions_key=keys.queue_completions,
            queue_failed_key=keys.queue_failed,
            queue_backoff_key=keys.queue_backoff,
            queue_orphaned_key=keys.queue_orphaned,
            queue_pending_key=keys.queue_pending,
            task_statuses_key=keys.task_status,
            task_agents_key=keys.task_agent,
            task_payloads_key=keys.task_payload,
            task_pickups_key=keys.task_pickups,
            task_retries_key=keys.task_retries,
            task_metas_key=keys.task_meta,
            processing_heartbeats_key=keys.processing_heartbeats,
            pending_tool_results_key=keys.pending_tool_results,
            pending_child_task_results_key=keys.pending_child_task_results,
            agent_metrics_bucket_key=keys.agent_metrics_bucket,
            global_metrics_bucket_key=keys.global_metrics_bucket,
            batch_meta_key=keys.batch_meta,
            batch_progress_key=keys.batch_progress,
            batch_remaining_tasks_key=keys.batch_remaining_tasks,
            batch_completed_key=keys.batch_completed,
            task_id=task_id,
            action=CompletionAction.FAIL.value,
            updated_task_payload_json=sample_task.payload.to_json(),
            metrics_ttl=3600,
            pending_sentinel=PENDING_SENTINEL,
            current_turn=0,
        )

        assert result.success
        status = await get_task_status(redis_client, test_namespace, task_id)
        assert status == TaskStatus.FAILED

    async def test_completion_removes_from_heartbeats(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_agent: SimpleTestAgent,
        sample_task,
        pickup_script: BatchPickupScript,
        completion_script: TaskCompletionScript,
    ) -> None:
        """Test that any completion action removes task from heartbeats."""
        keys = RedisKeys.format(
            namespace=test_namespace,
            agent=test_agent.name,
            task_id=sample_task.id,
        )

        task_id = await self._setup_processing_task(
            redis_client, test_namespace, test_agent, sample_task, pickup_script
        )

        # Verify task is in heartbeats
        score_before = await redis_client.zscore(keys.processing_heartbeats, task_id)
        assert score_before is not None

        # Complete the task
        await completion_script.execute(
            queue_main_key=keys.queue_main,
            queue_completions_key=keys.queue_completions,
            queue_failed_key=keys.queue_failed,
            queue_backoff_key=keys.queue_backoff,
            queue_orphaned_key=keys.queue_orphaned,
            queue_pending_key=keys.queue_pending,
            task_statuses_key=keys.task_status,
            task_agents_key=keys.task_agent,
            task_payloads_key=keys.task_payload,
            task_pickups_key=keys.task_pickups,
            task_retries_key=keys.task_retries,
            task_metas_key=keys.task_meta,
            processing_heartbeats_key=keys.processing_heartbeats,
            pending_tool_results_key=keys.pending_tool_results,
            pending_child_task_results_key=keys.pending_child_task_results,
            agent_metrics_bucket_key=keys.agent_metrics_bucket,
            global_metrics_bucket_key=keys.global_metrics_bucket,
            batch_meta_key=keys.batch_meta,
            batch_progress_key=keys.batch_progress,
            batch_remaining_tasks_key=keys.batch_remaining_tasks,
            batch_completed_key=keys.batch_completed,
            task_id=task_id,
            action=CompletionAction.COMPLETE.value,
            updated_task_payload_json=sample_task.payload.to_json(),
            metrics_ttl=3600,
            pending_sentinel=PENDING_SENTINEL,
            current_turn=0,
            final_output_json=json.dumps({"result": "done"}),
        )

        # Task should be removed from heartbeats
        score_after = await redis_client.zscore(keys.processing_heartbeats, task_id)
        assert score_after is None
