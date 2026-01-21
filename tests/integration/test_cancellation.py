"""Integration tests for task cancellation scenarios."""

import json
import uuid
from typing import Any

import pytest
import redis.asyncio as redis

from factorial.queue.keys import PENDING_SENTINEL, RedisKeys
from factorial.queue.lua import (
    BatchPickupScript,
    CancelTaskScript,
    TaskCompletionScript,
)
from factorial.queue.operations import enqueue_task
from factorial.queue.task import TaskStatus, get_task_status
from factorial.queue.worker import CompletionAction

from .conftest import SimpleTestAgent


@pytest.mark.asyncio
class TestCancelQueuedTask:
    """Tests for cancelling tasks in QUEUED state."""

    async def test_cancel_queued_task_via_pickup(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_agent: SimpleTestAgent,
        sample_task,
        pickup_script: BatchPickupScript,
    ) -> None:
        """Test that a queued task marked for cancellation is cancelled at pickup."""
        keys = RedisKeys.format(namespace=test_namespace, agent=test_agent.name)

        task_id = await enqueue_task(
            redis_client=redis_client,
            namespace=test_namespace,
            agent=test_agent,
            task=sample_task,
        )

        # Mark task for cancellation
        await redis_client.sadd(keys.task_cancellations, task_id)

        # Pickup should detect cancellation
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

        # Task should be in cancelled list, not process list
        assert task_id in result.tasks_to_cancel_ids
        assert task_id not in result.tasks_to_process_ids

        status = await get_task_status(redis_client, test_namespace, task_id)
        assert status == TaskStatus.CANCELLED


@pytest.mark.asyncio
class TestCancelBackoffTask:
    """Tests for cancelling tasks in BACKOFF state."""

    async def test_cancel_backoff_task(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_agent: SimpleTestAgent,
        sample_task,
        pickup_script: BatchPickupScript,
        completion_script: TaskCompletionScript,
        cancel_script: CancelTaskScript,
        agents_by_name: dict[str, Any],
    ) -> None:
        """Test cancelling a task that is in backoff state."""
        keys = RedisKeys.format(
            namespace=test_namespace,
            agent=test_agent.name,
            task_id=sample_task.id,
        )

        # Enqueue and pickup
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

        # Put task into backoff
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

        # Verify task is in backoff
        status = await get_task_status(redis_client, test_namespace, task_id)
        assert status == TaskStatus.BACKOFF

        # Cancel the task
        result = await cancel_script.execute(
            queue_cancelled_key=keys.queue_cancelled,
            queue_backoff_key=keys.queue_backoff,
            queue_orphaned_key=keys.queue_orphaned,
            queue_pending_key=keys.queue_pending,
            pending_cancellations_key=keys.task_cancellations,
            task_statuses_key=keys.task_status,
            task_agents_key=keys.task_agent,
            task_payloads_key=keys.task_payload,
            task_pickups_key=keys.task_pickups,
            task_retries_key=keys.task_retries,
            task_metas_key=keys.task_meta,
            pending_tool_results_key=keys.pending_tool_results,
            pending_child_task_results_key=keys.pending_child_task_results,
            agent_metrics_bucket_key=keys.agent_metrics_bucket,
            global_metrics_bucket_key=keys.global_metrics_bucket,
            task_id=task_id,
            metrics_ttl=3600,
        )

        assert result.success
        status = await get_task_status(redis_client, test_namespace, task_id)
        assert status == TaskStatus.CANCELLED


@pytest.mark.asyncio
class TestCancelProcessingTask:
    """Tests for cancelling tasks in PROCESSING state."""

    async def test_cancel_processing_task_marks_for_cancellation(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_agent: SimpleTestAgent,
        sample_task,
        pickup_script: BatchPickupScript,
        cancel_script: CancelTaskScript,
    ) -> None:
        """Test that cancelling a processing task marks it for cancellation."""
        keys = RedisKeys.format(
            namespace=test_namespace,
            agent=test_agent.name,
            task_id=sample_task.id,
        )

        # Enqueue and pickup
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

        # Verify task is processing
        status = await get_task_status(redis_client, test_namespace, task_id)
        assert status == TaskStatus.PROCESSING

        # Request cancellation
        result = await cancel_script.execute(
            queue_cancelled_key=keys.queue_cancelled,
            queue_backoff_key=keys.queue_backoff,
            queue_orphaned_key=keys.queue_orphaned,
            queue_pending_key=keys.queue_pending,
            pending_cancellations_key=keys.task_cancellations,
            task_statuses_key=keys.task_status,
            task_agents_key=keys.task_agent,
            task_payloads_key=keys.task_payload,
            task_pickups_key=keys.task_pickups,
            task_retries_key=keys.task_retries,
            task_metas_key=keys.task_meta,
            pending_tool_results_key=keys.pending_tool_results,
            pending_child_task_results_key=keys.pending_child_task_results,
            agent_metrics_bucket_key=keys.agent_metrics_bucket,
            global_metrics_bucket_key=keys.global_metrics_bucket,
            task_id=task_id,
            metrics_ttl=3600,
        )

        # Task should be marked for cancellation (picked up by worker later)
        assert result.success
        is_marked = await redis_client.sismember(keys.task_cancellations, task_id)
        assert is_marked


@pytest.mark.asyncio
class TestCancelTerminalStates:
    """Tests for cancelling tasks in terminal states."""

    async def test_cancel_completed_task_fails(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_agent: SimpleTestAgent,
        sample_task,
        pickup_script: BatchPickupScript,
        completion_script: TaskCompletionScript,
        cancel_script: CancelTaskScript,
    ) -> None:
        """Test that cancelling a completed task returns error."""
        keys = RedisKeys.format(
            namespace=test_namespace,
            agent=test_agent.name,
            task_id=sample_task.id,
        )

        # Setup: enqueue, pickup, complete
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

        # Verify task is completed
        status = await get_task_status(redis_client, test_namespace, task_id)
        assert status == TaskStatus.COMPLETED

        # Try to cancel
        result = await cancel_script.execute(
            queue_cancelled_key=keys.queue_cancelled,
            queue_backoff_key=keys.queue_backoff,
            queue_orphaned_key=keys.queue_orphaned,
            queue_pending_key=keys.queue_pending,
            pending_cancellations_key=keys.task_cancellations,
            task_statuses_key=keys.task_status,
            task_agents_key=keys.task_agent,
            task_payloads_key=keys.task_payload,
            task_pickups_key=keys.task_pickups,
            task_retries_key=keys.task_retries,
            task_metas_key=keys.task_meta,
            pending_tool_results_key=keys.pending_tool_results,
            pending_child_task_results_key=keys.pending_child_task_results,
            agent_metrics_bucket_key=keys.agent_metrics_bucket,
            global_metrics_bucket_key=keys.global_metrics_bucket,
            task_id=task_id,
            metrics_ttl=3600,
        )

        # Should fail because task is already completed
        assert not result.success
        assert result.current_status == "completed"

    async def test_cancel_failed_task_fails(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_agent: SimpleTestAgent,
        sample_task,
        pickup_script: BatchPickupScript,
        completion_script: TaskCompletionScript,
        cancel_script: CancelTaskScript,
    ) -> None:
        """Test that cancelling a failed task returns error."""
        keys = RedisKeys.format(
            namespace=test_namespace,
            agent=test_agent.name,
            task_id=sample_task.id,
        )

        # Setup: enqueue, pickup, fail
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
            action=CompletionAction.FAIL.value,
            updated_task_payload_json=sample_task.payload.to_json(),
            metrics_ttl=3600,
            pending_sentinel=PENDING_SENTINEL,
            current_turn=0,
        )

        # Try to cancel
        result = await cancel_script.execute(
            queue_cancelled_key=keys.queue_cancelled,
            queue_backoff_key=keys.queue_backoff,
            queue_orphaned_key=keys.queue_orphaned,
            queue_pending_key=keys.queue_pending,
            pending_cancellations_key=keys.task_cancellations,
            task_statuses_key=keys.task_status,
            task_agents_key=keys.task_agent,
            task_payloads_key=keys.task_payload,
            task_pickups_key=keys.task_pickups,
            task_retries_key=keys.task_retries,
            task_metas_key=keys.task_meta,
            pending_tool_results_key=keys.pending_tool_results,
            pending_child_task_results_key=keys.pending_child_task_results,
            agent_metrics_bucket_key=keys.agent_metrics_bucket,
            global_metrics_bucket_key=keys.global_metrics_bucket,
            task_id=task_id,
            metrics_ttl=3600,
        )

        assert not result.success
        assert result.current_status == "failed"

    async def test_cancel_nonexistent_task_fails(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_agent: SimpleTestAgent,
        cancel_script: CancelTaskScript,
    ) -> None:
        """Test that cancelling a nonexistent task returns error."""
        keys = RedisKeys.format(namespace=test_namespace, agent=test_agent.name)
        fake_task_id = str(uuid.uuid4())

        result = await cancel_script.execute(
            queue_cancelled_key=keys.queue_cancelled,
            queue_backoff_key=keys.queue_backoff,
            queue_orphaned_key=keys.queue_orphaned,
            queue_pending_key=keys.queue_pending,
            pending_cancellations_key=keys.task_cancellations,
            task_statuses_key=keys.task_status,
            task_agents_key=keys.task_agent,
            task_payloads_key=keys.task_payload,
            task_pickups_key=keys.task_pickups,
            task_retries_key=keys.task_retries,
            task_metas_key=keys.task_meta,
            pending_tool_results_key=f"{test_namespace}:pending:{fake_task_id}:tools",
            pending_child_task_results_key=f"{test_namespace}:pending:{fake_task_id}:children",
            agent_metrics_bucket_key=keys.agent_metrics_bucket,
            global_metrics_bucket_key=keys.global_metrics_bucket,
            task_id=fake_task_id,
            metrics_ttl=3600,
        )

        assert not result.success
        assert result.current_status is None
