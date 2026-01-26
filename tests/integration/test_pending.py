import json
import uuid

import pytest
import redis.asyncio as redis

from factorial.queue.keys import PENDING_SENTINEL, RedisKeys
from factorial.queue.lua import (
    BatchPickupScript,
    TaskCompletionScript,
    ToolCompletionScript,
)
from factorial.queue.operations import enqueue_task
from factorial.queue.task import TaskStatus, get_task_status
from factorial.queue.worker import CompletionAction

from .conftest import SimpleTestAgent


@pytest.mark.asyncio
class TestPendingToolResults:
    """Tests for pending tool result handling."""

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

    async def test_pending_tool_action_transitions_to_pending(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_agent: SimpleTestAgent,
        sample_task,
        pickup_script: BatchPickupScript,
        completion_script: TaskCompletionScript,
    ) -> None:
        """Test that pending tool action transitions task to PENDING_TOOL_RESULTS."""
        keys = RedisKeys.format(
            namespace=test_namespace,
            agent=test_agent.name,
            task_id=sample_task.id,
        )

        task_id = await self._setup_processing_task(
            redis_client, test_namespace, test_agent, sample_task, pickup_script
        )

        tool_call_ids = ["tool_call_1", "tool_call_2"]

        # Mark task as pending tool results
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
            action=CompletionAction.PENDING_TOOL.value,
            updated_task_payload_json=sample_task.payload.to_json(),
            metrics_ttl=3600,
            pending_sentinel=PENDING_SENTINEL,
            current_turn=0,
            pending_tool_call_ids_json=json.dumps(tool_call_ids),
        )

        assert result.success
        status = await get_task_status(redis_client, test_namespace, task_id)
        assert status == TaskStatus.PENDING_TOOL_RESULTS

    async def test_pending_tool_creates_sentinel_entries(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_agent: SimpleTestAgent,
        sample_task,
        pickup_script: BatchPickupScript,
        completion_script: TaskCompletionScript,
    ) -> None:
        """Test that pending tool action creates sentinel entries for each tool call."""
        keys = RedisKeys.format(
            namespace=test_namespace,
            agent=test_agent.name,
            task_id=sample_task.id,
        )

        task_id = await self._setup_processing_task(
            redis_client, test_namespace, test_agent, sample_task, pickup_script
        )

        tool_call_ids = ["tool_call_1", "tool_call_2", "tool_call_3"]

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
            action=CompletionAction.PENDING_TOOL.value,
            updated_task_payload_json=sample_task.payload.to_json(),
            metrics_ttl=3600,
            pending_sentinel=PENDING_SENTINEL,
            current_turn=0,
            pending_tool_call_ids_json=json.dumps(tool_call_ids),
        )

        # Check sentinel entries were created
        for tool_id in tool_call_ids:
            value = await redis_client.hget(keys.pending_tool_results, tool_id)
            assert value == PENDING_SENTINEL

    async def test_pending_task_added_to_pending_queue(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_agent: SimpleTestAgent,
        sample_task,
        pickup_script: BatchPickupScript,
        completion_script: TaskCompletionScript,
    ) -> None:
        """Test that pending task is added to pending queue."""
        keys = RedisKeys.format(
            namespace=test_namespace,
            agent=test_agent.name,
            task_id=sample_task.id,
        )

        task_id = await self._setup_processing_task(
            redis_client, test_namespace, test_agent, sample_task, pickup_script
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
            action=CompletionAction.PENDING_TOOL.value,
            updated_task_payload_json=sample_task.payload.to_json(),
            metrics_ttl=3600,
            pending_sentinel=PENDING_SENTINEL,
            current_turn=0,
            pending_tool_call_ids_json=json.dumps(["tool_call_1"]),
        )

        # Task should be in pending queue
        score = await redis_client.zscore(keys.queue_pending, task_id)
        assert score is not None


@pytest.mark.asyncio
class TestToolCompletion:
    """Tests for tool result completion flow."""

    async def _setup_pending_tool_task(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_agent: SimpleTestAgent,
        sample_task,
        pickup_script: BatchPickupScript,
        completion_script: TaskCompletionScript,
        tool_call_ids: list[str],
    ) -> str:
        """Helper to set up a task in PENDING_TOOL_RESULTS state."""
        keys = RedisKeys.format(
            namespace=test_namespace,
            agent=test_agent.name,
            task_id=sample_task.id,
        )

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
            action=CompletionAction.PENDING_TOOL.value,
            updated_task_payload_json=sample_task.payload.to_json(),
            metrics_ttl=3600,
            pending_sentinel=PENDING_SENTINEL,
            current_turn=0,
            pending_tool_call_ids_json=json.dumps(tool_call_ids),
        )

        return task_id

    async def test_tool_completion_requeues_task(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_agent: SimpleTestAgent,
        sample_task,
        pickup_script: BatchPickupScript,
        completion_script: TaskCompletionScript,
        tool_completion_script: ToolCompletionScript,
    ) -> None:
        """Test that completing all tool results requeues the task."""
        keys = RedisKeys.format(
            namespace=test_namespace,
            agent=test_agent.name,
            task_id=sample_task.id,
        )
        tool_call_id = "tool_call_1"

        task_id = await self._setup_pending_tool_task(
            redis_client,
            test_namespace,
            test_agent,
            sample_task,
            pickup_script,
            completion_script,
            [tool_call_id],
        )

        # Set the tool result
        await redis_client.hset(
            keys.pending_tool_results, tool_call_id, json.dumps({"result": "success"})
        )

        # Complete the tool results
        success, _ = await tool_completion_script.execute(
            queue_main_key=keys.queue_main,
            queue_orphaned_key=keys.queue_orphaned,
            queue_pending_key=keys.queue_pending,
            pending_tool_results_key=keys.pending_tool_results,
            task_statuses_key=keys.task_status,
            task_agents_key=keys.task_agent,
            task_payloads_key=keys.task_payload,
            task_pickups_key=keys.task_pickups,
            task_retries_key=keys.task_retries,
            task_metas_key=keys.task_meta,
            task_id=task_id,
            updated_task_context_json=sample_task.payload.to_json(),
        )

        assert success

        # Task should be back in queue with ACTIVE status
        status = await get_task_status(redis_client, test_namespace, task_id)
        assert status == TaskStatus.ACTIVE

        queue_length = await redis_client.llen(keys.queue_main)
        assert queue_length == 1


@pytest.mark.asyncio
class TestPendingChildTasks:
    """Tests for pending child task handling."""

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

    async def test_pending_child_action_transitions_to_pending(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_agent: SimpleTestAgent,
        sample_task,
        pickup_script: BatchPickupScript,
        completion_script: TaskCompletionScript,
    ) -> None:
        """Test that pending child action transitions task to PENDING_CHILD_TASKS."""
        keys = RedisKeys.format(
            namespace=test_namespace,
            agent=test_agent.name,
            task_id=sample_task.id,
        )

        task_id = await self._setup_processing_task(
            redis_client, test_namespace, test_agent, sample_task, pickup_script
        )

        child_task_ids = [str(uuid.uuid4()), str(uuid.uuid4())]

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
            action=CompletionAction.PENDING_CHILD.value,
            updated_task_payload_json=sample_task.payload.to_json(),
            metrics_ttl=3600,
            pending_sentinel=PENDING_SENTINEL,
            current_turn=0,
            pending_child_task_ids_json=json.dumps(child_task_ids),
        )

        assert result.success
        status = await get_task_status(redis_client, test_namespace, task_id)
        assert status == TaskStatus.PENDING_CHILD_TASKS

    async def test_pending_child_creates_sentinel_entries(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_agent: SimpleTestAgent,
        sample_task,
        pickup_script: BatchPickupScript,
        completion_script: TaskCompletionScript,
    ) -> None:
        """Test that pending child action creates sentinel entries for each child."""
        keys = RedisKeys.format(
            namespace=test_namespace,
            agent=test_agent.name,
            task_id=sample_task.id,
        )

        task_id = await self._setup_processing_task(
            redis_client, test_namespace, test_agent, sample_task, pickup_script
        )

        child_task_ids = [str(uuid.uuid4()) for _ in range(3)]

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
            action=CompletionAction.PENDING_CHILD.value,
            updated_task_payload_json=sample_task.payload.to_json(),
            metrics_ttl=3600,
            pending_sentinel=PENDING_SENTINEL,
            current_turn=0,
            pending_child_task_ids_json=json.dumps(child_task_ids),
        )

        # Check sentinel entries were created
        for child_id in child_task_ids:
            value = await redis_client.hget(keys.pending_child_task_results, child_id)
            assert value == PENDING_SENTINEL


@pytest.mark.asyncio
class TestChildTaskCompletion:
    """Tests for child task completion flow (child_completion.lua)."""

    async def _setup_pending_child_task(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_agent: SimpleTestAgent,
        sample_task,
        pickup_script: BatchPickupScript,
        completion_script: TaskCompletionScript,
        child_task_ids: list[str],
    ) -> str:
        """Helper to setup a task in PENDING_CHILD_TASKS state."""
        keys = RedisKeys.format(
            namespace=test_namespace,
            agent=test_agent.name,
            task_id=sample_task.id,
        )

        task_id = await enqueue_task(
            redis_client=redis_client,
            namespace=test_namespace,
            agent=test_agent,
            task=sample_task,
        )

        # Pickup
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

        # Put in pending child state
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
            action=CompletionAction.PENDING_CHILD.value,
            updated_task_payload_json=sample_task.payload.to_json(),
            metrics_ttl=3600,
            pending_sentinel=PENDING_SENTINEL,
            current_turn=0,
            pending_child_task_ids_json=json.dumps(child_task_ids),
        )

        return task_id

    async def test_child_completion_requeues_task(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_agent: SimpleTestAgent,
        sample_task,
        pickup_script: BatchPickupScript,
        completion_script: TaskCompletionScript,
        child_completion_script,
    ) -> None:
        """Test that completing all child tasks requeues the parent task."""
        keys = RedisKeys.format(
            namespace=test_namespace,
            agent=test_agent.name,
            task_id=sample_task.id,
        )

        child_task_ids = [str(uuid.uuid4())]
        task_id = await self._setup_pending_child_task(
            redis_client,
            test_namespace,
            test_agent,
            sample_task,
            pickup_script,
            completion_script,
            child_task_ids,
        )

        # Get keys for the task
        task_keys = RedisKeys.format(
            namespace=test_namespace,
            agent=test_agent.name,
            task_id=task_id,
        )

        # Set child result (replace sentinel with actual result)
        child_result = {"status": "completed", "output": "child output"}
        await redis_client.hset(
            task_keys.pending_child_task_results,
            child_task_ids[0],
            json.dumps(child_result),
        )

        # Complete child - should requeue parent
        success, status = await child_completion_script.execute(
            queue_main_key=keys.queue_main,
            queue_orphaned_key=keys.queue_orphaned,
            queue_pending_key=keys.queue_pending,
            pending_child_task_results_key=task_keys.pending_child_task_results,
            task_statuses_key=keys.task_status,
            task_agents_key=keys.task_agent,
            task_payloads_key=keys.task_payload,
            task_pickups_key=keys.task_pickups,
            task_retries_key=keys.task_retries,
            task_metas_key=keys.task_meta,
            task_id=task_id,
            updated_task_context_json=sample_task.payload.to_json(),
        )

        assert success
        assert status == "ok"

        # Task should be ACTIVE and in queue
        task_status = await get_task_status(redis_client, test_namespace, task_id)
        assert task_status == TaskStatus.ACTIVE

        queue_length = await redis_client.llen(keys.queue_main)
        assert queue_length == 1

    async def test_child_completion_updates_payload(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_agent: SimpleTestAgent,
        sample_task,
        pickup_script: BatchPickupScript,
        completion_script: TaskCompletionScript,
        child_completion_script,
    ) -> None:
        """Test that child completion updates the task payload."""
        keys = RedisKeys.format(
            namespace=test_namespace,
            agent=test_agent.name,
        )

        child_task_ids = [str(uuid.uuid4())]
        task_id = await self._setup_pending_child_task(
            redis_client,
            test_namespace,
            test_agent,
            sample_task,
            pickup_script,
            completion_script,
            child_task_ids,
        )

        task_keys = RedisKeys.format(
            namespace=test_namespace,
            agent=test_agent.name,
            task_id=task_id,
        )

        # Create updated payload with child results
        from factorial.context import AgentContext

        updated_context = AgentContext(
            query=sample_task.payload.query,
            messages=[{"role": "assistant", "content": "With child results"}],
            turn=1,
        )

        await child_completion_script.execute(
            queue_main_key=keys.queue_main,
            queue_orphaned_key=keys.queue_orphaned,
            queue_pending_key=keys.queue_pending,
            pending_child_task_results_key=task_keys.pending_child_task_results,
            task_statuses_key=keys.task_status,
            task_agents_key=keys.task_agent,
            task_payloads_key=keys.task_payload,
            task_pickups_key=keys.task_pickups,
            task_retries_key=keys.task_retries,
            task_metas_key=keys.task_meta,
            task_id=task_id,
            updated_task_context_json=updated_context.to_json(),
        )

        # Verify payload was updated
        payload_json = await redis_client.hget(keys.task_payload, task_id)
        assert payload_json is not None
        payload = json.loads(payload_json)
        assert payload["turn"] == 1
        assert len(payload["messages"]) == 1

    async def test_child_completion_cleans_up_pending_hash(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_agent: SimpleTestAgent,
        sample_task,
        pickup_script: BatchPickupScript,
        completion_script: TaskCompletionScript,
        child_completion_script,
    ) -> None:
        """Test that child completion removes pending hash."""
        keys = RedisKeys.format(
            namespace=test_namespace,
            agent=test_agent.name,
        )

        child_task_ids = [str(uuid.uuid4()), str(uuid.uuid4())]
        task_id = await self._setup_pending_child_task(
            redis_client,
            test_namespace,
            test_agent,
            sample_task,
            pickup_script,
            completion_script,
            child_task_ids,
        )

        task_keys = RedisKeys.format(
            namespace=test_namespace,
            agent=test_agent.name,
            task_id=task_id,
        )

        # Verify pending hash exists
        pending_exists = await redis_client.exists(task_keys.pending_child_task_results)
        assert pending_exists

        # Complete
        await child_completion_script.execute(
            queue_main_key=keys.queue_main,
            queue_orphaned_key=keys.queue_orphaned,
            queue_pending_key=keys.queue_pending,
            pending_child_task_results_key=task_keys.pending_child_task_results,
            task_statuses_key=keys.task_status,
            task_agents_key=keys.task_agent,
            task_payloads_key=keys.task_payload,
            task_pickups_key=keys.task_pickups,
            task_retries_key=keys.task_retries,
            task_metas_key=keys.task_meta,
            task_id=task_id,
            updated_task_context_json=sample_task.payload.to_json(),
        )

        # Pending hash should be deleted
        pending_exists = await redis_client.exists(task_keys.pending_child_task_results)
        assert not pending_exists

    async def test_child_completion_prevents_double_requeue(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_agent: SimpleTestAgent,
        sample_task,
        pickup_script: BatchPickupScript,
        completion_script: TaskCompletionScript,
        child_completion_script,
    ) -> None:
        """Test that double completion does not requeue twice."""
        keys = RedisKeys.format(
            namespace=test_namespace,
            agent=test_agent.name,
        )

        child_task_ids = [str(uuid.uuid4())]
        task_id = await self._setup_pending_child_task(
            redis_client,
            test_namespace,
            test_agent,
            sample_task,
            pickup_script,
            completion_script,
            child_task_ids,
        )

        task_keys = RedisKeys.format(
            namespace=test_namespace,
            agent=test_agent.name,
            task_id=task_id,
        )

        # First completion - should succeed
        success1, status1 = await child_completion_script.execute(
            queue_main_key=keys.queue_main,
            queue_orphaned_key=keys.queue_orphaned,
            queue_pending_key=keys.queue_pending,
            pending_child_task_results_key=task_keys.pending_child_task_results,
            task_statuses_key=keys.task_status,
            task_agents_key=keys.task_agent,
            task_payloads_key=keys.task_payload,
            task_pickups_key=keys.task_pickups,
            task_retries_key=keys.task_retries,
            task_metas_key=keys.task_meta,
            task_id=task_id,
            updated_task_context_json=sample_task.payload.to_json(),
        )

        assert success1
        assert status1 == "ok"

        # Second completion - should fail (task is now ACTIVE, not PENDING_CHILD_TASKS)
        success2, status2 = await child_completion_script.execute(
            queue_main_key=keys.queue_main,
            queue_orphaned_key=keys.queue_orphaned,
            queue_pending_key=keys.queue_pending,
            pending_child_task_results_key=task_keys.pending_child_task_results,
            task_statuses_key=keys.task_status,
            task_agents_key=keys.task_agent,
            task_payloads_key=keys.task_payload,
            task_pickups_key=keys.task_pickups,
            task_retries_key=keys.task_retries,
            task_metas_key=keys.task_meta,
            task_id=task_id,
            updated_task_context_json=sample_task.payload.to_json(),
        )

        assert not success2
        assert status2 == "already_completed"

        # Queue should still only have 1 task
        queue_length = await redis_client.llen(keys.queue_main)
        assert queue_length == 1

    async def test_child_completion_handles_missing_task(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_agent: SimpleTestAgent,
        child_completion_script,
    ) -> None:
        """Test child completion handles missing task data."""
        keys = RedisKeys.format(
            namespace=test_namespace,
            agent=test_agent.name,
        )

        fake_task_id = "nonexistent-task"
        fake_task_keys = RedisKeys.format(
            namespace=test_namespace,
            agent=test_agent.name,
            task_id=fake_task_id,
        )

        success, status = await child_completion_script.execute(
            queue_main_key=keys.queue_main,
            queue_orphaned_key=keys.queue_orphaned,
            queue_pending_key=keys.queue_pending,
            pending_child_task_results_key=fake_task_keys.pending_child_task_results,
            task_statuses_key=keys.task_status,
            task_agents_key=keys.task_agent,
            task_payloads_key=keys.task_payload,
            task_pickups_key=keys.task_pickups,
            task_retries_key=keys.task_retries,
            task_metas_key=keys.task_meta,
            task_id=fake_task_id,
            updated_task_context_json="{}",
        )

        assert not success
        assert status == "missing"

        # Should be added to orphaned queue
        orphaned_score = await redis_client.zscore(keys.queue_orphaned, fake_task_id)
        assert orphaned_score is not None

    async def test_child_completion_removes_from_pending_queue(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_agent: SimpleTestAgent,
        sample_task,
        pickup_script: BatchPickupScript,
        completion_script: TaskCompletionScript,
        child_completion_script,
    ) -> None:
        """Test that child completion removes task from pending queue."""
        keys = RedisKeys.format(
            namespace=test_namespace,
            agent=test_agent.name,
        )

        child_task_ids = [str(uuid.uuid4())]
        task_id = await self._setup_pending_child_task(
            redis_client,
            test_namespace,
            test_agent,
            sample_task,
            pickup_script,
            completion_script,
            child_task_ids,
        )

        task_keys = RedisKeys.format(
            namespace=test_namespace,
            agent=test_agent.name,
            task_id=task_id,
        )

        # Verify task is in pending queue
        pending_score = await redis_client.zscore(keys.queue_pending, task_id)
        assert pending_score is not None

        # Complete
        await child_completion_script.execute(
            queue_main_key=keys.queue_main,
            queue_orphaned_key=keys.queue_orphaned,
            queue_pending_key=keys.queue_pending,
            pending_child_task_results_key=task_keys.pending_child_task_results,
            task_statuses_key=keys.task_status,
            task_agents_key=keys.task_agent,
            task_payloads_key=keys.task_payload,
            task_pickups_key=keys.task_pickups,
            task_retries_key=keys.task_retries,
            task_metas_key=keys.task_meta,
            task_id=task_id,
            updated_task_context_json=sample_task.payload.to_json(),
        )

        # Task should be removed from pending queue
        pending_score = await redis_client.zscore(keys.queue_pending, task_id)
        assert pending_score is None
