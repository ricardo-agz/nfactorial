"""Integration tests for parent-child task relationships.

Tests the full lifecycle of parent tasks spawning child tasks, waiting for
them to complete, and resuming with results.
"""

from __future__ import annotations

import json
import time
import uuid
from typing import Any

import pytest
import redis.asyncio as redis

from factorial.queue.keys import RedisKeys
from factorial.queue.lua import (
    create_cancel_task_script,
    create_child_task_completion_script,
    create_enqueue_task_script,
    create_task_completion_script,
)
from factorial.queue.operations import PENDING_SENTINEL


@pytest.mark.asyncio
class TestParentChildEnqueue:
    """Tests for parent tasks spawning child tasks."""

    async def test_parent_spawns_single_child_sets_pending_status(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_owner_id: str,
        task_id: str,
        completion_script: Any,
    ) -> None:
        """Parent task moves to pending_child_tasks when spawning a child."""
        agent_name = "parent_agent"
        keys = RedisKeys.format(
            namespace=test_namespace, task_id=task_id, agent=agent_name
        )
        child_task_id = str(uuid.uuid4())

        # Setup parent task
        await self._setup_processing_task(
            redis_client, keys, task_id, agent_name, test_owner_id
        )

        # Complete parent with pending child using the conftest helper
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
            parent_pending_child_task_results_key=None,
            task_id=task_id,
            action="pending_child_task_results",
            updated_task_payload_json=json.dumps({"query": "test", "turn": 1}),
            metrics_ttl=3600,
            pending_sentinel=PENDING_SENTINEL,
            current_turn=1,
            pending_tool_call_ids_json=None,
            pending_child_task_ids_json=json.dumps([child_task_id]),
            final_output_json=json.dumps(None),
        )

        assert result.success is True

        # Verify parent is in pending_child_tasks status
        status = await redis_client.hget(keys.task_status, task_id)
        assert status == "pending_child_tasks"

        # Verify child task is tracked with pending sentinel
        child_result = await redis_client.hget(
            keys.pending_child_task_results, child_task_id
        )
        assert child_result == PENDING_SENTINEL

        # Verify parent is in pending queue
        pending_score = await redis_client.zscore(keys.queue_pending, task_id)
        assert pending_score is not None

    async def test_parent_spawns_multiple_children(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_owner_id: str,
        task_id: str,
        completion_script: Any,
    ) -> None:
        """Parent can spawn multiple children at once."""
        agent_name = "parent_agent"
        keys = RedisKeys.format(
            namespace=test_namespace, task_id=task_id, agent=agent_name
        )
        child_ids = [str(uuid.uuid4()) for _ in range(3)]

        await self._setup_processing_task(
            redis_client, keys, task_id, agent_name, test_owner_id
        )

        # Complete parent with multiple pending children
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
            parent_pending_child_task_results_key=None,
            task_id=task_id,
            action="pending_child_task_results",
            updated_task_payload_json=json.dumps({"query": "test", "turn": 1}),
            metrics_ttl=3600,
            pending_sentinel=PENDING_SENTINEL,
            current_turn=1,
            pending_tool_call_ids_json=None,
            pending_child_task_ids_json=json.dumps(child_ids),
            final_output_json=json.dumps(None),
        )

        # All children should be tracked
        for child_id in child_ids:
            child_result = await redis_client.hget(
                keys.pending_child_task_results, child_id
            )
            assert child_result == PENDING_SENTINEL

    async def test_child_task_has_parent_reference_in_meta(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_owner_id: str,
    ) -> None:
        """Child task metadata contains reference to parent task."""
        parent_id = str(uuid.uuid4())
        child_id = str(uuid.uuid4())
        child_agent = "child_agent"

        keys = RedisKeys.format(
            namespace=test_namespace, task_id=child_id, agent=child_agent
        )

        # Enqueue child with parent_id in metadata
        enqueue_script = await create_enqueue_task_script(redis_client)
        child_meta = {"owner_id": test_owner_id, "parent_id": parent_id}

        await enqueue_script.execute(
            agent_queue_key=keys.queue_main,
            task_statuses_key=keys.task_status,
            task_agents_key=keys.task_agent,
            task_payloads_key=keys.task_payload,
            task_pickups_key=keys.task_pickups,
            task_retries_key=keys.task_retries,
            task_metas_key=keys.task_meta,
            task_id=child_id,
            task_agent=child_agent,
            task_payload_json=json.dumps({"query": "child query"}),
            task_pickups=0,
            task_retries=0,
            task_meta_json=json.dumps(child_meta),
        )

        # Verify child has parent reference
        meta_json = await redis_client.hget(keys.task_meta, child_id)
        meta = json.loads(str(meta_json))
        assert meta["parent_id"] == parent_id

    async def _setup_processing_task(
        self,
        redis_client: redis.Redis,
        keys: RedisKeys,
        task_id: str,
        agent_name: str,
        owner_id: str,
    ) -> None:
        """Helper to set up a task in processing state."""
        await redis_client.hset(keys.task_status, task_id, "processing")
        await redis_client.hset(keys.task_agent, task_id, agent_name)
        await redis_client.hset(
            keys.task_payload, task_id, json.dumps({"query": "test"})
        )
        await redis_client.hset(keys.task_pickups, task_id, 1)
        await redis_client.hset(keys.task_retries, task_id, 0)
        await redis_client.hset(
            keys.task_meta, task_id, json.dumps({"owner_id": owner_id})
        )


@pytest.mark.asyncio
class TestChildCompletion:
    """Tests for child task completion and parent resumption."""

    async def test_child_completion_updates_parent_pending_results(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_owner_id: str,
    ) -> None:
        """When child completes, its result is stored in parent's pending results."""
        completion_script = await create_task_completion_script(redis_client)
        parent_id = str(uuid.uuid4())
        child_id = str(uuid.uuid4())
        parent_agent = "parent_agent"
        child_agent = "child_agent"

        parent_keys = RedisKeys.format(
            namespace=test_namespace, task_id=parent_id, agent=parent_agent
        )
        child_keys = RedisKeys.format(
            namespace=test_namespace, task_id=child_id, agent=child_agent
        )

        # Setup parent waiting for child
        await self._setup_pending_parent(
            redis_client,
            parent_keys,
            parent_id,
            parent_agent,
            test_owner_id,
            [child_id],
        )

        # Setup child as processing
        await self._setup_processing_child(
            redis_client,
            child_keys,
            child_id,
            child_agent,
            test_owner_id,
            parent_id,
        )

        child_output = {"result": "child completed successfully", "data": 42}

        # Complete child task
        result = await completion_script.execute(
            queue_main_key=child_keys.queue_main,
            queue_completions_key=child_keys.queue_completions,
            queue_failed_key=child_keys.queue_failed,
            queue_backoff_key=child_keys.queue_backoff,
            queue_orphaned_key=child_keys.queue_orphaned,
            queue_pending_key=child_keys.queue_pending,
            task_statuses_key=child_keys.task_status,
            task_agents_key=child_keys.task_agent,
            task_payloads_key=child_keys.task_payload,
            task_pickups_key=child_keys.task_pickups,
            task_retries_key=child_keys.task_retries,
            task_metas_key=child_keys.task_meta,
            processing_heartbeats_key=child_keys.processing_heartbeats,
            pending_tool_results_key=child_keys.pending_tool_results,
            pending_child_task_results_key=child_keys.pending_child_task_results,
            agent_metrics_bucket_key=child_keys.agent_metrics_bucket,
            global_metrics_bucket_key=child_keys.global_metrics_bucket,
            batch_meta_key=child_keys.batch_meta,
            batch_progress_key=child_keys.batch_progress,
            batch_remaining_tasks_key=child_keys.batch_remaining_tasks,
            batch_completed_key=child_keys.batch_completed,
            parent_pending_child_task_results_key=(
                parent_keys.pending_child_task_results
            ),
            parent_pending_child_wait_ids_key=parent_keys.pending_child_wait_ids,
            task_id=child_id,
            action="complete",
            updated_task_payload_json=json.dumps({
                "query": "child query",
                "turn": 1,
            }),
            metrics_ttl=3600,
            pending_sentinel=PENDING_SENTINEL,
            current_turn=1,
            pending_tool_call_ids_json=None,
            pending_child_task_ids_json=None,
            final_output_json=json.dumps(child_output),
        )

        assert result.success is True

        # Verify child result is stored in parent's pending results
        stored_result = await redis_client.hget(
            parent_keys.pending_child_task_results, child_id
        )
        assert stored_result is not None
        assert json.loads(str(stored_result)) == child_output

    async def test_parent_resumes_after_all_children_complete(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_owner_id: str,
    ) -> None:
        """Parent task resumes execution after all children complete."""
        completion_script = await create_task_completion_script(redis_client)
        parent_id = str(uuid.uuid4())
        child_ids = [str(uuid.uuid4()) for _ in range(2)]
        parent_agent = "parent_agent"
        child_agent = "child_agent"

        parent_keys = RedisKeys.format(
            namespace=test_namespace, task_id=parent_id, agent=parent_agent
        )
        child1_keys = RedisKeys.format(
            namespace=test_namespace, task_id=child_ids[0], agent=child_agent
        )
        child2_keys = RedisKeys.format(
            namespace=test_namespace, task_id=child_ids[1], agent=child_agent
        )

        # Setup parent waiting for both children
        await self._setup_pending_parent(
            redis_client,
            parent_keys,
            parent_id,
            parent_agent,
            test_owner_id,
            child_ids,
        )

        # Complete first child
        await self._setup_processing_child(
            redis_client,
            child1_keys,
            child_ids[0],
            child_agent,
            test_owner_id,
            parent_id,
        )
        await completion_script.execute(
            queue_main_key=child1_keys.queue_main,
            queue_completions_key=child1_keys.queue_completions,
            queue_failed_key=child1_keys.queue_failed,
            queue_backoff_key=child1_keys.queue_backoff,
            queue_orphaned_key=child1_keys.queue_orphaned,
            queue_pending_key=child1_keys.queue_pending,
            task_statuses_key=child1_keys.task_status,
            task_agents_key=child1_keys.task_agent,
            task_payloads_key=child1_keys.task_payload,
            task_pickups_key=child1_keys.task_pickups,
            task_retries_key=child1_keys.task_retries,
            task_metas_key=child1_keys.task_meta,
            processing_heartbeats_key=child1_keys.processing_heartbeats,
            pending_tool_results_key=child1_keys.pending_tool_results,
            pending_child_task_results_key=child1_keys.pending_child_task_results,
            agent_metrics_bucket_key=child1_keys.agent_metrics_bucket,
            global_metrics_bucket_key=child1_keys.global_metrics_bucket,
            batch_meta_key=child1_keys.batch_meta,
            batch_progress_key=child1_keys.batch_progress,
            batch_remaining_tasks_key=child1_keys.batch_remaining_tasks,
            batch_completed_key=child1_keys.batch_completed,
            parent_pending_child_task_results_key=(
                parent_keys.pending_child_task_results
            ),
            parent_pending_child_wait_ids_key=parent_keys.pending_child_wait_ids,
            task_id=child_ids[0],
            action="complete",
            updated_task_payload_json=json.dumps({"query": "child1", "turn": 1}),
            metrics_ttl=3600,
            pending_sentinel=PENDING_SENTINEL,
            current_turn=1,
            pending_tool_call_ids_json=None,
            pending_child_task_ids_json=None,
            final_output_json=json.dumps({"result": "child1_done"}),
        )

        # Parent should still be pending (second child not done)
        parent_status = await redis_client.hget(parent_keys.task_status, parent_id)
        assert parent_status == "pending_child_tasks"

        # Second child still pending
        second_child_result = await redis_client.hget(
            parent_keys.pending_child_task_results, child_ids[1]
        )
        assert second_child_result == PENDING_SENTINEL

        # Complete second child
        await self._setup_processing_child(
            redis_client,
            child2_keys,
            child_ids[1],
            child_agent,
            test_owner_id,
            parent_id,
        )
        await completion_script.execute(
            queue_main_key=child2_keys.queue_main,
            queue_completions_key=child2_keys.queue_completions,
            queue_failed_key=child2_keys.queue_failed,
            queue_backoff_key=child2_keys.queue_backoff,
            queue_orphaned_key=child2_keys.queue_orphaned,
            queue_pending_key=child2_keys.queue_pending,
            task_statuses_key=child2_keys.task_status,
            task_agents_key=child2_keys.task_agent,
            task_payloads_key=child2_keys.task_payload,
            task_pickups_key=child2_keys.task_pickups,
            task_retries_key=child2_keys.task_retries,
            task_metas_key=child2_keys.task_meta,
            processing_heartbeats_key=child2_keys.processing_heartbeats,
            pending_tool_results_key=child2_keys.pending_tool_results,
            pending_child_task_results_key=child2_keys.pending_child_task_results,
            agent_metrics_bucket_key=child2_keys.agent_metrics_bucket,
            global_metrics_bucket_key=child2_keys.global_metrics_bucket,
            batch_meta_key=child2_keys.batch_meta,
            batch_progress_key=child2_keys.batch_progress,
            batch_remaining_tasks_key=child2_keys.batch_remaining_tasks,
            batch_completed_key=child2_keys.batch_completed,
            parent_pending_child_task_results_key=(
                parent_keys.pending_child_task_results
            ),
            parent_pending_child_wait_ids_key=parent_keys.pending_child_wait_ids,
            task_id=child_ids[1],
            action="complete",
            updated_task_payload_json=json.dumps({"query": "child2", "turn": 1}),
            metrics_ttl=3600,
            pending_sentinel=PENDING_SENTINEL,
            current_turn=1,
            pending_tool_call_ids_json=None,
            pending_child_task_ids_json=None,
            final_output_json=json.dumps({"result": "child2_done"}),
        )

        # Now resume parent using child_completion script
        child_completion_script = await create_child_task_completion_script(
            redis_client
        )

        # Get all completed results for parent context update
        all_results = await redis_client.hgetall(
            parent_keys.pending_child_task_results
        )
        updated_context = {
            "query": "parent query",
            "turn": 1,
            "child_results": {
                k: json.loads(str(v)) for k, v in all_results.items()
            },
        }

        success, status = await child_completion_script.execute(
            queue_main_key=parent_keys.queue_main,
            queue_orphaned_key=parent_keys.queue_orphaned,
            queue_pending_key=parent_keys.queue_pending,
            pending_child_task_results_key=parent_keys.pending_child_task_results,
            task_statuses_key=parent_keys.task_status,
            task_agents_key=parent_keys.task_agent,
            task_payloads_key=parent_keys.task_payload,
            task_pickups_key=parent_keys.task_pickups,
            task_retries_key=parent_keys.task_retries,
            task_metas_key=parent_keys.task_meta,
            task_id=parent_id,
            updated_task_context_json=json.dumps(updated_context),
        )

        assert success is True
        assert status == "ok"

        # Parent should be active and back in queue
        parent_status = await redis_client.hget(parent_keys.task_status, parent_id)
        assert parent_status == "active"

        # Parent should be in main queue
        queue_contents = await redis_client.lrange(parent_keys.queue_main, 0, -1)
        assert parent_id in queue_contents

    async def test_child_failure_propagates_to_parent(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_owner_id: str,
    ) -> None:
        """Child task failure result is stored in parent's pending results."""
        completion_script = await create_task_completion_script(redis_client)
        parent_id = str(uuid.uuid4())
        child_id = str(uuid.uuid4())
        parent_agent = "parent_agent"
        child_agent = "child_agent"

        parent_keys = RedisKeys.format(
            namespace=test_namespace, task_id=parent_id, agent=parent_agent
        )
        child_keys = RedisKeys.format(
            namespace=test_namespace, task_id=child_id, agent=child_agent
        )

        await self._setup_pending_parent(
            redis_client,
            parent_keys,
            parent_id,
            parent_agent,
            test_owner_id,
            [child_id],
        )
        await self._setup_processing_child(
            redis_client,
            child_keys,
            child_id,
            child_agent,
            test_owner_id,
            parent_id,
        )

        # Fail child task
        error_output = {"error": "Child task failed", "code": "PROCESSING_ERROR"}
        await completion_script.execute(
            queue_main_key=child_keys.queue_main,
            queue_completions_key=child_keys.queue_completions,
            queue_failed_key=child_keys.queue_failed,
            queue_backoff_key=child_keys.queue_backoff,
            queue_orphaned_key=child_keys.queue_orphaned,
            queue_pending_key=child_keys.queue_pending,
            task_statuses_key=child_keys.task_status,
            task_agents_key=child_keys.task_agent,
            task_payloads_key=child_keys.task_payload,
            task_pickups_key=child_keys.task_pickups,
            task_retries_key=child_keys.task_retries,
            task_metas_key=child_keys.task_meta,
            processing_heartbeats_key=child_keys.processing_heartbeats,
            pending_tool_results_key=child_keys.pending_tool_results,
            pending_child_task_results_key=child_keys.pending_child_task_results,
            agent_metrics_bucket_key=child_keys.agent_metrics_bucket,
            global_metrics_bucket_key=child_keys.global_metrics_bucket,
            batch_meta_key=child_keys.batch_meta,
            batch_progress_key=child_keys.batch_progress,
            batch_remaining_tasks_key=child_keys.batch_remaining_tasks,
            batch_completed_key=child_keys.batch_completed,
            parent_pending_child_task_results_key=(
                parent_keys.pending_child_task_results
            ),
            parent_pending_child_wait_ids_key=parent_keys.pending_child_wait_ids,
            task_id=child_id,
            action="fail",
            updated_task_payload_json=json.dumps({
                "query": "child query",
                "turn": 1,
            }),
            metrics_ttl=3600,
            pending_sentinel=PENDING_SENTINEL,
            current_turn=1,
            pending_tool_call_ids_json=None,
            pending_child_task_ids_json=None,
            final_output_json=json.dumps(error_output),
        )

        # Verify error is stored in parent's pending results
        stored_result = await redis_client.hget(
            parent_keys.pending_child_task_results, child_id
        )  # type: ignore[misc]
        assert stored_result is not None
        assert json.loads(str(stored_result)) == error_output

    async def test_child_completion_does_not_write_when_parent_is_not_pending(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_owner_id: str,
    ) -> None:
        """Child completion should not mutate a terminal parent's pending results."""
        completion_script = await create_task_completion_script(redis_client)
        parent_id = str(uuid.uuid4())
        child_id = str(uuid.uuid4())
        parent_agent = "parent_agent"
        child_agent = "child_agent"
        parent_keys = RedisKeys.format(
            namespace=test_namespace,
            task_id=parent_id,
            agent=parent_agent,
        )
        child_keys = RedisKeys.format(
            namespace=test_namespace,
            task_id=child_id,
            agent=child_agent,
        )

        # Parent is terminal, even though stale child metadata still exists.
        await redis_client.hset(parent_keys.task_status, parent_id, "completed")
        await redis_client.hset(parent_keys.task_agent, parent_id, parent_agent)
        await redis_client.hset(parent_keys.task_payload, parent_id, json.dumps({}))
        await redis_client.hset(parent_keys.task_pickups, parent_id, 1)
        await redis_client.hset(parent_keys.task_retries, parent_id, 0)
        await redis_client.hset(
            parent_keys.task_meta,
            parent_id,
            json.dumps({"owner_id": test_owner_id}),
        )
        await redis_client.hset(
            parent_keys.pending_child_task_results,
            child_id,
            PENDING_SENTINEL,
        )
        await redis_client.sadd(parent_keys.pending_child_wait_ids, child_id)

        await self._setup_processing_child(
            redis_client,
            child_keys,
            child_id,
            child_agent,
            test_owner_id,
            parent_id,
        )

        await completion_script.execute(
            queue_main_key=child_keys.queue_main,
            queue_completions_key=child_keys.queue_completions,
            queue_failed_key=child_keys.queue_failed,
            queue_backoff_key=child_keys.queue_backoff,
            queue_orphaned_key=child_keys.queue_orphaned,
            queue_pending_key=child_keys.queue_pending,
            task_statuses_key=child_keys.task_status,
            task_agents_key=child_keys.task_agent,
            task_payloads_key=child_keys.task_payload,
            task_pickups_key=child_keys.task_pickups,
            task_retries_key=child_keys.task_retries,
            task_metas_key=child_keys.task_meta,
            processing_heartbeats_key=child_keys.processing_heartbeats,
            pending_tool_results_key=child_keys.pending_tool_results,
            pending_child_task_results_key=child_keys.pending_child_task_results,
            agent_metrics_bucket_key=child_keys.agent_metrics_bucket,
            global_metrics_bucket_key=child_keys.global_metrics_bucket,
            batch_meta_key=child_keys.batch_meta,
            batch_progress_key=child_keys.batch_progress,
            batch_remaining_tasks_key=child_keys.batch_remaining_tasks,
            batch_completed_key=child_keys.batch_completed,
            parent_pending_child_task_results_key=(
                parent_keys.pending_child_task_results
            ),
            parent_pending_child_wait_ids_key=parent_keys.pending_child_wait_ids,
            task_id=child_id,
            action="complete",
            updated_task_payload_json=json.dumps({"query": "child query", "turn": 1}),
            metrics_ttl=3600,
            pending_sentinel=PENDING_SENTINEL,
            current_turn=1,
            pending_tool_call_ids_json=None,
            pending_child_task_ids_json=None,
            final_output_json=json.dumps({"result": "late write"}),
        )

        stored_result = await redis_client.hget(
            parent_keys.pending_child_task_results,
            child_id,
        )
        assert stored_result == PENDING_SENTINEL

    async def test_child_completion_does_not_write_when_child_not_in_wait_set(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_owner_id: str,
    ) -> None:
        """Child completion should not add entries for non-awaited child IDs."""
        completion_script = await create_task_completion_script(redis_client)
        parent_id = str(uuid.uuid4())
        child_id = str(uuid.uuid4())
        tracked_child_id = str(uuid.uuid4())
        parent_agent = "parent_agent"
        child_agent = "child_agent"
        parent_keys = RedisKeys.format(
            namespace=test_namespace,
            task_id=parent_id,
            agent=parent_agent,
        )
        child_keys = RedisKeys.format(
            namespace=test_namespace,
            task_id=child_id,
            agent=child_agent,
        )

        await self._setup_pending_parent(
            redis_client,
            parent_keys,
            parent_id,
            parent_agent,
            test_owner_id,
            [tracked_child_id],
        )
        await self._setup_processing_child(
            redis_client,
            child_keys,
            child_id,
            child_agent,
            test_owner_id,
            parent_id,
        )

        await completion_script.execute(
            queue_main_key=child_keys.queue_main,
            queue_completions_key=child_keys.queue_completions,
            queue_failed_key=child_keys.queue_failed,
            queue_backoff_key=child_keys.queue_backoff,
            queue_orphaned_key=child_keys.queue_orphaned,
            queue_pending_key=child_keys.queue_pending,
            task_statuses_key=child_keys.task_status,
            task_agents_key=child_keys.task_agent,
            task_payloads_key=child_keys.task_payload,
            task_pickups_key=child_keys.task_pickups,
            task_retries_key=child_keys.task_retries,
            task_metas_key=child_keys.task_meta,
            processing_heartbeats_key=child_keys.processing_heartbeats,
            pending_tool_results_key=child_keys.pending_tool_results,
            pending_child_task_results_key=child_keys.pending_child_task_results,
            agent_metrics_bucket_key=child_keys.agent_metrics_bucket,
            global_metrics_bucket_key=child_keys.global_metrics_bucket,
            batch_meta_key=child_keys.batch_meta,
            batch_progress_key=child_keys.batch_progress,
            batch_remaining_tasks_key=child_keys.batch_remaining_tasks,
            batch_completed_key=child_keys.batch_completed,
            parent_pending_child_task_results_key=(
                parent_keys.pending_child_task_results
            ),
            parent_pending_child_wait_ids_key=parent_keys.pending_child_wait_ids,
            task_id=child_id,
            action="complete",
            updated_task_payload_json=json.dumps({"query": "child query", "turn": 1}),
            metrics_ttl=3600,
            pending_sentinel=PENDING_SENTINEL,
            current_turn=1,
            pending_tool_call_ids_json=None,
            pending_child_task_ids_json=None,
            final_output_json=json.dumps({"result": "unexpected"}),
        )

        tracked_result = await redis_client.hget(
            parent_keys.pending_child_task_results,
            tracked_child_id,
        )
        untracked_result = await redis_client.hget(
            parent_keys.pending_child_task_results,
            child_id,
        )
        assert tracked_result == PENDING_SENTINEL
        assert untracked_result is None

    async def _setup_pending_parent(
        self,
        redis_client: redis.Redis,
        keys: RedisKeys,
        task_id: str,
        agent_name: str,
        owner_id: str,
        child_ids: list[str],
    ) -> None:
        """Helper to set up a parent task waiting for children."""
        await redis_client.hset(keys.task_status, task_id, "pending_child_tasks")
        await redis_client.hset(keys.task_agent, task_id, agent_name)
        await redis_client.hset(
            keys.task_payload, task_id, json.dumps({"query": "parent query"})
        )
        await redis_client.hset(keys.task_pickups, task_id, 1)
        await redis_client.hset(keys.task_retries, task_id, 0)
        await redis_client.hset(
            keys.task_meta, task_id, json.dumps({"owner_id": owner_id})
        )

        # Track pending children
        for child_id in child_ids:
            await redis_client.hset(
                keys.pending_child_task_results, child_id, PENDING_SENTINEL
            )
            await redis_client.sadd(keys.pending_child_wait_ids, child_id)

        # Add to pending queue
        await redis_client.zadd(keys.queue_pending, {task_id: time.time()})

    async def _setup_processing_child(
        self,
        redis_client: redis.Redis,
        keys: RedisKeys,
        task_id: str,
        agent_name: str,
        owner_id: str,
        parent_id: str,
    ) -> None:
        """Helper to set up a child task in processing state."""
        await redis_client.hset(keys.task_status, task_id, "processing")
        await redis_client.hset(keys.task_agent, task_id, agent_name)
        await redis_client.hset(
            keys.task_payload, task_id, json.dumps({"query": "child query"})
        )
        await redis_client.hset(keys.task_pickups, task_id, 1)
        await redis_client.hset(keys.task_retries, task_id, 0)
        await redis_client.hset(
            keys.task_meta,
            task_id,
            json.dumps({"owner_id": owner_id, "parent_id": parent_id}),
        )


@pytest.mark.asyncio
class TestNestedChildTasks:
    """Tests for nested parent-child relationships (grandparent -> parent -> child)."""

    async def test_three_level_task_hierarchy(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_owner_id: str,
    ) -> None:
        """Test grandparent -> parent -> child task hierarchy completes correctly."""
        completion_script = await create_task_completion_script(redis_client)
        grandparent_id = str(uuid.uuid4())
        parent_id = str(uuid.uuid4())
        child_id = str(uuid.uuid4())

        gp_agent = "grandparent_agent"
        p_agent = "parent_agent"
        c_agent = "child_agent"

        gp_keys = RedisKeys.format(
            namespace=test_namespace, task_id=grandparent_id, agent=gp_agent
        )
        p_keys = RedisKeys.format(
            namespace=test_namespace, task_id=parent_id, agent=p_agent
        )
        c_keys = RedisKeys.format(
            namespace=test_namespace, task_id=child_id, agent=c_agent
        )

        # Setup grandparent waiting for parent
        await self._setup_task(
            redis_client,
            gp_keys,
            grandparent_id,
            gp_agent,
            test_owner_id,
            status="pending_child_tasks",
            parent_id=None,
        )
        await redis_client.hset(
            gp_keys.pending_child_task_results, parent_id, PENDING_SENTINEL
        )
        await redis_client.sadd(gp_keys.pending_child_wait_ids, parent_id)

        # Setup parent waiting for child
        await self._setup_task(
            redis_client,
            p_keys,
            parent_id,
            p_agent,
            test_owner_id,
            status="pending_child_tasks",
            parent_id=grandparent_id,
        )
        await redis_client.hset(
            p_keys.pending_child_task_results, child_id, PENDING_SENTINEL
        )
        await redis_client.sadd(p_keys.pending_child_wait_ids, child_id)

        # Setup child as processing
        await self._setup_task(
            redis_client,
            c_keys,
            child_id,
            c_agent,
            test_owner_id,
            status="processing",
            parent_id=parent_id,
        )

        # Complete child
        child_output = {"level": "child", "value": 100}
        await completion_script.execute(
            queue_main_key=c_keys.queue_main,
            queue_completions_key=c_keys.queue_completions,
            queue_failed_key=c_keys.queue_failed,
            queue_backoff_key=c_keys.queue_backoff,
            queue_orphaned_key=c_keys.queue_orphaned,
            queue_pending_key=c_keys.queue_pending,
            task_statuses_key=c_keys.task_status,
            task_agents_key=c_keys.task_agent,
            task_payloads_key=c_keys.task_payload,
            task_pickups_key=c_keys.task_pickups,
            task_retries_key=c_keys.task_retries,
            task_metas_key=c_keys.task_meta,
            processing_heartbeats_key=c_keys.processing_heartbeats,
            pending_tool_results_key=c_keys.pending_tool_results,
            pending_child_task_results_key=c_keys.pending_child_task_results,
            agent_metrics_bucket_key=c_keys.agent_metrics_bucket,
            global_metrics_bucket_key=c_keys.global_metrics_bucket,
            batch_meta_key=c_keys.batch_meta,
            batch_progress_key=c_keys.batch_progress,
            batch_remaining_tasks_key=c_keys.batch_remaining_tasks,
            batch_completed_key=c_keys.batch_completed,
            parent_pending_child_task_results_key=p_keys.pending_child_task_results,
            parent_pending_child_wait_ids_key=p_keys.pending_child_wait_ids,
            task_id=child_id,
            action="complete",
            updated_task_payload_json=json.dumps({"query": "child"}),
            metrics_ttl=3600,
            pending_sentinel=PENDING_SENTINEL,
            current_turn=1,
            pending_tool_call_ids_json=None,
            pending_child_task_ids_json=None,
            final_output_json=json.dumps(child_output),
        )

        # Verify child result in parent's pending results
        parent_child_result = await redis_client.hget(
            p_keys.pending_child_task_results, child_id
        )
        assert json.loads(str(parent_child_result)) == child_output

        # Resume parent
        child_completion_script = await create_child_task_completion_script(
            redis_client
        )
        await child_completion_script.execute(
            queue_main_key=p_keys.queue_main,
            queue_orphaned_key=p_keys.queue_orphaned,
            queue_pending_key=p_keys.queue_pending,
            pending_child_task_results_key=p_keys.pending_child_task_results,
            task_statuses_key=p_keys.task_status,
            task_agents_key=p_keys.task_agent,
            task_payloads_key=p_keys.task_payload,
            task_pickups_key=p_keys.task_pickups,
            task_retries_key=p_keys.task_retries,
            task_metas_key=p_keys.task_meta,
            task_id=parent_id,
            updated_task_context_json=json.dumps({
                "query": "parent",
                "child_results": child_output,
            }),
        )

        # Now complete parent, which should update grandparent
        await redis_client.hset(p_keys.task_status, parent_id, "processing")
        parent_output = {"level": "parent", "aggregated": child_output}
        await completion_script.execute(
            queue_main_key=p_keys.queue_main,
            queue_completions_key=p_keys.queue_completions,
            queue_failed_key=p_keys.queue_failed,
            queue_backoff_key=p_keys.queue_backoff,
            queue_orphaned_key=p_keys.queue_orphaned,
            queue_pending_key=p_keys.queue_pending,
            task_statuses_key=p_keys.task_status,
            task_agents_key=p_keys.task_agent,
            task_payloads_key=p_keys.task_payload,
            task_pickups_key=p_keys.task_pickups,
            task_retries_key=p_keys.task_retries,
            task_metas_key=p_keys.task_meta,
            processing_heartbeats_key=p_keys.processing_heartbeats,
            pending_tool_results_key=p_keys.pending_tool_results,
            pending_child_task_results_key=p_keys.pending_child_task_results,
            agent_metrics_bucket_key=p_keys.agent_metrics_bucket,
            global_metrics_bucket_key=p_keys.global_metrics_bucket,
            batch_meta_key=p_keys.batch_meta,
            batch_progress_key=p_keys.batch_progress,
            batch_remaining_tasks_key=p_keys.batch_remaining_tasks,
            batch_completed_key=p_keys.batch_completed,
            parent_pending_child_task_results_key=(
                gp_keys.pending_child_task_results
            ),
            parent_pending_child_wait_ids_key=gp_keys.pending_child_wait_ids,
            task_id=parent_id,
            action="complete",
            updated_task_payload_json=json.dumps({"query": "parent"}),
            metrics_ttl=3600,
            pending_sentinel=PENDING_SENTINEL,
            current_turn=1,
            pending_tool_call_ids_json=None,
            pending_child_task_ids_json=None,
            final_output_json=json.dumps(parent_output),
        )

        # Verify parent result in grandparent's pending results
        gp_child_result = await redis_client.hget(
            gp_keys.pending_child_task_results, parent_id
        )
        assert json.loads(str(gp_child_result)) == parent_output

    async def _setup_task(
        self,
        redis_client: redis.Redis,
        keys: RedisKeys,
        task_id: str,
        agent_name: str,
        owner_id: str,
        status: str,
        parent_id: str | None,
    ) -> None:
        """Helper to set up a task with given status."""
        await redis_client.hset(keys.task_status, task_id, status)
        await redis_client.hset(keys.task_agent, task_id, agent_name)
        await redis_client.hset(
            keys.task_payload, task_id, json.dumps({"query": "test"})
        )
        await redis_client.hset(keys.task_pickups, task_id, 1)
        await redis_client.hset(keys.task_retries, task_id, 0)

        meta: dict[str, Any] = {"owner_id": owner_id}
        if parent_id:
            meta["parent_id"] = parent_id
        await redis_client.hset(keys.task_meta, task_id, json.dumps(meta))

        if status == "pending_child_tasks":
            await redis_client.zadd(keys.queue_pending, {task_id: time.time()})


@pytest.mark.asyncio
class TestParentChildCancellation:
    """Tests for cancellation propagation in parent-child scenarios."""

    async def test_cancel_parent_with_pending_children(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_owner_id: str,
    ) -> None:
        """Cancelling a parent should record child IDs for cancellation."""
        parent_id = str(uuid.uuid4())
        child_ids = [str(uuid.uuid4()) for _ in range(3)]
        agent_name = "parent_agent"

        keys = RedisKeys.format(
            namespace=test_namespace, task_id=parent_id, agent=agent_name
        )

        # Setup parent in pending_child_tasks state
        await redis_client.hset(keys.task_status, parent_id, "pending_child_tasks")
        await redis_client.hset(keys.task_agent, parent_id, agent_name)
        await redis_client.hset(
            keys.task_payload, parent_id, json.dumps({"query": "parent"})
        )
        await redis_client.hset(keys.task_pickups, parent_id, "1")
        await redis_client.hset(keys.task_retries, parent_id, "0")
        await redis_client.hset(
            keys.task_meta, parent_id, json.dumps({"owner_id": test_owner_id})
        )

        # Track pending children
        for child_id in child_ids:
            await redis_client.hset(
                keys.pending_child_task_results, child_id, PENDING_SENTINEL
            )

        await redis_client.zadd(keys.queue_pending, {parent_id: time.time()})

        # Cancel parent
        cancellation_script = await create_cancel_task_script(redis_client)
        result = await cancellation_script.execute(
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
            task_id=parent_id,
            metrics_ttl=3600,
        )

        # Parent should be cancelled
        assert result.success is True
        assert result.message == "Task cancelled"

        # Parent should have cancelled status
        status = await redis_client.hget(keys.task_status, parent_id)
        assert status == "cancelled"

        # Child IDs should be in the pending cancellations set for cascade
        pending_cancellations = await redis_client.smembers(keys.task_cancellations)
        assert set(pending_cancellations) == set(child_ids)

        # Pending child results should be cleared
        pending_exists = await redis_client.exists(keys.pending_child_task_results)
        assert pending_exists == 0
