"""Integration tests for batch child task scenarios.

Tests the batch spawning of multiple child tasks from a parent,
including batch tracking, progress updates, and completion handling.
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
    create_child_task_completion_script,
    create_enqueue_batch_script,
    create_task_completion_script,
)
from factorial.queue.operations import PENDING_SENTINEL


@pytest.mark.asyncio
class TestBatchChildEnqueue:
    """Tests for batch enqueueing of child tasks."""

    async def test_batch_enqueue_creates_all_child_tasks(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_owner_id: str,
    ) -> None:
        """Batch enqueue creates all tasks atomically."""
        batch_id = str(uuid.uuid4())
        agent_name = "batch_worker"
        keys = RedisKeys.format(namespace=test_namespace, agent=agent_name)

        # Prepare batch of tasks
        tasks = [
            {
                "id": str(uuid.uuid4()),
                "payload_json": json.dumps({"query": f"task_{i}"}),
            }
            for i in range(5)
        ]
        task_ids = [t["id"] for t in tasks]

        batch_enqueue_script = await create_enqueue_batch_script(redis_client)

        base_meta = {"max_turns": 10}
        batch_meta = {
            "owner_id": test_owner_id,
            "created_at": time.time(),
            "total_tasks": len(tasks),
            "max_progress": len(tasks) * 10,  # max_turns * num_tasks
            "status": "active",
            "progress": 0,
        }

        result = await batch_enqueue_script.execute(
            agent_queue_key=keys.queue_main,
            task_statuses_key=keys.task_status,
            task_agents_key=keys.task_agent,
            task_payloads_key=keys.task_payload,
            task_pickups_key=keys.task_pickups,
            task_retries_key=keys.task_retries,
            task_metas_key=keys.task_meta,
            batch_tasks_key=keys.batch_tasks,
            batch_meta_key=keys.batch_meta,
            batch_remaining_tasks_key=keys.batch_remaining_tasks,
            batch_progress_key=keys.batch_progress,
            batch_id=batch_id,
            owner_id=test_owner_id,
            created_at=time.time(),
            agent_name=agent_name,
            tasks_json=json.dumps(tasks),
            base_task_meta_json=json.dumps(base_meta),
            batch_meta_json=json.dumps(batch_meta),
        )

        # Verify all task IDs returned
        assert set(result.task_ids) == set(task_ids)

        # Verify all tasks are in the queue
        queue_contents = await redis_client.lrange(keys.queue_main, 0, -1)
        for task_id in task_ids:
            assert task_id in queue_contents

        # Verify task statuses
        for task_id in task_ids:
            status = await redis_client.hget(keys.task_status, task_id)
            assert status == "queued"

        # Verify batch metadata
        stored_meta = await redis_client.hget(keys.batch_meta, batch_id)
        assert stored_meta is not None
        meta_dict = json.loads(str(stored_meta))
        assert meta_dict["total_tasks"] == 5


@pytest.mark.asyncio
class TestBatchChildCompletion:
    """Tests for batch child task completion scenarios."""

    async def test_batch_progress_updates_on_completion(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_owner_id: str,
    ) -> None:
        """Batch progress is updated when child tasks complete."""
        batch_id = str(uuid.uuid4())
        agent_name = "batch_worker"
        keys = RedisKeys.format(namespace=test_namespace, agent=agent_name)

        # Create batch with 3 tasks
        task_ids = [str(uuid.uuid4()) for _ in range(3)]
        max_turns = 5

        # Setup batch metadata
        batch_meta = {
            "owner_id": test_owner_id,
            "total_tasks": 3,
            "max_progress": 3 * max_turns,  # 15
            "status": "active",
            "progress": 0,
        }
        await redis_client.hset(keys.batch_meta, batch_id, json.dumps(batch_meta))
        await redis_client.hset(
            keys.batch_remaining_tasks, batch_id, json.dumps(task_ids)
        )
        await redis_client.hset(keys.batch_progress, batch_id, 0)
        await redis_client.hset(keys.batch_tasks, batch_id, json.dumps(task_ids))

        # Setup first task in processing state
        task_id = task_ids[0]
        task_keys = RedisKeys.format(
            namespace=test_namespace, task_id=task_id, agent=agent_name
        )
        task_meta = {
            "owner_id": test_owner_id,
            "batch_id": batch_id,
            "max_turns": max_turns,
        }

        await redis_client.hset(task_keys.task_status, task_id, "processing")
        await redis_client.hset(task_keys.task_agent, task_id, agent_name)
        await redis_client.hset(
            task_keys.task_payload, task_id, json.dumps({"query": "test", "turn": 2})
        )
        await redis_client.hset(task_keys.task_pickups, task_id, "1")
        await redis_client.hset(task_keys.task_retries, task_id, "0")
        await redis_client.hset(task_keys.task_meta, task_id, json.dumps(task_meta))

        # Complete the task
        completion_script = await create_task_completion_script(redis_client)
        await completion_script.execute(
            queue_main_key=task_keys.queue_main,
            queue_completions_key=task_keys.queue_completions,
            queue_failed_key=task_keys.queue_failed,
            queue_backoff_key=task_keys.queue_backoff,
            queue_orphaned_key=task_keys.queue_orphaned,
            queue_pending_key=task_keys.queue_pending,
            task_statuses_key=task_keys.task_status,
            task_agents_key=task_keys.task_agent,
            task_payloads_key=task_keys.task_payload,
            task_pickups_key=task_keys.task_pickups,
            task_retries_key=task_keys.task_retries,
            task_metas_key=task_keys.task_meta,
            processing_heartbeats_key=task_keys.processing_heartbeats,
            pending_tool_results_key=task_keys.pending_tool_results,
            pending_child_task_results_key=task_keys.pending_child_task_results,
            agent_metrics_bucket_key=task_keys.agent_metrics_bucket,
            global_metrics_bucket_key=task_keys.global_metrics_bucket,
            batch_meta_key=task_keys.batch_meta,
            batch_progress_key=task_keys.batch_progress,
            batch_remaining_tasks_key=task_keys.batch_remaining_tasks,
            batch_completed_key=task_keys.batch_completed,
            task_id=task_id,
            action="complete",
            updated_task_payload_json=json.dumps({
                "query": "test",
                "turn": 2,
                "output": "done",
            }),
            metrics_ttl=3600,
            pending_sentinel=PENDING_SENTINEL,
            current_turn=2,
            parent_pending_child_task_results_key=None,
            pending_tool_call_ids_json=None,
            pending_child_task_ids_json=None,
            final_output_json=json.dumps({"result": "completed"}),
        )

        # Check task is completed
        status = await redis_client.hget(task_keys.task_status, task_id)
        assert status == "completed"


@pytest.mark.asyncio
class TestParentWithBatchChildren:
    """Tests for parent tasks spawning batch children."""

    async def test_parent_spawns_batch_of_children(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_owner_id: str,
    ) -> None:
        """Parent can spawn a batch of children and wait for all to complete."""
        parent_id = str(uuid.uuid4())
        child_ids = [str(uuid.uuid4()) for _ in range(3)]
        parent_agent = "orchestrator"

        parent_keys = RedisKeys.format(
            namespace=test_namespace, task_id=parent_id, agent=parent_agent
        )

        # Setup parent waiting for batch children
        await redis_client.hset(
            parent_keys.task_status, parent_id, "pending_child_tasks"
        )
        await redis_client.hset(parent_keys.task_agent, parent_id, parent_agent)
        await redis_client.hset(
            parent_keys.task_payload,
            parent_id,
            json.dumps({"query": "orchestrate batch", "batch_size": 3}),
        )
        await redis_client.hset(parent_keys.task_pickups, parent_id, "1")
        await redis_client.hset(parent_keys.task_retries, parent_id, "0")
        await redis_client.hset(
            parent_keys.task_meta,
            parent_id,
            json.dumps({"owner_id": test_owner_id}),
        )

        # Track all children
        for child_id in child_ids:
            await redis_client.hset(
                parent_keys.pending_child_task_results, child_id, PENDING_SENTINEL
            )

        await redis_client.zadd(
            parent_keys.queue_pending, {parent_id: time.time()}
        )

        # Verify all children tracked
        pending = await redis_client.hgetall(parent_keys.pending_child_task_results)
        assert len(pending) == 3
        for child_id in child_ids:
            assert pending[child_id] == PENDING_SENTINEL

    async def test_parent_resumes_after_batch_completes(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_owner_id: str,
    ) -> None:
        """Parent resumes after all batch children complete."""
        parent_id = str(uuid.uuid4())
        child_ids = [str(uuid.uuid4()) for _ in range(3)]
        parent_agent = "orchestrator"

        parent_keys = RedisKeys.format(
            namespace=test_namespace, task_id=parent_id, agent=parent_agent
        )

        # Setup parent waiting for children
        await redis_client.hset(
            parent_keys.task_status, parent_id, "pending_child_tasks"
        )
        await redis_client.hset(parent_keys.task_agent, parent_id, parent_agent)
        await redis_client.hset(
            parent_keys.task_payload,
            parent_id,
            json.dumps({"query": "orchestrate"}),
        )
        await redis_client.hset(parent_keys.task_pickups, parent_id, "1")
        await redis_client.hset(parent_keys.task_retries, parent_id, "0")
        await redis_client.hset(
            parent_keys.task_meta,
            parent_id,
            json.dumps({"owner_id": test_owner_id}),
        )

        # Track children - all already completed
        child_results: dict[str, dict[str, Any]] = {}
        for i, child_id in enumerate(child_ids):
            result = {"value": (i + 1) * 10, "index": i}
            child_results[child_id] = result
            await redis_client.hset(
                parent_keys.pending_child_task_results,
                child_id,
                json.dumps(result),
            )

        await redis_client.zadd(
            parent_keys.queue_pending, {parent_id: time.time()}
        )

        # Resume parent with all results
        child_completion_script = await create_child_task_completion_script(
            redis_client
        )

        updated_context = {
            "query": "orchestrate",
            "turn": 1,
            "batch_results": child_results,
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

        # Parent should be active
        parent_status = await redis_client.hget(parent_keys.task_status, parent_id)
        assert parent_status == "active"

        # Parent should be in queue
        queue = await redis_client.lrange(parent_keys.queue_main, 0, -1)
        assert parent_id in queue

        # Pending child results should be cleared
        pending_exists = await redis_client.exists(
            parent_keys.pending_child_task_results
        )
        assert pending_exists == 0


@pytest.mark.asyncio
class TestMixedBatchAndSingleChildren:
    """Tests for scenarios mixing batch and single child tasks."""

    async def test_parent_with_mixed_children_types(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_owner_id: str,
    ) -> None:
        """Parent can have both batch and single child tasks."""
        parent_id = str(uuid.uuid4())
        batch_child_ids = [str(uuid.uuid4()) for _ in range(3)]
        single_child_id = str(uuid.uuid4())
        all_child_ids = batch_child_ids + [single_child_id]

        parent_agent = "orchestrator"
        parent_keys = RedisKeys.format(
            namespace=test_namespace, task_id=parent_id, agent=parent_agent
        )

        # Setup parent
        await redis_client.hset(
            parent_keys.task_status, parent_id, "pending_child_tasks"
        )
        await redis_client.hset(parent_keys.task_agent, parent_id, parent_agent)
        await redis_client.hset(
            parent_keys.task_payload,
            parent_id,
            json.dumps({
                "query": "mixed children",
                "batch_ids": batch_child_ids,
                "single_id": single_child_id,
            }),
        )
        await redis_client.hset(parent_keys.task_pickups, parent_id, "1")
        await redis_client.hset(parent_keys.task_retries, parent_id, "0")
        await redis_client.hset(
            parent_keys.task_meta,
            parent_id,
            json.dumps({"owner_id": test_owner_id}),
        )

        # Track all children (batch + single)
        for child_id in all_child_ids:
            await redis_client.hset(
                parent_keys.pending_child_task_results, child_id, PENDING_SENTINEL
            )
            await redis_client.sadd(parent_keys.pending_child_wait_ids, child_id)

        await redis_client.zadd(
            parent_keys.queue_pending, {parent_id: time.time()}
        )

        # Verify all tracked
        pending = await redis_client.hgetall(parent_keys.pending_child_task_results)
        assert len(pending) == 4  # 3 batch + 1 single

        # Complete batch children
        completion_script = await create_task_completion_script(redis_client)

        for i, child_id in enumerate(batch_child_ids):
            child_keys = RedisKeys.format(
                namespace=test_namespace, task_id=child_id, agent="batch_worker"
            )

            # Setup child
            await redis_client.hset(child_keys.task_status, child_id, "processing")
            await redis_client.hset(child_keys.task_agent, child_id, "batch_worker")
            await redis_client.hset(
                child_keys.task_payload, child_id, json.dumps({"query": f"batch_{i}"})
            )
            await redis_client.hset(child_keys.task_pickups, child_id, "1")
            await redis_client.hset(child_keys.task_retries, child_id, "0")
            await redis_client.hset(
                child_keys.task_meta,
                child_id,
                json.dumps({"owner_id": test_owner_id, "parent_id": parent_id}),
            )

            # Complete
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
                task_id=child_id,
                action="complete",
                updated_task_payload_json=json.dumps({"query": f"batch_{i}"}),
                metrics_ttl=3600,
                pending_sentinel=PENDING_SENTINEL,
                current_turn=1,
                parent_pending_child_task_results_key=(
                    parent_keys.pending_child_task_results
                ),
                parent_pending_child_wait_ids_key=parent_keys.pending_child_wait_ids,
                pending_tool_call_ids_json=None,
                pending_child_task_ids_json=None,
                final_output_json=json.dumps({"batch_value": i * 10}),
            )

        # Parent should still be pending (single child not done)
        parent_status = await redis_client.hget(parent_keys.task_status, parent_id)
        assert parent_status == "pending_child_tasks"

        # Single child still pending
        single_result = await redis_client.hget(
            parent_keys.pending_child_task_results, single_child_id
        )
        assert single_result == PENDING_SENTINEL

        # Batch children completed
        for child_id in batch_child_ids:
            result = await redis_client.hget(
                parent_keys.pending_child_task_results, child_id
            )
            assert result != PENDING_SENTINEL
            assert "batch_value" in json.loads(str(result))
