"""E2E tests for the worker loop and task processing flow.

These tests exercise the full path from task enqueue through worker processing
to completion, using mock LLMs and scripted agents.
"""

import asyncio
from typing import Any

import pytest
import redis.asyncio as redis

from factorial.context import AgentContext
from factorial.queue.keys import RedisKeys
from factorial.queue.lua import (
    create_batch_pickup_script,
    create_task_completion_script,
    create_task_steering_script,
)
from factorial.queue.operations import enqueue_task
from factorial.queue.task import Task, TaskStatus, get_task_data, get_task_status
from factorial.queue.worker import process_task

from .conftest import ScriptedAgent


@pytest.mark.asyncio
class TestWorkerProcessTask:
    """Tests for the process_task function (core worker logic)."""

    async def test_simple_task_completes_through_worker(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_owner_id: str,
        scripted_agent: ScriptedAgent,
    ) -> None:
        """Test that a simple task is processed to completion."""
        # Create task
        ctx = AgentContext(query="Test query")
        task = Task.create(
            owner_id=test_owner_id,
            agent=scripted_agent.name,
            payload=ctx,
        )

        # Enqueue
        task_id = await enqueue_task(
            redis_client=redis_client,
            namespace=test_namespace,
            agent=scripted_agent,
            task=task,
        )

        # Verify queued
        status = await get_task_status(redis_client, test_namespace, task_id)
        assert status == TaskStatus.QUEUED

        # Process through worker
        keys = RedisKeys.format(namespace=test_namespace, agent=scripted_agent.name)
        pickup_script = await create_batch_pickup_script(redis_client)
        completion_script = await create_task_completion_script(redis_client)
        steering_script = await create_task_steering_script(redis_client)

        # Pickup
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

        # Process the task
        agents_by_name: dict[str, Any] = {scripted_agent.name: scripted_agent}

        await process_task(
            redis_client=redis_client,
            namespace=test_namespace,
            task_id=task_id,
            completion_script=completion_script,
            steering_script=steering_script,
            agent=scripted_agent,
            agents_by_name=agents_by_name,
            max_retries=3,
            heartbeat_interval=5,
            task_timeout=60,
            metrics_retention_duration=3600,
        )

        # Verify completed
        status = await get_task_status(redis_client, test_namespace, task_id)
        assert status == TaskStatus.COMPLETED

    async def test_multi_turn_task_requeues_until_complete(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_owner_id: str,
        multi_turn_scripted_agent: ScriptedAgent,
    ) -> None:
        """Test that multi-turn tasks are requeued between turns."""
        agent = multi_turn_scripted_agent
        keys = RedisKeys.format(namespace=test_namespace, agent=agent.name)

        # Create task
        ctx = AgentContext(query="Multi-turn query")
        task = Task.create(
            owner_id=test_owner_id,
            agent=agent.name,
            payload=ctx,
        )

        await enqueue_task(
            redis_client=redis_client,
            namespace=test_namespace,
            agent=agent,
            task=task,
        )

        # Get scripts
        pickup_script = await create_batch_pickup_script(redis_client)
        completion_script = await create_task_completion_script(redis_client)
        steering_script = await create_task_steering_script(redis_client)
        agents_by_name: dict[str, Any] = {agent.name: agent}

        # Process multiple turns
        for _turn in range(3):
            # Pickup
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

            if not result.tasks_to_process_ids:
                break  # Task is done

            task_id = result.tasks_to_process_ids[0]

            # Process
            await process_task(
                redis_client=redis_client,
                namespace=test_namespace,
                task_id=task_id,
                completion_script=completion_script,
                steering_script=steering_script,
                agent=agent,
                agents_by_name=agents_by_name,
                max_retries=3,
                heartbeat_interval=5,
                task_timeout=60,
                metrics_retention_duration=3600,
            )

        # Verify completed after 3 turns
        status = await get_task_status(redis_client, test_namespace, task.id)
        assert status == TaskStatus.COMPLETED

        # Verify turn count in payload
        task_data = await get_task_data(redis_client, test_namespace, task.id)
        assert task_data["payload"]["turn"] == 3

    async def test_failing_task_retries_then_fails(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_owner_id: str,
    ) -> None:
        """Test that failing tasks retry up to max_retries then fail permanently."""
        # Create agent that fails on every turn
        agent = ScriptedAgent(
            name="always_failing",
            fail_on_turn=1,  # Fail immediately
        )
        keys = RedisKeys.format(namespace=test_namespace, agent=agent.name)

        # Create task
        ctx = AgentContext(query="Will fail")
        task = Task.create(
            owner_id=test_owner_id,
            agent=agent.name,
            payload=ctx,
        )

        await enqueue_task(
            redis_client=redis_client,
            namespace=test_namespace,
            agent=agent,
            task=task,
        )

        # Get scripts
        pickup_script = await create_batch_pickup_script(redis_client)
        completion_script = await create_task_completion_script(redis_client)
        steering_script = await create_task_steering_script(redis_client)
        agents_by_name: dict[str, Any] = {agent.name: agent}

        max_retries = 2

        # Process until failed (should take max_retries + 1 attempts)
        for _attempt in range(max_retries + 2):
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

            if not result.tasks_to_process_ids:
                break

            task_id = result.tasks_to_process_ids[0]

            await process_task(
                redis_client=redis_client,
                namespace=test_namespace,
                task_id=task_id,
                completion_script=completion_script,
                steering_script=steering_script,
                agent=agent,
                agents_by_name=agents_by_name,
                max_retries=max_retries,
                heartbeat_interval=5,
                task_timeout=60,
                metrics_retention_duration=3600,
            )

        # Verify task failed
        status = await get_task_status(redis_client, test_namespace, task.id)
        assert status == TaskStatus.FAILED

        # Verify retries were incremented
        task_data = await get_task_data(redis_client, test_namespace, task.id)
        assert task_data["retries"] == max_retries


@pytest.mark.asyncio
class TestWorkerCancellation:
    """Tests for task cancellation during worker processing."""

    async def test_cancellation_during_pickup(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_owner_id: str,
        scripted_agent: ScriptedAgent,
    ) -> None:
        """Test that tasks marked for cancellation are cancelled at pickup."""
        keys = RedisKeys.format(namespace=test_namespace, agent=scripted_agent.name)

        # Create and enqueue task
        ctx = AgentContext(query="To be cancelled")
        task = Task.create(
            owner_id=test_owner_id,
            agent=scripted_agent.name,
            payload=ctx,
        )

        await enqueue_task(
            redis_client=redis_client,
            namespace=test_namespace,
            agent=scripted_agent,
            task=task,
        )

        # Mark for cancellation BEFORE pickup
        await redis_client.sadd(keys.task_cancellations, task.id)  # type: ignore[misc]

        # Pickup
        pickup_script = await create_batch_pickup_script(redis_client)
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

        # Task should be in cancel list, not process list
        assert task.id in result.tasks_to_cancel_ids
        assert task.id not in result.tasks_to_process_ids

        # Task should be cancelled
        status = await get_task_status(redis_client, test_namespace, task.id)
        assert status == TaskStatus.CANCELLED


@pytest.mark.asyncio
class TestWorkerBatchProcessing:
    """Tests for batch task processing."""

    async def test_processes_batch_of_tasks(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_owner_id: str,
    ) -> None:
        """Test that multiple tasks are picked up and processed in a batch."""
        agent = ScriptedAgent(name="batch_agent")
        keys = RedisKeys.format(namespace=test_namespace, agent=agent.name)

        # Enqueue 5 tasks
        task_ids = []
        for i in range(5):
            ctx = AgentContext(query=f"Query {i}")
            task = Task.create(
                owner_id=test_owner_id,
                agent=agent.name,
                payload=ctx,
            )
            await enqueue_task(
                redis_client=redis_client,
                namespace=test_namespace,
                agent=agent,
                task=task,
            )
            task_ids.append(task.id)

        # Pickup batch of 3
        pickup_script = await create_batch_pickup_script(redis_client)
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
            batch_size=3,
            metrics_ttl=3600,
        )

        # Should have picked up 3
        assert len(result.tasks_to_process_ids) == 3

        # Remaining 2 should still be in queue
        queue_len = await redis_client.llen(keys.queue_main)  # type: ignore[misc]
        assert queue_len == 2

        # Process the batch
        completion_script = await create_task_completion_script(redis_client)
        steering_script = await create_task_steering_script(redis_client)
        agents_by_name: dict[str, Any] = {agent.name: agent}

        tasks = [
            process_task(
                redis_client=redis_client,
                namespace=test_namespace,
                task_id=tid,
                completion_script=completion_script,
                steering_script=steering_script,
                agent=agent,
                agents_by_name=agents_by_name,
                max_retries=3,
                heartbeat_interval=5,
                task_timeout=60,
                metrics_retention_duration=3600,
            )
            for tid in result.tasks_to_process_ids
        ]
        await asyncio.gather(*tasks)

        # All 3 should be completed
        for tid in result.tasks_to_process_ids:
            status = await get_task_status(redis_client, test_namespace, tid)
            assert status == TaskStatus.COMPLETED
