"""E2E tests for multi-agent orchestration scenarios.

Tests the full flow of tasks being processed by multiple agents, including:
- Parent-child agent relationships
- Agent handoffs
- Concurrent multi-agent processing
"""

from __future__ import annotations

import asyncio
import json
import time
import uuid
from dataclasses import dataclass, field
from typing import Any

import pytest
import redis.asyncio as redis

from factorial.agent import BaseAgent, TurnCompletion
from factorial.context import AgentContext, ExecutionContext
from factorial.queue.keys import RedisKeys
from factorial.queue.lua import (
    create_batch_pickup_script,
    create_task_completion_script,
    create_task_steering_script,
)
from factorial.queue.operations import (
    PENDING_SENTINEL,
    enqueue_task,
    resume_if_no_remaining_child_tasks,
)
from factorial.queue.task import Task, TaskStatus, get_task_data, get_task_status
from factorial.queue.worker import process_task

# =============================================================================
# Multi-Agent Test Contexts
# =============================================================================


class OrchestratorContext(AgentContext):
    """Context for an orchestrator agent that spawns child tasks."""

    child_task_ids: list[str] = []
    child_results: dict[str, Any] = {}
    phase: str = "initial"


class WorkerContext(AgentContext):
    """Context for a worker agent that performs actual work."""

    work_type: str = "default"


# =============================================================================
# Multi-Agent Test Agents
# =============================================================================


@dataclass
class ChildSpawningAgent(BaseAgent[OrchestratorContext]):
    """An orchestrator agent that spawns child tasks.

    Behavior:
    1. First turn: spawn N child tasks and wait
    2. Resume turn: process child results and complete
    """

    num_children: int = 2
    child_agent_name: str = "worker_agent"
    spawn_on_turn: int = 1
    _spawned: bool = field(default=False, init=False)

    def __post_init__(self) -> None:
        super().__init__(
            name="orchestrator_agent",
            instructions="Orchestrator that spawns child tasks",
            context_class=OrchestratorContext,
        )

    async def run_turn(
        self,
        agent_ctx: OrchestratorContext,
    ) -> TurnCompletion[OrchestratorContext]:
        agent_ctx.turn += 1

        # Check if we have child results (resume after children complete)
        if agent_ctx.child_results:
            agent_ctx.phase = "completed"
            aggregated = sum(
                r.get("value", 0) for r in agent_ctx.child_results.values()
            )
            return TurnCompletion(
                is_done=True,
                context=agent_ctx,
                output={
                    "total": aggregated,
                    "child_count": len(agent_ctx.child_results),
                },
            )

        # First turn: spawn children
        if agent_ctx.turn == self.spawn_on_turn and not self._spawned:
            ctx = ExecutionContext.current()
            child_ids = []

            # Get child agent from agents_by_name (would be injected in real scenario)
            for i in range(self.num_children):
                # In real usage, we'd have access to the child agent
                # For testing, we'll return pending_child_task_ids directly
                if ctx.enqueue_child_task:
                    child_ids.append(f"child_{i}_{uuid.uuid4().hex[:8]}")

            agent_ctx.child_task_ids = child_ids
            agent_ctx.phase = "waiting_for_children"
            self._spawned = True

            return TurnCompletion(
                is_done=False,
                context=agent_ctx,
                pending_child_task_ids=child_ids,
            )

        # Shouldn't reach here
        return TurnCompletion(is_done=False, context=agent_ctx)


class SimpleWorkerAgent(BaseAgent[WorkerContext]):
    """A simple worker agent that processes a task and returns a result."""

    def __init__(
        self,
        name: str = "worker_agent",
        work_value: int = 10,
        turns_to_complete: int = 1,
    ):
        super().__init__(
            name=name,
            instructions="Worker agent that performs work",
            context_class=WorkerContext,
        )
        self.work_value = work_value
        self.turns_to_complete = turns_to_complete

    async def run_turn(
        self,
        agent_ctx: WorkerContext,
    ) -> TurnCompletion[WorkerContext]:
        agent_ctx.turn += 1

        if agent_ctx.turn >= self.turns_to_complete:
            return TurnCompletion(
                is_done=True,
                context=agent_ctx,
                output={
                    "value": self.work_value,
                    "work_type": agent_ctx.work_type,
                    "completed_turn": agent_ctx.turn,
                },
            )

        return TurnCompletion(is_done=False, context=agent_ctx)


class HandoffAgent(BaseAgent[AgentContext]):
    """An agent that hands off work to another agent after N turns."""

    def __init__(
        self,
        name: str = "handoff_agent",
        handoff_to: str | None = None,
        turns_before_handoff: int = 1,
    ):
        super().__init__(
            name=name,
            instructions="Agent that hands off to another agent",
            context_class=AgentContext,
        )
        self.handoff_to = handoff_to
        self.turns_before_handoff = turns_before_handoff

    async def run_turn(
        self,
        agent_ctx: AgentContext,
    ) -> TurnCompletion[AgentContext]:
        agent_ctx.turn += 1

        if self.handoff_to and agent_ctx.turn >= self.turns_before_handoff:
            # Create a child task for the handoff agent
            ctx = ExecutionContext.current()
            if ctx.enqueue_child_task:
                child_id = f"handoff_{uuid.uuid4().hex[:8]}"
                return TurnCompletion(
                    is_done=False,
                    context=agent_ctx,
                    pending_child_task_ids=[child_id],
                )

        return TurnCompletion(
            is_done=True,
            context=agent_ctx,
            output={"handled_by": self.name, "turns": agent_ctx.turn},
        )


# =============================================================================
# Test Fixtures
# =============================================================================


@pytest.fixture
def orchestrator_agent() -> ChildSpawningAgent:
    """Create an orchestrator agent."""
    return ChildSpawningAgent(num_children=2)


@pytest.fixture
def worker_agent() -> SimpleWorkerAgent:
    """Create a worker agent."""
    return SimpleWorkerAgent(work_value=10)


@pytest.fixture
def second_worker_agent() -> SimpleWorkerAgent:
    """Create a second worker agent with different value."""
    return SimpleWorkerAgent(name="worker_agent_2", work_value=20)


# =============================================================================
# Tests
# =============================================================================


@pytest.mark.asyncio
class TestMultiAgentBasicFlow:
    """Tests for basic multi-agent task flow."""

    async def test_two_agents_process_tasks_independently(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_owner_id: str,
        worker_agent: SimpleWorkerAgent,
        second_worker_agent: SimpleWorkerAgent,
    ) -> None:
        """Two different agents can process their tasks independently."""
        agent1_keys = RedisKeys.format(
            namespace=test_namespace, agent=worker_agent.name
        )
        agent2_keys = RedisKeys.format(
            namespace=test_namespace, agent=second_worker_agent.name
        )

        # Create tasks for both agents
        ctx1 = WorkerContext(query="Task for agent 1", work_type="type_a")
        task1 = Task.create(
            owner_id=test_owner_id,
            agent=worker_agent.name,
            payload=ctx1,
        )

        ctx2 = WorkerContext(query="Task for agent 2", work_type="type_b")
        task2 = Task.create(
            owner_id=test_owner_id,
            agent=second_worker_agent.name,
            payload=ctx2,
        )

        # Enqueue both tasks
        await enqueue_task(
            redis_client=redis_client,
            namespace=test_namespace,
            agent=worker_agent,
            task=task1,
        )
        await enqueue_task(
            redis_client=redis_client,
            namespace=test_namespace,
            agent=second_worker_agent,
            task=task2,
        )

        # Verify both are queued
        status1 = await get_task_status(redis_client, test_namespace, task1.id)
        status2 = await get_task_status(redis_client, test_namespace, task2.id)
        assert status1 == TaskStatus.QUEUED
        assert status2 == TaskStatus.QUEUED

        # Setup scripts
        pickup_script = await create_batch_pickup_script(redis_client)
        completion_script = await create_task_completion_script(redis_client)
        steering_script = await create_task_steering_script(redis_client)

        agents_by_name: dict[str, Any] = {
            worker_agent.name: worker_agent,
            second_worker_agent.name: second_worker_agent,
        }

        # Process both tasks concurrently
        async def process_agent_tasks(
            agent: SimpleWorkerAgent, keys: RedisKeys
        ) -> None:
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

            if result.tasks_to_process_ids:
                task_id = result.tasks_to_process_ids[0]
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

        await asyncio.gather(
            process_agent_tasks(worker_agent, agent1_keys),
            process_agent_tasks(second_worker_agent, agent2_keys),
        )

        # Verify both completed
        status1 = await get_task_status(redis_client, test_namespace, task1.id)
        status2 = await get_task_status(redis_client, test_namespace, task2.id)
        assert status1 == TaskStatus.COMPLETED
        assert status2 == TaskStatus.COMPLETED

        # Verify tasks were processed by checking turn count
        task1_data = await get_task_data(redis_client, test_namespace, task1.id)
        task2_data = await get_task_data(redis_client, test_namespace, task2.id)

        # Each agent completed in 1 turn
        assert task1_data["payload"]["turn"] == 1
        assert task2_data["payload"]["turn"] == 1


@pytest.mark.asyncio
class TestParentChildAgentFlow:
    """Tests for parent-child agent orchestration patterns."""

    async def test_parent_spawns_child_tasks_and_waits(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_owner_id: str,
    ) -> None:
        """Parent agent spawns children and enters pending state."""
        # Create a simple orchestrator
        orchestrator = ChildSpawningAgent(num_children=2)
        worker = SimpleWorkerAgent()

        orchestrator_keys = RedisKeys.format(
            namespace=test_namespace, agent=orchestrator.name
        )

        # Create orchestrator task
        ctx = OrchestratorContext(query="Orchestrate work")
        task = Task.create(
            owner_id=test_owner_id,
            agent=orchestrator.name,
            payload=ctx,
        )

        await enqueue_task(
            redis_client=redis_client,
            namespace=test_namespace,
            agent=orchestrator,
            task=task,
        )

        # Setup scripts
        pickup_script = await create_batch_pickup_script(redis_client)
        completion_script = await create_task_completion_script(redis_client)
        steering_script = await create_task_steering_script(redis_client)

        agents_by_name: dict[str, Any] = {
            orchestrator.name: orchestrator,
            worker.name: worker,
        }

        # Pickup orchestrator task
        result = await pickup_script.execute(
            queue_main_key=orchestrator_keys.queue_main,
            queue_cancelled_key=orchestrator_keys.queue_cancelled,
            queue_orphaned_key=orchestrator_keys.queue_orphaned,
            task_statuses_key=orchestrator_keys.task_status,
            task_agents_key=orchestrator_keys.task_agent,
            task_payloads_key=orchestrator_keys.task_payload,
            task_pickups_key=orchestrator_keys.task_pickups,
            task_retries_key=orchestrator_keys.task_retries,
            task_metas_key=orchestrator_keys.task_meta,
            task_cancellations_key=orchestrator_keys.task_cancellations,
            processing_heartbeats_key=orchestrator_keys.processing_heartbeats,
            agent_metrics_bucket_key=orchestrator_keys.agent_metrics_bucket,
            global_metrics_bucket_key=orchestrator_keys.global_metrics_bucket,
            batch_size=1,
            metrics_ttl=3600,
        )

        assert task.id in result.tasks_to_process_ids

        # Process orchestrator (will spawn children)
        await process_task(
            redis_client=redis_client,
            namespace=test_namespace,
            task_id=task.id,
            completion_script=completion_script,
            steering_script=steering_script,
            agent=orchestrator,
            agents_by_name=agents_by_name,
            max_retries=3,
            heartbeat_interval=5,
            task_timeout=60,
            metrics_retention_duration=3600,
        )

        # Verify orchestrator is waiting for children
        status = await get_task_status(redis_client, test_namespace, task.id)
        assert status == TaskStatus.PENDING_CHILD_TASKS

        # Verify children are tracked
        task_keys = RedisKeys.format(
            namespace=test_namespace, task_id=task.id, agent=orchestrator.name
        )
        pending_children = await redis_client.hgetall(
            task_keys.pending_child_task_results
        )
        assert len(pending_children) == 2  # 2 children spawned

    async def test_full_parent_child_lifecycle(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_owner_id: str,
    ) -> None:
        """Full lifecycle: parent spawns children, children complete, parent resumes."""
        parent_agent = "parent_agent"
        child_agent = "child_agent"
        parent_id = str(uuid.uuid4())
        child_ids = [str(uuid.uuid4()), str(uuid.uuid4())]

        parent_keys = RedisKeys.format(
            namespace=test_namespace, task_id=parent_id, agent=parent_agent
        )
        child1_keys = RedisKeys.format(
            namespace=test_namespace, task_id=child_ids[0], agent=child_agent
        )
        child2_keys = RedisKeys.format(
            namespace=test_namespace, task_id=child_ids[1], agent=child_agent
        )

        # Setup parent waiting for children
        await redis_client.hset(
            parent_keys.task_status, parent_id, "pending_child_tasks"
        )
        await redis_client.hset(parent_keys.task_agent, parent_id, parent_agent)
        await redis_client.hset(
            parent_keys.task_payload,
            parent_id,
            json.dumps({
                "query": "parent task",
                "turn": 1,
                "child_task_ids": child_ids,
            }),
        )
        await redis_client.hset(parent_keys.task_pickups, parent_id, 1)
        await redis_client.hset(parent_keys.task_retries, parent_id, 0)
        await redis_client.hset(
            parent_keys.task_meta,
            parent_id,
            json.dumps({
                "owner_id": test_owner_id,
                "created_at": time.time(),
            }),
        )

        # Track pending children
        for child_id in child_ids:
            await redis_client.hset(
                parent_keys.pending_child_task_results, child_id, PENDING_SENTINEL
            )

        await redis_client.zadd(parent_keys.queue_pending, {parent_id: time.time()})

        # Setup children in processing state
        for i, (child_id, child_keys) in enumerate(
            [(child_ids[0], child1_keys), (child_ids[1], child2_keys)]
        ):
            await redis_client.hset(child_keys.task_status, child_id, "processing")
            await redis_client.hset(child_keys.task_agent, child_id, child_agent)
            await redis_client.hset(
                child_keys.task_payload,
                child_id,
                json.dumps({"query": f"child task {i}", "turn": 1}),
            )
            await redis_client.hset(child_keys.task_pickups, child_id, 1)
            await redis_client.hset(child_keys.task_retries, child_id, 0)
            await redis_client.hset(
                child_keys.task_meta,
                child_id,
                json.dumps({
                    "owner_id": test_owner_id,
                    "parent_id": parent_id,
                    "created_at": time.time(),
                }),
            )

        # Create agents
        parent_agent_obj = SimpleWorkerAgent(name=parent_agent, work_value=100)
        child_agent_obj = SimpleWorkerAgent(name=child_agent, work_value=10)

        agents_by_name: dict[str, Any] = {
            parent_agent: parent_agent_obj,
            child_agent: child_agent_obj,
        }

        completion_script = await create_task_completion_script(redis_client)

        # Complete both children
        for i, (child_id, child_keys) in enumerate(
            [(child_ids[0], child1_keys), (child_ids[1], child2_keys)]
        ):
            child_output = {"value": (i + 1) * 10, "child_index": i}
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
                updated_task_payload_json=json.dumps({
                    "query": f"child {i}",
                    "turn": 1,
                }),
                metrics_ttl=3600,
                pending_sentinel=PENDING_SENTINEL,
                current_turn=1,
                parent_pending_child_task_results_key=(
                    parent_keys.pending_child_task_results
                ),
                pending_tool_call_ids_json=None,
                pending_child_task_ids_json=None,
                final_output_json=json.dumps(child_output),
            )

        # Verify both children completed
        status1 = await get_task_status(redis_client, test_namespace, child_ids[0])
        status2 = await get_task_status(redis_client, test_namespace, child_ids[1])
        assert status1 == TaskStatus.COMPLETED
        assert status2 == TaskStatus.COMPLETED

        # Resume parent using resume_if_no_remaining_child_tasks
        resumed = await resume_if_no_remaining_child_tasks(
            redis_client=redis_client,
            namespace=test_namespace,
            agents_by_name=agents_by_name,
            task_id=parent_id,
        )

        assert resumed is True

        # Verify parent is back to active
        parent_status = await get_task_status(redis_client, test_namespace, parent_id)
        assert parent_status == TaskStatus.ACTIVE

        # Verify parent is back in queue
        queue_contents = await redis_client.lrange(parent_keys.queue_main, 0, -1)
        assert parent_id in queue_contents


@pytest.mark.asyncio
class TestMultiAgentConcurrency:
    """Tests for concurrent multi-agent processing scenarios."""

    async def test_multiple_parents_with_children_process_concurrently(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_owner_id: str,
    ) -> None:
        """Multiple parent-child hierarchies can process concurrently."""
        parent_agent = "orchestrator"
        child_agent = "worker"

        # Create two parent-child hierarchies
        parent1_id = str(uuid.uuid4())
        parent2_id = str(uuid.uuid4())
        child1_id = str(uuid.uuid4())
        child2_id = str(uuid.uuid4())

        parent1_keys = RedisKeys.format(
            namespace=test_namespace, task_id=parent1_id, agent=parent_agent
        )
        parent2_keys = RedisKeys.format(
            namespace=test_namespace, task_id=parent2_id, agent=parent_agent
        )
        child1_keys = RedisKeys.format(
            namespace=test_namespace, task_id=child1_id, agent=child_agent
        )
        child2_keys = RedisKeys.format(
            namespace=test_namespace, task_id=child2_id, agent=child_agent
        )

        # Setup both parents waiting for their respective children
        for parent_id, parent_keys_obj, child_id in [
            (parent1_id, parent1_keys, child1_id),
            (parent2_id, parent2_keys, child2_id),
        ]:
            await redis_client.hset(
                parent_keys_obj.task_status, parent_id, "pending_child_tasks"
            )
            await redis_client.hset(
                parent_keys_obj.task_agent, parent_id, parent_agent
            )
            await redis_client.hset(
                parent_keys_obj.task_payload,
                parent_id,
                json.dumps({"query": f"parent {parent_id[:8]}"}),
            )
            await redis_client.hset(parent_keys_obj.task_pickups, parent_id, 1)
            await redis_client.hset(parent_keys_obj.task_retries, parent_id, 0)
            await redis_client.hset(
                parent_keys_obj.task_meta,
                parent_id,
                json.dumps({"owner_id": test_owner_id}),
            )
            await redis_client.hset(
                parent_keys_obj.pending_child_task_results,
                child_id,
                PENDING_SENTINEL,
            )

        # Setup both children
        for child_id, child_keys_obj, parent_id in [
            (child1_id, child1_keys, parent1_id),
            (child2_id, child2_keys, parent2_id),
        ]:
            await redis_client.hset(
                child_keys_obj.task_status, child_id, "processing"
            )
            await redis_client.hset(child_keys_obj.task_agent, child_id, child_agent)
            await redis_client.hset(
                child_keys_obj.task_payload,
                child_id,
                json.dumps({"query": f"child {child_id[:8]}"}),
            )
            await redis_client.hset(child_keys_obj.task_pickups, child_id, 1)
            await redis_client.hset(child_keys_obj.task_retries, child_id, 0)
            await redis_client.hset(
                child_keys_obj.task_meta,
                child_id,
                json.dumps({"owner_id": test_owner_id, "parent_id": parent_id}),
            )

        completion_script = await create_task_completion_script(redis_client)

        # Complete both children concurrently
        async def complete_child(
            c_id: str,
            c_keys: RedisKeys,
            p_keys: RedisKeys,
            value: int,
        ) -> None:
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
                task_id=c_id,
                action="complete",
                updated_task_payload_json=json.dumps({"query": "child"}),
                metrics_ttl=3600,
                pending_sentinel=PENDING_SENTINEL,
                current_turn=1,
                parent_pending_child_task_results_key=(
                    p_keys.pending_child_task_results
                ),
                pending_tool_call_ids_json=None,
                pending_child_task_ids_json=None,
                final_output_json=json.dumps({"value": value}),
            )

        await asyncio.gather(
            complete_child(child1_id, child1_keys, parent1_keys, 100),
            complete_child(child2_id, child2_keys, parent2_keys, 200),
        )

        # Both children should be completed
        status1 = await get_task_status(redis_client, test_namespace, child1_id)
        status2 = await get_task_status(redis_client, test_namespace, child2_id)
        assert status1 == TaskStatus.COMPLETED
        assert status2 == TaskStatus.COMPLETED

        # Verify correct results are in correct parents
        parent1_result = await redis_client.hget(
            parent1_keys.pending_child_task_results, child1_id
        )
        parent2_result = await redis_client.hget(
            parent2_keys.pending_child_task_results, child2_id
        )

        assert json.loads(str(parent1_result)) == {"value": 100}
        assert json.loads(str(parent2_result)) == {"value": 200}

        # Verify no cross-contamination
        cross1 = await redis_client.hget(
            parent1_keys.pending_child_task_results, child2_id
        )
        cross2 = await redis_client.hget(
            parent2_keys.pending_child_task_results, child1_id
        )
        assert cross1 is None
        assert cross2 is None


@pytest.mark.asyncio
class TestAgentRegistration:
    """Tests for agent registration and lookup in multi-agent scenarios."""

    async def test_child_task_uses_correct_agent(
        self,
        redis_client: redis.Redis,
        test_namespace: str,
        test_owner_id: str,
        worker_agent: SimpleWorkerAgent,
        second_worker_agent: SimpleWorkerAgent,
    ) -> None:
        """Child tasks are processed by the correct agent based on task agent field."""
        agent1_keys = RedisKeys.format(
            namespace=test_namespace, agent=worker_agent.name
        )
        agent2_keys = RedisKeys.format(
            namespace=test_namespace, agent=second_worker_agent.name
        )

        # Create tasks explicitly assigned to different agents
        ctx1 = WorkerContext(query="For agent 1")
        task1 = Task.create(
            owner_id=test_owner_id,
            agent=worker_agent.name,
            payload=ctx1,
        )

        ctx2 = WorkerContext(query="For agent 2")
        task2 = Task.create(
            owner_id=test_owner_id,
            agent=second_worker_agent.name,
            payload=ctx2,
        )

        await enqueue_task(redis_client, test_namespace, worker_agent, task1)
        await enqueue_task(redis_client, test_namespace, second_worker_agent, task2)

        # Verify tasks are in correct queues
        agent1_queue = await redis_client.lrange(agent1_keys.queue_main, 0, -1)
        agent2_queue = await redis_client.lrange(agent2_keys.queue_main, 0, -1)

        assert task1.id in agent1_queue
        assert task2.id in agent2_queue
        assert task1.id not in agent2_queue
        assert task2.id not in agent1_queue
