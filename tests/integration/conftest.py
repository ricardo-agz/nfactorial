"""Integration test fixtures with Redis and test agents."""

import json
import uuid
from collections.abc import AsyncGenerator
from dataclasses import dataclass
from typing import Any, cast

import fakeredis.aioredis
import pytest
import pytest_asyncio
import redis.asyncio as redis

from factorial.agent import BaseAgent, TurnCompletion
from factorial.context import AgentContext
from factorial.queue.keys import PENDING_SENTINEL, RedisKeys
from factorial.queue.lua import (
    BackoffRecoveryScript,
    BatchPickupScript,
    BatchPickupScriptResult,
    CancelTaskScript,
    CancelTaskScriptResult,
    ChildTaskCompletionScript,
    ScheduledRecoveryScript,
    StaleRecoveryScript,
    StaleRecoveryScriptResult,
    TaskCompletionScript,
    TaskExpirationScript,
    TaskExpirationScriptResult,
    TaskSteeringScript,
    ToolCompletionScript,
    WaitScheduleScript,
    create_backoff_recovery_script,
    create_batch_pickup_script,
    create_cancel_task_script,
    create_child_task_completion_script,
    create_enqueue_batch_script,
    create_enqueue_task_script,
    create_scheduled_recovery_script,
    create_stale_recovery_script,
    create_task_completion_script,
    create_task_expiration_script,
    create_task_steering_script,
    create_tool_completion_script,
    create_wait_schedule_script,
)
from factorial.queue.lua._loader import (
    TaskCompletionScriptResult,
    TaskSteeringScriptResult,
)
from factorial.queue.operations import enqueue_task
from factorial.queue.task import Task, TaskStatus
from factorial.queue.worker import CompletionAction


class SimpleTestAgent(BaseAgent[AgentContext]):
    """A simple test agent that completes after one turn."""

    def __init__(self, name: str = "test_agent", max_turns: int | None = None):
        super().__init__(
            name=name,
            instructions="Test agent instructions",
            max_turns=max_turns,
            context_class=AgentContext,
        )

    async def run_turn(
        self,
        agent_ctx: AgentContext,
    ) -> TurnCompletion[AgentContext]:
        """Simple turn that completes immediately."""
        agent_ctx.turn += 1
        return TurnCompletion(
            is_done=True,
            context=agent_ctx,
            output={"result": "completed", "query": agent_ctx.query},
        )


class MultiTurnTestAgent(BaseAgent[AgentContext]):
    """A test agent that requires multiple turns to complete."""

    def __init__(
        self,
        name: str = "multi_turn_agent",
        turns_to_complete: int = 3,
        max_turns: int | None = None,
    ):
        super().__init__(
            name=name,
            instructions="Multi-turn test agent",
            max_turns=max_turns,
            context_class=AgentContext,
        )
        self.turns_to_complete = turns_to_complete

    async def run_turn(
        self,
        agent_ctx: AgentContext,
    ) -> TurnCompletion[AgentContext]:
        """Run a turn, completing after turns_to_complete iterations."""
        agent_ctx.turn += 1

        if agent_ctx.turn >= self.turns_to_complete:
            return TurnCompletion(
                is_done=True,
                context=agent_ctx,
                output={"result": "completed", "turns": agent_ctx.turn},
            )

        return TurnCompletion(
            is_done=False,
            context=agent_ctx,
        )


class FailingTestAgent(BaseAgent[AgentContext]):
    """A test agent that always fails."""

    def __init__(self, name: str = "failing_agent", error_message: str = "Test error"):
        super().__init__(
            name=name,
            instructions="Failing test agent",
            context_class=AgentContext,
        )
        self.error_message = error_message

    async def run_turn(
        self,
        agent_ctx: AgentContext,
    ) -> TurnCompletion[AgentContext]:
        """Always raise an exception."""
        raise ValueError(self.error_message)


class DeferredToolTestAgent(BaseAgent[AgentContext]):
    """A test agent that returns pending tool call IDs."""

    def __init__(self, name: str = "deferred_tool_agent"):
        super().__init__(
            name=name,
            instructions="Deferred tool test agent",
            context_class=AgentContext,
        )
        self._pending_tool_ids: list[str] = []

    def set_pending_tool_ids(self, tool_ids: list[str]) -> None:
        """Set tool IDs to return as pending."""
        self._pending_tool_ids = tool_ids

    async def run_turn(
        self,
        agent_ctx: AgentContext,
    ) -> TurnCompletion[AgentContext]:
        """Return pending tool call IDs."""
        if self._pending_tool_ids:
            return TurnCompletion(
                is_done=False,
                context=agent_ctx,
                pending_tool_call_ids=self._pending_tool_ids.copy(),
            )

        return TurnCompletion(
            is_done=True,
            context=agent_ctx,
            output={"result": "completed"},
        )


@pytest_asyncio.fixture
async def redis_client() -> AsyncGenerator[redis.Redis, None]:
    """Create a fakeredis client for testing."""
    client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    yield client
    await client.flushall()
    await client.aclose()


@pytest.fixture
def test_namespace() -> str:
    """Unique namespace for test isolation."""
    return f"test:{uuid.uuid4().hex[:8]}"


@pytest.fixture
def test_owner_id() -> str:
    """Test owner ID."""
    return str(uuid.uuid4())


@pytest.fixture
def test_agent() -> SimpleTestAgent:
    """Create a simple test agent."""
    return SimpleTestAgent()


@pytest.fixture
def multi_turn_agent() -> MultiTurnTestAgent:
    """Create a multi-turn test agent."""
    return MultiTurnTestAgent(turns_to_complete=3)


@pytest.fixture
def failing_agent() -> FailingTestAgent:
    """Create a failing test agent."""
    return FailingTestAgent()


@pytest.fixture
def deferred_tool_agent() -> DeferredToolTestAgent:
    """Create a deferred tool test agent."""
    return DeferredToolTestAgent()


@pytest.fixture
def agents_by_name(
    test_agent: SimpleTestAgent,
    multi_turn_agent: MultiTurnTestAgent,
    failing_agent: FailingTestAgent,
    deferred_tool_agent: DeferredToolTestAgent,
) -> dict[str, BaseAgent[Any]]:
    """Dictionary of agents by name for lookup."""
    return {
        test_agent.name: test_agent,
        multi_turn_agent.name: multi_turn_agent,
        failing_agent.name: failing_agent,
        deferred_tool_agent.name: deferred_tool_agent,
    }


@pytest.fixture
def sample_agent_context() -> AgentContext:
    """Create a sample AgentContext for testing."""
    return AgentContext(
        query="Test query",
        messages=[],
        turn=0,
        output=None,
    )


@pytest.fixture
def sample_task(
    test_agent: SimpleTestAgent,
    sample_agent_context: AgentContext,
    test_owner_id: str,
) -> Task[AgentContext]:
    """Create a sample task for testing."""
    return Task.create(
        owner_id=test_owner_id,
        agent=test_agent.name,
        payload=sample_agent_context,
        max_turns=10,
    )


@pytest.fixture
def redis_keys(test_namespace: str, test_agent: SimpleTestAgent) -> RedisKeys:
    """Create RedisKeys for testing."""
    return RedisKeys.format(namespace=test_namespace, agent=test_agent.name)


@pytest_asyncio.fixture
async def enqueue_script(redis_client: redis.Redis):
    """Create enqueue task script."""
    return await create_enqueue_task_script(redis_client)


@pytest_asyncio.fixture
async def pickup_script(redis_client: redis.Redis):
    """Create batch pickup script."""
    return await create_batch_pickup_script(redis_client)


@pytest_asyncio.fixture
async def completion_script(redis_client: redis.Redis):
    """Create task completion script."""
    return await create_task_completion_script(redis_client)


@pytest_asyncio.fixture
async def cancel_script(redis_client: redis.Redis):
    """Create cancel task script."""
    return await create_cancel_task_script(redis_client)


@pytest_asyncio.fixture
async def tool_completion_script(redis_client: redis.Redis):
    """Create tool completion script."""
    return await create_tool_completion_script(redis_client)


@pytest_asyncio.fixture
async def batch_enqueue_script(redis_client: redis.Redis):
    """Create batch enqueue script."""
    return await create_enqueue_batch_script(redis_client)


@pytest_asyncio.fixture
async def steering_script(redis_client: redis.Redis):
    """Create task steering script."""
    return await create_task_steering_script(redis_client)


@pytest_asyncio.fixture
async def stale_recovery_script(redis_client: redis.Redis):
    """Create stale recovery script."""
    return await create_stale_recovery_script(redis_client)


@pytest_asyncio.fixture
async def backoff_recovery_script(redis_client: redis.Redis):
    """Create backoff recovery script."""
    return await create_backoff_recovery_script(redis_client)


@pytest_asyncio.fixture
async def scheduled_recovery_script(redis_client: redis.Redis):
    """Create scheduled recovery script."""
    return await create_scheduled_recovery_script(redis_client)


@pytest_asyncio.fixture
async def expiration_script(redis_client: redis.Redis):
    """Create task expiration script."""
    return await create_task_expiration_script(redis_client)


@pytest_asyncio.fixture
async def child_completion_script(redis_client: redis.Redis):
    """Create child task completion script."""
    return await create_child_task_completion_script(redis_client)


@pytest_asyncio.fixture
async def wait_schedule_script(redis_client: redis.Redis):
    """Create wait schedule script."""
    return await create_wait_schedule_script(redis_client)


# =============================================================================
# Script Runner Helpers - Reduce boilerplate in tests
# =============================================================================


@dataclass
class ScriptRunner:
    """
    Helper class that wraps Lua script execution with pre-filled Redis keys.

    This dramatically reduces boilerplate in tests by handling key generation
    and providing simple method signatures for common operations.
    """

    redis_client: redis.Redis
    namespace: str
    agent_name: str
    keys: RedisKeys

    # Scripts (set after creation)
    _pickup_script: BatchPickupScript | None = None
    _completion_script: TaskCompletionScript | None = None
    _cancel_script: CancelTaskScript | None = None
    _tool_completion_script: ToolCompletionScript | None = None
    _child_completion_script: ChildTaskCompletionScript | None = None
    _steering_script: TaskSteeringScript | None = None
    _stale_recovery_script: StaleRecoveryScript | None = None
    _backoff_recovery_script: BackoffRecoveryScript | None = None
    _scheduled_recovery_script: ScheduledRecoveryScript | None = None
    _expiration_script: TaskExpirationScript | None = None
    _wait_schedule_script: WaitScheduleScript | None = None

    # Configuration
    metrics_ttl: int = 3600

    async def pickup(self, batch_size: int = 1) -> BatchPickupScriptResult:
        """Pick up tasks from the queue."""
        assert self._pickup_script is not None
        return await self._pickup_script.execute(
            queue_main_key=self.keys.queue_main,
            queue_cancelled_key=self.keys.queue_cancelled,
            queue_orphaned_key=self.keys.queue_orphaned,
            task_statuses_key=self.keys.task_status,
            task_agents_key=self.keys.task_agent,
            task_payloads_key=self.keys.task_payload,
            task_pickups_key=self.keys.task_pickups,
            task_retries_key=self.keys.task_retries,
            task_metas_key=self.keys.task_meta,
            task_cancellations_key=self.keys.task_cancellations,
            processing_heartbeats_key=self.keys.processing_heartbeats,
            agent_metrics_bucket_key=self.keys.agent_metrics_bucket,
            global_metrics_bucket_key=self.keys.global_metrics_bucket,
            batch_size=batch_size,
            metrics_ttl=self.metrics_ttl,
        )

    async def complete(
        self,
        task_id: str,
        action: CompletionAction,
        payload_json: str,
        current_turn: int = 0,
        pending_tool_call_ids: list[str] | None = None,
        pending_child_task_ids: list[str] | None = None,
        final_output: Any = None,
        parent_task_id: str | None = None,
    ) -> TaskCompletionScriptResult:
        """Complete a task with the given action."""
        assert self._completion_script is not None
        task_keys = RedisKeys.format(
            namespace=self.namespace,
            agent=self.agent_name,
            task_id=task_id,
        )
        parent_keys = (
            RedisKeys.format(namespace=self.namespace, task_id=parent_task_id)
            if parent_task_id
            else None
        )
        return await self._completion_script.execute(
            queue_main_key=self.keys.queue_main,
            queue_completions_key=self.keys.queue_completions,
            queue_failed_key=self.keys.queue_failed,
            queue_backoff_key=self.keys.queue_backoff,
            queue_orphaned_key=self.keys.queue_orphaned,
            queue_pending_key=self.keys.queue_pending,
            task_statuses_key=self.keys.task_status,
            task_agents_key=self.keys.task_agent,
            task_payloads_key=self.keys.task_payload,
            task_pickups_key=self.keys.task_pickups,
            task_retries_key=self.keys.task_retries,
            task_metas_key=self.keys.task_meta,
            processing_heartbeats_key=self.keys.processing_heartbeats,
            pending_tool_results_key=task_keys.pending_tool_results,
            pending_child_task_results_key=task_keys.pending_child_task_results,
            agent_metrics_bucket_key=self.keys.agent_metrics_bucket,
            global_metrics_bucket_key=self.keys.global_metrics_bucket,
            batch_meta_key=self.keys.batch_meta,
            batch_progress_key=self.keys.batch_progress,
            batch_remaining_tasks_key=self.keys.batch_remaining_tasks,
            batch_completed_key=self.keys.batch_completed,
            task_id=task_id,
            action=action.value,
            updated_task_payload_json=payload_json,
            metrics_ttl=self.metrics_ttl,
            pending_sentinel=PENDING_SENTINEL,
            current_turn=current_turn,
            pending_child_wait_ids_key=task_keys.pending_child_wait_ids,
            parent_pending_child_task_results_key=parent_keys.pending_child_task_results
            if parent_keys
            else None,
            parent_pending_child_wait_ids_key=parent_keys.pending_child_wait_ids
            if parent_keys
            else None,
            pending_tool_call_ids_json=json.dumps(pending_tool_call_ids)
            if pending_tool_call_ids
            else None,
            pending_child_task_ids_json=json.dumps(pending_child_task_ids)
            if pending_child_task_ids
            else None,
            final_output_json=json.dumps(final_output) if final_output else None,
        )

    async def cancel(self, task_id: str) -> CancelTaskScriptResult:
        """Cancel a task."""
        assert self._cancel_script is not None
        task_keys = RedisKeys.format(
            namespace=self.namespace,
            agent=self.agent_name,
            task_id=task_id,
        )
        return await self._cancel_script.execute(
            queue_cancelled_key=self.keys.queue_cancelled,
            queue_backoff_key=self.keys.queue_backoff,
            queue_orphaned_key=self.keys.queue_orphaned,
            queue_pending_key=self.keys.queue_pending,
            pending_cancellations_key=self.keys.task_cancellations,
            task_statuses_key=self.keys.task_status,
            task_agents_key=self.keys.task_agent,
            task_payloads_key=self.keys.task_payload,
            task_pickups_key=self.keys.task_pickups,
            task_retries_key=self.keys.task_retries,
            task_metas_key=self.keys.task_meta,
            pending_tool_results_key=task_keys.pending_tool_results,
            pending_child_task_results_key=task_keys.pending_child_task_results,
            agent_metrics_bucket_key=self.keys.agent_metrics_bucket,
            global_metrics_bucket_key=self.keys.global_metrics_bucket,
            task_id=task_id,
            metrics_ttl=self.metrics_ttl,
            queue_scheduled_key=self.keys.queue_scheduled,
            scheduled_wait_meta_key=self.keys.scheduled_wait_meta,
            pending_child_wait_ids_key=task_keys.pending_child_wait_ids,
        )

    async def complete_tool(
        self, task_id: str, payload_json: str
    ) -> tuple[bool, str]:
        """Complete tool results for a task."""
        assert self._tool_completion_script is not None
        task_keys = RedisKeys.format(
            namespace=self.namespace,
            agent=self.agent_name,
            task_id=task_id,
        )
        return await self._tool_completion_script.execute(
            queue_main_key=self.keys.queue_main,
            queue_orphaned_key=self.keys.queue_orphaned,
            queue_pending_key=self.keys.queue_pending,
            pending_tool_results_key=task_keys.pending_tool_results,
            task_statuses_key=self.keys.task_status,
            task_agents_key=self.keys.task_agent,
            task_payloads_key=self.keys.task_payload,
            task_pickups_key=self.keys.task_pickups,
            task_retries_key=self.keys.task_retries,
            task_metas_key=self.keys.task_meta,
            task_id=task_id,
            updated_task_context_json=payload_json,
        )

    async def complete_child(
        self, task_id: str, payload_json: str
    ) -> tuple[bool, str]:
        """Complete child task results for a task."""
        assert self._child_completion_script is not None
        task_keys = RedisKeys.format(
            namespace=self.namespace,
            agent=self.agent_name,
            task_id=task_id,
        )
        return await self._child_completion_script.execute(
            queue_main_key=self.keys.queue_main,
            queue_orphaned_key=self.keys.queue_orphaned,
            queue_pending_key=self.keys.queue_pending,
            pending_child_task_results_key=task_keys.pending_child_task_results,
            pending_child_wait_ids_key=task_keys.pending_child_wait_ids,
            task_statuses_key=self.keys.task_status,
            task_agents_key=self.keys.task_agent,
            task_payloads_key=self.keys.task_payload,
            task_pickups_key=self.keys.task_pickups,
            task_retries_key=self.keys.task_retries,
            task_metas_key=self.keys.task_meta,
            task_id=task_id,
            updated_task_context_json=payload_json,
        )

    async def steer(
        self,
        task_id: str,
        steering_message_ids: list[str],
        payload_json: str,
    ) -> TaskSteeringScriptResult:
        """Apply steering messages to a task."""
        assert self._steering_script is not None
        task_keys = RedisKeys.format(
            namespace=self.namespace,
            agent=self.agent_name,
            task_id=task_id,
        )
        return await self._steering_script.execute(
            queue_orphaned_key=self.keys.queue_orphaned,
            task_statuses_key=self.keys.task_status,
            task_agents_key=self.keys.task_agent,
            task_payloads_key=self.keys.task_payload,
            task_pickups_key=self.keys.task_pickups,
            task_retries_key=self.keys.task_retries,
            task_metas_key=self.keys.task_meta,
            steering_messages_key=task_keys.task_steering,
            task_id=task_id,
            steering_message_ids=steering_message_ids,
            updated_task_payload_json=payload_json,
        )

    async def recover_stale(
        self,
        cutoff_timestamp: float,
        max_recovery_batch: int = 100,
        max_retries: int = 3,
    ) -> StaleRecoveryScriptResult:
        """Recover stale tasks from processing heartbeats."""
        assert self._stale_recovery_script is not None
        return await self._stale_recovery_script.execute(
            queue_main_key=self.keys.queue_main,
            queue_failed_key=self.keys.queue_failed,
            queue_orphaned_key=self.keys.queue_orphaned,
            task_statuses_key=self.keys.task_status,
            task_agents_key=self.keys.task_agent,
            task_payloads_key=self.keys.task_payload,
            task_pickups_key=self.keys.task_pickups,
            task_retries_key=self.keys.task_retries,
            task_metas_key=self.keys.task_meta,
            processing_heartbeats_key=self.keys.processing_heartbeats,
            agent_metrics_bucket_key=self.keys.agent_metrics_bucket,
            global_metrics_bucket_key=self.keys.global_metrics_bucket,
            cutoff_timestamp=cutoff_timestamp,
            max_recovery_batch=max_recovery_batch,
            max_retries=max_retries,
            metrics_ttl=self.metrics_ttl,
        )

    async def recover_backoff(self, max_batch_size: int = 100) -> list[str]:
        """Recover tasks from backoff queue."""
        assert self._backoff_recovery_script is not None
        return await self._backoff_recovery_script.execute(
            queue_backoff_key=self.keys.queue_backoff,
            queue_main_key=self.keys.queue_main,
            queue_orphaned_key=self.keys.queue_orphaned,
            task_statuses_key=self.keys.task_status,
            task_agents_key=self.keys.task_agent,
            task_payloads_key=self.keys.task_payload,
            task_pickups_key=self.keys.task_pickups,
            task_retries_key=self.keys.task_retries,
            task_metas_key=self.keys.task_meta,
            max_batch_size=max_batch_size,
        )

    async def recover_scheduled(self, max_batch_size: int = 100) -> list[str]:
        """Recover tasks from scheduled queue."""
        assert self._scheduled_recovery_script is not None
        return await self._scheduled_recovery_script.execute(
            queue_scheduled_key=self.keys.queue_scheduled,
            queue_main_key=self.keys.queue_main,
            queue_orphaned_key=self.keys.queue_orphaned,
            task_statuses_key=self.keys.task_status,
            task_agents_key=self.keys.task_agent,
            task_payloads_key=self.keys.task_payload,
            task_pickups_key=self.keys.task_pickups,
            task_retries_key=self.keys.task_retries,
            task_metas_key=self.keys.task_meta,
            scheduled_wait_meta_key=self.keys.scheduled_wait_meta,
            max_batch_size=max_batch_size,
        )

    async def schedule_wait(
        self,
        *,
        task_id: str,
        payload_json: str,
        wake_timestamp: float,
        wait_metadata: dict[str, Any],
    ) -> tuple[bool, str]:
        """Park a processing task in scheduled wait state."""
        assert self._wait_schedule_script is not None
        result = await self._wait_schedule_script.execute(
            queue_scheduled_key=self.keys.queue_scheduled,
            queue_pending_key=self.keys.queue_pending,
            queue_orphaned_key=self.keys.queue_orphaned,
            processing_heartbeats_key=self.keys.processing_heartbeats,
            task_statuses_key=self.keys.task_status,
            task_agents_key=self.keys.task_agent,
            task_payloads_key=self.keys.task_payload,
            task_pickups_key=self.keys.task_pickups,
            task_retries_key=self.keys.task_retries,
            task_metas_key=self.keys.task_meta,
            scheduled_wait_meta_key=self.keys.scheduled_wait_meta,
            task_id=task_id,
            updated_task_payload_json=payload_json,
            wake_timestamp=wake_timestamp,
            wait_metadata_json=json.dumps(wait_metadata),
        )
        return result.success, result.message

    async def expire_tasks(
        self,
        completed_cutoff: float,
        failed_cutoff: float,
        cancelled_cutoff: float,
        max_cleanup_batch: int = 100,
    ) -> TaskExpirationScriptResult:
        """Expire old tasks from terminal queues."""
        assert self._expiration_script is not None
        return await self._expiration_script.execute(
            queue_completions_key=self.keys.queue_completions,
            queue_failed_key=self.keys.queue_failed,
            queue_cancelled_key=self.keys.queue_cancelled,
            queue_orphaned_key=self.keys.queue_orphaned,
            task_statuses_key=self.keys.task_status,
            task_agents_key=self.keys.task_agent,
            task_payloads_key=self.keys.task_payload,
            task_pickups_key=self.keys.task_pickups,
            task_retries_key=self.keys.task_retries,
            task_metas_key=self.keys.task_meta,
            completed_cutoff_timestamp=completed_cutoff,
            failed_cutoff_timestamp=failed_cutoff,
            cancelled_cutoff_timestamp=cancelled_cutoff,
            max_cleanup_batch=max_cleanup_batch,
        )

    # Convenience methods for common test patterns

    async def enqueue_and_pickup(
        self, agent: BaseAgent[Any], task: Task[AgentContext]
    ) -> str:
        """Enqueue a task and pick it up, returning the task_id."""
        task_id = await enqueue_task(
            redis_client=self.redis_client,
            namespace=self.namespace,
            agent=agent,
            task=task,
        )
        await self.pickup(batch_size=1)
        return task_id

    async def get_task_status(self, task_id: str) -> TaskStatus | None:
        """Get the current status of a task."""
        status_str = await cast(
            Any,
            self.redis_client.hget(self.keys.task_status, task_id),
        )
        return TaskStatus(status_str) if status_str else None

    async def mark_for_cancellation(self, task_id: str) -> None:
        """Mark a task for cancellation."""
        await cast(Any, self.redis_client.sadd(self.keys.task_cancellations, task_id))

    async def set_tool_result(
        self, task_id: str, tool_call_id: str, result: Any
    ) -> None:
        """Set a tool result for a pending task."""
        task_keys = RedisKeys.format(
            namespace=self.namespace,
            agent=self.agent_name,
            task_id=task_id,
        )
        await cast(
            Any,
            self.redis_client.hset(
                task_keys.pending_tool_results,
                tool_call_id,
                json.dumps(result),
            ),
        )

    async def set_child_result(
        self, task_id: str, child_task_id: str, result: Any
    ) -> None:
        """Set a child task result for a pending task."""
        task_keys = RedisKeys.format(
            namespace=self.namespace,
            agent=self.agent_name,
            task_id=task_id,
        )
        await cast(
            Any,
            self.redis_client.hset(
                task_keys.pending_child_task_results,
                child_task_id,
                json.dumps(result),
            ),
        )

    async def add_steering_message(
        self, task_id: str, message_id: str, message: dict[str, Any]
    ) -> None:
        """Add a steering message for a task."""
        task_keys = RedisKeys.format(
            namespace=self.namespace,
            task_id=task_id,
        )
        await cast(
            Any,
            self.redis_client.hset(
                task_keys.task_steering,
                message_id,
                json.dumps(message),
            ),
        )


@pytest_asyncio.fixture
async def script_runner(
    redis_client: redis.Redis,
    test_namespace: str,
    test_agent: SimpleTestAgent,
    pickup_script: BatchPickupScript,
    completion_script: TaskCompletionScript,
    cancel_script: CancelTaskScript,
    tool_completion_script: ToolCompletionScript,
    child_completion_script: ChildTaskCompletionScript,
    steering_script: TaskSteeringScript,
    stale_recovery_script: StaleRecoveryScript,
    backoff_recovery_script: BackoffRecoveryScript,
    scheduled_recovery_script: ScheduledRecoveryScript,
    expiration_script: TaskExpirationScript,
    wait_schedule_script: WaitScheduleScript,
) -> ScriptRunner:
    """
    Create a ScriptRunner with all scripts pre-loaded.

    Usage in tests:
        async def test_something(script_runner, sample_task, test_agent):
            task_id = await script_runner.enqueue_and_pickup(test_agent, sample_task)
            result = await script_runner.complete(
                task_id, CompletionAction.COMPLETE, sample_task.payload.to_json()
            )
            assert result.success
    """
    keys = RedisKeys.format(namespace=test_namespace, agent=test_agent.name)
    runner = ScriptRunner(
        redis_client=redis_client,
        namespace=test_namespace,
        agent_name=test_agent.name,
        keys=keys,
    )
    runner._pickup_script = pickup_script
    runner._completion_script = completion_script
    runner._cancel_script = cancel_script
    runner._tool_completion_script = tool_completion_script
    runner._child_completion_script = child_completion_script
    runner._steering_script = steering_script
    runner._stale_recovery_script = stale_recovery_script
    runner._backoff_recovery_script = backoff_recovery_script
    runner._scheduled_recovery_script = scheduled_recovery_script
    runner._expiration_script = expiration_script
    runner._wait_schedule_script = wait_schedule_script
    return runner
