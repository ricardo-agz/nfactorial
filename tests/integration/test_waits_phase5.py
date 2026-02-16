import json
import time
from types import SimpleNamespace
from typing import Any, cast

import pytest
import redis.asyncio as redis
from openai.types.chat.chat_completion_message_function_tool_call import (
    ChatCompletionMessageFunctionToolCall,
    Function as ToolCallFunction,
)

from factorial.agent import BaseAgent, TurnCompletion
from factorial.context import AgentContext, ExecutionContext
from factorial.queue.keys import PENDING_SENTINEL, RedisKeys
from factorial.queue.lua import (
    BatchPickupScript,
    TaskCompletionScript,
    TaskSteeringScript,
)
from factorial.queue.operations import enqueue_task
from factorial.queue.task import Task, TaskStatus, get_task_data, get_task_status
from factorial.queue.worker import CompletionAction, process_task
from factorial.subagents import subagents
from factorial.waits import wait

from .conftest import ScriptRunner, SimpleTestAgent


def _make_tool_call(
    tool_name: str,
    call_id: str,
) -> ChatCompletionMessageFunctionToolCall:
    return ChatCompletionMessageFunctionToolCall(
        id=call_id,
        type="function",
        function=ToolCallFunction(name=tool_name, arguments="{}"),
    )


class _WaitSleepAgent(BaseAgent[AgentContext]):
    def __init__(self, *, name: str = "wait_sleep_agent", seconds: float = 5.0):
        super().__init__(
            name=name,
            instructions="Sleep wait agent",
            context_class=AgentContext,
        )
        self._seconds = seconds

    async def run_turn(
        self,
        agent_ctx: AgentContext,
    ) -> TurnCompletion[AgentContext]:
        agent_ctx.turn += 1
        return TurnCompletion(
            is_done=False,
            context=agent_ctx,
            tool_call_results=[
                (
                    _make_tool_call("sleep_wait", "call_wait_sleep"),
                    wait.sleep(self._seconds, message="cooling down"),
                )
            ],
        )


class _SubagentsRunAgent(BaseAgent[AgentContext]):
    def __init__(
        self,
        *,
        child_agent: BaseAgent[AgentContext],
        payloads: list[AgentContext],
        name: str = "wait_children_parent",
    ):
        super().__init__(
            name=name,
            instructions="subagents.run agent",
            context_class=AgentContext,
        )
        self._child_agent = child_agent
        self._payloads = payloads

    async def run_turn(
        self,
        agent_ctx: AgentContext,
    ) -> TurnCompletion[AgentContext]:
        agent_ctx.turn += 1
        run_instruction = await subagents.run(
            agent=self._child_agent,
            inputs=self._payloads,
            key="children",
            message="waiting on child tasks",
        )
        return TurnCompletion(
            is_done=False,
            context=agent_ctx,
            tool_call_results=[
                (
                    _make_tool_call("subagents_run", "call_subagents_run"),
                    run_instruction,
                )
            ],
        )


class _SpawnThenWaitJobsAgent(BaseAgent[AgentContext]):
    def __init__(
        self,
        *,
        research_agent: BaseAgent[AgentContext],
        research_inputs: list[AgentContext],
        risk_agent: BaseAgent[AgentContext],
        risk_inputs: list[AgentContext],
        name: str = "spawn_then_wait_jobs_parent",
    ):
        super().__init__(
            name=name,
            instructions="Spawn and wait on mixed subagents",
            context_class=AgentContext,
        )
        self._research_agent = research_agent
        self._research_inputs = research_inputs
        self._risk_agent = risk_agent
        self._risk_inputs = risk_inputs

    async def run_turn(
        self,
        agent_ctx: AgentContext,
    ) -> TurnCompletion[AgentContext]:
        agent_ctx.turn += 1
        research_jobs = await subagents.spawn(
            agent=self._research_agent,
            inputs=self._research_inputs,
            key="research",
        )
        risk_jobs = await subagents.spawn(
            agent=self._risk_agent,
            inputs=self._risk_inputs,
            key="risk",
        )
        return TurnCompletion(
            is_done=False,
            context=agent_ctx,
            tool_call_results=[
                (
                    _make_tool_call("wait_jobs", "call_wait_jobs"),
                    wait.jobs(
                        [*research_jobs, *risk_jobs],
                        message="Waiting for mixed subagent jobs",
                    ),
                )
            ],
        )


class _SpawnNoWaitAgent(BaseAgent[AgentContext]):
    def __init__(
        self,
        *,
        agent_a: BaseAgent[AgentContext],
        agent_a_inputs: list[AgentContext],
        agent_b: BaseAgent[AgentContext],
        agent_b_inputs: list[AgentContext],
        name: str = "spawn_no_wait_parent",
    ):
        super().__init__(
            name=name,
            instructions="Spawn subagents without waiting",
            context_class=AgentContext,
        )
        self._agent_a = agent_a
        self._agent_a_inputs = agent_a_inputs
        self._agent_b = agent_b
        self._agent_b_inputs = agent_b_inputs

    async def run_turn(
        self,
        agent_ctx: AgentContext,
    ) -> TurnCompletion[AgentContext]:
        agent_ctx.turn += 1
        jobs_a = await subagents.spawn(
            agent=self._agent_a,
            inputs=self._agent_a_inputs,
            key="a",
        )
        jobs_b = await subagents.spawn(
            agent=self._agent_b,
            inputs=self._agent_b_inputs,
            key="b",
        )
        return TurnCompletion(
            is_done=True,
            context=agent_ctx,
            output={"spawned": len(jobs_a) + len(jobs_b)},
        )


class _WaitReadyJobsAgent(BaseAgent[AgentContext]):
    def __init__(
        self,
        *,
        child_task_ids: list[str],
        name: str = "wait_ready_jobs_parent",
    ):
        super().__init__(
            name=name,
            instructions="Wait on pre-completed child jobs",
            context_class=AgentContext,
        )
        self._child_task_ids = child_task_ids

    async def run_turn(
        self,
        agent_ctx: AgentContext,
    ) -> TurnCompletion[AgentContext]:
        agent_ctx.turn += 1
        parent_task_id = ExecutionContext.current().task_id
        jobs = [
            {
                "task_id": child_task_id,
                "agent_name": "ready-child",
                "parent_task_id": parent_task_id,
                "key": "ready",
            }
            for child_task_id in self._child_task_ids
        ]
        return TurnCompletion(
            is_done=False,
            context=agent_ctx,
            tool_call_results=[
                (
                    _make_tool_call("wait_jobs", "call_wait_ready_jobs"),
                    wait.jobs(jobs, message="wait on already completed jobs"),
                )
            ],
        )


class _FailContinueCompletionScript:
    def __init__(self, base_script: TaskCompletionScript, *, task_id: str):
        self._base_script = base_script
        self._task_id = task_id
        self._failed_once = False

    async def execute(self, **kwargs: Any) -> Any:
        if (
            kwargs.get("task_id") == self._task_id
            and kwargs.get("action") == CompletionAction.CONTINUE.value
            and not self._failed_once
        ):
            self._failed_once = True
            return SimpleNamespace(success=False, batch_completed=False)
        return await self._base_script.execute(**kwargs)


class _RetrySpawnThenSucceedAgent(BaseAgent[AgentContext]):
    def __init__(
        self,
        *,
        child_agent: BaseAgent[AgentContext],
        child_inputs: list[AgentContext],
        name: str = "retry_spawn_parent",
    ):
        super().__init__(
            name=name,
            instructions="Spawn subagents, fail once, then succeed",
            context_class=AgentContext,
        )
        self._child_agent = child_agent
        self._child_inputs = child_inputs
        self._attempt = 0

    async def run_turn(
        self,
        agent_ctx: AgentContext,
    ) -> TurnCompletion[AgentContext]:
        agent_ctx.turn += 1
        await subagents.spawn(
            agent=self._child_agent,
            inputs=self._child_inputs,
            key="retry-spawn",
        )
        self._attempt += 1
        if self._attempt == 1:
            raise RuntimeError("simulated transient failure")

        return TurnCompletion(
            is_done=True,
            context=agent_ctx,
            output={"attempt": self._attempt},
        )


async def _pickup_single_task(
    *,
    redis_client: redis.Redis,
    keys: RedisKeys,
    pickup_script: BatchPickupScript,
) -> list[str]:
    pickup_result = await pickup_script.execute(
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
    return pickup_result.tasks_to_process_ids


@pytest.mark.asyncio
async def test_process_task_parks_wait_sleep_in_scheduled_queue(
    redis_client: redis.Redis,
    test_namespace: str,
    test_owner_id: str,
    pickup_script: BatchPickupScript,
    completion_script: TaskCompletionScript,
    steering_script: TaskSteeringScript,
) -> None:
    agent = _WaitSleepAgent(seconds=7.0)
    keys = RedisKeys.format(namespace=test_namespace, agent=agent.name)
    task = Task.create(
        owner_id=test_owner_id,
        agent=agent.name,
        payload=AgentContext(query="schedule me"),
    )
    task_id = await enqueue_task(
        redis_client=redis_client,
        namespace=test_namespace,
        agent=agent,
        task=task,
    )
    picked = await _pickup_single_task(
        redis_client=redis_client,
        keys=keys,
        pickup_script=pickup_script,
    )
    assert picked == [task_id]

    await process_task(
        redis_client=redis_client,
        namespace=test_namespace,
        task_id=task_id,
        completion_script=completion_script,
        steering_script=steering_script,
        agent=agent,
        agents_by_name={agent.name: agent},
        max_retries=3,
        heartbeat_interval=30,
        task_timeout=30,
        metrics_retention_duration=3600,
    )

    assert (
        await get_task_status(redis_client, test_namespace, task_id)
        == TaskStatus.PAUSED
    )
    wake_score = await cast(Any, redis_client.zscore(keys.queue_scheduled, task_id))
    assert wake_score is not None
    assert wake_score > time.time()

    wait_meta_json = await cast(
        Any,
        redis_client.hget(keys.scheduled_wait_meta, task_id),
    )
    assert wait_meta_json is not None
    wait_meta = json.loads(wait_meta_json)
    assert wait_meta["kind"] == "sleep"
    assert wait_meta["source_tool_call_ids"] == ["call_wait_sleep"]
    assert int(await cast(Any, redis_client.hget(keys.task_retries, task_id))) == 0


@pytest.mark.asyncio
async def test_process_task_subagents_run_spawns_children_and_parks_parent(
    redis_client: redis.Redis,
    test_namespace: str,
    test_owner_id: str,
    pickup_script: BatchPickupScript,
    completion_script: TaskCompletionScript,
    steering_script: TaskSteeringScript,
    test_agent: SimpleTestAgent,
) -> None:
    child_payloads = [
        AgentContext(query="child-1"),
        AgentContext(query="child-2"),
    ]
    parent_agent = _SubagentsRunAgent(
        child_agent=test_agent,
        payloads=child_payloads,
    )
    parent_keys = RedisKeys.format(namespace=test_namespace, agent=parent_agent.name)
    parent_task = Task.create(
        owner_id=test_owner_id,
        agent=parent_agent.name,
        payload=AgentContext(query="spawn children"),
    )
    parent_task_id = await enqueue_task(
        redis_client=redis_client,
        namespace=test_namespace,
        agent=parent_agent,
        task=parent_task,
    )
    picked = await _pickup_single_task(
        redis_client=redis_client,
        keys=parent_keys,
        pickup_script=pickup_script,
    )
    assert picked == [parent_task_id]

    await process_task(
        redis_client=redis_client,
        namespace=test_namespace,
        task_id=parent_task_id,
        completion_script=completion_script,
        steering_script=steering_script,
        agent=parent_agent,
        agents_by_name={
            parent_agent.name: parent_agent,
            test_agent.name: test_agent,
        },
        max_retries=3,
        heartbeat_interval=30,
        task_timeout=30,
        metrics_retention_duration=3600,
    )

    parent_task_keys = RedisKeys.format(
        namespace=test_namespace,
        agent=parent_agent.name,
        task_id=parent_task_id,
    )
    pending_children = cast(
        dict[str, str],
        await cast(
            Any,
            redis_client.hgetall(parent_task_keys.pending_child_task_results),
        ),
    )
    wait_ids = set(
        await cast(
            Any,
            redis_client.smembers(parent_task_keys.pending_child_wait_ids),
        )
    )
    assert (
        await get_task_status(redis_client, test_namespace, parent_task_id)
        == TaskStatus.PENDING_CHILD_TASKS
    )
    assert len(pending_children) == 2
    assert wait_ids == set(pending_children.keys())
    for child_task_id, child_value in pending_children.items():
        assert child_value == PENDING_SENTINEL
        child_task_data = await get_task_data(
            redis_client,
            test_namespace,
            child_task_id,
        )
        assert child_task_data["agent"] == test_agent.name
        assert child_task_data["metadata"]["parent_id"] == parent_task_id


@pytest.mark.asyncio
async def test_process_task_spawn_then_wait_jobs_supports_mixed_agents(
    redis_client: redis.Redis,
    test_namespace: str,
    test_owner_id: str,
    pickup_script: BatchPickupScript,
    completion_script: TaskCompletionScript,
    steering_script: TaskSteeringScript,
) -> None:
    research_agent = SimpleTestAgent(name="research_child")
    risk_agent = SimpleTestAgent(name="risk_child")
    parent_agent = _SpawnThenWaitJobsAgent(
        research_agent=research_agent,
        research_inputs=[
            AgentContext(query="research-1"),
            AgentContext(query="research-2"),
        ],
        risk_agent=risk_agent,
        risk_inputs=[AgentContext(query="risk-1")],
    )
    parent_keys = RedisKeys.format(namespace=test_namespace, agent=parent_agent.name)
    parent_task = Task.create(
        owner_id=test_owner_id,
        agent=parent_agent.name,
        payload=AgentContext(query="fan-out mixed"),
    )
    parent_task_id = await enqueue_task(
        redis_client=redis_client,
        namespace=test_namespace,
        agent=parent_agent,
        task=parent_task,
    )
    picked = await _pickup_single_task(
        redis_client=redis_client,
        keys=parent_keys,
        pickup_script=pickup_script,
    )
    assert picked == [parent_task_id]

    await process_task(
        redis_client=redis_client,
        namespace=test_namespace,
        task_id=parent_task_id,
        completion_script=completion_script,
        steering_script=steering_script,
        agent=parent_agent,
        agents_by_name={
            parent_agent.name: parent_agent,
            research_agent.name: research_agent,
            risk_agent.name: risk_agent,
        },
        max_retries=3,
        heartbeat_interval=30,
        task_timeout=30,
        metrics_retention_duration=3600,
    )

    parent_task_keys = RedisKeys.format(
        namespace=test_namespace,
        agent=parent_agent.name,
        task_id=parent_task_id,
    )
    pending_children = cast(
        dict[str, str],
        await cast(
            Any,
            redis_client.hgetall(parent_task_keys.pending_child_task_results),
        ),
    )
    wait_ids = set(
        await cast(
            Any,
            redis_client.smembers(parent_task_keys.pending_child_wait_ids),
        )
    )
    assert (
        await get_task_status(redis_client, test_namespace, parent_task_id)
        == TaskStatus.PENDING_CHILD_TASKS
    )
    assert len(pending_children) == 3
    assert wait_ids == set(pending_children.keys())

    child_agents = {
        (await get_task_data(redis_client, test_namespace, child_task_id))["agent"]
        for child_task_id in pending_children
    }
    assert child_agents == {"research_child", "risk_child"}


@pytest.mark.asyncio
async def test_process_task_spawn_without_wait_is_non_blocking(
    redis_client: redis.Redis,
    test_namespace: str,
    test_owner_id: str,
    pickup_script: BatchPickupScript,
    completion_script: TaskCompletionScript,
    steering_script: TaskSteeringScript,
) -> None:
    agent_a = SimpleTestAgent(name="child_a")
    agent_b = SimpleTestAgent(name="child_b")
    parent_agent = _SpawnNoWaitAgent(
        agent_a=agent_a,
        agent_a_inputs=[AgentContext(query="a1"), AgentContext(query="a2")],
        agent_b=agent_b,
        agent_b_inputs=[AgentContext(query="b1")],
    )
    parent_keys = RedisKeys.format(namespace=test_namespace, agent=parent_agent.name)
    parent_task = Task.create(
        owner_id=test_owner_id,
        agent=parent_agent.name,
        payload=AgentContext(query="spawn only"),
    )
    parent_task_id = await enqueue_task(
        redis_client=redis_client,
        namespace=test_namespace,
        agent=parent_agent,
        task=parent_task,
    )
    picked = await _pickup_single_task(
        redis_client=redis_client,
        keys=parent_keys,
        pickup_script=pickup_script,
    )
    assert picked == [parent_task_id]

    await process_task(
        redis_client=redis_client,
        namespace=test_namespace,
        task_id=parent_task_id,
        completion_script=completion_script,
        steering_script=steering_script,
        agent=parent_agent,
        agents_by_name={
            parent_agent.name: parent_agent,
            agent_a.name: agent_a,
            agent_b.name: agent_b,
        },
        max_retries=3,
        heartbeat_interval=30,
        task_timeout=30,
        metrics_retention_duration=3600,
    )

    assert (
        await get_task_status(redis_client, test_namespace, parent_task_id)
        == TaskStatus.COMPLETED
    )

    a_keys = RedisKeys.format(namespace=test_namespace, agent=agent_a.name)
    b_keys = RedisKeys.format(namespace=test_namespace, agent=agent_b.name)
    assert await cast(Any, redis_client.llen(a_keys.queue_main)) == 2
    assert await cast(Any, redis_client.llen(b_keys.queue_main)) == 1


@pytest.mark.asyncio
async def test_subagents_spawn_is_idempotent_across_parent_retry(
    redis_client: redis.Redis,
    test_namespace: str,
    test_owner_id: str,
    pickup_script: BatchPickupScript,
    completion_script: TaskCompletionScript,
    steering_script: TaskSteeringScript,
) -> None:
    child_agent = SimpleTestAgent(name="retry_spawn_child")
    parent_agent = _RetrySpawnThenSucceedAgent(
        child_agent=child_agent,
        child_inputs=[
            AgentContext(query="child-1"),
            AgentContext(query="child-2"),
        ],
    )

    parent_keys = RedisKeys.format(namespace=test_namespace, agent=parent_agent.name)
    child_keys = RedisKeys.format(namespace=test_namespace, agent=child_agent.name)
    parent_task = Task.create(
        owner_id=test_owner_id,
        agent=parent_agent.name,
        payload=AgentContext(query="retry spawn parent"),
    )
    parent_task_id = await enqueue_task(
        redis_client=redis_client,
        namespace=test_namespace,
        agent=parent_agent,
        task=parent_task,
    )

    first_pick = await _pickup_single_task(
        redis_client=redis_client,
        keys=parent_keys,
        pickup_script=pickup_script,
    )
    assert first_pick == [parent_task_id]
    await process_task(
        redis_client=redis_client,
        namespace=test_namespace,
        task_id=parent_task_id,
        completion_script=completion_script,
        steering_script=steering_script,
        agent=parent_agent,
        agents_by_name={
            parent_agent.name: parent_agent,
            child_agent.name: child_agent,
        },
        max_retries=3,
        heartbeat_interval=30,
        task_timeout=30,
        metrics_retention_duration=3600,
    )
    assert (
        await get_task_status(redis_client, test_namespace, parent_task_id)
        == TaskStatus.ACTIVE
    )
    assert await cast(Any, redis_client.llen(child_keys.queue_main)) == 2

    second_pick = await _pickup_single_task(
        redis_client=redis_client,
        keys=parent_keys,
        pickup_script=pickup_script,
    )
    assert second_pick == [parent_task_id]
    await process_task(
        redis_client=redis_client,
        namespace=test_namespace,
        task_id=parent_task_id,
        completion_script=completion_script,
        steering_script=steering_script,
        agent=parent_agent,
        agents_by_name={
            parent_agent.name: parent_agent,
            child_agent.name: child_agent,
        },
        max_retries=3,
        heartbeat_interval=30,
        task_timeout=30,
        metrics_retention_duration=3600,
    )

    assert (
        await get_task_status(redis_client, test_namespace, parent_task_id)
        == TaskStatus.COMPLETED
    )
    child_queue_ids = cast(
        list[str],
        await cast(Any, redis_client.lrange(child_keys.queue_main, 0, -1)),
    )
    assert len(child_queue_ids) == 2
    assert len(set(child_queue_ids)) == 2


@pytest.mark.asyncio
async def test_wait_jobs_fast_path_preserves_results_when_continue_rejected(
    redis_client: redis.Redis,
    test_namespace: str,
    test_owner_id: str,
    pickup_script: BatchPickupScript,
    completion_script: TaskCompletionScript,
    steering_script: TaskSteeringScript,
) -> None:
    child_task_ids = ["ready-child-1", "ready-child-2"]
    parent_agent = _WaitReadyJobsAgent(child_task_ids=child_task_ids)
    parent_keys = RedisKeys.format(namespace=test_namespace, agent=parent_agent.name)
    parent_task = Task.create(
        owner_id=test_owner_id,
        agent=parent_agent.name,
        payload=AgentContext(query="wait on ready jobs"),
    )
    parent_task_id = await enqueue_task(
        redis_client=redis_client,
        namespace=test_namespace,
        agent=parent_agent,
        task=parent_task,
    )
    picked = await _pickup_single_task(
        redis_client=redis_client,
        keys=parent_keys,
        pickup_script=pickup_script,
    )
    assert picked == [parent_task_id]

    parent_task_keys = RedisKeys.format(
        namespace=test_namespace,
        agent=parent_agent.name,
        task_id=parent_task_id,
    )
    for idx, child_task_id in enumerate(child_task_ids):
        await cast(
            Any,
            redis_client.hset(
                parent_task_keys.pending_child_task_results,
                child_task_id,
                json.dumps({"result": idx}),
            ),
        )

    flaky_completion = _FailContinueCompletionScript(
        completion_script,
        task_id=parent_task_id,
    )
    await process_task(
        redis_client=redis_client,
        namespace=test_namespace,
        task_id=parent_task_id,
        completion_script=cast(Any, flaky_completion),
        steering_script=steering_script,
        agent=parent_agent,
        agents_by_name={parent_agent.name: parent_agent},
        max_retries=3,
        heartbeat_interval=30,
        task_timeout=30,
        metrics_retention_duration=3600,
    )

    remaining_results = cast(
        dict[str, str],
        await cast(
            Any,
            redis_client.hgetall(parent_task_keys.pending_child_task_results),
        ),
    )
    assert set(remaining_results.keys()) == set(child_task_ids)
    assert all(value != PENDING_SENTINEL for value in remaining_results.values())


@pytest.mark.asyncio
async def test_scheduled_recovery_requeues_due_paused_task(
    redis_client: redis.Redis,
    script_runner: ScriptRunner,
    test_agent: SimpleTestAgent,
    sample_task: Task[AgentContext],
    test_namespace: str,
) -> None:
    task_id = await script_runner.enqueue_and_pickup(test_agent, sample_task)
    success, message = await script_runner.schedule_wait(
        task_id=task_id,
        payload_json=sample_task.payload.to_json(),
        wake_timestamp=time.time() - 1,
        wait_metadata={"kind": "sleep"},
    )
    assert success, message
    assert (
        await get_task_status(redis_client, test_namespace, task_id)
        == TaskStatus.PAUSED
    )

    recovered_ids = await script_runner.recover_scheduled(max_batch_size=10)
    assert task_id in recovered_ids
    assert (
        await get_task_status(redis_client, test_namespace, task_id)
        == TaskStatus.ACTIVE
    )
    assert await cast(
        Any,
        redis_client.hget(script_runner.keys.scheduled_wait_meta, task_id),
    ) is None


@pytest.mark.asyncio
async def test_pending_child_parking_preserves_already_completed_child_results(
    redis_client: redis.Redis,
    script_runner: ScriptRunner,
    test_agent: SimpleTestAgent,
    sample_task: Task[AgentContext],
) -> None:
    task_id = await script_runner.enqueue_and_pickup(test_agent, sample_task)
    parent_task_keys = RedisKeys.format(
        namespace=script_runner.namespace,
        task_id=task_id,
    )
    child_done_id = "child-done"
    child_pending_id = "child-pending"
    child_done_result = {"result": "already done"}
    await cast(
        Any,
        redis_client.hset(
            parent_task_keys.pending_child_task_results,
            child_done_id,
            json.dumps(child_done_result),
        ),
    )

    await script_runner.complete(
        task_id=task_id,
        action=CompletionAction.PENDING_CHILD,
        payload_json=sample_task.payload.to_json(),
        pending_child_task_ids=[child_done_id, child_pending_id],
    )

    all_results = cast(
        dict[str, str],
        await cast(
            Any,
            redis_client.hgetall(parent_task_keys.pending_child_task_results),
        ),
    )
    wait_ids = set(
        await cast(
            Any,
            redis_client.smembers(parent_task_keys.pending_child_wait_ids),
        )
    )
    assert all_results[child_done_id] == json.dumps(child_done_result)
    assert all_results[child_pending_id] == PENDING_SENTINEL
    assert wait_ids == {child_done_id, child_pending_id}
    assert (
        await get_task_status(redis_client, script_runner.namespace, task_id)
        == TaskStatus.PENDING_CHILD_TASKS
    )


@pytest.mark.asyncio
async def test_cancel_paused_task_clears_scheduled_wait_state(
    redis_client: redis.Redis,
    script_runner: ScriptRunner,
    test_agent: SimpleTestAgent,
    sample_task: Task[AgentContext],
    test_namespace: str,
) -> None:
    task_id = await script_runner.enqueue_and_pickup(test_agent, sample_task)
    success, message = await script_runner.schedule_wait(
        task_id=task_id,
        payload_json=sample_task.payload.to_json(),
        wake_timestamp=time.time() + 120,
        wait_metadata={"kind": "sleep"},
    )
    assert success, message
    assert (
        await get_task_status(redis_client, test_namespace, task_id)
        == TaskStatus.PAUSED
    )

    cancel_result = await script_runner.cancel(task_id)
    assert cancel_result.success
    assert (
        await get_task_status(redis_client, test_namespace, task_id)
        == TaskStatus.CANCELLED
    )
    assert (
        await redis_client.zscore(script_runner.keys.queue_cancelled, task_id)
        is not None
    )
    assert (
        await redis_client.zscore(script_runner.keys.queue_scheduled, task_id)
        is None
    )
    assert await cast(
        Any,
        redis_client.hget(script_runner.keys.scheduled_wait_meta, task_id),
    ) is None
