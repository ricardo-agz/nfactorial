"""Integration coverage for wait.until_signal and subagents signaling."""

from __future__ import annotations

import json
from typing import Any, cast

import pytest
import redis.asyncio as redis
from openai.types.chat.chat_completion_message_function_tool_call import (
    ChatCompletionMessageFunctionToolCall,
    Function as ToolCallFunction,
)

from factorial.agent import BaseAgent, TurnCompletion
from factorial.ai.models import Model, Provider
from factorial.agent.context import AgentContext

MOCK_MODEL = Model(
    name="mock-model",
    provider=Provider.OPENAI,
    provider_model_id="mock-v1",
    context_window=128000,
)
from factorial.execution.signals import signals
from factorial.execution.waits import wait
from factorial.queue.keys import RedisKeys
from factorial.queue.lua import (
    BatchPickupScript,
    TaskCompletionScript,
    TaskSteeringScript,
    create_scheduled_recovery_script,
)
from factorial.queue.operations import enqueue_task, signal_task
from factorial.queue.task import Task, TaskStatus, get_task_status
from factorial.queue.worker import process_task


def _make_tool_call(
    tool_name: str,
    call_id: str,
) -> ChatCompletionMessageFunctionToolCall:
    return ChatCompletionMessageFunctionToolCall(
        id=call_id,
        type="function",
        function=ToolCallFunction(name=tool_name, arguments="{}"),
    )


class _WaitForSignalAgent(BaseAgent[AgentContext]):
    def __init__(
        self,
        *,
        signal_id: str,
        timeout: Any = None,
        name: str = "wait_signal_agent",
    ):
        super().__init__(
            name=name,
            instructions="Signal wait agent",
            model=MOCK_MODEL,
        )
        self._signal_id = signal_id
        self._timeout = timeout

    async def run_turn(
        self,
        agent_ctx: AgentContext,
    ) -> TurnCompletion[AgentContext]:
        agent_ctx.turn_number += 1
        current_signal = signals.current()
        if current_signal is not None and current_signal.signal_id == self._signal_id:
            return TurnCompletion(
                is_done=True,
                context=agent_ctx,
                output={
                    "wake_reason": signals.wake_reason(),
                    "signal_id": current_signal.signal_id,
                    "payload": current_signal.payload,
                },
            )
        if signals.wake_reason() == "timeout":
            return TurnCompletion(
                is_done=True,
                context=agent_ctx,
                output={"wake_reason": "timeout"},
            )

        return TurnCompletion(
            is_done=False,
            context=agent_ctx,
            tool_call_results=[
                (
                    _make_tool_call("wait_for_signal", "call_wait_signal"),
                    wait.until_signal(
                        self._signal_id,
                        timeout=self._timeout,
                        data={"reason": "awaiting signal"},
                    ),
                )
            ],
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
async def test_process_task_parks_wait_until_signal(
    redis_client: redis.Redis,
    test_namespace: str,
    test_owner_id: str,
    pickup_script: BatchPickupScript,
    completion_script: TaskCompletionScript,
    steering_script: TaskSteeringScript,
) -> None:
    agent = _WaitForSignalAgent(signal_id="day_vote_open:1")
    keys = RedisKeys.format(namespace=test_namespace, agent=agent.name)
    task = Task.create(
        owner_id=test_owner_id,
        agent=agent.name,
        payload=AgentContext(messages=[{"role": "user", "content": "wait for signal"}]),
        max_turns=10,
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
        max_retries=1,
        heartbeat_interval=30,
        task_timeout=60,
        metrics_retention_duration=3600,
    )

    assert await get_task_status(redis_client, test_namespace, task_id) == TaskStatus.PAUSED
    wait_meta_raw = await redis_client.hget(keys.signal_wait_meta, task_id)
    assert wait_meta_raw is not None
    wait_meta = json.loads(wait_meta_raw)
    assert wait_meta["kind"] == "signal"
    assert wait_meta["signal_id"] == "day_vote_open:1"
    assert await redis_client.zscore(keys.queue_pending, task_id) is not None
    assert await redis_client.hget(keys.signal_wake_meta, task_id) is None


@pytest.mark.asyncio
async def test_signal_task_wakes_waiting_signal_task(
    redis_client: redis.Redis,
    test_namespace: str,
    test_owner_id: str,
    pickup_script: BatchPickupScript,
    completion_script: TaskCompletionScript,
    steering_script: TaskSteeringScript,
) -> None:
    sender_agent = _WaitForSignalAgent(
        signal_id="unused-sender-signal",
        name="signal_sender_agent",
    )
    receiver_agent = _WaitForSignalAgent(
        signal_id="day_vote_open:2",
        name="signal_receiver_agent",
    )
    sender_task = Task.create(
        owner_id=test_owner_id,
        agent=sender_agent.name,
        payload=AgentContext(messages=[{"role": "user", "content": "sender"}]),
        max_turns=5,
    )
    receiver_task = Task.create(
        owner_id=test_owner_id,
        agent=receiver_agent.name,
        payload=AgentContext(messages=[{"role": "user", "content": "receiver"}]),
        max_turns=5,
    )
    await enqueue_task(
        redis_client=redis_client,
        namespace=test_namespace,
        agent=sender_agent,
        task=sender_task,
    )
    receiver_task_id = await enqueue_task(
        redis_client=redis_client,
        namespace=test_namespace,
        agent=receiver_agent,
        task=receiver_task,
    )
    receiver_keys = RedisKeys.format(
        namespace=test_namespace,
        agent=receiver_agent.name,
        task_id=receiver_task_id,
    )
    picked = await _pickup_single_task(
        redis_client=redis_client,
        keys=receiver_keys,
        pickup_script=pickup_script,
    )
    assert picked == [receiver_task_id]
    await process_task(
        redis_client=redis_client,
        namespace=test_namespace,
        task_id=receiver_task_id,
        completion_script=completion_script,
        steering_script=steering_script,
        agent=receiver_agent,
        agents_by_name={receiver_agent.name: receiver_agent},
        max_retries=1,
        heartbeat_interval=30,
        task_timeout=60,
        metrics_retention_duration=3600,
    )
    assert (
        await get_task_status(redis_client, test_namespace, receiver_task_id)
        == TaskStatus.PAUSED
    )

    report = await signal_task(
        redis_client=redis_client,
        namespace=test_namespace,
        sender_task_id=sender_task.id,
        task_id=receiver_task_id,
        signal_id="day_vote_open:2",
        payload={"round": 2, "deadline_s": 45},
    )
    assert report["woken_task_ids"] == [receiver_task_id]
    assert (
        await get_task_status(redis_client, test_namespace, receiver_task_id)
        == TaskStatus.ACTIVE
    )
    assert await redis_client.hget(receiver_keys.signal_wait_meta, receiver_task_id) is None
    assert await redis_client.zscore(receiver_keys.queue_pending, receiver_task_id) is None
    queued_ids = await redis_client.lrange(receiver_keys.queue_main, 0, -1)
    assert receiver_task_id in queued_ids

    repicked = await _pickup_single_task(
        redis_client=redis_client,
        keys=receiver_keys,
        pickup_script=pickup_script,
    )
    assert repicked == [receiver_task_id]
    await process_task(
        redis_client=redis_client,
        namespace=test_namespace,
        task_id=receiver_task_id,
        completion_script=completion_script,
        steering_script=steering_script,
        agent=receiver_agent,
        agents_by_name={receiver_agent.name: receiver_agent},
        max_retries=1,
        heartbeat_interval=30,
        task_timeout=60,
        metrics_retention_duration=3600,
    )

    assert (
        await get_task_status(redis_client, test_namespace, receiver_task_id)
        == TaskStatus.COMPLETED
    )
    assert await redis_client.hget(receiver_keys.signal_wake_meta, receiver_task_id) is None
    assert await redis_client.hget(receiver_keys.signal_wait_meta, receiver_task_id) is None


@pytest.mark.asyncio
async def test_wait_until_signal_timeout_exposes_timeout_wake_reason(
    redis_client: redis.Redis,
    test_namespace: str,
    test_owner_id: str,
    pickup_script: BatchPickupScript,
    completion_script: TaskCompletionScript,
    steering_script: TaskSteeringScript,
) -> None:
    agent = _WaitForSignalAgent(
        signal_id="night_action_open:4",
        timeout=wait.sleep(0.0),
        name="signal_timeout_agent",
    )
    keys = RedisKeys.format(namespace=test_namespace, agent=agent.name)
    task = Task.create(
        owner_id=test_owner_id,
        agent=agent.name,
        payload=AgentContext(
            messages=[{"role": "user", "content": "timeout on signal wait"}]
        ),
        max_turns=10,
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
        max_retries=1,
        heartbeat_interval=30,
        task_timeout=60,
        metrics_retention_duration=3600,
    )
    assert await get_task_status(redis_client, test_namespace, task_id) == TaskStatus.PAUSED

    scheduled_recovery_script = await create_scheduled_recovery_script(redis_client)
    recovered_ids = await scheduled_recovery_script.execute(
        queue_scheduled_key=keys.queue_scheduled,
        queue_main_key=keys.queue_main,
        queue_pending_key=keys.queue_pending,
        queue_orphaned_key=keys.queue_orphaned,
        task_statuses_key=keys.task_status,
        task_agents_key=keys.task_agent,
        task_payloads_key=keys.task_payload,
        task_pickups_key=keys.task_pickups,
        task_retries_key=keys.task_retries,
        task_metas_key=keys.task_meta,
        scheduled_wait_meta_key=keys.scheduled_wait_meta,
        activity_wait_meta_key=keys.activity_wait_meta,
        signal_wait_meta_key=keys.signal_wait_meta,
        signal_wake_meta_key=keys.signal_wake_meta,
        max_batch_size=10,
    )
    assert task_id in recovered_ids
    wake_meta_raw = await redis_client.hget(keys.signal_wake_meta, task_id)
    assert wake_meta_raw is not None
    wake_meta = json.loads(cast(str, wake_meta_raw))
    assert wake_meta["wake_reason"] == "timeout"
    assert wake_meta["signal_id"] == "night_action_open:4"

    repicked = await _pickup_single_task(
        redis_client=redis_client,
        keys=keys,
        pickup_script=pickup_script,
    )
    assert repicked == [task_id]
    await process_task(
        redis_client=redis_client,
        namespace=test_namespace,
        task_id=task_id,
        completion_script=completion_script,
        steering_script=steering_script,
        agent=agent,
        agents_by_name={agent.name: agent},
        max_retries=1,
        heartbeat_interval=30,
        task_timeout=60,
        metrics_retention_duration=3600,
    )
    assert await get_task_status(redis_client, test_namespace, task_id) == TaskStatus.COMPLETED
    assert await redis_client.hget(keys.signal_wake_meta, task_id) is None
    assert await redis_client.hget(keys.signal_wait_meta, task_id) is None
