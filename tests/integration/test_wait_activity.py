"""Integration coverage for wait.activity wake semantics."""

from __future__ import annotations

import asyncio
import json
import time
from typing import Any

import pytest
import redis.asyncio as redis
from openai.types.chat.chat_completion_message_function_tool_call import (
    ChatCompletionMessageFunctionToolCall,
    Function as ToolCallFunction,
)

from factorial.agent import BaseAgent, TurnCompletion
from factorial.execution.context import AgentContext
from factorial.execution.waits import wait
from factorial.queue.keys import PENDING_SENTINEL, RedisKeys
from factorial.queue.lua import (
    ActivityWaitScript,
    BatchPickupScript,
    TaskCompletionScript,
    TaskSteeringScript,
    create_activity_wait_script,
    create_scheduled_recovery_script,
)
from factorial.queue.operations import (
    cancel_task,
    enqueue_task,
    messaging_groups_create,
    messaging_groups_send,
    messaging_send_direct,
    steer_task,
)
from factorial.queue.task import Task, TaskStatus, get_task_status
from factorial.queue.worker import CompletionAction, process_task

from .conftest import SimpleTestAgent


def _make_tool_call(
    tool_name: str,
    call_id: str,
) -> ChatCompletionMessageFunctionToolCall:
    return ChatCompletionMessageFunctionToolCall(
        id=call_id,
        type="function",
        function=ToolCallFunction(name=tool_name, arguments="{}"),
    )


class _WaitActivityAgent(BaseAgent[AgentContext]):
    def __init__(self, *, name: str = "wait_activity_agent"):
        super().__init__(
            name=name,
            instructions="Activity wait agent",
            context_class=AgentContext,
        )

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
                    _make_tool_call("wait_activity", "call_wait_activity"),
                    wait.activity(data={"reason": "awaiting activity"}),
                )
            ],
        )


class _WaitActivityWithTimeoutAgent(BaseAgent[AgentContext]):
    def __init__(
        self,
        *,
        timeout: Any,
        name: str = "wait_activity_timeout_agent",
    ):
        super().__init__(
            name=name,
            instructions="Activity wait with timeout agent",
            context_class=AgentContext,
        )
        self._timeout = timeout

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
                    _make_tool_call(
                        "wait_activity_timeout",
                        "call_wait_activity_timeout",
                    ),
                    wait.activity(
                        timeout=self._timeout,
                        data={"reason": "awaiting activity or timeout"},
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


async def _set_task_status(
    *,
    redis_client: redis.Redis,
    keys: RedisKeys,
    task: Task[AgentContext],
    status: TaskStatus,
) -> None:
    await redis_client.hset(keys.task_status, task.id, status.value)
    await redis_client.hset(keys.task_payload, task.id, task.payload.to_json())


async def _park_activity_wait(
    *,
    redis_client: redis.Redis,
    namespace: str,
    script: ActivityWaitScript,
    task: Task[AgentContext],
    ):
    keys = RedisKeys.format(namespace=namespace, agent=task.agent, task_id=task.id)
    root_keys = RedisKeys.format(namespace=namespace)
    steering_template = RedisKeys.format(
        namespace=namespace,
        task_id="{task_id}",
    ).task_steering
    queue_templates = RedisKeys.format(namespace=namespace, agent="{agent}")
    result = await script.execute(
        queue_pending_key=keys.queue_pending,
        queue_orphaned_key=keys.queue_orphaned,
        processing_heartbeats_key=keys.processing_heartbeats,
        task_statuses_key=keys.task_status,
        task_agents_key=keys.task_agent,
        task_payloads_key=keys.task_payload,
        task_pickups_key=keys.task_pickups,
        task_retries_key=keys.task_retries,
        task_metas_key=keys.task_meta,
        activity_wait_meta_key=root_keys.activity_wait_meta,
        message_seq_key=root_keys.messaging_message_seq,
        task_id=task.id,
        updated_task_payload_json=task.payload.to_json(),
        wait_metadata_json=json.dumps({"kind": "activity"}),
        task_steering_key_template=steering_template,
        task_children_key_template=root_keys.task_children("{parent_task_id}"),
        queue_main_key_template=queue_templates.queue_main,
        queue_pending_key_template=queue_templates.queue_pending,
    )
    assert result.success
    return result


@pytest.mark.asyncio
async def test_process_task_parks_wait_activity(
    redis_client: redis.Redis,
    test_namespace: str,
    test_owner_id: str,
    pickup_script: BatchPickupScript,
    completion_script: TaskCompletionScript,
    steering_script: TaskSteeringScript,
) -> None:
    agent = _WaitActivityAgent()
    keys = RedisKeys.format(namespace=test_namespace, agent=agent.name)
    task = Task.create(
        owner_id=test_owner_id,
        agent=agent.name,
        payload=AgentContext(query="wait on activity"),
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

    status = await get_task_status(redis_client, test_namespace, task_id)
    assert status == TaskStatus.PAUSED
    wait_meta_raw = await redis_client.hget(keys.activity_wait_meta, task_id)
    assert wait_meta_raw is not None
    wait_meta = json.loads(wait_meta_raw)
    assert wait_meta["kind"] == "activity"
    pending_score = await redis_client.zscore(keys.queue_pending, task_id)
    assert pending_score is not None


@pytest.mark.asyncio
async def test_process_task_parks_wait_activity_with_sleep_timeout(
    redis_client: redis.Redis,
    test_namespace: str,
    test_owner_id: str,
    pickup_script: BatchPickupScript,
    completion_script: TaskCompletionScript,
    steering_script: TaskSteeringScript,
) -> None:
    agent = _WaitActivityWithTimeoutAgent(timeout=wait.sleep(7.0))
    keys = RedisKeys.format(namespace=test_namespace, agent=agent.name)
    task = Task.create(
        owner_id=test_owner_id,
        agent=agent.name,
        payload=AgentContext(query="wait on activity or timeout"),
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

    status = await get_task_status(redis_client, test_namespace, task_id)
    assert status == TaskStatus.PAUSED
    activity_meta_raw = await redis_client.hget(keys.activity_wait_meta, task_id)
    assert activity_meta_raw is not None
    scheduled_meta_raw = await redis_client.hget(keys.scheduled_wait_meta, task_id)
    assert scheduled_meta_raw is not None
    scheduled_meta = json.loads(scheduled_meta_raw)
    assert scheduled_meta["kind"] == "activity_timeout"
    assert scheduled_meta["timeout_kind"] == "sleep"
    wake_score = await redis_client.zscore(keys.queue_scheduled, task_id)
    assert wake_score is not None
    assert wake_score > time.time()


@pytest.mark.asyncio
async def test_process_task_parks_wait_activity_with_cron_timeout(
    redis_client: redis.Redis,
    test_namespace: str,
    test_owner_id: str,
    pickup_script: BatchPickupScript,
    completion_script: TaskCompletionScript,
    steering_script: TaskSteeringScript,
) -> None:
    agent = _WaitActivityWithTimeoutAgent(
        timeout=wait.cron("*/5 * * * *", timezone="UTC"),
        name="wait_activity_cron_timeout_agent",
    )
    keys = RedisKeys.format(namespace=test_namespace, agent=agent.name)
    task = Task.create(
        owner_id=test_owner_id,
        agent=agent.name,
        payload=AgentContext(query="wait on activity or cron"),
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

    status = await get_task_status(redis_client, test_namespace, task_id)
    assert status == TaskStatus.PAUSED
    scheduled_meta_raw = await redis_client.hget(keys.scheduled_wait_meta, task_id)
    assert scheduled_meta_raw is not None
    scheduled_meta = json.loads(scheduled_meta_raw)
    assert scheduled_meta["kind"] == "activity_timeout"
    assert scheduled_meta["timeout_kind"] == "cron"
    assert scheduled_meta["cron"] == "*/5 * * * *"
    assert scheduled_meta["timezone"] == "UTC"


@pytest.mark.asyncio
async def test_activity_wait_wake_clears_scheduled_timeout_state(
    redis_client: redis.Redis,
    test_namespace: str,
    test_owner_id: str,
    pickup_script: BatchPickupScript,
    completion_script: TaskCompletionScript,
    steering_script: TaskSteeringScript,
) -> None:
    agent = _WaitActivityWithTimeoutAgent(timeout=wait.sleep(120.0))
    keys = RedisKeys.format(namespace=test_namespace, agent=agent.name)
    task = Task.create(
        owner_id=test_owner_id,
        agent=agent.name,
        payload=AgentContext(query="wake me before timeout"),
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

    await steer_task(
        redis_client=redis_client,
        namespace=test_namespace,
        task_id=task_id,
        messages=[{"role": "user", "content": "wake now"}],
    )
    status = await get_task_status(redis_client, test_namespace, task_id)
    assert status == TaskStatus.ACTIVE
    assert await redis_client.hget(keys.activity_wait_meta, task_id) is None
    assert await redis_client.hget(keys.scheduled_wait_meta, task_id) is None
    assert await redis_client.zscore(keys.queue_pending, task_id) is None
    assert await redis_client.zscore(keys.queue_scheduled, task_id) is None


@pytest.mark.asyncio
async def test_activity_wait_sleep_timeout_recovers_via_scheduled_queue(
    redis_client: redis.Redis,
    test_namespace: str,
    test_owner_id: str,
    pickup_script: BatchPickupScript,
    completion_script: TaskCompletionScript,
    steering_script: TaskSteeringScript,
) -> None:
    agent = _WaitActivityWithTimeoutAgent(timeout=wait.sleep(0.0))
    keys = RedisKeys.format(namespace=test_namespace, agent=agent.name)
    task = Task.create(
        owner_id=test_owner_id,
        agent=agent.name,
        payload=AgentContext(query="timeout immediately"),
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
        max_batch_size=10,
    )
    assert task_id in recovered_ids
    assert (
        await get_task_status(redis_client, test_namespace, task_id)
        == TaskStatus.ACTIVE
    )
    assert await redis_client.hget(keys.activity_wait_meta, task_id) is None
    assert await redis_client.hget(keys.scheduled_wait_meta, task_id) is None
    assert await redis_client.zscore(keys.queue_pending, task_id) is None
    queued_ids = await redis_client.lrange(keys.queue_main, 0, -1)
    assert task_id in queued_ids


@pytest.mark.asyncio
async def test_steer_task_wakes_activity_wait_task(
    redis_client: redis.Redis,
    test_namespace: str,
    test_owner_id: str,
) -> None:
    agent = SimpleTestAgent(name="steer_wait_activity_agent")
    task = Task.create(
        owner_id=test_owner_id,
        agent=agent.name,
        payload=AgentContext(query="wake me"),
        max_turns=10,
    )
    task_id = await enqueue_task(
        redis_client=redis_client,
        namespace=test_namespace,
        agent=agent,
        task=task,
    )
    task_keys = RedisKeys.format(
        namespace=test_namespace,
        agent=agent.name,
        task_id=task_id,
    )
    await _set_task_status(
        redis_client=redis_client,
        keys=task_keys,
        task=task,
        status=TaskStatus.ACTIVE,
    )

    activity_wait_script = await create_activity_wait_script(redis_client)
    await _park_activity_wait(
        redis_client=redis_client,
        namespace=test_namespace,
        script=activity_wait_script,
        task=task,
    )

    await steer_task(
        redis_client=redis_client,
        namespace=test_namespace,
        task_id=task_id,
        messages=[{"role": "user", "content": "new steering"}],
    )

    status = await get_task_status(redis_client, test_namespace, task_id)
    assert status == TaskStatus.ACTIVE
    assert await redis_client.hget(task_keys.activity_wait_meta, task_id) is None
    queued_ids = await redis_client.lrange(task_keys.queue_main, 0, -1)
    assert task_id in queued_ids


@pytest.mark.asyncio
async def test_direct_message_wakes_activity_waiting_recipient_across_agents(
    redis_client: redis.Redis,
    test_namespace: str,
    test_owner_id: str,
) -> None:
    team_id = "team-cross-agent"
    sender_agent = SimpleTestAgent(name="sender_wait_activity_agent")
    recipient_agent = SimpleTestAgent(name="recipient_wait_activity_agent")
    sender_task = Task.create(
        owner_id=test_owner_id,
        agent=sender_agent.name,
        payload=AgentContext(query="sender"),
        team_id=team_id,
    )
    recipient_task = Task.create(
        owner_id=test_owner_id,
        agent=recipient_agent.name,
        payload=AgentContext(query="recipient"),
        team_id=team_id,
    )
    await enqueue_task(
        redis_client=redis_client,
        namespace=test_namespace,
        agent=sender_agent,
        task=sender_task,
    )
    await enqueue_task(
        redis_client=redis_client,
        namespace=test_namespace,
        agent=recipient_agent,
        task=recipient_task,
    )

    recipient_keys = RedisKeys.format(
        namespace=test_namespace,
        agent=recipient_agent.name,
        task_id=recipient_task.id,
    )
    await redis_client.delete(recipient_keys.queue_main)
    await _set_task_status(
        redis_client=redis_client,
        keys=recipient_keys,
        task=recipient_task,
        status=TaskStatus.ACTIVE,
    )

    activity_wait_script = await create_activity_wait_script(redis_client)
    await _park_activity_wait(
        redis_client=redis_client,
        namespace=test_namespace,
        script=activity_wait_script,
        task=recipient_task,
    )

    report = await messaging_send_direct(
        redis_client=redis_client,
        namespace=test_namespace,
        sender_task_id=sender_task.id,
        to_task_id=recipient_task.id,
        content="wake up",
    )

    assert recipient_task.id in report["delivered_task_ids"]
    status = await get_task_status(redis_client, test_namespace, recipient_task.id)
    assert status == TaskStatus.ACTIVE
    queued_ids = await redis_client.lrange(recipient_keys.queue_main, 0, -1)
    assert recipient_task.id in queued_ids


@pytest.mark.asyncio
async def test_subtree_idle_and_child_terminal_wake_parent_activity_wait(
    redis_client: redis.Redis,
    test_namespace: str,
    test_owner_id: str,
    completion_script: TaskCompletionScript,
) -> None:
    team_id = "team-subtree"
    parent_agent = SimpleTestAgent(name="parent_wait_activity_agent")
    child_agent = SimpleTestAgent(name="child_wait_activity_agent")
    parent_task = Task.create(
        owner_id=test_owner_id,
        agent=parent_agent.name,
        payload=AgentContext(query="parent"),
        team_id=team_id,
    )
    child_one = Task.create(
        owner_id=test_owner_id,
        agent=child_agent.name,
        payload=AgentContext(query="child-1"),
        team_id=team_id,
    )
    child_two = Task.create(
        owner_id=test_owner_id,
        agent=child_agent.name,
        payload=AgentContext(query="child-2"),
        team_id=team_id,
    )
    child_one.metadata.parent_id = parent_task.id
    child_two.metadata.parent_id = parent_task.id

    await enqueue_task(
        redis_client=redis_client,
        namespace=test_namespace,
        agent=parent_agent,
        task=parent_task,
    )
    await enqueue_task(
        redis_client=redis_client,
        namespace=test_namespace,
        agent=child_agent,
        task=child_one,
    )
    await enqueue_task(
        redis_client=redis_client,
        namespace=test_namespace,
        agent=child_agent,
        task=child_two,
    )

    parent_keys = RedisKeys.format(
        namespace=test_namespace,
        agent=parent_agent.name,
        task_id=parent_task.id,
    )
    child_one_keys = RedisKeys.format(
        namespace=test_namespace,
        agent=child_agent.name,
        task_id=child_one.id,
    )
    child_two_keys = RedisKeys.format(
        namespace=test_namespace,
        agent=child_agent.name,
        task_id=child_two.id,
    )
    root_keys = RedisKeys.format(namespace=test_namespace)
    queue_templates = RedisKeys.format(namespace=test_namespace, agent="{agent}")
    steering_template = RedisKeys.format(
        namespace=test_namespace,
        task_id="{task_id}",
    ).task_steering

    await redis_client.delete(parent_keys.queue_main)
    await redis_client.delete(child_one_keys.queue_main)
    await _set_task_status(
        redis_client=redis_client,
        keys=parent_keys,
        task=parent_task,
        status=TaskStatus.ACTIVE,
    )
    await _set_task_status(
        redis_client=redis_client,
        keys=child_one_keys,
        task=child_one,
        status=TaskStatus.ACTIVE,
    )
    await _set_task_status(
        redis_client=redis_client,
        keys=child_two_keys,
        task=child_two,
        status=TaskStatus.ACTIVE,
    )

    activity_wait_script = await create_activity_wait_script(redis_client)
    await _park_activity_wait(
        redis_client=redis_client,
        namespace=test_namespace,
        script=activity_wait_script,
        task=parent_task,
    )
    await _park_activity_wait(
        redis_client=redis_client,
        namespace=test_namespace,
        script=activity_wait_script,
        task=child_one,
    )
    parent_status_after_first_child = await get_task_status(
        redis_client,
        test_namespace,
        parent_task.id,
    )
    assert parent_status_after_first_child == TaskStatus.PAUSED

    await _park_activity_wait(
        redis_client=redis_client,
        namespace=test_namespace,
        script=activity_wait_script,
        task=child_two,
    )
    parent_status_after_subtree_idle = await get_task_status(
        redis_client,
        test_namespace,
        parent_task.id,
    )
    assert parent_status_after_subtree_idle == TaskStatus.ACTIVE
    queued_parent_ids = await redis_client.lrange(parent_keys.queue_main, 0, -1)
    assert parent_task.id in queued_parent_ids
    parent_steering_messages = await redis_client.hvals(parent_keys.task_steering)
    assert any("subtree_idle" in str(msg) for msg in parent_steering_messages)

    await _set_task_status(
        redis_client=redis_client,
        keys=parent_keys,
        task=parent_task,
        status=TaskStatus.ACTIVE,
    )
    await redis_client.delete(parent_keys.queue_main)
    await _set_task_status(
        redis_client=redis_client,
        keys=child_one_keys,
        task=child_one,
        status=TaskStatus.PROCESSING,
    )
    await redis_client.zadd(
        child_one_keys.processing_heartbeats,
        {child_one.id: time.time()},
    )
    await _park_activity_wait(
        redis_client=redis_client,
        namespace=test_namespace,
        script=activity_wait_script,
        task=parent_task,
    )

    completion_result = await completion_script.execute(
        queue_main_key=child_one_keys.queue_main,
        queue_completions_key=child_one_keys.queue_completions,
        queue_failed_key=child_one_keys.queue_failed,
        queue_backoff_key=child_one_keys.queue_backoff,
        queue_orphaned_key=child_one_keys.queue_orphaned,
        queue_pending_key=child_one_keys.queue_pending,
        task_statuses_key=child_one_keys.task_status,
        task_agents_key=child_one_keys.task_agent,
        task_payloads_key=child_one_keys.task_payload,
        task_pickups_key=child_one_keys.task_pickups,
        task_retries_key=child_one_keys.task_retries,
        task_metas_key=child_one_keys.task_meta,
        processing_heartbeats_key=child_one_keys.processing_heartbeats,
        pending_tool_results_key=child_one_keys.pending_tool_results,
        pending_child_task_results_key=child_one_keys.pending_child_task_results,
        agent_metrics_bucket_key=child_one_keys.agent_metrics_bucket,
        global_metrics_bucket_key=child_one_keys.global_metrics_bucket,
        batch_meta_key=child_one_keys.batch_meta,
        batch_progress_key=child_one_keys.batch_progress,
        batch_remaining_tasks_key=child_one_keys.batch_remaining_tasks,
        batch_completed_key=child_one_keys.batch_completed,
        task_id=child_one.id,
        action=CompletionAction.COMPLETE.value,
        updated_task_payload_json=child_one.payload.to_json(),
        metrics_ttl=3600,
        pending_sentinel=PENDING_SENTINEL,
        current_turn=child_one.payload.turn,
        final_output_json=json.dumps({"ok": True}),
        activity_wait_meta_key=root_keys.activity_wait_meta,
        task_steering_key_template=steering_template,
        message_seq_key=root_keys.messaging_message_seq,
        queue_main_key_template=queue_templates.queue_main,
        queue_pending_key_template=queue_templates.queue_pending,
    )
    assert completion_result.success

    parent_status_after_child_terminal = await get_task_status(
        redis_client,
        test_namespace,
        parent_task.id,
    )
    assert parent_status_after_child_terminal == TaskStatus.ACTIVE
    queued_parent_ids = await redis_client.lrange(parent_keys.queue_main, 0, -1)
    assert parent_task.id in queued_parent_ids
    parent_steering_messages = await redis_client.hvals(parent_keys.task_steering)
    assert any("child_terminal" in str(msg) for msg in parent_steering_messages)


@pytest.mark.asyncio
async def test_root_activity_wait_self_wakes_when_children_already_waiting(
    redis_client: redis.Redis,
    test_namespace: str,
    test_owner_id: str,
) -> None:
    team_id = "team-root-self-wake"
    root_agent = SimpleTestAgent(name="root_wait_activity_agent")
    child_agent = SimpleTestAgent(name="child_wait_activity_for_root_agent")

    root_task = Task.create(
        owner_id=test_owner_id,
        agent=root_agent.name,
        payload=AgentContext(query="root"),
        team_id=team_id,
    )
    child_task = Task.create(
        owner_id=test_owner_id,
        agent=child_agent.name,
        payload=AgentContext(query="child"),
        team_id=team_id,
    )
    child_task.metadata.parent_id = root_task.id

    await enqueue_task(
        redis_client=redis_client,
        namespace=test_namespace,
        agent=root_agent,
        task=root_task,
    )
    await enqueue_task(
        redis_client=redis_client,
        namespace=test_namespace,
        agent=child_agent,
        task=child_task,
    )

    root_keys = RedisKeys.format(
        namespace=test_namespace,
        agent=root_agent.name,
        task_id=root_task.id,
    )
    child_keys = RedisKeys.format(
        namespace=test_namespace,
        agent=child_agent.name,
        task_id=child_task.id,
    )
    await redis_client.delete(root_keys.queue_main)
    await redis_client.delete(child_keys.queue_main)

    await _set_task_status(
        redis_client=redis_client,
        keys=root_keys,
        task=root_task,
        status=TaskStatus.ACTIVE,
    )
    await _set_task_status(
        redis_client=redis_client,
        keys=child_keys,
        task=child_task,
        status=TaskStatus.ACTIVE,
    )

    activity_wait_script = await create_activity_wait_script(redis_client)
    await _park_activity_wait(
        redis_client=redis_client,
        namespace=test_namespace,
        script=activity_wait_script,
        task=child_task,
    )
    await _park_activity_wait(
        redis_client=redis_client,
        namespace=test_namespace,
        script=activity_wait_script,
        task=root_task,
    )

    root_status = await get_task_status(redis_client, test_namespace, root_task.id)
    assert root_status == TaskStatus.ACTIVE
    queued_root_ids = await redis_client.lrange(root_keys.queue_main, 0, -1)
    assert root_task.id in queued_root_ids
    pending_score = await redis_client.zscore(root_keys.queue_pending, root_task.id)
    assert pending_score is None
    root_steering_messages = await redis_client.hvals(root_keys.task_steering)
    assert any("subtree_idle" in str(msg) for msg in root_steering_messages)


@pytest.mark.asyncio
async def test_root_activity_wait_self_wakes_when_children_already_terminal(
    redis_client: redis.Redis,
    test_namespace: str,
    test_owner_id: str,
) -> None:
    team_id = "team-root-self-wake-terminal"
    root_agent = SimpleTestAgent(name="root_wait_activity_terminal_agent")
    child_agent = SimpleTestAgent(name="child_wait_activity_terminal_agent")

    root_task = Task.create(
        owner_id=test_owner_id,
        agent=root_agent.name,
        payload=AgentContext(query="root"),
        team_id=team_id,
    )
    child_one = Task.create(
        owner_id=test_owner_id,
        agent=child_agent.name,
        payload=AgentContext(query="child-1"),
        team_id=team_id,
    )
    child_two = Task.create(
        owner_id=test_owner_id,
        agent=child_agent.name,
        payload=AgentContext(query="child-2"),
        team_id=team_id,
    )
    child_one.metadata.parent_id = root_task.id
    child_two.metadata.parent_id = root_task.id

    await enqueue_task(
        redis_client=redis_client,
        namespace=test_namespace,
        agent=root_agent,
        task=root_task,
    )
    await enqueue_task(
        redis_client=redis_client,
        namespace=test_namespace,
        agent=child_agent,
        task=child_one,
    )
    await enqueue_task(
        redis_client=redis_client,
        namespace=test_namespace,
        agent=child_agent,
        task=child_two,
    )

    root_keys = RedisKeys.format(
        namespace=test_namespace,
        agent=root_agent.name,
        task_id=root_task.id,
    )
    child_one_keys = RedisKeys.format(
        namespace=test_namespace,
        agent=child_agent.name,
        task_id=child_one.id,
    )
    child_two_keys = RedisKeys.format(
        namespace=test_namespace,
        agent=child_agent.name,
        task_id=child_two.id,
    )

    await redis_client.delete(root_keys.queue_main)
    await redis_client.delete(child_one_keys.queue_main)

    await _set_task_status(
        redis_client=redis_client,
        keys=root_keys,
        task=root_task,
        status=TaskStatus.ACTIVE,
    )
    await _set_task_status(
        redis_client=redis_client,
        keys=child_one_keys,
        task=child_one,
        status=TaskStatus.COMPLETED,
    )
    await _set_task_status(
        redis_client=redis_client,
        keys=child_two_keys,
        task=child_two,
        status=TaskStatus.FAILED,
    )

    activity_wait_script = await create_activity_wait_script(redis_client)
    await _park_activity_wait(
        redis_client=redis_client,
        namespace=test_namespace,
        script=activity_wait_script,
        task=root_task,
    )

    root_status = await get_task_status(redis_client, test_namespace, root_task.id)
    assert root_status == TaskStatus.ACTIVE
    queued_root_ids = await redis_client.lrange(root_keys.queue_main, 0, -1)
    assert root_task.id in queued_root_ids
    pending_score = await redis_client.zscore(root_keys.queue_pending, root_task.id)
    assert pending_score is None
    root_steering_messages = await redis_client.hvals(root_keys.task_steering)
    assert any("subtree_idle" in str(msg) for msg in root_steering_messages)


@pytest.mark.asyncio
async def test_group_message_wakes_activity_waiting_member(
    redis_client: redis.Redis,
    test_namespace: str,
    test_owner_id: str,
) -> None:
    team_id = "team-group-wake"
    sender_agent = SimpleTestAgent(name="group_sender_wait_activity_agent")
    recipient_agent = SimpleTestAgent(name="group_recipient_wait_activity_agent")

    sender_task = Task.create(
        owner_id=test_owner_id,
        agent=sender_agent.name,
        payload=AgentContext(query="sender"),
        team_id=team_id,
    )
    recipient_task = Task.create(
        owner_id=test_owner_id,
        agent=recipient_agent.name,
        payload=AgentContext(query="recipient"),
        team_id=team_id,
    )

    await enqueue_task(
        redis_client=redis_client,
        namespace=test_namespace,
        agent=sender_agent,
        task=sender_task,
    )
    await enqueue_task(
        redis_client=redis_client,
        namespace=test_namespace,
        agent=recipient_agent,
        task=recipient_task,
    )

    recipient_keys = RedisKeys.format(
        namespace=test_namespace,
        agent=recipient_agent.name,
        task_id=recipient_task.id,
    )
    await redis_client.delete(recipient_keys.queue_main)
    await _set_task_status(
        redis_client=redis_client,
        keys=recipient_keys,
        task=recipient_task,
        status=TaskStatus.ACTIVE,
    )

    activity_wait_script = await create_activity_wait_script(redis_client)
    await _park_activity_wait(
        redis_client=redis_client,
        namespace=test_namespace,
        script=activity_wait_script,
        task=recipient_task,
    )

    await messaging_groups_create(
        redis_client=redis_client,
        namespace=test_namespace,
        sender_task_id=sender_task.id,
        group_name="research",
        member_task_ids=[recipient_task.id],
    )
    report = await messaging_groups_send(
        redis_client=redis_client,
        namespace=test_namespace,
        sender_task_id=sender_task.id,
        group_name="research",
        content="wake from group",
    )

    assert recipient_task.id in report["delivered_task_ids"]
    status = await get_task_status(redis_client, test_namespace, recipient_task.id)
    assert status == TaskStatus.ACTIVE
    queued_ids = await redis_client.lrange(recipient_keys.queue_main, 0, -1)
    assert recipient_task.id in queued_ids


@pytest.mark.asyncio
async def test_cancellation_clears_activity_wait_state(
    redis_client: redis.Redis,
    test_namespace: str,
    test_owner_id: str,
) -> None:
    agent = SimpleTestAgent(name="cancel_wait_activity_agent")
    task = Task.create(
        owner_id=test_owner_id,
        agent=agent.name,
        payload=AgentContext(query="cancel while paused"),
        max_turns=10,
    )
    await enqueue_task(
        redis_client=redis_client,
        namespace=test_namespace,
        agent=agent,
        task=task,
    )

    task_keys = RedisKeys.format(
        namespace=test_namespace,
        agent=agent.name,
        task_id=task.id,
    )
    await redis_client.delete(task_keys.queue_main)
    await _set_task_status(
        redis_client=redis_client,
        keys=task_keys,
        task=task,
        status=TaskStatus.ACTIVE,
    )

    activity_wait_script = await create_activity_wait_script(redis_client)
    await _park_activity_wait(
        redis_client=redis_client,
        namespace=test_namespace,
        script=activity_wait_script,
        task=task,
    )

    await cancel_task(
        redis_client=redis_client,
        namespace=test_namespace,
        task_id=task.id,
        agents_by_name={agent.name: agent},
        metrics_retention_duration=3600,
    )
    status = await get_task_status(redis_client, test_namespace, task.id)
    assert status == TaskStatus.CANCELLED
    assert await redis_client.hget(task_keys.activity_wait_meta, task.id) is None
    assert await redis_client.zscore(task_keys.queue_pending, task.id) is None


@pytest.mark.asyncio
async def test_activity_wait_concurrent_wake_signals_do_not_duplicate_queue_entries(
    redis_client: redis.Redis,
    test_namespace: str,
    test_owner_id: str,
) -> None:
    team_id = "team-fanin"
    sender_agent = SimpleTestAgent(name="fanin_sender_agent")
    recipient_agent = SimpleTestAgent(name="fanin_recipient_agent")

    sender_task = Task.create(
        owner_id=test_owner_id,
        agent=sender_agent.name,
        payload=AgentContext(query="sender"),
        team_id=team_id,
    )
    recipient_task = Task.create(
        owner_id=test_owner_id,
        agent=recipient_agent.name,
        payload=AgentContext(query="recipient"),
        team_id=team_id,
    )
    await enqueue_task(
        redis_client=redis_client,
        namespace=test_namespace,
        agent=sender_agent,
        task=sender_task,
    )
    await enqueue_task(
        redis_client=redis_client,
        namespace=test_namespace,
        agent=recipient_agent,
        task=recipient_task,
    )

    recipient_keys = RedisKeys.format(
        namespace=test_namespace,
        agent=recipient_agent.name,
        task_id=recipient_task.id,
    )
    await redis_client.delete(recipient_keys.queue_main)
    await _set_task_status(
        redis_client=redis_client,
        keys=recipient_keys,
        task=recipient_task,
        status=TaskStatus.ACTIVE,
    )

    activity_wait_script = await create_activity_wait_script(redis_client)
    await _park_activity_wait(
        redis_client=redis_client,
        namespace=test_namespace,
        script=activity_wait_script,
        task=recipient_task,
    )

    await asyncio.gather(
        messaging_send_direct(
            redis_client=redis_client,
            namespace=test_namespace,
            sender_task_id=sender_task.id,
            to_task_id=recipient_task.id,
            content="dm wake",
        ),
        steer_task(
            redis_client=redis_client,
            namespace=test_namespace,
            task_id=recipient_task.id,
            messages=[{"role": "user", "content": "manual steer wake"}],
        ),
    )

    status = await get_task_status(redis_client, test_namespace, recipient_task.id)
    assert status == TaskStatus.ACTIVE
    queued_ids = await redis_client.lrange(recipient_keys.queue_main, 0, -1)
    assert queued_ids.count(recipient_task.id) == 1
