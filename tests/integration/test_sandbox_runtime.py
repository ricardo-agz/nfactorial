"""Integration coverage for sandbox resource lifecycle wiring."""

from __future__ import annotations

import json
from dataclasses import dataclass, field

import pytest
import redis.asyncio as redis
from openai.types.chat.chat_completion_message_function_tool_call import (
    ChatCompletionMessageFunctionToolCall,
    Function as ToolCallFunction,
)

from factorial import BaseAgent, TurnCompletion, sandboxes
from factorial.agent.context import AgentContext
from factorial.ai.models import Model, Provider
from factorial.execution.waits import wait
from factorial.queue.keys import RedisKeys
from factorial.queue.lua import BatchPickupScript, TaskCompletionScript, TaskSteeringScript
from factorial.queue.operations import enqueue_task
from factorial.queue.task import Task, TaskStatus, get_task_status
from factorial.queue.worker import process_task

MOCK_MODEL = Model(
    name="mock-model",
    provider=Provider.OPENAI,
    provider_model_id="mock-v1",
    context_window=128000,
)


@dataclass
class _FakeSnapshot:
    snapshot_id: str
    source_sandbox_id: str
    expires_at: int = 9999999999


class _FakeAsyncSandbox:
    instances: dict[str, "_FakeAsyncSandbox"] = {}
    created_kwargs: list[dict] = []
    counter: int = 0

    def __init__(self, sandbox_id: str) -> None:
        self.sandbox_id = sandbox_id
        self.status = "running"
        self.files: dict[str, bytes] = {}
        self.stop_calls = 0
        _FakeAsyncSandbox.instances[sandbox_id] = self

    @classmethod
    def reset(cls) -> None:
        cls.instances = {}
        cls.created_kwargs = []
        cls.counter = 0

    @classmethod
    async def create(cls, **kwargs):
        cls.created_kwargs.append(dict(kwargs))
        cls.counter += 1
        return cls(f"sb-{cls.counter}")

    @classmethod
    async def get(cls, *, sandbox_id: str):
        sandbox = cls.instances.get(sandbox_id)
        if sandbox is None:
            raise RuntimeError("missing sandbox")
        return sandbox

    async def wait_for_status(self, status: str, *, timeout: float) -> None:
        del timeout
        self.status = status

    async def run_command(self, *args, **kwargs):
        raise AssertionError("run_command should not be called in this integration test")

    async def run_command_detached(self, *args, **kwargs):
        raise AssertionError(
            "run_command_detached should not be called in this integration test"
        )

    async def read_file(self, path: str):
        return self.files.get(path)

    async def write_files(self, files: list[dict]) -> None:
        for file in files:
            self.files[str(file["path"])] = bytes(file["content"])

    async def mk_dir(self, path: str) -> None:
        del path

    def domain(self, port: int) -> str:
        return f"https://sandbox-{port}.example.test"

    async def snapshot(self) -> _FakeSnapshot:
        self.status = "stopped"
        return _FakeSnapshot(
            snapshot_id=f"snap-{self.sandbox_id}",
            source_sandbox_id=self.sandbox_id,
        )

    async def stop(self) -> None:
        self.stop_calls += 1
        self.status = "stopped"


def _make_tool_call(
    tool_name: str,
    call_id: str,
) -> ChatCompletionMessageFunctionToolCall:
    return ChatCompletionMessageFunctionToolCall(
        id=call_id,
        type="function",
        function=ToolCallFunction(name=tool_name, arguments="{}"),
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


class _SandboxWaitAgent(BaseAgent[AgentContext]):
    def __init__(self) -> None:
        super().__init__(
            name="sandbox_wait_agent",
            instructions="Pause with a sandbox",
            model=MOCK_MODEL,
        )

    async def run_turn(
        self,
        agent_ctx: AgentContext,
    ) -> TurnCompletion[AgentContext]:
        agent_ctx.turn_number += 1
        sandbox = await sandboxes.get()
        await sandbox.write_file("state.txt", "sandbox state")
        return TurnCompletion(
            is_done=False,
            context=agent_ctx,
            tool_call_results=[
                (
                    _make_tool_call("sleep_wait", "call_wait_sleep"),
                    wait.sleep(5.0, data="cooling down"),
                )
            ],
        )


class _SandboxCompleteAgent(BaseAgent[AgentContext]):
    def __init__(self) -> None:
        super().__init__(
            name="sandbox_complete_agent",
            instructions="Complete with a sandbox",
            model=MOCK_MODEL,
        )

    async def run_turn(
        self,
        agent_ctx: AgentContext,
    ) -> TurnCompletion[AgentContext]:
        agent_ctx.turn_number += 1
        sandbox = await sandboxes.get()
        await sandbox.write_file("done.txt", "done")
        return TurnCompletion(
            is_done=True,
            context=agent_ctx,
            output={"sandbox_id": sandbox.id},
        )


@pytest.mark.asyncio
async def test_process_task_checkpoints_sandbox_on_pause(
    monkeypatch,
    redis_client: redis.Redis,
    test_namespace: str,
    test_owner_id: str,
    pickup_script: BatchPickupScript,
    completion_script: TaskCompletionScript,
    steering_script: TaskSteeringScript,
) -> None:
    _FakeAsyncSandbox.reset()
    monkeypatch.setattr(
        "factorial.resources.sandbox.vercel._load_vercel_async_sandbox",
        lambda: _FakeAsyncSandbox,
    )

    agent = _SandboxWaitAgent()
    keys = RedisKeys.format(namespace=test_namespace, agent=agent.name, task_id="unused")
    task = Task.create(
        owner_id=test_owner_id,
        agent=agent.name,
        payload=AgentContext(messages=[{"role": "user", "content": "pause"}]),
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

    picked = await _pickup_single_task(
        redis_client=redis_client,
        keys=RedisKeys.format(namespace=test_namespace, agent=agent.name),
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

    assert await get_task_status(redis_client, test_namespace, task_id) == TaskStatus.PAUSED
    bindings = await redis_client.hgetall(task_keys.resource_bindings)
    assert len(bindings) == 1
    binding = json.loads(next(iter(bindings.values())))
    assert binding["checkpoint"]["ref"] == "snap-sb-1"
    assert binding["checkpoint"]["metadata"]["source_sandbox_id"] == "sb-1"


@pytest.mark.asyncio
async def test_process_task_destroys_sandbox_on_completion(
    monkeypatch,
    redis_client: redis.Redis,
    test_namespace: str,
    test_owner_id: str,
    pickup_script: BatchPickupScript,
    completion_script: TaskCompletionScript,
    steering_script: TaskSteeringScript,
) -> None:
    _FakeAsyncSandbox.reset()
    monkeypatch.setattr(
        "factorial.resources.sandbox.vercel._load_vercel_async_sandbox",
        lambda: _FakeAsyncSandbox,
    )

    agent = _SandboxCompleteAgent()
    task = Task.create(
        owner_id=test_owner_id,
        agent=agent.name,
        payload=AgentContext(messages=[{"role": "user", "content": "finish"}]),
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

    picked = await _pickup_single_task(
        redis_client=redis_client,
        keys=RedisKeys.format(namespace=test_namespace, agent=agent.name),
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
        == TaskStatus.COMPLETED
    )
    assert await redis_client.exists(task_keys.resource_bindings) == 0
    assert _FakeAsyncSandbox.instances["sb-1"].stop_calls == 1
