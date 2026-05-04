"""Integration coverage for task and batch handle APIs."""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from typing import Annotated, Any, cast

import httpx
import pytest
import redis.asyncio as redis
from openai.types.chat.chat_completion_message_function_tool_call import (
    ChatCompletionMessageFunctionToolCall,
    Function as ToolCallFunction,
)

from factorial import (
    Agent,
    AgentContext,
    FinishEvent,
    Hook,
    HookCompletionStatus,
    HookRequestContext,
    Orchestrator,
    PendingHook,
    RunStatus,
    TaskSnapshotStatus,
    TurnFinishEvent,
    TurnStartEvent,
    WaitKind,
    hook,
    subagents,
    tool,
    verify,
    with_context,
)
from factorial._internal.lua.queue import (
    BatchPickupScript,
    TaskCompletionScript,
    TaskSteeringScript,
)
from factorial._internal.queue.keys import RedisKeys
from factorial._internal.queue.worker import process_task
from factorial.agent import BaseAgent, TurnCompletion
from factorial.ai.models import Model, Provider
from factorial.execution.signals import signals
from factorial.execution.waits import wait
from factorial.testing import MockAgent, tool_call
from tests.mocks.llm import MockLLMClient, MockResponse

MOCK_MODEL = Model(
    name="mock-model",
    provider=Provider.OPENAI,
    provider_model_id="mock-model-v1",
    context_window=128000,
)


@dataclass
class SessionState:
    priority: int = 0


@dataclass
class RequestMetadata:
    source: str = "cli"


class ApprovalHook(Hook):
    approved: bool


def _request_transfer_approval(
    ctx: HookRequestContext,
) -> PendingHook[ApprovalHook]:
    return ApprovalHook.pending(
        ctx=ctx,
        hook_id=f"approval-hook-{ctx.task_id}",
        token=f"approval-token-{ctx.task_id}",
        title="Approve transfer",
        metadata={"channel": "ops"},
    )


@tool(name="request_transfer")
def request_transfer_tool(
    approval: Annotated[ApprovalHook, hook.requires(_request_transfer_approval)],
) -> str:
    return f"approved:{approval.approved}"


def _first_user_content(messages: list[dict[str, Any]]) -> str:
    for message in messages:
        if message.get("role") == "user" and isinstance(message.get("content"), str):
            return message["content"]
    return ""


def _make_echo_agent(name: str) -> Agent[SessionState, RequestMetadata]:
    def response_generator(
        messages: list[dict[str, Any]],
        _ctx: AgentContext,
    ) -> MockResponse:
        return MockResponse(
            content=f"echo:{_first_user_content(messages)}",
            is_final=True,
        )

    return Agent[SessionState, RequestMetadata](
        name=name,
        model=MOCK_MODEL,
        client=cast(Any, MockLLMClient(response_generator=response_generator)),
        http_client=httpx.AsyncClient(verify=False, trust_env=False),
    )


class _WaitForSignalAgent(BaseAgent[AgentContext]):
    def __init__(
        self,
        *,
        signal_id: str = "approval",
        name: str = "wait_for_signal_agent",
    ) -> None:
        super().__init__(
            name=name,
            instructions="Wait until manually resumed.",
            model=MOCK_MODEL,
        )
        self._signal_id = signal_id

    async def run_turn(self, agent_ctx: AgentContext) -> TurnCompletion[AgentContext]:
        agent_ctx.turn_number += 1
        if any(
            message.get("role") == "user"
            and message.get("content") == "Approval granted. Continue."
            for message in agent_ctx.messages
        ):
            return TurnCompletion(
                is_done=True,
                context=agent_ctx,
                output={
                    "wake_reason": signals.wake_reason() or "manual_wake",
                    "messages": list(agent_ctx.messages),
                },
            )
        current_signal = signals.current()
        if current_signal is not None and current_signal.signal_id == self._signal_id:
            return TurnCompletion(
                is_done=True,
                context=agent_ctx,
                output={
                    "wake_reason": signals.wake_reason() or "signal",
                    "messages": list(agent_ctx.messages),
                },
            )

        return TurnCompletion(
            is_done=False,
            context=agent_ctx,
            tool_call_results=[
                (
                    _make_tool_call("wait_for_signal", "call_wait_signal"),
                    wait.until_signal(
                        self._signal_id,
                        data={"reason": "awaiting approval"},
                    ),
                )
            ],
        )


class _WaitForActivityAgent(BaseAgent[AgentContext]):
    def __init__(
        self,
        *,
        name: str = "wait_for_activity_agent",
        wake_message: str = "Continue now.",
    ) -> None:
        super().__init__(
            name=name,
            instructions="Wait for a steering message before continuing.",
            model=MOCK_MODEL,
        )
        self._wake_message = wake_message

    async def run_turn(self, agent_ctx: AgentContext) -> TurnCompletion[AgentContext]:
        agent_ctx.turn_number += 1
        if any(
            message.get("role") == "user"
            and message.get("content") == self._wake_message
            for message in agent_ctx.messages
        ):
            return TurnCompletion(
                is_done=True,
                context=agent_ctx,
                output={
                    "wake_message": self._wake_message,
                    "messages": list(agent_ctx.messages),
                },
            )

        return TurnCompletion(
            is_done=False,
            context=agent_ctx,
            tool_call_results=[
                (
                    _make_tool_call("wait_for_activity", "call_wait_activity"),
                    wait.activity(data={"reason": "awaiting user steer"}),
                )
            ],
        )


class _AlwaysFailingAgent(BaseAgent[AgentContext]):
    def __init__(self, *, name: str = "always_failing_agent") -> None:
        super().__init__(
            name=name,
            instructions="Always fail for retry coverage.",
            model=MOCK_MODEL,
        )

    async def run_turn(self, agent_ctx: AgentContext) -> TurnCompletion[AgentContext]:
        del agent_ctx
        raise RuntimeError("synthetic failure")


def _make_tool_call(
    tool_name: str,
    call_id: str,
) -> ChatCompletionMessageFunctionToolCall:
    return ChatCompletionMessageFunctionToolCall(
        id=call_id,
        type="function",
        function=ToolCallFunction(name=tool_name, arguments="{}"),
    )


def _make_orchestrator(
    *,
    redis_client: redis.Redis,
    namespace: str,
    agents: list[BaseAgent[Any]],
) -> Orchestrator:
    orchestrator = Orchestrator(
        redis_pool=redis_client.connection_pool,
        namespace=namespace,
        wake_transport="none",
    )
    orchestrator.agents = agents
    return orchestrator


async def _pickup_task_ids(
    *,
    redis_client: redis.Redis,
    namespace: str,
    agent_name: str,
    pickup_script: BatchPickupScript,
) -> list[str]:
    keys = RedisKeys.format(namespace=namespace, agent=agent_name)
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
        batch_size=10,
        metrics_ttl=3600,
    )
    return list(result.tasks_to_process_ids)


async def _process_one_task(
    *,
    redis_client: redis.Redis,
    namespace: str,
    task_id: str,
    agent: BaseAgent[Any],
    agents_by_name: dict[str, BaseAgent[Any]],
    completion_script: TaskCompletionScript,
    steering_script: TaskSteeringScript,
    max_retries: int = 1,
) -> None:
    await process_task(
        redis_client=redis_client,
        namespace=namespace,
        task_id=task_id,
        completion_script=completion_script,
        steering_script=steering_script,
        agent=agent,
        agents_by_name=agents_by_name,
        max_retries=max_retries,
        heartbeat_interval=30,
        task_timeout=60,
        metrics_retention_duration=3600,
    )


async def _drain_agent_queue(
    *,
    redis_client: redis.Redis,
    namespace: str,
    agent: BaseAgent[Any],
    agents_by_name: dict[str, BaseAgent[Any]],
    pickup_script: BatchPickupScript,
    completion_script: TaskCompletionScript,
    steering_script: TaskSteeringScript,
    max_retries: int = 1,
) -> list[str]:
    processed_task_ids: list[str] = []
    while True:
        task_ids = await _pickup_task_ids(
            redis_client=redis_client,
            namespace=namespace,
            agent_name=agent.name,
            pickup_script=pickup_script,
        )
        if not task_ids:
            return processed_task_ids

        for task_id in task_ids:
            await _process_one_task(
                redis_client=redis_client,
                namespace=namespace,
                task_id=task_id,
                agent=agent,
                agents_by_name=agents_by_name,
                completion_script=completion_script,
                steering_script=steering_script,
                max_retries=max_retries,
            )
            processed_task_ids.append(task_id)


async def _drain_registered_queues(
    *,
    redis_client: redis.Redis,
    namespace: str,
    agents_by_name: dict[str, BaseAgent[Any]],
    pickup_script: BatchPickupScript,
    completion_script: TaskCompletionScript,
    steering_script: TaskSteeringScript,
    max_retries: int = 1,
    max_passes: int = 20,
) -> list[str]:
    processed_task_ids: list[str] = []
    for _ in range(max_passes):
        processed_this_pass = 0
        for agent in agents_by_name.values():
            task_ids = await _drain_agent_queue(
                redis_client=redis_client,
                namespace=namespace,
                agent=agent,
                agents_by_name=agents_by_name,
                pickup_script=pickup_script,
                completion_script=completion_script,
                steering_script=steering_script,
                max_retries=max_retries,
            )
            processed_task_ids.extend(task_ids)
            processed_this_pass += len(task_ids)
        if processed_this_pass == 0:
            return processed_task_ids
    raise AssertionError("draining agent queues exceeded max_passes")


async def _close_agents(*agents: BaseAgent[Any]) -> None:
    for agent in agents:
        await agent.http_client.aclose()


@pytest.mark.asyncio
async def test_task_handle_snapshot_and_wait_return_expected_shapes(
    redis_client: redis.Redis,
    test_namespace: str,
    test_owner_id: str,
    pickup_script: BatchPickupScript,
    completion_script: TaskCompletionScript,
    steering_script: TaskSteeringScript,
) -> None:
    agent = _make_echo_agent("echo_wait_agent")
    orchestrator = _make_orchestrator(
        redis_client=redis_client,
        namespace=test_namespace,
        agents=[agent],
    )
    try:
        task = await orchestrator.enqueue(
            agent,
            "Plan the rollout.",
            owner_id=test_owner_id,
            state={"priority": 5},
            metadata={"source": "api"},
        )

        queued_snapshot = await task.snapshot()
        assert queued_snapshot.status is TaskSnapshotStatus.QUEUED
        assert queued_snapshot.state == SessionState(priority=5)
        assert queued_snapshot.metadata == RequestMetadata(source="api")

        waiter = asyncio.create_task(task.wait(timeout=2))
        await asyncio.sleep(0)

        task_ids = await _pickup_task_ids(
            redis_client=redis_client,
            namespace=test_namespace,
            agent_name=agent.name,
            pickup_script=pickup_script,
        )
        assert task_ids == [task.id]
        await _process_one_task(
            redis_client=redis_client,
            namespace=test_namespace,
            task_id=task.id,
            agent=agent,
            agents_by_name=orchestrator.agents_by_name,
            completion_script=completion_script,
            steering_script=steering_script,
        )

        result = await waiter
        final_snapshot = await task.snapshot()
    finally:
        await _close_agents(agent)

    assert result.status is RunStatus.COMPLETED
    assert result.output == "echo:Plan the rollout."
    assert result.state == SessionState(priority=5)
    assert result.metadata == RequestMetadata(source="api")
    assert final_snapshot.status is TaskSnapshotStatus.COMPLETED
    assert final_snapshot.output == "echo:Plan the rollout."


@pytest.mark.asyncio
async def test_task_handle_updates_yield_typed_events(
    redis_client: redis.Redis,
    test_namespace: str,
    test_owner_id: str,
    pickup_script: BatchPickupScript,
    completion_script: TaskCompletionScript,
    steering_script: TaskSteeringScript,
) -> None:
    agent = _make_echo_agent("echo_updates_agent")
    orchestrator = _make_orchestrator(
        redis_client=redis_client,
        namespace=test_namespace,
        agents=[agent],
    )
    try:
        task = await orchestrator.enqueue(
            agent,
            "Stream the lifecycle.",
            owner_id=test_owner_id,
        )

        observed_events: list[Any] = []

        async def _collect() -> None:
            async for event in task.updates(types=(TurnStartEvent, FinishEvent)):
                observed_events.append(event)
                if isinstance(event, FinishEvent):
                    return

        collector = asyncio.create_task(_collect())
        await asyncio.sleep(0)

        await _drain_agent_queue(
            redis_client=redis_client,
            namespace=test_namespace,
            agent=agent,
            agents_by_name=orchestrator.agents_by_name,
            pickup_script=pickup_script,
            completion_script=completion_script,
            steering_script=steering_script,
        )
        await collector
    finally:
        await _close_agents(agent)

    assert [type(event) for event in observed_events] == [
        TurnStartEvent,
        FinishEvent,
    ]
    assert observed_events[-1].status is RunStatus.COMPLETED
    assert observed_events[-1].output == "echo:Stream the lifecycle."


@pytest.mark.asyncio
async def test_task_wake_resumes_signal_wait_and_appends_manual_messages(
    redis_client: redis.Redis,
    test_namespace: str,
    test_owner_id: str,
    pickup_script: BatchPickupScript,
    completion_script: TaskCompletionScript,
    steering_script: TaskSteeringScript,
) -> None:
    agent = _WaitForSignalAgent()
    orchestrator = _make_orchestrator(
        redis_client=redis_client,
        namespace=test_namespace,
        agents=[agent],
    )
    try:
        task = await orchestrator.enqueue(
            agent,
            "Wait for approval.",
            owner_id=test_owner_id,
        )

        await _drain_agent_queue(
            redis_client=redis_client,
            namespace=test_namespace,
            agent=agent,
            agents_by_name=orchestrator.agents_by_name,
            pickup_script=pickup_script,
            completion_script=completion_script,
            steering_script=steering_script,
        )

        waiting_snapshot = await task.snapshot()
        assert waiting_snapshot.status is TaskSnapshotStatus.WAITING
        assert waiting_snapshot.wait is not None
        assert waiting_snapshot.wait.kind is WaitKind.SIGNAL
        assert waiting_snapshot.wait.signal_id == "approval"

        woke = await task.wake("Approval granted. Continue.")
        assert woke is True

        waiter = asyncio.create_task(task.wait(timeout=2))
        await asyncio.sleep(0)
        await _drain_agent_queue(
            redis_client=redis_client,
            namespace=test_namespace,
            agent=agent,
            agents_by_name=orchestrator.agents_by_name,
            pickup_script=pickup_script,
            completion_script=completion_script,
            steering_script=steering_script,
        )
        result = await waiter
    finally:
        await _close_agents(agent)

    assert result.status is RunStatus.COMPLETED
    transcript = list(result.messages)
    assert any(
        "Runtime note: task was manually resumed and interrupted a signal wait."
        in str(message.get("content", ""))
        for message in transcript
        if message.get("role") == "system"
    )
    assert any(
        message.get("content") == "Approval granted. Continue."
        for message in transcript
        if message.get("role") == "user"
    )


@pytest.mark.asyncio
async def test_task_branch_reuses_context_and_rejects_non_terminal_source(
    redis_client: redis.Redis,
    test_namespace: str,
    test_owner_id: str,
    pickup_script: BatchPickupScript,
    completion_script: TaskCompletionScript,
    steering_script: TaskSteeringScript,
) -> None:
    agent = _make_echo_agent("branch_agent")
    orchestrator = _make_orchestrator(
        redis_client=redis_client,
        namespace=test_namespace,
        agents=[agent],
    )
    try:
        root = await orchestrator.enqueue(
            agent,
            "Draft one.",
            owner_id=test_owner_id,
            state={"priority": 3},
            metadata={"source": "root"},
        )

        with pytest.raises(ValueError, match="terminal task"):
            await root.branch("Revise before completion.")

        await _drain_agent_queue(
            redis_client=redis_client,
            namespace=test_namespace,
            agent=agent,
            agents_by_name=orchestrator.agents_by_name,
            pickup_script=pickup_script,
            completion_script=completion_script,
            steering_script=steering_script,
        )
        await root.wait(timeout=2)

        branched = await root.branch(
            "Revise with stronger evidence.",
            state={"priority": 8},
            metadata={"source": "branch"},
        )
        queued_snapshot = await branched.snapshot()
        assert queued_snapshot.status is TaskSnapshotStatus.QUEUED
        assert queued_snapshot.state == SessionState(priority=8)
        assert queued_snapshot.metadata == RequestMetadata(source="branch")

        await _drain_agent_queue(
            redis_client=redis_client,
            namespace=test_namespace,
            agent=agent,
            agents_by_name=orchestrator.agents_by_name,
            pickup_script=pickup_script,
            completion_script=completion_script,
            steering_script=steering_script,
        )
        branched_result = await branched.wait(timeout=2)
    finally:
        await _close_agents(agent)

    assert branched_result.status is RunStatus.COMPLETED
    assert branched_result.output == "echo:Revise with stronger evidence."
    assert branched_result.state == SessionState(priority=8)
    assert branched_result.metadata == RequestMetadata(source="branch")


@pytest.mark.asyncio
async def test_batch_handle_wait_preserves_per_item_context_with_with_context(
    redis_client: redis.Redis,
    test_namespace: str,
    test_owner_id: str,
    pickup_script: BatchPickupScript,
    completion_script: TaskCompletionScript,
    steering_script: TaskSteeringScript,
) -> None:
    agent = _make_echo_agent("batch_agent")
    orchestrator = _make_orchestrator(
        redis_client=redis_client,
        namespace=test_namespace,
        agents=[agent],
    )
    try:
        batch = await orchestrator.enqueue_many(
            agent,
            [
                with_context(
                    "First item.",
                    state={"priority": 1},
                ),
                with_context(
                    "Second item.",
                    metadata={"source": "batch-override"},
                ),
                "Third item.",
            ],
            owner_id=test_owner_id,
            state={"priority": 0},
            metadata={"source": "default"},
        )

        batch_snapshot = await batch.snapshot()
        assert batch_snapshot.total_tasks == 3
        assert batch_snapshot.remaining_tasks == 3
        assert batch_snapshot.is_finished is False

        await _drain_agent_queue(
            redis_client=redis_client,
            namespace=test_namespace,
            agent=agent,
            agents_by_name=orchestrator.agents_by_name,
            pickup_script=pickup_script,
            completion_script=completion_script,
            steering_script=steering_script,
        )
        results = await batch.wait(timeout=2)
        final_snapshot = await batch.snapshot()
    finally:
        await _close_agents(agent)

    assert [result.output for result in results] == [
        "echo:First item.",
        "echo:Second item.",
        "echo:Third item.",
    ]
    assert [result.state for result in results] == [
        SessionState(priority=1),
        SessionState(priority=0),
        SessionState(priority=0),
    ]
    assert [result.metadata for result in results] == [
        RequestMetadata(source="default"),
        RequestMetadata(source="batch-override"),
        RequestMetadata(source="default"),
    ]
    assert final_snapshot.remaining_tasks == 0
    assert final_snapshot.is_finished is True


@pytest.mark.asyncio
async def test_task_steer_resumes_activity_wait_and_preserves_transcript(
    redis_client: redis.Redis,
    test_namespace: str,
    test_owner_id: str,
    pickup_script: BatchPickupScript,
    completion_script: TaskCompletionScript,
    steering_script: TaskSteeringScript,
) -> None:
    agent = _WaitForActivityAgent()
    orchestrator = _make_orchestrator(
        redis_client=redis_client,
        namespace=test_namespace,
        agents=[agent],
    )
    try:
        task = await orchestrator.enqueue(
            agent,
            "Wait for user steer.",
            owner_id=test_owner_id,
        )
        await _drain_agent_queue(
            redis_client=redis_client,
            namespace=test_namespace,
            agent=agent,
            agents_by_name=orchestrator.agents_by_name,
            pickup_script=pickup_script,
            completion_script=completion_script,
            steering_script=steering_script,
        )

        waiting_snapshot = await task.snapshot()
        assert waiting_snapshot.status is TaskSnapshotStatus.WAITING
        assert waiting_snapshot.wait is not None
        assert waiting_snapshot.wait.kind is WaitKind.ACTIVITY

        await task.steer("Continue now.")
        await _drain_agent_queue(
            redis_client=redis_client,
            namespace=test_namespace,
            agent=agent,
            agents_by_name=orchestrator.agents_by_name,
            pickup_script=pickup_script,
            completion_script=completion_script,
            steering_script=steering_script,
        )
        result = await task.wait(timeout=2)
    finally:
        await _close_agents(agent)

    assert result.status is RunStatus.COMPLETED
    assert any(
        message.get("role") == "user" and message.get("content") == "Continue now."
        for message in result.messages
    )


@pytest.mark.asyncio
async def test_task_and_batch_cancel_surface_cancelled_results(
    redis_client: redis.Redis,
    test_namespace: str,
    test_owner_id: str,
    pickup_script: BatchPickupScript,
    completion_script: TaskCompletionScript,
    steering_script: TaskSteeringScript,
) -> None:
    waiting_agent = _WaitForSignalAgent(name="cancel_waiting_agent")
    batch_agent = _make_echo_agent("cancel_batch_agent")
    orchestrator = _make_orchestrator(
        redis_client=redis_client,
        namespace=test_namespace,
        agents=[waiting_agent, batch_agent],
    )
    try:
        waiting_task = await orchestrator.enqueue(
            waiting_agent,
            "Cancel while waiting.",
            owner_id=test_owner_id,
        )
        await _drain_agent_queue(
            redis_client=redis_client,
            namespace=test_namespace,
            agent=waiting_agent,
            agents_by_name=orchestrator.agents_by_name,
            pickup_script=pickup_script,
            completion_script=completion_script,
            steering_script=steering_script,
        )
        assert (await waiting_task.snapshot()).status is TaskSnapshotStatus.WAITING

        await waiting_task.cancel()
        waiting_result = await waiting_task.wait(timeout=2)
        assert waiting_result.status is RunStatus.CANCELLED
        assert (await waiting_task.snapshot()).status is TaskSnapshotStatus.CANCELLED

        batch = await orchestrator.enqueue_many(
            batch_agent,
            ["Cancel one.", "Cancel two."],
            owner_id=test_owner_id,
        )
        await batch.cancel()
        await _drain_registered_queues(
            redis_client=redis_client,
            namespace=test_namespace,
            agents_by_name=orchestrator.agents_by_name,
            pickup_script=pickup_script,
            completion_script=completion_script,
            steering_script=steering_script,
        )
        batch_results = await batch.wait(timeout=2)
        batch_snapshot = await batch.snapshot()
    finally:
        await _close_agents(waiting_agent, batch_agent)

    assert [result.status for result in batch_results] == [
        RunStatus.CANCELLED,
        RunStatus.CANCELLED,
    ]
    assert batch_snapshot.is_finished is True


@pytest.mark.asyncio
async def test_task_wait_returns_failed_status_after_retries_are_exhausted(
    redis_client: redis.Redis,
    test_namespace: str,
    test_owner_id: str,
    pickup_script: BatchPickupScript,
    completion_script: TaskCompletionScript,
    steering_script: TaskSteeringScript,
) -> None:
    agent = _AlwaysFailingAgent()
    orchestrator = _make_orchestrator(
        redis_client=redis_client,
        namespace=test_namespace,
        agents=[agent],
    )
    try:
        task = await orchestrator.enqueue(
            agent,
            "Fail this task.",
            owner_id=test_owner_id,
        )
        await _drain_registered_queues(
            redis_client=redis_client,
            namespace=test_namespace,
            agents_by_name=orchestrator.agents_by_name,
            pickup_script=pickup_script,
            completion_script=completion_script,
            steering_script=steering_script,
            max_retries=0,
        )
        result = await task.wait(timeout=2)
        snapshot = await task.snapshot()
    finally:
        await _close_agents(agent)

    assert result.status is RunStatus.FAILED
    assert result.output is None
    assert snapshot.status is TaskSnapshotStatus.FAILED


@pytest.mark.asyncio
async def test_subagent_waiting_task_surfaces_pending_child_task_ids_and_completes(
    redis_client: redis.Redis,
    test_namespace: str,
    test_owner_id: str,
    pickup_script: BatchPickupScript,
    completion_script: TaskCompletionScript,
    steering_script: TaskSteeringScript,
) -> None:
    child_agent = _make_echo_agent("child_research_agent")

    @tool
    async def research(queries: list[str]) -> Any:
        jobs = await subagents.spawn(
            agent=child_agent,
            inputs=[child_agent.build_context(input=query) for query in queries],
            key="research",
        )
        return wait.jobs(jobs, data={"kind": "research"})

    parent_agent = MockAgent(
        name="parent_research_agent",
        instructions="Spawn child research tasks, then finish once they complete.",
        tools=[research],
        responses=[
            tool_call("research", queries=["alpha", "beta"]),
            "Research complete.",
        ],
    )
    orchestrator = _make_orchestrator(
        redis_client=redis_client,
        namespace=test_namespace,
        agents=[parent_agent, child_agent],
    )
    try:
        task = await orchestrator.enqueue(
            parent_agent,
            "Research options.",
            owner_id=test_owner_id,
        )

        await _drain_agent_queue(
            redis_client=redis_client,
            namespace=test_namespace,
            agent=parent_agent,
            agents_by_name=orchestrator.agents_by_name,
            pickup_script=pickup_script,
            completion_script=completion_script,
            steering_script=steering_script,
        )

        waiting_snapshot = await task.snapshot()
        assert waiting_snapshot.status is TaskSnapshotStatus.WAITING
        assert waiting_snapshot.pending_child_task_ids
        assert len(waiting_snapshot.pending_child_task_ids) == 2

        await _drain_registered_queues(
            redis_client=redis_client,
            namespace=test_namespace,
            agents_by_name=orchestrator.agents_by_name,
            pickup_script=pickup_script,
            completion_script=completion_script,
            steering_script=steering_script,
        )
        result = await task.wait(timeout=2)
        final_snapshot = await task.snapshot()
    finally:
        await _close_agents(parent_agent, child_agent)

    assert result.status is RunStatus.COMPLETED
    assert result.output == "Research complete."
    assert final_snapshot.pending_child_task_ids == ()
    assert final_snapshot.status is TaskSnapshotStatus.COMPLETED


@pytest.mark.asyncio
async def test_task_wait_surfaces_verifier_retry_feedback_before_success(
    redis_client: redis.Redis,
    test_namespace: str,
    test_owner_id: str,
    pickup_script: BatchPickupScript,
    completion_script: TaskCompletionScript,
    steering_script: TaskSteeringScript,
) -> None:
    async def verifier(output: Any) -> Any:
        payload = json.loads(output)
        if payload["score"] < 5:
            return verify.retry("score too low", code="score_low")
        return verify.accept(metadata={"summary": payload["summary"], "verified": True})

    agent = MockAgent(
        name="queued_verification_agent",
        instructions="Produce a weak answer first, then revise after feedback.",
        responses=[
            json.dumps({"summary": "first attempt", "score": 1}),
            json.dumps({"summary": "second attempt", "score": 10}),
        ],
        verifier=verifier,
    )
    orchestrator = _make_orchestrator(
        redis_client=redis_client,
        namespace=test_namespace,
        agents=[agent],
    )
    try:
        task = await orchestrator.enqueue(
            agent,
            "Verify this output.",
            owner_id=test_owner_id,
        )

        observed_events: list[Any] = []

        async def _collect() -> None:
            async for event in task.updates(types=(TurnFinishEvent, FinishEvent)):
                observed_events.append(event)
                if isinstance(event, FinishEvent):
                    return

        collector = asyncio.create_task(_collect())
        await asyncio.sleep(0)
        await _drain_registered_queues(
            redis_client=redis_client,
            namespace=test_namespace,
            agents_by_name=orchestrator.agents_by_name,
            pickup_script=pickup_script,
            completion_script=completion_script,
            steering_script=steering_script,
        )
        await collector
        result = await task.wait(timeout=2)
    finally:
        await _close_agents(agent)

    assert result.status is RunStatus.COMPLETED
    assert result.output == json.dumps({"summary": "second attempt", "score": 10})
    assert agent.mock_client.call_count == 2
    assert sum(isinstance(event, TurnFinishEvent) for event in observed_events) == 2
    assert any(
        message.get("role") == "system"
        and "Verifier feedback [score_low]" in str(message.get("content", ""))
        for message in result.messages
    )


@pytest.mark.asyncio
async def test_task_wait_surfaces_verifier_failure_as_terminal_failed_status(
    redis_client: redis.Redis,
    test_namespace: str,
    test_owner_id: str,
    pickup_script: BatchPickupScript,
    completion_script: TaskCompletionScript,
    steering_script: TaskSteeringScript,
) -> None:
    async def verifier(_output: Any, *, agent_ctx: AgentContext) -> Any:
        if agent_ctx.verification.attempts_used >= 1:
            return verify.fail("verification retry limit reached", code="tests_failed")
        return verify.retry("not acceptable", code="tests_failed")

    agent = MockAgent(
        name="queued_verification_failure_agent",
        instructions="Keep producing unacceptable answers until verification fails.",
        responses=[
            json.dumps({"summary": "bad", "score": 0}),
            json.dumps({"summary": "still bad", "score": 0}),
        ],
        verifier=verifier,
    )
    orchestrator = _make_orchestrator(
        redis_client=redis_client,
        namespace=test_namespace,
        agents=[agent],
    )
    try:
        task = await orchestrator.enqueue(
            agent,
            "Reject until exhausted.",
            owner_id=test_owner_id,
        )
        await _drain_registered_queues(
            redis_client=redis_client,
            namespace=test_namespace,
            agents_by_name=orchestrator.agents_by_name,
            pickup_script=pickup_script,
            completion_script=completion_script,
            steering_script=steering_script,
        )
        result = await task.wait(timeout=2)
        snapshot = await task.snapshot()
    finally:
        await _close_agents(agent)

    assert result.status is RunStatus.FAILED
    assert snapshot.status is TaskSnapshotStatus.FAILED
    assert agent.mock_client.call_count == 2
    assert any(
        message.get("role") == "system"
        and "Verifier feedback [tests_failed]" in str(message.get("content", ""))
        for message in result.messages
    )


@pytest.mark.asyncio
async def test_pending_hook_handles_complete_task_and_support_idempotent_resolution(
    redis_client: redis.Redis,
    test_namespace: str,
    test_owner_id: str,
    pickup_script: BatchPickupScript,
    completion_script: TaskCompletionScript,
    steering_script: TaskSteeringScript,
) -> None:
    def _make_hook_agent(name: str) -> Agent[Any, Any]:
        return MockAgent(
            name=name,
            instructions="Request approval and finish after the hook resolves.",
            tools=[request_transfer_tool],
            responses=[tool_call("request_transfer"), "Transfer completed."],
        )

    complete_agent = _make_hook_agent("hook_complete_agent")
    idempotent_agent = _make_hook_agent("hook_idempotent_agent")
    orchestrator = _make_orchestrator(
        redis_client=redis_client,
        namespace=test_namespace,
        agents=[complete_agent, idempotent_agent],
    )
    try:
        complete_task = await orchestrator.enqueue(
            complete_agent,
            "Approve this transfer.",
            owner_id=test_owner_id,
        )
        await _drain_agent_queue(
            redis_client=redis_client,
            namespace=test_namespace,
            agent=complete_agent,
            agents_by_name=orchestrator.agents_by_name,
            pickup_script=pickup_script,
            completion_script=completion_script,
            steering_script=steering_script,
        )

        waiting_snapshot = await complete_task.snapshot()
        assert waiting_snapshot.status is TaskSnapshotStatus.WAITING
        assert len(waiting_snapshot.pending_hooks) == 1
        hook_snapshot = waiting_snapshot.pending_hooks[0]
        assert hook_snapshot.tool_name == "request_transfer"
        assert hook_snapshot.metadata == {"channel": "ops"}

        hook_handle = await complete_task.hook(hook_snapshot.id)
        completion = await hook_handle.complete({"approved": True})
        assert completion.status is HookCompletionStatus.RESOLVED
        assert completion.task_resumed is True

        await _drain_registered_queues(
            redis_client=redis_client,
            namespace=test_namespace,
            agents_by_name=orchestrator.agents_by_name,
            pickup_script=pickup_script,
            completion_script=completion_script,
            steering_script=steering_script,
        )
        complete_result = await complete_task.wait(timeout=2)

        idempotent_task = await orchestrator.enqueue(
            idempotent_agent,
            "Approve idempotently.",
            owner_id=test_owner_id,
        )
        await _drain_agent_queue(
            redis_client=redis_client,
            namespace=test_namespace,
            agent=idempotent_agent,
            agents_by_name=orchestrator.agents_by_name,
            pickup_script=pickup_script,
            completion_script=completion_script,
            steering_script=steering_script,
        )

        idempotent_hook = (await idempotent_task.hooks())[0]
        rotated_token = await orchestrator.rotate_hook_token(
            hook_id=idempotent_hook.snapshot.id,
            revoke_previous=False,
        )
        first_resolution = await orchestrator.resolve_hook(
            hook_id=idempotent_hook.snapshot.id,
            payload={"approved": True},
            token=rotated_token,
            idempotency_key="approval-event-1",
        )
        second_resolution = await orchestrator.resolve_hook(
            hook_id=idempotent_hook.snapshot.id,
            payload={"approved": True},
            token=rotated_token,
            idempotency_key="approval-event-1",
        )
        await _drain_registered_queues(
            redis_client=redis_client,
            namespace=test_namespace,
            agents_by_name=orchestrator.agents_by_name,
            pickup_script=pickup_script,
            completion_script=completion_script,
            steering_script=steering_script,
        )
        idempotent_result = await idempotent_task.wait(timeout=2)
    finally:
        await _close_agents(complete_agent, idempotent_agent)

    assert complete_result.status is RunStatus.COMPLETED
    assert complete_result.output == "Transfer completed."
    assert any(message.get("role") == "tool" for message in complete_result.messages)

    assert first_resolution.status == "resolved"
    assert second_resolution.status == "idempotent"
    assert idempotent_result.status is RunStatus.COMPLETED
    assert idempotent_result.output == "Transfer completed."
