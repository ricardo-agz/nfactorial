"""E2E tests for output verification flow through worker processing."""

from __future__ import annotations

import json
from typing import Any

import pytest
import redis.asyncio as redis
from pydantic import BaseModel

from factorial import verify
from factorial.agent.context import AgentContext
from factorial.queue.keys import RedisKeys
from factorial.queue.lua import (
    create_batch_pickup_script,
    create_task_completion_script,
    create_task_steering_script,
)
from factorial.queue.operations import enqueue_task
from factorial.queue.task import Task, TaskStatus, get_task_data, get_task_status
from factorial.queue.worker import process_task
from tests.mocks.llm import MockLLMClient, MockResponse

from .conftest import MOCK_MODEL, MockLLMAgent


class VerificationOutput(BaseModel):
    summary: str
    score: int


async def _pickup_single_task(
    *,
    redis_client: redis.Redis,
    keys: RedisKeys,
) -> list[str]:
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
    return result.tasks_to_process_ids


@pytest.mark.asyncio
async def test_verifier_rejection_persists_then_completes_on_revision(
    redis_client: redis.Redis,
    test_namespace: str,
    test_owner_id: str,
) -> None:
    mock_client = MockLLMClient(
        responses=[
            MockResponse(
                content=json.dumps({"summary": "first attempt", "score": 1}),
                is_final=True,
            ),
            MockResponse(
                content=json.dumps({"summary": "second attempt", "score": 10}),
                is_final=True,
            ),
        ]
    )

    async def verifier(output: Any):
        parsed = (
            VerificationOutput.model_validate(json.loads(output))
            if isinstance(output, str)
            else output
        )
        if parsed.score < 5:
            return verify.retry(
                message="score too low",
                code="score_low",
            )
        return verify.accept(metadata={"summary": parsed.summary, "verified": True})

    agent = MockLLMAgent(
        mock_client=mock_client,
        name="verification_agent",
        model=MOCK_MODEL,
        verifier=verifier,
    )
    try:
        keys = RedisKeys.format(namespace=test_namespace, agent=agent.name)
        completion_script = await create_task_completion_script(redis_client)
        steering_script = await create_task_steering_script(redis_client)
        agents_by_name: dict[str, Any] = {agent.name: agent}

        task = Task.create(
            owner_id=test_owner_id,
            agent=agent.name,
            payload=AgentContext(
                messages=[{"role": "user", "content": "verify this output"}]
            ),
        )
        task_id = await enqueue_task(
            redis_client=redis_client,
            namespace=test_namespace,
            agent=agent,
            task=task,
        )

        first_pick = await _pickup_single_task(redis_client=redis_client, keys=keys)
        assert first_pick == [task_id]

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

        status_after_rejection = await get_task_status(
            redis_client,
            test_namespace,
            task_id,
        )
        assert status_after_rejection == TaskStatus.ACTIVE

        task_data_after_rejection = await get_task_data(
            redis_client, test_namespace, task_id
        )
        payload_after_rejection = task_data_after_rejection["payload"]
        assert payload_after_rejection["verification"]["attempts_used"] == 1
        assert payload_after_rejection["verification"]["last_outcome"] == "retry_requested"
        assert any(
            "Verifier feedback" in str(message.get("content", ""))
            for message in payload_after_rejection["messages"]
            if message.get("role") == "system"
        )

        second_pick = await _pickup_single_task(redis_client=redis_client, keys=keys)
        assert second_pick == [task_id]

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

        final_status = await get_task_status(redis_client, test_namespace, task_id)
        assert final_status == TaskStatus.COMPLETED

        final_task_data = await get_task_data(redis_client, test_namespace, task_id)
        final_payload = final_task_data["payload"]
        assert final_payload["output"] == json.dumps(
            {"summary": "second attempt", "score": 10}
        )
        assert final_payload["verification"]["attempts_used"] == 1
        assert final_payload["verification"]["last_outcome"] == "passed"
    finally:
        await agent.http_client.aclose()


@pytest.mark.asyncio
async def test_verifier_can_fail_task_after_attempt_threshold(
    redis_client: redis.Redis,
    test_namespace: str,
    test_owner_id: str,
) -> None:
    mock_client = MockLLMClient(
        responses=[
            MockResponse(
                content=json.dumps({"summary": "bad", "score": 0}),
                is_final=True,
            ),
            MockResponse(
                content=json.dumps({"summary": "still bad", "score": 0}),
                is_final=True,
            ),
        ]
    )

    async def verifier(_output: Any, *, agent_ctx: AgentContext):
        if agent_ctx.verification.attempts_used >= 1:
            return verify.fail(
                message="verification retry limit reached",
                code="tests_failed",
            )
        return verify.retry(message="not acceptable", code="tests_failed")

    agent = MockLLMAgent(
        mock_client=mock_client,
        name="verification_exhaustion_agent",
        model=MOCK_MODEL,
        verifier=verifier,
    )
    try:
        keys = RedisKeys.format(namespace=test_namespace, agent=agent.name)
        completion_script = await create_task_completion_script(redis_client)
        steering_script = await create_task_steering_script(redis_client)
        agents_by_name: dict[str, Any] = {agent.name: agent}

        task = Task.create(
            owner_id=test_owner_id,
            agent=agent.name,
            payload=AgentContext(
                messages=[{"role": "user", "content": "reject until exhausted"}]
            ),
        )
        task_id = await enqueue_task(
            redis_client=redis_client,
            namespace=test_namespace,
            agent=agent,
            task=task,
        )

        # Process until terminal (verifier retry puts task back in queue; exhaust on 2nd turn)
        for _ in range(5):
            picked = await _pickup_single_task(redis_client=redis_client, keys=keys)
            if not picked:
                break
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
            status = await get_task_status(redis_client, test_namespace, task_id)
            if status in (TaskStatus.COMPLETED, TaskStatus.FAILED):
                break

        status = await get_task_status(redis_client, test_namespace, task_id)
        assert status == TaskStatus.FAILED
    finally:
        await agent.http_client.aclose()
