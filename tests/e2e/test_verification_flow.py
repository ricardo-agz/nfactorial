"""E2E tests for output verification flow through worker processing."""

from __future__ import annotations

from typing import Any

import pytest
import redis.asyncio as redis
from pydantic import BaseModel

from factorial.context import AgentContext
from factorial.exceptions import VerificationRejected
from factorial.queue.keys import RedisKeys
from factorial.queue.lua import (
    create_batch_pickup_script,
    create_task_completion_script,
    create_task_steering_script,
)
from factorial.queue.operations import enqueue_task
from factorial.queue.task import Task, TaskStatus, get_task_data, get_task_status
from factorial.queue.worker import process_task
from tests.mocks.llm import MockLLMClient, MockResponse, MockToolCall

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
                tool_calls=[
                    MockToolCall(
                        name="final_output",
                        arguments={"summary": "first attempt", "score": 1},
                    )
                ]
            ),
            MockResponse(
                tool_calls=[
                    MockToolCall(
                        name="final_output",
                        arguments={"summary": "second attempt", "score": 10},
                    )
                ]
            ),
        ]
    )

    async def verifier(output: VerificationOutput) -> dict[str, Any]:
        if output.score < 5:
            raise VerificationRejected(message="score too low", code="score_low")
        return {"summary": output.summary, "verified": True}

    agent = MockLLMAgent(
        mock_client=mock_client,
        name="verification_agent",
        model=MOCK_MODEL,
        output_type=VerificationOutput,
        verifier=verifier,
        verifier_max_attempts=3,
    )
    try:
        keys = RedisKeys.format(namespace=test_namespace, agent=agent.name)
        completion_script = await create_task_completion_script(redis_client)
        steering_script = await create_task_steering_script(redis_client)
        agents_by_name: dict[str, Any] = {agent.name: agent}

        task = Task.create(
            owner_id=test_owner_id,
            agent=agent.name,
            payload=AgentContext(query="verify this output"),
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
        assert payload_after_rejection["verification"]["last_outcome"] == "rejected"
        assert any(
            "verification_rejected" in message.get("content", "")
            for message in payload_after_rejection["messages"]
            if message.get("role") == "user"
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
        assert final_payload["output"] == {
            "summary": "second attempt",
            "verified": True,
        }
        assert final_payload["verification"]["attempts_used"] == 1
        assert final_payload["verification"]["last_outcome"] == "passed"
    finally:
        await agent.http_client.aclose()


@pytest.mark.asyncio
async def test_verifier_exhaustion_fails_task(
    redis_client: redis.Redis,
    test_namespace: str,
    test_owner_id: str,
) -> None:
    mock_client = MockLLMClient(
        responses=[
            MockResponse(
                tool_calls=[
                    MockToolCall(
                        name="final_output",
                        arguments={"summary": "bad", "score": 0},
                    )
                ]
            )
        ]
    )

    async def verifier(_output: VerificationOutput) -> dict[str, Any]:
        raise VerificationRejected(message="not acceptable", code="tests_failed")

    agent = MockLLMAgent(
        mock_client=mock_client,
        name="verification_exhaustion_agent",
        model=MOCK_MODEL,
        output_type=VerificationOutput,
        verifier=verifier,
        verifier_max_attempts=1,
    )
    try:
        keys = RedisKeys.format(namespace=test_namespace, agent=agent.name)
        completion_script = await create_task_completion_script(redis_client)
        steering_script = await create_task_steering_script(redis_client)
        agents_by_name: dict[str, Any] = {agent.name: agent}

        task = Task.create(
            owner_id=test_owner_id,
            agent=agent.name,
            payload=AgentContext(query="reject until exhausted"),
        )
        task_id = await enqueue_task(
            redis_client=redis_client,
            namespace=test_namespace,
            agent=agent,
            task=task,
        )

        picked = await _pickup_single_task(redis_client=redis_client, keys=keys)
        assert picked == [task_id]

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
        assert status == TaskStatus.FAILED
    finally:
        await agent.http_client.aclose()
