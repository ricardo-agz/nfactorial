"""Contracts for agent output verifier configuration and lifecycle."""

from __future__ import annotations

import json
import uuid
from contextvars import Token
from typing import Any, cast

import httpx
import pytest
from pydantic import BaseModel

from factorial import Agent, AgentContext, ExecutionContext, verify
from factorial.ai.models import Model, Provider
from factorial.core.events import EventPublisher
from factorial.core.exceptions import FatalAgentError
from factorial.execution.context import execution_context
from tests.mocks.llm import MockLLMClient, MockResponse

MOCK_MODEL = Model(
    name="mock-model",
    provider=Provider.OPENAI,
    provider_model_id="mock-model-v1",
    context_window=128000,
)


class OutputPayload(BaseModel):
    summary: str
    score: int


class _EventCapture:
    def __init__(self) -> None:
        self.events: list[Any] = []

    async def publish_event(self, event: object) -> None:
        self.events.append(event)


def _set_test_execution_context(
    event_capture: _EventCapture,
) -> Token[ExecutionContext]:
    ctx = ExecutionContext(
        task_id=str(uuid.uuid4()),
        owner_id="test-owner",
        retry_count=0,
        events=cast(EventPublisher, event_capture),
    )
    return execution_context.set(ctx)


def _make_agent(
    *,
    mock_client: MockLLMClient,
    verifier: Any | None = None,
) -> Agent:
    http_client = httpx.AsyncClient(verify=False, trust_env=False)
    return Agent(
        model=MOCK_MODEL,
        client=mock_client,
        http_client=http_client,
        verifier=verifier,
    )


def _event_types(event_capture: _EventCapture) -> list[str]:
    return [getattr(event, "event_type", "") for event in event_capture.events]


@pytest.mark.asyncio
async def test_verifier_passes_and_transforms_output() -> None:
    event_capture = _EventCapture()
    token = _set_test_execution_context(event_capture)
    mock_client = MockLLMClient(
        responses=[
            MockResponse(
                content=json.dumps({"summary": "ready", "score": 99}),
                is_final=True,
            )
        ]
    )

    async def verifier(
        output: Any,
        agent_ctx: AgentContext,
        execution_ctx: ExecutionContext,
    ) -> dict[str, Any]:
        parsed = (
            OutputPayload.model_validate(json.loads(output))
            if isinstance(output, str)
            else output
        )
        assert parsed.summary == "ready"
        assert parsed.score == 99
        first_user = next(
            (m for m in agent_ctx.messages if m.get("role") == "user"), None
        )
        assert first_user and first_user.get("content") == "verify me"
        assert execution_ctx.owner_id == "test-owner"
        return verify.accept(
            metadata={"accepted_summary": parsed.summary, "verified": True}
        )

    agent = _make_agent(mock_client=mock_client, verifier=verifier)
    try:
        ctx = AgentContext(messages=[{"role": "user", "content": "verify me"}])
        completion = await agent.run_turn(ctx)

        assert completion.is_done is True
        assert completion.output == json.dumps({"summary": "ready", "score": 99})
        assert ctx.verification.attempts_used == 0
        assert ctx.verification.last_outcome == "passed"
        assert "turn_finish" in _event_types(event_capture)
    finally:
        execution_context.reset(token)
        await agent.http_client.aclose()


@pytest.mark.asyncio
async def test_verifier_rejection_continues_and_counts_attempt() -> None:
    event_capture = _EventCapture()
    token = _set_test_execution_context(event_capture)
    mock_client = MockLLMClient(
        responses=[
            MockResponse(
                content=json.dumps({"summary": "bad answer", "score": 1}),
                is_final=True,
            )
        ]
    )

    async def verifier(output: Any):
        parsed = (
            OutputPayload.model_validate(json.loads(output))
            if isinstance(output, str)
            else output
        )
        return verify.retry(
            message=f"score too low: {parsed.score}",
            code="score_low",
            metadata={"score": parsed.score},
        )

    agent = _make_agent(mock_client=mock_client, verifier=verifier)
    try:
        ctx = AgentContext(messages=[{"role": "user", "content": "verify me"}])
        completion = await agent.run_turn(ctx)

        assert completion.is_done is False
        assert ctx.turn_number == 2
        assert ctx.verification.attempts_used == 1
        assert ctx.verification.last_outcome == "retry_requested"
        assert any(
            "Verifier feedback" in str(message.get("content", ""))
            for message in ctx.messages
            if message.get("role") == "system"
        )
        assert "turn_finish" in _event_types(event_capture)
    finally:
        execution_context.reset(token)
        await agent.http_client.aclose()


@pytest.mark.asyncio
async def test_verifier_replay_rejection_does_not_double_count_attempt() -> None:
    event_capture = _EventCapture()
    token = _set_test_execution_context(event_capture)
    mock_client = MockLLMClient(
        responses=[
            MockResponse(
                content=json.dumps({"summary": "same", "score": 3}),
                is_final=True,
            ),
            MockResponse(
                content=json.dumps({"summary": "same", "score": 3}),
                is_final=True,
            ),
        ]
    )

    async def verifier(_output: Any):
        return verify.retry(
            message="still not good enough",
            code="needs_revision",
        )

    agent = _make_agent(mock_client=mock_client, verifier=verifier)
    try:
        ctx = AgentContext(messages=[{"role": "user", "content": "verify me"}])
        first = await agent.run_turn(ctx)
        second = await agent.run_turn(ctx)

        assert first.is_done is False
        assert second.is_done is False
        assert ctx.turn_number == 3
        assert ctx.verification.attempts_used == 2

        assert ctx.verification.attempts_used == 2
        assert ctx.verification.last_outcome == "retry_requested"
    finally:
        execution_context.reset(token)
        await agent.http_client.aclose()


@pytest.mark.asyncio
async def test_verifier_can_enforce_retry_limit_from_agent_context() -> None:
    event_capture = _EventCapture()
    token = _set_test_execution_context(event_capture)
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

    async def verifier(_output: Any, agent_ctx: AgentContext):
        if agent_ctx.verification.attempts_used >= 1:
            return verify.fail(
                message="verification retry limit reached",
                code="tests_failed",
            )
        return verify.retry(message="hard fail", code="tests_failed")

    agent = _make_agent(mock_client=mock_client, verifier=verifier)
    try:
        ctx = AgentContext(messages=[{"role": "user", "content": "verify me"}])
        first = await agent.run_turn(ctx)
        assert first.is_done is False
        with pytest.raises(
            FatalAgentError,
            match="verification retry limit reached",
        ):
            await agent.run_turn(ctx)

        assert "turn_finish" in _event_types(event_capture)
    finally:
        execution_context.reset(token)
        await agent.http_client.aclose()


@pytest.mark.asyncio
async def test_verifier_supports_sync_callable() -> None:
    event_capture = _EventCapture()
    token = _set_test_execution_context(event_capture)
    mock_client = MockLLMClient(
        responses=[
            MockResponse(
                content=json.dumps({"summary": "sync-path", "score": 7}),
                is_final=True,
            )
        ]
    )

    def verifier(output: Any):
        parsed = (
            OutputPayload.model_validate(json.loads(output))
            if isinstance(output, str)
            else output
        )
        return verify.accept(
            metadata={"summary": parsed.summary, "score_bucket": "high"}
        )

    agent = _make_agent(mock_client=mock_client, verifier=verifier)
    try:
        completion = await agent.run_turn(
            AgentContext(messages=[{"role": "user", "content": "verify me"}])
        )
        assert completion.is_done is True
        assert completion.is_done is True
        assert completion.output == json.dumps({"summary": "sync-path", "score": 7})
    finally:
        execution_context.reset(token)
        await agent.http_client.aclose()


@pytest.mark.asyncio
async def test_verifier_system_error_is_not_counted() -> None:
    event_capture = _EventCapture()
    token = _set_test_execution_context(event_capture)
    mock_client = MockLLMClient(
        responses=[
            MockResponse(
                content=json.dumps({"summary": "good-shape", "score": 42}),
                is_final=True,
            )
        ]
    )

    async def verifier(_output: Any):
        raise RuntimeError("verification infra outage")

    agent = _make_agent(mock_client=mock_client, verifier=verifier)
    try:
        ctx = AgentContext(messages=[{"role": "user", "content": "verify me"}])
        with pytest.raises(RuntimeError, match="verification infra outage"):
            await agent.run_turn(ctx)
        assert ctx.verification.attempts_used == 0
        assert ctx.verification.last_outcome is None
    finally:
        execution_context.reset(token)
        await agent.http_client.aclose()


@pytest.mark.asyncio
async def test_verifier_receives_invalid_json_output() -> None:
    """Verifier receives raw output; invalid JSON may cause verifier to fail."""
    event_capture = _EventCapture()
    token = _set_test_execution_context(event_capture)
    mock_client = MockLLMClient(
        responses=[
            MockResponse(
                content="not valid json",
                is_final=True,
            )
        ]
    )
    verifier_called = False

    async def verifier(output: Any) -> dict[str, Any]:
        nonlocal verifier_called
        verifier_called = True
        OutputPayload.model_validate(json.loads(output))
        return {"ok": True}

    agent = _make_agent(mock_client=mock_client, verifier=verifier)
    try:
        with pytest.raises((ValueError, Exception)):
            await agent.run_turn(
                AgentContext(messages=[{"role": "user", "content": "verify me"}])
            )
        assert verifier_called is True
    finally:
        execution_context.reset(token)
        await agent.http_client.aclose()


@pytest.mark.asyncio
async def test_agent_does_not_accept_verifier_max_attempts() -> None:
    client = MockLLMClient()
    http_client = httpx.AsyncClient(verify=False, trust_env=False)
    try:
        with pytest.raises(TypeError, match="verifier_max_attempts"):
            Agent(
                model=MOCK_MODEL,
                client=client,
                http_client=http_client,
                verifier=lambda output: output,
                verifier_max_attempts=1,
            )
    finally:
        await http_client.aclose()
