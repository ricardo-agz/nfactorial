"""Contracts for agent output verifier configuration and lifecycle."""

from __future__ import annotations

import uuid
from contextvars import Token
from typing import Any, cast

import httpx
import pytest
from pydantic import BaseModel

from factorial import Agent, AgentContext, ExecutionContext
from factorial.context import execution_context
from factorial.events import EventPublisher
from factorial.exceptions import (
    FatalAgentError,
    InvalidLLMResponseError,
    VerificationRejected,
)
from factorial.llms import Model, Provider
from tests.mocks.llm import MockLLMClient, MockResponse, MockToolCall

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
        retries=0,
        iterations=0,
        events=cast(EventPublisher, event_capture),
    )
    return execution_context.set(ctx)


def _make_agent(
    *,
    mock_client: MockLLMClient,
    verifier: Any | None = None,
    verifier_max_attempts: int = 3,
) -> Agent:
    http_client = httpx.AsyncClient(verify=False, trust_env=False)
    return Agent(
        model=MOCK_MODEL,
        client=mock_client,
        http_client=http_client,
        output_type=OutputPayload,
        verifier=verifier,
        verifier_max_attempts=verifier_max_attempts,
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
                tool_calls=[
                    MockToolCall(
                        name="final_output",
                        arguments={"summary": "ready", "score": 99},
                    )
                ]
            )
        ]
    )

    async def verifier(
        output: OutputPayload,
        agent_ctx: AgentContext,
        execution_ctx: ExecutionContext,
    ) -> dict[str, Any]:
        assert output.summary == "ready"
        assert output.score == 99
        assert agent_ctx.query == "verify me"
        assert execution_ctx.owner_id == "test-owner"
        return {"accepted_summary": output.summary, "verified": True}

    agent = _make_agent(mock_client=mock_client, verifier=verifier)
    try:
        ctx = AgentContext(query="verify me")
        completion = await agent.run_turn(ctx)

        assert completion.is_done is True
        assert completion.output == {"accepted_summary": "ready", "verified": True}
        assert ctx.verification.attempts_used == 0
        assert ctx.verification.last_outcome == "passed"
        assert "verification_passed" in _event_types(event_capture)
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
                tool_calls=[
                    MockToolCall(
                        name="final_output",
                        arguments={"summary": "bad answer", "score": 1},
                    )
                ]
            )
        ]
    )

    async def verifier(output: OutputPayload) -> dict[str, Any]:
        raise VerificationRejected(
            message=f"score too low: {output.score}",
            code="score_low",
            metadata={"score": output.score},
        )

    agent = _make_agent(mock_client=mock_client, verifier=verifier)
    try:
        ctx = AgentContext(query="verify me")
        completion = await agent.run_turn(ctx)

        assert completion.is_done is False
        assert ctx.turn == 1
        assert ctx.verification.attempts_used == 1
        assert ctx.verification.last_outcome == "rejected"
        assert any(
            "verification_rejected" in message.get("content", "")
            for message in ctx.messages
            if message.get("role") == "user"
        )
        assert "verification_rejected" in _event_types(event_capture)
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
                tool_calls=[
                    MockToolCall(
                        name="final_output",
                        arguments={"summary": "same", "score": 3},
                    )
                ]
            ),
            MockResponse(
                tool_calls=[
                    MockToolCall(
                        name="final_output",
                        arguments={"summary": "same", "score": 3},
                    )
                ]
            ),
        ]
    )

    async def verifier(_output: OutputPayload) -> dict[str, Any]:
        raise VerificationRejected(
            message="still not good enough",
            code="needs_revision",
        )

    agent = _make_agent(
        mock_client=mock_client,
        verifier=verifier,
        verifier_max_attempts=5,
    )
    try:
        ctx = AgentContext(query="verify me")
        first = await agent.run_turn(ctx)
        second = await agent.run_turn(ctx)

        assert first.is_done is False
        assert second.is_done is False
        assert ctx.turn == 2
        assert ctx.verification.attempts_used == 1

        rejected_events = [
            event
            for event in event_capture.events
            if getattr(event, "event_type", "") == "verification_rejected"
        ]
        assert len(rejected_events) == 2
        assert rejected_events[0].data["attempt_counted"] is True
        assert rejected_events[1].data["attempt_counted"] is False
    finally:
        execution_context.reset(token)
        await agent.http_client.aclose()


@pytest.mark.asyncio
async def test_verifier_exhaustion_raises_fatal_agent_error() -> None:
    event_capture = _EventCapture()
    token = _set_test_execution_context(event_capture)
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

    async def verifier(_output: OutputPayload) -> dict[str, Any]:
        raise VerificationRejected(message="hard fail", code="tests_failed")

    agent = _make_agent(
        mock_client=mock_client,
        verifier=verifier,
        verifier_max_attempts=1,
    )
    try:
        with pytest.raises(
            FatalAgentError,
            match="Verification attempt budget exhausted",
        ):
            await agent.run_turn(AgentContext(query="verify me"))

        assert "verification_rejected" in _event_types(event_capture)
        assert "verification_exhausted" in _event_types(event_capture)
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
                tool_calls=[
                    MockToolCall(
                        name="final_output",
                        arguments={"summary": "sync-path", "score": 7},
                    )
                ]
            )
        ]
    )

    def verifier(output: OutputPayload) -> dict[str, Any]:
        return {"summary": output.summary, "score_bucket": "high"}

    agent = _make_agent(mock_client=mock_client, verifier=verifier)
    try:
        completion = await agent.run_turn(AgentContext(query="verify me"))
        assert completion.is_done is True
        assert completion.output == {"summary": "sync-path", "score_bucket": "high"}
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
                tool_calls=[
                    MockToolCall(
                        name="final_output",
                        arguments={"summary": "good-shape", "score": 42},
                    )
                ]
            )
        ]
    )

    async def verifier(_output: OutputPayload) -> dict[str, Any]:
        raise RuntimeError("verification infra outage")

    agent = _make_agent(mock_client=mock_client, verifier=verifier)
    try:
        ctx = AgentContext(query="verify me")
        with pytest.raises(RuntimeError, match="verification infra outage"):
            await agent.run_turn(ctx)
        assert ctx.verification.attempts_used == 0
        assert ctx.verification.last_outcome == "system_error"
    finally:
        execution_context.reset(token)
        await agent.http_client.aclose()


@pytest.mark.asyncio
async def test_verifier_validates_output_payload_against_output_type() -> None:
    event_capture = _EventCapture()
    token = _set_test_execution_context(event_capture)
    mock_client = MockLLMClient(
        responses=[
            MockResponse(
                tool_calls=[
                    MockToolCall(
                        name="final_output",
                        arguments={"summary": "missing score"},
                    )
                ]
            )
        ]
    )
    verifier_called = False

    async def verifier(_output: OutputPayload) -> dict[str, Any]:
        nonlocal verifier_called
        verifier_called = True
        return {"ok": True}

    agent = _make_agent(mock_client=mock_client, verifier=verifier)
    try:
        with pytest.raises(
            InvalidLLMResponseError,
            match="failed output_type validation",
        ):
            await agent.run_turn(AgentContext(query="verify me"))
        assert verifier_called is False
    finally:
        execution_context.reset(token)
        await agent.http_client.aclose()


@pytest.mark.asyncio
async def test_verifier_requires_output_type_and_positive_attempt_budget() -> None:
    client = MockLLMClient()
    http_client_one = httpx.AsyncClient(verify=False, trust_env=False)
    with pytest.raises(ValueError, match="verifier requires output_type"):
        Agent(
            model=MOCK_MODEL,
            client=client,
            http_client=http_client_one,
            verifier=lambda output: output,
        )
    await http_client_one.aclose()

    http_client_two = httpx.AsyncClient(verify=False, trust_env=False)
    with pytest.raises(
        ValueError, match="verifier_max_attempts must be greater than 0"
    ):
        Agent(
            model=MOCK_MODEL,
            client=client,
            http_client=http_client_two,
            output_type=OutputPayload,
            verifier=lambda output: output,
            verifier_max_attempts=0,
        )
    await http_client_two.aclose()
