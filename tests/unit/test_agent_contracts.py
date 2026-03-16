"""High-signal contracts for the public agent API."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

import pytest

from factorial import (
    AgentContext,
    ExecutionContext,
    FinishEvent,
    ModelFinishEvent,
    ModelStartEvent,
    RunStatus,
    StartEvent,
    TurnFinishEvent,
    TurnStartEvent,
    tool,
    tool_called,
    turn_count_is,
    verify,
)
from factorial.testing import MockAgent, tool_call


@dataclass
class SessionState:
    priority: int = 0


@dataclass
class RequestMetadata:
    source: str = "cli"


@dataclass
class RequiredState:
    repo: str


def _first_system_message(messages: list[dict[str, Any]]) -> str | None:
    for message in messages:
        if message.get("role") == "system" and isinstance(message.get("content"), str):
            return message["content"]
    return None


@pytest.mark.asyncio
async def test_agent_run_returns_run_result_with_typed_state_and_metadata() -> None:
    agent = MockAgent[SessionState, RequestMetadata](
        name="typed_state_metadata_agent",
        instructions="",
        responses=["All set."],
    )
    try:
        result = await agent.run(
            "Ship the migration plan.",
            state={"priority": 7},
            metadata={"source": "api"},
        )
    finally:
        await agent.http_client.aclose()

    assert result.status is RunStatus.COMPLETED
    assert result.output == "All set."
    assert result.state == SessionState(priority=7)
    assert result.metadata == RequestMetadata(source="api")
    assert result.turn_count == 1
    assert result.messages[0] == {
        "role": "user",
        "content": "Ship the migration plan.",
    }
    assert result.messages[-1] == {"role": "assistant", "content": "All set."}


@pytest.mark.asyncio
async def test_agent_run_requires_explicit_non_default_state() -> None:
    agent = MockAgent[RequiredState, RequestMetadata](
        name="required_state_agent",
        instructions="",
        responses=["unused"],
    )
    try:
        with pytest.raises(ValueError, match="state must be provided"):
            await agent.run("Missing required state.")
    finally:
        await agent.http_client.aclose()


@pytest.mark.asyncio
async def test_agent_stream_yields_typed_lifecycle_events() -> None:
    agent = MockAgent(
        name="streaming_agent",
        instructions="",
        responses=["Ready."],
    )
    try:
        events = [event async for event in agent.stream("Stream this run.")]
    finally:
        await agent.http_client.aclose()

    assert [type(event) for event in events] == [
        StartEvent,
        TurnStartEvent,
        ModelStartEvent,
        ModelFinishEvent,
        TurnFinishEvent,
        FinishEvent,
    ]
    finish_event = events[-1]
    assert isinstance(finish_event, FinishEvent)
    assert finish_event.status is RunStatus.COMPLETED
    assert finish_event.output == "Ready."


@pytest.mark.asyncio
async def test_stop_when_explicit_finish_tool_returns_tool_output() -> None:
    @tool
    def done(summary: str) -> dict[str, str]:
        return {"summary": summary}

    agent = MockAgent(
        name="finish_tool_agent",
        instructions="",
        tools=[done],
        stop_when=tool_called("done"),
        responses=[
            tool_call("done", summary="Ship it."),
            "should not be reached",
        ],
    )
    try:
        result = await agent.run("Finish with the done tool.")
    finally:
        await agent.http_client.aclose()

    assert result.status is RunStatus.COMPLETED
    assert result.output == {"summary": "Ship it."}
    assert result.messages[-2]["role"] == "assistant_tool_calls"
    assert result.messages[-1]["role"] == "tool"


@pytest.mark.asyncio
async def test_turn_limit_stop_when_fails_without_finalized_output() -> None:
    @tool
    def noop() -> None:
        return None

    agent = MockAgent(
        name="turn_limit_agent",
        instructions="",
        tools=[noop],
        stop_when=turn_count_is(1),
        responses=[tool_call("noop"), "should not be reached"],
    )
    try:
        result = await agent.run("Trigger a non-final tool call.")
    finally:
        await agent.http_client.aclose()

    assert result.status is RunStatus.FAILED
    assert result.output is None
    assert result.error is not None
    assert "finalized output" in result.error.message


@pytest.mark.asyncio
async def test_verifier_retry_loops_without_transforming_final_output() -> None:
    async def verifier(
        output: Any,
        *,
        agent_ctx: AgentContext,
        execution_ctx: ExecutionContext,
    ) -> Any:
        parsed = json.loads(output)
        if parsed["score"] < 5:
            return verify.retry(
                "Need stronger evidence.",
                code="needs_evidence",
                metadata={"score": parsed["score"]},
            )
        return verify.accept(
            metadata={
                "verified": True,
                "owner_id": execution_ctx.owner_id,
                "turn_number": agent_ctx.turn_number,
            }
        )

    agent = MockAgent(
        name="verifier_retry_agent",
        instructions="",
        responses=[
            json.dumps({"summary": "draft", "score": 1}),
            json.dumps({"summary": "final", "score": 9}),
        ],
        verifier=verifier,
    )
    try:
        result = await agent.run("Verify this answer.")
    finally:
        await agent.http_client.aclose()

    assert result.status is RunStatus.COMPLETED
    assert result.output == json.dumps({"summary": "final", "score": 9})
    assert result.verification is not None
    assert result.verification.attempts_used == 1
    assert result.verification.metadata == {
        "verified": True,
        "owner_id": result.owner_id,
        "turn_number": 2,
    }
    assert agent.mock_client.call_count == 2
    feedback_message = _first_system_message(agent.mock_client.call_history[1]["messages"])
    assert feedback_message is not None
    assert "Verifier feedback [needs_evidence]" in feedback_message
