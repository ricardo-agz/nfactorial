"""High-signal contracts for prepare_turn behavior."""

from __future__ import annotations

import pytest

from factorial import RunStatus, tool
from factorial.ai.models import Model, Provider
from factorial.testing import MockAgent


@pytest.mark.asyncio
async def test_prepare_turn_mutates_next_request_without_rewriting_transcript() -> None:
    @tool
    def unused_tool() -> str:
        return "unused"

    seen: dict[str, object] = {}

    def prepare_turn(turn, agent_ctx, execution_ctx) -> None:
        seen["task_id"] = execution_ctx.task_id
        seen["turn_number"] = agent_ctx.turn_number
        turn.model = Model(
            name="override-model",
            provider=Provider.OPENAI,
            provider_model_id="override-v1",
            context_window=64000,
        )
        turn.messages = [
            {"role": "system", "content": "Runtime-compacted prompt."},
            {"role": "user", "content": "Only send this input."},
        ]
        turn.tools = []
        turn.tool_choice = "required"
        turn.parallel_tool_calls = False
        turn.temperature = 0.1
        turn.max_output_tokens = 32

    agent = MockAgent(
        name="prepare_turn_agent",
        instructions="Base instructions.",
        tools=[unused_tool],
        responses=["Prepared."],
        prepare_turn=prepare_turn,
    )
    try:
        result = await agent.run("Original input.")
    finally:
        await agent.http_client.aclose()

    first_call = agent.mock_client.call_history[0]
    assert seen["task_id"] == result.task_id
    assert seen["turn_number"] == 1
    assert first_call["model"] == "override-model"
    assert first_call["messages"] == [
        {"role": "system", "content": "Runtime-compacted prompt."},
        {"role": "user", "content": "Only send this input."},
    ]
    assert first_call["tools"] is None
    assert first_call["tool_choice"] == "required"
    assert first_call["parallel_tool_calls"] is False
    assert first_call["temperature"] == 0.1
    assert first_call["max_completion_tokens"] == 32

    # prepare_turn shapes only the next request. It should not rewrite the live transcript.
    assert result.status is RunStatus.COMPLETED
    assert result.messages == (
        {"role": "system", "content": "Base instructions."},
        {"role": "user", "content": "Original input."},
        {"role": "assistant", "content": "Prepared."},
    )


@pytest.mark.asyncio
async def test_prepare_turn_authoring_error_fails_run_with_clear_message() -> None:
    def prepare_turn(turn, unsupported) -> None:
        del turn, unsupported

    agent = MockAgent(
        name="prepare_turn_error_agent",
        instructions="",
        responses=["unused"],
        prepare_turn=prepare_turn,
    )
    try:
        result = await agent.run("Trigger prepare_turn error.")
    finally:
        await agent.http_client.aclose()

    assert result.status is RunStatus.FAILED
    assert result.error is not None
    assert "Unsupported required prepare_turn parameter 'unsupported'" in (
        result.error.message
    )
    assert "agent_ctx" in result.error.message
    assert "execution_ctx" in result.error.message
