from __future__ import annotations

from collections.abc import Iterable

import pytest

from factorial import RunStatus, tool
from factorial.testing import MockAgent, assistant, tool_call


async def _close_agents(agents: Iterable[MockAgent]) -> None:
    for agent in agents:
        await agent.http_client.aclose()


@pytest.mark.asyncio
async def test_mock_agent_string_response_completes_run() -> None:
    agent = MockAgent(
        name="greeter",
        instructions="Return a greeting.",
        responses=["hello from mock"],
    )
    try:
        result = await agent.run("say hello")
        assert result.status is RunStatus.COMPLETED
        assert result.output == "hello from mock"
        assert agent.mock_client.call_count == 1
    finally:
        await _close_agents([agent])


@pytest.mark.asyncio
async def test_mock_agent_bare_tool_call_executes_tool() -> None:
    observed: list[str] = []

    @tool
    def remember(value: str) -> str:
        observed.append(value)
        return f"stored:{value}"

    agent = MockAgent(
        name="tool_user",
        instructions="Use the remember tool before finishing.",
        tools=[remember],
        responses=[
            tool_call("remember", value="hello"),
            "done",
        ],
    )
    try:
        result = await agent.run("store this")
        assert result.status is RunStatus.COMPLETED
        assert result.output == "done"
        assert observed == ["hello"]
        assert agent.mock_client.call_count == 2
    finally:
        await _close_agents([agent])


@pytest.mark.asyncio
async def test_mock_agent_assistant_can_mix_text_and_tool_calls() -> None:
    observed: list[str] = []

    @tool
    def remember(value: str) -> str:
        observed.append(value)
        return f"stored:{value}"

    agent = MockAgent(
        name="mixed_reply_agent",
        instructions="Reply with text and a tool call.",
        tools=[remember],
        responses=[
            assistant("thinking...", tool_call("remember", value="mixed")),
            "done",
        ],
    )
    try:
        result = await agent.run("do both")
        assert result.status is RunStatus.COMPLETED
        assert result.output == "done"
        assert observed == ["mixed"]
        assert any(
            message.get("role") == "assistant"
            and message.get("content") == "thinking..."
            for message in result.messages
            if isinstance(message, dict)
        )
    finally:
        await _close_agents([agent])


@pytest.mark.asyncio
async def test_mock_agent_exception_response_fails_run() -> None:
    agent = MockAgent(
        name="failing_agent",
        instructions="Fail immediately.",
        responses=[RuntimeError("boom")],
    )
    try:
        result = await agent.run("fail")
        assert result.status is RunStatus.FAILED
        assert result.error is not None
        assert result.error.message == "boom"
    finally:
        await _close_agents([agent])


@pytest.mark.asyncio
async def test_mock_agent_responses_are_tracked_per_task() -> None:
    agent = MockAgent(
        name="per_task_agent",
        instructions="Return the first scripted response for each task.",
        responses=["fresh response"],
    )
    try:
        first = await agent.run("first task")
        second = await agent.run("second task")

        assert first.status is RunStatus.COMPLETED
        assert second.status is RunStatus.COMPLETED
        assert first.output == "fresh response"
        assert second.output == "fresh response"

        task_keys = [entry["task_key"] for entry in agent.mock_client.call_history]
        assert len(task_keys) == 2
        assert task_keys[0] != task_keys[1]
        assert all(entry["response_index"] == 0 for entry in agent.mock_client.call_history)
    finally:
        await _close_agents([agent])
