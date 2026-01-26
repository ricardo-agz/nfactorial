"""E2E test fixtures.

These tests use a real Redis instance (via fakeredis) and exercise
the full worker loop with mock LLM responses.
"""

from __future__ import annotations

import uuid
from collections.abc import AsyncGenerator
from typing import Any

import fakeredis.aioredis
import pytest
import pytest_asyncio
import redis.asyncio as redis

from factorial.agent import BaseAgent, TurnCompletion
from factorial.context import AgentContext
from factorial.llms import Model, Provider
from tests.mocks.llm import (
    MockLLMClient,
    MockResponse,
    MockToolCall,
    create_simple_mock,
)

MOCK_MODEL = Model(
    name="mock-model",
    provider=Provider.OPENAI,
    provider_model_id="mock-model-v1",
    context_window=128000,
)


class MockLLMAgent(BaseAgent[AgentContext]):
    """An agent that uses the full LLM completion path but with a mock client.

    This tests the complete flow: prepare_messages -> completion -> execute_tools
    """

    def __init__(
        self,
        mock_client: MockLLMClient,
        name: str = "mock_llm_agent",
        model: Model | None = None,
        **kwargs: Any,
    ):
        super().__init__(
            name=name,
            instructions="You are a test agent.",
            model=model or MOCK_MODEL,
            context_class=AgentContext,
            **kwargs,
        )
        self._mock_client = mock_client
        # Replace the client's completion method
        self.client = mock_client


class ScriptedAgent(BaseAgent[AgentContext]):
    """An agent with scripted responses for deterministic testing.

    Unlike SimpleTestAgent which just completes immediately, this one
    can be configured to do multiple turns, return pending tool calls, etc.
    """

    def __init__(
        self,
        name: str = "scripted_agent",
        turns_until_done: int = 1,
        pending_tool_ids: list[str] | None = None,
        pending_child_ids: list[str] | None = None,
        fail_on_turn: int | None = None,
        fail_exception: Exception | None = None,
        **kwargs: Any,
    ):
        super().__init__(
            name=name,
            instructions="Scripted test agent",
            context_class=AgentContext,
            **kwargs,
        )
        self.turns_until_done = turns_until_done
        self.pending_tool_ids = pending_tool_ids
        self.pending_child_ids = pending_child_ids
        self.fail_on_turn = fail_on_turn
        self.fail_exception = fail_exception or ValueError("Scripted failure")

    async def run_turn(
        self,
        agent_ctx: AgentContext,
    ) -> TurnCompletion[AgentContext]:
        agent_ctx.turn += 1

        # Fail on specific turn if configured
        if self.fail_on_turn is not None and agent_ctx.turn == self.fail_on_turn:
            raise self.fail_exception

        # Return pending tool calls on first turn if configured
        if agent_ctx.turn == 1 and self.pending_tool_ids:
            return TurnCompletion(
                is_done=False,
                context=agent_ctx,
                pending_tool_call_ids=self.pending_tool_ids,
            )

        # Return pending child tasks on first turn if configured
        if agent_ctx.turn == 1 and self.pending_child_ids:
            return TurnCompletion(
                is_done=False,
                context=agent_ctx,
                pending_child_task_ids=self.pending_child_ids,
            )

        # Check if we're done
        if agent_ctx.turn >= self.turns_until_done:
            return TurnCompletion(
                is_done=True,
                context=agent_ctx,
                output={"result": "completed", "turns": agent_ctx.turn},
            )

        # Continue
        return TurnCompletion(is_done=False, context=agent_ctx)


# =============================================================================
# Fixtures
# =============================================================================


@pytest_asyncio.fixture
async def redis_client() -> AsyncGenerator[redis.Redis, None]:
    """Create a fakeredis client for testing."""
    client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    yield client
    await client.flushall()
    await client.aclose()


@pytest.fixture
def test_namespace() -> str:
    """Unique namespace for test isolation."""
    return f"e2e_test:{uuid.uuid4().hex[:8]}"


@pytest.fixture
def test_owner_id() -> str:
    """Test owner ID."""
    return str(uuid.uuid4())


@pytest.fixture
def simple_mock_client() -> MockLLMClient:
    """A mock client that returns a simple completion."""
    return create_simple_mock("Test completed!")


@pytest.fixture
def multi_turn_mock_client() -> MockLLMClient:
    """A mock client that requires multiple LLM calls."""
    return MockLLMClient(
        responses=[
            MockResponse(content="Processing step 1..."),
            MockResponse(content="Processing step 2..."),
            MockResponse(content="All done!", is_final=True),
        ]
    )


@pytest.fixture
def tool_call_mock_client() -> MockLLMClient:
    """A mock client that makes a tool call."""
    return MockLLMClient(
        responses=[
            MockResponse(
                tool_calls=[
                    MockToolCall(name="search", arguments={"query": "test"})
                ]
            ),
            MockResponse(content="Found the results!", is_final=True),
        ]
    )


@pytest.fixture
def scripted_agent() -> ScriptedAgent:
    """A scripted agent for basic testing."""
    return ScriptedAgent(turns_until_done=1)


@pytest.fixture
def multi_turn_scripted_agent() -> ScriptedAgent:
    """A scripted agent that takes 3 turns."""
    return ScriptedAgent(turns_until_done=3)
