"""Contracts for prepare_turn injection behavior."""

from __future__ import annotations

import pytest

from factorial import AgentContext, ExecutionContext
from factorial.ai.models import Model, Provider
from factorial.agent.base import Turn, _maybe_call_prepare_turn

MOCK_MODEL = Model(
    name="mock-model",
    provider=Provider.OPENAI,
    provider_model_id="mock-v1",
    context_window=128000,
)


def _make_turn() -> Turn[AgentContext]:
    return Turn(model=MOCK_MODEL, messages=[], tools=[])


def _make_execution_context() -> ExecutionContext:
    return ExecutionContext(task_id="test-task", owner_id="test-owner")


@pytest.mark.asyncio
async def test_prepare_turn_supports_agent_context_without_execution_context() -> None:
    seen: dict[str, object] = {}
    turn = _make_turn()
    agent_ctx = AgentContext(messages=[{"role": "user", "content": "hello"}])
    execution_ctx = _make_execution_context()

    def prepare_turn(
        turn: Turn[AgentContext],
        agent_ctx: AgentContext,
    ) -> None:
        seen["turn"] = turn
        seen["turn_number"] = agent_ctx.turn_number
        turn.temperature = 0.1

    returned = await _maybe_call_prepare_turn(
        prepare_turn,
        turn,
        agent_ctx,
        execution_ctx,
    )

    assert returned is turn
    assert seen["turn"] is turn
    assert seen["turn_number"] == agent_ctx.turn_number
    assert turn.temperature == 0.1


@pytest.mark.asyncio
async def test_prepare_turn_injects_execution_context_when_declared() -> None:
    seen: dict[str, object] = {}
    turn = _make_turn()
    agent_ctx = AgentContext(messages=[])
    execution_ctx = _make_execution_context()

    def prepare_turn(
        turn: Turn[AgentContext],
        execution_ctx: ExecutionContext,
    ) -> None:
        seen["task_id"] = execution_ctx.task_id
        turn.parallel_tool_calls = False

    returned = await _maybe_call_prepare_turn(
        prepare_turn,
        turn,
        agent_ctx,
        execution_ctx,
    )

    assert returned is turn
    assert seen["task_id"] == "test-task"
    assert turn.parallel_tool_calls is False


@pytest.mark.asyncio
async def test_prepare_turn_rejects_unknown_required_parameter() -> None:
    turn = _make_turn()
    agent_ctx = AgentContext(messages=[])
    execution_ctx = _make_execution_context()

    def prepare_turn(turn: Turn[AgentContext], unsupported: object) -> None:
        del turn, unsupported

    with pytest.raises(
        TypeError,
        match="Unsupported required prepare_turn parameter 'unsupported'",
    ):
        await _maybe_call_prepare_turn(
            prepare_turn,
            turn,
            agent_ctx,
            execution_ctx,
        )
