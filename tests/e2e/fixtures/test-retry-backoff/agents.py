from __future__ import annotations

from factorial import AgentContext, BaseAgent, ExecutionContext, TurnCompletion
from factorial.core.exceptions import RetryableError
from factorial.testing import mock_model


class _BackoffRecoveryAgent(BaseAgent[AgentContext]):
    def __init__(self) -> None:
        super().__init__(
            name="backoff_recovery_agent",
            instructions="Fail once at the queue boundary, then recover after backoff.",
            model=mock_model,
        )

    async def run_turn(
        self,
        agent_ctx: AgentContext,
    ) -> TurnCompletion[AgentContext]:
        agent_ctx.turn_number += 1
        if ExecutionContext.current().retry_count == 0:
            raise RetryableError("temporary upstream failure")
        agent_ctx.output = "recovered after backoff"
        return TurnCompletion(
            is_done=True,
            context=agent_ctx,
            output=agent_ctx.output,
        )


class _BackoffExhaustionAgent(BaseAgent[AgentContext]):
    def __init__(self) -> None:
        super().__init__(
            name="backoff_exhaustion_agent",
            instructions="Always fail at the queue boundary until retries exhaust.",
            model=mock_model,
        )

    async def run_turn(
        self,
        agent_ctx: AgentContext,
    ) -> TurnCompletion[AgentContext]:
        agent_ctx.turn_number += 1
        raise RetryableError("temporary upstream failure")


backoff_recovery_agent = _BackoffRecoveryAgent()
backoff_exhaustion_agent = _BackoffExhaustionAgent()
