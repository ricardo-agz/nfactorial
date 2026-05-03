from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, TypeAlias

from factorial.agent.context import AgentContext
from factorial.execution.context import ExecutionContext

StopWhen: TypeAlias = Callable[[AgentContext[Any, Any], ExecutionContext], bool]


class StopCondition:
    def __call__(
        self,
        agent_ctx: AgentContext[Any, Any],
        execution_ctx: ExecutionContext,
    ) -> bool:
        raise NotImplementedError


@dataclass(frozen=True)
class NoToolCallsCondition(StopCondition):
    def __call__(
        self,
        agent_ctx: AgentContext[Any, Any],
        execution_ctx: ExecutionContext,
    ) -> bool:
        del agent_ctx
        last_turn = execution_ctx.last_turn
        if last_turn is None:
            return False
        return not last_turn.finish_reason.startswith("tool_called:")


@dataclass(frozen=True)
class TurnCountIsCondition(StopCondition):
    limit: int

    def __call__(
        self,
        agent_ctx: AgentContext[Any, Any],
        execution_ctx: ExecutionContext,
    ) -> bool:
        del execution_ctx
        return agent_ctx.turn_number >= self.limit


@dataclass(frozen=True)
class ToolCalledCondition(StopCondition):
    name: str

    def __call__(
        self,
        agent_ctx: AgentContext[Any, Any],
        execution_ctx: ExecutionContext,
    ) -> bool:
        del agent_ctx
        last_turn = execution_ctx.last_turn
        if last_turn is None or not last_turn.finish_reason.startswith("tool_called:"):
            return False
        suffix = last_turn.finish_reason.removeprefix("tool_called:")
        return self.name in {value for value in suffix.split(",") if value}


@dataclass(frozen=True)
class TotalTokensExceedCondition(StopCondition):
    limit: int

    def __call__(
        self,
        agent_ctx: AgentContext[Any, Any],
        execution_ctx: ExecutionContext,
    ) -> bool:
        del agent_ctx
        return execution_ctx.usage.total_tokens > self.limit


@dataclass(frozen=True)
class AnyOfCondition(StopCondition):
    conditions: tuple[StopWhen | StopCondition, ...]

    def __call__(
        self,
        agent_ctx: AgentContext[Any, Any],
        execution_ctx: ExecutionContext,
    ) -> bool:
        return any(
            condition(agent_ctx, execution_ctx) for condition in self.conditions
        )


@dataclass(frozen=True)
class AllOfCondition(StopCondition):
    conditions: tuple[StopWhen | StopCondition, ...]

    def __call__(
        self,
        agent_ctx: AgentContext[Any, Any],
        execution_ctx: ExecutionContext,
    ) -> bool:
        return all(
            condition(agent_ctx, execution_ctx) for condition in self.conditions
        )


class stop:
    @staticmethod
    def no_tool_calls() -> StopCondition:
        return NoToolCallsCondition()

    @staticmethod
    def turn_count_is(limit: int) -> StopCondition:
        return TurnCountIsCondition(limit=limit)

    @staticmethod
    def tool_called(name: str) -> StopCondition:
        return ToolCalledCondition(name=name)

    @staticmethod
    def total_tokens_exceed(limit: int) -> StopCondition:
        return TotalTokensExceedCondition(limit=limit)

    @staticmethod
    def any_of(*conditions: StopWhen | StopCondition) -> StopCondition:
        return AnyOfCondition(conditions=conditions)

    @staticmethod
    def all_of(*conditions: StopWhen | StopCondition) -> StopCondition:
        return AllOfCondition(conditions=conditions)


def no_tool_calls() -> StopCondition:
    return stop.no_tool_calls()


def turn_count_is(limit: int) -> StopCondition:
    return stop.turn_count_is(limit)


def tool_called(name: str) -> StopCondition:
    return stop.tool_called(name)


def total_tokens_exceed(limit: int) -> StopCondition:
    return stop.total_tokens_exceed(limit)


def any_of(*conditions: StopWhen | StopCondition) -> StopCondition:
    return stop.any_of(*conditions)


def all_of(*conditions: StopWhen | StopCondition) -> StopCondition:
    return stop.all_of(*conditions)


def _infer_turn_limit_hint(
    condition: StopWhen | StopCondition | None,
) -> int | None:
    if condition is None:
        return None
    if isinstance(condition, TurnCountIsCondition):
        return condition.limit
    if isinstance(condition, AnyOfCondition):
        limits = [
            limit
            for limit in (
                _infer_turn_limit_hint(child) for child in condition.conditions
            )
            if limit is not None
        ]
        return min(limits) if limits else None
    if isinstance(condition, AllOfCondition):
        limits = [
            limit
            for limit in (
                _infer_turn_limit_hint(child) for child in condition.conditions
            )
            if limit is not None
        ]
        return max(limits) if limits else None
    return None


__all__ = [
    "AllOfCondition",
    "AnyOfCondition",
    "NoToolCallsCondition",
    "StopCondition",
    "StopWhen",
    "ToolCalledCondition",
    "TotalTokensExceedCondition",
    "TurnCountIsCondition",
    "all_of",
    "any_of",
    "no_tool_calls",
    "stop",
    "tool_called",
    "total_tokens_exceed",
    "turn_count_is",
]
