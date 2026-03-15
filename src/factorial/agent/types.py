from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any, Generic

from openai.types.chat import ChatCompletionMessageToolCall

from factorial.agent.context import ContextType
from factorial.agent.tools.core import ToolDefinition, _ToolResultInternal
from factorial.ai.messages import Message
from factorial.ai.models import Model
from factorial.core.run_types import TurnSummary, UsageSummary, VerificationSummary

ToolChoice = str | dict[str, Any] | None
EventCallback = Callable[..., Awaitable[None] | None]
PrepareTurnHook = Callable[..., Any]


@dataclass
class Turn(Generic[ContextType]):
    model: Model
    messages: list[Message]
    tools: list[ToolDefinition[ContextType]]
    tool_choice: ToolChoice = "auto"
    parallel_tool_calls: bool | None = None
    temperature: float | None = None
    max_output_tokens: int | None = None


@dataclass
class TurnCompletion(Generic[ContextType]):
    is_done: bool
    context: ContextType
    output: Any = None
    tool_call_results: list[tuple[ChatCompletionMessageToolCall, Any | Exception]] = (
        field(default_factory=list)
    )
    pending_tool_call_ids: list[str] = field(default_factory=list)
    pending_child_task_ids: list[str] = field(default_factory=list)
    finish_reason: str = "continue"
    usage: UsageSummary = field(default_factory=UsageSummary.zero)
    turn_summary: TurnSummary | None = None
    verification_summary: VerificationSummary[Any] | None = None


@dataclass
class ToolExecutionResults:
    new_messages: list[Message]
    tool_call_results: list[tuple[ChatCompletionMessageToolCall, Any | Exception]]
    resolved_results: list[
        tuple[ChatCompletionMessageToolCall, _ToolResultInternal | Exception]
    ]
    pending_tool_call_ids: list[str]
    pending_child_task_ids: list[str]


@dataclass
class Callbacks:
    on_start: EventCallback | None = None
    on_turn_start: EventCallback | None = None
    on_model_start: EventCallback | None = None
    on_model_finish: EventCallback | None = None
    on_tool_start: EventCallback | None = None
    on_tool_finish: EventCallback | None = None
    on_wait: EventCallback | None = None
    on_turn_finish: EventCallback | None = None
    on_finish: EventCallback | None = None


__all__ = [
    "Callbacks",
    "EventCallback",
    "PrepareTurnHook",
    "ToolChoice",
    "ToolExecutionResults",
    "Turn",
    "TurnCompletion",
]
