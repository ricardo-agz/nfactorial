from dataclasses import dataclass
from typing import Any

from openai.types.chat import ChatCompletionMessageToolCall

from factorial._internal.agent.tools.types import _ToolResultInternal
from factorial.ai.messages import Message


@dataclass
class ToolExecutionResults:
    new_messages: list[Message]
    tool_call_results: list[tuple[ChatCompletionMessageToolCall, Any | BaseException]]
    resolved_results: list[
        tuple[ChatCompletionMessageToolCall, _ToolResultInternal | BaseException]
    ]
    pending_tool_call_ids: list[str]
    pending_child_task_ids: list[str]


__all__ = ["ToolExecutionResults"]
