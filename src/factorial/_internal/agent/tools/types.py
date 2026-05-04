from dataclasses import dataclass
from typing import Any

from openai.types.chat import ChatCompletionMessageToolCall


@dataclass
class _ToolResultInternal:
    """Internal representation of a tool execution result."""

    tool_call: ChatCompletionMessageToolCall | None = None
    model_output: str = ""
    client_output: Any = None
    pending_result: bool = False
    pending_child_task_ids: list[str] | None = None


__all__ = ["_ToolResultInternal"]
