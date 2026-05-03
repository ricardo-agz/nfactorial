from __future__ import annotations

import time
import uuid
from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Generic, Literal

from openai.types.chat import ChatCompletion, ChatCompletionMessage
from openai.types.chat.chat_completion import Choice
from openai.types.chat.chat_completion_message_custom_tool_call import (
    ChatCompletionMessageCustomToolCall,
)
from openai.types.chat.chat_completion_message_function_tool_call import (
    ChatCompletionMessageFunctionToolCall,
    Function as ToolCallFunction,
)
from openai.types.completion_usage import CompletionUsage
from typing_extensions import TypeVar

from factorial.agent import Agent
from factorial.ai.models import Model, MultiClient, Provider
from factorial.execution.context import ExecutionContext

StateT = TypeVar("StateT")
MetadataT = TypeVar("MetadataT")


mock_model = Model(
    name="mock",
    provider=Provider.OPENAI,
    provider_model_id="mock",
    context_window=128000,
)


@dataclass(frozen=True)
class MockToolCall:
    name: str
    arguments: dict[str, Any]
    id: str | None = None

    def to_openai(self) -> ChatCompletionMessageFunctionToolCall:
        import json

        return ChatCompletionMessageFunctionToolCall(
            id=self.id or f"call_{uuid.uuid4().hex[:8]}",
            type="function",
            function=ToolCallFunction(
                name=self.name,
                arguments=json.dumps(self.arguments),
            ),
        )


@dataclass(frozen=True)
class MockAssistantMessage:
    content: str | None = None
    tool_calls: tuple[MockToolCall, ...] = ()


MockResponseLike = str | MockToolCall | MockAssistantMessage | Exception


def tool_call(
    name: str,
    arguments: dict[str, Any] | None = None,
    /,
    **kwargs: Any,
) -> MockToolCall:
    """Create a mocked assistant tool call.

    Examples:
        tool_call("search", query="foo")
        tool_call("search", {"query": "foo"})
    """
    if arguments is not None and kwargs:
        raise ValueError(
            "tool_call(...) accepts either a positional arguments dict or keyword "
            "arguments, but not both."
        )
    return MockToolCall(name=name, arguments=dict(arguments or kwargs))


def assistant(*parts: str | MockToolCall) -> MockAssistantMessage:
    """Compose one mocked assistant completion from text and tool calls."""
    if not parts:
        raise ValueError("assistant(...) requires at least one text or tool-call part")

    text_parts: list[str] = []
    tool_calls: list[MockToolCall] = []
    for part in parts:
        if isinstance(part, str):
            text_parts.append(part)
        elif isinstance(part, MockToolCall):
            tool_calls.append(part)
        else:
            raise TypeError(
                "assistant(...) only accepts strings and tool_call(...) values"
            )

    content = "\n".join(part for part in text_parts if part) or None
    return MockAssistantMessage(content=content, tool_calls=tuple(tool_calls))


def _normalize_response(response: MockResponseLike) -> MockAssistantMessage:
    if isinstance(response, str):
        return MockAssistantMessage(content=response)
    if isinstance(response, MockToolCall):
        return MockAssistantMessage(tool_calls=(response,))
    if isinstance(response, MockAssistantMessage):
        return response
    if isinstance(response, Exception):
        raise response
    raise TypeError(f"Unsupported mock response type: {type(response).__name__}")


def _build_chat_completion(
    *,
    model_name: str,
    response: MockAssistantMessage,
) -> ChatCompletion:
    tool_calls_openai: list[
        ChatCompletionMessageFunctionToolCall | ChatCompletionMessageCustomToolCall
    ] | None = None
    if response.tool_calls:
        tool_calls_openai = [tool.to_openai() for tool in response.tool_calls]
    finish_reason: Literal["tool_calls", "stop"] = (
        "tool_calls" if tool_calls_openai else "stop"
    )
    return ChatCompletion(
        id=f"chatcmpl-{uuid.uuid4().hex[:12]}",
        model=model_name,
        created=int(time.time()),
        object="chat.completion",
        choices=[
            Choice(
                index=0,
                message=ChatCompletionMessage(
                    role="assistant",
                    content=response.content,
                    tool_calls=tool_calls_openai,
                ),
                finish_reason=finish_reason,
            )
        ],
        usage=CompletionUsage(
            prompt_tokens=1,
            completion_tokens=1,
            total_tokens=2,
        ),
    )


class ScriptedMockLLMClient(MultiClient):
    """Simple MultiClient-compatible scripted completion client for MockAgent."""

    def __init__(self, responses: Sequence[MockResponseLike]) -> None:
        if not responses:
            raise ValueError("MockAgent requires at least one scripted response")
        self._responses = list(responses)
        self._task_indices: dict[str, int] = defaultdict(int)
        self.call_history: list[dict[str, Any]] = []

    def _current_task_key(self) -> str:
        try:
            return ExecutionContext.current().task_id
        except Exception:
            return "__global__"

    async def completion(
        self,
        model: Model,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]] | None = None,
        tool_choice: str | dict[str, Any] | None = None,
        max_completion_tokens: int | None = None,
        temperature: float | None = None,
        stream: bool = False,
        parallel_tool_calls: bool | None = None,
        response_format: Any = None,
        **kwargs: Any,
    ) -> ChatCompletion:
        del kwargs, response_format
        model_name = model.name
        task_key = self._current_task_key()
        response_index = self._task_indices[task_key]

        self.call_history.append(
            {
                "task_key": task_key,
                "response_index": response_index,
                "model": model_name,
                "messages": messages,
                "tools": tools,
                "temperature": temperature,
                "max_completion_tokens": max_completion_tokens,
                "tool_choice": tool_choice,
                "parallel_tool_calls": parallel_tool_calls,
                "stream": stream,
            }
        )

        if response_index >= len(self._responses):
            raise AssertionError(
                f"Mock responses exhausted for task '{task_key}'. "
                f"Configured {len(self._responses)} scripted response(s), "
                f"but completion call index {response_index} was requested."
            )

        normalized = _normalize_response(self._responses[response_index])
        self._task_indices[task_key] += 1
        return _build_chat_completion(model_name=model_name, response=normalized)

    def reset(self) -> None:
        self._task_indices.clear()
        self.call_history.clear()

    @property
    def call_count(self) -> int:
        return len(self.call_history)


class MockAgent(Agent[StateT, MetadataT], Generic[StateT, MetadataT]):
    """Canonical Agent(...) wrapper backed by scripted mock completions."""

    def __init__(
        self,
        *,
        name: str,
        instructions: str,
        tools: Sequence[Any] | None = None,
        responses: Sequence[MockResponseLike],
        stop_when: Any = None,
        model: Model | None = None,
        **kwargs: Any,
    ) -> None:
        scripted_client = ScriptedMockLLMClient(responses=responses)
        super().__init__(
            name=name,
            instructions=instructions,
            tools=tools or [],
            client=scripted_client,
            model=model or mock_model,
            stop_when=stop_when,
            **kwargs,
        )
        self.mock_client = scripted_client


__all__ = [
    "MockAgent",
    "MockAssistantMessage",
    "MockResponseLike",
    "MockToolCall",
    "ScriptedMockLLMClient",
    "assistant",
    "mock_model",
    "tool_call",
]
