"""Mock LLM client and responses for testing.

This module provides a mock LLM that returns predictable, configurable responses
without making any actual API calls. Useful for:
- E2E tests that need to exercise the full agent path
- Testing tool execution flows
- Testing multi-turn conversations
- Testing error handling
"""

from __future__ import annotations

import json
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from openai.types.chat import ChatCompletion, ChatCompletionMessage
from openai.types.chat.chat_completion import Choice
from openai.types.chat.chat_completion_message_function_tool_call import (
    ChatCompletionMessageFunctionToolCall,
    Function as ToolCallFunction,
)
from openai.types.completion_usage import CompletionUsage

from factorial.context import AgentContext
from factorial.llms import Model


@dataclass
class MockToolCall:
    """A mock tool call to be returned by the mock LLM."""

    name: str
    arguments: dict[str, Any]
    id: str | None = None

    def to_openai(self) -> ChatCompletionMessageFunctionToolCall:
        return ChatCompletionMessageFunctionToolCall(
            id=self.id or f"call_{uuid.uuid4().hex[:8]}",
            type="function",
            function=ToolCallFunction(
                name=self.name,
                arguments=json.dumps(self.arguments),
            ),
        )


@dataclass
class MockResponse:
    """A configurable mock response for the LLM."""

    # Text content to return (mutually exclusive with tool_calls for "done" state)
    content: str | None = None

    # Tool calls to return
    tool_calls: list[MockToolCall] | None = None

    # If True, this response represents a "final" response (agent should complete)
    is_final: bool = False

    # Simulated latency in seconds
    latency: float = 0.0

    # If set, raise this exception instead of returning a response
    raise_exception: Exception | None = None

    def to_chat_completion(self, model_name: str = "mock-model") -> ChatCompletion:
        """Convert to OpenAI ChatCompletion format."""
        tool_calls_openai = None
        if self.tool_calls:
            tool_calls_openai = [tc.to_openai() for tc in self.tool_calls]

        finish_reason: str = "stop"
        if tool_calls_openai:
            finish_reason = "tool_calls"

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
                        content=self.content,
                        tool_calls=tool_calls_openai,
                    ),
                    finish_reason=finish_reason,
                )
            ],
            usage=CompletionUsage(
                prompt_tokens=100,
                completion_tokens=50,
                total_tokens=150,
            ),
        )


# Type alias for response generators
ResponseGenerator = Callable[[list[dict[str, Any]], AgentContext], MockResponse]


@dataclass
class MockLLMClient:
    """A mock LLM client that returns configurable responses.

    Usage:
        # Simple single response
        mock_client = MockLLMClient(
            responses=[MockResponse(content="Hello!")]
        )

        # Multi-turn with tool calls
        mock_client = MockLLMClient(responses=[
            MockResponse(
                tool_calls=[MockToolCall(name="search", arguments={"q": "test"})]
            ),
            MockResponse(content="Found results!"),
        ])

        # Dynamic responses based on context
        def dynamic_response(messages, ctx):
            if ctx.turn == 0:
                return MockResponse(content="First turn!")
            return MockResponse(content="Done!", is_final=True)

        mock_client = MockLLMClient(response_generator=dynamic_response)
    """

    # Static list of responses (consumed in order)
    responses: list[MockResponse] = field(default_factory=list)

    # Dynamic response generator (overrides responses list if set)
    response_generator: ResponseGenerator | None = None

    # Track calls for assertions
    call_history: list[dict[str, Any]] = field(default_factory=list)

    # Current response index (for static responses)
    _response_index: int = 0

    async def completion(
        self,
        model: Model | str,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]] | None = None,
        temperature: float | None = None,
        max_completion_tokens: int | None = None,
        tool_choice: str | dict[str, Any] | None = None,
        parallel_tool_calls: bool | None = None,
        stream: bool = False,
    ) -> ChatCompletion:
        """Mock completion that returns configured responses."""
        model_name = model.name if isinstance(model, Model) else model

        # Record the call
        self.call_history.append(
            {
                "model": model_name,
                "messages": messages,
                "tools": tools,
                "temperature": temperature,
                "tool_choice": tool_choice,
            }
        )

        # Get response
        response: MockResponse

        if self.response_generator:
            # Extract context from messages if possible (for dynamic responses)
            ctx = AgentContext(query="mock")  # Default context
            response = self.response_generator(messages, ctx)
        elif self._response_index < len(self.responses):
            response = self.responses[self._response_index]
            self._response_index += 1
        else:
            # Default: return simple completion
            response = MockResponse(content="Mock response", is_final=True)

        # Handle latency simulation
        if response.latency > 0:
            import asyncio

            await asyncio.sleep(response.latency)

        # Handle exception simulation
        if response.raise_exception:
            raise response.raise_exception

        return response.to_chat_completion(model_name)

    def reset(self) -> None:
        """Reset the mock state for reuse."""
        self._response_index = 0
        self.call_history.clear()

    @property
    def call_count(self) -> int:
        """Number of times completion was called."""
        return len(self.call_history)

    def assert_called(self, times: int | None = None) -> None:
        """Assert completion was called (optionally a specific number of times)."""
        if times is not None:
            assert self.call_count == times, (
                f"Expected {times} calls, got {self.call_count}"
            )
        else:
            assert self.call_count > 0, "Expected at least one call"

    def get_last_messages(self) -> list[dict[str, Any]]:
        """Get the messages from the last call."""
        if not self.call_history:
            return []
        return self.call_history[-1]["messages"]


# Convenience factory functions


def create_simple_mock(content: str = "Done!") -> MockLLMClient:
    """Create a mock that returns a single text response."""
    return MockLLMClient(responses=[MockResponse(content=content, is_final=True)])


def create_tool_mock(
    tool_name: str,
    tool_args: dict[str, Any],
    final_content: str = "Completed!",
) -> MockLLMClient:
    """Create a mock that calls a tool then completes."""
    return MockLLMClient(
        responses=[
            MockResponse(
                tool_calls=[MockToolCall(name=tool_name, arguments=tool_args)]
            ),
            MockResponse(content=final_content, is_final=True),
        ]
    )


def create_multi_turn_mock(
    num_turns: int, final_content: str = "Done!"
) -> MockLLMClient:
    """Create a mock that takes N turns before completing."""
    responses = [
        MockResponse(content=f"Turn {i+1} response")
        for i in range(num_turns - 1)
    ]
    responses.append(MockResponse(content=final_content, is_final=True))
    return MockLLMClient(responses=responses)


def create_final_output_tool_mock(output: dict[str, Any]) -> MockLLMClient:
    """Create a mock that uses the final_output tool (for agents with output_type)."""
    return MockLLMClient(
        responses=[
            MockResponse(
                tool_calls=[MockToolCall(name="final_output", arguments=output)],
                is_final=True,
            )
        ]
    )


def create_failing_mock(
    exception: Exception,
    fail_on_call: int = 1,
    success_content: str = "Recovered!",
) -> MockLLMClient:
    """Create a mock that fails on a specific call then succeeds."""
    responses: list[MockResponse] = []
    for i in range(1, fail_on_call):
        responses.append(MockResponse(content=f"Response {i}"))
    responses.append(MockResponse(raise_exception=exception))
    responses.append(MockResponse(content=success_content, is_final=True))
    return MockLLMClient(responses=responses)
