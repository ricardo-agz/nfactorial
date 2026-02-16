"""Phase 0 regression tests for tool/forking behavior.

These tests lock current behavior before API redesign runtime changes begin.
"""

from __future__ import annotations

import json
import uuid
from collections.abc import Callable
from contextvars import Token
from typing import Any, cast

import httpx
import pytest
from openai.types.chat.chat_completion_message_function_tool_call import (
    ChatCompletionMessageFunctionToolCall,
    Function as ToolCallFunction,
)

from factorial import (
    Agent,
    AgentContext,
    ExecutionContext,
    function_tool,
)
from factorial.context import execution_context
from factorial.events import EventPublisher
from factorial.tools import FunctionTool, convert_tools_list, forking_tool


def _make_tool_call(
    tool_name: str, arguments: dict[str, object] | None = None
) -> ChatCompletionMessageFunctionToolCall:
    return ChatCompletionMessageFunctionToolCall(
        id=f"call_{uuid.uuid4().hex[:8]}",
        type="function",
        function=ToolCallFunction(
            name=tool_name, arguments=json.dumps(arguments or {})
        ),
    )


class _NoopEvents:
    async def publish_event(self, _event: object) -> None:
        return None


def _set_test_execution_context() -> Token[ExecutionContext]:
    ctx = ExecutionContext(
        task_id=str(uuid.uuid4()),
        owner_id="test-owner",
        retries=0,
        iterations=0,
        events=cast(EventPublisher, _NoopEvents()),
    )
    return execution_context.set(ctx)


def _make_agent_with_tools(
    tools: list[FunctionTool[AgentContext] | Callable[..., Any]]
) -> Agent:
    # Disable cert verification in tests to avoid platform cert store coupling.
    http_client = httpx.AsyncClient(verify=False, trust_env=False)
    return Agent(tools=tools, http_client=http_client)


class TestFunctionToolRegression:
    def test_function_tool_uses_docstring_and_schema(self) -> None:
        def greet(name: str) -> str:
            """Return a friendly greeting."""
            return f"hi {name}"

        tool = function_tool(greet)

        assert isinstance(tool, FunctionTool)
        assert tool.name == "greet"
        assert tool.description == "Return a friendly greeting."
        assert tool.strict_json_schema is True
        assert tool.params_json_schema == {
            "type": "object",
            "properties": {"name": {"type": "string"}},
            "additionalProperties": False,
            "required": ["name"],
        }

    def test_optional_parameter_disables_strict_json_schema(self) -> None:
        def search(query: str, limit: int | None = None) -> str:
            return f"{query}:{limit}"

        tool = function_tool(search)

        assert tool.strict_json_schema is False
        assert "query" in tool.params_json_schema["required"]
        assert "limit" not in tool.params_json_schema["required"]

    def test_context_parameters_are_excluded_from_schema(self) -> None:
        def contextual(
            query: str,
            agent_ctx: AgentContext,
            execution_ctx: ExecutionContext,
        ) -> str:
            return f"{query}:{agent_ctx.query}:{execution_ctx.task_id}"

        tool = function_tool(contextual)

        assert set(tool.params_json_schema["properties"]) == {"query"}

    def test_convert_tools_list_accepts_mixed_inputs(self) -> None:
        @function_tool
        def first_tool(value: int) -> str:
            return str(value)

        def second_tool(topic: str) -> str:
            return topic.upper()

        schemas, actions = convert_tools_list([first_tool, second_tool])
        names = [schema.name for schema in schemas]

        assert names == ["first_tool", "second_tool"]
        assert set(actions) == {"first_tool", "second_tool"}


@pytest.mark.asyncio
class TestForkingRegression:
    async def test_forking_tool_wraps_sync_function(self) -> None:
        @forking_tool(timeout=20.0)
        def spawn() -> list[str]:
            return [str(uuid.uuid4())]

        result = await spawn()

        assert len(result) == 1
        assert hasattr(spawn, "forking_tool")
        assert hasattr(spawn, "timeout")
        assert spawn.forking_tool is True
        assert spawn.timeout == 20.0

    async def test_tool_action_collects_child_ids_from_forking_list(self) -> None:
        child_ids = [str(uuid.uuid4()), str(uuid.uuid4())]

        @forking_tool(timeout=600.0)
        def spawn_children() -> list[str]:
            return child_ids

        agent = _make_agent_with_tools([spawn_children])
        token = _set_test_execution_context()
        try:
            tool_call = _make_tool_call("spawn_children")
            result = await agent.tool_action(tool_call, AgentContext(query="q"))

            assert result.pending_child_task_ids == child_ids
        finally:
            execution_context.reset(token)
            await agent.http_client.aclose()

    async def test_tool_action_collects_child_ids_from_tuple_shape(self) -> None:
        child_ids = [str(uuid.uuid4())]

        @forking_tool(timeout=600.0)
        def spawn_children_with_message() -> tuple[str, list[str]]:
            return "spawned", child_ids

        agent = _make_agent_with_tools([spawn_children_with_message])
        token = _set_test_execution_context()
        try:
            tool_call = _make_tool_call("spawn_children_with_message")
            result = await agent.tool_action(tool_call, AgentContext(query="q"))

            assert result.output_str == "spawned"
            assert result.output_data == child_ids
            assert result.pending_child_task_ids == child_ids
        finally:
            execution_context.reset(token)
            await agent.http_client.aclose()

    async def test_tool_action_rejects_invalid_forking_ids(self) -> None:
        @forking_tool(timeout=600.0)
        def spawn_children_invalid() -> list[str]:
            return ["not-a-task-id"]

        agent = _make_agent_with_tools([spawn_children_invalid])
        token = _set_test_execution_context()
        try:
            tool_call = _make_tool_call("spawn_children_invalid")
            with pytest.raises(ValueError, match="invalid task IDs"):
                await agent.tool_action(tool_call, AgentContext(query="q"))
        finally:
            execution_context.reset(token)
            await agent.http_client.aclose()
