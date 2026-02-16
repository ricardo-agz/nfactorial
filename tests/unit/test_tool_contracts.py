"""Contracts for tool decorators, schemas, and forking behavior."""

from __future__ import annotations

import json
import threading
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
    FunctionTool,
    ToolResult,
    function_tool,
    tool,
)
from factorial.context import execution_context
from factorial.events import EventPublisher
from factorial.tools import convert_tools_list, forking_tool


def _make_tool_call(
    tool_name: str,
    arguments: dict[str, object] | None = None,
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
    tools: list[FunctionTool[AgentContext] | Callable[..., Any]],
) -> Agent:
    # Disable cert verification in tests to avoid platform cert store coupling.
    http_client = httpx.AsyncClient(verify=False, trust_env=False)
    return Agent(tools=tools, http_client=http_client)


def test_legacy_function_tool_uses_docstring_and_schema() -> None:
    def greet(name: str) -> str:
        """Return a friendly greeting."""
        return f"hi {name}"

    tool_def = function_tool(greet)

    assert isinstance(tool_def, FunctionTool)
    assert tool_def.name == "greet"
    assert tool_def.description == "Return a friendly greeting."
    assert tool_def.strict_json_schema is True
    assert tool_def.params_json_schema == {
        "type": "object",
        "properties": {"name": {"type": "string"}},
        "additionalProperties": False,
        "required": ["name"],
    }


def test_optional_params_disable_strict_schema() -> None:
    def search(query: str, limit: int | None = None) -> str:
        return f"{query}:{limit}"

    tool_def = function_tool(search)
    assert tool_def.strict_json_schema is False
    assert "query" in tool_def.params_json_schema["required"]
    assert "limit" not in tool_def.params_json_schema["required"]


def test_tool_schema_excludes_injected_context_params() -> None:
    def contextual(
        query: str,
        agent_ctx: AgentContext,
        execution_ctx: ExecutionContext,
    ) -> str:
        return f"{query}:{agent_ctx.query}:{execution_ctx.task_id}"

    tool_def = function_tool(contextual)
    assert set(tool_def.params_json_schema["properties"]) == {"query"}


def test_convert_tools_list_accepts_tool_defs_and_callables() -> None:
    @function_tool
    def first_tool(value: int) -> str:
        return str(value)

    def second_tool(topic: str) -> str:
        return topic.upper()

    schemas, actions = convert_tools_list([first_tool, second_tool])
    assert [schema.name for schema in schemas] == ["first_tool", "second_tool"]
    assert set(actions) == {"first_tool", "second_tool"}


def test_tool_namespace_decorator_defaults_name_and_description() -> None:
    @tool
    def greet(name: str) -> str:
        return f"hi {name}"

    assert isinstance(greet, FunctionTool)
    assert greet.name == "greet"
    assert greet.description == "Execute greet"


def test_tool_namespace_decorator_accepts_explicit_name_and_description() -> None:
    @tool(name="lookup", description="Lookup data")
    def query(term: str) -> str:
        return term

    assert isinstance(query, FunctionTool)
    assert query.name == "lookup"
    assert query.description == "Lookup data"


def test_tool_namespace_result_builders_set_status_and_payload() -> None:
    ok_result = tool.ok("completed", {"id": 1})
    fail_result = tool.fail("needs action", {"code": "REVIEW"})
    error_result = tool.error("unexpected", {"code": "E_GENERIC"})

    assert isinstance(ok_result, ToolResult)
    assert ok_result.status == "ok"
    assert ok_result.output_str == "completed"
    assert ok_result.output_data == {"id": 1}

    assert fail_result.status == "fail"
    assert fail_result.output_str == "needs action"
    assert fail_result.output_data == {"code": "REVIEW"}

    assert error_result.status == "error"
    assert error_result.output_str == "unexpected"
    assert error_result.output_data == {"code": "E_GENERIC"}


@pytest.mark.asyncio
async def test_forking_tool_wraps_sync_callable_with_timeout_metadata() -> None:
    @forking_tool(timeout=20.0)
    def spawn() -> list[str]:
        return [str(uuid.uuid4())]

    result = await spawn()
    assert len(result) == 1
    assert hasattr(spawn, "forking_tool")
    assert hasattr(spawn, "timeout")
    assert spawn.forking_tool is True
    assert spawn.timeout == 20.0


@pytest.mark.asyncio
async def test_tool_action_extracts_child_ids_from_forking_list_return() -> None:
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


@pytest.mark.asyncio
async def test_tool_action_offloads_sync_callbacks_to_worker_thread() -> None:
    callback_thread_ids: list[int] = []
    loop_thread_id = threading.get_ident()

    def sync_search(query: str) -> str:
        callback_thread_ids.append(threading.get_ident())
        return f"result:{query}"

    agent = _make_agent_with_tools([sync_search])
    token = _set_test_execution_context()
    try:
        tool_call = _make_tool_call("sync_search", {"query": "weather"})
        result = await agent.tool_action(tool_call, AgentContext(query="q"))
        assert result.output_str == "result:weather"
        assert callback_thread_ids
        assert callback_thread_ids[0] != loop_thread_id
    finally:
        execution_context.reset(token)
        await agent.http_client.aclose()


@pytest.mark.asyncio
async def test_tool_action_extracts_child_ids_from_forking_tuple_return() -> None:
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


@pytest.mark.asyncio
async def test_tool_action_rejects_invalid_forking_child_ids() -> None:
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

