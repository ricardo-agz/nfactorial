"""Contracts for tool decorators, schemas, and forking behavior."""

from __future__ import annotations

import json
import threading
import uuid
from collections.abc import Callable
from contextvars import Token
from typing import Annotated, Any, cast

import httpx
import pytest
from openai.types.chat.chat_completion_message_function_tool_call import (
    ChatCompletionMessageFunctionToolCall,
    Function as ToolCallFunction,
)
from pydantic import BaseModel

from factorial import (
    Agent,
    AgentContext,
    ExecutionContext,
    Hidden,
    ToolDefinition,
    tool,
)
from factorial.context import execution_context
from factorial.events import EventPublisher
from factorial.tools import (
    _ToolResultInternal,
    convert_tools_list,
    forking_tool,
    has_hidden_annotation,
    serialize_for_client,
    serialize_for_model,
)
from factorial.waits import WaitInstruction, wait


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
    tools: list[ToolDefinition[AgentContext] | Callable[..., Any]],
) -> Agent:
    http_client = httpx.AsyncClient(verify=False, trust_env=False)
    return Agent(tools=tools, http_client=http_client)


# ---------------------------------------------------------------------------
# Hidden sentinel and serialization
# ---------------------------------------------------------------------------


class _SampleModel(BaseModel):
    summary: str
    secret: Annotated[str, Hidden]
    count: Annotated[int, Hidden]


class _PlainModel(BaseModel):
    name: str
    value: int


def test_hidden_annotation_detected() -> None:
    from typing import get_type_hints

    hints = get_type_hints(_SampleModel, include_extras=True)
    assert has_hidden_annotation(hints["secret"])
    assert has_hidden_annotation(hints["count"])
    assert not has_hidden_annotation(hints["summary"])


def test_serialize_for_model_excludes_hidden() -> None:
    obj = _SampleModel(summary="done", secret="s3cr3t", count=42)
    result = serialize_for_model(obj)
    assert result == {"summary": "done"}
    assert "secret" not in result
    assert "count" not in result


def test_serialize_for_client_includes_all() -> None:
    obj = _SampleModel(summary="done", secret="s3cr3t", count=42)
    result = serialize_for_client(obj)
    assert result == {"summary": "done", "secret": "s3cr3t", "count": 42}


def test_serialize_plain_model_same_for_model_and_client() -> None:
    obj = _PlainModel(name="test", value=99)
    assert serialize_for_model(obj) == {"name": "test", "value": 99}
    assert serialize_for_client(obj) == {"name": "test", "value": 99}


# ---------------------------------------------------------------------------
# Tool decorator
# ---------------------------------------------------------------------------


def test_tool_decorator_defaults_name_and_description() -> None:
    @tool
    def greet(name: str) -> str:
        return f"hi {name}"

    assert isinstance(greet, ToolDefinition)
    assert greet.name == "greet"
    assert greet.description == "Execute greet"


def test_tool_decorator_uses_docstring() -> None:
    @tool
    def greet(name: str) -> str:
        """Return a friendly greeting."""
        return f"hi {name}"

    assert isinstance(greet, ToolDefinition)
    assert greet.name == "greet"
    assert greet.description == "Return a friendly greeting."
    assert greet.strict_json_schema is True
    assert greet.params_json_schema == {
        "type": "object",
        "properties": {"name": {"type": "string"}},
        "additionalProperties": False,
        "required": ["name"],
    }


def test_tool_decorator_accepts_explicit_name_and_description() -> None:
    @tool(name="lookup", description="Lookup data")
    def query(term: str) -> str:
        return term

    assert isinstance(query, ToolDefinition)
    assert query.name == "lookup"
    assert query.description == "Lookup data"


def test_optional_params_disable_strict_schema() -> None:
    @tool
    def search(query: str, limit: int | None = None) -> str:
        return f"{query}:{limit}"

    assert search.strict_json_schema is False
    assert "query" in search.params_json_schema["required"]
    assert "limit" not in search.params_json_schema["required"]


def test_tool_schema_excludes_injected_context_params() -> None:
    @tool
    def contextual(
        query: str,
        agent_ctx: AgentContext,
        execution_ctx: ExecutionContext,
    ) -> str:
        return f"{query}:{agent_ctx.query}:{execution_ctx.task_id}"

    assert set(contextual.params_json_schema["properties"]) == {"query"}


def test_convert_tools_list_accepts_tool_defs_and_callables() -> None:
    @tool
    def first_tool(value: int) -> str:
        return str(value)

    def second_tool(topic: str) -> str:
        return topic.upper()

    schemas, actions = convert_tools_list([first_tool, second_tool])
    assert [schema.name for schema in schemas] == ["first_tool", "second_tool"]
    assert set(actions) == {"first_tool", "second_tool"}


# ---------------------------------------------------------------------------
# tool.ok/fail/error are REMOVED -- no test for them
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# tool_action return normalization
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_tool_action_plain_string_return() -> None:
    def echo(text: str) -> str:
        return f"echo: {text}"

    agent = _make_agent_with_tools([echo])
    token = _set_test_execution_context()
    try:
        tool_call = _make_tool_call("echo", {"text": "hello"})
        result = await agent.tool_action(tool_call, AgentContext(query="q"))
        assert isinstance(result, _ToolResultInternal)
        assert result.model_output == "echo: hello"
        assert result.client_output == "echo: hello"
    finally:
        execution_context.reset(token)
        await agent.http_client.aclose()


@pytest.mark.asyncio
async def test_tool_action_dict_return() -> None:
    def get_data() -> dict[str, Any]:
        return {"key": "value", "count": 42}

    agent = _make_agent_with_tools([get_data])
    token = _set_test_execution_context()
    try:
        tool_call = _make_tool_call("get_data")
        result = await agent.tool_action(tool_call, AgentContext(query="q"))
        assert isinstance(result, _ToolResultInternal)
        assert result.client_output == {"key": "value", "count": 42}
        assert "key" in result.model_output
        assert "value" in result.model_output
    finally:
        execution_context.reset(token)
        await agent.http_client.aclose()


@pytest.mark.asyncio
async def test_tool_action_none_return() -> None:
    def noop() -> None:
        pass

    agent = _make_agent_with_tools([noop])
    token = _set_test_execution_context()
    try:
        tool_call = _make_tool_call("noop")
        result = await agent.tool_action(tool_call, AgentContext(query="q"))
        assert isinstance(result, _ToolResultInternal)
        assert result.model_output == ""
        assert result.client_output is None
    finally:
        execution_context.reset(token)
        await agent.http_client.aclose()


class _EditResult(BaseModel):
    summary: str
    new_code: Annotated[str, Hidden]


class _InfoModel(BaseModel):
    name: str
    value: int


@pytest.mark.asyncio
async def test_tool_action_basemodel_with_hidden() -> None:
    def edit() -> _EditResult:
        return _EditResult(summary="Edited file", new_code="print('hello')")

    agent = _make_agent_with_tools([edit])
    token = _set_test_execution_context()
    try:
        tool_call = _make_tool_call("edit")
        result = await agent.tool_action(tool_call, AgentContext(query="q"))
        assert isinstance(result, _ToolResultInternal)
        # Model sees only non-hidden fields
        assert "summary" in result.model_output
        assert "Edited file" in result.model_output
        assert "print('hello')" not in result.model_output
        # Client sees all fields
        assert result.client_output == {
            "summary": "Edited file",
            "new_code": "print('hello')",
        }
    finally:
        execution_context.reset(token)
        await agent.http_client.aclose()


@pytest.mark.asyncio
async def test_tool_action_basemodel_without_hidden() -> None:
    def get_info() -> _InfoModel:
        return _InfoModel(name="test", value=99)

    agent = _make_agent_with_tools([get_info])
    token = _set_test_execution_context()
    try:
        tool_call = _make_tool_call("get_info")
        result = await agent.tool_action(tool_call, AgentContext(query="q"))
        assert isinstance(result, _ToolResultInternal)
        # Both model and client see all fields
        assert "name" in result.model_output
        assert "test" in result.model_output
        assert result.client_output == {"name": "test", "value": 99}
    finally:
        execution_context.reset(token)
        await agent.http_client.aclose()


@pytest.mark.asyncio
async def test_tool_action_wait_instruction_with_data() -> None:
    def wait_tool() -> WaitInstruction:
        return wait.sleep(30, data="Cooling down")

    agent = _make_agent_with_tools([wait_tool])
    token = _set_test_execution_context()
    try:
        tool_call = _make_tool_call("wait_tool")
        result = await agent.tool_action(tool_call, AgentContext(query="q"))
        assert isinstance(result, _ToolResultInternal)
        assert result.model_output == "Cooling down"
        assert isinstance(result.client_output, WaitInstruction)
        assert result.client_output.data == "Cooling down"
    finally:
        execution_context.reset(token)
        await agent.http_client.aclose()


@pytest.mark.asyncio
async def test_tool_action_wait_instruction_default_message() -> None:
    def wait_tool() -> WaitInstruction:
        return wait.sleep(60)

    agent = _make_agent_with_tools([wait_tool])
    token = _set_test_execution_context()
    try:
        tool_call = _make_tool_call("wait_tool")
        result = await agent.tool_action(tool_call, AgentContext(query="q"))
        assert isinstance(result, _ToolResultInternal)
        assert "60" in result.model_output
        assert isinstance(result.client_output, WaitInstruction)
    finally:
        execution_context.reset(token)
        await agent.http_client.aclose()


# ---------------------------------------------------------------------------
# Forking tools
# ---------------------------------------------------------------------------


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
        assert result.model_output == "result:weather"
        assert callback_thread_ids
        assert callback_thread_ids[0] != loop_thread_id
    finally:
        execution_context.reset(token)
        await agent.http_client.aclose()


@pytest.mark.asyncio
async def test_tool_action_rejects_legacy_forking_tuple_return() -> None:
    child_ids = [str(uuid.uuid4())]

    @forking_tool(timeout=600.0)
    def spawn_children_with_message() -> tuple[str, list[str]]:
        return "spawned", child_ids

    agent = _make_agent_with_tools([spawn_children_with_message])
    token = _set_test_execution_context()
    try:
        tool_call = _make_tool_call("spawn_children_with_message")
        with pytest.raises(ValueError, match="must return list\\[str\\]"):
            await agent.tool_action(tool_call, AgentContext(query="q"))
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


# ---------------------------------------------------------------------------
# WaitInstruction data= parameter
# ---------------------------------------------------------------------------


def test_wait_sleep_data_string() -> None:
    instr = wait.sleep(10, data="pausing")
    assert instr.data == "pausing"
    assert instr.sleep_s == 10


def test_wait_sleep_data_dict() -> None:
    instr = wait.sleep(10, data={"reason": "cooldown"})
    assert instr.data == {"reason": "cooldown"}


def test_wait_cron_data() -> None:
    instr = wait.cron("*/5 * * * *", data="next tick")
    assert instr.data == "next tick"
    assert instr.cron == "*/5 * * * *"


def test_wait_sleep_no_data_defaults_none() -> None:
    instr = wait.sleep(5)
    assert instr.data is None
