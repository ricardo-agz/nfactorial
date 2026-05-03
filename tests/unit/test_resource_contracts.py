from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from typing import Any

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
    ResourceCheckpoint,
    ResourceContext,
    ResourceRequest,
    Resources,
    Sandboxes,
    resource,
    tool,
    verify,
)
from factorial.agent.tools.runtime import tool_action as run_tool_action
from factorial.ai.models import Model, Provider
from factorial.core.events import EventPublisher
from factorial.execution.context import execution_context
from factorial.resources import (
    InMemoryResourceBindingStore,
    ResourceManager,
    ResourcesExecutionNamespace,
)
from factorial.testing import MockAgent
from tests.mocks.llm import MockLLMClient


@dataclass
class BrowserSession:
    session_id: str


class _NoopEvents:
    async def publish_event(self, _event: object) -> None:
        return None


def _make_tool_call(
    tool_name: str,
    arguments: dict[str, object] | None = None,
) -> ChatCompletionMessageFunctionToolCall:
    return ChatCompletionMessageFunctionToolCall(
        id=f"call_{uuid.uuid4().hex[:8]}",
        type="function",
        function=ToolCallFunction(
            name=tool_name,
            arguments=json.dumps(arguments or {}),
        ),
    )


MOCK_MODEL = Model(
    name="mock-model",
    provider=Provider.OPENAI,
    provider_model_id="mock-v1",
    context_window=128000,
)


def _make_agent_with_tools(tools: list[Any]) -> Agent:
    http_client = httpx.AsyncClient(verify=False, trust_env=False)
    return Agent(
        model=MOCK_MODEL,
        client=MockLLMClient(),
        tools=tools,
        http_client=http_client,
    )


def _set_test_execution_context(
    *,
    manager: ResourceManager | None = None,
) -> Any:
    ctx = ExecutionContext(
        task_id=str(uuid.uuid4()),
        owner_id="test-owner",
        agent_name="test-agent",
        retry_count=0,
        events=EventPublisher.__new__(EventPublisher),
        resources=ResourcesExecutionNamespace(manager=manager),
    )
    ctx.events = _NoopEvents()  # type: ignore[assignment]
    return execution_context.set(ctx)


def test_tool_schema_excludes_runtime_resource_params() -> None:
    @resource(BrowserSession)
    class BrowserLifecycle:
        @classmethod
        async def create(
            cls,
            ctx: ResourceContext,
            request: ResourceRequest[BrowserSession],
        ) -> BrowserSession:
            del ctx, request
            return BrowserSession(session_id="browser")

        @classmethod
        async def restore(
            cls,
            checkpoint: ResourceCheckpoint,
            ctx: ResourceContext,
            request: ResourceRequest[BrowserSession],
        ) -> BrowserSession:
            del ctx, request
            return BrowserSession(session_id=checkpoint.ref)

        @classmethod
        async def checkpoint(
            cls,
            resource_value: BrowserSession,
            ctx: ResourceContext,
            request: ResourceRequest[BrowserSession],
        ) -> ResourceCheckpoint | None:
            del ctx, request
            return ResourceCheckpoint(
                provider="browser",
                kind="session",
                ref=resource_value.session_id,
            )

        @classmethod
        async def destroy(
            cls,
            resource_value: BrowserSession,
            ctx: ResourceContext,
            request: ResourceRequest[BrowserSession],
        ) -> None:
            del resource_value, ctx, request

    @tool
    def inspect_resources(
        query: str,
        browser: BrowserSession,
        sandboxes: Sandboxes,
        resources: Resources,
    ) -> str:
        del browser, sandboxes, resources
        return query

    assert set(inspect_resources.params_json_schema["properties"]) == {"query"}


@pytest.mark.asyncio
async def test_tool_action_injects_custom_resource_dependency() -> None:
    calls: dict[str, int] = {"create": 0, "destroy": 0}

    @resource(BrowserSession)
    class BrowserLifecycle:
        @classmethod
        async def create(
            cls,
            ctx: ResourceContext,
            request: ResourceRequest[BrowserSession],
        ) -> BrowserSession:
            del ctx, request
            calls["create"] += 1
            return BrowserSession(session_id="browser-1")

        @classmethod
        async def restore(
            cls,
            checkpoint: ResourceCheckpoint,
            ctx: ResourceContext,
            request: ResourceRequest[BrowserSession],
        ) -> BrowserSession:
            del ctx, request
            return BrowserSession(session_id=checkpoint.ref)

        @classmethod
        async def checkpoint(
            cls,
            resource_value: BrowserSession,
            ctx: ResourceContext,
            request: ResourceRequest[BrowserSession],
        ) -> ResourceCheckpoint | None:
            del ctx, request
            return ResourceCheckpoint(
                provider="browser",
                kind="session",
                ref=resource_value.session_id,
            )

        @classmethod
        async def destroy(
            cls,
            resource_value: BrowserSession,
            ctx: ResourceContext,
            request: ResourceRequest[BrowserSession],
        ) -> None:
            del resource_value, ctx, request
            calls["destroy"] += 1

    @tool
    async def inspect_browser(topic: str, browser: BrowserSession) -> str:
        return f"{topic}:{browser.session_id}"

    manager = ResourceManager(
        store=InMemoryResourceBindingStore(),
        task_id="task-1",
        owner_id="owner-1",
        agent_name="resource-agent",
    )
    agent = _make_agent_with_tools([inspect_browser])
    token = _set_test_execution_context(manager=manager)
    try:
        result = await run_tool_action(
            agent,
            _make_tool_call("inspect_browser", {"topic": "cats"}),
            AgentContext(messages=[{"role": "user", "content": "q"}]),
        )
        assert result.client_output == "cats:browser-1"
        assert calls["create"] == 1
        await manager.destroy_all()
        assert calls["destroy"] == 1
    finally:
        execution_context.reset(token)
        await agent.http_client.aclose()


@pytest.mark.asyncio
async def test_prepare_turn_supports_runtime_resource_injection() -> None:
    calls: dict[str, int] = {"create": 0, "destroy": 0}

    @resource(BrowserSession)
    class BrowserLifecycle:
        @classmethod
        async def create(
            cls,
            ctx: ResourceContext,
            request: ResourceRequest[BrowserSession],
        ) -> BrowserSession:
            del ctx, request
            calls["create"] += 1
            return BrowserSession(session_id="browser-prepare")

        @classmethod
        async def restore(
            cls,
            checkpoint: ResourceCheckpoint,
            ctx: ResourceContext,
            request: ResourceRequest[BrowserSession],
        ) -> BrowserSession:
            del ctx, request
            return BrowserSession(session_id=checkpoint.ref)

        @classmethod
        async def checkpoint(
            cls,
            resource_value: BrowserSession,
            ctx: ResourceContext,
            request: ResourceRequest[BrowserSession],
        ) -> ResourceCheckpoint | None:
            del ctx, request
            return ResourceCheckpoint(
                provider="browser",
                kind="session",
                ref=resource_value.session_id,
            )

        @classmethod
        async def destroy(
            cls,
            resource_value: BrowserSession,
            ctx: ResourceContext,
            request: ResourceRequest[BrowserSession],
        ) -> None:
            del resource_value, ctx, request
            calls["destroy"] += 1

    def prepare_turn(turn, browser: BrowserSession) -> None:
        turn.messages = [
            {"role": "system", "content": browser.session_id},
            {"role": "user", "content": "prepared"},
        ]

    agent = MockAgent(
        name="resource_prepare_agent",
        instructions="Base instructions.",
        responses=["Prepared."],
        prepare_turn=prepare_turn,
    )
    try:
        result = await agent.run("Original input.")
    finally:
        await agent.http_client.aclose()

    first_call = agent.mock_client.call_history[0]
    assert first_call["messages"][0]["content"] == "browser-prepare"
    assert result.status.value == "completed"
    assert calls["create"] == 1
    assert calls["destroy"] == 1


@pytest.mark.asyncio
async def test_verifier_supports_runtime_resource_injection() -> None:
    @resource(BrowserSession)
    class BrowserLifecycle:
        @classmethod
        async def create(
            cls,
            ctx: ResourceContext,
            request: ResourceRequest[BrowserSession],
        ) -> BrowserSession:
            del ctx, request
            return BrowserSession(session_id="browser-verify")

        @classmethod
        async def restore(
            cls,
            checkpoint: ResourceCheckpoint,
            ctx: ResourceContext,
            request: ResourceRequest[BrowserSession],
        ) -> BrowserSession:
            del ctx, request
            return BrowserSession(session_id=checkpoint.ref)

        @classmethod
        async def checkpoint(
            cls,
            resource_value: BrowserSession,
            ctx: ResourceContext,
            request: ResourceRequest[BrowserSession],
        ) -> ResourceCheckpoint | None:
            del ctx, request
            return ResourceCheckpoint(
                provider="browser",
                kind="session",
                ref=resource_value.session_id,
            )

        @classmethod
        async def destroy(
            cls,
            resource_value: BrowserSession,
            ctx: ResourceContext,
            request: ResourceRequest[BrowserSession],
        ) -> None:
            del resource_value, ctx, request

    def verifier(output: Any, browser: BrowserSession):
        return verify.accept(
            metadata={
                "browser_session": browser.session_id,
                "output": output,
            }
        )

    agent = MockAgent(
        name="resource_verifier_agent",
        instructions="Verify outputs.",
        responses=["Verified."],
        verifier=verifier,
    )
    try:
        result = await agent.run("Check verifier.")
    finally:
        await agent.http_client.aclose()

    assert result.verification is not None
    assert result.verification.metadata == {
        "browser_session": "browser-verify",
        "output": "Verified.",
    }
