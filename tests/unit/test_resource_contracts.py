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
    Sandbox,
    SandboxCheckpoint,
    Sandboxes,
    SandboxExecResult,
    SandboxProcess,
    SandboxWriteFile,
    register_sandbox_provider,
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
    LiveResourceRef,
    ResourceManager,
    ResourcesExecutionNamespace,
)
from factorial.resources.sandbox.guarded import GuardedSandbox
from factorial.testing import MockAgent
from tests.mocks.llm import MockLLMClient


@dataclass
class BrowserSession:
    session_id: str


class _TestSandboxProcess(SandboxProcess):
    @property
    def id(self) -> str:
        return "test-process"

    async def wait(self) -> SandboxExecResult:
        return SandboxExecResult(
            command_id=self.id,
            exit_code=0,
            stdout_text="",
            stderr_text="",
        )

    async def output(self, stream: str = "both") -> str:
        del stream
        return ""

    async def stdout(self) -> str:
        return ""

    async def stderr(self) -> str:
        return ""

    async def kill(self, signal: int = 15) -> None:
        del signal


@dataclass
class _TestSandbox(Sandbox):
    sandbox_id: str
    provider_alias: str

    @property
    def id(self) -> str:
        return self.sandbox_id

    @property
    def provider(self) -> str:
        return self.provider_alias

    @property
    def native(self) -> object:
        return self

    async def exec(
        self,
        *args: str,
        cwd: str | None = None,
        env: dict[str, str] | None = None,
        timeout_s: float | None = None,
        sudo: bool = False,
    ) -> SandboxExecResult:
        del args, cwd, env, timeout_s, sudo
        return SandboxExecResult(
            command_id="exec",
            exit_code=0,
            stdout_text="",
            stderr_text="",
        )

    async def spawn(
        self,
        *args: str,
        cwd: str | None = None,
        env: dict[str, str] | None = None,
        timeout_s: float | None = None,
        sudo: bool = False,
    ) -> SandboxProcess:
        del args, cwd, env, timeout_s, sudo
        return _TestSandboxProcess()

    async def read_file(self, path: str) -> bytes | None:
        del path
        return None

    async def write_files(self, files: list[SandboxWriteFile]) -> None:
        del files

    async def mkdir(self, path: str, *, parents: bool = True) -> None:
        del path, parents

    async def url(self, port: int) -> str:
        return f"https://{self.sandbox_id}-{port}.example.test"

    async def checkpoint(self) -> SandboxCheckpoint:
        return SandboxCheckpoint(
            provider=self.provider_alias,
            kind="sandbox",
            ref=f"checkpoint-{self.sandbox_id}",
        )


@pytest.mark.asyncio
async def test_guarded_sandbox_native_methods_are_lease_validated() -> None:
    async def _validator() -> None:
        raise RuntimeError("lease lost")

    sandbox = GuardedSandbox(
        sandbox=_TestSandbox(sandbox_id="sb-1", provider_alias="test"),
        validator=_validator,
    )

    with pytest.raises(RuntimeError, match="lease lost"):
        await sandbox.native.exec("echo", "unsafe")  # type: ignore[attr-defined]


@dataclass
class _RecordingSandboxProvider:
    alias: str
    created_names: list[str]

    async def create(
        self,
        ctx: ResourceContext,
        request: ResourceRequest[Sandbox],
    ) -> Sandbox:
        del ctx
        self.created_names.append(request.logical_name)
        return _TestSandbox(
            sandbox_id=f"{self.alias}-{request.logical_name}",
            provider_alias=self.alias,
        )

    async def restore(
        self,
        checkpoint: ResourceCheckpoint,
        ctx: ResourceContext,
        request: ResourceRequest[Sandbox],
    ) -> Sandbox:
        del ctx, request
        return _TestSandbox(
            sandbox_id=checkpoint.ref,
            provider_alias=self.alias,
        )

    async def checkpoint(
        self,
        resource: Sandbox,
        ctx: ResourceContext,
        request: ResourceRequest[Sandbox],
    ) -> ResourceCheckpoint | None:
        del ctx, request
        return ResourceCheckpoint(
            provider=self.alias,
            kind="sandbox",
            ref=f"checkpoint-{resource.id}",
        )

    async def destroy(
        self,
        resource: Sandbox,
        ctx: ResourceContext,
        request: ResourceRequest[Sandbox],
    ) -> None:
        del resource, ctx, request

    async def attach_live(
        self,
        live_ref: LiveResourceRef,
        ctx: ResourceContext,
        request: ResourceRequest[Sandbox],
    ) -> Sandbox | None:
        del ctx, request
        return _TestSandbox(
            sandbox_id=live_ref.ref,
            provider_alias=self.alias,
        )

    def capture_live_ref(
        self,
        resource: Sandbox,
        ctx: ResourceContext,
        request: ResourceRequest[Sandbox],
    ) -> LiveResourceRef | None:
        del ctx, request
        return LiveResourceRef(
            provider=self.alias,
            kind="sandbox",
            ref=resource.id,
        )

    async def delete_checkpoint(
        self,
        checkpoint: ResourceCheckpoint,
        ctx: ResourceContext,
        request: ResourceRequest[Sandbox],
    ) -> None:
        del checkpoint, ctx, request


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
    default_sandbox_provider: str | None = None,
) -> Any:
    ctx = ExecutionContext(
        task_id=str(uuid.uuid4()),
        owner_id="test-owner",
        agent_name="test-agent",
        retry_count=0,
        events=EventPublisher.__new__(EventPublisher),
        resources=ResourcesExecutionNamespace(
            manager=manager,
            default_sandbox_provider=default_sandbox_provider,
        ),
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
async def test_sandbox_runtime_uses_default_and_explicit_provider_aliases() -> None:
    default_alias = f"test-default-{uuid.uuid4().hex}"
    explicit_alias = f"test-explicit-{uuid.uuid4().hex}"
    default_provider = _RecordingSandboxProvider(default_alias, [])
    explicit_provider = _RecordingSandboxProvider(explicit_alias, [])
    register_sandbox_provider(default_provider, alias=default_alias)
    register_sandbox_provider(explicit_provider, alias=explicit_alias)

    @tool
    async def inspect_default_provider(sandbox: Sandbox) -> str:
        return sandbox.provider

    @tool
    async def inspect_explicit_provider(sandboxes: Sandboxes) -> str:
        sandbox = await sandboxes.get("browser", provider=explicit_alias)
        return sandbox.provider

    manager = ResourceManager(
        store=InMemoryResourceBindingStore(),
        task_id="task-sandbox",
        owner_id="owner-sandbox",
        agent_name="sandbox-agent",
    )
    agent = _make_agent_with_tools(
        [inspect_default_provider, inspect_explicit_provider]
    )
    token = _set_test_execution_context(
        manager=manager,
        default_sandbox_provider=default_alias,
    )
    try:
        default_result = await run_tool_action(
            agent,
            _make_tool_call("inspect_default_provider"),
            AgentContext(messages=[{"role": "user", "content": "q"}]),
        )
        explicit_result = await run_tool_action(
            agent,
            _make_tool_call("inspect_explicit_provider"),
            AgentContext(messages=[{"role": "user", "content": "q"}]),
        )

        assert default_result.client_output == default_alias
        assert explicit_result.client_output == explicit_alias
        assert default_provider.created_names == ["default"]
        assert explicit_provider.created_names == ["browser"]
    finally:
        execution_context.reset(token)
        await manager.destroy_all()
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
