from __future__ import annotations

import asyncio
import contextlib
import hashlib
import inspect
import json
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from datetime import datetime, timezone
from typing import (
    Any,
    Generic,
    Literal,
    cast,
    get_args,
    get_origin,
)

import httpx
from openai.types.chat import (
    ChatCompletion,
    ChatCompletionMessageToolCall,
)
from openai.types.chat.chat_completion_message_function_tool_call import (
    ChatCompletionMessageFunctionToolCall,
)
from pydantic import TypeAdapter
from typing_extensions import TypeVar

from factorial.agent.context import (
    AgentContext,
    ContextType,
    EmptyMetadata,
    EmptyState,
)
from factorial.agent.helpers import (
    _DirectEventPublisher,
    _maybe_call_prepare_turn,
    _RunFailureError,
    chain_prepare_turn,
    invoke_callable_non_blocking,
    retry,
)
from factorial.agent.stop import (
    StopCondition,
    StopWhen,
    _infer_turn_limit_hint,
    all_of,
    any_of,
    no_tool_calls,
    stop,
    tool_called,
    total_tokens_exceed,
    turn_count_is,
)
from factorial.agent.tools.core import (
    ToolDefinition,
    _ToolResultInternal,
    convert_tools_list,
)
from factorial.agent.tools.runtime import execute_tools
from factorial.agent.types import (
    Callbacks,
    EventCallback,
    PrepareTurnHook,
    ToolChoice,
    ToolExecutionResults,
    Turn,
    TurnCompletion,
)
from factorial.ai.messages import (
    Message,
    MessageLike,
    assistant,
    messages_to_chat_messages,
    normalize_message,
    normalize_messages_input,
    system,
    tool_call as message_tool_call,
    tool_calls as message_tool_calls,
)
from factorial.ai.models import Model, MultiClient
from factorial.core.events import (
    BaseEvent,
    EventPublisher,
    FinishEvent,
    ModelFinishEvent,
    ModelStartEvent,
    StartEvent,
    ToolFinishEvent,
    ToolStartEvent,
    TurnFinishEvent,
    TurnStartEvent,
    WaitEvent,
)
from factorial.core.exceptions import (
    FatalAgentError,
)
from factorial.core.logging import get_logger
from factorial.core.run_types import (
    RunError,
    RunResult,
    RunStatus,
    TurnSummary,
    UsageSummary,
    VerificationSummary,
    VerifierAccept,
    VerifierFail,
    VerifierRetry,
    verify,
)
from factorial.core.utils import serialize_data, to_snake_case
from factorial.execution.context import (
    ExecutionContext,
    execution_context,
)
from factorial.execution.dependencies import (
    inject_runtime_kwargs,
    is_runtime_injected_annotation,
)
from factorial.execution.waits import WaitInstruction
from factorial.resources import (
    InMemoryResourceBindingStore,
    ResourceManager,
    ResourcesExecutionNamespace,
)

logger = get_logger(__name__)

StateT = TypeVar("StateT")
MetadataT = TypeVar("MetadataT", default=EmptyMetadata)
VerificationMetaT = TypeVar("VerificationMetaT")

Verifier = Callable[..., Any]


class BaseAgent(Generic[ContextType]):
    def __init__(
        self,
        *,
        name: str | None = None,
        instructions: str | None = None,
        description: str | None = None,
        tools: Sequence[ToolDefinition[ContextType] | Callable[..., Any]] | None = None,
        model: Model | Callable[[ContextType], Model] | None = None,
        tool_choice: ToolChoice = "auto",
        parallel_tool_calls: bool | None = None,
        temperature: float | None = None,
        max_output_tokens: int | None = None,
        prepare_turn: PrepareTurnHook | None = None,
        stop_when: StopWhen | StopCondition | None = None,
        verifier: Verifier | None = None,
        callbacks: Callbacks | None = None,
        http_client: httpx.AsyncClient | None = None,
        client: MultiClient | None = None,
        request_timeout: float = 120.0,
    ):
        self.name = to_snake_case(name or self.__class__.__name__)
        self.description = description or self.__class__.__name__
        self.instructions = instructions
        self.tools, self.tool_actions = convert_tools_list(tools or [])
        self.http_client = http_client or httpx.AsyncClient(timeout=request_timeout)
        self.client = client or MultiClient(http_client=self.http_client)
        self.request_timeout = request_timeout
        self.default_tool_choice = tool_choice
        self.default_parallel_tool_calls = parallel_tool_calls
        self.default_temperature = temperature
        self.default_max_output_tokens = max_output_tokens
        self.prepare_turn = prepare_turn
        self.stop_when = stop_when or any_of(
            no_tool_calls(),
            turn_count_is(10),
        )
        self.verifier = verifier
        self.callbacks = callbacks or Callbacks()
        self.max_turns = _infer_turn_limit_hint(self.stop_when)

        if model is None:
            raise ValueError("model is required")
        self.model: Model | Callable[[ContextType], Model] = model

    def _resolve_state_and_metadata_types(self) -> tuple[Any, Any]:
        original = getattr(self, "__orig_class__", None)
        if original is None:
            return EmptyState, EmptyMetadata

        original_args = get_args(original)
        if len(original_args) >= 2:
            return original_args[0], original_args[1]

        if len(original_args) == 1:
            first_arg = original_args[0]
            context_origin = get_origin(first_arg)
            context_args = get_args(first_arg)
            if context_origin is AgentContext:
                if len(context_args) >= 2:
                    return context_args[0], context_args[1]
                if len(context_args) == 1:
                    return context_args[0], EmptyMetadata
                return EmptyState, EmptyMetadata
            if isinstance(first_arg, type) and issubclass(first_arg, AgentContext):
                return EmptyState, EmptyMetadata
            return first_arg, EmptyMetadata

        return EmptyState, EmptyMetadata

    def _default_typed_payload(self, target_type: Any, *, label: str) -> Any:
        if target_type in (Any, object, None, EmptyState, EmptyMetadata):
            return EmptyState() if label == "state" else EmptyMetadata()
        if isinstance(target_type, type):
            try:
                return target_type()
            except Exception as exc:
                raise ValueError(
                    f"{label} must be provided because {target_type!r} "
                    "does not have a default constructor"
                ) from exc
        raise ValueError(f"{label} must be provided")

    def _coerce_typed_payload(self, value: Any, target_type: Any, *, label: str) -> Any:
        if value is None:
            return self._default_typed_payload(target_type, label=label)
        if target_type in (Any, object, None):
            return value
        if isinstance(target_type, type) and isinstance(value, target_type):
            return value
        try:
            return TypeAdapter(target_type).validate_python(value)
        except Exception as exc:
            raise TypeError(f"Invalid {label} for agent {self.name}: {exc}") from exc

    def build_context(
        self,
        input: str | Sequence[MessageLike],
        *,
        state: Any = None,
        metadata: Any = None,
    ) -> ContextType:
        state_type, metadata_type = self._resolve_state_and_metadata_types()
        messages = normalize_messages_input(input)
        if self.instructions:
            messages = [system(self.instructions), *messages]

        return cast(
            ContextType,
            AgentContext(
                messages=messages,
                state=self._coerce_typed_payload(state, state_type, label="state"),
                metadata=self._coerce_typed_payload(
                    metadata,
                    metadata_type,
                    label="metadata",
                ),
            ),
        )

    def context_from_dict(self, data: dict[str, Any]) -> ContextType:
        state_type, metadata_type = self._resolve_state_and_metadata_types()
        return cast(
            ContextType,
            AgentContext.from_dict(
                data,
                state_type=state_type,
                metadata_type=metadata_type,
            ),
        )

    def resolve_model(self, agent_ctx: ContextType) -> Model:
        model_or_factory = self.model
        if callable(model_or_factory):
            model = cast(Callable[[ContextType], Model], model_or_factory)(agent_ctx)
        else:
            model = model_or_factory
        if not isinstance(model, Model):
            raise TypeError("Resolved model must be a factorial.ai.models.Model")
        return model

    def resolve_tools(
        self, agent_ctx: ContextType
    ) -> list[ToolDefinition[ContextType]]:
        return [
            tool
            for tool in self.tools
            if (
                tool.is_enabled(agent_ctx)
                if callable(tool.is_enabled)
                else bool(tool.is_enabled)
            )
        ]

    def _build_turn(
        self,
        agent_ctx: ContextType,
        execution_ctx: ExecutionContext,
    ) -> Turn[ContextType]:
        return Turn(
            model=self.resolve_model(agent_ctx),
            messages=list(agent_ctx.messages),
            tools=self.resolve_tools(agent_ctx),
            tool_choice=self.default_tool_choice,
            parallel_tool_calls=self.default_parallel_tool_calls,
            temperature=self.default_temperature,
            max_output_tokens=self.default_max_output_tokens,
        )

    async def validate_completion(
        self,
        agent_ctx: ContextType,
        response: ChatCompletion,
    ) -> None:
        del agent_ctx, response

    @retry
    async def _completion_with_retry(
        self,
        turn: Turn[ContextType],
        agent_ctx: ContextType,
    ) -> ChatCompletion:
        response = cast(
            ChatCompletion,
            await self.client.completion(
                model=turn.model,
                messages=messages_to_chat_messages(turn.messages),
                tools=[tool.to_openai_tool_schema() for tool in turn.tools] or None,
                tool_choice=turn.tool_choice,
                parallel_tool_calls=turn.parallel_tool_calls,
                temperature=turn.temperature,
                max_completion_tokens=turn.max_output_tokens,
                stream=False,
            ),
        )
        await self.validate_completion(agent_ctx, response)
        return response

    async def _invoke_callback(
        self,
        callback: EventCallback | None,
        event: BaseEvent,
        agent_ctx: ContextType,
        execution_ctx: ExecutionContext,
    ) -> None:
        if callback is None:
            return

        kwargs: dict[str, Any] = {}
        signature = inspect.signature(callback)
        params = list(signature.parameters.values())
        if params:
            first = params[0]
            if first.kind not in (
                inspect.Parameter.POSITIONAL_ONLY,
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
            ):
                raise TypeError("Callback must accept the event as its first parameter")

        for param in params[1:]:
            if param.kind not in (
                inspect.Parameter.KEYWORD_ONLY,
                inspect.Parameter.VAR_KEYWORD,
            ):
                raise TypeError(
                    "Additional callback parameters must be keyword-only "
                    "for injected context values"
                )
            if param.kind is inspect.Parameter.VAR_KEYWORD:
                continue
            if param.name == "agent_ctx":
                kwargs[param.name] = agent_ctx
            elif param.name == "execution_ctx":
                kwargs[param.name] = execution_ctx
            elif param.default is inspect.Parameter.empty:
                raise TypeError(
                    f"Unsupported required callback parameter '{param.name}'. "
                    "Only keyword-only agent_ctx and execution_ctx are injected."
                )

        try:
            result = callback(event, **kwargs)
            if inspect.isawaitable(result):
                await cast(Awaitable[Any], result)
        except FatalAgentError:
            raise
        except Exception:
            logger.exception("Callback failed", exc_info=True)

    async def _dispatch_callbacks(
        self,
        event: BaseEvent,
        agent_ctx: ContextType,
        execution_ctx: ExecutionContext,
    ) -> None:
        callback: EventCallback | None = None
        if isinstance(event, StartEvent):
            callback = self.callbacks.on_start
        elif isinstance(event, TurnStartEvent):
            callback = self.callbacks.on_turn_start
        elif isinstance(event, ModelStartEvent):
            callback = self.callbacks.on_model_start
        elif isinstance(event, ModelFinishEvent):
            callback = self.callbacks.on_model_finish
        elif isinstance(event, ToolStartEvent):
            callback = self.callbacks.on_tool_start
        elif isinstance(event, ToolFinishEvent):
            callback = self.callbacks.on_tool_finish
        elif isinstance(event, WaitEvent):
            callback = self.callbacks.on_wait
        elif isinstance(event, TurnFinishEvent):
            callback = self.callbacks.on_turn_finish
        elif isinstance(event, FinishEvent):
            callback = self.callbacks.on_finish
        await self._invoke_callback(callback, event, agent_ctx, execution_ctx)

    async def _emit_event(
        self,
        event: BaseEvent,
        agent_ctx: ContextType,
        execution_ctx: ExecutionContext,
    ) -> None:
        if execution_ctx.events is not None:
            await execution_ctx.events.publish_event(event)
        await self._dispatch_callbacks(event, agent_ctx, execution_ctx)

    def _response_tool_calls(
        self,
        response: ChatCompletion,
    ) -> list[ChatCompletionMessageFunctionToolCall]:
        tool_calls = response.choices[0].message.tool_calls or []
        return [
            tool_call
            for tool_call in tool_calls
            if isinstance(tool_call, ChatCompletionMessageFunctionToolCall)
        ]

    def _canonical_response_messages(
        self,
        response: ChatCompletion,
    ) -> tuple[list[Message], list[ChatCompletionMessageFunctionToolCall]]:
        messages: list[Message] = []
        content = response.choices[0].message.content or ""
        function_tool_calls = self._response_tool_calls(response)

        if content:
            messages.append(assistant(content))

        if function_tool_calls:
            messages.append(
                message_tool_calls(
                    *[
                        message_tool_call(
                            tool_call.function.name,
                            json.loads(tool_call.function.arguments),
                            call_id=tool_call.id,
                        )
                        for tool_call in function_tool_calls
                    ]
                )
            )

        return messages, function_tool_calls

    def _candidate_output_from_turn(
        self,
        *,
        assistant_content: str,
        resolved_tool_results: list[
            tuple[ChatCompletionMessageToolCall, _ToolResultInternal | BaseException]
        ],
    ) -> Any:
        if assistant_content or not resolved_tool_results:
            return assistant_content

        for tool_call, result in resolved_tool_results:
            del tool_call
            if isinstance(result, BaseException):
                continue
            if result.pending_result:
                continue
            return result.client_output
        return None

    def _turn_finish_reason(
        self,
        response: ChatCompletion,
        function_tool_calls: list[ChatCompletionMessageFunctionToolCall],
    ) -> str:
        if function_tool_calls:
            tool_names = ",".join(
                tool_call.function.name for tool_call in function_tool_calls
            )
            return f"tool_called:{tool_names}"
        return str(response.choices[0].finish_reason or "stop")

    def _event_pending_child_details(
        self,
        resolved_tool_results: list[
            tuple[ChatCompletionMessageToolCall, _ToolResultInternal | BaseException]
        ],
    ) -> tuple[tuple[str, ...], tuple[str, ...]]:
        child_task_ids: list[str] = []
        source_tool_call_ids: list[str] = []

        for tool_call, result in resolved_tool_results:
            if isinstance(result, BaseException):
                continue

            result_child_task_ids = list(result.pending_child_task_ids or ())
            if result_child_task_ids:
                child_task_ids.extend(result_child_task_ids)
                source_tool_call_ids.append(tool_call.id)

            if (
                isinstance(result.client_output, WaitInstruction)
                and result.client_output.kind == "jobs"
                and result.client_output.child_task_ids
            ):
                child_task_ids.extend(result.client_output.child_task_ids)
                source_tool_call_ids.append(tool_call.id)

        return (
            tuple(dict.fromkeys(child_task_ids)),
            tuple(dict.fromkeys(source_tool_call_ids)),
        )

    def _format_verifier_feedback(self, decision: VerifierRetry[Any]) -> str:
        code_suffix = f" [{decision.code}]" if decision.code else ""
        return f"Verifier feedback{code_suffix}: {decision.message}"

    def _compute_candidate_hash(self, output: Any) -> str:
        canonical_payload = json.dumps(
            serialize_data(output),
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        )
        return hashlib.sha256(canonical_payload.encode("utf-8")).hexdigest()

    async def _resolve_verifier_injected_kwargs(
        self,
        agent_ctx: ContextType,
        execution_ctx: ExecutionContext,
    ) -> dict[str, Any]:
        verifier = self.verifier
        if verifier is None:
            return {}

        signature = inspect.signature(verifier)
        params = list(signature.parameters.values())
        if not params:
            raise TypeError("verifier must accept at least one argument for output")

        kwargs = await inject_runtime_kwargs(
            func=verifier,
            existing_kwargs={},
            agent_ctx=agent_ctx,
            execution_ctx=execution_ctx,
            start_at=1,
        )
        for param in params[1:]:
            if param.kind in (
                inspect.Parameter.VAR_POSITIONAL,
                inspect.Parameter.VAR_KEYWORD,
            ):
                continue
            if param.name in kwargs or param.default is not inspect.Parameter.empty:
                continue
            if is_runtime_injected_annotation(param.name, param.annotation):
                raise RuntimeError(
                    f"Failed to resolve verifier parameter '{param.name}'."
                )
            raise TypeError(
                f"Unsupported required verifier parameter '{param.name}'. "
                "Only output (first arg) and runtime-injected dependencies "
                "are supported."
            )
        return kwargs

    async def _apply_verifier(
        self,
        candidate_output: Any,
        agent_ctx: ContextType,
        execution_ctx: ExecutionContext,
    ) -> tuple[Literal["accept", "retry"], VerificationSummary[Any] | None]:
        if self.verifier is None:
            return "accept", None

        decision = await invoke_callable_non_blocking(
            self.verifier,
            candidate_output,
            **(await self._resolve_verifier_injected_kwargs(agent_ctx, execution_ctx)),
        )
        if not isinstance(decision, (VerifierAccept, VerifierRetry, VerifierFail)):
            raise TypeError(
                "verifier must return verify.accept(...), verify.retry(...), "
                "or verify.fail(...)"
            )

        candidate_hash = self._compute_candidate_hash(candidate_output)
        verification_state = agent_ctx.verification
        verification_state.last_candidate_hash = candidate_hash

        if isinstance(decision, VerifierAccept):
            verification_state.last_outcome = "passed"
            summary = VerificationSummary(
                status="passed",
                attempts_used=verification_state.attempts_used,
                metadata=decision.metadata,
            )
            return "accept", summary

        if isinstance(decision, VerifierRetry):
            verification_state.attempts_used += 1
            verification_state.last_outcome = "retry_requested"
            verification_state.last_code = decision.code
            summary = VerificationSummary(
                status="retry_requested",
                attempts_used=verification_state.attempts_used,
                code=decision.code,
                message=decision.message,
                metadata=decision.metadata,
            )
            agent_ctx.messages.append(system(self._format_verifier_feedback(decision)))
            return "retry", summary

        verification_state.last_outcome = "failed"
        verification_state.last_code = decision.code
        raise _RunFailureError(
            decision.message,
            verification_summary=VerificationSummary(
                status="failed",
                attempts_used=verification_state.attempts_used,
                code=decision.code,
                message=decision.message,
                metadata=decision.metadata,
            ),
        )

    async def run_turn(
        self,
        agent_ctx: ContextType,
    ) -> TurnCompletion[ContextType]:
        execution_ctx = ExecutionContext.current()
        turn = await _maybe_call_prepare_turn(
            self.prepare_turn,
            self._build_turn(agent_ctx, execution_ctx),
            agent_ctx,
            execution_ctx,
        )

        await self._emit_event(
            TurnStartEvent(
                task_id=execution_ctx.task_id,
                owner_id=execution_ctx.owner_id,
                agent_name=self.name,
                turn=agent_ctx.turn_number,
            ),
            agent_ctx,
            execution_ctx,
        )
        await self._emit_event(
            ModelStartEvent(
                task_id=execution_ctx.task_id,
                owner_id=execution_ctx.owner_id,
                agent_name=self.name,
                turn=agent_ctx.turn_number,
                model_name=turn.model.name,
            ),
            agent_ctx,
            execution_ctx,
        )

        response = await self._completion_with_retry(turn, agent_ctx)
        turn_usage = UsageSummary.from_provider_usage(getattr(response, "usage", None))
        execution_ctx.usage = execution_ctx.usage.add(turn_usage)

        canonical_response_messages, function_tool_calls = (
            self._canonical_response_messages(response)
        )
        assistant_content = response.choices[0].message.content or ""
        updated_messages = [*agent_ctx.messages, *canonical_response_messages]

        finish_reason = self._turn_finish_reason(response, function_tool_calls)
        await self._emit_event(
            ModelFinishEvent(
                task_id=execution_ctx.task_id,
                owner_id=execution_ctx.owner_id,
                agent_name=self.name,
                turn=agent_ctx.turn_number,
                model_name=turn.model.name,
                finish_reason=finish_reason,
                usage=turn_usage,
            ),
            agent_ctx,
            execution_ctx,
        )

        tool_results = ToolExecutionResults(
            new_messages=[],
            tool_call_results=[],
            resolved_results=[],
            pending_tool_call_ids=[],
            pending_child_task_ids=[],
        )
        if function_tool_calls:
            tool_results = await execute_tools(self, function_tool_calls, agent_ctx)
            updated_messages.extend(tool_results.new_messages)

        agent_ctx.messages = updated_messages
        candidate_output = self._candidate_output_from_turn(
            assistant_content=assistant_content,
            resolved_tool_results=tool_results.resolved_results,
        )
        turn_summary = TurnSummary(
            turn_number=agent_ctx.turn_number,
            finish_reason=finish_reason,
            status="completed",
            output=candidate_output,
            usage=turn_usage,
        )
        execution_ctx.last_turn = turn_summary
        event_pending_child_task_ids, event_source_tool_call_ids = (
            self._event_pending_child_details(tool_results.resolved_results)
        )

        if tool_results.pending_tool_call_ids:
            wait_event = WaitEvent(
                task_id=execution_ctx.task_id,
                owner_id=execution_ctx.owner_id,
                agent_name=self.name,
                turn=agent_ctx.turn_number,
                wait_kind="pending_tool_call_results",
                source_tool_call_ids=tuple(tool_results.pending_tool_call_ids),
            )
            await self._emit_event(wait_event, agent_ctx, execution_ctx)

        if event_pending_child_task_ids:
            wait_event = WaitEvent(
                task_id=execution_ctx.task_id,
                owner_id=execution_ctx.owner_id,
                agent_name=self.name,
                turn=agent_ctx.turn_number,
                wait_kind="pending_child_task_results",
                source_tool_call_ids=event_source_tool_call_ids,
                pending_child_task_ids=event_pending_child_task_ids,
            )
            await self._emit_event(wait_event, agent_ctx, execution_ctx)

        should_stop = bool(self.stop_when(agent_ctx, execution_ctx))
        verification_summary: VerificationSummary[Any] | None = None
        if should_stop:
            if candidate_output is None:
                raise _RunFailureError(
                    "Agent stopped without producing a finalized output"
                )
            verifier_action, verification_summary = await self._apply_verifier(
                candidate_output,
                agent_ctx,
                execution_ctx,
            )
            if verifier_action == "accept":
                agent_ctx.output = candidate_output
                completion = TurnCompletion(
                    is_done=True,
                    context=agent_ctx,
                    output=candidate_output,
                    tool_call_results=tool_results.tool_call_results,
                    pending_tool_call_ids=tool_results.pending_tool_call_ids,
                    pending_child_task_ids=tool_results.pending_child_task_ids,
                    finish_reason=finish_reason,
                    usage=turn_usage,
                    turn_summary=turn_summary,
                    verification_summary=verification_summary,
                )
                await self._emit_event(
                    TurnFinishEvent(
                        task_id=execution_ctx.task_id,
                        owner_id=execution_ctx.owner_id,
                        agent_name=self.name,
                        turn=agent_ctx.turn_number,
                        finish_reason=finish_reason,
                        output=candidate_output,
                        pending_tool_call_ids=tuple(tool_results.pending_tool_call_ids),
                        pending_child_task_ids=event_pending_child_task_ids,
                        usage=turn_usage,
                    ),
                    agent_ctx,
                    execution_ctx,
                )
                return completion

        agent_ctx.turn_number += 1
        completion = TurnCompletion(
            is_done=False,
            context=agent_ctx,
            tool_call_results=tool_results.tool_call_results,
            pending_tool_call_ids=tool_results.pending_tool_call_ids,
            pending_child_task_ids=tool_results.pending_child_task_ids,
            finish_reason=finish_reason,
            usage=turn_usage,
            turn_summary=turn_summary,
            verification_summary=verification_summary,
        )
        await self._emit_event(
            TurnFinishEvent(
                task_id=execution_ctx.task_id,
                owner_id=execution_ctx.owner_id,
                agent_name=self.name,
                turn=turn_summary.turn_number,
                finish_reason=finish_reason,
                output=None,
                pending_tool_call_ids=tuple(tool_results.pending_tool_call_ids),
                pending_child_task_ids=event_pending_child_task_ids,
                usage=turn_usage,
            ),
            agent_ctx,
            execution_ctx,
        )
        return completion

    async def steer(
        self,
        messages: list[dict[str, Any]],
        agent_ctx: ContextType,
        execution_ctx: ExecutionContext,
    ) -> ContextType:
        del execution_ctx
        agent_ctx.messages.extend(
            normalize_message(cast(MessageLike, message)) for message in messages
        )
        return agent_ctx

    async def run(
        self,
        input: str | Sequence[MessageLike],
        *,
        state: Any = None,
        metadata: Any = None,
    ) -> RunResult[Any, Any, Any]:
        run_id = str(uuid.uuid4())
        task_id = str(uuid.uuid4())
        owner_id = str(uuid.uuid4())
        started_at = datetime.now(timezone.utc)
        agent_ctx = self.build_context(input, state=state, metadata=metadata)
        execution_ctx = ExecutionContext(
            task_id=task_id,
            owner_id=owner_id,
            agent_name=self.name,
            retry_count=0,
            events=cast(EventPublisher, _DirectEventPublisher()),
            resources=ResourcesExecutionNamespace(
                manager=ResourceManager(
                    store=InMemoryResourceBindingStore(),
                    task_id=task_id,
                    owner_id=owner_id,
                    agent_name=self.name,
                )
            ),
        )
        token = execution_context.set(execution_ctx)
        last_verification_summary: VerificationSummary[Any] | None = None

        try:
            await self._emit_event(
                StartEvent(
                    task_id=task_id,
                    owner_id=owner_id,
                    agent_name=self.name,
                ),
                agent_ctx,
                execution_ctx,
            )

            while True:
                completion = await self.run_turn(agent_ctx)
                if completion.verification_summary is not None:
                    last_verification_summary = completion.verification_summary
                if (
                    completion.pending_tool_call_ids
                    or completion.pending_child_task_ids
                ):
                    raise RuntimeError(
                        "Direct agent.run(...) does not support pending tool results "
                        "or pending child-task continuations"
                    )
                if completion.is_done:
                    finished_at = datetime.now(timezone.utc)
                    result = RunResult(
                        run_id=run_id,
                        task_id=task_id,
                        agent_name=self.name,
                        owner_id=owner_id,
                        status=RunStatus.COMPLETED,
                        output=completion.output,
                        state=agent_ctx.state,
                        metadata=agent_ctx.metadata,
                        messages=tuple(agent_ctx.messages),
                        usage=execution_ctx.usage,
                        turn_count=completion.turn_summary.turn_number
                        if completion.turn_summary
                        else agent_ctx.turn_number,
                        last_turn=completion.turn_summary,
                        verification=last_verification_summary,
                        started_at=started_at,
                        finished_at=finished_at,
                    )
                    await self._emit_event(
                        FinishEvent(
                            task_id=task_id,
                            owner_id=owner_id,
                            agent_name=self.name,
                            status=RunStatus.COMPLETED,
                            output=result.output,
                            turn_count=result.turn_count,
                            usage=result.usage,
                        ),
                        agent_ctx,
                        execution_ctx,
                    )
                    return result

        except _RunFailureError as exc:
            finished_at = datetime.now(timezone.utc)
            error = RunError.from_exception(exc)
            result = RunResult(
                run_id=run_id,
                task_id=task_id,
                agent_name=self.name,
                owner_id=owner_id,
                status=RunStatus.FAILED,
                output=None,
                state=agent_ctx.state,
                metadata=agent_ctx.metadata,
                messages=tuple(agent_ctx.messages),
                usage=execution_ctx.usage,
                turn_count=execution_ctx.last_turn.turn_number
                if execution_ctx.last_turn
                else max(agent_ctx.turn_number - 1, 0),
                last_turn=execution_ctx.last_turn,
                verification=exc.verification_summary or last_verification_summary,
                started_at=started_at,
                finished_at=finished_at,
                error=error,
            )
            await self._emit_event(
                FinishEvent(
                    task_id=task_id,
                    owner_id=owner_id,
                    agent_name=self.name,
                    status=RunStatus.FAILED,
                    run_error=error,
                    turn_count=result.turn_count,
                    usage=result.usage,
                ),
                agent_ctx,
                execution_ctx,
            )
            return result
        except Exception as exc:
            finished_at = datetime.now(timezone.utc)
            error = RunError.from_exception(exc)
            result = RunResult(
                run_id=run_id,
                task_id=task_id,
                agent_name=self.name,
                owner_id=owner_id,
                status=RunStatus.FAILED,
                output=None,
                state=agent_ctx.state,
                metadata=agent_ctx.metadata,
                messages=tuple(agent_ctx.messages),
                usage=execution_ctx.usage,
                turn_count=execution_ctx.last_turn.turn_number
                if execution_ctx.last_turn
                else max(agent_ctx.turn_number - 1, 0),
                last_turn=execution_ctx.last_turn,
                verification=last_verification_summary,
                started_at=started_at,
                finished_at=finished_at,
                error=error,
            )
            await self._emit_event(
                FinishEvent(
                    task_id=task_id,
                    owner_id=owner_id,
                    agent_name=self.name,
                    status=RunStatus.FAILED,
                    run_error=error,
                    turn_count=result.turn_count,
                    usage=result.usage,
                ),
                agent_ctx,
                execution_ctx,
            )
            return result
        finally:
            try:
                await execution_ctx.resources.destroy_all()
            except Exception:
                logger.exception("Failed to destroy runtime resources for run()")
            execution_context.reset(token)

    async def stream(
        self,
        input: str | Sequence[MessageLike],
        *,
        state: Any = None,
        metadata: Any = None,
    ) -> AsyncIterator[BaseEvent]:
        queue: asyncio.Queue[BaseEvent | None] = asyncio.Queue()

        async def sink(event: BaseEvent) -> None:
            await queue.put(event)

        async def _runner() -> None:
            run_id = str(uuid.uuid4())
            task_id = str(uuid.uuid4())
            owner_id = str(uuid.uuid4())
            started_at = datetime.now(timezone.utc)
            agent_ctx = self.build_context(input, state=state, metadata=metadata)
            execution_ctx = ExecutionContext(
                task_id=task_id,
                owner_id=owner_id,
                agent_name=self.name,
                retry_count=0,
                events=cast(EventPublisher, _DirectEventPublisher(sink)),
                resources=ResourcesExecutionNamespace(
                    manager=ResourceManager(
                        store=InMemoryResourceBindingStore(),
                        task_id=task_id,
                        owner_id=owner_id,
                        agent_name=self.name,
                    )
                ),
            )
            token = execution_context.set(execution_ctx)
            last_verification_summary: VerificationSummary[Any] | None = None
            try:
                await self._emit_event(
                    StartEvent(
                        task_id=task_id,
                        owner_id=owner_id,
                        agent_name=self.name,
                    ),
                    agent_ctx,
                    execution_ctx,
                )
                while True:
                    completion = await self.run_turn(agent_ctx)
                    if completion.verification_summary is not None:
                        last_verification_summary = completion.verification_summary
                    if (
                        completion.pending_tool_call_ids
                        or completion.pending_child_task_ids
                    ):
                        raise RuntimeError(
                            "Direct agent.stream(...) does not support pending tool "
                            "results or pending child-task continuations"
                        )
                    if completion.is_done:
                        result = RunResult(
                            run_id=run_id,
                            task_id=task_id,
                            agent_name=self.name,
                            owner_id=owner_id,
                            status=RunStatus.COMPLETED,
                            output=completion.output,
                            state=agent_ctx.state,
                            metadata=agent_ctx.metadata,
                            messages=tuple(agent_ctx.messages),
                            usage=execution_ctx.usage,
                            turn_count=completion.turn_summary.turn_number
                            if completion.turn_summary
                            else agent_ctx.turn_number,
                            last_turn=completion.turn_summary,
                            verification=last_verification_summary,
                            started_at=started_at,
                            finished_at=datetime.now(timezone.utc),
                        )
                        await self._emit_event(
                            FinishEvent(
                                task_id=task_id,
                                owner_id=owner_id,
                                agent_name=self.name,
                                status=RunStatus.COMPLETED,
                                output=result.output,
                                turn_count=result.turn_count,
                                usage=result.usage,
                            ),
                            agent_ctx,
                            execution_ctx,
                        )
                        return
            except _RunFailureError as exc:
                error = RunError.from_exception(exc)
                await self._emit_event(
                    FinishEvent(
                        task_id=task_id,
                        owner_id=owner_id,
                        agent_name=self.name,
                        status=RunStatus.FAILED,
                        run_error=error,
                        turn_count=execution_ctx.last_turn.turn_number
                        if execution_ctx.last_turn
                        else max(agent_ctx.turn_number - 1, 0),
                        usage=execution_ctx.usage,
                    ),
                    agent_ctx,
                    execution_ctx,
                )
            except Exception as exc:
                error = RunError.from_exception(exc)
                await self._emit_event(
                    FinishEvent(
                        task_id=task_id,
                        owner_id=owner_id,
                        agent_name=self.name,
                        status=RunStatus.FAILED,
                        run_error=error,
                        turn_count=execution_ctx.last_turn.turn_number
                        if execution_ctx.last_turn
                        else max(agent_ctx.turn_number - 1, 0),
                        usage=execution_ctx.usage,
                    ),
                    agent_ctx,
                    execution_ctx,
                )
            finally:
                try:
                    await execution_ctx.resources.destroy_all()
                except Exception:
                    logger.exception("Failed to destroy runtime resources for stream()")
                execution_context.reset(token)
                await queue.put(None)

        runner = asyncio.create_task(_runner())
        try:
            while True:
                item = await queue.get()
                if item is None:
                    break
                yield item
            await runner
        finally:
            if not runner.done():
                runner.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await runner


class Agent(
    BaseAgent[AgentContext[StateT, MetadataT]],
    Generic[StateT, MetadataT],
):
    pass


__all__ = [
    "Agent",
    "BaseAgent",
    "Callbacks",
    "Turn",
    "TurnCompletion",
    "all_of",
    "any_of",
    "chain_prepare_turn",
    "no_tool_calls",
    "retry",
    "stop",
    "tool_called",
    "total_tokens_exceed",
    "turn_count_is",
    "verify",
]
