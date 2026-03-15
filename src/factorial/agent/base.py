from __future__ import annotations

import asyncio
import contextlib
import hashlib
import inspect
import json
import random
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from functools import wraps
from typing import (
    Annotated,
    Any,
    Generic,
    Literal,
    cast,
    get_args,
    get_origin,
    overload,
)

import httpx
from openai.types.chat import (
    ChatCompletion,
    ChatCompletionMessageToolCall,
)
from openai.types.chat.chat_completion_message_function_tool_call import (
    ChatCompletionMessageFunctionToolCall,
)
from pydantic import BaseModel, TypeAdapter
from typing_extensions import TypeVar

from factorial.agent.context import (
    AgentContext,
    ContextType,
    EmptyMetadata,
    EmptyState,
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
    tool_result,
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
    RETRYABLE_EXCEPTIONS,
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
from factorial.core.utils import is_valid_task_id, serialize_data, to_snake_case
from factorial.execution.context import (
    ExecutionContext,
    execution_context,
)
from factorial.execution.hooks import (
    HookRequestContext,
    HookSessionNode,
    HookSessionRecord,
    PendingHook,
    build_request_builder_kwargs,
)
from factorial.execution.tools import (
    ToolDefinition,
    _ToolResultInternal,
    convert_tools_list,
    serialize_for_client,
    serialize_for_model,
)
from factorial.execution.waits import WaitInstruction

logger = get_logger(__name__)

T = TypeVar("T")
StateT = TypeVar("StateT")
MetadataT = TypeVar("MetadataT", default=EmptyMetadata)
VerificationMetaT = TypeVar("VerificationMetaT")

ToolChoice = str | dict[str, Any] | None


async def _invoke_callable_non_blocking(
    fn: Callable[..., Any],
    *args: Any,
    **kwargs: Any,
) -> Any:
    if asyncio.iscoroutinefunction(fn):
        async_fn = cast(Callable[..., Awaitable[Any]], fn)
        return await async_fn(*args, **kwargs)
    return await asyncio.to_thread(fn, *args, **kwargs)


@overload
def retry(
    func: Callable[..., Awaitable[T]],
) -> Callable[..., Awaitable[T]]: ...


@overload
def retry(
    func: None = None,
    *,
    max_attempts: int = 3,
    delay: float = 1.0,
    max_delay: float = 60.0,
    exponential_base: float = 2.0,
    jitter: bool = True,
) -> Callable[[Callable[..., Awaitable[T]]], Callable[..., Awaitable[T]]]: ...


def retry(
    func: Callable[..., Awaitable[T]] | None = None,
    *,
    max_attempts: int = 3,
    delay: float = 1.0,
    max_delay: float = 60.0,
    exponential_base: float = 2.0,
    jitter: bool = True,
) -> (
    Callable[..., Awaitable[T]]
    | Callable[[Callable[..., Awaitable[T]]], Callable[..., Awaitable[T]]]
):
    def _create_retry_decorator(
        the_func: Callable[..., Awaitable[T]],
    ) -> Callable[..., Awaitable[T]]:
        sig = inspect.signature(the_func)

        ctx_param_name: str | None = None
        ctx_pos: int | None = None
        for idx, param in enumerate(sig.parameters.values()):
            ann = param.annotation
            if (
                ann is not inspect.Parameter.empty
                and isinstance(ann, type)
                and issubclass(ann, AgentContext)
            ):
                ctx_param_name = param.name
                ctx_pos = idx - 1
                break

        if ctx_param_name is None and "agent_ctx" in sig.parameters:
            ctx_param_name = "agent_ctx"
            ctx_pos = list(sig.parameters).index("agent_ctx") - 1

        @wraps(the_func)
        async def wrapper(self: BaseAgent[Any], *args: Any, **kwargs: Any) -> T:
            if max_attempts <= 0:
                raise ValueError("max_attempts must be greater than 0")

            agent_ctx_obj: Any | None = None
            if ctx_param_name is not None:
                if ctx_param_name in kwargs:
                    agent_ctx_obj = kwargs[ctx_param_name]
                elif ctx_pos is not None and ctx_pos < len(args):
                    agent_ctx_obj = args[ctx_pos]

            last_exception: Exception | None = None
            for attempt_index in range(max_attempts):
                if isinstance(agent_ctx_obj, AgentContext):
                    agent_ctx_obj.attempt_number = attempt_index + 1

                try:
                    return await the_func(self, *args, **kwargs)
                except Exception as exc:
                    if isinstance(exc, RETRYABLE_EXCEPTIONS):
                        last_exception = exc
                        if attempt_index < max_attempts - 1:
                            backoff_delay = min(
                                delay * (exponential_base**attempt_index),
                                max_delay,
                            )
                            if jitter:
                                backoff_delay *= random.uniform(0.5, 1.5)
                            await asyncio.sleep(backoff_delay)
                            continue
                    raise

            raise last_exception or RuntimeError("Retry failed unexpectedly")

        return wrapper

    if func is not None:
        return _create_retry_decorator(func)
    return _create_retry_decorator


@dataclass
class Turn(Generic[ContextType]):
    model: Model
    messages: list[Message]
    tools: list[ToolDefinition[ContextType]]
    tool_choice: ToolChoice = "auto"
    parallel_tool_calls: bool | None = None
    temperature: float | None = None
    max_output_tokens: int | None = None


@dataclass
class TurnCompletion(Generic[ContextType]):
    is_done: bool
    context: ContextType
    output: Any = None
    tool_call_results: list[tuple[ChatCompletionMessageToolCall, Any | Exception]] = field(
        default_factory=list
    )
    pending_tool_call_ids: list[str] = field(default_factory=list)
    pending_child_task_ids: list[str] = field(default_factory=list)
    finish_reason: str = "continue"
    usage: UsageSummary = field(default_factory=UsageSummary.zero)
    turn_summary: TurnSummary | None = None
    verification_summary: VerificationSummary[Any] | None = None


@dataclass
class ToolExecutionResults:
    new_messages: list[Message]
    tool_call_results: list[tuple[ChatCompletionMessageToolCall, Any | Exception]]
    resolved_results: list[
        tuple[ChatCompletionMessageToolCall, _ToolResultInternal | Exception]
    ]
    pending_tool_call_ids: list[str]
    pending_child_task_ids: list[str]


EventCallback = Callable[..., Awaitable[None] | None]


@dataclass
class Callbacks:
    on_start: EventCallback | None = None
    on_turn_start: EventCallback | None = None
    on_model_start: EventCallback | None = None
    on_model_finish: EventCallback | None = None
    on_tool_start: EventCallback | None = None
    on_tool_finish: EventCallback | None = None
    on_wait: EventCallback | None = None
    on_turn_finish: EventCallback | None = None
    on_finish: EventCallback | None = None


StopWhen = Callable[[AgentContext[Any, Any], ExecutionContext], bool]
PrepareTurnHook = Callable[..., Any]
Verifier = Callable[..., Any]


class StopCondition:
    def __call__(self, agent_ctx: AgentContext[Any, Any], execution_ctx: ExecutionContext) -> bool:
        raise NotImplementedError


@dataclass(frozen=True)
class NoToolCallsCondition(StopCondition):
    def __call__(
        self,
        agent_ctx: AgentContext[Any, Any],
        execution_ctx: ExecutionContext,
    ) -> bool:
        last_turn = execution_ctx.last_turn
        if last_turn is None:
            return False
        return not last_turn.finish_reason.startswith("tool_called:")


@dataclass(frozen=True)
class TurnCountIsCondition(StopCondition):
    limit: int

    def __call__(
        self,
        agent_ctx: AgentContext[Any, Any],
        execution_ctx: ExecutionContext,
    ) -> bool:
        return agent_ctx.turn_number >= self.limit


@dataclass(frozen=True)
class ToolCalledCondition(StopCondition):
    name: str

    def __call__(
        self,
        agent_ctx: AgentContext[Any, Any],
        execution_ctx: ExecutionContext,
    ) -> bool:
        last_turn = execution_ctx.last_turn
        if last_turn is None or not last_turn.finish_reason.startswith("tool_called:"):
            return False
        suffix = last_turn.finish_reason.removeprefix("tool_called:")
        return self.name in {value for value in suffix.split(",") if value}


@dataclass(frozen=True)
class TotalTokensExceedCondition(StopCondition):
    limit: int

    def __call__(
        self,
        agent_ctx: AgentContext[Any, Any],
        execution_ctx: ExecutionContext,
    ) -> bool:
        return execution_ctx.usage.total_tokens > self.limit


@dataclass(frozen=True)
class AnyOfCondition(StopCondition):
    conditions: tuple[StopWhen | StopCondition, ...]

    def __call__(
        self,
        agent_ctx: AgentContext[Any, Any],
        execution_ctx: ExecutionContext,
    ) -> bool:
        return any(
            condition(agent_ctx, execution_ctx) for condition in self.conditions
        )


@dataclass(frozen=True)
class AllOfCondition(StopCondition):
    conditions: tuple[StopWhen | StopCondition, ...]

    def __call__(
        self,
        agent_ctx: AgentContext[Any, Any],
        execution_ctx: ExecutionContext,
    ) -> bool:
        return all(
            condition(agent_ctx, execution_ctx) for condition in self.conditions
        )


class stop:
    @staticmethod
    def no_tool_calls() -> StopCondition:
        return NoToolCallsCondition()

    @staticmethod
    def turn_count_is(limit: int) -> StopCondition:
        return TurnCountIsCondition(limit=limit)

    @staticmethod
    def tool_called(name: str) -> StopCondition:
        return ToolCalledCondition(name=name)

    @staticmethod
    def total_tokens_exceed(limit: int) -> StopCondition:
        return TotalTokensExceedCondition(limit=limit)

    @staticmethod
    def any_of(*conditions: StopWhen | StopCondition) -> StopCondition:
        return AnyOfCondition(conditions=conditions)

    @staticmethod
    def all_of(*conditions: StopWhen | StopCondition) -> StopCondition:
        return AllOfCondition(conditions=conditions)


def no_tool_calls() -> StopCondition:
    return stop.no_tool_calls()


def turn_count_is(limit: int) -> StopCondition:
    return stop.turn_count_is(limit)


def tool_called(name: str) -> StopCondition:
    return stop.tool_called(name)


def total_tokens_exceed(limit: int) -> StopCondition:
    return stop.total_tokens_exceed(limit)


def any_of(*conditions: StopWhen | StopCondition) -> StopCondition:
    return stop.any_of(*conditions)


def all_of(*conditions: StopWhen | StopCondition) -> StopCondition:
    return stop.all_of(*conditions)


async def _maybe_call_prepare_turn(
    func: PrepareTurnHook | None,
    turn: Turn[Any],
    agent_ctx: AgentContext[Any, Any],
    execution_ctx: ExecutionContext,
) -> Turn[Any]:
    if func is None:
        return turn
    signature = inspect.signature(func)
    params = list(signature.parameters.values())
    if not params:
        raise TypeError("prepare_turn must accept at least one argument for turn")

    first = params[0]
    if first.kind not in (
        inspect.Parameter.POSITIONAL_ONLY,
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
    ):
        raise TypeError("prepare_turn must accept the turn as its first parameter")

    args: list[Any] = [turn]
    kwargs: dict[str, Any] = {}
    for param in params[1:]:
        if param.kind in (
            inspect.Parameter.VAR_POSITIONAL,
            inspect.Parameter.VAR_KEYWORD,
        ):
            continue

        annotation = param.annotation
        annotation_origin = get_origin(annotation)
        injected_value: Any | None = None
        if (
            param.name == "agent_ctx"
            or annotation is AgentContext
            or annotation_origin is AgentContext
        ):
            injected_value = agent_ctx
        elif (
            param.name == "execution_ctx"
            or annotation is ExecutionContext
            or annotation_origin is ExecutionContext
        ):
            injected_value = execution_ctx
        elif (
            annotation is not inspect.Parameter.empty
            and isinstance(annotation, type)
            and issubclass(annotation, AgentContext)
        ):
            injected_value = agent_ctx
        elif (
            annotation is not inspect.Parameter.empty
            and isinstance(annotation, type)
            and issubclass(annotation, ExecutionContext)
        ):
            injected_value = execution_ctx
        elif param.default is not inspect.Parameter.empty:
            continue
        else:
            raise TypeError(
                f"Unsupported required prepare_turn parameter '{param.name}'. "
                "Only turn (first arg), agent_ctx, and execution_ctx are supported."
            )

        if param.kind in (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
        ):
            args.append(injected_value)
        else:
            kwargs[param.name] = injected_value

    result = func(*args, **kwargs)
    if inspect.isawaitable(result):
        result = await cast(Awaitable[Any], result)
    if isinstance(result, Turn):
        return result
    return turn


def chain_prepare_turn(
    *functions: PrepareTurnHook,
) -> PrepareTurnHook:
    async def _chained(
        turn: Turn[Any],
        agent_ctx: AgentContext[Any, Any],
        execution_ctx: ExecutionContext,
    ) -> Turn[Any]:
        current = turn
        for function in functions:
            current = await _maybe_call_prepare_turn(
                function,
                current,
                agent_ctx,
                execution_ctx,
            )
        return current

    return _chained


def _infer_turn_limit_hint(
    condition: StopWhen | StopCondition | None,
) -> int | None:
    if condition is None:
        return None
    if isinstance(condition, TurnCountIsCondition):
        return condition.limit
    if isinstance(condition, AnyOfCondition):
        limits = [
            limit
            for limit in (
                _infer_turn_limit_hint(child) for child in condition.conditions
            )
            if limit is not None
        ]
        return min(limits) if limits else None
    if isinstance(condition, AllOfCondition):
        limits = [
            limit
            for limit in (
                _infer_turn_limit_hint(child) for child in condition.conditions
            )
            if limit is not None
        ]
        return max(limits) if limits else None
    return None


class _DirectEventPublisher:
    def __init__(self, sink: Callable[[BaseEvent], Awaitable[None]] | None = None):
        self._sink = sink

    async def publish_event(self, event: BaseEvent) -> None:
        if self._sink is not None:
            await self._sink(event)


class _RunFailureError(FatalAgentError):
    def __init__(
        self,
        message: str,
        *,
        verification_summary: VerificationSummary[Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.verification_summary = verification_summary


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
        parse_tool_args: bool = True,
    ):
        self.name = to_snake_case(name or self.__class__.__name__)
        self.description = description or self.__class__.__name__
        self.instructions = instructions
        self.tools, self.tool_actions = convert_tools_list(tools or [])
        self.http_client = http_client or httpx.AsyncClient(timeout=request_timeout)
        self.client = client or MultiClient(http_client=self.http_client)
        self.request_timeout = request_timeout
        self.model = model
        self.default_tool_choice = tool_choice
        self.default_parallel_tool_calls = parallel_tool_calls
        self.default_temperature = temperature
        self.default_max_output_tokens = max_output_tokens
        self.prepare_turn = prepare_turn
        self.stop_when = stop_when or stop.any_of(
            stop.no_tool_calls(),
            stop.turn_count_is(10),
        )
        self.verifier = verifier
        self.callbacks = callbacks or Callbacks()
        self.parse_tool_args = parse_tool_args
        self.max_turns = _infer_turn_limit_hint(self.stop_when)

        if self.model is None:
            raise ValueError("model is required")

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
        input: str | list[MessageLike],
        *,
        state: Any = None,
        metadata: Any = None,
    ) -> AgentContext[Any, Any]:
        state_type, metadata_type = self._resolve_state_and_metadata_types()
        messages = normalize_messages_input(input)
        if self.instructions:
            messages = [system(self.instructions), *messages]

        return AgentContext(
            messages=messages,
            state=self._coerce_typed_payload(state, state_type, label="state"),
            metadata=self._coerce_typed_payload(
                metadata,
                metadata_type,
                label="metadata",
            ),
        )

    def context_from_dict(self, data: dict[str, Any]) -> AgentContext[Any, Any]:
        state_type, metadata_type = self._resolve_state_and_metadata_types()
        return AgentContext.from_dict(
            data,
            state_type=state_type,
            metadata_type=metadata_type,
        )

    def context_from_json(self, json_str: str) -> AgentContext[Any, Any]:
        state_type, metadata_type = self._resolve_state_and_metadata_types()
        return AgentContext.from_json(
            json_str,
            state_type=state_type,
            metadata_type=metadata_type,
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

    def resolve_tools(self, agent_ctx: ContextType) -> list[ToolDefinition[ContextType]]:
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

    @staticmethod
    def _stringify_for_model(value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, str):
            return value
        serialized = serialize_data(value)
        if isinstance(serialized, str):
            return serialized
        try:
            return json.dumps(serialized, ensure_ascii=False, sort_keys=True)
        except (TypeError, ValueError):
            return str(serialized)

    @classmethod
    def _wait_model_output(cls, wait_instr: WaitInstruction) -> str:
        if wait_instr.data is not None:
            if isinstance(wait_instr.data, BaseModel):
                return cls._stringify_for_model(serialize_for_model(wait_instr.data))
            return cls._stringify_for_model(wait_instr.data)
        if wait_instr.kind == "sleep":
            return f"Waiting for {wait_instr.sleep_s or 0}s"
        if wait_instr.kind == "cron":
            expr = wait_instr.cron or "<cron>"
            tz = wait_instr.timezone or "UTC"
            return f"Waiting for next cron tick '{expr}' ({tz})"
        if wait_instr.kind == "jobs":
            return "Waiting for spawned jobs"
        if wait_instr.kind == "activity":
            return "Waiting for activity"
        if wait_instr.kind == "signal":
            return "Waiting for signal"
        return "Waiting"

    def _normalize_tool_result(
        self,
        result: Any,
        tool_call: ChatCompletionMessageToolCall,
        pending_child_task_ids: list[str] | None = None,
    ) -> _ToolResultInternal:
        if isinstance(result, _ToolResultInternal):
            result.tool_call = tool_call
            if pending_child_task_ids:
                existing = result.pending_child_task_ids or []
                result.pending_child_task_ids = list(
                    dict.fromkeys([*existing, *pending_child_task_ids])
                )
            return result

        if isinstance(result, WaitInstruction):
            return _ToolResultInternal(
                tool_call=tool_call,
                model_output=self._wait_model_output(result),
                client_output=result,
                pending_child_task_ids=pending_child_task_ids,
            )

        if isinstance(result, BaseModel):
            return _ToolResultInternal(
                tool_call=tool_call,
                model_output=self._stringify_for_model(serialize_for_model(result)),
                client_output=serialize_for_client(result),
                pending_child_task_ids=pending_child_task_ids,
            )

        if result is None:
            return _ToolResultInternal(
                tool_call=tool_call,
                model_output="",
                client_output=None,
                pending_child_task_ids=pending_child_task_ids,
            )

        return _ToolResultInternal(
            tool_call=tool_call,
            model_output=self._stringify_for_model(result),
            client_output=result,
            pending_child_task_ids=pending_child_task_ids,
        )

    async def tool_action(
        self,
        tool_call: ChatCompletionMessageToolCall,
        agent_ctx: ContextType,
    ) -> _ToolResultInternal:
        tool_name = tool_call.function.name
        tool_args = tool_call.function.arguments
        action = self.tool_actions.get(tool_name)
        tool_def = next((tool for tool in self.tools if tool.name == tool_name), None)
        hook_plan = tool_def.hook_plan if tool_def else None

        execution_ctx = ExecutionContext.current()

        if action is None:
            raise ValueError(f"Agent {self.name} has no tool action for {tool_name}")

        is_forking_tool = getattr(action, "forking_tool", False)

        if not self.parse_tool_args:
            result = await _invoke_callable_non_blocking(action, tool_args, agent_ctx)
        else:
            raw_tool_args = json.loads(tool_args)
            parsed_tool_args = dict(raw_tool_args)

            for param_name, param in inspect.signature(action).parameters.items():
                if param_name in parsed_tool_args:
                    continue
                if param_name == "agent_ctx":
                    parsed_tool_args[param_name] = agent_ctx
                    continue
                if (
                    param.annotation
                    and param.annotation is not inspect.Parameter.empty
                    and isinstance(param.annotation, type)
                    and issubclass(param.annotation, AgentContext)
                ):
                    parsed_tool_args[param_name] = agent_ctx
                    continue
                if param_name == "execution_ctx":
                    parsed_tool_args[param_name] = execution_ctx
                    continue
                if (
                    param.annotation
                    and param.annotation is not inspect.Parameter.empty
                    and isinstance(param.annotation, type)
                    and issubclass(param.annotation, ExecutionContext)
                ):
                    parsed_tool_args[param_name] = execution_ctx
                    continue

            for param_name, param in inspect.signature(action).parameters.items():
                if param_name not in parsed_tool_args:
                    continue
                expected = param.annotation
                if expected is inspect.Parameter.empty:
                    continue

                try:
                    if get_origin(expected) is Annotated:
                        annotated_args = get_args(expected)
                        if annotated_args:
                            expected = annotated_args[0]
                    origin = get_origin(expected)
                    if (
                        isinstance(expected, type)
                        and issubclass(expected, BaseModel)
                        and isinstance(parsed_tool_args[param_name], dict)
                    ):
                        parsed_tool_args[param_name] = expected(
                            **parsed_tool_args[param_name]
                        )
                    elif origin is list:
                        item_type = get_args(expected)[0] if get_args(expected) else None
                        if (
                            item_type
                            and isinstance(item_type, type)
                            and issubclass(item_type, BaseModel)
                            and isinstance(parsed_tool_args[param_name], list)
                        ):
                            parsed_tool_args[param_name] = [
                                item_type(**item)
                                if not isinstance(item, item_type)
                                else item
                                for item in parsed_tool_args[param_name]
                            ]
                except Exception as exc:
                    logger.debug(
                        "Failed to coerce argument '%s' to %s: %s",
                        param_name,
                        expected,
                        exc,
                    )

            if hook_plan is not None:
                hook_param_names = list(hook_plan.hook_order)
                present_hook_params = [
                    param_name
                    for param_name in hook_param_names
                    if param_name in parsed_tool_args
                ]
                if present_hook_params and len(present_hook_params) != len(
                    hook_param_names
                ):
                    raise ValueError(
                        f"Tool '{tool_name}' continuation received partial hook "
                        f"payloads: {present_hook_params}. Expected all of "
                        f"{hook_param_names}."
                    )

                if not present_hook_params:
                    request_tool_args = {
                        key: parsed_tool_args[key]
                        for key in raw_tool_args.keys()
                        if key in parsed_tool_args
                    }
                    serialized_tool_args = cast(
                        dict[str, Any], serialize_data(request_tool_args)
                    )
                    now_ts = datetime.now(timezone.utc).timestamp()
                    session_seed = f"{execution_ctx.task_id}:{tool_call.id}:{tool_name}"
                    session_id = hashlib.sha256(
                        session_seed.encode("utf-8")
                    ).hexdigest()
                    session_nodes: dict[str, HookSessionNode] = {}
                    for hook_param_name in hook_plan.hook_order:
                        node_spec = hook_plan.nodes[hook_param_name]
                        session_nodes[hook_param_name] = HookSessionNode(
                            param_name=hook_param_name,
                            mode=node_spec.mode,
                            hook_type=node_spec.hook_type.__name__,
                            depends_on=node_spec.depends_on,
                        )

                    first_stage = hook_plan.stages[0] if hook_plan.stages else ()
                    if not first_stage:
                        raise ValueError(
                            f"Hook plan for tool '{tool_name}' has no requestable stage."
                        )

                    request_ctx = HookRequestContext(
                        task_id=execution_ctx.task_id,
                        owner_id=execution_ctx.owner_id,
                        agent_name=self.name,
                        tool_name=tool_name,
                        tool_call_id=tool_call.id,
                        args=serialized_tool_args,
                    )
                    requested_hooks: list[dict[str, Any]] = []
                    for hook_param_name in first_stage:
                        node_spec = hook_plan.nodes[hook_param_name]
                        request_kwargs = build_request_builder_kwargs(
                            request_builder=node_spec.request_builder,
                            request_ctx=request_ctx,
                            tool_args=request_tool_args,
                            resolved_hook_payloads={},
                        )
                        pending_hook = await _invoke_callable_non_blocking(
                            node_spec.request_builder,
                            **request_kwargs,
                        )
                        if not isinstance(pending_hook, PendingHook):
                            raise TypeError(
                                f"Hook request builder for '{hook_param_name}' must "
                                "return PendingHook[...]"
                            )

                        stable_hook_id = f"{session_id}:{hook_param_name}"
                        pending_hook.hook_id = stable_hook_id

                        node_state = session_nodes[hook_param_name]
                        node_state.status = "requested"
                        node_state.hook_id = stable_hook_id
                        node_state.requested_at = now_ts

                        requested_hooks.append(
                            {
                                "param_name": hook_param_name,
                                "mode": node_spec.mode,
                                "hook_type": node_spec.hook_type.__name__,
                                "depends_on": list(node_spec.depends_on),
                                "hook_id": stable_hook_id,
                                "submit_url": pending_hook.submit_url,
                                "token": pending_hook.token,
                                "expires_at": pending_hook.expires_at.timestamp(),
                                "title": pending_hook.title,
                                "metadata": pending_hook.metadata,
                            }
                        )

                    session = HookSessionRecord(
                        session_id=session_id,
                        task_id=execution_ctx.task_id,
                        tool_call_id=tool_call.id,
                        tool_name=tool_name,
                        tool_args=serialized_tool_args,
                        nodes=session_nodes,
                        status="active",
                        created_at=now_ts,
                        updated_at=now_ts,
                    )

                    await execution_ctx.persist_hook_session(
                        {
                            "kind": "hook_session_init",
                            "session": session.to_dict(),
                            "requested_hooks": requested_hooks,
                        }
                    )
                    return _ToolResultInternal(
                        tool_call=tool_call,
                        model_output="Awaiting hook dependency resolution",
                        client_output={
                            "kind": "hook_session_pending",
                            "session_id": session_id,
                            "requested_hooks": requested_hooks,
                        },
                        pending_result=True,
                    )

            result = await _invoke_callable_non_blocking(action, **parsed_tool_args)

        pending_child_task_ids: list[str] = []
        if is_forking_tool:
            candidate_ids: list[str] | tuple[str, ...] | None
            if isinstance(result, _ToolResultInternal):
                if not result.pending_child_task_ids:
                    raise ValueError(
                        f"Forking tool '{tool_call.function.name}' returned an "
                        "_ToolResultInternal without pending_child_task_ids."
                    )
                candidate_ids = result.pending_child_task_ids
            elif isinstance(result, (list, tuple)) and all(
                isinstance(item, str) for item in result
            ):
                candidate_ids = result
            else:
                raise ValueError(
                    f"Forking tool '{tool_call.function.name}' must return "
                    "list[str] or tuple[str, ...] of task IDs."
                )

            if not all(
                isinstance(item, str) and is_valid_task_id(item)
                for item in candidate_ids
            ):
                raise ValueError(
                    f"Forking tool '{tool_call.function.name}' "
                    f"returned invalid task IDs: {candidate_ids}"
                )
            pending_child_task_ids = list(candidate_ids)

        return self._normalize_tool_result(
            result,
            tool_call,
            pending_child_task_ids or None,
        )

    @retry(max_attempts=3, delay=0.5)
    async def _tool_action_with_retry(
        self,
        tool_call: ChatCompletionMessageToolCall,
        agent_ctx: ContextType,
    ) -> _ToolResultInternal:
        return await self.tool_action(tool_call, agent_ctx)

    async def execute_tools(
        self,
        tool_calls: list[ChatCompletionMessageToolCall],
        agent_ctx: ContextType,
    ) -> ToolExecutionResults:
        new_messages: list[Message] = []
        pending_tool_call_ids: list[str] = []
        all_pending_child_task_ids: list[str] = []
        tool_call_results: list[tuple[ChatCompletionMessageToolCall, Any | Exception]] = []
        resolved_results: list[
            tuple[ChatCompletionMessageToolCall, _ToolResultInternal | Exception]
        ] = []
        execution_ctx = ExecutionContext.current()

        for tool_call in tool_calls:
            await self._emit_event(
                ToolStartEvent(
                    task_id=execution_ctx.task_id,
                    owner_id=execution_ctx.owner_id,
                    agent_name=self.name,
                    turn=agent_ctx.turn_number,
                    tool_name=tool_call.function.name,
                    tool_call_id=tool_call.id,
                ),
                agent_ctx,
                execution_ctx,
            )

        results = await asyncio.gather(
            *[
                self._tool_action_with_retry(tool_call, agent_ctx)
                for tool_call in tool_calls
            ],
            return_exceptions=True,
        )

        for tool_call, result in zip(tool_calls, results, strict=True):
            resolved_results.append(
                (
                    tool_call,
                    cast(_ToolResultInternal | Exception, result),
                )
            )
            tool_call_results.append(
                (
                    tool_call,
                    result
                    if isinstance(result, Exception)
                    else result.client_output,
                )
            )

            if (
                isinstance(result, _ToolResultInternal)
                and result.pending_child_task_ids
            ):
                all_pending_child_task_ids.extend(result.pending_child_task_ids)

            if isinstance(result, Exception):
                logger.error(
                    "Tool %s failed: %s",
                    tool_call.function.name,
                    result,
                    exc_info=result,
                )
                new_messages.append(
                    tool_result(
                        tool_call.id,
                        str(result),
                        tool_name=tool_call.function.name,
                        is_error=True,
                        model_output=str(result),
                    )
                )
                await self._emit_event(
                    ToolFinishEvent(
                        task_id=execution_ctx.task_id,
                        owner_id=execution_ctx.owner_id,
                        agent_name=self.name,
                        turn=agent_ctx.turn_number,
                        tool_name=tool_call.function.name,
                        tool_call_id=tool_call.id,
                        output=str(result),
                        is_error=True,
                    ),
                    agent_ctx,
                    execution_ctx,
                )
            elif result.pending_result:
                pending_tool_call_ids.append(tool_call.id)
                await self._emit_event(
                    ToolFinishEvent(
                        task_id=execution_ctx.task_id,
                        owner_id=execution_ctx.owner_id,
                        agent_name=self.name,
                        turn=agent_ctx.turn_number,
                        tool_name=tool_call.function.name,
                        tool_call_id=tool_call.id,
                        output=result.client_output,
                        is_error=False,
                    ),
                    agent_ctx,
                    execution_ctx,
                )
            else:
                new_messages.append(
                    tool_result(
                        tool_call.id,
                        result.client_output,
                        tool_name=tool_call.function.name,
                        is_error=False,
                        model_output=result.model_output,
                    )
                )
                await self._emit_event(
                    ToolFinishEvent(
                        task_id=execution_ctx.task_id,
                        owner_id=execution_ctx.owner_id,
                        agent_name=self.name,
                        turn=agent_ctx.turn_number,
                        tool_name=tool_call.function.name,
                        tool_call_id=tool_call.id,
                        output=result.client_output,
                        is_error=False,
                    ),
                    agent_ctx,
                    execution_ctx,
                )

            if isinstance(result, FatalAgentError):
                raise result

        return ToolExecutionResults(
            new_messages=new_messages,
            tool_call_results=tool_call_results,
            resolved_results=resolved_results,
            pending_tool_call_ids=pending_tool_call_ids,
            pending_child_task_ids=all_pending_child_task_ids,
        )

    def process_deferred_tool_results(
        self,
        agent_ctx: ContextType,
        tool_call_results: list[tuple[str, Any]],
    ) -> TurnCompletion[ContextType]:
        updated_messages = list(agent_ctx.messages)
        for tool_call_id, result in tool_call_results:
            if isinstance(result, Exception):
                updated_messages.append(
                    tool_result(
                        tool_call_id,
                        str(result),
                        is_error=True,
                        model_output=str(result),
                    )
                )
            else:
                updated_messages.append(
                    tool_result(
                        tool_call_id,
                        result,
                        model_output=self._stringify_for_model(result),
                    )
                )
        agent_ctx.messages = updated_messages
        return TurnCompletion(is_done=False, context=agent_ctx)

    def format_child_task_result(
        self,
        child_task_id: str,
        result: Any | Exception,
    ) -> str:
        if isinstance(result, Exception):
            return (
                f'<sub_task_error sub_task_id="{child_task_id}">\n'
                f"Error running sub task:\n{result}\n</sub_task_error>"
            )
        return (
            f'<sub_task_result sub_task_id="{child_task_id}">\n'
            f"{str(result)}\n</sub_task_result>"
        )

    def process_child_task_results(
        self,
        agent_ctx: ContextType,
        child_task_results: list[tuple[str, Any]],
    ) -> TurnCompletion[ContextType]:
        updated_messages = list(agent_ctx.messages)
        formatted_results = [
            self.format_child_task_result(child_task_id, result)
            for child_task_id, result in child_task_results
        ]
        if formatted_results:
            updated_messages.append(
                assistant(
                    "<sub_task_results>\n"
                    + "\n\n".join(formatted_results)
                    + "\n</sub_task_results>"
                )
            )
        agent_ctx.messages = updated_messages
        return TurnCompletion(is_done=False, context=agent_ctx)

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
                            json.loads(tool_call.function.arguments)
                            if self.parse_tool_args
                            else tool_call.function.arguments,
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
            tuple[ChatCompletionMessageToolCall, _ToolResultInternal | Exception]
        ],
    ) -> Any:
        if assistant_content or not resolved_tool_results:
            return assistant_content

        for tool_call, result in resolved_tool_results:
            del tool_call
            if isinstance(result, Exception) or result.pending_result:
                continue
            return result.client_output
        return None

    def _turn_finish_reason(
        self,
        response: ChatCompletion,
        function_tool_calls: list[ChatCompletionMessageFunctionToolCall],
    ) -> str:
        if function_tool_calls:
            tool_names = ",".join(tool_call.function.name for tool_call in function_tool_calls)
            return f"tool_called:{tool_names}"
        return str(response.choices[0].finish_reason or "stop")

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

    def _resolve_verifier_injected_kwargs(
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

        kwargs: dict[str, Any] = {}
        for param in params[1:]:
            if param.kind in (
                inspect.Parameter.VAR_POSITIONAL,
                inspect.Parameter.VAR_KEYWORD,
            ):
                continue
            annotation = param.annotation
            if param.name == "agent_ctx":
                kwargs[param.name] = agent_ctx
                continue
            if (
                annotation is not inspect.Parameter.empty
                and isinstance(annotation, type)
                and issubclass(annotation, AgentContext)
            ):
                kwargs[param.name] = agent_ctx
                continue
            if param.name == "execution_ctx":
                kwargs[param.name] = execution_ctx
                continue
            if (
                annotation is not inspect.Parameter.empty
                and isinstance(annotation, type)
                and issubclass(annotation, ExecutionContext)
            ):
                kwargs[param.name] = execution_ctx
                continue
            if param.default is inspect.Parameter.empty:
                raise TypeError(
                    f"Unsupported required verifier parameter '{param.name}'. "
                    "Only output (first arg), agent_ctx, and execution_ctx are supported."
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

        decision = await _invoke_callable_non_blocking(
            self.verifier,
            candidate_output,
            **self._resolve_verifier_injected_kwargs(agent_ctx, execution_ctx),
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

        canonical_response_messages, function_tool_calls = self._canonical_response_messages(
            response
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
            tool_results = await self.execute_tools(function_tool_calls, agent_ctx)
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

        if tool_results.pending_child_task_ids:
            wait_event = WaitEvent(
                task_id=execution_ctx.task_id,
                owner_id=execution_ctx.owner_id,
                agent_name=self.name,
                turn=agent_ctx.turn_number,
                wait_kind="pending_child_task_results",
                source_tool_call_ids=tuple(tool_results.pending_child_task_ids),
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
                        pending_child_task_ids=tuple(tool_results.pending_child_task_ids),
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
                pending_child_task_ids=tuple(tool_results.pending_child_task_ids),
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
        agent_ctx.messages.extend(normalize_message(cast(MessageLike, message)) for message in messages)
        return agent_ctx

    async def cancel(
        self,
        agent_ctx: ContextType,
        execution_ctx: ExecutionContext,
    ) -> None:
        del agent_ctx, execution_ctx

    async def run(
        self,
        input: str | list[MessageLike],
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
            retry_count=0,
            events=cast(EventPublisher, _DirectEventPublisher()),
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
                if completion.pending_tool_call_ids or completion.pending_child_task_ids:
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
                        turn_count=completion.turn_summary.turn_number if completion.turn_summary else agent_ctx.turn_number,
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
                turn_count=execution_ctx.last_turn.turn_number if execution_ctx.last_turn else max(agent_ctx.turn_number - 1, 0),
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
                turn_count=execution_ctx.last_turn.turn_number if execution_ctx.last_turn else max(agent_ctx.turn_number - 1, 0),
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
            execution_context.reset(token)

    async def stream(
        self,
        input: str | list[MessageLike],
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
                retry_count=0,
                events=cast(EventPublisher, _DirectEventPublisher(sink)),
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
                    if completion.pending_tool_call_ids or completion.pending_child_task_ids:
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
                            turn_count=completion.turn_summary.turn_number if completion.turn_summary else agent_ctx.turn_number,
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
                        turn_count=execution_ctx.last_turn.turn_number if execution_ctx.last_turn else max(agent_ctx.turn_number - 1, 0),
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
                        turn_count=execution_ctx.last_turn.turn_number if execution_ctx.last_turn else max(agent_ctx.turn_number - 1, 0),
                        usage=execution_ctx.usage,
                    ),
                    agent_ctx,
                    execution_ctx,
                )
            finally:
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

    async def execute(
        self,
        agent_ctx: ContextType,
        execution_ctx: ExecutionContext,
    ) -> TurnCompletion[ContextType]:
        token = execution_context.set(execution_ctx)
        try:
            return await self.run_turn(agent_ctx)
        finally:
            execution_context.reset(token)

    def get_execution_context(self) -> ExecutionContext:
        return ExecutionContext.current()


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
