from __future__ import annotations
import random
from typing import (
    Any,
    Callable,
    Awaitable,
    cast,
    final,
    Generic,
    overload,
    TypeVar,
    get_origin,
    get_args,
)
from functools import wraps
from dataclasses import dataclass, field
from openai import AsyncStream
from openai.types.chat.chat_completion import Choice
from pydantic import BaseModel
import json
import asyncio
import inspect
import httpx
from openai.types.chat import (
    ChatCompletion,
    ChatCompletionChunk,
    ChatCompletionMessageToolCall,
    ChatCompletionMessage,
)
from datetime import datetime

from factorial.context import (
    AgentContext,
    ExecutionContext,
    execution_context,
    ContextType,
)
from factorial.llms import Model, MultiClient
from factorial.utils import (
    serialize_data,
    to_snake_case,
    is_valid_task_id,
    validate_callback_signature as _vcs,
)
from factorial.events import EventPublisher, AgentEvent
from factorial.logging import get_logger
from factorial.exceptions import RETRYABLE_EXCEPTIONS, FatalAgentError
from factorial.tools import (
    FunctionTool,
    FunctionToolActionResult,
    convert_tools_list,
    create_final_output_tool,
    function_tool,
)

logger = get_logger(__name__)

T = TypeVar("T")


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
    """Decorator to add retry logic to agent methods.

    Can be used as a decorator with or without arguments:

    @retry
    async def my_method(self, ...):
        ...

    @retry(max_attempts=5, delay=2.0)
    async def my_method(self, ...):
        ...
    """

    def _create_retry_decorator(
        the_func: Callable[..., Awaitable[T]],
    ) -> Callable[..., Awaitable[T]]:
        @wraps(the_func)
        async def wrapper(self: "BaseAgent[Any]", *args: Any, **kwargs: Any) -> T:
            if max_attempts <= 0:
                raise ValueError("max_attempts must be greater than 0")

            last_exception: Exception | None = None
            for attempt in range(max_attempts):
                try:
                    return await the_func(self, *args, **kwargs)
                except Exception as e:
                    if isinstance(e, RETRYABLE_EXCEPTIONS):
                        last_exception = e
                        if attempt < max_attempts - 1:
                            backoff_delay = min(
                                delay * (exponential_base**attempt), max_delay
                            )
                            if jitter:
                                jitter_factor = random.uniform(0.5, 1.5)
                                backoff_delay *= jitter_factor

                            await asyncio.sleep(backoff_delay)
                    else:
                        raise e

            raise last_exception or Exception("Retry failed")

        return wrapper

    # If func is provided, we were used as @retry (no parentheses)
    if func is not None:
        return _create_retry_decorator(func)

    # Otherwise, we were used as @retry(...), so return a decorator
    return _create_retry_decorator


@overload
def publish_progress(
    func: Callable[..., Awaitable[T]],
) -> Callable[..., Awaitable[T]]: ...


@overload
def publish_progress(
    func: None = None,
    *,
    func_name: str | None = None,
    include_context: bool = True,
    include_args: bool = True,
    include_result: bool = True,
) -> Callable[[Callable[..., Awaitable[T]]], Callable[..., Awaitable[T]]]: ...


def publish_progress(
    func: Callable[..., Awaitable[T]] | None = None,
    *,
    func_name: str | None = None,
    include_context: bool = True,
    include_args: bool = True,
    include_result: bool = True,
) -> (
    Callable[..., Awaitable[T]]
    | Callable[[Callable[..., Awaitable[T]]], Callable[..., Awaitable[T]]]
):
    """Decorator to publish progress events for agent methods.

    Can be used as a decorator with or without arguments:

    @publish_progress
    async def my_method(self, ...):
        ...

    @publish_progress(func_name="custom_name", include_result=False)
    async def my_method(self, ...):
        ...
    """

    def _create_progress_decorator(
        the_func: Callable[..., Awaitable[T]],
    ) -> Callable[..., Awaitable[T]]:
        actual_func_name = func_name or the_func.__name__

        @wraps(the_func)
        async def wrapper(self: "BaseAgent[Any]", *args: Any, **kwargs: Any) -> T:
            async def publish(
                event_suffix: str,
                data: dict[str, Any] | None = None,
                error: str | None = None,
            ) -> None:
                try:
                    ctx = ExecutionContext.current()

                    event_data: dict[str, Any] = {}
                    if data:
                        event_data.update(data)
                    if include_context and ctx:
                        event_data["context"] = ctx

                    await ctx.events.publish_event(
                        AgentEvent(
                            event_type=f"progress_update_{actual_func_name}_{event_suffix}",
                            task_id=ctx.task_id,
                            owner_id=ctx.owner_id,
                            agent_name=self.name,
                            data=serialize_data(event_data) if event_data else None,
                            error=error,
                        )
                    )
                except Exception:
                    logger.exception("Failed to publish progress update", exc_info=True)
                    pass

            start_data: dict[str, Any] = {}
            if include_args:
                start_data.update(
                    {
                        "args": args,
                        "kwargs": kwargs,
                    }
                )
            await publish("started", start_data)

            try:
                result = await the_func(self, *args, **kwargs)

                completion_data: dict[str, Any] = {}
                if include_args:
                    completion_data.update(
                        {
                            "args": args,
                            "kwargs": kwargs,
                        }
                    )
                if include_result:
                    completion_data["result"] = serialize_data(result)

                await publish("completed", completion_data if completion_data else None)
                return result
            except Exception as e:
                failed_data: dict[str, Any] = {}
                if include_args:
                    failed_data.update(
                        {
                            "args": args,
                            "kwargs": kwargs,
                        }
                    )
                await publish(
                    "failed", failed_data if failed_data else None, error=str(e)
                )
                raise

        return wrapper

    # If func is provided, we were used as @publish_progress (no parentheses)
    if func is not None:
        return _create_progress_decorator(func)

    # Otherwise, we were used as @publish_progress(...), so return a decorator
    return _create_progress_decorator


@dataclass
class TurnCompletion(Generic[ContextType]):
    is_done: bool
    context: ContextType
    output: Any = None
    tool_call_results: list[tuple[ChatCompletionMessageToolCall, Any | Exception]] = (
        field(default_factory=list)
    )
    pending_tool_call_ids: list[str] = field(default_factory=list)
    pending_child_task_ids: list[str] = field(default_factory=list)


@dataclass
class RunCompletion(Generic[ContextType]):
    """Summary object for the *entire* run passed to ``on_run_end``.

    Timestamps are stored instead of a raw *duration* so consumers can
    calculate different metrics, and we expose a convenience ``duration``
    property.
    """

    output: Any | None
    started_at: datetime
    finished_at: datetime
    error: Exception | None = None

    @property
    def duration(self) -> float:
        """Runtime in **seconds**."""

        return (self.finished_at - self.started_at).total_seconds()

    @property
    def succeeded(self) -> bool:
        return self.error is None


@dataclass
class ModelSettings(Generic[ContextType]):
    temperature: float | Callable[[ContextType], float] | None = None
    tool_choice: (
        str
        | dict[str, Any]
        | Callable[[ContextType], str | dict[str, Any] | None]
        | None
    ) = None
    parallel_tool_calls: bool | Callable[[ContextType], bool] | None = None
    max_completion_tokens: int | Callable[[ContextType], int] | None = None

    def resolve(self, agent_ctx: ContextType) -> "ResolvedModelSettings":
        return ResolvedModelSettings(
            temperature=self.temperature(agent_ctx)
            if callable(self.temperature)
            else self.temperature,
            tool_choice=self.tool_choice(agent_ctx)
            if callable(self.tool_choice)
            else self.tool_choice,
            parallel_tool_calls=self.parallel_tool_calls(agent_ctx)
            if callable(self.parallel_tool_calls)
            else self.parallel_tool_calls,
            max_completion_tokens=self.max_completion_tokens(agent_ctx)
            if callable(self.max_completion_tokens)
            else self.max_completion_tokens,
        )


@dataclass
class ResolvedModelSettings:
    temperature: float | None
    tool_choice: str | dict[str, Any] | None
    parallel_tool_calls: bool | None
    max_completion_tokens: int | None


@dataclass
class ToolExecutionResults:
    new_messages: list[dict[str, Any]]
    tool_call_results: list[tuple[ChatCompletionMessageToolCall, Any | Exception]]
    pending_tool_call_ids: list[str]
    pending_child_task_ids: list[str]


class BaseAgent(Generic[ContextType]):
    def __init__(
        self,
        name: str | None = None,
        instructions: str | Callable[..., str] | None = None,
        description: str | None = None,
        tools: list[FunctionTool[ContextType] | Callable[..., Any]] | None = None,
        model: Model | Callable[[ContextType], Model] | None = None,
        model_settings: ModelSettings[ContextType] | None = None,
        stream: bool = False,
        context_window_limit: int | None = None,
        max_turns: int | None = None,
        http_client: httpx.AsyncClient | None = None,
        client: MultiClient | None = None,
        request_timeout: float = 120.0,
        parse_tool_args: bool = True,
        context_class: type = AgentContext,
        output_type: type[BaseModel] | None = None,
        # ---- Lifecycle callbacks ---- #
        on_run_start: Callable[[ContextType, ExecutionContext], Awaitable[None] | None]
        | None = None,
        on_run_end: Callable[
            [ContextType, ExecutionContext, RunCompletion[ContextType]],
            Awaitable[None] | None,
        ]
        | None = None,
        on_turn_start: Callable[[ContextType, ExecutionContext], Awaitable[None] | None]
        | None = None,
        on_turn_end: Callable[
            [ContextType, ExecutionContext, TurnCompletion[ContextType]],
            Awaitable[None] | None,
        ]
        | None = None,
        on_pending_tool_call: Callable[
            [ContextType, ExecutionContext, ChatCompletionMessageToolCall],
            Awaitable[None] | None,
        ]
        | None = None,
        on_pending_tool_results: Callable[
            [ContextType, ExecutionContext, list[tuple[str, Any]]],
            Awaitable[None] | None,
        ]
        | None = None,
        on_run_cancelled: Callable[
            [ContextType, ExecutionContext], Awaitable[None] | None
        ]
        | None = None,
    ):
        self.name = to_snake_case(name or self.__class__.__name__)
        self.description = description or self.__class__.__name__
        self.instructions = instructions
        self.tools, self.tool_actions = convert_tools_list(tools or [])
        self.http_client = http_client or httpx.AsyncClient(timeout=request_timeout)
        self.client = client or MultiClient(http_client=self.http_client)
        self.request_timeout = request_timeout
        self.event_publisher: EventPublisher | None = None
        self.model = model
        self.model_settings = model_settings or ModelSettings()
        self.stream = stream
        self.context_window_limit = context_window_limit
        self.max_turns = max_turns
        self.parse_tool_args = parse_tool_args
        self.context_class = context_class
        self.output_type = output_type
        self.final_output_tool = (
            create_final_output_tool(self.output_type) if self.output_type else None
        )

        # Lifecycle callbacks
        self.on_run_start = on_run_start
        self.on_run_end = on_run_end
        self.on_turn_start = on_turn_start
        self.on_turn_end = on_turn_end
        self.on_pending_tool_call = on_pending_tool_call
        self.on_pending_tool_results = on_pending_tool_results
        self.on_run_cancelled = on_run_cancelled

        # --- Validate lifecycle callback signatures ---------------------------------- #
        _vcs("on_run_start", self.on_run_start, (AgentContext, ExecutionContext))
        _vcs("on_turn_start", self.on_turn_start, (AgentContext, ExecutionContext))
        _vcs(
            "on_turn_end",
            self.on_turn_end,
            (AgentContext, ExecutionContext, TurnCompletion),
        )
        _vcs(
            "on_run_end",
            self.on_run_end,
            (AgentContext, ExecutionContext, RunCompletion),
        )
        _vcs(
            "on_pending_tool_call",
            self.on_pending_tool_call,
            (AgentContext, ExecutionContext, ChatCompletionMessageToolCall),
        )
        _vcs(
            "on_pending_tool_results",
            self.on_pending_tool_results,
            (AgentContext, ExecutionContext, list),
        )
        _vcs(
            "on_run_cancelled",
            self.on_run_cancelled,
            (AgentContext, ExecutionContext),
        )

    # ===== Overridable Methods ===== #

    async def completion(
        self,
        agent_ctx: ContextType,
        messages: list[dict[str, Any]],
    ) -> ChatCompletion:
        """Execute LLM completion. Override to customize."""
        model_settings = self.resolve_model_settings(agent_ctx)
        tools = self.resolve_tools(agent_ctx)
        model = self.resolve_model(agent_ctx)

        execution_ctx = ExecutionContext.current()

        # If ``max_turns`` is set we need to *force* the agent to finish on the final allowed turn.
        is_last_turn = (
            self.max_turns is not None and agent_ctx.turn >= self.max_turns - 1
        )

        if is_last_turn:
            if self.final_output_tool is not None:
                model_settings.tool_choice = {
                    "type": "function",
                    "function": {"name": "final_output"},
                }
            else:
                model_settings.tool_choice = "none"

        if not self.stream:
            response = cast(
                ChatCompletion,
                await self.client.completion(
                    model=model,
                    messages=messages,
                    tools=tools,
                    temperature=model_settings.temperature,
                    max_completion_tokens=model_settings.max_completion_tokens,
                    tool_choice=model_settings.tool_choice,
                    parallel_tool_calls=model_settings.parallel_tool_calls,
                ),
            )
        else:
            # progressively reconstruct both the assistant text content
            # *and* any tool calls (potentially multiple)

            content: str = ""
            # tool-call index -> partial data
            tool_call_acc: dict[int, dict[str, Any]] = {}

            res_id: str = ""
            created_ts: int | None = None
            finish_reason: str | None = None
            usage: Any | None = None  # ``usage`` is populated in the last chunk

            res = cast(
                AsyncStream[ChatCompletionChunk],
                await self.client.completion(
                    model=model,
                    messages=messages,
                    tools=tools,
                    temperature=model_settings.temperature,
                    max_completion_tokens=model_settings.max_completion_tokens,
                    tool_choice=model_settings.tool_choice,
                    parallel_tool_calls=model_settings.parallel_tool_calls,
                    stream=True,
                ),
            )

            async for chunk in res:
                res_id = chunk.id
                created_ts = chunk.created
                delta = chunk.choices[0].delta

                if delta.content:
                    content += delta.content

                # Accumulate tool call deltas (could be multiple per chunk)
                if delta.tool_calls:
                    for delta_tool in delta.tool_calls:
                        idx = delta_tool.index or 0

                        acc = tool_call_acc.setdefault(
                            idx,
                            {
                                "id": None,
                                "type": None,
                                "name": None,
                                "arguments": "",
                            },
                        )

                        # Static fields can arrive at any time – keep the first non-None.
                        if delta_tool.id is not None:
                            acc["id"] = delta_tool.id
                        if delta_tool.type is not None:
                            acc["type"] = delta_tool.type

                        # Function fragment (name/arguments)
                        if delta_tool.function:
                            if delta_tool.function.name is not None:
                                acc["name"] = delta_tool.function.name
                            if delta_tool.function.arguments:
                                acc["arguments"] += delta_tool.function.arguments

                # Capture finish reason (set in the last chunk)
                if chunk.choices[0].finish_reason is not None:
                    finish_reason = chunk.choices[0].finish_reason

                # Capture usage if provided (usually only in last chunk)
                if chunk.usage is not None:
                    usage = chunk.usage

                # Publish progress update for this chunk (streaming)
                try:
                    if execution_ctx and execution_ctx.events:
                        await execution_ctx.events.publish_event(
                            AgentEvent(
                                event_type="progress_update_completion_chunk",
                                task_id=execution_ctx.task_id,
                                owner_id=execution_ctx.owner_id,
                                agent_name=self.name,
                                turn=agent_ctx.turn,
                                data=serialize_data(chunk),
                            )
                        )
                except Exception:
                    logger.exception(
                        "Failed to publish streaming progress update", exc_info=True
                    )

            # Convert accumulated tool call data into the structured OpenAI type
            formatted_tool_calls: list[ChatCompletionMessageToolCall] | None = None
            if tool_call_acc:
                formatted_tool_calls = []
                for idx in sorted(tool_call_acc.keys()):
                    acc = tool_call_acc[idx]
                    if acc["name"] is None:
                        # Incomplete tool call – skip for safety (shouldn't happen)
                        logger.debug(
                            "Incomplete tool call: %s",
                            acc,
                        )
                        continue
                    formatted_tool_calls.append(
                        ChatCompletionMessageToolCall(
                            id=acc["id"] or f"call_{idx}",
                            type=acc["type"] or "function",
                            function={  # type: ignore[arg-type]
                                "name": acc["name"],
                                "arguments": acc["arguments"],
                            },
                        )
                    )

            response = ChatCompletion(
                id=res_id,
                model=model.name,
                created=created_ts or int(datetime.now().timestamp()),
                object="chat.completion",
                choices=[
                    Choice(
                        index=0,
                        message=ChatCompletionMessage(
                            role="assistant",
                            content=content or None,
                            tool_calls=formatted_tool_calls,
                        ),
                        finish_reason=finish_reason or "stop",
                    )
                ],
                usage=usage,
            )

        return response

    async def tool_action(
        self,
        tool_call: ChatCompletionMessageToolCall,
        agent_ctx: ContextType,
    ) -> FunctionToolActionResult:
        """Execute a tool action. Override to customize."""
        tool_name = tool_call.function.name
        tool_args = tool_call.function.arguments
        action = self.tool_actions.get(tool_name)

        execution_ctx = ExecutionContext.current()

        if not action:
            raise ValueError(f"Agent {self.name} has no tool action for {tool_name}")

        is_deferred_result = getattr(action, "deferred_result", False)
        is_forking_tool = getattr(action, "forking_tool", False)

        if not self.parse_tool_args:
            result = (
                await action(tool_args, agent_ctx)
                if asyncio.iscoroutinefunction(action)
                else action(tool_args, agent_ctx)
            )

        else:
            parsed_tool_args = json.loads(tool_args)

            for param_name, param in inspect.signature(action).parameters.items():
                # Skip if the argument is already provided by the LLM call
                if param_name in parsed_tool_args:
                    continue

                # Case 1: Parameter named 'agent_ctx'
                if param_name == "agent_ctx":
                    parsed_tool_args[param_name] = agent_ctx
                    continue

                # Case 2: Parameter type is a subclass of AgentContext
                if (
                    param.annotation
                    and param.annotation is not inspect.Parameter.empty
                    and isinstance(param.annotation, type)
                    and issubclass(param.annotation, AgentContext)
                ):
                    parsed_tool_args[param_name] = agent_ctx
                    continue

                # Case 3: Parameter named 'execution_ctx'
                if param_name == "execution_ctx":
                    parsed_tool_args[param_name] = execution_ctx
                    continue

                # Case 4: Parameter type is a subclass of ExecutionContext
                if (
                    param.annotation
                    and param.annotation is not inspect.Parameter.empty
                    and isinstance(param.annotation, type)
                    and issubclass(param.annotation, ExecutionContext)
                ):
                    parsed_tool_args[param_name] = execution_ctx
                    continue

            # ────────────────────────────────────────────────────────────
            # Second pass: coerce parsed JSON args into Pydantic models.
            # Supports both single ``BaseModel`` parameters and
            # ``list[BaseModel]`` collections.
            # ────────────────────────────────────────────────────────────
            for _pname, _param in inspect.signature(action).parameters.items():
                if _pname not in parsed_tool_args:
                    continue

                _expected = _param.annotation
                if _expected is inspect.Parameter.empty:
                    continue

                try:
                    _origin = get_origin(_expected)

                    # Case 1 – standalone BaseModel
                    if (
                        isinstance(_expected, type)
                        and issubclass(_expected, BaseModel)
                        and isinstance(parsed_tool_args[_pname], dict)
                    ):
                        parsed_tool_args[_pname] = _expected(**parsed_tool_args[_pname])

                    # Case 2 – list[BaseModel]
                    elif _origin is list:
                        _item_type = (
                            get_args(_expected)[0] if get_args(_expected) else None
                        )
                        if (
                            _item_type
                            and isinstance(_item_type, type)
                            and issubclass(_item_type, BaseModel)
                            and isinstance(parsed_tool_args[_pname], list)
                        ):
                            parsed_tool_args[_pname] = [
                                _item_type(**_it)
                                if not isinstance(_it, _item_type)
                                else _it  # type: ignore[arg-type]
                                for _it in parsed_tool_args[_pname]
                            ]
                except Exception as _e:
                    logger.debug(
                        "Failed to coerce argument '%s' to %s: %s",
                        _pname,
                        _expected,
                        _e,
                    )

            result = (
                await action(**parsed_tool_args)
                if asyncio.iscoroutinefunction(action)
                else action(**parsed_tool_args)
            )

        pending_child_task_ids: list[str] = []
        if is_forking_tool:
            # Determine candidate ID list depending on the return shape
            candidate_ids: list[str] | tuple[str, ...] | None = None

            # Case 1 – raw list/tuple of IDs returned by the tool
            if isinstance(result, (list, tuple)) and all(
                isinstance(item, str) for item in result
            ):
                candidate_ids = result  # type: ignore[assignment]

            # Case 2 – (message: str, ids: list/tuple[str])
            if (
                candidate_ids is None
                and isinstance(result, tuple)
                and len(result) == 2
                and isinstance(result[0], str)
                and isinstance(result[1], (list, tuple))
            ):
                candidate_ids = result[1]  # type: ignore[assignment]

            # Validate candidate IDs (if any)
            if candidate_ids is not None:
                if all(
                    isinstance(item, str) and is_valid_task_id(item)
                    for item in candidate_ids
                ):
                    pending_child_task_ids = list(candidate_ids)
                else:
                    raise ValueError(
                        f"Forking tool '{tool_call.function.name}' returned invalid task IDs: {candidate_ids}"
                    )

        if isinstance(result, FunctionToolActionResult):
            result.tool_call = tool_call
            result.pending_result = is_deferred_result
            result.pending_child_task_ids = pending_child_task_ids
            return result
        else:
            if (
                isinstance(result, tuple)
                and len(cast(tuple[Any, ...], result)) == 2
                and isinstance(result[0], str)
            ):
                result = cast(tuple[str, Any], result)
                output_str, output_data = result
            elif result is None:
                output_str = ""
                output_data = None
            else:
                result = cast(Any, result)
                output_str = str(result)
                output_data = result

            return FunctionToolActionResult(
                tool_call=tool_call,
                output_str=output_str,
                output_data=output_data,
                pending_result=is_deferred_result,
                pending_child_task_ids=pending_child_task_ids,
            )

    def extract_tool_call_result(
        self,
        tool_call: ChatCompletionMessageToolCall,
        result: Any | FunctionToolActionResult | Exception,
    ) -> tuple[str, Any | None, Exception | None]:
        if isinstance(result, Exception):
            str_result = f'<tool_call_error tool="{tool_call.id}">\nError running tool:\n{result}\n</tool_call_error>'
            return str_result, None, result
        elif isinstance(result, FunctionToolActionResult):
            str_result = f'<tool_call_result tool="{tool_call.id}">\n{result.output_str}\n</tool_call_result>'
            return str_result, result.output_data, None
        else:
            str_result = f'<tool_call_result tool="{tool_call.id}">\n{str(result)}\n</tool_call_result>'
            return str_result, result, None

    def format_tool_result(
        self, tool_call_id: str, result: Any | FunctionToolActionResult | Exception
    ) -> str:
        if isinstance(result, Exception):
            return f'<tool_call_error tool="{tool_call_id}">\nError running tool:\n{result}\n</tool_call_error>'
        elif isinstance(result, FunctionToolActionResult):
            return f'<tool_call_result tool="{tool_call_id}">\n{result.output_str}\n</tool_call_result>'
        else:
            return f'<tool_call_result tool="{tool_call_id}">\n{str(result)}\n</tool_call_result>'

    def format_child_task_result(
        self, child_task_id: str, result: Any | Exception
    ) -> str:
        if isinstance(result, Exception):
            return f'<sub_task_error sub_task_id="{child_task_id}">\nError running sub task:\n{result}\n</sub_task_error>'
        else:
            return f'<sub_task_result sub_task_id="{child_task_id}">\n{str(result)}\n</sub_task_result>'

    async def execute_tools(
        self,
        tool_calls: list[ChatCompletionMessageToolCall],
        agent_ctx: ContextType,
    ) -> ToolExecutionResults:
        """Execute tool calls and add results to messages"""
        new_messages: list[dict[str, Any]] = []
        pending_tool_call_ids: list[str] = []
        all_pending_child_task_ids: list[str] = []
        tool_call_results: list[
            tuple[ChatCompletionMessageToolCall, Any | Exception]
        ] = []

        # Run tools in parallel
        tasks = [
            self._tool_action_with_retry_and_progress(
                tc,
                agent_ctx,
            )
            for tc in tool_calls
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Add results to messages
        for tc, result in zip(tool_calls, results):
            tool_call_results.append(
                (
                    tc,
                    result
                    if isinstance(result, Exception)
                    else result.output_data
                    if isinstance(result, FunctionToolActionResult)
                    else result,
                )
            )

            if (
                isinstance(result, FunctionToolActionResult)
                and result.pending_child_task_ids
            ):
                all_pending_child_task_ids.extend(result.pending_child_task_ids)

            if isinstance(result, Exception):
                logger.error(
                    f"Tool {tc.function.name} failed: {result}", exc_info=result
                )
                # Still need to add a tool response message to maintain conversation format
                content = self.format_tool_result(tc.id, result)
                new_messages.append(
                    {"role": "tool", "tool_call_id": tc.id, "content": content}
                )
            elif isinstance(result, FunctionToolActionResult) and result.pending_result:
                pending_tool_call_ids.append(tc.id)
                # Invoke lifecycle callback for pending tool calls
                await self._safe_call(
                    self.on_pending_tool_call,
                    agent_ctx,
                    ExecutionContext.current(),
                    tc,
                )
            else:
                content = self.format_tool_result(tc.id, result)
                new_messages.append(
                    {"role": "tool", "tool_call_id": tc.id, "content": content}
                )

            # Propagate unrecoverable errors immediately so the worker can fail the task.
            if isinstance(result, FatalAgentError):
                raise result

        return ToolExecutionResults(
            new_messages=new_messages,
            tool_call_results=tool_call_results,
            pending_tool_call_ids=pending_tool_call_ids,
            pending_child_task_ids=all_pending_child_task_ids,
        )

    async def run_turn(
        self,
        agent_ctx: ContextType,
    ) -> TurnCompletion[ContextType]:
        """Run a single turn. Override for custom logic."""

        # Lifecycle callback – turn start
        execution_ctx = ExecutionContext.current()
        await self._safe_call(self.on_turn_start, agent_ctx, execution_ctx)

        messages = self.prepare_messages(agent_ctx)

        response = await self._completion_with_retry_and_progress(
            agent_ctx,
            messages,
        )

        assistant_msg = {
            "role": "assistant",
            "content": response.choices[0].message.content,
            "tool_calls": (
                [
                    tc.model_dump()  # type: ignore
                    for tc in response.choices[0].message.tool_calls
                ]
                if response.choices[0].message.tool_calls
                else None
            ),
        }
        messages.append(assistant_msg)

        if self._is_done(response):
            output = self._extract_output(response)
            agent_ctx.output = output  # Store the final output in the agent context
            agent_ctx.messages = messages
            completion = TurnCompletion(
                is_done=True,
                context=agent_ctx,
                output=output,
            )

            # Lifecycle callback – turn end
            await self._safe_call(
                self.on_turn_end, agent_ctx, execution_ctx, completion
            )
            return completion

        # Handle tool calls and append results to messages
        tool_call_results: list[
            tuple[ChatCompletionMessageToolCall, Any | Exception]
        ] = []
        pending_tool_call_ids: list[str] = []
        pending_child_task_ids: list[str] = []
        if response.choices[0].message.tool_calls:
            results = await self.execute_tools(
                response.choices[0].message.tool_calls,
                agent_ctx,
            )
            messages += results.new_messages
            pending_tool_call_ids += results.pending_tool_call_ids
            pending_child_task_ids += results.pending_child_task_ids
            tool_call_results += results.tool_call_results

        agent_ctx.messages = messages
        agent_ctx.turn += 1
        completion = TurnCompletion(
            is_done=False,
            context=agent_ctx,
            pending_tool_call_ids=pending_tool_call_ids,
            pending_child_task_ids=pending_child_task_ids,
            tool_call_results=tool_call_results,
        )

        # Lifecycle callback – turn end
        await self._safe_call(self.on_turn_end, agent_ctx, execution_ctx, completion)
        return completion

    def process_deferred_tool_results(
        self,
        agent_ctx: ContextType,
        tool_call_results: list[tuple[str, Any]],
    ) -> TurnCompletion[ContextType]:
        updated_messages = agent_ctx.messages.copy()
        for tool_call_id, result in tool_call_results:
            if isinstance(result, Exception):
                logger.error(f"Tool {tool_call_id} failed: {result}", exc_info=result)

            content = self.format_tool_result(tool_call_id, result)
            updated_messages.append(
                {"role": "tool", "tool_call_id": tool_call_id, "content": content}
            )
        agent_ctx.messages = updated_messages

        return TurnCompletion(is_done=False, context=agent_ctx)

    def process_child_task_results(
        self,
        agent_ctx: ContextType,
        child_task_results: list[tuple[str, Any]],
    ) -> TurnCompletion[ContextType]:
        updated_messages = agent_ctx.messages.copy()
        formatted_child_task_results = []

        for child_task_id, result in child_task_results:
            if isinstance(result, Exception):
                logger.error(
                    f"Child task {child_task_id} failed: {result}", exc_info=result
                )
            formatted_child_task_results.append(
                self.format_child_task_result(child_task_id, result)
            )

        if formatted_child_task_results:
            joined_results = "\n\n".join(formatted_child_task_results)
            updated_messages.append(
                {
                    "role": "assistant",
                    "content": f"<sub_task_results>\n{joined_results}\n</sub_task_results>",
                }
            )

        agent_ctx.messages = updated_messages

        return TurnCompletion(is_done=False, context=agent_ctx)

    # ===== Helper Methods ===== #
    def resolve_tools(self, agent_ctx: ContextType) -> list[dict[str, Any]]:
        tools = [
            tool.to_openai_tool_schema()
            for tool in self.tools
            if (
                tool.is_enabled(agent_ctx)
                if callable(tool.is_enabled)
                else tool.is_enabled
            )
        ]
        if self.final_output_tool:
            tools.append(self.final_output_tool)

        return tools

    def resolve_model(self, agent_ctx: ContextType) -> Model:
        model = self.model(agent_ctx) if callable(self.model) else self.model
        if not model:
            raise ValueError("No model is configured for agent")

        return model

    def resolve_model_settings(self, agent_ctx: ContextType) -> ResolvedModelSettings:
        return self.model_settings.resolve(agent_ctx)

    def resolve_instructions(self, agent_ctx: ContextType) -> str:
        """Resolve instructions, handling both string and callable instructions."""
        instructions = self.instructions

        if callable(instructions):
            sig = inspect.signature(instructions)
            params = list(sig.parameters.values())

            # Always pass agent_ctx as first argument
            args = [agent_ctx]

            # Check if any parameter has ExecutionContext type annotation
            has_execution_ctx = any(
                param.annotation
                and param.annotation != inspect.Parameter.empty
                and isinstance(param.annotation, type)
                and issubclass(param.annotation, ExecutionContext)
                for param in params
            )

            if has_execution_ctx:
                execution_ctx = ExecutionContext.current()
                args.append(cast(Any, execution_ctx))

            instructions = instructions(*args)

        return instructions or ""

    def _is_done(self, response: ChatCompletion) -> bool:
        """Check if agent should complete"""
        tool_calls = response.choices[0].message.tool_calls

        return (not tool_calls and self.output_type is None) or any(
            tc.function.name == "final_output" for tc in tool_calls or []
        )

    def _prepare_instructions(self, agent_ctx: ContextType) -> str:
        """Prepare instructions for LLM request. Override to customize instructions preparation."""

        instructions = self.resolve_instructions(agent_ctx)

        if self.max_turns:
            instructions += f"\n\nYou must reach a final response in {self.max_turns} turns or less."
        if (
            self.output_type
            and isinstance(self.output_type, type)
            and issubclass(self.output_type, BaseModel)
        ):
            instructions += (
                "\n\nYour final response must be made using the 'final_output' tool."
            )

        return instructions

    def prepare_messages(self, agent_ctx: ContextType) -> list[dict[str, Any]]:
        """Prepare messages for LLM request. Override to customize message preparation."""
        if agent_ctx.turn == 0:
            messages = (
                [{"role": "system", "content": self._prepare_instructions(agent_ctx)}]
                if self.instructions
                else []
            )
            if agent_ctx.messages:
                messages.extend(
                    [message for message in agent_ctx.messages if message["content"]]
                )
            messages.append({"role": "user", "content": agent_ctx.query})
        else:
            messages = agent_ctx.messages

        return messages

    def _extract_output(
        self,
        response: ChatCompletion,
    ) -> str | dict[str, Any] | None:
        """Extract the final output from response content or tool calls."""
        content = response.choices[0].message.content
        tool_calls = response.choices[0].message.tool_calls

        if content:
            return content

        if tool_calls:
            for tool_call in tool_calls:
                if tool_call.function.name == "final_output":
                    if self.parse_tool_args:
                        return json.loads(tool_call.function.arguments)
                    else:
                        return tool_call.function.arguments

        return None

    # ===== Core logic ===== #

    def get_execution_context(self) -> ExecutionContext:
        return ExecutionContext.current()

    @retry(max_attempts=3, delay=0.5)
    @publish_progress(func_name="completion")
    async def _completion_with_retry_and_progress(
        self,
        agent_ctx: ContextType,
        messages: list[dict[str, Any]],
    ) -> ChatCompletion:
        """Internal method that wraps completion with retry and progress publishing"""
        return await self.completion(
            agent_ctx,
            messages,
        )

    @retry(max_attempts=2, delay=0.25)
    @publish_progress(func_name="tool_action")
    async def _tool_action_with_retry_and_progress(
        self,
        tool_call: ChatCompletionMessageToolCall,
        agent_ctx: ContextType,
    ) -> FunctionToolActionResult:
        """Internal method that wraps tool_action with retry and progress publishing"""
        return await self.tool_action(tool_call, agent_ctx)

    @publish_progress(func_name="run_turn")
    async def _run_turn_with_progress(
        self,
        agent_ctx: ContextType,
    ) -> TurnCompletion[ContextType]:
        """Internal method that wraps run_turn with progress publishing"""
        return await self.run_turn(agent_ctx)

    async def steer(
        self,
        messages: list[dict[str, Any]],
        agent_ctx: ContextType,
        execution_ctx: ExecutionContext,
    ) -> ContextType:
        agent_ctx.messages.extend(messages)
        return agent_ctx

    async def cancel(
        self, agent_ctx: ContextType, execution_ctx: ExecutionContext
    ) -> None:
        pass

    @final
    async def execute(
        self,
        agent_ctx: ContextType,
        execution_ctx: ExecutionContext,
    ) -> TurnCompletion[ContextType]:
        token = execution_context.set(execution_ctx)

        try:
            response = await self._run_turn_with_progress(agent_ctx)

            if response.is_done:
                await execution_ctx.events.publish_event(
                    AgentEvent(
                        event_type="agent_output",
                        task_id=execution_ctx.task_id,
                        owner_id=execution_ctx.owner_id,
                        agent_name=self.name,
                        turn=agent_ctx.turn,
                        data=response.output,
                    )
                )

            return response

        except Exception as e:
            logger.error(
                f"Agent {self.name} failed to execute turn {agent_ctx.turn}", exc_info=e
            )

            raise e

        finally:
            execution_context.reset(token)

    async def _safe_call(self, func: Callable[..., Any] | None, *args: Any) -> None:
        """Safely call a (possibly async) callback without letting it crash the agent.

        *All* exceptions are logged, but only non-fatal ones are swallowed. If the
        callback raises ``FatalAgentError`` we re-raise so the orchestrator can
        treat the task as a hard failure.
        """
        if func is None:
            return

        try:
            result = func(*args)
            if inspect.isawaitable(result):
                await result  # type: ignore[func-returns-value]
        except FatalAgentError as e:
            # Log and propagate so the caller can handle a fatal error.
            logger.error("FatalAgentError in lifecycle callback", exc_info=e)
            raise
        except Exception:
            # Non-fatal errors are logged but swallowed to avoid taking down the task.
            logger.exception("Error in lifecycle callback", exc_info=True)


class Agent(BaseAgent[AgentContext]):
    """Base agent class that uses AgentContext by default. Use this for simple agents."""

    pass
