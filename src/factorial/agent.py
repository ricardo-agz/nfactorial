import random
from typing import (
    Any,
    Callable,
    Awaitable,
    cast,
    final,
    Generic,
    TypeVar,
    overload,
)
from functools import wraps
from dataclasses import dataclass, field
from pydantic import BaseModel
import json
import asyncio
import inspect
import httpx
from openai.types.chat import (
    ChatCompletion,
    ChatCompletionMessageToolCall,
)

from factorial.context import AgentContext, ExecutionContext, execution_context
from factorial.task import Task, ContextType
from factorial.llms import Model, MultiClient
from factorial.utils import serialize_data, to_snake_case
from factorial.events import EventPublisher, AgentEvent
from factorial.logging import get_logger
from factorial.exceptions import RETRYABLE_EXCEPTIONS
from factorial.tools import (
    FunctionTool,
    FunctionToolActionResult,
    FunctionToolAction,
    convert_tools_list,
    create_final_output_tool,
    _function_to_json_schema,
    function_tool,
)


logger = get_logger(__name__)


ContextT = TypeVar("ContextT", bound=AgentContext)
T = TypeVar("T")


def deferred_result(
    timeout: float,
) -> Callable[[Callable[..., Awaitable[T]]], Callable[..., Awaitable[T]]]:
    def decorator(func: Callable[..., Awaitable[T]]) -> Callable[..., Awaitable[T]]:
        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> T:
            return await func(*args, **kwargs)

        wrapper.deferred_result = True  # type: ignore
        wrapper.timeout = timeout  # type: ignore
        return wrapper

    return decorator


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
    pending_tool_call_ids: list[str] = field(default_factory=list)


@dataclass
class ModelSettings(Generic[ContextT]):
    temperature: float | Callable[[ContextT], float] | None = None
    tool_choice: (
        str | dict[str, Any] | Callable[[ContextT], str | dict[str, Any] | None] | None
    ) = None
    parallel_tool_calls: bool | Callable[[ContextT], bool] | None = None
    max_completion_tokens: int | Callable[[ContextT], int] | None = None

    def resolve(self, agent_ctx: ContextT) -> "ResolvedModelSettings":
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
    pending_tool_call_ids: list[str]


class BaseAgent(Generic[ContextType]):
    def __init__(
        self,
        instructions: str | None = None,
        description: str | None = None,
        tools: list[FunctionTool[ContextType] | Callable[..., Any]] | None = None,
        model: Model | Callable[[ContextType], Model] | None = None,
        model_settings: ModelSettings[ContextType] | None = None,
        context_window_limit: int | None = None,
        max_turns: int | None = None,
        http_client: httpx.AsyncClient | None = None,
        client: MultiClient | None = None,
        request_timeout: float = 120.0,
        parse_tool_args: bool = True,
        context_class: type = AgentContext,
        output_type: type[BaseModel] | None = None,
    ):
        self.name = to_snake_case(self.__class__.__name__)
        self.description = description or self.__class__.__name__
        self.instructions = instructions
        self.tools, self.tool_actions = convert_tools_list(tools or [])
        self.http_client = http_client or httpx.AsyncClient(timeout=request_timeout)
        self.client = client or MultiClient(http_client=self.http_client)
        self.request_timeout = request_timeout
        self.event_publisher: EventPublisher | None = None
        self.model = model
        self.model_settings = model_settings or ModelSettings()
        self.context_window_limit = context_window_limit
        self.max_turns = max_turns
        self.parse_tool_args = parse_tool_args
        self.context_class = context_class
        self.output_type = output_type
        self.final_output_tool = (
            create_final_output_tool(self.output_type) if self.output_type else None
        )

    def create_task(
        self,
        owner_id: str,
        payload: ContextType,
    ) -> Task[ContextType]:
        """Create a task with the correct context type for this agent"""
        return Task.create(
            owner_id=owner_id,
            agent=self.name,
            payload=payload,
        )

    def deserialize_task(self, task_json: str) -> Task[ContextType]:
        return Task.from_json(task_json)

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

        if not self.parse_tool_args:
            result = (
                await action(tool_args, agent_ctx)
                if asyncio.iscoroutinefunction(action)
                else action(tool_args, agent_ctx)
            )

        else:
            parsed_tool_args = json.loads(tool_args)

            # Check if function expects agent context
            sig = inspect.signature(action)
            for param_name, param in sig.parameters.items():
                # Case 1: Parameter named 'agent_ctx'
                if param_name == "agent_ctx":
                    parsed_tool_args[param_name] = agent_ctx
                    break
                # Case 2: Parameter with AgentContext subclass type annotation
                elif (
                    param.annotation
                    and param.annotation != inspect.Parameter.empty
                    and isinstance(param.annotation, type)
                    and issubclass(param.annotation, AgentContext)
                ):
                    parsed_tool_args[param_name] = agent_ctx
                    break
                # Case 3: Parameter named 'execution_ctx'
                elif param_name == "execution_ctx":
                    parsed_tool_args[param_name] = execution_ctx
                    break
                # Case 4: Parameter with ExecutionContext subclass type annotation
                elif (
                    param.annotation
                    and param.annotation != inspect.Parameter.empty
                    and isinstance(param.annotation, type)
                    and issubclass(param.annotation, ExecutionContext)
                ):
                    parsed_tool_args[param_name] = execution_ctx
                    break

            result = (
                await action(**parsed_tool_args)
                if asyncio.iscoroutinefunction(action)
                else action(**parsed_tool_args)
            )

        if isinstance(result, FunctionToolActionResult):
            result.tool_call = tool_call
            result.pending_result = is_deferred_result
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
            )

    def format_tool_result(
        self, tool_call_id: str, result: Any | FunctionToolActionResult | Exception
    ) -> str:
        if isinstance(result, Exception):
            return f'<tool_call_error tool="{tool_call_id}">\nError running tool:\n{result}\n</tool_call_error>'
        elif isinstance(result, FunctionToolActionResult):
            return f'<tool_call_result tool="{tool_call_id}">\n{result.output_str}\n</tool_call_result>'
        else:
            return f'<tool_call_result tool="{tool_call_id}">\n{str(result)}\n</tool_call_result>'

    async def execute_tools(
        self,
        tool_calls: list[ChatCompletionMessageToolCall],
        agent_ctx: ContextType,
    ) -> ToolExecutionResults:
        """Execute tool calls and add results to messages"""
        new_messages: list[dict[str, Any]] = []
        pending_tool_call_ids: list[str] = []

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
            else:
                content = self.format_tool_result(tc.id, result)
                new_messages.append(
                    {"role": "tool", "tool_call_id": tc.id, "content": content}
                )

        return ToolExecutionResults(
            new_messages=new_messages,
            pending_tool_call_ids=pending_tool_call_ids,
        )

    async def run_turn(
        self,
        agent_ctx: ContextType,
    ) -> TurnCompletion[ContextType]:
        """Run a single turn. Override for custom logic."""

        messages = self._prepare_messages(agent_ctx)

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
            return TurnCompletion(
                is_done=True,
                context=agent_ctx,
                output=output,
            )

        # Handle tool calls and append results to messages
        pending_tool_call_ids: list[str] = []
        if response.choices[0].message.tool_calls:
            results = await self.execute_tools(
                response.choices[0].message.tool_calls,
                agent_ctx,
            )
            messages += results.new_messages
            pending_tool_call_ids += results.pending_tool_call_ids

        agent_ctx.messages = messages
        agent_ctx.turn += 1
        return TurnCompletion(
            is_done=False,
            context=agent_ctx,
            pending_tool_call_ids=pending_tool_call_ids,
        )

    def process_long_running_tool_results(
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

    def _is_done(self, response: ChatCompletion) -> bool:
        """Check if agent should complete"""
        tool_calls = response.choices[0].message.tool_calls
        return (not tool_calls and self.output_type is None) or any(
            tc.function.name == "final_output" for tc in tool_calls or []
        )

    def _prepare_instructions(self, agent_ctx: ContextType) -> str:
        """Prepare instructions for LLM request. Override to customize instructions preparation."""

        instructions = self.instructions
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

    def _prepare_messages(self, agent_ctx: ContextType) -> list[dict[str, Any]]:
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


class Agent(BaseAgent[AgentContext]):
    """Base agent class that uses AgentContext by default. Use this for simple agents."""

    pass


@function_tool(name="get_weather")
def get_weather(agent_ctx: AgentContext, location: str) -> str:
    return f"The weather in {location} is sunny."


@function_tool
def get_time(agent_ctx: AgentContext) -> str:
    return "The current time is 12:00 PM."


my_agennt = Agent(
    description="Dummy agent",
    instructions="You are a helpful assistant",
    tools=[get_weather, get_time],
)
