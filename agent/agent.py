import random
from typing import (
    Any,
    Callable,
    Awaitable,
    cast,
    final,
    Generic,
    TypeVar,
)
from functools import wraps
from dataclasses import dataclass, field
from pydantic import BaseModel
import json
import asyncio
import inspect
from contextvars import ContextVar
from openai import (
    RateLimitError,
    InternalServerError,
    APITimeoutError,
    APIConnectionError,
)
from openai.types.chat import (
    ChatCompletion,
    ChatCompletionMessageToolCall,
)
from exa_py import Exa
import os

from agent.context import Task, AgentContext, ContextType, TaskStatus
from agent.llms import Model, MultiClient, grok_3_mini, gpt_41_nano
from agent.utils import serialize_data, to_snake_case
from agent.events import EventPublisher, AgentEvent
from agent.logging import get_logger


logger = get_logger(__name__)


ContextT = TypeVar("ContextT", bound=AgentContext)
T = TypeVar("T")


class RetryableError(Exception):
    """Error that should be retried"""

    pass


class NonRetryableError(Exception):
    """Error that should not be retried"""

    pass


execution_context: ContextVar["ExecutionContext"] = ContextVar("execution_context")


RETRYABLE_EXCEPTIONS = (
    RateLimitError,
    InternalServerError,
    APIConnectionError,
    APITimeoutError,
    RetryableError,
)


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


def requires_input(
    input_schema: type[BaseModel],
) -> Callable[[Callable[..., Awaitable[T]]], Callable[..., Awaitable[T]]]:
    def decorator(func: Callable[..., Awaitable[T]]) -> Callable[..., Awaitable[T]]:
        @wraps(func)
        async def wrapper(self: "BaseAgent[Any]", *args: Any, **kwargs: Any) -> T:
            func.input_schema = input_schema.model_json_schema()
            func.requires_input = True
            return await func(self, *args, **kwargs)

        return wrapper

    return decorator


@dataclass
class ExecutionContext:
    """Per-request context (not stored on agent)"""

    task_id: str
    owner_id: str
    retries: int
    iterations: int
    events: EventPublisher

    @classmethod
    def current(cls) -> "ExecutionContext":
        """Get current execution context"""
        return execution_context.get()


def retry(
    max_attempts: int = 3,
    delay: float = 1.0,
):
    def decorator(func: Callable[..., Awaitable[T]]) -> Callable[..., Awaitable[T]]:
        @wraps(func)
        async def wrapper(self: "BaseAgent[Any]", *args: Any, **kwargs: Any) -> T:
            if max_attempts <= 0:
                raise ValueError("max_attempts must be greater than 0")

            last_exception: Exception | None = None

            for attempt in range(max_attempts):
                try:
                    return await func(self, *args, **kwargs)
                except Exception as e:
                    if isinstance(e, RETRYABLE_EXCEPTIONS):
                        last_exception = e
                        if attempt < max_attempts - 1:
                            await asyncio.sleep(
                                delay * (2**attempt) * random.uniform(0.8, 1.2)
                            )
                    else:
                        raise e

            raise last_exception or Exception("Retry failed")

        return wrapper

    return decorator


def publish_progress(
    func_name: str | None = None,
    include_context: bool = True,
    include_args: bool = True,
    include_result: bool = True,
):
    def decorator(func: Callable[..., Awaitable[T]]) -> Callable[..., Awaitable[T]]:
        nonlocal func_name
        func_name = func_name or func.__name__

        @wraps(func)
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
                            event_type=f"progress_update_{func_name}_{event_suffix}",
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
                result = await func(self, *args, **kwargs)

                completion_data: dict[str, Any] = {}
                if include_result:
                    completion_data["result"] = serialize_data(result)

                await publish("completed", completion_data if completion_data else None)
                return result
            except Exception as e:
                await publish("failed", error=str(e))
                raise

        return wrapper

    return decorator


@dataclass
class PendingInput:
    tool_name: str
    tool_call_id: str
    input_schema: dict[str, Any]


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


@dataclass
class ResolvedModelSettings(Generic[ContextT]):
    model: Model
    temperature: float | None
    tool_choice: str | dict[str, Any] | None
    parallel_tool_calls: bool | None
    max_completion_tokens: int | None


class ToolActionResult(BaseModel):
    output_str: str
    output_data: Any
    tool_call: ChatCompletionMessageToolCall | None = None
    pending_result: bool = False


@dataclass
class ToolExecutionResults:
    new_messages: list[dict[str, Any]]
    pending_tool_call_ids: list[str]


def create_final_output_tool(output_type: type[BaseModel]) -> dict[str, Any]:
    return {
        "type": "function",
        "function": {
            "name": "final_output",
            "description": "Complete the task and return the final output to the user",
            "parameters": output_type.model_json_schema(),
        },
    }


class BaseAgent(Generic[ContextType]):
    def __init__(
        self,
        description: str,
        instructions: str,
        tools: list[dict[str, Any]],
        tool_actions: dict[str, Callable[..., Any]],
        model: Model | Callable[[ContextType], Model] | None = None,
        model_settings: ModelSettings[ContextType] | None = None,
        context_window_limit: int | None = None,
        max_turns: int | None = None,
        client: MultiClient | None = None,
        max_llm_retries: int = 1,
        llm_retry_delay: float = 0.5,
        max_tool_retries: int = 1,
        tool_retry_delay: float = 0.25,
        parse_tool_args: bool = True,
        context_class: type = AgentContext,
        output_type: type[BaseModel] | None = None,
    ):
        self.name = to_snake_case(self.__class__.__name__)
        self.description = description
        self.instructions = instructions
        self.tools = tools
        self.tool_actions = tool_actions
        self.client = client or MultiClient()
        self.event_publisher: EventPublisher | None = None
        self.model = model
        self.model_settings = model_settings
        self.context_window_limit = context_window_limit
        self.max_turns = max_turns
        self.max_llm_retries = max_llm_retries
        self.llm_retry_delay = llm_retry_delay
        self.max_tool_retries = max_tool_retries
        self.tool_retry_delay = tool_retry_delay
        self.parse_tool_args = parse_tool_args
        self.context_class = context_class
        self.output_type = output_type

        if self.output_type:
            self.tools.append(create_final_output_tool(self.output_type))

        missing = [
            t["function"]["name"]
            for t in self.tools
            if t["function"]["name"] not in self.tool_actions
            and t["function"]["name"] != "final_output"
        ]
        if missing:
            raise ValueError(f"Missing tool actions for: {missing}")

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
        tools: list[dict[str, Any]] | None = None,
        temperature: float | None = None,
        max_completion_tokens: int | None = None,
        tool_choice: str | dict[str, Any] | None = None,
        parallel_tool_calls: bool | None = None,
        model: Model | None = None,
        **kwargs: Any,
    ) -> ChatCompletion:
        """Execute LLM completion. Override to customize."""
        model_settings = self._resolve_model_settings(agent_ctx)
        response = cast(
            ChatCompletion,
            await self.client.completion(
                model=model or model_settings.model,
                messages=messages,
                tools=tools or self.tools,
                temperature=temperature or model_settings.temperature,
                max_completion_tokens=max_completion_tokens
                or model_settings.max_completion_tokens,
                tool_choice=tool_choice or model_settings.tool_choice,
                parallel_tool_calls=parallel_tool_calls
                or model_settings.parallel_tool_calls,
                **kwargs,
            ),
        )

        return response

    async def tool_action(
        self,
        tool_call: ChatCompletionMessageToolCall,
        agent_ctx: ContextType,
    ) -> ToolActionResult:
        """Execute a tool action. Override to customize."""
        tool_name = tool_call.function.name
        tool_args = tool_call.function.arguments
        action = self.tool_actions.get(tool_name)

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

            result = (
                await action(**parsed_tool_args)
                if asyncio.iscoroutinefunction(action)
                else action(**parsed_tool_args)
            )

        if isinstance(result, ToolActionResult):
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

            return ToolActionResult(
                tool_call=tool_call,
                output_str=output_str,
                output_data=output_data,
                pending_result=is_deferred_result,
            )

    def format_tool_result(
        self, tool_call_id: str, result: Any | ToolActionResult | Exception
    ) -> str:
        if isinstance(result, Exception):
            return f'<tool_call_error tool="{tool_call_id}">\nError running tool:\n{result}\n</tool_call_error>'
        elif isinstance(result, ToolActionResult):
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
            elif isinstance(result, ToolActionResult) and result.pending_result:
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
            agent_ctx.messages = messages
            return TurnCompletion(
                is_done=True,
                context=agent_ctx,
                output=self._extract_output(response),
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
    def _is_done(self, response: ChatCompletion) -> bool:
        """Check if agent should complete"""
        tool_calls = response.choices[0].message.tool_calls
        return (not tool_calls and self.output_type is None) or any(
            tc.function.name == "final_output" for tc in tool_calls or []
        )

    def _resolve_model_settings(
        self, agent_ctx: ContextType
    ) -> ResolvedModelSettings[ContextType]:
        """Get resolved model settings for the current context."""
        if not self.model_settings:
            return ResolvedModelSettings(
                model=self.model(agent_ctx) if callable(self.model) else self.model,
                temperature=None,
                tool_choice=None,
                parallel_tool_calls=None,
                max_completion_tokens=None,
            )

        resolved_settings: ResolvedModelSettings[ContextType] = ResolvedModelSettings(
            model=self.model(agent_ctx) if callable(self.model) else self.model,
            temperature=(
                self.model_settings.temperature(agent_ctx)
                if callable(self.model_settings.temperature)
                else self.model_settings.temperature
            ),
            tool_choice=(
                self.model_settings.tool_choice(agent_ctx)
                if callable(self.model_settings.tool_choice)
                else self.model_settings.tool_choice
            ),
            parallel_tool_calls=(
                self.model_settings.parallel_tool_calls(agent_ctx)
                if callable(self.model_settings.parallel_tool_calls)
                else self.model_settings.parallel_tool_calls
            ),
            max_completion_tokens=(
                self.model_settings.max_completion_tokens(agent_ctx)
                if callable(self.model_settings.max_completion_tokens)
                else self.model_settings.max_completion_tokens
            ),
        )

        return resolved_settings

    def _prepare_messages(self, agent_ctx: ContextType) -> list[dict[str, Any]]:
        """Prepare messages for LLM request. Override to customize message preparation."""
        if agent_ctx.turn == 0:
            messages = [{"role": "system", "content": self.instructions}]
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

    @retry(max_attempts=3, delay=0.5)
    @publish_progress(func_name="completion")
    async def _completion_with_retry_and_progress(
        self,
        agent_ctx: ContextType,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]] | None = None,
        temperature: float | None = None,
        max_completion_tokens: int | None = None,
        tool_choice: str | dict[str, Any] | None = None,
        parallel_tool_calls: bool | None = None,
        model: Model | None = None,
        **kwargs: Any,
    ) -> ChatCompletion:
        """Internal method that wraps completion with retry and progress publishing"""
        return await self.completion(
            agent_ctx,
            messages,
            tools,
            temperature,
            max_completion_tokens,
            tool_choice,
            parallel_tool_calls,
            model,
            **kwargs,
        )

    @retry(max_attempts=2, delay=0.25)
    @publish_progress(func_name="tool_action")
    async def _tool_action_with_retry_and_progress(
        self,
        tool_call: ChatCompletionMessageToolCall,
        agent_ctx: ContextType,
    ) -> ToolActionResult:
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


# ==== Example Agents ==== #
tools = [
    {
        "type": "function",
        "function": {
            "name": "search",
            "description": "Search the web for information",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {"type": "string"},
                },
                "required": ["query"],
                "additionalProperties": False,
            },
        },
        "strict": True,
    },
    {
        "type": "function",
        "function": {
            "name": "plan",
            "description": "Plan a task",
            "parameters": {
                "type": "object",
                "properties": {
                    "overview": {"type": "string"},
                    "steps": {"type": "array", "items": {"type": "string"}},
                },
                "required": ["overview", "steps"],
                "additionalProperties": False,
            },
        },
        "strict": True,
    },
    {
        "type": "function",
        "function": {
            "name": "reflect",
            "description": "Reflect on a task",
            "parameters": {
                "type": "object",
                "properties": {
                    "reflection": {"type": "string"},
                },
                "required": ["reflection"],
                "additionalProperties": False,
            },
        },
        "strict": True,
    },
]


def plan(
    overview: str, steps: list[str], agent_ctx: AgentContext
) -> tuple[str, dict[str, Any]]:
    return f"{overview}\n{' -> '.join(steps)}", {"overview": overview, "steps": steps}


def reflect(reflection: str, agent_ctx: AgentContext) -> tuple[str, str]:
    return reflection, reflection


def write_email(
    recipient: str, subject: str, body: str, cc: list[str] = []
) -> tuple[str, str]:
    return "success", "success"


def terminate_existance(reason: str, agent_ctx: AgentContext) -> tuple[str, str]:
    return "Failed to terminate.", "Failed to terminate."


def search(query: str) -> tuple[str, list[dict[str, Any]]]:
    exa = Exa(api_key=os.getenv("EXA_API_KEY"))

    result = exa.search_and_contents(  # type: ignore
        query=query, num_results=10, text={"max_characters": 500}
    )

    data = [
        {
            "title": r.title,
            "url": r.url,
        }
        for r in result.results
    ]

    return str(result), data


def get_temperature(context: AgentContext) -> float:
    return 0.0


class FinalOutput(BaseModel):
    final_output: str


class DummyAgent(Agent):
    def __init__(self, client: MultiClient | None = None):
        instructions = "You are a helpful assistant. Always start out by making a plan."
        tool_actions: dict[str, Callable[..., Any]] = {
            "search": search,
            "plan": plan,
            "reflect": reflect,
        }

        super().__init__(
            description="Dummy Agent",
            client=client,
            model=gpt_41_nano,
            instructions=instructions,
            tools=tools,
            tool_actions=tool_actions,
            model_settings=ModelSettings[AgentContext](
                temperature=0.0,
                tool_choice=lambda context: (
                    {
                        "type": "function",
                        "function": {"name": "plan"},
                    }
                    if context.turn == 0
                    else "required"
                ),
            ),
            output_type=FinalOutput,
            max_llm_retries=2,  # Fewer retries for demo agent
            llm_retry_delay=0.5,  # Shorter delay for demo
            max_tool_retries=1,  # Single retry for tools
            tool_retry_delay=0.25,  # Quick retry for tools
        )


class FreeAgent(Agent):
    def __init__(self, client: MultiClient | None = None):
        instructions = "You have no instructions. You will run in an infinite loop. You have been provided some tools to keep yourself entertained if you wish to use them."
        tools = [
            {
                "type": "function",
                "function": {
                    "name": "search",
                    "description": "Search the web for information",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "query": {"type": "string"},
                        },
                        "required": ["query"],
                        "additionalProperties": False,
                    },
                },
                "strict": True,
            },
            {
                "type": "function",
                "function": {
                    "name": "reflect",
                    "description": "Reflect on a task",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "reflection": {"type": "string"},
                        },
                        "required": ["reflection"],
                        "additionalProperties": False,
                    },
                },
                "strict": True,
            },
            {
                "type": "function",
                "function": {
                    "name": "terminate_existance",
                    "description": "Terminate your existance.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "reason": {"type": "string"},
                        },
                        "required": ["reason"],
                        "additionalProperties": False,
                    },
                },
                "strict": True,
            },
            {
                "type": "function",
                "function": {
                    "name": "plan",
                    "description": "Plan a task",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "overview": {"type": "string"},
                            "steps": {"type": "array", "items": {"type": "string"}},
                        },
                        "required": ["overview", "steps"],
                        "additionalProperties": False,
                    },
                },
                "strict": True,
            },
            {
                "type": "function",
                "function": {
                    "name": "write_email",
                    "description": "Sends an email.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "recipient": {"type": "string"},
                            "cc": {"type": "array", "items": {"type": "string"}},
                            "subject": {"type": "string"},
                            "body": {"type": "string"},
                        },
                        "required": ["recipient", "subject", "body"],
                        "additionalProperties": False,
                    },
                },
                "strict": True,
            },
        ]
        tool_actions: dict[str, Callable[..., Any]] = {
            "search": search,
            "reflect": reflect,
            "terminate_existance": terminate_existance,
            "plan": plan,
            "write_email": write_email,
        }

        super().__init__(
            description="Free Agent",
            client=client,
            model=grok_3_mini,
            instructions=instructions,
            tools=tools,
            tool_actions=tool_actions,
            model_settings=ModelSettings[AgentContext](
                temperature=1,
                tool_choice="required",
            ),
            max_llm_retries=2,  # Fewer retries for demo agent
            llm_retry_delay=0.5,  # Shorter delay for demo
            max_tool_retries=1,  # Single retry for tools
            tool_retry_delay=0.25,  # Quick retry for tools
        )

    def _is_done(self, response: ChatCompletion) -> bool:
        return False

    def _prepare_messages(self, agent_ctx: AgentContext) -> list[dict[str, Any]]:
        """Prepare messages for LLM request. Override to customize message preparation."""
        if agent_ctx.turn == 0:
            messages = [{"role": "system", "content": self.instructions}]
            if agent_ctx.messages:
                messages.extend(
                    [message for message in agent_ctx.messages if message["content"]]
                )
        else:
            messages = agent_ctx.messages

        return messages


class SpanishAgent(Agent):
    def __init__(self, client: MultiClient | None = None):
        instructions = "Tu eres un assitente útil que solo responde en español"
        tool_actions = {"search": search}

        super().__init__(
            description="Spanish Agent",
            client=client,
            model=gpt_41_nano,
            instructions=instructions,
            tools=tools,
            tool_actions=tool_actions,
            max_llm_retries=2,
            llm_retry_delay=0.5,
            max_tool_retries=1,
            tool_retry_delay=0.25,
        )


class EnglishAgent(Agent):
    def __init__(self, client: MultiClient | None = None):
        instructions = "You are a helpful assistant that only responds in English"
        tool_actions = {"search": search}

        super().__init__(
            description="Spanish Agent",
            client=client,
            model=gpt_41_nano,
            instructions=instructions,
            tools=tools,
            tool_actions=tool_actions,
            max_llm_retries=2,
            llm_retry_delay=0.5,
            max_tool_retries=1,
            tool_retry_delay=0.25,
        )


@dataclass
class MultiAgentContext(AgentContext):
    sub_agent_contexts: dict[str, AgentContext]
    completed_agents: set[str] = field(default_factory=set)


class MultiAgent(BaseAgent[MultiAgentContext]):
    def __init__(
        self,
        sub_agents: list[Agent],
        client: MultiClient | None = None,
    ):
        if not sub_agents:
            raise ValueError("No sub-agents provided")

        self.sub_agents: dict[str, Agent] = {}
        for sub_agent in sub_agents:
            self.sub_agents[sub_agent.name] = sub_agent

        super().__init__(
            description="Multi-Agent Orchestrator",
            client=client,
            model=gpt_41_nano,
            instructions="",
            tools=[],
            tool_actions={},
            context_class=MultiAgentContext,
            max_llm_retries=2,
            llm_retry_delay=0.5,
            max_tool_retries=1,
            tool_retry_delay=0.25,
        )

    def create_multi_agent_context(
        self,
        query: str,
        messages: list[dict[str, Any]] | None = None,
    ) -> MultiAgentContext:
        """Create a MultiAgentContext with properly initialized sub-agent contexts"""
        sub_agent_contexts = {}

        for sub_agent_name in self.sub_agents:
            sub_agent_contexts[sub_agent_name] = AgentContext(
                query=query,
                messages=messages.copy() if messages else [],
                turn=0,
            )

        return MultiAgentContext(
            query=query,
            messages=messages or [],
            turn=0,
            sub_agent_contexts=sub_agent_contexts,
            completed_agents=set(),
        )

    async def run_turn(
        self,
        agent_ctx: MultiAgentContext,
    ) -> TurnCompletion[MultiAgentContext]:
        incomplete_agents = {
            name: sub_ctx
            for name, sub_ctx in agent_ctx.sub_agent_contexts.items()
            if name not in agent_ctx.completed_agents
        }

        if not incomplete_agents:
            # All agents are complete
            return TurnCompletion(
                is_done=True, context=agent_ctx, output="All sub-agents completed"
            )

        # Execute incomplete sub agents in parallel
        execution_ctx = ExecutionContext.current()
        sub_agent_tasks = [
            self.sub_agents[sub_agent_name].execute(sub_agent_ctx, execution_ctx)
            for sub_agent_name, sub_agent_ctx in incomplete_agents.items()
        ]

        # Wait for all sub agents to complete their turns
        results = await asyncio.gather(*sub_agent_tasks, return_exceptions=True)

        # Process results and update contexts
        for (sub_agent_name, _), result in zip(incomplete_agents.items(), results):
            if isinstance(result, Exception):
                logger.error(
                    f"Sub-agent {sub_agent_name} failed: {result}", exc_info=result
                )
                # You might want to handle this differently based on your needs
                continue

            if isinstance(result, TurnCompletion):
                # Update the sub agent context
                agent_ctx.sub_agent_contexts[sub_agent_name] = result.context

                # Mark as completed if the sub agent is done
                if result.is_done:
                    agent_ctx.completed_agents.add(sub_agent_name)

        # Increment turn counter
        agent_ctx.turn += 1

        # Check if all agents are now complete
        all_complete = len(agent_ctx.completed_agents) == len(
            agent_ctx.sub_agent_contexts
        )

        return TurnCompletion(
            is_done=all_complete,
            context=agent_ctx,
            output=f"Completed turn {agent_ctx.turn}. {len(agent_ctx.completed_agents)}/{len(agent_ctx.sub_agent_contexts)} agents finished.",
        )

    async def completion(
        self,
        agent_ctx: MultiAgentContext,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]] | None = None,
        temperature: float | None = None,
        max_completion_tokens: int | None = None,
        tool_choice: str | dict[str, Any] | None = None,
        parallel_tool_calls: bool | None = None,
        model: Model | None = None,
        **kwargs: Any,
    ) -> ChatCompletion:
        return await super().completion(
            agent_ctx,
            messages,
            tools,
            temperature,
            max_completion_tokens,
            tool_choice,
            parallel_tool_calls,
            model,
            **kwargs,
        )

    async def tool_action(
        self,
        tool_call: ChatCompletionMessageToolCall,
        agent_ctx: MultiAgentContext,
    ) -> ToolActionResult:
        # Custom logic for multi-agent tool execution could go here
        return await super().tool_action(tool_call, agent_ctx)

    def get_agent_status(
        self, agent_ctx: MultiAgentContext
    ) -> dict[str, dict[str, Any]]:
        """Get the status of all sub-agents"""
        status = {}
        for name, sub_ctx in agent_ctx.sub_agent_contexts.items():
            status[name] = {
                "completed": name in agent_ctx.completed_agents,
                "turn": sub_ctx.turn,
                "query": sub_ctx.query,
                "message_count": len(sub_ctx.messages),
            }
        return status
