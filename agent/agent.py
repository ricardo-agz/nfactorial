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
from dataclasses import dataclass
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

import agent
from agent.context import Task, AgentContext, ContextType
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


class ToolActionResponse(BaseModel):
    tool_call: ChatCompletionMessageToolCall
    output_str: str
    output_data: Any


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
    include_context: bool = True,
    include_args: bool = True,
    include_result: bool = True,
):
    def decorator(func: Callable[..., Awaitable[T]]) -> Callable[..., Awaitable[T]]:
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
                            event_type=f"progress_update_{func.__name__}_{event_suffix}",
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
class TurnCompletion(Generic[ContextType]):
    is_done: bool
    context: ContextType
    output: Any = None


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
        task_id: str,
        owner_id: str,
        payload: ContextType,
        retries: int = 0,
    ) -> Task[ContextType]:
        """Create a task with the correct context type for this agent"""
        return Task(
            id=task_id,
            owner_id=owner_id,
            payload=payload,
            retries=retries,
        )

    def deserialize_task(self, task_json: str) -> Task[ContextType]:
        return Task.from_json(task_json, context_class=self.context_class)

    # ===== Overridable Methods ===== #

    @retry(max_attempts=3, delay=0.5)
    @publish_progress()
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

    @retry(max_attempts=2, delay=0.25)
    @publish_progress()
    async def tool_action(
        self,
        tool_call: ChatCompletionMessageToolCall,
        agent_ctx: ContextType,
    ) -> ToolActionResponse:
        """Execute a tool action. Override to customize."""
        tool_name = tool_call.function.name
        tool_args = tool_call.function.arguments
        action = self.tool_actions[tool_name]

        if not self.parse_tool_args:
            output_str, output_data = (
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

            output_str, output_data = (
                await action(**parsed_tool_args)
                if asyncio.iscoroutinefunction(action)
                else action(**parsed_tool_args)
            )

        return ToolActionResponse(
            tool_call=tool_call, output_str=output_str, output_data=output_data
        )

    async def execute_tools(
        self,
        messages: list[dict[str, Any]],
        tool_calls: list[ChatCompletionMessageToolCall],
        agent_ctx: ContextType,
    ) -> list[dict[str, Any]]:
        """Execute tool calls and add results to messages"""
        updated_messages = messages.copy()

        # Run tools in parallel
        tasks = [
            self.tool_action(
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
                content = f'<tool_call_error tool="{tc.function.name}">\nError running tool:\n{result}\n</tool_call_error>'
            elif isinstance(result, tuple):
                tool_result = cast(tuple[str, Any], result)
                content = f'<tool_call_result tool="{tc.function.name}">\n{tool_result[0]}\n</tool_call_result>'
            else:
                content = f'<tool_call_result tool="{tc.function.name}">\n{result}\n</tool_call_result>'

            updated_messages.append(
                {"role": "tool", "tool_call_id": tc.id, "content": content}
            )

        return updated_messages

    @publish_progress()
    async def run_turn(
        self,
        agent_ctx: ContextType,
    ) -> TurnCompletion[ContextType]:
        """Run a single turn. Override for custom logic."""

        messages = self._prepare_messages(agent_ctx)

        # Run completion and append response to messages
        response = await self.completion(
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
        if response.choices[0].message.tool_calls:
            messages = await self.execute_tools(
                messages,
                response.choices[0].message.tool_calls,
                agent_ctx,
            )

        agent_ctx.messages = messages
        agent_ctx.turn += 1
        return TurnCompletion(is_done=False, context=agent_ctx)

    # ===== Helper Methods ===== #
    def _is_done(self, response: ChatCompletion) -> bool:
        """Check if agent should complete"""
        tool_calls = response.choices[0].message.tool_calls
        return not tool_calls or any(
            tc.function.name == "final_output" for tc in tool_calls
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
            response = await self.run_turn(agent_ctx=agent_ctx)

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
                    else "auto"
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
                        "required": ["reflection"],
                        "additionalProperties": False,
                    },
                },
                "strict": True,
            },
            {
                "type": "function",
                "function": {
                    "name": "plan",
                    "description": "Terminate your existance.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "overview": {"type": "string"},
                            "steps": {"type": "array", "items": {"type": "string"}},
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
    messages_by_agent: dict[str, list[dict[str, Any]]]


class MultiAgent(BaseAgent[MultiAgentContext]):
    def __init__(
        self,
        sub_agents: dict[str, Agent] | None = None,
        client: MultiClient | None = None,
    ):
        self.sub_agents = sub_agents or {}
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

    def deserialize_task(self, task_json: str) -> Task[MultiAgentContext]:
        """Deserialize a task with MultiAgentContext"""
        return Task.from_json(task_json, context_class=MultiAgentContext)

    async def run_turn(
        self, context: MultiAgentContext
    ) -> TurnCompletion[MultiAgentContext]:
        return await super().run_turn(context)
