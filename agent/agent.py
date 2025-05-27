from typing import (
    Any,
    Callable,
    cast,
    final,
    Generic,
    TypeVar,
)
from dataclasses import dataclass
from pydantic import BaseModel
import json
import asyncio
from typing import Any, cast
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

from agent.context import Task, AgentContext, ContextType
from agent.llms import Model, MultiClient, grok_3_mini, gpt_41_nano
from agent.utils import to_snake_case
from agent.events import EventPublisher, AgentEvent, EventType

# Add a type variable for the context type in ModelSettings
ContextT = TypeVar("ContextT", bound=AgentContext)


class RetryableError(Exception):
    """Error that should be retried"""

    pass


class NonRetryableError(Exception):
    """Error that should not be retried"""

    pass


@dataclass
class TurnMeta:
    task_id: str
    owner_id: str
    retries: int
    turn: int


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
        model: Model,
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
        self.model = model
        self.client = client or MultiClient()
        self.event_publisher: EventPublisher | None = None
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
        if not all(
            tool["function"]["name"] in self.tool_actions for tool in self.tools
        ):
            raise ValueError(
                "All tools must have a corresponding action registered in the `tool_actions` argument."
            )

        if self.output_type:
            final_output_tool = create_final_output_tool(self.output_type)
            # self.tools = list(self.tools)
            self.tools.append(final_output_tool)

    def set_event_publisher(self, publisher: EventPublisher):
        self.event_publisher = publisher

    async def _publish_event(self, event: AgentEvent):
        if self.event_publisher:
            await self.event_publisher.publish_event(event)

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

    def _prepare_messages(self, context: ContextType) -> list[dict[str, Any]]:
        """Prepare messages for LLM request. Override to customize message preparation."""
        if context.turn == 0:
            messages = [{"role": "system", "content": self.instructions}]
            if context.messages:
                messages.extend(
                    [message for message in context.messages if message["content"]]
                )
            messages.append({"role": "user", "content": context.query})
        else:
            messages = context.messages

        return messages

    async def completion(
        self,
        model: Model,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]] = [],
        context: ContextType | None = None,
        temperature: float | None = None,
        max_completion_tokens: int | None = None,
        tool_choice: str | dict[str, Any] | None = None,
        parallel_tool_calls: bool | None = None,
        **kwargs: Any,
    ) -> ChatCompletion:
        """Execute LLM completion. Override to customize LLM interaction."""

        completion_kwargs: dict[str, Any] = {}
        if temperature is not None:
            completion_kwargs["temperature"] = temperature
        if max_completion_tokens is not None:
            completion_kwargs["max_completion_tokens"] = max_completion_tokens
        if tool_choice is not None:
            completion_kwargs["tool_choice"] = tool_choice
        if parallel_tool_calls is not None:
            completion_kwargs["parallel_tool_calls"] = parallel_tool_calls

        response = cast(
            ChatCompletion,
            await self.client.completion(
                model,
                messages,
                tools=tools,
                **completion_kwargs,
            ),
        )

        return response

    async def _completion_with_retry(
        self,
        model: Model,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]] = [],
        meta: TurnMeta | None = None,
        **kwargs: Any,
    ) -> ChatCompletion:
        """Execute LLM completion with retry logic and error handling."""
        last_exception = None

        for attempt in range(self.max_llm_retries + 1):
            try:
                response = await self.completion(
                    model,
                    messages,
                    tools=tools,
                    **kwargs,
                )
                return response

            except Exception as e:
                last_exception = e

                is_retryable = isinstance(
                    e,
                    (
                        RateLimitError,
                        InternalServerError,
                        APITimeoutError,
                        APIConnectionError,
                        RetryableError,
                    ),
                )

                if is_retryable and attempt < self.max_llm_retries:
                    delay = self.llm_retry_delay * (2**attempt)

                    await self._publish_event(
                        AgentEvent(
                            event_type=EventType.LLM_REQUEST_RETRIED,
                            task_id=meta.task_id if meta else None,
                            owner_id=meta.owner_id if meta else None,
                            agent_name=self.name,
                            data={
                                "attempt": attempt + 1,
                                "max_retries": self.max_llm_retries,
                                "delay": delay,
                                "model": (
                                    model.provider_model_id
                                    if hasattr(model, "value")
                                    else str(model)
                                ),
                            },
                            error=str(e),
                        )
                    )

                    await asyncio.sleep(delay)
                else:
                    break

        raise last_exception or Exception("LLM completion failed after all retries")

    async def tool_action(
        self, tool_name: str, tool_args: dict[str, Any] | str
    ) -> tuple[str, Any]:
        """Execute a tool action. Override to customize individual tool execution."""
        action = self.tool_actions[tool_name]

        if asyncio.iscoroutinefunction(action):
            if isinstance(tool_args, str):
                return await action(tool_args)
            else:
                return await action(**tool_args)
        else:
            if isinstance(tool_args, str):
                return action(tool_args)  # type: ignore
            else:
                return action(**tool_args)  # type: ignore

    async def _tool_action_with_retry(
        self,
        tool_name: str,
        tool_args: dict[str, Any] | str,
        meta: TurnMeta | None = None,
    ) -> tuple[str, Any]:
        """Execute a tool action with retry logic and error handling."""
        last_exception = None

        for attempt in range(self.max_tool_retries + 1):
            try:
                result = await self.tool_action(tool_name, tool_args)
                return result

            except Exception as e:
                last_exception = e

                if attempt < self.max_tool_retries:
                    delay = self.tool_retry_delay * (2**attempt)

                    await self._publish_event(
                        AgentEvent(
                            event_type=EventType.TOOL_CALL_RETRIED,
                            task_id=meta.task_id if meta else None,
                            owner_id=meta.owner_id if meta else None,
                            agent_name=self.name,
                            data={
                                "tool_name": tool_name,
                                "attempt": attempt + 1,
                                "max_retries": self.max_tool_retries,
                                "delay": delay,
                                "arguments": tool_args,
                            },
                            error=str(e),
                        )
                    )

                    await asyncio.sleep(delay)
                else:
                    break

        raise last_exception or Exception(f"Tool {tool_name} failed after all retries")

    def _is_final_turn(self, response: ChatCompletion) -> bool:
        """Check if this turn should be the final one."""
        tool_calls = response.choices[0].message.tool_calls
        if not tool_calls:
            return True

        return any(
            tool_call.function.name == "final_output" for tool_call in tool_calls
        )

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

    async def run_turn(
        self,
        context: ContextType,
        meta: TurnMeta | None = None,
    ) -> TurnCompletion[ContextType]:
        """
        Run a turn of the agent.

        Args:
        - context (ContextType): The agent context.

        Returns:
        - is_done (bool): Indicates if the turn is complete.
        - context (ContextType): The updated agent context.
        """

        messages = self._prepare_messages(context)
        response = await self._execute_completion(context, messages, meta)
        response_tool_calls = response.choices[0].message.tool_calls
        response_content = response.choices[0].message.content

        messages.append(
            {
                "role": "assistant",
                "content": response_content,
                "tool_calls": [
                    tool_call.model_dump() for tool_call in response_tool_calls
                ]
                if response_tool_calls
                else None,
            }
        )

        is_final_turn = self._is_final_turn(response)
        if is_final_turn:
            output = self._extract_output(response)
            context.messages = messages
            return TurnCompletion(is_done=True, context=context, output=output)

        if response_tool_calls:
            messages = await self._handle_tool_response(
                messages, response_tool_calls, meta
            )

        context.messages = messages
        context.turn += 1

        return TurnCompletion(is_done=False, context=context)

    def create_tool_result_message(
        self,
        tool_call: ChatCompletionMessageToolCall,
        result: tuple[str, Any] | Exception,
    ) -> dict[str, Any]:
        """Format tool result into message. Override to customize tool result formatting."""
        tool_output_message = {
            "role": "tool",
            "tool_call_id": tool_call.id,
        }
        tool_name = tool_call.function.name

        if isinstance(result, Exception):
            tool_output_message["content"] = (
                f'<tool_call_error tool="{tool_name}">\nError running tool:\n{result}\n</tool_call_error>'
            )
        else:
            tool_output_message["content"] = (
                f'<tool_call_output tool="{tool_name}">\n{result[0]}\n</tool_call_output>'
            )

        return tool_output_message

    # ===== Internal Methods ===== #

    def _should_complete(self, context: ContextType, response: ChatCompletion) -> bool:
        """Check if the agent should complete."""
        response_tool_calls = response.choices[0].message.tool_calls
        should_complete = not response_tool_calls or "final_output" in [
            tool_call.function.name for tool_call in response_tool_calls
        ]
        return should_complete

    def _last_turn(self, context: ContextType) -> bool:
        return context.turn == self.max_turns

    def _resolve_model_settings(self, context: ContextType) -> dict[str, Any]:
        """Get resolved model settings for the current context."""
        if not self.model_settings:
            return {}

        resolved_settings: dict[str, Any] = {}

        # Handle temperature
        if self.model_settings.temperature is not None:
            if callable(self.model_settings.temperature):
                resolved_settings["temperature"] = self.model_settings.temperature(
                    context
                )
            else:
                resolved_settings["temperature"] = self.model_settings.temperature

        # Handle tool_choice
        if self.model_settings.tool_choice is not None:
            if callable(self.model_settings.tool_choice):
                tool_choice_result = self.model_settings.tool_choice(context)
                resolved_settings["tool_choice"] = tool_choice_result
            else:
                resolved_settings["tool_choice"] = self.model_settings.tool_choice

        # Handle parallel_tool_calls
        if self.model_settings.parallel_tool_calls is not None:
            if callable(self.model_settings.parallel_tool_calls):
                resolved_settings["parallel_tool_calls"] = (
                    self.model_settings.parallel_tool_calls(context)
                )
            else:
                resolved_settings["parallel_tool_calls"] = (
                    self.model_settings.parallel_tool_calls
                )

        # Handle max_completion_tokens
        if self.model_settings.max_completion_tokens is not None:
            if callable(self.model_settings.max_completion_tokens):
                resolved_settings["max_completion_tokens"] = (
                    self.model_settings.max_completion_tokens(context)
                )
            else:
                resolved_settings["max_completion_tokens"] = (
                    self.model_settings.max_completion_tokens
                )

        return resolved_settings

    @final
    async def execute_turn(
        self, task: Task[ContextType]
    ) -> TurnCompletion[ContextType]:
        await self._publish_event(
            AgentEvent(
                event_type=EventType.TURN_STARTED,
                task_id=task.id,
                owner_id=task.owner_id,
                agent_name=self.name,
                turn=task.payload.turn,
            )
        )

        meta = TurnMeta(
            task_id=task.id,
            owner_id=task.owner_id,
            retries=task.retries,
            turn=task.payload.turn,
        )

        try:
            response = await self.run_turn(context=task.payload, meta=meta)

            await self._publish_event(
                AgentEvent(
                    event_type=EventType.TURN_COMPLETED,
                    task_id=task.id,
                    owner_id=task.owner_id,
                    agent_name=self.name,
                    turn=task.payload.turn,
                )
            )

            if response.is_done:
                await self._publish_event(
                    AgentEvent(
                        event_type=EventType.AGENT_OUTPUT,
                        task_id=task.id,
                        owner_id=task.owner_id,
                        agent_name=self.name,
                        turn=task.payload.turn,
                        data=response.output,
                    )
                )
                return response

            return response

        except Exception as e:
            await self._publish_event(
                AgentEvent(
                    event_type=EventType.TURN_FAILED,
                    task_id=task.id,
                    owner_id=task.owner_id,
                    agent_name=self.name,
                    turn=task.payload.turn,
                    error=str(e),
                )
            )

            # Re-raise the exception to be handled by the task system
            raise e

    @final
    async def _execute_completion(
        self,
        context: ContextType,
        messages: list[dict[str, Any]],
        meta: TurnMeta | None = None,
    ) -> ChatCompletion:
        await self._publish_event(
            AgentEvent(
                event_type=EventType.LLM_REQUEST_STARTED,
                task_id=meta.task_id if meta else None,
                owner_id=meta.owner_id if meta else None,
                agent_name=self.name,
                turn=context.turn,
            )
        )

        try:
            model_settings = self._resolve_model_settings(context)

            resolved_tool_choice = model_settings.get("tool_choice")
            if self.output_type and resolved_tool_choice in ["none", "auto", None]:
                model_settings["tool_choice"] = "required"

            if self._last_turn(context):
                model_settings["tool_choice"] = (
                    {"type": "function", "function": {"name": "final_output"}}
                    if self.output_type
                    else "none"
                )
                model_settings["parallel_tool_calls"] = False

            response = await self._completion_with_retry(
                self.model,
                messages,
                tools=self.tools,
                meta=meta,
                **model_settings,
            )

            await self._publish_event(
                AgentEvent(
                    event_type=EventType.LLM_RESPONSE_RECEIVED,
                    task_id=meta.task_id if meta else None,
                    owner_id=meta.owner_id if meta else None,
                    agent_name=self.name,
                    turn=context.turn,
                    data=response.model_dump(),  # type: ignore
                )
            )

            return response

        except Exception as e:
            await self._publish_event(
                AgentEvent(
                    event_type=EventType.LLM_REQUEST_FAILED,
                    task_id=meta.task_id if meta else None,
                    owner_id=meta.owner_id if meta else None,
                    agent_name=self.name,
                    data={
                        "total_attempts": self.max_llm_retries + 1,
                        "model": (
                            self.model.provider_model_id
                            if hasattr(self.model, "value")
                            else str(self.model)
                        ),
                    },
                    error=str(e),
                )
            )

            raise e

    @final
    async def _execute_tool_call(
        self, tool_call: ChatCompletionMessageToolCall, meta: TurnMeta | None = None
    ) -> Any | Exception:
        """Execute a single tool call. Override to customize tool execution."""
        await self._publish_event(
            AgentEvent(
                event_type=EventType.TOOL_CALL_STARTED,
                task_id=meta.task_id if meta else None,
                owner_id=meta.owner_id if meta else None,
                agent_name=self.name,
                turn=meta.turn if meta else None,
                data={
                    "tool_name": tool_call.function.name,
                    "tool_call_id": tool_call.id,
                    "arguments": tool_call.function.arguments,
                },
            )
        )

        try:
            tool_args = (
                json.loads(tool_call.function.arguments)
                if self.parse_tool_args
                else tool_call.function.arguments
            )
        except json.JSONDecodeError as e:
            error_msg = f"Invalid JSON in tool arguments: {e}"
            await self._publish_event(
                AgentEvent(
                    event_type=EventType.TOOL_CALL_FAILED,
                    task_id=meta.task_id if meta else None,
                    owner_id=meta.owner_id if meta else None,
                    agent_name=self.name,
                    turn=meta.turn if meta else None,
                    data={
                        "tool_name": tool_call.function.name,
                        "tool_call_id": tool_call.id,
                        "failure_reason": "json_parse_error",
                    },
                    error=error_msg,
                )
            )
            return Exception(error_msg)

        # Execute the tool action (which now includes retry logic)
        result = await self._tool_action_with_retry(
            tool_call.function.name, tool_args, meta
        )

        # Check if the result is an exception (indicating failure after retries)
        if isinstance(result, Exception):
            await self._publish_event(
                AgentEvent(
                    event_type=EventType.TOOL_CALL_FAILED,
                    task_id=meta.task_id if meta else None,
                    owner_id=meta.owner_id if meta else None,
                    agent_name=self.name,
                    turn=meta.turn if meta else None,
                    data={
                        "tool_name": tool_call.function.name,
                        "tool_call_id": tool_call.id,
                        "failure_reason": "execution_failed_after_retries",
                        "total_attempts": self.max_tool_retries + 1,
                    },
                    error=str(result),
                )
            )
            return result
        else:
            # Success case
            await self._publish_event(
                AgentEvent(
                    event_type=EventType.TOOL_CALL_COMPLETED,
                    task_id=meta.task_id if meta else None,
                    owner_id=meta.owner_id if meta else None,
                    agent_name=self.name,
                    turn=meta.turn if meta else None,
                    data={
                        "tool_name": tool_call.function.name,
                        "tool_call_id": tool_call.id,
                        "result": result[1],
                    },
                )
            )
            return result

    @final
    async def _execute_tool_calls(
        self,
        tool_calls: list[ChatCompletionMessageToolCall],
        meta: TurnMeta | None = None,
    ) -> list[tuple[ChatCompletionMessageToolCall, Any | Exception]]:
        results: list[Any | Exception] = await asyncio.gather(
            *[self._execute_tool_call(tool_call, meta) for tool_call in tool_calls],
            return_exceptions=True,
        )

        return list(zip(tool_calls, results))

    @final
    async def _handle_tool_response(
        self,
        messages: list[dict[str, Any]],
        tool_calls: list[ChatCompletionMessageToolCall],
        meta: TurnMeta | None = None,
    ) -> list[dict[str, Any]]:
        """Handle response with tool calls. Override to customize tool response handling."""
        tool_call_results = await self._execute_tool_calls(tool_calls, meta)

        for tool_call, result in tool_call_results:
            tool_message = self.create_tool_result_message(tool_call, result)
            messages.append(tool_message)

        return messages


class Agent(BaseAgent[AgentContext]):
    """Base agent class that uses AgentContext by default. Use this for simple agents."""

    pass


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


def plan(overview: str, steps: list[str]) -> tuple[str, dict[str, Any]]:
    return f"{overview}\n{' -> '.join(steps)}", {"overview": overview, "steps": steps}


def reflect(reflection: str) -> tuple[str, str]:
    return reflection, reflection


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
                tool_choice=lambda context: {
                    "type": "function",
                    "function": {"name": "plan"},
                }
                if context.turn == 0
                else "auto",
            ),
            output_type=FinalOutput,
            max_llm_retries=2,  # Fewer retries for demo agent
            llm_retry_delay=0.5,  # Shorter delay for demo
            max_tool_retries=1,  # Single retry for tools
            tool_retry_delay=0.25,  # Quick retry for tools
        )


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
        self, context: MultiAgentContext, meta: TurnMeta | None = None
    ) -> tuple[bool, MultiAgentContext]:
        return await super().run_turn(context, meta)
