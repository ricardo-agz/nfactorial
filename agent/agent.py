from typing import Any, Callable, Awaitable, Mapping, cast
from dataclasses import dataclass
from openai.types.chat import ChatCompletion, ChatCompletionMessageToolCall
import json
import asyncio

from agent.context import AgentContext
from agent.llms import Model, MultiClient, grok_3_mini, gpt_41_nano


@dataclass
class TurnCompletion:
    response: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {"response": self.response}


class BaseAgent:
    def __init__(
        self,
        client: MultiClient,
        instructions: str,
        tools: list[dict[str, Any]],
        tool_actions: Mapping[str, Callable[..., Any] | Callable[..., Awaitable[Any]]],
        model: Model,
    ):
        self.client = client
        self.instructions = instructions
        self.tools = tools
        self.tool_actions = tool_actions
        self.model = model

        if not all(tool["function"]["name"] in self.tool_actions for tool in self.tools):
            raise ValueError(
                "All tools must have a corresponding action registered in the `tool_actions` argument."
            )

    def init_messages(
        self, message_history: list[dict[str, Any]], query: str
    ) -> list[dict[str, Any]]:
        messages = [{"role": "system", "content": self.instructions}]
        if message_history:
            messages.extend(
                [message for message in message_history if message["content"]]
            )
        messages.append({"role": "user", "content": query})
        return messages

    async def run_turn(self, context: AgentContext) -> tuple[bool, AgentContext]:
        """
        Run a turn of the agent.

        Args:
        - context (AgentContext): The agent context.

        Returns:
        - is_done (bool): Indicates if the turn is complete.
        - context (AgentContext): The updated agent context.
        """
        if context.turn == 0:
            messages = self.init_messages(context.messages, context.query)
        else:
            messages = context.messages

        response = cast(
            ChatCompletion,
            await self.client.completion(
                self.model,
                messages,
                tools=self.tools,
            ),
        )
        response_tool_calls = response.choices[0].message.tool_calls
        response_content = response.choices[0].message.content

        if not response_tool_calls:
            messages.append({"role": "assistant", "content": response_content})
            context.messages = messages
            return True, context

        messages.append(
            {
                "role": "assistant",
                "tool_calls": [tool_call.model_dump() for tool_call in response_tool_calls],  # type: ignore
            }
        )
        task_pairs = [
            (
                tool_call,
                self.run_tool_action(
                    tool_call.function.name,
                    json.loads(tool_call.function.arguments),
                ),
            )
            for tool_call in response_tool_calls
        ]
        results: list[Any | Exception] = await asyncio.gather(
            *[task for _, task in task_pairs], return_exceptions=True
        )

        tool_call_results: list[
            tuple[ChatCompletionMessageToolCall, Any | Exception]
        ] = [(tool_call, result) for (tool_call, _), result in zip(task_pairs, results)]

        for tool_call, result in tool_call_results:
            tool_output_message = {
                "role": "tool",
                "tool_call_id": tool_call.id,
            }
            print("name!")
            tool_name = getattr(tool_call.function, "name")
            print(tool_name)
            if isinstance(result, Exception):
                tool_output_message["content"] = (
                    f'<tool_call_error tool="{tool_name}">\nError running tool:\n{result}\n</tool_call_error>'
                )
            else:
                tool_output_message["content"] = (
                    f'<tool_call_output tool="{tool_name}">\n{result}\n</tool_call_output>'
                )
            messages.append(tool_output_message)

        context.messages = messages
        context.turn += 1

        return False, context

    async def run_completion(self, context: AgentContext):
        pass

    async def run_tool_action(self, tool_name: str, tool_args: dict[str, Any]):
        action = self.tool_actions[tool_name]
        if asyncio.iscoroutinefunction(action):
            return await action(**tool_args)
        else:
            return action(**tool_args)


tools = [
    {
        "type": "function",
        "function": {
            "name": "get_weather",
            "description": "Get current temperature for provided coordinates in celsius.",
            "parameters": {
                "type": "object",
                "properties": {
                    "latitude": {"type": "number"},
                    "longitude": {"type": "number"},
                },
                "required": ["latitude", "longitude"],
                "additionalProperties": False,
            },
        },
        "strict": True,
    }
]


def get_weather(latitude: float, longitude: float) -> int:
    return 25


class DummyAgent(BaseAgent):
    def __init__(self, client: MultiClient):
        instructions = "You are a helpful assistant"
        tool_actions = {"get_weather": get_weather}

        super().__init__(
            client=client,
            model=gpt_41_nano,
            instructions=instructions,
            tools=tools,
            tool_actions=tool_actions,
        )
