from __future__ import annotations

import json
import time
import uuid
from typing import Any

from openai.types.chat import ChatCompletion, ChatCompletionMessage
from openai.types.chat.chat_completion import Choice
from openai.types.completion_usage import CompletionUsage

from factorial import Agent, Model, Provider, tool

BASE_MODEL = Model(
    name="fixture-base-model",
    provider=Provider.OPENAI,
    provider_model_id="fixture-base-v1",
    context_window=128000,
)


class PrepareTurnEchoClient:
    async def completion(
        self,
        model: Model | str,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]] | None = None,
        temperature: float | None = None,
        max_completion_tokens: int | None = None,
        tool_choice: str | dict[str, Any] | None = None,
        parallel_tool_calls: bool | None = None,
        stream: bool = False,
    ) -> ChatCompletion:
        del stream
        model_name = model.name if isinstance(model, Model) else str(model)
        content = json.dumps(
            {
                "model": model_name,
                "messages": messages,
                "tool_count": len(tools or []),
                "tool_choice": tool_choice,
                "parallel_tool_calls": parallel_tool_calls,
                "temperature": temperature,
                "max_completion_tokens": max_completion_tokens,
            },
            separators=(",", ":"),
            sort_keys=True,
        )
        return ChatCompletion(
            id=f"chatcmpl-{uuid.uuid4().hex[:12]}",
            model=model_name,
            created=int(time.time()),
            object="chat.completion",
            choices=[
                Choice(
                    index=0,
                    message=ChatCompletionMessage(
                        role="assistant",
                        content=content,
                    ),
                    finish_reason="stop",
                )
            ],
            usage=CompletionUsage(
                prompt_tokens=100,
                completion_tokens=50,
                total_tokens=150,
            ),
        )


@tool
def unused_tool() -> str:
    return "unused"


def _prepare_turn(turn: Any, agent_ctx: Any, execution_ctx: Any) -> None:
    del agent_ctx, execution_ctx
    turn.model = Model(
        name="override-model",
        provider=Provider.OPENAI,
        provider_model_id="override-v1",
        context_window=64000,
    )
    turn.messages = [
        {"role": "system", "content": "Runtime-compacted prompt."},
        {"role": "user", "content": "Only send this input."},
    ]
    turn.tools = []
    turn.tool_choice = "required"
    turn.parallel_tool_calls = False
    turn.temperature = 0.1
    turn.max_output_tokens = 32


prepare_turn_agent = Agent[Any, Any](
    name="prepare_turn_agent",
    instructions="Echo the fully prepared request.",
    tools=[unused_tool],
    client=PrepareTurnEchoClient(),
    model=BASE_MODEL,
    prepare_turn=_prepare_turn,
)
