from dataclasses import dataclass
from enum import Enum
import os
from typing import Any
from dotenv import load_dotenv
from openai import AsyncOpenAI
from openai.types.chat import ChatCompletion, ChatCompletionChunk
from openai._streaming import AsyncStream

load_dotenv()


class Provider(Enum):
    OPENAI = "openai"
    XAI = "xai"


@dataclass
class Model:
    name: str
    provider: Provider
    provider_model_id: str
    context_window: int


class MultiClient:
    def __init__(
        self, openai_api_key: str | None = None, xai_api_key: str | None = None
    ):
        if openai_api_key or os.environ.get("OPENAI_API_KEY"):
            self.openai = AsyncOpenAI(
                api_key=openai_api_key or os.environ.get("OPENAI_API_KEY")
            )
        if xai_api_key or os.environ.get("XAI_API_KEY"):
            self.xai = AsyncOpenAI(
                base_url="https://api.x.ai/v1",
                api_key=xai_api_key or os.environ.get("XAI_API_KEY"),
            )

    async def completion(
        self,
        model: Model,
        messages: list[dict[str, str]],
        max_completion_tokens: int | None = None,
        temperature: float | None = None,
        stream: bool = False,
        **kwargs: Any,
    ) -> ChatCompletion | AsyncStream[ChatCompletionChunk]:
        kwargs["model"] = model.provider_model_id
        kwargs["messages"] = messages
        kwargs["stream"] = stream

        if max_completion_tokens:
            kwargs["max_completion_tokens"] = max_completion_tokens
        if temperature:
            kwargs["temperature"] = temperature

        if model.provider == Provider.OPENAI:
            return await self.openai.chat.completions.create(**kwargs)
        if model.provider == Provider.XAI:
            return await self.xai.chat.completions.create(**kwargs)

        raise ValueError(f"Unsupported provider: {model.provider}")


gpt_41 = Model(
    name="gpt-4.1",
    provider=Provider.OPENAI,
    provider_model_id="gpt-4.1",
    context_window=1_047_576,
)

gpt_41_mini = Model(
    name="gpt-4.1-mini",
    provider=Provider.OPENAI,
    provider_model_id="gpt-4.1-mini",
    context_window=1_047_576,
)

gpt_41_nano = Model(
    name="gpt-4.1-nano",
    provider=Provider.OPENAI,
    provider_model_id="gpt-4.1-nano",
    context_window=1_047_576,
)

grok_3 = Model(
    name="grok-3",
    provider=Provider.XAI,
    provider_model_id="grok-3",
    context_window=1_047_576,
)

grok_3_mini = Model(
    name="grok-3-mini",
    provider=Provider.XAI,
    provider_model_id="grok-3-mini",
    context_window=1_047_576,
)
