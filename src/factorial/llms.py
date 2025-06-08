from dataclasses import dataclass
from enum import Enum
import os
from typing import Any
from dotenv import load_dotenv
import httpx
from openai import AsyncOpenAI
from openai.types.chat import ChatCompletion, ChatCompletionChunk
from openai.types.chat.completion_create_params import ResponseFormat
from openai._streaming import AsyncStream
from pydantic import BaseModel

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
        self,
        openai_api_key: str | None = None,
        xai_api_key: str | None = None,
        http_client: httpx.AsyncClient | None = None,
        max_connections: int = 1500,
        max_keepalive_connections: int = 1000,
        timeout: float = 120.0,
    ):
        if http_client:
            self.http_client = http_client
        else:
            # Configure HTTP client with proper connection pooling
            self.http_client = httpx.AsyncClient(
                limits=httpx.Limits(
                    max_connections=max_connections,
                    max_keepalive_connections=max_keepalive_connections,
                ),
                timeout=httpx.Timeout(timeout),
            )

        if openai_api_key or os.environ.get("OPENAI_API_KEY"):
            self.openai = AsyncOpenAI(
                api_key=openai_api_key or os.environ.get("OPENAI_API_KEY"),
                http_client=http_client,
            )
        if xai_api_key or os.environ.get("XAI_API_KEY"):
            # Create separate HTTP client for XAI to avoid conflicts
            xai_http_client = httpx.AsyncClient(
                limits=httpx.Limits(
                    max_connections=max_connections,
                    max_keepalive_connections=max_keepalive_connections,
                ),
                timeout=httpx.Timeout(timeout),
            )
            self.xai = AsyncOpenAI(
                base_url="https://api.x.ai/v1",
                api_key=xai_api_key or os.environ.get("XAI_API_KEY"),
                http_client=xai_http_client,
            )

    async def completion(
        self,
        model: Model,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]] | None = None,
        tool_choice: str | dict[str, Any] | None = None,
        max_completion_tokens: int | None = None,
        temperature: float | None = None,
        stream: bool = False,
        parallel_tool_calls: bool | None = None,
        response_format: ResponseFormat | type[BaseModel] | None = None,
        **kwargs: Any,
    ) -> ChatCompletion | AsyncStream[ChatCompletionChunk]:
        kwargs["model"] = model.provider_model_id
        kwargs["messages"] = messages
        kwargs["stream"] = stream

        if tools:
            kwargs["tools"] = tools
        if max_completion_tokens:
            kwargs["max_completion_tokens"] = max_completion_tokens
        if temperature:
            kwargs["temperature"] = temperature
        if tool_choice:
            kwargs["tool_choice"] = tool_choice
        if parallel_tool_calls is not None:
            kwargs["parallel_tool_calls"] = parallel_tool_calls
        if response_format:
            kwargs["response_format"] = response_format

        if model.provider == Provider.OPENAI:
            return await self.openai.chat.completions.create(**kwargs)
        if model.provider == Provider.XAI:
            return await self.xai.chat.completions.create(**kwargs)

        raise ValueError(f"Unsupported provider: {model.provider}")

    async def close(self):
        """Close HTTP clients to clean up connections"""
        if hasattr(self, "openai") and self.openai._client:
            await self.openai._client.aclose()
        if hasattr(self, "xai") and self.xai._client:
            await self.xai._client.aclose()


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
