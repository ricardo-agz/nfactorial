from dataclasses import dataclass
from enum import Enum
import os
import re
import random
import string
from typing import Any, Callable, cast
from dotenv import load_dotenv
import httpx
import json
from openai import AsyncOpenAI
from openai.types.chat import (
    ChatCompletion,
    ChatCompletionChunk,
    ChatCompletionMessageToolCall,
)
from openai.types.chat.chat_completion_message_tool_call import Function
from openai.types.chat.completion_create_params import ResponseFormat
from openai._streaming import AsyncStream
from pydantic import BaseModel

load_dotenv()


class ToolSupportWrapper:
    def __init__(
        self,
        instructions: Callable[
            [list[dict[str, Any]], dict[str, Any] | str | None, bool], str
        ],
        parser: Callable[[str], tuple[str, list[ChatCompletionMessageToolCall]]],
    ):
        self.instructions = instructions
        self.parser = parser


class Provider(Enum):
    OPENAI = "openai"
    ANTHROPIC = "anthropic"
    XAI = "xai"
    FIREWORKS = "fireworks"


@dataclass
class Model:
    name: str
    provider: Provider
    provider_model_id: str
    context_window: int
    custom_tool_support: ToolSupportWrapper | None = None


class MultiClient:
    def __init__(
        self,
        openai_api_key: str | None = None,
        xai_api_key: str | None = None,
        anthropic_api_key: str | None = None,
        fireworks_api_key: str | None = None,
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
                http_client=self.http_client,
            )
        if xai_api_key or os.environ.get("XAI_API_KEY"):
            self.xai = AsyncOpenAI(
                base_url="https://api.x.ai/v1",
                api_key=xai_api_key or os.environ.get("XAI_API_KEY"),
                http_client=self.http_client,
            )
        if anthropic_api_key or os.environ.get("ANTHROPIC_API_KEY"):
            self.anthropic = AsyncOpenAI(
                base_url="https://api.anthropic.com/v1",
                api_key=anthropic_api_key or os.environ.get("ANTHROPIC_API_KEY"),
                http_client=self.http_client,
            )
        if fireworks_api_key or os.environ.get("FIREWORKS_API_KEY"):
            self.fireworks = AsyncOpenAI(
                base_url="https://api.fireworks.ai/inference/v1",
                api_key=fireworks_api_key or os.environ.get("FIREWORKS_API_KEY"),
                http_client=self.http_client,
            )

    async def _client_completion(
        self,
        model_obj: Model,
        **kwargs: Any,
    ) -> ChatCompletion | AsyncStream[ChatCompletionChunk]:
        if model_obj.provider == Provider.OPENAI:
            if not self.openai:
                raise ValueError("OpenAI client not initialized")
            return await self.openai.chat.completions.create(**kwargs)
        if model_obj.provider == Provider.XAI:
            if not self.xai:
                raise ValueError("XAI client not initialized")
            return await self.xai.chat.completions.create(**kwargs)
        if model_obj.provider == Provider.ANTHROPIC:
            if not self.anthropic:
                raise ValueError("Anthropic client not initialized")
            return await self.anthropic.chat.completions.create(**kwargs)
        if model_obj.provider == Provider.FIREWORKS:
            if not self.fireworks:
                raise ValueError("Fireworks client not initialized")
            return await self.fireworks.chat.completions.create(**kwargs)

        raise ValueError(f"Unsupported provider: {model_obj.provider}")

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

        if max_completion_tokens:
            kwargs["max_completion_tokens"] = max_completion_tokens
        if temperature:
            kwargs["temperature"] = temperature
        if tools and not model.custom_tool_support:
            kwargs["tools"] = tools
        if tool_choice and not model.custom_tool_support:
            kwargs["tool_choice"] = tool_choice
        if parallel_tool_calls is not None and not model.custom_tool_support:
            kwargs["parallel_tool_calls"] = parallel_tool_calls
        if response_format:
            kwargs["response_format"] = response_format

        if stream and model.custom_tool_support:
            raise ValueError(
                "Streaming is not supported for this model when custom tool support is provided"
            )

        if model.custom_tool_support and tools:
            custom_system_message = [
                {
                    "role": "system",
                    "content": model.custom_tool_support.instructions(
                        tools,
                        tool_choice,
                        parallel_tool_calls or True,
                    ),
                }
            ]
            kwargs["messages"] = custom_system_message + messages

        response = await self._client_completion(model, **kwargs)
        if model.custom_tool_support:
            response = cast(ChatCompletion, response)
            for choice in response.choices:
                raw_content = choice.message.content or ""
                content, tool_calls = model.custom_tool_support.parser(raw_content)
                choice.message.content = content
                choice.message.tool_calls = tool_calls

        return response

    async def close(self):
        """Close HTTP clients to clean up connections"""
        if hasattr(self, "openai") and self.openai._client:
            await self.openai._client.aclose()
        if hasattr(self, "xai") and self.xai._client:
            await self.xai._client.aclose()
        if hasattr(self, "anthropic") and self.anthropic._client:
            await self.anthropic._client.aclose()


base_tool_instructions_template = """
In this environment you have access to a set of tools you can use to answer the user's question.

You can invoke functions by writing a "<tool_call>" block like the following as part of your reply to the user:
<tool_call>
{{"name": "TOOL_NAME", "arguments": {{"ARGUMENT_1_NAME": "ARGUMENT_1_VALUE", "ARGUMENT_2_NAME": "ARGUMENT_2_VALUE", ...}}}}
</tool_call>

String and scalar parameters should be specified as is, while lists and objects should use JSON format. Note that spaces for string values are not stripped. The output is not expected to be valid XML and is parsed with regular expressions.

Here are the functions available in JSONSchema format:
<tools>
{TOOLS}
</tools>
{TOOL_CHOICE_INSTRUCTIONS}
"""


def base_tool_instructions(
    tools: list[dict[str, Any]],
    tool_choice: dict[str, Any] | str | None,
    parallel_tool_calls: bool = True,
) -> str:
    if not tools:
        return ""

    tool_choice = tool_choice or "auto"
    tools_json = json.dumps(tools, indent=2)

    if isinstance(tool_choice, dict):
        tool_choice_instructions = f"\nYou are ONLY allowed to use the tool `{tool_choice.get('name')}` for this response."
        if parallel_tool_calls:
            tool_choice_instructions += "\nYou are allowed to call this tool multiple times to be executed in parallel if necessary."
        else:
            tool_choice_instructions += "\nYou are only allowed to call this tool once."
    elif isinstance(tool_choice, str):
        if tool_choice == "required":
            tool_choice_instructions = (
                "\nYou are REQUIRED to use a tool for this response."
            )
            if parallel_tool_calls:
                tool_choice_instructions += "\nYour response MUST invoke at least one tool and you are allowed to call multiple tools to be executed in parallel if necessary."
            else:
                tool_choice_instructions += (
                    "\nYour response MUST invoke exactly one tool."
                )
        elif tool_choice == "auto":
            tool_choice_instructions = "\nDetermine if any tools are required to complete the request. If so, use the appropriate tool(s)."
            if parallel_tool_calls:
                tool_choice_instructions += "\nYou are allowed to call multiple tools to be executed in parallel if necessary."
            else:
                tool_choice_instructions += (
                    "\nYour response may not invoke more than one tool."
                )
        elif tool_choice == "none":
            tool_choice_instructions = (
                "\nYou are NOT allowed to use any tools for this response."
            )
        else:
            raise ValueError(f"Invalid tool choice: {tool_choice}")

    return base_tool_instructions_template.format(
        TOOLS=tools_json,
        TOOL_CHOICE_INSTRUCTIONS=tool_choice_instructions,
    )


def base_tool_parser(response: str) -> tuple[str, list[ChatCompletionMessageToolCall]]:
    # pattern for qwen models
    pattern_a = re.compile(
        r"<tool_call>(?P<json>.*?)</tool_call>", re.DOTALL | re.IGNORECASE
    )

    # pattern for kimi k2
    pattern_b = re.compile(
        r"<\|tool_call_begin\|>(?P<func>[^<]+?)"  # function name + optional id
        r"<\|tool_call_argument_begin\|>(?P<args>.*?)"  # JSON args
        r"<\|tool_call_end\|>",
        re.DOTALL | re.IGNORECASE,
    )

    # pattern for simplified openai format: <|tool_call|>FUNC<|tool_call_argument|>{...}<|tool_call|>
    pattern_c = re.compile(
        r"<\|tool_call\|>(?P<func>[^<]+?)"  # function name + optional id
        r"<\|tool_call_argument\|>(?P<args>.*?)"  # JSON args
        r"<\|tool_call\|>",
        re.DOTALL | re.IGNORECASE,
    )

    # Each entry holds (start_idx, end_idx, parsed_tool_dict)
    extracted: list[tuple[int, int, dict[str, Any]]] = []

    # Legacy matches -------------------------------------------------------
    for m in pattern_a.finditer(response):
        json_str = m.group("json")
        first, last = json_str.find("{"), json_str.rfind("}")
        if first == -1 or last == -1 or last <= first:
            # Malformed – skip
            continue
        try:
            tool_dict = json.loads(json_str[first : last + 1])
            if not isinstance(tool_dict, dict):
                continue
            extracted.append((m.start(), m.end(), tool_dict))
        except json.JSONDecodeError:
            # Ignore malformed JSON so we don't crash the pipeline.
            continue

    # ---------------------------------------------------------------------
    # Fallback for *missing* closing tag ("</tool_call>").
    # We look for the opening "<tool_call>" marker, then try to locate a
    # *balanced* JSON object immediately afterwards. This lets us recover
    # calls that look like:
    #   <tool_call>{ ... }  \n  (no terminator)
    # ---------------------------------------------------------------------
    pattern_incomplete = re.compile(r"<tool_call>\s*", re.IGNORECASE)
    for m in pattern_incomplete.finditer(response):
        start_idx = m.end()
        # Quick skip if this span was already covered by a proper match
        if any(s <= m.start() < e for s, e, _ in extracted):
            continue

        # Scan forward from the first '{' and keep a brace counter to find
        # the matching closing brace. This is more reliable than a regex
        # when nested braces may appear inside strings.
        brace_level = 0
        json_start = None
        json_end = None
        for idx in range(start_idx, len(response)):
            ch = response[idx]
            if ch == "{":
                if brace_level == 0:
                    json_start = idx
                brace_level += 1
            elif ch == "}":
                brace_level -= 1
                if brace_level == 0 and json_start is not None:
                    json_end = idx
                    break
        # If we found a balanced JSON object try to parse it.
        if json_start is not None and json_end is not None:
            json_str = response[json_start : json_end + 1]
            try:
                tool_dict = json.loads(json_str)
                if isinstance(tool_dict, dict):
                    extracted.append((m.start(), json_end + 1, tool_dict))
            except json.JSONDecodeError:
                continue

    # New-style matches ----------------------------------------------------
    for m in pattern_b.finditer(response):
        func_name_part: str = m.group("func").strip()
        # Drop prefix ("functions.") if present and id suffix after ':'
        if func_name_part.startswith("functions."):
            func_name_part = func_name_part[len("functions.") :]
        func_name = func_name_part.split(":", 1)[0]

        args_str = m.group("args").strip()
        try:
            args_dict = json.loads(args_str)
            if not isinstance(args_dict, dict):
                continue
            extracted.append(
                (m.start(), m.end(), {"name": func_name, "arguments": args_dict})
            )
        except json.JSONDecodeError:
            continue

    # Simplified openai style matches -------------------------------------
    for m in pattern_c.finditer(response):
        func_name_c_part: str = m.group("func").strip()
        # Drop prefix ("functions.") if present and id suffix after ':'
        if func_name_c_part.startswith("functions."):
            func_name_c_part = func_name_c_part[len("functions.") :]
        func_name = func_name_c_part.split(":", 1)[0]

        args_str = m.group("args").strip()
        try:
            args_dict = json.loads(args_str)
            if not isinstance(args_dict, dict):
                continue
            extracted.append(
                (m.start(), m.end(), {"name": func_name, "arguments": args_dict})
            )
        except json.JSONDecodeError:
            continue

    if not extracted:
        # Fast-path: no tool calls – just clean up content and return.
        clean_content = (
            response.replace("<think>", "")
            .replace("</think>", "")
            .replace("<|tool_calls_section_begin|>", "")
            .replace("<|tool_calls_section_end|>", "")
            .strip()
        )
        return clean_content, []

    extracted.sort(key=lambda x: x[0])

    content_parts: list[str] = []
    cursor = 0
    tool_dicts: list[dict[str, Any]] = []

    for start, end, tool_dict in extracted:
        # Keep everything before the tool-call markup
        content_parts.append(response[cursor:start])
        cursor = end  # Skip over the markup
        tool_dicts.append(tool_dict)

    # Remainder after the last tool call
    content_parts.append(response[cursor:])

    content = "".join(content_parts)
    content = (
        content.replace("<think>", "")
        .replace("</think>", "")
        .replace("<|tool_calls_section_begin|>", "")
        .replace("<|tool_calls_section_end|>", "")
        .strip()
    )

    def _gen_call_id() -> str:
        return "call_" + "".join(
            random.choices(string.ascii_letters + string.digits, k=8)
        )

    parsed_tool_calls: list[ChatCompletionMessageToolCall] = []
    for tool in tool_dicts:
        try:
            parsed_tool_calls.append(
                ChatCompletionMessageToolCall(
                    id=_gen_call_id(),
                    type="function",
                    function=Function(
                        name=tool["name"],
                        arguments=json.dumps(tool["arguments"]),
                    ),
                )
            )
        except (KeyError, TypeError):
            # Skip malformed entries silently
            continue

    return content, parsed_tool_calls


# ---------- OPENAI MODELS ----------

o3 = Model(
    name="o3",
    provider=Provider.OPENAI,
    provider_model_id="o3",
    context_window=200_000,
)

o4_mini = Model(
    name="o4-mini",
    provider=Provider.OPENAI,
    provider_model_id="o4-mini",
    context_window=200_000,
)

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

# ---------- ANTHROPIC MODELS ----------


claude_4_opus = Model(
    name="claude-4-opus",
    provider=Provider.ANTHROPIC,
    provider_model_id="claude-4-opus-20250219",
    context_window=200_000,
)

claude_4_sonnet = Model(
    name="claude-4-sonnet",
    provider=Provider.ANTHROPIC,
    provider_model_id="claude-sonnet-4-20250514",
    context_window=200_000,
)

claude_37_sonnet = Model(
    name="claude-3.7-sonnet",
    provider=Provider.ANTHROPIC,
    provider_model_id="claude-3.7-sonnet-20250219",
    context_window=200_000,
)

claude_35_sonnet = Model(
    name="claude-3.5-sonnet",
    provider=Provider.ANTHROPIC,
    provider_model_id="claude-3.5-sonnet-20241022",
    context_window=200_000,
)

claude_35_haiku = Model(
    name="claude-3.5-haiku",
    provider=Provider.ANTHROPIC,
    provider_model_id="claude-3.5-haiku-20241022",
    context_window=200_000,
)

# ---------- XAI MODELS ----------

grok_4 = Model(
    name="grok-4",
    provider=Provider.XAI,
    provider_model_id="grok-4",
    context_window=256_000,
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

# ---------- FIREWORKS MODELS ----------

fireworks_kimi_k2 = Model(
    name="kimi-k2-instruct",
    provider=Provider.FIREWORKS,
    provider_model_id="accounts/fireworks/models/kimi-k2-instruct",
    context_window=128_000,
)


fireworks_qwen_3_235b = Model(
    name="qwen-3-235b",
    provider=Provider.FIREWORKS,
    provider_model_id="accounts/fireworks/models/qwen3-235b-a22b",
    context_window=128_000,
    custom_tool_support=ToolSupportWrapper(
        instructions=base_tool_instructions,
        parser=base_tool_parser,
    ),
)

fireworks_qwen_3_coder_480b = Model(
    name="qwen-3-coder-480b",
    provider=Provider.FIREWORKS,
    provider_model_id="accounts/fireworks/models/qwen3-coder-480b-a35b-instruct",
    context_window=256_000,
    custom_tool_support=ToolSupportWrapper(
        instructions=base_tool_instructions,
        parser=base_tool_parser,
    ),
)
