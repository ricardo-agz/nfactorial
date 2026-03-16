import os
from collections.abc import Callable
from typing import Any

import httpx
from dotenv import load_dotenv
from pydantic import BaseModel

from factorial import Agent, Model, MultiClient, any_of, no_tool_calls, turn_count_is
from factorial.agent.context import AgentContext
from factorial.agent.types import Turn

from .tools.file import file_tools
from .tools.project import project_tools
from .tools.search import search_tools
from .tools.thinking import design_doc, plan, think

current_dir = os.path.dirname(os.path.abspath(__file__))
env_path = os.path.join(current_dir, ".env")


load_dotenv(env_path, override=True)


PROMPT_ENDPOINT = os.getenv(
    "NFACTORIAL_PROMPT_ENDPOINT", "http://localhost:8081/agent-prompt"
)


def _fetch_prompt(mode: str, model: str, timeout: float = 30.0) -> str:
    with httpx.Client(timeout=timeout) as client:
        response = client.get(
            PROMPT_ENDPOINT, params={"agent_mode": mode, "model": model}
        )
        response.raise_for_status()
        return response.text


class FinalOutput(BaseModel):
    done: bool
    run_commands: list[str]


def _cli_prepare_turn(turn: Turn[Any], agent_ctx: AgentContext[Any, Any]) -> None:
    if agent_ctx.turn_number == 1:
        turn.tool_choice = {"type": "function", "function": {"name": "think"}}
    elif agent_ctx.turn_number == 2:
        turn.tool_choice = {"type": "function", "function": {"name": "tree"}}
    else:
        turn.tool_choice = "required"


class NFactorialAgent:
    """Factory for creating NFactorial CLI agents."""

    def __init__(
        self,
        mode: str,
        model: Model | Callable[[Any], Model],
        client: MultiClient,
    ):
        self._agent = create_nfactorial_agent(mode=mode, model=model, client=client)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._agent, name)


def create_nfactorial_agent(
    mode: str,
    model: Model | Callable[[Any], Model],
    client: MultiClient,
) -> Agent[Any, Any]:
    """Create an NFactorial CLI agent for the given mode and model."""
    model_name = model.name if isinstance(model, Model) else "default"
    if mode == "create":
        instructions = _fetch_prompt("create", model_name)
    elif mode == "edit":
        instructions = _fetch_prompt("edit", model_name)
    else:
        raise ValueError(f"Invalid mode: {mode}")

    return Agent(
        name="nfactorial_cli_agent",
        instructions=instructions,
        tools=[
            *file_tools,
            *project_tools,
            *search_tools,
            *[think, plan, design_doc],
        ],
        model=model,
        prepare_turn=_cli_prepare_turn,
        client=client,
        stop_when=any_of(no_tool_calls(), turn_count_is(60)),
    )
