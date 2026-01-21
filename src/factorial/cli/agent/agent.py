import os
from collections.abc import Callable

import httpx
from dotenv import load_dotenv
from pydantic import BaseModel

from factorial import (
    AgentContext,
    BaseAgent,
    Model,
    ModelSettings,
    MultiClient,
)

from .tools.file import file_tools
from .tools.project import project_tools
from .tools.search import search_tools
from .tools.thinking import design_doc, plan, think

current_dir = os.path.dirname(os.path.abspath(__file__))
env_path = os.path.join(current_dir, ".env")


load_dotenv(env_path, override=True)


class CLIAgentContext(AgentContext):
    pass


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


class NFactorialAgent(BaseAgent[CLIAgentContext]):
    def __init__(
        self,
        mode: str,
        model: Callable[[CLIAgentContext], Model],
        client: MultiClient,
    ):
        model_name = model.name if isinstance(model, Model) else "default"
        if mode == "create":
            instructions = _fetch_prompt("create", model_name)
        elif mode == "edit":
            instructions = _fetch_prompt("edit", model_name)
        else:
            raise ValueError(f"Invalid mode: {mode}")

        super().__init__(
            context_class=CLIAgentContext,
            instructions=instructions,
            tools=[
                *file_tools,
                *project_tools,
                *search_tools,
                *[think, plan, design_doc],
            ],
            model=model,
            model_settings=ModelSettings(
                tool_choice=lambda ctx: (
                    {
                        "type": "function",
                        "function": {"name": "think"},
                    }
                    if ctx.turn == 0
                    else {
                        "type": "function",
                        "function": {"name": "tree"},
                    }
                    if ctx.turn == 1
                    else "required"
                ),
            ),
            output_type=FinalOutput,
            client=client,
            max_turns=60,
        )
