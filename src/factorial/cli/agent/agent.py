import os
from typing import Callable
import httpx
import yaml
from dotenv import load_dotenv
from pydantic import BaseModel
from factorial import (
    BaseAgent,
    AgentContext,
    ModelSettings,
    Model,
    MultiClient,
)

from .tools.file import file_tools
from .tools.project import project_tools
from .tools.search import search_tools
from .tools.thinking import think, plan, design_doc

current_dir = os.path.dirname(os.path.abspath(__file__))
env_path = os.path.join(current_dir, ".env")


load_dotenv(env_path, override=True)


class CLIAgentContext(AgentContext):
    pass


DOCS_ENDPOINT = os.getenv("NFACTORIAL_DOCS_ENDPOINT", "http://localhost:8081/docs")


with open(os.path.join(current_dir, "agent-base.md"), "r") as fh:
    base_prompt = fh.read()
with open(os.path.join(current_dir, "agent-create.yaml"), "r") as fh:
    create_variables = yaml.safe_load(fh)
    create_prompt = base_prompt
    for k, v in create_variables.items():
        create_prompt = create_prompt.replace(f"{{{k.upper()}}}", v)
with open(os.path.join(current_dir, "agent-edit.yaml"), "r") as fh:
    edit_variables = yaml.safe_load(fh)
    edit_prompt = base_prompt
    for k, v in edit_variables.items():
        edit_prompt = edit_prompt.replace(f"{{{k.upper()}}}", v)


def _fetch_framework_docs(url: str = DOCS_ENDPOINT, timeout: float = 30.0) -> str:
    """Retrieve the framework docs from the FastAPI docs server."""
    with httpx.Client(timeout=timeout) as client:
        response = client.get(url)
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
        if mode == "create":
            instructions = create_prompt
        elif mode == "edit":
            instructions = edit_prompt
        else:
            raise ValueError(f"Invalid mode: {mode}")

        framework_docs = _fetch_framework_docs()
        instructions = instructions.replace("{{FRAMEWORK_DOCS}}", framework_docs)

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
        )
