import os
from datetime import datetime
from pathlib import Path
from typing import Callable
import httpx
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
from .tools.search.tree import tree
from .prefetch import prefetch

current_dir = os.path.dirname(os.path.abspath(__file__))
env_path = os.path.join(current_dir, ".env")


load_dotenv(env_path, override=True)


class CLIAgentContext(AgentContext):
    pass


# PROMPT_ENDPOINT = os.getenv(
#     "NFACTORIAL_PROMPT_ENDPOINT", "https://www.factorial.sh/api/agent-prompt"
# )
PROMPT_ENDPOINT = os.getenv(
    "NFACTORIAL_PROMPT_ENDPOINT", "http://localhost:8081/api/agent-prompt"
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
                    else "required"
                ),
            ),
            output_type=FinalOutput,
            client=client,
            max_turns=60,
        )


async def build_user_prompt(
    client: MultiClient, cwd: Path, prompt: str
) -> tuple[str, list[Path]]:
    is_dir_empty = not any(cwd.iterdir())
    tree_str = await tree(str(cwd.resolve())) if not is_dir_empty else ""
    try:
        relevant_files, files_blob = (
            await prefetch(client, cwd, tree_str, prompt)
            if not is_dir_empty
            else ([], "")
        )
    except Exception:
        relevant_files, files_blob = [], ""

    files_preview = (
        f"""
        Prefetched relevant files ({len(relevant_files)}):
        <files>
        {files_blob}
        </files>
        """
        if not is_dir_empty
        else "The current working directory is empty."
    )

    return (
        f"""
    <metadata>
    Current working directory: {cwd.resolve()}
    Current working directory contents:
    {tree_str}
    {files_preview}
    
    The current date and time is {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
    </metadata>
    
    <task>
    {prompt}
    </task>
    """,
        relevant_files,
    )
