import os
from typing import Any
from dotenv import load_dotenv

from factorial import (
    BaseAgent,
    AgentContext,
    ExecutionContext,
    Orchestrator,
    ModelSettings,
    gpt_41_mini,
    AgentWorkerConfig,
    deferred_result,
)

current_dir = os.path.dirname(os.path.abspath(__file__))
env_path = os.path.join(current_dir, ".env")

load_dotenv(env_path, override=True)


class IdeAgentContext(AgentContext):
    code: str


def think(thoughts: str) -> str:
    """Think deeply about the task and plan your next steps before executing"""
    return thoughts


def edit_code(
    find: str,
    find_start_line: int,
    find_end_line: int,
    replace: str,
    agent_ctx: IdeAgentContext,
) -> tuple[str, dict[str, Any]]:
    """
    Edit code in a file

    Arguments:
    find: The text to find and replace
    find_start_line: The start line number where the 'find' text is located
    find_end_line: The end line number where the 'find' text is located
    replace: The text to replace the 'find' text with
    """
    lines = agent_ctx.code.split("\n")

    # Convert to 0-based indexing
    start_idx = find_start_line - 1
    end_idx = find_end_line - 1

    # Validate line numbers
    if start_idx < 0 or end_idx >= len(lines) or start_idx > end_idx:
        return "Error: Invalid line numbers", {
            "error": "Line numbers out of range or invalid",
            "total_lines": len(lines),
        }

    # Extract the text from the specified lines
    existing_text = "\n".join(lines[start_idx : end_idx + 1])

    # Check if the find text matches what's at those line numbers
    if find not in existing_text:
        return (
            f"Error: Text '{find}' not found at lines {find_start_line}-{find_end_line}",
            {
                "error": "Find text not found at specified lines",
                "existing_text": existing_text,
            },
        )

    # Perform the replacement
    new_text = existing_text.replace(find, replace)

    # Replace the lines in the code
    new_lines = lines[:start_idx] + new_text.split("\n") + lines[end_idx + 1 :]

    # Update the agent context with the modified code
    agent_ctx.code = "\n".join(new_lines)

    return (
        f"Code successfully edited: replaced '{find}' with '{replace}' at lines {find_start_line}-{find_end_line}",
        {
            "find": find,
            "find_start_line": find_start_line,
            "find_end_line": find_end_line,
            "replace": replace,
            "old_text": existing_text,
            "new_text": new_text,
            "new_code": agent_ctx.code,
        },
    )


@deferred_result(timeout=300.0)  # 5-minute timeout waiting for user decision
def request_code_execution(
    response_on_reject: str, agent_ctx: AgentContext, execution_ctx: ExecutionContext
) -> None:
    """
    Request the code to be run. The use must approve this request before the code is run.

    Parameters
    ----------
    response_on_reject : str
        A message the agent should send if the user rejects the execution request.
    """
    pass


instructions = """
You are an IDE assistant that helps with coding tasks. You can write, read, and analyze code. 
For anything non-trivial, always start by making a plan for the coding task.

You will be given a code file and a query. Your job is to either respond to the query with an answer,
or edit the code file if the query requires it.

Please note, the code file will be shown to you in a format that displays the line numbers to make
it easier for you to make edits at the correct line numbers, when you write code you should write
valid code and NOT include the line numbers as part of the code.

When code is shown to you as:
[1]def hello_world():
[2]    print("Hello, world!")

This means the code is actually:
def hello_world():
    print("Hello, world!")

In your final response, just clearly and consisely explain what you did without writing any code. The code changes will be 
shown to the user in a diff editor.
"""


class IDEAgent(BaseAgent[IdeAgentContext]):
    def __init__(self):
        super().__init__(
            context_class=IdeAgentContext,
            instructions=instructions,
            tools=[think, edit_code, request_code_execution],
            model=gpt_41_mini,
            model_settings=ModelSettings(
                temperature=0.1,
            ),
        )

    def prepare_messages(self, agent_ctx: IdeAgentContext) -> list[dict[str, Any]]:
        if agent_ctx.turn == 0:
            messages = [{"role": "system", "content": self.instructions}]
            if agent_ctx.messages:
                messages.extend(
                    [message for message in agent_ctx.messages if message["content"]]
                )
            messages.append(
                {
                    "role": "user",
                    "content": f"Code file with line numbers:\n{self.display_code_with_line_numbers(agent_ctx.code)}\n---\nQuery: {agent_ctx.query}",
                }
            )
        else:
            messages = agent_ctx.messages

        return messages

    def display_code_with_line_numbers(self, code: str) -> str:
        """Display code with line numbers"""
        return "\n".join(
            [f"[{i + 1}]{line}" for i, line in enumerate(code.split("\n"))]
        )


ide_agent = IDEAgent()

orchestrator = Orchestrator(
    redis_host=os.getenv("REDIS_HOST", "localhost"),
    openai_api_key=os.getenv("OPENAI_API_KEY"),
)

orchestrator.register_runner(
    agent=ide_agent,
    agent_worker_config=AgentWorkerConfig(
        workers=20,
        batch_size=15,
        max_retries=5,
    ),
)


if __name__ == "__main__":
    orchestrator.run()
