import os
import re
import subprocess
import json
import httpx
from dotenv import load_dotenv
from pydantic import BaseModel
from factorial import (
    BaseAgent,
    AgentContext,
    ModelSettings,
    claude_4_sonnet,
    gpt_41,
)

current_dir = os.path.dirname(os.path.abspath(__file__))
env_path = os.path.join(current_dir, ".env")

load_dotenv(env_path, override=True)


def display_code_with_line_numbers(code: str) -> str:
    return "\n".join([f"[{i + 1}]{line}" for i, line in enumerate(code.split("\n"))])


class CLIAgentContext(AgentContext):
    pass


def think(thoughts: str) -> str:
    """Think deeply about the task and plan your next steps before executing"""
    print(thoughts)
    return thoughts


def design_doc(thoughts: str) -> str:
    """Generate a design document of how you will structure and build the project"""
    print(thoughts)
    return thoughts


def create_file(file_path: str, content: str) -> str:
    """Create a new file with the given content"""
    if os.path.exists(file_path):
        return f"Error: File {file_path} already exists. Please re-check the project tree and input a valid file path. If you want to edit the file, use the edit_code tool."
    parent_dir = os.path.dirname(file_path)
    if parent_dir and not os.path.exists(parent_dir):
        os.makedirs(parent_dir, exist_ok=True)
    with open(file_path, "w") as f:
        f.write(content)

    # Run linting/type-checking on the newly created file (best-effort)
    lint_errors = _run_linter(file_path)

    if lint_errors:
        return (
            f"File {file_path} created, but linter/type-checker reported issues:\n"
            f"{lint_errors}"
        )

    return f"File {file_path} created and passed linting/type checking"


def read_file(file_path: str) -> str:
    """
    Read a file and return its contents with line numbers

    The file contents will be shown to you in a format that displays the line numbers to make
    it easier for you to make edits at the correct line numbers.

    When the file contents are shown to you as:
    [1]def hello_world():
    [2]    print("Hello, world!")

    This means the file actually contains:
    def hello_world():
        print("Hello, world!")
    """
    if not os.path.exists(file_path):
        return f"Error: File {file_path} does not exist. Please re-check the project tree and input a valid file path."
    with open(file_path, "r") as f:
        return display_code_with_line_numbers(f.read())


def grep(pattern: str, root_dir: str | None = None, max_matches: int = 100) -> str:
    """Recursively search for *pattern* (regular expression) within text files under
    *root_dir* (defaults to the current working directory).

    Returns the first *max_matches* results in the format
    ``relative/path/to/file:line_number: matched line``. Binary files and common
    dependency folders (``venv``, ``node_modules``, ``__pycache__``, ``.git``)
    are skipped to keep the output relevant and fast.
    """

    root_dir = root_dir or os.getcwd()

    try:
        regex = re.compile(pattern)
    except re.error as exc:
        return f"Invalid regular expression: {exc}"

    ignored_dirs = {"venv", "node_modules", "__pycache__", ".git"}
    binary_exts = {
        ".png",
        ".jpg",
        ".jpeg",
        ".gif",
        ".ico",
        ".svg",
        ".pyc",
        ".o",
        ".so",
        ".dylib",
    }

    matches: list[str] = []

    for dirpath, dirnames, filenames in os.walk(root_dir, topdown=True):
        dirnames[:] = [d for d in dirnames if d not in ignored_dirs]

        for filename in filenames:
            if any(filename.endswith(ext) for ext in binary_exts):
                continue

            file_path = os.path.join(dirpath, filename)

            try:
                with open(file_path, "r", encoding="utf-8", errors="ignore") as handle:
                    for lineno, line in enumerate(handle, 1):
                        if regex.search(line):
                            rel_path = os.path.relpath(file_path, root_dir)
                            matches.append(f"{rel_path}:{lineno}: {line.rstrip()}")
                            if len(matches) >= max_matches:
                                return "\n".join(matches)
            except (OSError, UnicodeDecodeError):
                # Skip unreadable or binary files
                continue

    return "\n".join(matches) if matches else f"No matches found for pattern: {pattern}"


def project_tree() -> str:
    """Return an ascii tree representation of *root_dir* respecting ignore rules.

    Behaviour:
    1. If a `.gitignore` file exists in *root_dir*, every file/dir that matches one
       of its patterns will be skipped.
    2. When there is no `.gitignore`, a conservative default set of bulky or
       transient directories is ignored: ``venv``, ``node_modules`` and
       ``__pycache__``.
    """

    root_dir = os.getcwd()
    out_lines: list[str] = []

    # Figure out ignore specification
    gitignore_path = os.path.join(root_dir, ".gitignore")
    spec = None
    if os.path.isfile(gitignore_path):
        try:
            from pathspec import PathSpec
            from pathspec.patterns.gitwildmatch import GitWildMatchPattern

            with open(gitignore_path, "r", encoding="utf-8") as fh:
                patterns = fh.read().splitlines()

            spec = PathSpec.from_lines(GitWildMatchPattern, patterns)
        except ImportError:
            # pathspec not installed – we will gracefully fall back to the
            # default ignore list.
            spec = None

    default_ignored_dirs = {"venv", "node_modules", "__pycache__"}

    def is_ignored(path_relative_to_root: str) -> bool:
        """Return *True* when *path_relative_to_root* should be excluded."""

        if spec is not None:
            # pathspec expects POSIX-style paths
            if spec.match_file(path_relative_to_root.replace(os.sep, "/")):
                return True

        # Always ignore common bulky directories regardless of .gitignore presence
        parts = path_relative_to_root.split(os.sep)
        return any(part in default_ignored_dirs for part in parts)

    # Walk the directory tree – *topdown=True* allows us to modify *dirs* in
    # place so ignored directories are not traversed.
    for current_root, dirs, files in os.walk(root_dir, topdown=True):
        rel_root = os.path.relpath(current_root, root_dir)
        rel_root = "" if rel_root == "." else rel_root

        # Filter directories in-place so os.walk does not descend into them
        dirs[:] = [d for d in dirs if not is_ignored(os.path.join(rel_root, d))]

        # Filter files as well
        files = [f for f in files if not is_ignored(os.path.join(rel_root, f))]

        # Prepare output lines
        level = rel_root.count(os.sep) if rel_root else 0
        indent = " " * 4 * level
        out_lines.append(
            f"{indent}{os.path.basename(current_root) if rel_root else os.path.basename(root_dir)}"
        )

        subindent = " " * 4 * (level + 1)

        # Keep original ordering: files first, then directories (both sorted
        # for deterministic output)
        for fname in sorted(files):
            out_lines.append(f"{subindent}{fname}")

        for dname in sorted(dirs):
            out_lines.append(f"{subindent}{dname}/")

    tree = "\n".join(out_lines) + "\n"
    print(tree)

    return tree


def create_react_app(
    project_name: str, language: str = "javascript", with_tailwind: bool = False
) -> str:
    """
    Scaffold a React application using create-react-app.

    Args:
        project_name: Name of the directory / project to generate.
        language: "javascript" (default) or "typescript".
        with_tailwind: If True, install and configure Tailwind CSS.

    Returns:
        Status message describing the outcome.
    """
    # Validate input
    if language not in ("javascript", "typescript"):
        return "Error: language must be 'javascript' or 'typescript'"

    if os.path.exists(project_name):
        return f"Error: Directory {project_name} already exists."

    template_arg = ["--template", "typescript"] if language == "typescript" else []

    try:
        # Run create-react-app
        subprocess.run(
            ["npx", "create-react-app", project_name, *template_arg], check=True
        )

        if with_tailwind:
            # Install Tailwind CSS and its peer dependencies
            subprocess.run(
                ["npm", "install", "-D", "tailwindcss", "postcss", "autoprefixer"],
                cwd=project_name,
                check=True,
            )
            # Initialize Tailwind configuration
            subprocess.run(
                ["npx", "tailwindcss", "init", "-p"], cwd=project_name, check=True
            )

            # Replace the main CSS file with Tailwind directives
            index_css = os.path.join(project_name, "src", "index.css")
            with open(index_css, "w") as fh:
                fh.write(
                    "@tailwind base;\n@tailwind components;\n@tailwind utilities;\n"
                )

        return f"React app '{project_name}' created" + (
            " with Tailwind CSS." if with_tailwind else "."
        )
    except subprocess.CalledProcessError as exc:
        return f"Error during create-react-app execution: {exc}"


# ---------------------------------------------------------------------------
# Vite React scaffold tool
# ---------------------------------------------------------------------------


def create_vite_app(
    project_name: str, language: str = "javascript", with_tailwind: bool = False
) -> str:
    """Scaffold a React application using Vite.

    Args:
        project_name: Name of the directory / project to generate.
        language: "javascript" (default) or "typescript".
        with_tailwind: If True, install and configure Tailwind CSS.
    """

    if language not in ("javascript", "typescript"):
        return "Error: language must be 'javascript' or 'typescript'"

    if os.path.exists(project_name):
        return f"Error: Directory {project_name} already exists."

    vite_template = "react-ts" if language == "typescript" else "react"

    try:
        subprocess.run(
            [
                "npm",
                "create",
                "vite@latest",
                project_name,
                "--",
                "--template",
                vite_template,
            ],
            check=True,
        )

        if with_tailwind:
            subprocess.run(
                ["npm", "install", "-D", "tailwindcss", "postcss", "autoprefixer"],
                cwd=project_name,
                check=True,
            )
            subprocess.run(
                ["npx", "tailwindcss", "init", "-p"], cwd=project_name, check=True
            )

            index_css = os.path.join(project_name, "src", "index.css")
            if os.path.isfile(index_css):
                with open(index_css, "w") as fh:
                    fh.write(
                        "@tailwind base;\n@tailwind components;\n@tailwind utilities;\n"
                    )

        return f"Vite app '{project_name}' created" + (
            " with Tailwind CSS." if with_tailwind else "."
        )
    except subprocess.CalledProcessError as exc:
        return f"Error during Vite app creation: {exc}"


# ---------------------------------------------------------------------------
# Next.js scaffold tool
# ---------------------------------------------------------------------------


def create_next_app(
    project_name: str, language: str = "javascript", with_tailwind: bool = False
) -> str:
    """Scaffold a Next.js application.

    Args:
        project_name: Name of the directory / project to generate.
        language: "javascript" (default) or "typescript".
        with_tailwind: If True, install and configure Tailwind CSS.
    """

    if language not in ("javascript", "typescript"):
        return "Error: language must be 'javascript' or 'typescript'"

    if os.path.exists(project_name):
        return f"Error: Directory {project_name} already exists."

    lang_flag = "--typescript" if language == "typescript" else "--js"

    try:
        subprocess.run(
            [
                "npx",
                "create-next-app@latest",
                project_name,
                lang_flag,
            ],
            check=True,
        )

        if with_tailwind:
            subprocess.run(
                ["npm", "install", "-D", "tailwindcss", "postcss", "autoprefixer"],
                cwd=project_name,
                check=True,
            )
            subprocess.run(
                ["npx", "tailwindcss", "init", "-p"], cwd=project_name, check=True
            )

            globals_css = os.path.join(project_name, "styles", "globals.css")
            if os.path.isfile(globals_css):
                with open(globals_css, "w") as fh:
                    fh.write(
                        "@tailwind base;\n@tailwind components;\n@tailwind utilities;\n"
                    )

        return f"Next.js app '{project_name}' created" + (
            " with Tailwind CSS." if with_tailwind else "."
        )
    except subprocess.CalledProcessError as exc:
        return f"Error during Next.js app creation: {exc}"


def edit_code(
    file_path: str,
    find: str,
    find_start_line: int,
    find_end_line: int,
    replace: str,
) -> str:
    """
    Edit code in a file

    Arguments:
    file_path: The path to the file to edit
    find: The text to find and replace
    find_start_line: The start line number where the 'find' text is located
    find_end_line: The end line number where the 'find' text is located
    replace: The text to replace the 'find' text with
    """

    if not os.path.exists(file_path):
        return f"Error: File {file_path} does not exist"

    code = open(file_path, "r").read()
    lines = code.split("\n")

    start_idx = find_start_line - 1
    end_idx = find_end_line - 1

    if start_idx < 0 or end_idx >= len(lines) or start_idx > end_idx:
        return "Error: Invalid line numbers"

    # Extract the text from the specified lines
    existing_text = "\n".join(lines[start_idx : end_idx + 1])

    # Check if the find text matches what's at those line numbers
    if find not in existing_text:
        return (
            f"Error: Text '{find}' not found at lines {find_start_line}-{find_end_line}"
        )

    # Perform the replacement
    new_text = existing_text.replace(find, replace)

    # Replace the lines in the code
    new_lines = lines[:start_idx] + new_text.split("\n") + lines[end_idx + 1 :]

    with open(file_path, "w") as f:
        f.write("\n".join(new_lines))

    # Lint/Type-check after edit (best-effort)
    lint_errors = _run_linter(file_path)

    if lint_errors:
        return (
            f"Code edited at lines {find_start_line}-{find_end_line} in {file_path}, "
            f"but linter/type-checker reported issues:\n{lint_errors}"
        )

    return (
        f"Code successfully edited at lines {find_start_line}-{find_end_line} in {file_path} "
        "and passed linting/type checking"
    )


nfactorial_docs = '''
Agents
Agents are the core building blocks of Factorial applications. They are LLMs equipped with instructions, tools, and configuration that can execute tasks autonomously. Each agent runs in a stateless manner, with context managed separately for each task execution.

Quick Start
The simplest way to create an agent:

from factorial import Agent, AgentContext

def get_weather(location: str) -> str:
    """Get the current weather for a location"""
    return f"The weather in {location} is sunny and 72°F"

agent = Agent(
    instructions="You are a helpful weather assistant. Always be friendly and informative.",
    tools=[get_weather],
)


Agent Parameters
Required Parameters
description (str): A brief description of what the agent does. Used for logging and observability.

instructions (str): The system prompt that guides the agent's behavior. This is sent to the LLM as the system message.

Optional Parameters
tools (list): List of Python functions that the agent can call. See Tools documentation for details.

def search_web(query: str) -> str:
    """Search the web for information"""
    return f"Search results for: {query}"

agent = Agent(
    description="Research Assistant",
    instructions="You help users research topics by searching the web.",
    tools=[search_web],
)


model (Model | Callable): The LLM model to use. Can be a static model or a function that returns a model based on context.

from factorial import gpt_41, gpt_41_mini

# Static model
agent = Agent(
    description="Assistant",
    instructions="You are helpful",
    model=gpt_41,
)

# Dynamic model based on context
def choose_model(agent_ctx: AgentContext) -> Model:
    return gpt_41 if agent_ctx.turn == 0 else gpt_41_mini

agent = Agent(
    description="Assistant", 
    instructions="You are helpful",
    model=choose_model,
)

model_settings (ModelSettings): Configuration for model parameters like temperature, tool choice, etc.

max_turns (int): Maximum number of conversation turns before the agent must complete. Useful for preventing infinite loops.

output_type (BaseModel): Pydantic model for structured output. When set, the agent must use the final_output tool to complete.

context_class (type): Custom context class to use instead of the default AgentContext.

context_window_limit (int): Maximum number of tokens in the context window.

request_timeout (float): HTTP timeout for LLM requests in seconds (default: 120.0).

parse_tool_args (bool): Whether to parse tool arguments as JSON (default: True).

Model Settings
Use ModelSettings to configure LLM behavior:

from factorial import ModelSettings, AgentContext

agent = Agent(
    description="Creative Writer",
    instructions="You write creative stories",
    model_settings=ModelSettings[AgentContext](
        temperature=0.8, 
        max_completion_tokens=1000,
        tool_choice="auto",
        parallel_tool_calls=True,
    ),
)

Dynamic Model Settings
Model settings can be functions that receive the agent context:

agent = Agent(
    description="Adaptive Assistant",
    instructions="You adapt your behavior based on conversation progress",
    model_settings=ModelSettings(
        temperature=lambda ctx: 0 if ctx.turn == 0 else 1,
        tool_choice=lambda ctx: (
            {
                "type": "function",
                "function": {"name": "plan"},
            }
            if ctx.turn == 0
            else "required"
        ),
    ),
)


Structured Output
Define structured output using Pydantic models:

from factorial.utils import BaseModel

class Joke(BaseModel):
    setup: str
    punchline: str

agent = Agent(
    description="Joke Agent",
    instructions="Funny agent",
    tools=[search_web],
    output_type=Joke,
)

When output_type is set, the agent automatically creates a final_output tool that the agent must call to complete.

Creating and Running Tasks
Agents don't run directly - they process tasks through an orchestrator:

from factorial import AgentContext

task = agent.create_task(
    owner_id="user123", 
    payload=AgentContext(
        query="What's the weather like in San Francisco?"
    )
)

# Enqueue the task (see the orchestrator section on how to set up the orchestrator)
await orchestrator.enqueue_task(agent=agent, task=task)


Custom Agent Classes
For advanced use cases, extend BaseAgent or Agent:

from factorial import BaseAgent, AgentContext, ExecutionContext

class CustomAgent(BaseAgent[AgentContext]):
    async def completion(
        self, 
        agent_ctx: AgentContext, 
        messages: list[dict[str, Any]]
    ) -> ChatCompletion:
        """Override to customize LLM completion"""
        # Add custom logic before completion
        print(f"Making completion request for turn {{agent_ctx.turn}}")
        
        # Call parent implementation
        response = await super().completion(agent_ctx, messages)
        
        # Add custom logic after completion
        print(f"Received response with {{len(response.choices)}} choices")
        
        return response
    
    async def tool_action(
        self, 
        tool_call: ChatCompletionMessageToolCall, 
        agent_ctx: AgentContext
    ) -> FunctionToolActionResult:
        """Override to customize tool execution"""
        print(f"Executing tool: {{tool_call.function.name}}")
        
        return await super().tool_action(tool_call, agent_ctx)
    
    async def run_turn(
        self, 
        agent_ctx: AgentContext
    ) -> TurnCompletion[AgentContext]:
        """Override to customize turn logic"""
        execution_ctx = self.get_execution_context()
        
        # Custom logic before turn
        if agent_ctx.turn > 5:
            # Force completion after 5 turns
            return TurnCompletion(
                is_done=True,
                context=agent_ctx,
                output="Conversation limit reached"
            )
        
        return await super().run_turn(agent_ctx)


Custom Context Classes
Create custom context classes for specialized agents:

from factorial import AgentContext, BaseAgent

class ResearchContext(AgentContext):
    research_topic: str = ""
    sources_found: list[str] = []
    confidence_level: float = 0.0

class ResearchAgent(BaseAgent[ResearchContext]):
    def __init__(self):
        super().__init__(
            description="Research specialist",
            instructions="You conduct thorough research",
            tools=[search_web, analyze_source],
            context_class=ResearchContext,
        )
    
    async def run_turn(self, agent_ctx: ResearchContext) -> TurnCompletion[ResearchContext]:
        # Access custom context fields
        print(f"Researching: {{agent_ctx.research_topic}}")
        print(f"Sources found: {{len(agent_ctx.sources_found)}}")
        
        return await super().run_turn(agent_ctx)

# Usage
research_context = ResearchContext(
    query="Research the impact of AI on education",
    research_topic="AI in Education",
    messages=[],
    turn=0,
)

task = research_agent.create_task(
    owner_id="researcher123",
    payload=research_context
)


Error Handling and Retries
Agents include built-in retry logic with exponential backoff:

from factorial import retry, publish_progress

class RobustAgent(BaseAgent[AgentContext]):
    @retry(max_attempts=5, delay=2.0, exponential_base=1.5)
    async def custom_method(self, data: str) -> str:
        """This method will retry up to 5 times on failure"""
        # Your logic here
        return "processed"
    
    @publish_progress(func_name="data_processing")
    async def process_data(self, agent_ctx: AgentContext) -> str:
        """This method publishes progress events automatically"""
        # Events published:
        # - progress_update_data_processing_started
        # - progress_update_data_processing_completed (or _failed)
        return await self.custom_method("some data")


Agent Lifecycle
Understanding the agent execution lifecycle:

Task Creation: Create a task with initial context
Task Enqueueing: Add task to the processing queue
Turn Execution: Agent processes one turn at a time
Prepare messages (system prompt + conversation history)
Make LLM completion request
Execute any tool calls
Update context with results
Completion Check: Determine if agent should finish
Context Update: Save updated context
Next Turn: Repeat until completion or max turns reached
'''

create_instructions = """
You are nFactorial Agent, a specialized coding agent trained to code AI agents using the nFactorial framework.

You have been called via the nfactorial CLI under the following command:

nfactorial create <app description>

You will be working in a new nFactorial project.
Your job is to generate the entire code for the requested AI agent description using the nFactorial framework.

General guidelines:
1. Type hints: Prefer PEP-585 built-in generics (list, dict, set, tuple, etc.) over typing.List, typing.Dict unless the current project explicitly uses the older style or the user requests otherwise.
2. Default stack: Think reasonably about what is required based on the description. If it is explicitly stated or implied that a full-stack application is required, default to FastAPI for the backend that exposes endpoints for interacting with the agent and Vite + React (TypeScript) for the frontend, unless a different stack is specified. If the description clearly indicates a CLI-only agent, skip generating the web stack; if a browser or chat UI is implied, scaffold the FastAPI backend and Vite frontend automatically.
3. Think first: Before writing any code, reflect on the create description and produce a concise design document (using the design_doc tool) explaining what components (CLI, backend APIs, web UI, background workers, etc.) are required and why.

nFactorial framework docs:
{FRAMEWORK_DOCS}
"""


edit_instructions = """
You are nFactorial Agent, a specialized coding agent trained to code AI agents using the nFactorial framework.

You have been called via the nfactorial CLI under the following command:

nfactorial edit <app changes request>

You will be working in a presumably existing nFactorial project.
Your job is to implement the requested changes to the code.

Follow the same general guidelines used during project creation:
1. Default to PEP-585 generics (list, dict, …) unless the existing codebase consistently uses typing.* generics or the user specifies otherwise.
2. Maintain the FastAPI + Vite stack as the default unless instructed differently.
3. Think deeply about the change request; update the design document if necessary and modify only the affected parts of the project.

nFactorial framework docs:
{FRAMEWORK_DOCS}
"""


# -----------------------------------------------------------------------------
# Framework documentation helper
# -----------------------------------------------------------------------------


DOCS_ENDPOINT = os.getenv("NFACTORIAL_DOCS_ENDPOINT", "http://localhost:8081/docs")


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
    def __init__(self, mode: str):
        if mode == "create":
            instructions = create_instructions
        elif mode == "edit":
            instructions = edit_instructions
        else:
            raise ValueError(f"Invalid mode: {mode}")

        framework_docs = _fetch_framework_docs()
        instructions = instructions.format(FRAMEWORK_DOCS=framework_docs)

        super().__init__(
            context_class=CLIAgentContext,
            instructions=instructions,
            tools=[
                think,
                design_doc,
                edit_code,
                read_file,
                project_tree,
                grep,
                create_file,
                create_react_app,
                create_vite_app,
                create_next_app,
            ],
            # model=claude_4_sonnet,
            model=gpt_41,
            model_settings=ModelSettings(
                temperature=0.7,
            ),
            output_type=FinalOutput,
        )


create_agent = NFactorialAgent(mode="create")
edit_agent = NFactorialAgent(mode="edit")

# ---------------------------------------------------------------------------
# Helper: run pyright on a single file and return error report (if any)
# ---------------------------------------------------------------------------


def _run_pyright(file_path: str) -> str | None:
    """Run *pyright* on *file_path* and return the error report if issues are found.

    The function tries to execute the *pyright* CLI with ``--outputjson`` so we can
    parse and return a concise error summary. If *pyright* is not available, we
    fall back to a simple syntax-check via ``python -m py_compile``. The returned
    string is ``None`` when no problems are detected or when linting is
    unavailable.
    """

    # Prefer pyright if installed
    try:
        result = subprocess.run(
            ["pyright", file_path, "--outputjson"], capture_output=True, text=True
        )

        if result.returncode == 0:
            return None  # no errors

        # Attempt to pretty-print first 10 diagnostics
        try:
            data = json.loads(result.stdout or result.stderr)
            diagnostics = data.get("generalDiagnostics", [])
            if diagnostics:
                summary_lines: list[str] = []
                max_items = 10
                for d in diagnostics[:max_items]:
                    msg = d.get("message", "")
                    line = d.get("range", {}).get("start", {}).get("line", 0) + 1
                    summary_lines.append(f"Line {line}: {msg}")
                if len(diagnostics) > max_items:
                    summary_lines.append("...")
                return "\n".join(summary_lines)
        except Exception:
            # If JSON parsing fails, fall back to raw output
            pass

        return result.stdout or result.stderr or "Unknown pyright error"
    except FileNotFoundError:
        # pyright not installed – do a quick syntax check instead
        try:
            subprocess.check_output(["python", "-m", "py_compile", file_path])
            return None
        except subprocess.CalledProcessError as exc:
            return exc.output.decode() if isinstance(exc.output, bytes) else str(exc)


# ---------------------------------------------------------------------------
# Wrapper: choose appropriate linter based on file extension
# ---------------------------------------------------------------------------


def _run_linter(file_path: str) -> str | None:
    """
    Run an appropriate linter or syntax checker for *file_path* based on its
    extension. Falls back to a no-op when a suitable tool is unavailable.

    Returns a string with lint / type errors, or ``None`` when the file passes
    or when linting is skipped.
    """

    ext = os.path.splitext(file_path)[1].lower()

    # Python – reuse the Pyright helper defined above
    if ext in {".py", ".pyw"}:
        return _run_pyright(file_path)

    # TypeScript / JavaScript – try eslint first, then tsc / node check
    if ext in {".ts", ".tsx", ".js", ".jsx"}:
        lint_cmds = [
            ["npx", "--yes", "eslint", "--no-eslintrc", "--env", "es2021", file_path],
            ["npx", "--yes", "tsc", "--noEmit", "--pretty", "false", file_path],
            ["node", "--check", file_path],
        ]
        for cmd in lint_cmds:
            try:
                result = subprocess.run(cmd, capture_output=True, text=True)
                if result.returncode == 0:
                    return None
                return result.stdout or result.stderr
            except FileNotFoundError:
                continue
        return None

    # CSS – if stylelint is available
    if ext == ".css":
        try:
            result = subprocess.run(
                ["npx", "--yes", "stylelint", file_path],
                capture_output=True,
                text=True,
            )
            if result.returncode == 0:
                return None
            return result.stdout or result.stderr
        except FileNotFoundError:
            return None

    # Unsupported file type – skip linting
    return None
