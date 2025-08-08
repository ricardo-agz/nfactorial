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
    gpt_5,
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

from factorial import gpt_5_mini, gpt_5_nano

# Static model
agent = Agent(
    description="Assistant",
    instructions="You are helpful",
    model=gpt_5_mini,
)

# Dynamic model based on context
def choose_model(agent_ctx: AgentContext) -> Model:
    return gpt_5_mini if agent_ctx.turn == 0 else gpt_5_nano

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

        thinking_tools = [think, plan, design_doc]

        super().__init__(
            context_class=CLIAgentContext,
            instructions=instructions,
            tools=[
                *file_tools,
                *project_tools,
                *search_tools,
                *thinking_tools,
            ],
            model=gpt_5,
            model_settings=ModelSettings(
                temperature=0.7,
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
        )


create_agent = NFactorialAgent(mode="create")
edit_agent = NFactorialAgent(mode="edit")
