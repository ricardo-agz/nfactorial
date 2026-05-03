import os
from dataclasses import dataclass
from typing import Annotated, Any

from dotenv import load_dotenv
from exa_py import Exa  # type: ignore[import-not-found]
from pydantic import BaseModel

from factorial import (
    Agent,
    Hidden,
    WaitInstruction,
    any_of,
    subagents,
    tool,
    tool_called,
    turn_count_is,
    wait,
)
from factorial.ai.models import ai_gateway, gpt_41_mini

current_dir = os.path.dirname(os.path.abspath(__file__))
env_path = os.path.join(current_dir, ".env")

load_dotenv(env_path, override=True)


class PlanResult(BaseModel):
    summary: str
    overview: Annotated[str, Hidden]
    steps: Annotated[list[str], Hidden]


def plan(overview: str, steps: list[str], agent_ctx) -> PlanResult:
    """Structure your plan to accomplish the task.

    This should be user-readable and not mention any specific tool names.
    """
    return PlanResult(
        summary=f"{overview}\n{' -> '.join(steps)}",
        overview=overview,
        steps=steps,
    )


def reflect(reflection: str, agent_ctx) -> str:
    """Reflect on a task"""
    return reflection


class SearchResult(BaseModel):
    summary: str
    results: Annotated[list[dict[str, Any]], Hidden]


def search(query: str) -> SearchResult:
    """Search the web for information"""
    exa = Exa(api_key=os.getenv("EXA_API_KEY"))

    result = exa.search_and_contents(
        query=query, num_results=10, text={"max_characters": 500}
    )

    data = [
        {
            "title": r.title,
            "url": r.url,
        }
        for r in result.results
    ]

    return SearchResult(summary=str(result), results=data)


@dataclass
class MainAgentState:
    has_used_research: bool = False
    done_turn: int | None = None


@tool
def done(final_output: str, agent_ctx) -> str:
    """Finish the task with the final user-facing response."""
    return final_output.strip()


def _research_enabled(agent_ctx) -> bool:
    return not agent_ctx.state.has_used_research


@tool(is_enabled=_research_enabled)
async def research(
    queries: list[str],
    agent_ctx,
) -> WaitInstruction:
    """Spawn child search tasks and block until they all complete."""
    payloads = [search_agent.build_context(input=q) for q in queries]
    jobs = await subagents.spawn(agent=search_agent, inputs=payloads, key="research")
    agent_ctx.state.has_used_research = True
    return wait.jobs(jobs, data="Waiting on research subagents")


def _main_prepare_turn(turn, agent_ctx):
    if agent_ctx.turn_number == 1:
        turn.tool_choice = {"type": "function", "function": {"name": "plan"}}
    else:
        turn.tool_choice = "required"
    turn.parallel_tool_calls = False
    turn.temperature = 0.0


search_agent = Agent[Any](
    name="research_subagent",
    description="Research Sub-Agent",
    model=ai_gateway(gpt_41_mini),
    instructions="You are an intelligent research assistant.",
    tools=[reflect, search, done],
    temperature=1.0,
    tool_choice="required",
    stop_when=any_of(tool_called("done"), turn_count_is(10)),
)

basic_agent = Agent[MainAgentState](
    name="main_agent",
    description="Main Agent",
    model=ai_gateway(gpt_41_mini),
    instructions="You are a helpful assistant. Always start by making a plan.",
    tools=[plan, reflect, research, search, done],
    prepare_turn=_main_prepare_turn,
    stop_when=any_of(tool_called("done"), turn_count_is(15)),
)
