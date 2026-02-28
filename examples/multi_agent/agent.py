import os
from typing import Annotated, Any

from dotenv import load_dotenv
from exa_py import Exa  # type: ignore[import-not-found]
from pydantic import BaseModel

from factorial import (
    Agent,
    AgentContext,
    BaseAgent,
    Hidden,
    ModelSettings,
    VerificationRejected,
    WaitInstruction,
    ai_gateway,
    gpt_41_mini,
    subagents,
    tool,
    wait,
)

current_dir = os.path.dirname(os.path.abspath(__file__))
env_path = os.path.join(current_dir, ".env")

load_dotenv(env_path, override=True)


class PlanResult(BaseModel):
    summary: str
    overview: Annotated[str, Hidden]
    steps: Annotated[list[str], Hidden]


def plan(
    overview: str, steps: list[str], agent_ctx: AgentContext
) -> PlanResult:
    """Structure your plan to accomplish the task.

    This should be user-readable and not mention any specific tool names.
    """
    return PlanResult(
        summary=f"{overview}\n{' -> '.join(steps)}",
        overview=overview,
        steps=steps,
    )


def reflect(reflection: str, agent_ctx: AgentContext) -> str:
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


class FinalOutput(BaseModel):
    final_output: str


class SearchOutput(BaseModel):
    findings: list[str]


search_agent = Agent(
    name="research_subagent",
    description="Research Sub-Agent",
    model=ai_gateway(gpt_41_mini),
    instructions="You are an intelligent research assistant.",
    tools=[reflect, search],
    output_type=SearchOutput,
    model_settings=ModelSettings[AgentContext](
        temperature=1.0,
        tool_choice="required",
    ),
    max_turns=10,
)


class MainAgentContext(AgentContext):
    has_used_research: bool = False


def verify_final_output(
    output: FinalOutput,
    agent_ctx: MainAgentContext,
) -> dict[str, Any]:
    text = output.final_output.strip()
    if not text:
        raise VerificationRejected(
            message="Final output cannot be empty.",
            code="empty_output",
        )
    if len(text) < 40:
        raise VerificationRejected(
            message="Final output is too short; provide a more complete response.",
            code="output_too_short",
            metadata={"min_chars": 40, "actual_chars": len(text)},
        )
    if not agent_ctx.has_used_research:
        raise VerificationRejected(
            message="Use the research tool at least once before finalizing.",
            code="research_required",
        )

    return {
        "final_output": text,
        "verification": {
            "used_research": agent_ctx.has_used_research,
            "char_count": len(text),
        },
    }


@tool(is_enabled=lambda context: not context.has_used_research)
async def research(
    queries: list[str],
    agent_ctx: MainAgentContext,
) -> WaitInstruction:
    """Spawn child search tasks and block until they all complete."""
    payloads = [AgentContext(query=q) for q in queries]
    jobs = await subagents.spawn(agent=search_agent, inputs=payloads, key="research")
    agent_ctx.has_used_research = True
    return wait.jobs(jobs, data="Waiting on research subagents")


class MainAgent(BaseAgent[MainAgentContext]):
    def __init__(self):
        super().__init__(
            name="main_agent",
            description="Main Agent",
            model=ai_gateway(gpt_41_mini),
            instructions="You are a helpful assistant. Always start by making a plan.",
            tools=[plan, reflect, research, search],
            model_settings=ModelSettings[MainAgentContext](
                temperature=0.0,
                tool_choice=lambda context: (
                    {
                        "type": "function",
                        "function": {"name": "plan"},
                    }
                    if context.turn == 0
                    else "required"
                ),
                parallel_tool_calls=False,
            ),
            context_class=MainAgentContext,
            output_type=FinalOutput,
            verifier=verify_final_output,
            verifier_max_attempts=3,
            max_turns=15,
        )


basic_agent = MainAgent()
