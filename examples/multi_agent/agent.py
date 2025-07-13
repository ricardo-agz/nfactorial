from typing import Any
import os
from dotenv import load_dotenv

from factorial import (
    BaseAgent,
    Agent,
    AgentContext,
    Orchestrator,
    ModelSettings,
    gpt_41_mini,
    AgentWorkerConfig,
    MaintenanceWorkerConfig,
    TaskTTLConfig,
    ObservabilityConfig,
    MetricsTimelineConfig,
    function_tool,
)
from factorial.tools import forking_tool
from factorial.context import ExecutionContext
from factorial.utils import BaseModel
from exa_py import Exa


current_dir = os.path.dirname(os.path.abspath(__file__))
env_path = os.path.join(current_dir, ".env")

load_dotenv(env_path, override=True)


def plan(
    overview: str, steps: list[str], agent_ctx: AgentContext
) -> tuple[str, dict[str, Any]]:
    """Structure your plan to accomplish the task. This should be user-readable and not mention any specific tool names."""
    return f"{overview}\n{' -> '.join(steps)}", {"overview": overview, "steps": steps}


def reflect(reflection: str, agent_ctx: AgentContext) -> tuple[str, str]:
    """Reflect on a task"""
    return reflection, reflection


def search(query: str) -> tuple[str, list[dict[str, Any]]]:
    """Search the web for information"""
    exa = Exa(api_key=os.getenv("EXA_API_KEY"))

    result = exa.search_and_contents(  # type: ignore
        query=query, num_results=10, text={"max_characters": 500}
    )

    data = [
        {
            "title": r.title,
            "url": r.url,
        }
        for r in result.results
    ]

    return str(result), data


class FinalOutput(BaseModel):
    final_output: str


class SearchOutput(BaseModel):
    findings: list[str]


search_agent = Agent(
    name="research_subagent",
    description="Research Sub-Agent",
    model=gpt_41_mini,
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


@function_tool(is_enabled=lambda context: not context.has_used_research)
@forking_tool(timeout=600)
async def research(
    queries: list[str],
    agent_ctx: MainAgentContext,
    execution_ctx: ExecutionContext,
) -> list[str]:
    """Spawn child search tasks for each query and wait for them to complete."""

    payloads = [AgentContext(query=q) for q in queries]
    batch = await execution_ctx.spawn_child_tasks(search_agent, payloads)
    agent_ctx.has_used_research = True
    return batch.task_ids


class MainAgent(BaseAgent[MainAgentContext]):
    def __init__(self):
        super().__init__(
            name="main_agent",
            description="Main Agent",
            model=gpt_41_mini,
            instructions="You are a helpful assistant. Always start out by making a plan.",
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
            max_turns=15,
        )


basic_agent = MainAgent()

orchestrator = Orchestrator(
    redis_host=os.getenv("REDIS_HOST", "localhost"),
    redis_port=int(os.getenv("REDIS_PORT", 6379)),
    redis_db=int(os.getenv("REDIS_DB", 0)),
    redis_max_connections=int(os.getenv("REDIS_MAX_CONNECTIONS", 1000)),
    openai_api_key=os.getenv("OPENAI_API_KEY"),
    xai_api_key=os.getenv("XAI_API_KEY"),
    observability_config=ObservabilityConfig(
        enabled=True,
        host="0.0.0.0",
        port=8081,
        cors_origins=["*"],
    ),
)

orchestrator.register_runner(
    agent=search_agent,
    agent_worker_config=AgentWorkerConfig(
        workers=25,
        batch_size=15,
        max_retries=3,
        heartbeat_interval=2,
        missed_heartbeats_threshold=3,
        missed_heartbeats_grace_period=1,
        turn_timeout=60,
    ),
)

orchestrator.register_runner(
    agent=basic_agent,
    agent_worker_config=AgentWorkerConfig(
        workers=50,
        batch_size=15,
        max_retries=5,
        heartbeat_interval=2,
        missed_heartbeats_threshold=3,
        missed_heartbeats_grace_period=1,
        turn_timeout=120,
    ),
    maintenance_worker_config=MaintenanceWorkerConfig(
        workers=5,
        interval=5,
        task_ttl=TaskTTLConfig(
            failed_ttl=1800,
            completed_ttl=60,
            cancelled_ttl=30,
        ),
        metrics_timeline=MetricsTimelineConfig(
            timeline_duration=3600,  # 1 hour
            bucket_size="minutes",
            retention_multiplier=2.0,
        ),
    ),
)


if __name__ == "__main__":
    orchestrator.run()
