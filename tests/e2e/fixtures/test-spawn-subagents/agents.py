from __future__ import annotations

from factorial import WaitInstruction, subagents, tool, wait
from factorial.testing import MockAgent, tool_call


child_agent = MockAgent(
    name="spawn_child",
    instructions="Return a deterministic child completion string.",
    responses=["child complete"],
)


@tool
async def spawn_children(labels: list[str]) -> WaitInstruction:
    jobs = await subagents.spawn(
        agent=child_agent,
        inputs=labels,
        key="fixture_spawn_children",
    )
    return wait.jobs(
        jobs,
        data={
            "reason": "waiting_for_fixture_children",
            "expected_children": len(jobs),
        },
    )


parent_agent = MockAgent(
    name="spawn_parent",
    instructions="Spawn two child tasks and wait for them to complete.",
    tools=[spawn_children],
    responses=[
        tool_call("spawn_children", labels=["alpha", "beta"]),
        "joined 2 child tasks",
    ],
)
