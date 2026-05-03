from __future__ import annotations

from factorial import TurnFinishEvent
from tests.e2e import (
    ProbeContext,
    output_contains,
    pending_children,
    probe,
    status_is,
)


@probe(timeout_s=20.0)
async def parent_waits_for_child_tasks(ctx: ProbeContext) -> None:
    run = await ctx.run(
        "spawn_parent",
        input="spawn two child tasks and wait for them before finishing",
    )
    turn_finish = await run.wait_for_event(
        TurnFinishEvent,
        pending_children(2),
        timeout_s=10.0,
    )

    assert turn_finish.agent_name == "spawn_parent"
    assert len(turn_finish.pending_child_task_ids) == 2


@probe(timeout_s=20.0)
async def parent_joins_child_outputs(ctx: ProbeContext) -> None:
    run = await ctx.run(
        "spawn_parent",
        input="spawn two child tasks and wait for them to finish",
    )
    turn_finish = await run.wait_for_event(
        TurnFinishEvent,
        pending_children(2),
        timeout_s=10.0,
    )
    for child_id in turn_finish.pending_child_task_ids:
        child = ctx.handle(
            child_id,
            agent_name="spawn_child",
            owner_id=run.owner_id,
        )
        child_result = await child.wait_for_result(
            status_is("completed"),
            output_contains("child complete"),
            timeout_s=10.0,
        )
        assert child_result.output == "child complete"

    result = await run.wait_for_result(
        status_is("completed"),
        output_contains("joined 2 child tasks"),
        timeout_s=12.0,
    )
    assert result.output == "joined 2 child tasks"
