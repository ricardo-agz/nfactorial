from __future__ import annotations

import pytest

from factorial.context import AgentContext
from factorial.engine import WorkerTickContext, worker_tick
from factorial.queue.operations import enqueue_task
from factorial.queue.task import Task, TaskStatus, get_task_status


@pytest.mark.asyncio
async def test_worker_tick_processes_enqueued_task(
    redis_client,
    test_namespace: str,
    test_agent,
) -> None:
    task = Task.create(
        owner_id="owner-123",
        agent=test_agent.name,
        payload=AgentContext(query="run once", messages=[], turn=0),
    )
    await enqueue_task(
        redis_client=redis_client,
        namespace=test_namespace,
        agent=test_agent,
        task=task,
    )

    tick_context = await WorkerTickContext.create(
        redis_client=redis_client,
        namespace=test_namespace,
        agent=test_agent,
        agents_by_name={test_agent.name: test_agent},
        batch_size=25,
        max_retries=3,
        heartbeat_interval=1,
        task_timeout=10,
        metrics_retention_duration=3600,
    )
    result = await worker_tick(
        tick_context,
        max_batches=3,
        max_tasks=25,
        max_runtime_s=5.0,
    )

    status = await get_task_status(
        redis_client=redis_client,
        namespace=test_namespace,
        task_id=task.id,
    )
    assert status == TaskStatus.COMPLETED
    assert result.picked_tasks >= 1
    assert result.processed_tasks >= 1
