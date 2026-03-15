from __future__ import annotations

import importlib
from types import SimpleNamespace

import pytest

from factorial.agent.context import AgentContext
from factorial.queue.operations import enqueue_task
from factorial.queue.task import Task, TaskStatus, get_task_status
from factorial.queue.worker.tick import WorkerTickContext, worker_tick

worker_tick_module = importlib.import_module("factorial.queue.worker.tick")


@pytest.mark.asyncio
async def test_worker_tick_processes_enqueued_task(
    redis_client,
    test_namespace: str,
    test_agent,
) -> None:
    task = Task.create(
        owner_id="owner-123",
        agent=test_agent.name,
        payload=AgentContext(messages=[{"role": "user", "content": "run once"}]),
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


@pytest.mark.asyncio
async def test_worker_tick_reports_partial_processing_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _fake_get_task_batch(**_: object) -> tuple[list[str], list[str]]:
        return ["task-ok", "task-fail"], []

    async def _fake_process_task(*, task_id: str, **_: object) -> None:
        if task_id == "task-fail":
            raise RuntimeError("synthetic failure")

    async def _fake_estimate_backlog(_: WorkerTickContext) -> int:
        return 0

    monkeypatch.setattr(worker_tick_module, "get_task_batch", _fake_get_task_batch)
    monkeypatch.setattr(worker_tick_module, "process_task", _fake_process_task)
    monkeypatch.setattr(worker_tick_module, "_estimate_backlog", _fake_estimate_backlog)

    context = WorkerTickContext(
        redis_client=object(),  # type: ignore[arg-type]
        namespace="test-namespace",
        agent=SimpleNamespace(name="test-agent"),  # type: ignore[arg-type]
        agents_by_name={},
        batch_size=2,
        max_retries=3,
        heartbeat_interval=1,
        task_timeout=10,
        metrics_retention_duration=3600,
        batch_script=object(),  # type: ignore[arg-type]
        completion_script=object(),  # type: ignore[arg-type]
        steering_script=object(),  # type: ignore[arg-type]
        wait_schedule_script=object(),  # type: ignore[arg-type]
        activity_wait_script=object(),  # type: ignore[arg-type]
    )

    result = await worker_tick(
        context,
        max_batches=1,
        max_tasks=10,
        max_runtime_s=1.0,
    )

    assert result.picked_tasks == 2
    assert result.processed_tasks == 1
    assert result.failed_tasks == 1
