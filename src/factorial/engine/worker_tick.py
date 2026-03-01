from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any

import redis.asyncio as redis

from factorial.agent import BaseAgent
from factorial.logging import get_logger
from factorial.queue.keys import RedisKeys
from factorial.queue.lua import (
    ActivityWaitScript,
    BatchPickupScript,
    TaskCompletionScript,
    TaskSteeringScript,
    WaitScheduleScript,
    create_activity_wait_script,
    create_batch_pickup_script,
    create_task_completion_script,
    create_task_steering_script,
    create_wait_schedule_script,
)
from factorial.queue.operations import get_task_batch, process_cancelled_tasks
from factorial.queue.worker.processor import process_task

logger = get_logger(__name__)


@dataclass
class WorkerTickResult:
    processed_tasks: int = 0
    picked_tasks: int = 0
    cancelled_tasks_processed: int = 0
    failed_tasks: int = 0
    remaining_backlog_estimate: int | None = None
    touched_agents: list[str] = field(default_factory=list)
    duration_ms: int = 0


@dataclass
class WorkerTickContext:
    redis_client: redis.Redis
    namespace: str
    agent: BaseAgent[Any]
    agents_by_name: dict[str, BaseAgent[Any]]
    batch_size: int
    max_retries: int
    heartbeat_interval: int
    task_timeout: int
    metrics_retention_duration: int
    batch_script: BatchPickupScript
    completion_script: TaskCompletionScript
    steering_script: TaskSteeringScript
    wait_schedule_script: WaitScheduleScript
    activity_wait_script: ActivityWaitScript
    strict_batch_pickup_errors: bool = False

    @classmethod
    async def create(
        cls,
        *,
        redis_client: redis.Redis,
        namespace: str,
        agent: BaseAgent[Any],
        agents_by_name: dict[str, BaseAgent[Any]],
        batch_size: int,
        max_retries: int,
        heartbeat_interval: int,
        task_timeout: int,
        metrics_retention_duration: int,
        strict_batch_pickup_errors: bool = False,
    ) -> WorkerTickContext:
        return cls(
            redis_client=redis_client,
            namespace=namespace,
            agent=agent,
            agents_by_name=agents_by_name,
            batch_size=batch_size,
            max_retries=max_retries,
            heartbeat_interval=heartbeat_interval,
            task_timeout=task_timeout,
            metrics_retention_duration=metrics_retention_duration,
            strict_batch_pickup_errors=strict_batch_pickup_errors,
            batch_script=await create_batch_pickup_script(redis_client),
            completion_script=await create_task_completion_script(redis_client),
            steering_script=await create_task_steering_script(redis_client),
            wait_schedule_script=await create_wait_schedule_script(redis_client),
            activity_wait_script=await create_activity_wait_script(redis_client),
        )


async def worker_tick(
    context: WorkerTickContext,
    *,
    max_batches: int = 1,
    max_tasks: int | None = None,
    max_runtime_s: float | None = None,
) -> WorkerTickResult:
    started_at = time.monotonic()
    result = WorkerTickResult()

    if max_batches <= 0:
        result.duration_ms = int((time.monotonic() - started_at) * 1000)
        return result

    batch_count = 0
    while batch_count < max_batches:
        if (
            max_runtime_s is not None
            and (time.monotonic() - started_at) >= max_runtime_s
        ):
            break

        tasks_to_process_ids, tasks_to_cancel_ids = await get_task_batch(
            batch_script=context.batch_script,
            namespace=context.namespace,
            agent=context.agent,
            batch_size=context.batch_size,
            metrics_ttl=context.metrics_retention_duration,
            raise_on_error=context.strict_batch_pickup_errors,
        )

        if max_tasks is not None:
            remaining = max(max_tasks - result.processed_tasks, 0)
            if remaining == 0:
                break
            if len(tasks_to_process_ids) > remaining:
                tasks_to_process_ids = tasks_to_process_ids[:remaining]

        if not tasks_to_process_ids and not tasks_to_cancel_ids:
            break

        if tasks_to_process_ids:
            result.picked_tasks += len(tasks_to_process_ids)
        cancellations_requested = len(tasks_to_cancel_ids)

        cancellation_task: asyncio.Task[Any] | None = None
        if tasks_to_cancel_ids:
            cancellation_task = asyncio.create_task(
                process_cancelled_tasks(
                    redis_client=context.redis_client,
                    namespace=context.namespace,
                    cancelled_task_ids=tasks_to_cancel_ids,
                    agent=context.agent,
                )
            )

        current_tasks = [
            asyncio.create_task(
                process_task(
                    redis_client=context.redis_client,
                    namespace=context.namespace,
                    task_id=task_id,
                    completion_script=context.completion_script,
                    steering_script=context.steering_script,
                    agent=context.agent,
                    agents_by_name=context.agents_by_name,
                    max_retries=context.max_retries,
                    heartbeat_interval=context.heartbeat_interval,
                    task_timeout=context.task_timeout,
                    metrics_retention_duration=context.metrics_retention_duration,
                    wait_schedule_script=context.wait_schedule_script,
                    activity_wait_script=context.activity_wait_script,
                )
            )
            for task_id in tasks_to_process_ids
        ]

        all_tasks = current_tasks + ([cancellation_task] if cancellation_task else [])
        gathered_results: list[Any] = []
        if all_tasks:
            gathered_results = list(
                await asyncio.gather(*all_tasks, return_exceptions=True)
            )

        process_results = gathered_results[: len(current_tasks)]
        successful_tasks = 0
        for task_id, process_result in zip(
            tasks_to_process_ids,
            process_results,
            strict=True,
        ):
            if isinstance(process_result, BaseException):
                result.failed_tasks += 1
                logger.error(
                    "Worker tick task execution failed for agent=%s task_id=%s",
                    context.agent.name,
                    task_id,
                    exc_info=process_result,
                )
            else:
                successful_tasks += 1
        result.processed_tasks += successful_tasks

        if cancellation_task is not None:
            cancellation_result = gathered_results[-1]
            if isinstance(cancellation_result, BaseException):
                logger.error(
                    "Worker tick cancellation processing failed for agent=%s",
                    context.agent.name,
                    exc_info=cancellation_result,
                )
            else:
                result.cancelled_tasks_processed += cancellations_requested

        batch_count += 1

    result.remaining_backlog_estimate = await _estimate_backlog(context)
    if (
        result.processed_tasks > 0
        or result.cancelled_tasks_processed > 0
        or result.picked_tasks > 0
    ):
        result.touched_agents.append(context.agent.name)
    result.duration_ms = int((time.monotonic() - started_at) * 1000)
    return result


async def _estimate_backlog(context: WorkerTickContext) -> int | None:
    keys = RedisKeys.format(namespace=context.namespace, agent=context.agent.name)
    try:
        queue_main = await context.redis_client.llen(keys.queue_main)  # type: ignore[misc]
        queue_pending = await context.redis_client.zcard(keys.queue_pending)  # type: ignore[misc]
        return int(queue_main) + int(queue_pending)
    except Exception as exc:  # pragma: no cover - best-effort metric
        logger.debug(
            "Failed to estimate backlog for agent %s", context.agent.name, exc_info=exc
        )
        return None
