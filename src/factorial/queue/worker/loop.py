import asyncio
import random
from typing import Any

import redis.asyncio as redis

from factorial.agent import BaseAgent
from factorial.queue.lua import (
    create_activity_wait_script,
    create_batch_pickup_script,
    create_task_completion_script,
    create_task_steering_script,
    create_wait_schedule_script,
)
from factorial.queue.operations import get_task_batch, process_cancelled_tasks

from .common import logger
from .processor import process_task


async def worker_loop(
    shutdown_event: asyncio.Event,
    redis_pool: redis.ConnectionPool,
    namespace: str,
    worker_id: str,
    agent: BaseAgent[Any],
    agents_by_name: dict[str, BaseAgent[Any]],
    batch_size: int,
    max_retries: int,
    heartbeat_interval: int,
    task_timeout: int,
    metrics_retention_duration: int,
) -> None:
    """Main worker loop."""
    redis_client = redis.Redis(connection_pool=redis_pool, decode_responses=True)
    batch_script = await create_batch_pickup_script(redis_client)
    completion_script = await create_task_completion_script(redis_client)
    steering_script = await create_task_steering_script(redis_client)
    wait_schedule_script = await create_wait_schedule_script(redis_client)
    activity_wait_script = await create_activity_wait_script(redis_client)

    logger.info(f"Worker {worker_id} started")
    current_tasks: list[asyncio.Task[Any]] = []

    try:
        while not shutdown_event.is_set():
            task_batch: tuple[list[str], list[str]] = await get_task_batch(
                batch_script=batch_script,
                agent=agent,
                batch_size=batch_size,
                metrics_ttl=metrics_retention_duration,
                namespace=namespace,
            )
            tasks_to_process_ids: list[str] = task_batch[0]
            tasks_to_cancel_ids: list[str] = task_batch[1]

            if tasks_to_process_ids:
                logger.info(
                    f"Worker {worker_id} got "
                    f"{len(tasks_to_process_ids)} tasks to process"
                )

                cancellation_task = (
                    asyncio.create_task(
                        process_cancelled_tasks(
                            redis_client=redis_client,
                            namespace=namespace,
                            cancelled_task_ids=tasks_to_cancel_ids,
                            agent=agent,
                        )
                    )
                    if tasks_to_cancel_ids
                    else None
                )

                current_tasks = [
                    asyncio.create_task(
                        process_task(
                            redis_client=redis_client,
                            namespace=namespace,
                            task_id=task_id,
                            completion_script=completion_script,
                            steering_script=steering_script,
                            agent=agent,
                            agents_by_name=agents_by_name,
                            max_retries=max_retries,
                            heartbeat_interval=heartbeat_interval,
                            task_timeout=task_timeout,
                            metrics_retention_duration=metrics_retention_duration,
                            wait_schedule_script=wait_schedule_script,
                            activity_wait_script=activity_wait_script,
                        )
                    )
                    for task_id in tasks_to_process_ids
                ]

                all_tasks = current_tasks + (
                    [cancellation_task] if cancellation_task else []
                )
                await asyncio.gather(*all_tasks, return_exceptions=True)
                current_tasks = []

                if shutdown_event.is_set():
                    logger.info(
                        f"Worker {worker_id} shutting down after completing batch"
                    )
                    break
            else:
                if tasks_to_cancel_ids:
                    await process_cancelled_tasks(
                        redis_client=redis_client,
                        namespace=namespace,
                        cancelled_task_ids=tasks_to_cancel_ids,
                        agent=agent,
                    )

                sleep_time = 0.25 + random.uniform(0, 0.5)
                try:
                    await asyncio.wait_for(shutdown_event.wait(), timeout=sleep_time)
                    break
                except asyncio.TimeoutError:
                    pass

    except asyncio.CancelledError:
        for task in current_tasks:
            if not task.done():
                task.cancel()
        if current_tasks:
            await asyncio.gather(*current_tasks, return_exceptions=True)
        logger.info(f"Worker {worker_id} cancelled")
        raise

    finally:
        await redis_client.close()
        logger.info(f"Worker {worker_id} finished")

