import asyncio
import random
from typing import Any

import redis.asyncio as redis

from factorial.agent import BaseAgent
from factorial.queue.worker.tick import WorkerTickContext, worker_tick

from .common import logger


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
    tick_context = await WorkerTickContext.create(
        redis_client=redis_client,
        namespace=namespace,
        agent=agent,
        agents_by_name=agents_by_name,
        batch_size=batch_size,
        max_retries=max_retries,
        heartbeat_interval=heartbeat_interval,
        task_timeout=task_timeout,
        metrics_retention_duration=metrics_retention_duration,
    )

    logger.info(f"Worker {worker_id} started")

    try:
        while not shutdown_event.is_set():
            tick_result = await worker_tick(
                tick_context,
                max_batches=1,
                max_tasks=batch_size,
            )
            if (
                tick_result.picked_tasks <= 0
                and tick_result.cancelled_tasks_processed <= 0
            ):
                sleep_time = 0.25 + random.uniform(0, 0.5)
                try:
                    await asyncio.wait_for(shutdown_event.wait(), timeout=sleep_time)
                    break
                except asyncio.TimeoutError:
                    pass

    except asyncio.CancelledError:
        logger.info(f"Worker {worker_id} cancelled")
        raise

    finally:
        await redis_client.close()
        logger.info(f"Worker {worker_id} finished")

