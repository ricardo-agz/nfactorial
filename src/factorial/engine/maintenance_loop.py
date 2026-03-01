from __future__ import annotations

import asyncio
import random
from typing import Any

import redis.asyncio as redis

from factorial.agent import BaseAgent
from factorial.logging import get_logger

from .maintenance_tick import MaintenanceTickContext, maintenance_tick

logger = get_logger(__name__)


async def maintenance_loop(
    shutdown_event: asyncio.Event,
    redis_pool: redis.ConnectionPool,
    namespace: str,
    agent: BaseAgent[Any],
    heartbeat_timeout: int,
    max_retries: int,
    batch_size: int,
    interval: int,
    task_ttl_config: Any,
    max_cleanup_batch: int,
    metrics_retention_duration: int,
) -> None:
    """Background maintenance worker to recover stale tasks and clean up."""
    redis_client = redis.Redis(connection_pool=redis_pool, decode_responses=True)
    tick_context = await MaintenanceTickContext.create(
        redis_client=redis_client,
        namespace=namespace,
        agent=agent,
        heartbeat_timeout=heartbeat_timeout,
        max_retries=max_retries,
        batch_size=batch_size,
        task_ttl_config=task_ttl_config,
        max_cleanup_batch=max_cleanup_batch,
        metrics_retention_duration=metrics_retention_duration,
    )

    logger.info(
        f"Maintenance worker started (checking every {interval}s for "
        f"stale tasks >{heartbeat_timeout}s old and cleaning expired tasks)"
    )

    try:
        while not shutdown_event.is_set():
            try:
                tick_result = await maintenance_tick(tick_context)
                if tick_result.expired_hooks > 0:
                    logger.info(f"⏰ Expired {tick_result.expired_hooks} pending hooks")

                try:
                    jitter = random.uniform(-0.2, 0.2)
                    jittered_interval = interval * (1 + jitter)
                    await asyncio.wait_for(
                        shutdown_event.wait(), timeout=jittered_interval
                    )
                    break
                except asyncio.TimeoutError:
                    continue
            except Exception as exc:
                logger.error(f"Error in maintenance worker: {exc}")
                try:
                    await asyncio.wait_for(
                        shutdown_event.wait(), timeout=interval * 1.5
                    )
                    break
                except asyncio.TimeoutError:
                    continue
    except asyncio.CancelledError:
        logger.info("Maintenance worker cancelled")
        raise
    finally:
        await redis_client.close()
        logger.info("Maintenance worker finished")
