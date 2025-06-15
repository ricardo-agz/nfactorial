import random
from typing import Any
import redis.asyncio as redis
import time
import asyncio

from factorial.agent import BaseAgent
from factorial.logging import get_logger, colored
from factorial.queue.lua import (
    StaleRecoveryScript,
    TaskExpirationScript,
    create_stale_recovery_script,
    create_task_expiration_script,
    create_backoff_recovery_script,
    StaleRecoveryScriptResult,
    TaskExpirationScriptResult,
)
from factorial.queue.keys import RedisKeys


logger = get_logger(__name__)


async def recover_stale_tasks(
    recovery_script: StaleRecoveryScript,
    namespace: str,
    agent: BaseAgent[Any],
    heartbeat_timeout: int,
    max_retries: int,
    batch_size: int,
    metrics_bucket_duration: int,
    metrics_retention_duration: int,
) -> int:
    """Atomically move stale tasks back to main queue"""
    cutoff_timestamp = time.time() - heartbeat_timeout

    keys = RedisKeys.format(
        namespace=namespace,
        agent=agent.name,
        metrics_bucket_duration=metrics_bucket_duration,
    )

    try:
        result: StaleRecoveryScriptResult = await recovery_script.execute(
            queue_main_key=keys.queue_main,
            queue_failed_key=keys.queue_failed,
            queue_orphaned_key=keys.queue_orphaned,
            task_statuses_key=keys.task_status,
            task_agents_key=keys.task_agent,
            task_payloads_key=keys.task_payload,
            task_pickups_key=keys.task_pickups,
            task_retries_key=keys.task_retries,
            task_metas_key=keys.task_meta,
            processing_heartbeats_key=keys.processing_heartbeats,
            agent_metrics_bucket_key=keys.agent_metrics_bucket,
            global_metrics_bucket_key=keys.global_metrics_bucket,
            cutoff_timestamp=cutoff_timestamp,
            max_recovery_batch=batch_size,
            max_retries=max_retries,
            metrics_ttl=metrics_retention_duration,
        )
        recovered_count = result.recovered_count
        failed_count = result.failed_count
        stale_task_actions = result.stale_task_actions
        if recovered_count > 0:
            logger.warning(
                f"⚠️ Found {recovered_count + failed_count} tasks with stale heartbeats (>{heartbeat_timeout}s)"
            )
            if recovered_count > 0:
                logger.info(
                    f"✅ Recovered {recovered_count} stale tasks back to main queue"
                )
            if failed_count > 0:
                logger.error(
                    f"❌ Moved {failed_count} tasks to failed queue (max retries exceeded)"
                )

            for task_id, action in stale_task_actions:
                if action == "recovered":
                    logger.info(colored(f"    [{task_id}]: recovered", "dim"))
                elif action == "failed":
                    logger.error(colored(f"    [{task_id}]: failed", "dim"))

        return recovered_count

    except Exception as e:
        logger.error(f"Error during stale task recovery: {e}")
        return 0


async def recover_backoff_tasks(
    redis_client: redis.Redis,
    namespace: str,
    agent: BaseAgent[Any],
    batch_size: int,
) -> int:
    """Move tasks from backoff queue back to main queue when their backoff time has expired"""
    keys = RedisKeys.format(namespace=namespace, agent=agent.name)

    try:
        backoff_recovery_script = await create_backoff_recovery_script(redis_client)
        recovered_task_ids = await backoff_recovery_script.execute(
            queue_backoff_key=keys.queue_backoff,
            queue_main_key=keys.queue_main,
            queue_orphaned_key=keys.queue_orphaned,
            task_statuses_key=keys.task_status,
            task_agents_key=keys.task_agent,
            task_payloads_key=keys.task_payload,
            task_pickups_key=keys.task_pickups,
            task_retries_key=keys.task_retries,
            task_metas_key=keys.task_meta,
            max_batch_size=batch_size,
        )

        recovered_count = len(recovered_task_ids)

        if recovered_count > 0:
            logger.info(f"⏰ Recovered {recovered_count} tasks from backoff queue")

        return recovered_count

    except Exception as e:
        logger.error(f"Error during backoff task recovery: {e}")
        return 0


async def remove_expired_tasks(
    task_expiration_script: TaskExpirationScript,
    namespace: str,
    agent: BaseAgent[Any],
    task_ttl_config: Any,  # Will be TaskTTLConfig from manager.py
    max_cleanup_batch: int,
) -> int:
    """Atomically remove expired tasks from completion, failed, and cancelled queues"""
    current_time = time.time()

    # Calculate cutoff timestamps for each queue
    completed_cutoff = current_time - task_ttl_config.completed_ttl
    failed_cutoff = current_time - task_ttl_config.failed_ttl
    cancelled_cutoff = current_time - task_ttl_config.cancelled_ttl

    keys = RedisKeys.format(namespace=namespace, agent=agent.name)

    try:
        result: TaskExpirationScriptResult = await task_expiration_script.execute(
            queue_completions_key=keys.queue_completions,
            queue_failed_key=keys.queue_failed,
            queue_cancelled_key=keys.queue_cancelled,
            queue_orphaned_key=keys.queue_orphaned,
            task_statuses_key=keys.task_status,
            task_agents_key=keys.task_agent,
            task_payloads_key=keys.task_payload,
            task_pickups_key=keys.task_pickups,
            task_retries_key=keys.task_retries,
            task_metas_key=keys.task_meta,
            completed_cutoff_timestamp=completed_cutoff,
            failed_cutoff_timestamp=failed_cutoff,
            cancelled_cutoff_timestamp=cancelled_cutoff,
            max_cleanup_batch=max_cleanup_batch,
        )

        total_cleaned = (
            result.completed_cleaned + result.failed_cleaned + result.cancelled_cleaned
        )

        if total_cleaned > 0:
            logger.info(
                f"🗑️  Removed {total_cleaned} expired tasks "
                f"(completed: {result.completed_cleaned}, "
                f"failed: {result.failed_cleaned}, "
                f"cancelled: {result.cancelled_cleaned})"
            )

            # Log details of cleaned tasks
            for queue_type, task_id in result.cleaned_task_details:
                logger.debug(
                    colored(f"    [{task_id}]: cleaned from {queue_type} queue", "dim")
                )

        return total_cleaned

    except Exception as e:
        logger.error(f"Error during expired task removal: {e}")
        return 0


async def maintenance_loop(
    shutdown_event: asyncio.Event,
    redis_pool: redis.ConnectionPool,
    namespace: str,
    agent: BaseAgent[Any],
    heartbeat_timeout: int,
    max_retries: int,
    batch_size: int,
    interval: int,
    task_ttl_config: Any,  # Will be TaskTTLConfig from manager.py
    max_cleanup_batch: int,
    metrics_bucket_duration: int,
    metrics_retention_duration: int,
) -> None:
    """Background maintenance worker to periodically recover stale tasks and clean up expired tasks"""
    redis_client = redis.Redis(connection_pool=redis_pool, decode_responses=True)
    recovery_script = await create_stale_recovery_script(redis_client)
    task_expiration_script = await create_task_expiration_script(redis_client)

    logger.info(
        f"Maintenance worker started (checking every {interval}s for stale tasks >{heartbeat_timeout}s old and cleaning expired tasks)"
    )

    try:
        while not shutdown_event.is_set():
            try:
                await recover_stale_tasks(
                    recovery_script=recovery_script,
                    agent=agent,
                    heartbeat_timeout=heartbeat_timeout,
                    max_retries=max_retries,
                    batch_size=batch_size,
                    metrics_bucket_duration=metrics_bucket_duration,
                    metrics_retention_duration=metrics_retention_duration,
                    namespace=namespace,
                )

                # Recover tasks from backoff queue
                await recover_backoff_tasks(
                    redis_client=redis_client,
                    agent=agent,
                    batch_size=batch_size,
                    namespace=namespace,
                )

                # Then, remove expired tasks
                await remove_expired_tasks(
                    task_expiration_script=task_expiration_script,
                    agent=agent,
                    task_ttl_config=task_ttl_config,
                    max_cleanup_batch=max_cleanup_batch,
                    namespace=namespace,
                )

                # Wait before next check, but allow early exit on shutdown
                try:
                    jitter = random.uniform(-0.2, 0.2)  # ±20% jitter
                    jittered_interval = interval * (1 + jitter)
                    await asyncio.wait_for(
                        shutdown_event.wait(), timeout=jittered_interval
                    )
                    break  # Shutdown requested
                except asyncio.TimeoutError:
                    continue  # Normal timeout, continue monitoring

            except Exception as e:
                logger.error(f"Error in maintenance worker: {e}")
                # Wait a bit before retrying to avoid tight error loops
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
