from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any

import redis.asyncio as redis

from factorial.agent import BaseAgent
from factorial.logging import get_logger
from factorial.queue.keys import RedisKeys
from factorial.queue.lua import (
    ScheduledRecoveryScript,
    StaleRecoveryScript,
    StaleRecoveryScriptResult,
    TaskExpirationScript,
    TaskExpirationScriptResult,
    create_backoff_recovery_script,
    create_scheduled_recovery_script,
    create_stale_recovery_script,
    create_task_expiration_script,
)
from factorial.queue.operations import (
    expire_pending_hooks,
    resume_if_no_remaining_child_tasks,
)
from factorial.queue.task import TaskStatus

logger = get_logger(__name__)


@dataclass
class MaintenanceTickResult:
    stale_recovered: int = 0
    backoff_recovered: int = 0
    scheduled_recovered: int = 0
    pending_child_resumed: int = 0
    expired_hooks: int = 0
    expired_tasks_removed: int = 0
    expired_batches_removed: int = 0
    touched_agents: list[str] = field(default_factory=list)
    duration_ms: int = 0


@dataclass
class MaintenanceTickContext:
    redis_client: redis.Redis
    namespace: str
    agent: BaseAgent[Any]
    heartbeat_timeout: int
    max_retries: int
    batch_size: int
    task_ttl_config: Any
    max_cleanup_batch: int
    metrics_retention_duration: int
    recovery_script: StaleRecoveryScript
    task_expiration_script: TaskExpirationScript
    scheduled_recovery_script: ScheduledRecoveryScript

    @classmethod
    async def create(
        cls,
        *,
        redis_client: redis.Redis,
        namespace: str,
        agent: BaseAgent[Any],
        heartbeat_timeout: int,
        max_retries: int,
        batch_size: int,
        task_ttl_config: Any,
        max_cleanup_batch: int,
        metrics_retention_duration: int,
    ) -> MaintenanceTickContext:
        return cls(
            redis_client=redis_client,
            namespace=namespace,
            agent=agent,
            heartbeat_timeout=heartbeat_timeout,
            max_retries=max_retries,
            batch_size=batch_size,
            task_ttl_config=task_ttl_config,
            max_cleanup_batch=max_cleanup_batch,
            metrics_retention_duration=metrics_retention_duration,
            recovery_script=await create_stale_recovery_script(redis_client),
            task_expiration_script=await create_task_expiration_script(redis_client),
            scheduled_recovery_script=await create_scheduled_recovery_script(
                redis_client
            ),
        )


async def maintenance_tick(context: MaintenanceTickContext) -> MaintenanceTickResult:
    started_at = time.monotonic()
    result = MaintenanceTickResult()

    result.stale_recovered = await recover_stale_tasks(
        recovery_script=context.recovery_script,
        namespace=context.namespace,
        agent=context.agent,
        heartbeat_timeout=context.heartbeat_timeout,
        max_retries=context.max_retries,
        batch_size=context.batch_size,
        metrics_retention_duration=context.metrics_retention_duration,
    )

    result.backoff_recovered = await recover_backoff_tasks(
        redis_client=context.redis_client,
        namespace=context.namespace,
        agent=context.agent,
        batch_size=context.batch_size,
    )

    result.scheduled_recovered = await recover_scheduled_tasks(
        scheduled_recovery_script=context.scheduled_recovery_script,
        namespace=context.namespace,
        agent=context.agent,
        batch_size=context.batch_size,
    )

    result.pending_child_resumed = await recover_ready_pending_child_tasks(
        redis_client=context.redis_client,
        namespace=context.namespace,
        agent=context.agent,
        batch_size=context.batch_size,
    )

    result.expired_hooks = await expire_pending_hooks(
        redis_client=context.redis_client,
        namespace=context.namespace,
        max_cleanup_batch=context.max_cleanup_batch,
    )

    result.expired_tasks_removed = await remove_expired_tasks(
        task_expiration_script=context.task_expiration_script,
        namespace=context.namespace,
        agent=context.agent,
        task_ttl_config=context.task_ttl_config,
        max_cleanup_batch=context.max_cleanup_batch,
    )

    result.expired_batches_removed = await cleanup_finished_batches(
        redis_client=context.redis_client,
        namespace=context.namespace,
        completed_ttl=context.task_ttl_config.completed_ttl,
        max_cleanup_batch=context.max_cleanup_batch,
    )

    if any(
        [
            result.stale_recovered,
            result.backoff_recovered,
            result.scheduled_recovered,
            result.pending_child_resumed,
            result.expired_hooks,
        ]
    ):
        result.touched_agents.append(context.agent.name)

    result.duration_ms = int((time.monotonic() - started_at) * 1000)
    return result


async def recover_stale_tasks(
    recovery_script: StaleRecoveryScript,
    namespace: str,
    agent: BaseAgent[Any],
    heartbeat_timeout: int,
    max_retries: int,
    batch_size: int,
    metrics_retention_duration: int,
) -> int:
    cutoff_timestamp = time.time() - heartbeat_timeout
    keys = RedisKeys.format(
        namespace=namespace,
        agent=agent.name,
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
        return result.recovered_count
    except Exception as exc:
        logger.error("Error during stale task recovery", exc_info=exc)
        return 0


async def recover_backoff_tasks(
    redis_client: redis.Redis,
    namespace: str,
    agent: BaseAgent[Any],
    batch_size: int,
) -> int:
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
        return len(recovered_task_ids)
    except Exception as exc:
        logger.error("Error during backoff task recovery", exc_info=exc)
        return 0


async def recover_scheduled_tasks(
    scheduled_recovery_script: ScheduledRecoveryScript,
    namespace: str,
    agent: BaseAgent[Any],
    batch_size: int,
) -> int:
    keys = RedisKeys.format(namespace=namespace, agent=agent.name)
    try:
        recovered_task_ids = await scheduled_recovery_script.execute(
            queue_scheduled_key=keys.queue_scheduled,
            queue_main_key=keys.queue_main,
            queue_pending_key=keys.queue_pending,
            queue_orphaned_key=keys.queue_orphaned,
            task_statuses_key=keys.task_status,
            task_agents_key=keys.task_agent,
            task_payloads_key=keys.task_payload,
            task_pickups_key=keys.task_pickups,
            task_retries_key=keys.task_retries,
            task_metas_key=keys.task_meta,
            scheduled_wait_meta_key=keys.scheduled_wait_meta,
            activity_wait_meta_key=keys.activity_wait_meta,
            max_batch_size=batch_size,
        )
        return len(recovered_task_ids)
    except Exception as exc:
        logger.error("Error during scheduled task recovery", exc_info=exc)
        return 0


async def recover_ready_pending_child_tasks(
    redis_client: redis.Redis,
    namespace: str,
    agent: BaseAgent[Any],
    batch_size: int,
) -> int:
    keys = RedisKeys.format(namespace=namespace, agent=agent.name)
    try:
        pending_task_ids = await redis_client.zrange(
            keys.queue_pending,
            0,
            max(batch_size - 1, 0),
        )
        if not pending_task_ids:
            return 0

        resumed = 0
        agents_by_name = {agent.name: agent}
        for pending_task_id in pending_task_ids:
            task_id = (
                pending_task_id.decode("utf-8")
                if isinstance(pending_task_id, bytes)
                else str(pending_task_id)
            )
            status = await redis_client.hget(keys.task_status, task_id)  # type: ignore[misc]
            if status != TaskStatus.PENDING_CHILD_TASKS:
                continue
            if await resume_if_no_remaining_child_tasks(
                redis_client=redis_client,
                namespace=namespace,
                agents_by_name=agents_by_name,
                task_id=task_id,
            ):
                resumed += 1
        return resumed
    except Exception as exc:
        logger.error("Error during pending-child recovery", exc_info=exc)
        return 0


async def remove_expired_tasks(
    task_expiration_script: TaskExpirationScript,
    namespace: str,
    agent: BaseAgent[Any],
    task_ttl_config: Any,
    max_cleanup_batch: int,
) -> int:
    current_time = time.time()
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
        return (
            result.completed_cleaned + result.failed_cleaned + result.cancelled_cleaned
        )
    except Exception as exc:
        logger.error("Error during expired task removal", exc_info=exc)
        return 0


async def cleanup_finished_batches(
    redis_client: redis.Redis,
    namespace: str,
    completed_ttl: int,
    max_cleanup_batch: int,
) -> int:
    cutoff_timestamp = time.time() - completed_ttl
    keys = RedisKeys.format(namespace=namespace)
    expired_batch_ids: list[str] = await redis_client.zrangebyscore(
        keys.batch_completed,
        "-inf",
        cutoff_timestamp,
        start=0,
        num=max_cleanup_batch,
    )  # type: ignore[arg-type]

    if not expired_batch_ids:
        return 0

    pipe = redis_client.pipeline(transaction=True)
    for batch_id in expired_batch_ids:
        pipe.hdel(keys.batch_meta, batch_id)
        pipe.hdel(keys.batch_tasks, batch_id)
        pipe.hdel(keys.batch_remaining_tasks, batch_id)
        pipe.hdel(keys.batch_progress, batch_id)
        pipe.zrem(keys.batch_completed, batch_id)
    await pipe.execute()
    return len(expired_batch_ids)
