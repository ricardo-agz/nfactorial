from dataclasses import dataclass

import redis.asyncio as redis
from redis.commands.core import AsyncScript

from factorial.core.utils import decode

from ._core import LuaScriptContract, _execute_contract, get_cached_script


@dataclass
class StaleRecoveryScriptResult:
    recovered_count: int
    failed_count: int
    stale_task_actions: list[tuple[str, str]]


class StaleRecoveryScript(AsyncScript):
    _CONTRACT = LuaScriptContract(
        script_name="StaleRecoveryScript.execute",
        key_fields=(
            "queue_main_key",
            "queue_failed_key",
            "queue_orphaned_key",
            "task_statuses_key",
            "task_agents_key",
            "task_payloads_key",
            "task_pickups_key",
            "task_retries_key",
            "task_metas_key",
            "processing_heartbeats_key",
            "agent_metrics_bucket_key",
            "global_metrics_bucket_key",
        ),
        arg_fields=(
            "cutoff_timestamp",
            "max_recovery_batch",
            "max_retries",
            "metrics_ttl",
        ),
    )

    async def execute(
        self,
        *,
        queue_main_key: str,
        queue_failed_key: str,
        queue_orphaned_key: str,
        task_statuses_key: str,
        task_agents_key: str,
        task_payloads_key: str,
        task_pickups_key: str,
        task_retries_key: str,
        task_metas_key: str,
        processing_heartbeats_key: str,
        agent_metrics_bucket_key: str,
        global_metrics_bucket_key: str,
        cutoff_timestamp: float,
        max_recovery_batch: int,
        max_retries: int,
        metrics_ttl: int,
    ) -> StaleRecoveryScriptResult:
        result: tuple[int, int, list[tuple[str, str]]] = await _execute_contract(
            self, self._CONTRACT, locals()
        )
        stale_task_actions = [
            (decode(task_id), decode(action)) for task_id, action in result[2]
        ]
        return StaleRecoveryScriptResult(
            recovered_count=result[0],
            failed_count=result[1],
            stale_task_actions=stale_task_actions,
        )


async def create_stale_recovery_script(
    redis_client: redis.Redis,
) -> StaleRecoveryScript:
    return get_cached_script(redis_client, "recovery", StaleRecoveryScript)


@dataclass
class TaskExpirationScriptResult:
    completed_cleaned: int
    failed_cleaned: int
    cancelled_cleaned: int
    cleaned_task_details: list[tuple[str, str]]


class TaskExpirationScript(AsyncScript):
    _CONTRACT = LuaScriptContract(
        script_name="TaskExpirationScript.execute",
        key_fields=(
            "queue_completions_key",
            "queue_failed_key",
            "queue_cancelled_key",
            "queue_orphaned_key",
            "task_statuses_key",
            "task_agents_key",
            "task_payloads_key",
            "task_pickups_key",
            "task_retries_key",
            "task_metas_key",
        ),
        arg_fields=(
            "completed_cutoff_timestamp",
            "failed_cutoff_timestamp",
            "cancelled_cutoff_timestamp",
            "max_cleanup_batch",
        ),
    )

    async def execute(
        self,
        *,
        queue_completions_key: str,
        queue_failed_key: str,
        queue_cancelled_key: str,
        queue_orphaned_key: str,
        task_statuses_key: str,
        task_agents_key: str,
        task_payloads_key: str,
        task_pickups_key: str,
        task_retries_key: str,
        task_metas_key: str,
        completed_cutoff_timestamp: float,
        failed_cutoff_timestamp: float,
        cancelled_cutoff_timestamp: float,
        max_cleanup_batch: int,
    ) -> TaskExpirationScriptResult:
        result: tuple[int, int, int, list[tuple[str, str]]] = await _execute_contract(
            self, self._CONTRACT, locals()
        )
        cleaned_task_details = [
            (decode(queue_type), decode(task_id)) for queue_type, task_id in result[3]
        ]
        return TaskExpirationScriptResult(
            completed_cleaned=result[0],
            failed_cleaned=result[1],
            cancelled_cleaned=result[2],
            cleaned_task_details=cleaned_task_details,
        )


async def create_task_expiration_script(
    redis_client: redis.Redis,
) -> TaskExpirationScript:
    return get_cached_script(redis_client, "expiration", TaskExpirationScript)


class BackoffRecoveryScript(AsyncScript):
    _CONTRACT = LuaScriptContract(
        script_name="BackoffRecoveryScript.execute",
        key_fields=(
            "queue_backoff_key",
            "queue_main_key",
            "queue_orphaned_key",
            "task_statuses_key",
            "task_agents_key",
            "task_payloads_key",
            "task_pickups_key",
            "task_retries_key",
            "task_metas_key",
        ),
        arg_fields=("max_batch_size",),
    )

    async def execute(
        self,
        *,
        queue_backoff_key: str,
        queue_main_key: str,
        queue_orphaned_key: str,
        task_statuses_key: str,
        task_agents_key: str,
        task_payloads_key: str,
        task_pickups_key: str,
        task_retries_key: str,
        task_metas_key: str,
        max_batch_size: int,
    ) -> list[str]:
        result: list[str | bytes] = await _execute_contract(
            self, self._CONTRACT, locals()
        )
        return [decode(task_id) for task_id in result]


async def create_backoff_recovery_script(
    redis_client: redis.Redis,
) -> BackoffRecoveryScript:
    return get_cached_script(redis_client, "backoff", BackoffRecoveryScript)


class ScheduledRecoveryScript(AsyncScript):
    _CONTRACT = LuaScriptContract(
        script_name="ScheduledRecoveryScript.execute",
        key_fields=(
            "queue_scheduled_key",
            "queue_main_key",
            "queue_pending_key",
            "queue_orphaned_key",
            "task_statuses_key",
            "task_agents_key",
            "task_payloads_key",
            "task_pickups_key",
            "task_retries_key",
            "task_metas_key",
            "scheduled_wait_meta_key",
            "activity_wait_meta_key",
            "signal_wait_meta_key",
            "signal_wake_meta_key",
        ),
        arg_fields=("max_batch_size",),
    )

    async def execute(
        self,
        *,
        queue_scheduled_key: str,
        queue_main_key: str,
        queue_pending_key: str,
        queue_orphaned_key: str,
        task_statuses_key: str,
        task_agents_key: str,
        task_payloads_key: str,
        task_pickups_key: str,
        task_retries_key: str,
        task_metas_key: str,
        scheduled_wait_meta_key: str,
        activity_wait_meta_key: str,
        signal_wait_meta_key: str,
        signal_wake_meta_key: str,
        max_batch_size: int,
    ) -> list[str]:
        result: list[str | bytes] = await _execute_contract(
            self, self._CONTRACT, locals()
        )
        return [decode(task_id) for task_id in result]


async def create_scheduled_recovery_script(
    redis_client: redis.Redis,
) -> ScheduledRecoveryScript:
    return get_cached_script(redis_client, "scheduled", ScheduledRecoveryScript)
