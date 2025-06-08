from dataclasses import dataclass
from pathlib import Path
import json
import redis.asyncio as redis
from redis.commands.core import AsyncScript
from typing import Type, TypeVar, Any, cast

from factorial.utils import decode

T = TypeVar("T", bound=AsyncScript)

# Cache for loaded scripts
_SCRIPT_CONTENT: dict[str, str] = {}
_SCRIPT_INSTANCES: dict[tuple[int, str], Any] = {}


def get_script_path(script_name: str) -> Path:
    """Get the path to a Lua script file"""
    script_dir = Path(__file__).parent
    return script_dir / f"{script_name}.lua"


def load_script(name: str) -> str:
    shared_script_path = Path(__file__).parent / "shared.lua"
    if not shared_script_path.exists():
        raise FileNotFoundError(f"Shared script not found: {shared_script_path}")

    with open(shared_script_path) as f:
        shared_script_content = f.read()

    if name not in _SCRIPT_CONTENT:
        with open(get_script_path(name)) as f:
            script_content = f.read()
        _SCRIPT_CONTENT[name] = f"{shared_script_content}\n\n{script_content}"

    return _SCRIPT_CONTENT[name]


def get_cached_script(client: redis.Redis, name: str, cls: type[T]) -> T:
    key = (id(client), name)
    if key not in _SCRIPT_INSTANCES:
        script_content = load_script(name)
        base_script = client.register_script(script_content)
        inst = cls(
            registered_client=client, script=script_content
        )  # Properly initialize with required args
        inst.__dict__.update(base_script.__dict__)
        _SCRIPT_INSTANCES[key] = inst

    return cast(T, _SCRIPT_INSTANCES[key])


def create_script(
    redis_client: redis.Redis, script_name: str, script_class: Type[T]
) -> T:
    """Create a Redis script from a Lua file"""
    script_content = load_script(script_name)
    base_script = redis_client.register_script(script_content)

    script_instance = script_class(
        registered_client=redis_client, script=script_content
    )
    script_instance.__dict__.update(base_script.__dict__)
    return script_instance


@dataclass
class BatchPickupScriptResult:
    """Result of the batch pickup script"""

    tasks_to_process_ids: list[str]
    tasks_to_cancel_ids: list[str]
    orphaned_task_ids: list[str]
    corrupted_task_ids: list[str]


class BatchPickupScript(AsyncScript):
    """
    Handles picking up tasks from main queue

    Keys:
    * KEYS[1] = queue_main_key (str)
    * KEYS[2] = queue_cancelled_key (str)
    * KEYS[3] = queue_orphaned_key (str)
    * KEYS[4] = task_statuses_key (str)
    * KEYS[5] = task_agents_key (str)
    * KEYS[6] = task_payloads_key (str)
    * KEYS[7] = task_pickups_key (str)
    * KEYS[8] = task_retries_key (str)
    * KEYS[9] = task_metas_key (str)
    * KEYS[10] = task_cancellations_key (str)
    * KEYS[11] = processing_heartbeats_key (str)
    * KEYS[12] = agent_metrics_bucket_key (str)
    * KEYS[13] = global_metrics_bucket_key (str)

    Args:
    * ARGV[1] = batch_size (int)
    * ARGV[2] = metrics_ttl (int)
    """

    async def execute(
        self,
        *,
        queue_main_key: str,
        queue_cancelled_key: str,
        queue_orphaned_key: str,
        task_statuses_key: str,
        task_agents_key: str,
        task_payloads_key: str,
        task_pickups_key: str,
        task_retries_key: str,
        task_metas_key: str,
        task_cancellations_key: str,
        processing_heartbeats_key: str,
        agent_metrics_bucket_key: str,
        global_metrics_bucket_key: str,
        batch_size: int,
        metrics_ttl: int,
    ) -> BatchPickupScriptResult:
        result: tuple[
            list[str], list[str], list[str], list[str]
        ] = await super().__call__(  # type: ignore
            keys=[
                queue_main_key,
                queue_cancelled_key,
                queue_orphaned_key,
                task_statuses_key,
                task_agents_key,
                task_payloads_key,
                task_pickups_key,
                task_retries_key,
                task_metas_key,
                task_cancellations_key,
                processing_heartbeats_key,
                agent_metrics_bucket_key,
                global_metrics_bucket_key,
            ],
            args=[
                batch_size,
                metrics_ttl,
            ],
        )

        tasks_to_process_ids = [decode(id) for id in result[0]]
        tasks_to_cancel_ids = [decode(id) for id in result[1]]
        orphaned_task_ids = [decode(id) for id in result[2]]
        corrupted_task_ids = [decode(id) for id in result[3]]

        return BatchPickupScriptResult(
            tasks_to_process_ids=tasks_to_process_ids,
            tasks_to_cancel_ids=tasks_to_cancel_ids,
            orphaned_task_ids=orphaned_task_ids,
            corrupted_task_ids=corrupted_task_ids,
        )


# Convenience functions for each script
async def create_batch_pickup_script(redis_client: redis.Redis) -> BatchPickupScript:
    """
    Create atomic script for picking up tasks from main queue
    """
    return get_cached_script(redis_client, "pickup", BatchPickupScript)


@dataclass
class TaskSteeringScriptResult:
    """Result of the task steering script"""

    success: bool
    status: str


class TaskSteeringScript(AsyncScript):
    """
    Handles updating a task with steering messages

    Keys:
    * KEYS[1] = queue_orphaned_key (str)
    * KEYS[2] = task_statuses_key (str)
    * KEYS[3] = task_agents_key (str)
    * KEYS[4] = task_payloads_key (str)
    * KEYS[5] = task_pickups_key (str)
    * KEYS[6] = task_retries_key (str)
    * KEYS[7] = task_metas_key (str)
    * KEYS[8] = steering_messages_key (str)

    Args:
    * ARGV[1] = task_id (str)
    * ARGV[2] = steering_message_ids (str)
    * ARGV[3] = updated_task_payload_json (str)
    """

    async def execute(
        self,
        *,
        queue_orphaned_key: str,
        task_statuses_key: str,
        task_agents_key: str,
        task_payloads_key: str,
        task_pickups_key: str,
        task_retries_key: str,
        task_metas_key: str,
        steering_messages_key: str,
        task_id: str,
        steering_message_ids: list[str],
        updated_task_payload_json: str,
    ) -> TaskSteeringScriptResult:
        steering_message_ids_json = json.dumps(steering_message_ids)
        result: tuple[bool, str] = await super().__call__(  # type: ignore
            keys=[
                queue_orphaned_key,
                task_statuses_key,
                task_agents_key,
                task_payloads_key,
                task_pickups_key,
                task_retries_key,
                task_metas_key,
                steering_messages_key,
            ],
            args=[task_id, steering_message_ids_json, updated_task_payload_json],
        )

        return TaskSteeringScriptResult(
            success=result[0],
            status=result[1],
        )


async def create_task_steering_script(redis_client: redis.Redis) -> TaskSteeringScript:
    """
    Create atomic script for to handle updating a task with steering messages

    Keys:
    * KEYS[1] = processing_tasks (str)
    * KEYS[2] = steering_messages (str)

    Args:
    * ARGV[1] = processing_key (str)
    * ARGV[2] = steering_message_ids (str)
    * ARGV[3] = updated_task_json (str)
    """
    return get_cached_script(redis_client, "steering", TaskSteeringScript)


class TaskCompletionScript(AsyncScript):
    """
    Handles task completion, continuation, or failure

    Keys:
    * KEYS[1] = queue_main_key (str)
    * KEYS[2] = queue_completions_key (str)
    * KEYS[3] = queue_failed_key (str)
    * KEYS[4] = queue_backoff_key (str)
    * KEYS[5] = queue_orphaned_key (str)
    * KEYS[6] = task_statuses_key (str)
    * KEYS[7] = task_agents_key (str)
    * KEYS[8] = task_payloads_key (str)
    * KEYS[9] = task_pickups_key (str)
    * KEYS[10] = task_retries_key (str)
    * KEYS[11] = task_metas_key (str)
    * KEYS[12] = processing_heartbeats_key (str)
    * KEYS[13] = pending_tool_results_key (str)
    * KEYS[14] = agent_metrics_bucket_key (str)
    * KEYS[15] = global_metrics_bucket_key (str)
    * KEYS[16] = agent_idle_gauge_key (str)
    * KEYS[17] = global_idle_gauge_key (str)

    Args:
    * ARGV[1] = task_id (str)
    * ARGV[2] = action (str)
    * ARGV[3] = updated_task_payload_json (str)
    * ARGV[4] = metrics_ttl (int)
    * ARGV[5] = pending_sentinel (str)
    * ARGV[6] = pending_tool_call_ids_json (str)
    """

    async def execute(
        self,
        *,
        queue_main_key: str,
        queue_completions_key: str,
        queue_failed_key: str,
        queue_backoff_key: str,
        queue_orphaned_key: str,
        task_statuses_key: str,
        task_agents_key: str,
        task_payloads_key: str,
        task_pickups_key: str,
        task_retries_key: str,
        task_metas_key: str,
        processing_heartbeats_key: str,
        pending_tool_results_key: str,
        agent_metrics_bucket_key: str,
        global_metrics_bucket_key: str,
        agent_idle_gauge_key: str,
        global_idle_gauge_key: str,
        task_id: str,
        action: str,
        updated_task_payload_json: str,
        metrics_ttl: int,
        pending_sentinel: str | None = None,
        pending_tool_call_ids_json: str | None = None,
    ) -> bool:
        args = [
            task_id,
            action,
            updated_task_payload_json,
            metrics_ttl,
        ]

        if pending_sentinel is not None and pending_tool_call_ids_json is not None:
            args.append(pending_sentinel)
            args.append(pending_tool_call_ids_json)
        elif pending_tool_call_ids_json is not None:
            raise ValueError(
                "pending_sentinel must be provided if pending_tool_call_ids_json is provided"
            )

        return await super().__call__(  # type: ignore
            keys=[
                queue_main_key,
                queue_completions_key,
                queue_failed_key,
                queue_backoff_key,
                queue_orphaned_key,
                task_statuses_key,
                task_agents_key,
                task_payloads_key,
                task_pickups_key,
                task_retries_key,
                task_metas_key,
                processing_heartbeats_key,
                pending_tool_results_key,
                agent_metrics_bucket_key,
                global_metrics_bucket_key,
                agent_idle_gauge_key,
                global_idle_gauge_key,
            ],
            args=args,
        )


async def create_task_completion_script(
    redis_client: redis.Redis,
) -> TaskCompletionScript:
    """
    Create atomic script for handling task completion, continuation, or failure
    """
    return get_cached_script(redis_client, "completion", TaskCompletionScript)


@dataclass
class StaleRecoveryScriptResult:
    """Result of the stale recovery script"""

    recovered_count: int
    failed_count: int
    stale_task_actions: list[tuple[str, str]]


class StaleRecoveryScript(AsyncScript):
    """
    Handles recovering stale tasks with retry increment

    Keys:
    * KEYS[1] = queue_main_key (str)
    * KEYS[2] = queue_failed_key (str)
    * KEYS[3] = queue_orphaned_key (str)
    * KEYS[4] = task_statuses_key (str)
    * KEYS[5] = task_agents_key (str)
    * KEYS[6] = task_payloads_key (str)
    * KEYS[7] = task_pickups_key (str)
    * KEYS[8] = task_retries_key (str)
    * KEYS[9] = task_metas_key (str)
    * KEYS[10] = processing_heartbeats_key (str)
    * KEYS[11] = agent_metrics_bucket_key (str)
    * KEYS[12] = global_metrics_bucket_key (str)

    Args:
    * ARGV[1] = cutoff_timestamp (int)
    * ARGV[2] = max_recovery_batch (int)
    * ARGV[3] = max_retries (int)
    * ARGV[4] = metrics_ttl (int)
    """

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
        result: tuple[int, int, list[tuple[str, str]]] = await super().__call__(  # type: ignore
            keys=[
                queue_main_key,
                queue_failed_key,
                queue_orphaned_key,
                task_statuses_key,
                task_agents_key,
                task_payloads_key,
                task_pickups_key,
                task_retries_key,
                task_metas_key,
                processing_heartbeats_key,
                agent_metrics_bucket_key,
                global_metrics_bucket_key,
            ],
            args=[
                cutoff_timestamp,
                max_recovery_batch,
                max_retries,
                metrics_ttl,
            ],
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
    """
    Creates an atomic script for recovering stale tasks with retry increment
    """
    return get_cached_script(redis_client, "recovery", StaleRecoveryScript)


@dataclass
class TaskExpirationScriptResult:
    """Result of the task expiration script"""

    completed_cleaned: int
    failed_cleaned: int
    cancelled_cleaned: int
    cleaned_task_details: list[tuple[str, str]]  # [(queue_type, task_id), ...]


class TaskExpirationScript(AsyncScript):
    """
    Handles task expiration from completion, failed, and cancelled queues

    Keys:
    * KEYS[1] = queue_completions_key (str)
    * KEYS[2] = queue_failed_key (str)
    * KEYS[3] = queue_cancelled_key (str)
    * KEYS[4] = queue_orphaned_key (str)
    * KEYS[5] = task_statuses_key (str)
    * KEYS[6] = task_agents_key (str)
    * KEYS[7] = task_payloads_key (str)
    * KEYS[8] = task_pickups_key (str)
    * KEYS[9] = task_retries_key (str)
    * KEYS[10] = task_metas_key (str)

    Args:
    * ARGV[1] = completed_cutoff_timestamp (float)
    * ARGV[2] = failed_cutoff_timestamp (float)
    * ARGV[3] = cancelled_cutoff_timestamp (float)
    * ARGV[4] = max_cleanup_batch (int)
    """

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
        result: tuple[int, int, int, list[tuple[str, str]]] = await super().__call__(  # type: ignore
            keys=[
                queue_completions_key,
                queue_failed_key,
                queue_cancelled_key,
                queue_orphaned_key,
                task_statuses_key,
                task_agents_key,
                task_payloads_key,
                task_pickups_key,
                task_retries_key,
                task_metas_key,
            ],
            args=[
                completed_cutoff_timestamp,
                failed_cutoff_timestamp,
                cancelled_cutoff_timestamp,
                max_cleanup_batch,
            ],
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
    """
    Creates an atomic script for task expiration
    """
    return get_cached_script(redis_client, "expiration", TaskExpirationScript)


class ToolCompletionScript(AsyncScript):
    """
    Simple atomic script to move completed task back to queue and cleanup

    Keys:
    * KEYS[1] = queue_main_key (str)
    * KEYS[2] = queue_orphaned_key (str)
    * KEYS[3] = pending_tool_results_key (str)
    * KEYS[4] = task_statuses_key (str)
    * KEYS[5] = task_agents_key (str)
    * KEYS[6] = task_payloads_key (str)
    * KEYS[7] = task_pickups_key (str)
    * KEYS[8] = task_retries_key (str)
    * KEYS[9] = task_metas_key (str)

    Args:
    * ARGV[1] = task_id (str)
    * ARGV[2] = updated_task_context_json (str)
    """

    async def execute(
        self,
        *,
        queue_main_key: str,
        queue_orphaned_key: str,
        pending_tool_results_key: str,
        task_statuses_key: str,
        task_agents_key: str,
        task_payloads_key: str,
        task_pickups_key: str,
        task_retries_key: str,
        task_metas_key: str,
        task_id: str,
        updated_task_context_json: str,
    ) -> bool:
        return await super().__call__(  # type: ignore
            keys=[
                pending_tool_results_key,
                queue_main_key,
                queue_orphaned_key,
                task_statuses_key,
                task_agents_key,
                task_payloads_key,
                task_pickups_key,
                task_retries_key,
                task_metas_key,
            ],
            args=[
                task_id,
                updated_task_context_json,
            ],
        )


async def create_tool_completion_script(
    redis_client: redis.Redis,
) -> ToolCompletionScript:
    """
    Creates a simple atomic script to move completed task back to queue and cleanup
    """
    return get_cached_script(redis_client, "tool_completion", ToolCompletionScript)


class EnqueueTaskScript(AsyncScript):
    """
    Creates a script for enqueuing a task

    Keys:
    * KEYS[1] = agent_queue_key (str)
    * KEYS[2] = task_statuses_key (str)
    * KEYS[3] = task_agents_key (str)
    * KEYS[4] = task_payloads_key (str)
    * KEYS[5] = task_pickups_key (str)
    * KEYS[6] = task_retries_key (str)
    * KEYS[7] = task_metas_key (str)

    Args:
    * ARGV[1] = task_id (str)
    * ARGV[2] = task_agent (str)
    * ARGV[3] = task_payload_json (str)
    * ARGV[4] = task_pickups (int)
    * ARGV[5] = task_retries (int)
    * ARGV[6] = task_meta_json (str)
    """

    async def execute(
        self,
        *,
        agent_queue_key: str,
        task_statuses_key: str,
        task_agents_key: str,
        task_payloads_key: str,
        task_pickups_key: str,
        task_retries_key: str,
        task_metas_key: str,
        task_id: str,
        task_agent: str,
        task_payload_json: str,
        task_pickups: int,
        task_retries: int,
        task_meta_json: str,
    ) -> bool:
        return await super().__call__(  # type: ignore
            keys=[
                agent_queue_key,
                task_statuses_key,
                task_agents_key,
                task_payloads_key,
                task_pickups_key,
                task_retries_key,
                task_metas_key,
            ],
            args=[
                task_id,
                task_agent,
                task_payload_json,
                task_pickups,
                task_retries,
                task_meta_json,
            ],
        )


async def create_enqueue_task_script(redis_client: redis.Redis) -> EnqueueTaskScript:
    """
    Creates a script for enqueuing a task
    """
    return get_cached_script(redis_client, "enqueue", EnqueueTaskScript)


class BackoffRecoveryScript(AsyncScript):
    """
    Handles recovering tasks from backoff queue

    Keys:
    * KEYS[1] = queue_backoff_key (str)
    * KEYS[2] = queue_main_key (str)
    * KEYS[3] = queue_orphaned_key (str)
    * KEYS[4] = task_statuses_key (str)
    * KEYS[5] = task_agents_key (str)
    * KEYS[6] = task_payloads_key (str)
    * KEYS[7] = task_pickups_key (str)
    * KEYS[8] = task_retries_key (str)
    * KEYS[9] = task_metas_key (str)

    Args:
    * ARGV[1] = max_batch_size (int)
    """

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
        res: list[str | bytes] = await super().__call__(  # type: ignore
            keys=[
                queue_backoff_key,
                queue_main_key,
                queue_orphaned_key,
                task_statuses_key,
                task_agents_key,
                task_payloads_key,
                task_pickups_key,
                task_retries_key,
                task_metas_key,
            ],
            args=[max_batch_size],
        )
        return [decode(id) for id in res]


async def create_backoff_recovery_script(
    redis_client: redis.Redis,
) -> BackoffRecoveryScript:
    """
    Creates an atomic script for recovering tasks from backoff queue
    """
    return get_cached_script(redis_client, "backoff", BackoffRecoveryScript)


@dataclass
class CancelTaskScriptResult:
    """Result of the cancel task script"""

    success: bool
    current_status: str | None
    message: str


class CancelTaskScript(AsyncScript):
    """
    Handles unified task cancellation for all task states

    Keys:
    * KEYS[1] = queue_cancelled_key (str)
    * KEYS[2] = queue_backoff_key (str)
    * KEYS[3] = queue_orphaned_key (str)
    * KEYS[4] = pending_cancellations_key (str)
    * KEYS[5] = task_statuses_key (str)
    * KEYS[6] = task_agents_key (str)
    * KEYS[7] = task_payloads_key (str)
    * KEYS[8] = task_pickups_key (str)
    * KEYS[9] = task_retries_key (str)
    * KEYS[10] = task_metas_key (str)
    * KEYS[11] = pending_tool_results_key (str)
    * KEYS[12] = agent_metrics_bucket_key (str)
    * KEYS[13] = global_metrics_bucket_key (str)

    Args:
    * ARGV[1] = task_id (str)
    * ARGV[2] = metrics_ttl (int)
    """

    async def execute(
        self,
        *,
        queue_cancelled_key: str,
        queue_backoff_key: str,
        queue_orphaned_key: str,
        pending_cancellations_key: str,
        task_statuses_key: str,
        task_agents_key: str,
        task_payloads_key: str,
        task_pickups_key: str,
        task_retries_key: str,
        task_metas_key: str,
        pending_tool_results_key: str,
        agent_metrics_bucket_key: str,
        global_metrics_bucket_key: str,
        task_id: str,
        metrics_ttl: int,
    ) -> CancelTaskScriptResult:
        result: tuple[bool, str | None, str] = await super().__call__(  # type: ignore
            keys=[
                queue_cancelled_key,
                queue_backoff_key,
                queue_orphaned_key,
                pending_cancellations_key,
                task_statuses_key,
                task_agents_key,
                task_payloads_key,
                task_pickups_key,
                task_retries_key,
                task_metas_key,
                pending_tool_results_key,
                agent_metrics_bucket_key,
                global_metrics_bucket_key,
            ],
            args=[
                task_id,
                metrics_ttl,
            ],
        )

        return CancelTaskScriptResult(
            success=bool(decode(result[0])),
            current_status=decode(result[1]) if result[1] is not None else None,
            message=decode(result[2]),
        )


async def create_cancel_task_script(
    redis_client: redis.Redis,
) -> CancelTaskScript:
    """
    Creates a unified script for cancelling tasks in any state
    """
    return get_cached_script(redis_client, "cancellation", CancelTaskScript)
