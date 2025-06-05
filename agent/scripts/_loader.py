from dataclasses import dataclass
from pathlib import Path
import json
import redis.asyncio as redis
from redis.commands.core import AsyncScript
from typing import Type, TypeVar, Any, cast

from agent.utils import decode

T = TypeVar("T", bound=AsyncScript)

# Cache for loaded scripts
_SCRIPT_CONTENT: dict[str, str] = {}
_SCRIPT_INSTANCES: dict[tuple[int, str], Any] = {}


def get_script_path(script_name: str) -> Path:
    """Get the path to a Lua script file"""
    script_dir = Path(__file__).parent
    return script_dir / f"{script_name}.lua"


def load_script(name: str) -> str:
    if name not in _SCRIPT_CONTENT:
        with open(get_script_path(name)) as f:
            _SCRIPT_CONTENT[name] = f.read()
    return _SCRIPT_CONTENT[name]


def get_cached_script(client: redis.Redis, name: str, cls: type[T]) -> T:
    key = (id(client), name)
    if key not in _SCRIPT_INSTANCES:
        base = client.register_script(load_script(name))
        inst = cls.__new__(cls)  # type: ignore
        inst.__dict__.update(base.__dict__)
        _SCRIPT_INSTANCES[key] = inst
    return cast(T, _SCRIPT_INSTANCES[key])


def create_script(
    redis_client: redis.Redis, script_name: str, script_class: Type[T]
) -> T:
    """Create a Redis script from a Lua file"""
    script_content = load_script(script_name)
    base_script = redis_client.register_script(script_content)

    # Create an instance of the specific script class with the same internals
    script_instance = script_class.__new__(script_class)
    script_instance.__dict__.update(base_script.__dict__)
    return script_instance


@dataclass
class BatchPickupScriptResult:
    """Result of the batch pickup script"""

    tasks_to_process_ids: list[str]
    tasks_to_cancel_ids: list[str]
    orphaned_task_ids: list[str]


class BatchPickupScript(AsyncScript):
    """
    Handles picking up tasks from main queue

    Keys:
    * KEYS[1] = task_data_key_template (str)
    * KEYS[2] = queue_main_key (str)
    * KEYS[3] = queue_cancelled_key (str)
    * KEYS[4] = processing_heartbeats_key (str)
    * KEYS[5] = task_cancellations_key (str)
    * KEYS[6] = queue_orphaned_key (str)
    * KEYS[7] = task_metrics_key_template (str)

    Args:
    * ARGV[1] = batch_size (int)
    * ARGV[2] = agent_name (str)
    * ARGV[3] = metrics_bucket_duration (int)
    * ARGV[4] = metrics_retention_duration (int)
    """

    async def execute(
        self,
        *,
        task_data_key_template: str,
        queue_main_key: str,
        queue_cancelled_key: str,
        processing_heartbeats_key: str,
        task_cancellations_key: str,
        queue_orphaned_key: str,
        task_metrics_key_template: str,
        batch_size: int,
        agent_name: str,
        metrics_bucket_duration: int,
        metrics_retention_duration: int,
    ) -> BatchPickupScriptResult:
        result: tuple[list[str], list[str]] = await super().__call__(  # type: ignore
            keys=[
                task_data_key_template,
                queue_main_key,
                queue_cancelled_key,
                processing_heartbeats_key,
                task_cancellations_key,
                queue_orphaned_key,
                task_metrics_key_template,
            ],
            args=[
                batch_size,
                agent_name,
                metrics_bucket_duration,
                metrics_retention_duration,
            ],
        )

        # Ensure task IDs are strings, handling potential bytes objects
        tasks_to_process_ids = [
            id.decode("utf-8") if isinstance(id, bytes) else str(id) for id in result[0]
        ]
        tasks_to_cancel_ids = [
            id.decode("utf-8") if isinstance(id, bytes) else str(id) for id in result[1]
        ]
        orphaned_task_ids = [
            id.decode("utf-8") if isinstance(id, bytes) else str(id) for id in result[2]
        ]

        return BatchPickupScriptResult(
            tasks_to_process_ids=tasks_to_process_ids,
            tasks_to_cancel_ids=tasks_to_cancel_ids,
            orphaned_task_ids=orphaned_task_ids,
        )


# Convenience functions for each script
def create_batch_pickup_script(redis_client: redis.Redis) -> BatchPickupScript:
    """
    Create atomic script for picking up tasks from main queue
    """
    return get_cached_script(redis_client, "pickup", BatchPickupScript)


class TaskSteeringScript(AsyncScript):
    """
    Handles updating a task with steering messages

    Keys:
    * KEYS[1] = tasks_data_key (str)
    * KEYS[2] = steering_messages_key (str)

    Args:
    * ARGV[1] = steering_message_ids (str)
    * ARGV[2] = updated_task_payload_json (str)
    """

    async def execute(
        self,
        *,
        task_data_key: str,
        steering_messages_key: str,
        steering_message_ids: list[str],
        updated_task_payload_json: str,
    ) -> None:
        steering_message_ids_json = json.dumps(steering_message_ids)
        return await super().__call__(  # type: ignore
            keys=[task_data_key, steering_messages_key],
            args=[steering_message_ids_json, updated_task_payload_json],
        )


def create_task_steering_script(redis_client: redis.Redis) -> TaskSteeringScript:
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
    * KEYS[1] = tasks_data_key (str)
    * KEYS[2] = processing_heartbeats_key (str)
    * KEYS[3] = queue_main_key (str)
    * KEYS[4] = queue_completions_key (str)
    * KEYS[5] = queue_failed_key (str)
    * KEYS[6] = pending_tool_results_key (str)
    * KEYS[7] = queue_backoff_key (str)
    * KEYS[8] = task_metrics_key_template (str)
    * KEYS[9] = gauge_idle_key_template (str)

    Args:
    * ARGV[1] = task_id (str)
    * ARGV[2] = agent_name (str)
    * ARGV[3] = action (str)
    * ARGV[4] = updated_task_payload_json (str)
    * ARGV[5] = metrics_bucket_duration (int)
    * ARGV[6] = metrics_retention_duration (int)
    * ARGV[7] = pending_tool_call_result_sentinel (str)
    * ARGV[8] = pending_tool_call_ids_json (str)
    """

    async def execute(
        self,
        *,
        tasks_data_key: str,
        processing_heartbeats_key: str,
        queue_main_key: str,
        queue_completions_key: str,
        queue_failed_key: str,
        pending_tool_results_key: str,
        queue_backoff_key: str,
        task_metrics_key_template: str,
        gauge_idle_key_template: str,
        task_id: str,
        agent_name: str,
        action: str,
        updated_task_payload_json: str,
        metrics_bucket_duration: int,
        metrics_retention_duration: int,
        pending_tool_call_result_sentinel: str | None = None,
        pending_tool_call_ids_json: str | None = None,
    ) -> bool:
        args = [
            task_id,
            agent_name,
            action,
            updated_task_payload_json,
            metrics_bucket_duration,
            metrics_retention_duration,
        ]

        if (
            pending_tool_call_result_sentinel is not None
            and pending_tool_call_ids_json is not None
        ):
            args.append(pending_tool_call_result_sentinel)
            args.append(pending_tool_call_ids_json)
        elif pending_tool_call_ids_json is not None:
            raise ValueError(
                "pending_tool_call_result_sentinel must be provided if pending_tool_call_ids_json is provided"
            )

        return await super().__call__(  # type: ignore
            keys=[
                tasks_data_key,
                processing_heartbeats_key,
                queue_main_key,
                queue_completions_key,
                queue_failed_key,
                pending_tool_results_key,
                queue_backoff_key,
                task_metrics_key_template,
                gauge_idle_key_template,
            ],
            args=args,
        )


def create_task_completion_script(redis_client: redis.Redis) -> TaskCompletionScript:
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
    * KEYS[1] = task_data_key_template (str)
    * KEYS[2] = queue_main_key (str)
    * KEYS[3] = processing_heartbeats_key (str)
    * KEYS[4] = queue_failed_key (str)
    * KEYS[5] = task_metrics_key_template (str)

    Args:
    * ARGV[1] = cutoff_timestamp (int)
    * ARGV[2] = max_recovery_batch (int)
    * ARGV[3] = max_retries (int)
    * ARGV[4] = agent_name (str)
    * ARGV[5] = metrics_bucket_duration (int)
    * ARGV[6] = metrics_retention_duration (int)
    """

    async def execute(
        self,
        *,
        task_data_key_template: str,
        queue_main_key: str,
        processing_heartbeats_key: str,
        queue_failed_key: str,
        task_metrics_key_template: str,
        cutoff_timestamp: float,
        max_recovery_batch: int,
        max_retries: int,
        agent_name: str,
        metrics_bucket_duration: int,
        metrics_retention_duration: int,
    ) -> StaleRecoveryScriptResult:
        result: tuple[int, int, list[tuple[str, str]]] = await super().__call__(  # type: ignore
            keys=[
                task_data_key_template,
                queue_main_key,
                processing_heartbeats_key,
                queue_failed_key,
                task_metrics_key_template,
            ],
            args=[
                cutoff_timestamp,
                max_recovery_batch,
                max_retries,
                agent_name,
                metrics_bucket_duration,
                metrics_retention_duration,
            ],
        )

        # Ensure task IDs and actions are strings, handling potential bytes objects
        stale_task_actions = [
            (
                task_id.decode("utf-8") if isinstance(task_id, bytes) else str(task_id),
                action.decode("utf-8") if isinstance(action, bytes) else str(action),
            )
            for task_id, action in result[2]
        ]

        return StaleRecoveryScriptResult(
            recovered_count=result[0],
            failed_count=result[1],
            stale_task_actions=stale_task_actions,
        )


def create_stale_recovery_script(redis_client: redis.Redis) -> StaleRecoveryScript:
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
    * KEYS[1] = task_data_key_template (str)
    * KEYS[2] = queue_completions_key (str)
    * KEYS[3] = queue_failed_key (str)
    * KEYS[4] = queue_cancelled_key (str)

    Args:
    * ARGV[1] = completed_cutoff_timestamp (float)
    * ARGV[2] = failed_cutoff_timestamp (float)
    * ARGV[3] = cancelled_cutoff_timestamp (float)
    * ARGV[4] = max_cleanup_batch (int)
    """

    async def execute(
        self,
        *,
        task_data_key_template: str,
        queue_completions_key: str,
        queue_failed_key: str,
        queue_cancelled_key: str,
        completed_cutoff_timestamp: float,
        failed_cutoff_timestamp: float,
        cancelled_cutoff_timestamp: float,
        max_cleanup_batch: int,
    ) -> TaskExpirationScriptResult:
        result: tuple[int, int, int, list[tuple[str, str]]] = await super().__call__(  # type: ignore
            keys=[
                task_data_key_template,
                queue_completions_key,
                queue_failed_key,
                queue_cancelled_key,
            ],
            args=[
                completed_cutoff_timestamp,
                failed_cutoff_timestamp,
                cancelled_cutoff_timestamp,
                max_cleanup_batch,
            ],
        )

        # Ensure task details are strings, handling potential bytes objects
        cleaned_task_details = [
            (
                (
                    queue_type.decode("utf-8")
                    if isinstance(queue_type, bytes)
                    else str(queue_type)
                ),
                task_id.decode("utf-8") if isinstance(task_id, bytes) else str(task_id),
            )
            for queue_type, task_id in result[3]
        ]

        return TaskExpirationScriptResult(
            completed_cleaned=result[0],
            failed_cleaned=result[1],
            cancelled_cleaned=result[2],
            cleaned_task_details=cleaned_task_details,
        )


def create_task_expiration_script(
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
    * KEYS[1] = pending_tool_results_key (str)
    * KEYS[2] = main_queue (str)
    * KEYS[3] = tasks_key (str)

    Args:
    * ARGV[1] = task_id (str)
    * ARGV[2] = updated_task_context_json (str)
    """

    async def execute(
        self,
        *,
        pending_tool_results_key: str,
        queue_main_key: str,
        tasks_data_key: str,
        task_id: str,
        updated_task_context_json: str,
    ) -> bool:
        return await super().__call__(  # type: ignore
            keys=[pending_tool_results_key, queue_main_key, tasks_data_key],
            args=[task_id, updated_task_context_json],
        )


def create_tool_completion_script(redis_client: redis.Redis) -> ToolCompletionScript:
    """
    Creates a simple atomic script to move completed task back to queue and cleanup
    """
    return get_cached_script(redis_client, "tool_completion", ToolCompletionScript)


class EnqueueTaskScript(AsyncScript):
    """
    Creates a script for enqueuing a task

    Keys:
    * KEYS[1] = queue_main_key (str)
    * KEYS[2] = tasks_data_key (str)

    Args:
    * ARGV[1] = task_id (str)
    * ARGV[2] = task_data (str)
    """

    async def execute(
        self,
        *,
        queue_main_key: str,
        tasks_data_key: str,
        task_id: str,
        task_data: str,
    ) -> bool:
        return await super().__call__(  # type: ignore
            keys=[queue_main_key, tasks_data_key],
            args=[task_id, task_data],
        )


def create_enqueue_task_script(redis_client: redis.Redis) -> EnqueueTaskScript:
    """
    Creates a script for enqueuing a task
    """
    return get_cached_script(redis_client, "enqueue", EnqueueTaskScript)


class HeartbeatScript(AsyncScript):
    """
    Simple atomic script to update a task's heartbeat
    """

    async def execute(
        self,
        *,
        agent_heartbeats_key: str,
        task_id: str,
    ) -> float:
        return await super().__call__(  # type: ignore
            keys=[agent_heartbeats_key],
            args=[task_id],
        )


def create_heartbeat_script(redis_client: redis.Redis) -> HeartbeatScript:
    """
    Creates a script for updating a task's heartbeat
    """
    return get_cached_script(redis_client, "heartbeat", HeartbeatScript)


@dataclass
class MetricsAggregationResult:
    """Result of the metrics aggregation script"""

    system_totals: dict[str, int]
    activity_timeline: dict[str, dict[str, Any]]
    agent_metrics: dict[str, dict[str, Any]]
    agent_activity_timelines: dict[str, dict[str, dict[str, Any]]]


class MetricsAggregationScript(AsyncScript):
    """
    Efficiently collect all timeline metrics in a single atomic operation

    Keys:
    * KEYS[1] = timeline_duration (seconds)
    * KEYS[2] = bucket_duration (seconds)
    * KEYS[3] = agent_names_json (JSON array of agent names)
    """

    async def execute(
        self, *, timeline_duration: int, bucket_duration: int, agent_names: list[str]
    ) -> MetricsAggregationResult:
        agent_names_json = json.dumps(agent_names)

        result_json: str = await super().__call__(  # type: ignore
            keys=[str(timeline_duration), str(bucket_duration), agent_names_json],
            args=[],
        )

        result_data = json.loads(result_json)

        return MetricsAggregationResult(
            system_totals=result_data["system_totals"],
            activity_timeline=result_data["activity_timeline"],
            agent_metrics=result_data["agent_metrics"],
            agent_activity_timelines=result_data["agent_activity_timelines"],
        )


def create_metrics_aggregation_script(
    redis_client: redis.Redis,
) -> MetricsAggregationScript:
    """
    Creates an atomic script for efficient metrics collection
    """
    return get_cached_script(redis_client, "metrics", MetricsAggregationScript)


class BackoffRecoveryScript(AsyncScript):
    """
    Handles recovering tasks from backoff queue

    Keys:
    * KEYS[1] = queue_backoff_key (str)
    * KEYS[2] = queue_main_key (str)
    * KEYS[3] = task_data_key_template (str)

    Args:
    * ARGV[1] = max_batch_size (int)
    """

    async def execute(
        self,
        *,
        queue_backoff_key: str,
        queue_main_key: str,
        task_data_key_template: str,
        max_batch_size: int,
    ) -> list[str]:
        res: list[str | bytes] = await super().__call__(  # type: ignore
            keys=[queue_backoff_key, queue_main_key, task_data_key_template],
            args=[max_batch_size],
        )
        return [decode(id) for id in res]


def create_backoff_recovery_script(redis_client: redis.Redis) -> BackoffRecoveryScript:
    """
    Creates an atomic script for recovering tasks from backoff queue
    """
    return get_cached_script(redis_client, "backoff", BackoffRecoveryScript)
