from dataclasses import dataclass
from pathlib import Path
import json
import redis.asyncio as redis
from redis.commands.core import AsyncScript
from typing import Type, TypeVar

T = TypeVar("T", bound=AsyncScript)

# Cache for loaded scripts
_script_cache: dict[tuple[int, str], AsyncScript] = {}


def get_cached_script(redis_client, script_name, script_class):
    key = (id(redis_client), script_name)
    if key not in _script_cache:
        _script_cache[key] = create_script(redis_client, script_name, script_class)
    return _script_cache[key]


def get_script_path(script_name: str) -> Path:
    """Get the path to a Lua script file"""
    script_dir = Path(__file__).parent
    return script_dir / f"{script_name}.lua"


def load_script(script_name: str) -> str:
    """Load a Lua script from file with caching"""
    if script_name in _script_cache:
        return _script_cache[script_name]

    script_path = get_script_path(script_name)
    if not script_path.exists():
        raise FileNotFoundError(f"Lua script not found: {script_path}")

    with open(script_path, "r") as f:
        script_content = f.read()

    _script_cache[script_name] = script_content
    return script_content


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
class BatchMoveScriptResult:
    """Result of the batch move script"""

    tasks_to_process_ids: list[str]
    tasks_to_cancel_ids: list[str]


class BatchMoveScript(AsyncScript):
    """
    Handles moving tasks from main queue to processing

    Keys:
    * KEYS[1] = task_data_key_template (str)
    * KEYS[2] = queue_main_key (str)
    * KEYS[3] = queue_cancelled_key (str)
    * KEYS[4] = processing_heartbeats_key (str)
    * KEYS[5] = task_cancellations_key (str)

    Args:
    * ARGV[1] = batch_size (int)
    """

    async def execute(
        self,
        *,
        task_data_key_template: str,
        queue_main_key: str,
        queue_cancelled_key: str,
        processing_heartbeats_key: str,
        task_cancellations_key: str,
        batch_size: int,
    ) -> BatchMoveScriptResult:
        result: tuple[list[str], list[str]] = await super().__call__(  # type: ignore
            keys=[
                task_data_key_template,
                queue_main_key,
                queue_cancelled_key,
                processing_heartbeats_key,
                task_cancellations_key,
            ],
            args=[batch_size],
        )

        # Ensure task IDs are strings, handling potential bytes objects
        tasks_to_process_ids = [
            id.decode("utf-8") if isinstance(id, bytes) else str(id) for id in result[0]
        ]
        tasks_to_cancel_ids = [
            id.decode("utf-8") if isinstance(id, bytes) else str(id) for id in result[1]
        ]

        return BatchMoveScriptResult(
            tasks_to_process_ids=tasks_to_process_ids,
            tasks_to_cancel_ids=tasks_to_cancel_ids,
        )


# Convenience functions for each script
def create_batch_move_script(redis_client: redis.Redis) -> BatchMoveScript:
    """
    Create atomic script for moving tasks from main queue to processing
    """
    return get_cached_script(redis_client, "batch_move", BatchMoveScript)


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

    Args:
    * ARGV[1] = task_id (str)
    * ARGV[2] = action (str)
    * ARGV[3] = updated_task_payload_json (str)
    """

    async def execute(
        self,
        *,
        tasks_data_key: str,
        processing_heartbeats_key: str,
        queue_main_key: str,
        queue_completions_key: str,
        queue_failed_key: str,
        task_id: str,
        action: str,
        updated_task_payload_json: str,
    ) -> bool:
        return await super().__call__(  # type: ignore
            keys=[
                tasks_data_key,
                processing_heartbeats_key,
                queue_main_key,
                queue_completions_key,
                queue_failed_key,
            ],
            args=[task_id, action, updated_task_payload_json],
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

    Args:
    * ARGV[1] = cutoff_timestamp (int)
    * ARGV[2] = max_recovery_batch (int)
    * ARGV[3] = max_retries (int)
    """

    async def execute(
        self,
        *,
        task_data_key_template: str,
        queue_main_key: str,
        processing_heartbeats_key: str,
        queue_failed_key: str,
        cutoff_timestamp: float,
        max_recovery_batch: int,
        max_retries: int,
    ) -> StaleRecoveryScriptResult:
        result: tuple[int, int, list[tuple[str, str]]] = await super().__call__(  # type: ignore
            keys=[
                task_data_key_template,
                queue_main_key,
                processing_heartbeats_key,
                queue_failed_key,
            ],
            args=[cutoff_timestamp, max_recovery_batch, max_retries],
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
    return get_cached_script(redis_client, "stale_recovery", StaleRecoveryScript)


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
