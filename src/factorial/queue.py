import random
from typing import Any, cast
import uuid
import redis.asyncio as redis
import secrets
import time
import json
import asyncio
from dataclasses import replace

from factorial.task import Task, TaskStatus, ContextType
from factorial.agent import BaseAgent, ExecutionContext
from factorial.events import QueueEvent, AgentEvent, EventPublisher
from factorial.logging import get_logger, colored
from factorial.exceptions import RETRYABLE_EXCEPTIONS
from factorial.lua import (
    BatchPickupScript,
    TaskSteeringScript,
    TaskCompletionScript,
    StaleRecoveryScript,
    TaskExpirationScript,
    create_batch_pickup_script,
    create_task_steering_script,
    create_task_completion_script,
    create_stale_recovery_script,
    create_task_expiration_script,
    create_tool_completion_script,
    create_enqueue_task_script,
    create_backoff_recovery_script,
    create_cancel_task_script,
    BatchPickupScriptResult,
    StaleRecoveryScriptResult,
    TaskExpirationScriptResult,
    CancelTaskScriptResult,
)
from factorial.utils import decode


logger = get_logger("factorial.queue")

# ***** REDIS KEY SPACE *****

# HASH: task_id -> full task data (status, agent, payload, metadata)
TASKS_DATA = "{namespace}:tasks:data:{task_id}"
# HASH: task_id -> status
TASK_STATUS = (
    "{namespace}:tasks:status"  # queued | processing | completed | failed | cancelled
)
# HASH: task_id -> agent
TASK_AGENT = "{namespace}:tasks:agent"
# HASH: task_id -> agent payload
TASK_PAYLOAD = "{namespace}:tasks:payload"
# HASH: task_id -> num pickups (int)
TASK_PICKUPS = "{namespace}:tasks:pickups"
# HASH: task_id -> num retries (int)
TASK_RETRIES = "{namespace}:tasks:retries"
# HASH: task_id -> {created_at, owner_id}
TASK_META = "{namespace}:tasks:meta"

# ===== QUEUE MANAGEMENT =====
QUEUE_BASE = "{namespace}:queue"
# LIST: task_ids waiting to be processed
QUEUE_MAIN = "{namespace}:queue:{agent}:main"
# ZSET: task_id -> timestamp of failure
QUEUE_FAILED = "{namespace}:queue:{agent}:failed"
# ZSET: task_id -> timestamp of completion
QUEUE_COMPLETIONS = "{namespace}:queue:{agent}:completed"
# ZSET: task_id -> timestamp of cancellation
QUEUE_CANCELLED = "{namespace}:queue:{agent}:cancelled"
# ZSET: task_id -> timestamp of when task is ready to be picked up again
QUEUE_BACKOFF = "{namespace}:queue:{agent}:backoff"
# ZSET: task_id -> timestamp of orphaned task
QUEUE_ORPHANED = "{namespace}:queue:{agent}:orphaned"

# ===== ACTIVE TASK PROCESSING =====
# ZSET: task_id -> timestamp of last heartbeat
PROCESSING_HEARTBEATS = "{namespace}:processing:{agent}:heartbeats"

# ===== STEERING & CONTROL =====
# SET: task_ids marked for cancellation
TASK_CANCELLATIONS = "{namespace}:cancel:pending"
# HASH: message_id -> steering_message_json
TASK_STEERING = "{namespace}:steer:{task_id}:messages"

# ===== PENDING TOOL EXECUTION =====
# HASH: tool_call_id -> result_json or <|PENDING|>
PENDING_TOOL_RESULTS = "{namespace}:tool:{agent}:{task_id}:results"

# ===== COMMUNICATION =====
# PUBSUB: real-time updates to task owners
UPDATES_CHANNEL = "{namespace}:updates:{owner_id}"

# ===== METRICS =====
# HASH: {completed, failed, ..., completed_duration, failed_duration, ...} (bucket is a timestamp of a time bucket)
# completed, failed, cancelled, retried
AGENT_ACTIVITY_METRICS = "{namespace}:metrics:{agent}:{bucket}"
GLOBAL_ACTIVITY_METRICS = "{namespace}:metrics:__all__:{bucket}"

# COUNTER: number of idle workers for this agent
GAUGE_IDLE = "{namespace}:metrics:gauge:{agent}:idle"
GAUGE_IDLE_ALL = "{namespace}:metrics:gauge:__all__:idle"


# ===== SENTINEL VALUES =====
PENDING_SENTINEL = "<|PENDING|>"


"""
Workflow:

1. Enqueue a task:
    * (atomic): 
    Store task data in TASKS:task_id, set status to "queued", retries to 0, etc.
    Push the task_id to AGENT_QUEUE

2. Worker picks up a batch of tasks from the main queue
    * (atomic):
    Pop up to BATCH_SIZE task ids from AGENT_QUEUE
    For each task ID:
        * If task ID is in PENDING_CANCELLATIONS set:
            Remove from PENDING_CANCELLATIONS
            Set status = "cancelled" in TASKS:{task_id}
            Add to CANCELLED sorted set with score = current timestamp
        * Otherwise:
            Add task_id to PROCESSING ZSET with score = current timestamp
            Set status = "processing" in TASKS:{task_id}

3. Same worker processes tasks concurrently in a batch
    Start a heartbeat loop: every HEARTBEAT_INTERVAL, update timestamp in PROCESSING.
    Before executing a task:
        * Check for STEERING:{task_id} hash:
            If steering messages exist, pass them to agent.steer() to update task payload.
            Apply updated payload to TASKS:{task_id} and clear used messages from STEERING.
    Execute the task with agent.execute()

4. Once tasks are completed:
    * (atomic):
    Remove task_id from PROCESSING.
    Normal completion (not dependent on queued tool results):
        Update TASKS:{task_id} with:
            * status = "completed" if successful
            * status = "failed" if max retries exceeded
            * status = "active" otherwise (for continuation or retry)
        Depending on result:
            * Add task id to COMPLETIONS with score = current timestamp (successfully completed)
            * Add task id to FAILED with score = current timestamp (max retries exceeded)
            * Push back to front of AGENT_QUEUE (continue or retry)
    Task dependent on queued tool results:
        * Add task id to PENDING_TOOL_RESULTS with set of tool call ids that need to be awaited

5. (Optional) Await tool results:
    Seperate consumer or webhook listens for tool result completions
    complete_deferred_tool(tool_call_id, result)
    Update set the result in the PENDING_TOOL_RESULTS key
    If all tool calls have completed, update the task context with the completed results and move task back to queue

Recovery Mechanism:
1. Detect stale tasks and move them back to the front of the queue to be retried
    * (atomic):
    Get tasks in PROCESSING sorted set that are older than X seconds (heartbeat has not been updated for a while)
    For each task:
        * Increment retries field in TASKS:{task_id}.
        * If retries < MAX_RETRIES:
            Set status = "queued" and push back to front of AGENT_QUEUE
        * Else:
            Set status = "failed" and push to FAILED queue
        * Remove the task ID from PROCESSING
"""


class TaskNotFoundError(Exception):
    """Exception raised when a task is not found"""

    def __init__(self, task_id: str):
        self.task_id = task_id
        super().__init__(f"Task {task_id} not found")


class InvalidTaskIdError(Exception):
    """Exception raised when a task ID is invalid"""

    def __init__(self, task_id: str):
        self.task_id = task_id
        super().__init__(f"Invalid task ID format: {task_id}")


class InactiveTaskError(Exception):
    """Exception raised when trying to control a task that is not active"""

    def __init__(self, task_id: str):
        self.task_id = task_id
        super().__init__(f"Task {task_id} is not active")


class CorruptedTaskDataError(Exception):
    """Exception raised when task data is corrupted"""

    def __init__(self, task_id: str, missing_fields: list[str]):
        self.task_id = task_id
        super().__init__(
            f"Task {task_id} data is corrupted, missing fields: {missing_fields}"
        )


def is_valid_task_id(task_id: str) -> bool:
    try:
        uuid.UUID(task_id)
    except ValueError:
        return False
    return True


async def get_task_data(
    redis_client: redis.Redis, namespace: str, task_id: str
) -> dict[str, Any]:
    pipe = redis_client.pipeline(transaction=True)
    pipe.multi()
    pipe.hget(TASK_STATUS.format(namespace=namespace), task_id)
    pipe.hget(TASK_AGENT.format(namespace=namespace), task_id)
    pipe.hget(TASK_PAYLOAD.format(namespace=namespace), task_id)
    pipe.hget(TASK_PICKUPS.format(namespace=namespace), task_id)
    pipe.hget(TASK_RETRIES.format(namespace=namespace), task_id)
    pipe.hget(TASK_META.format(namespace=namespace), task_id)

    status, agent, payload_json, pickups, retries, meta_json = await pipe.execute()

    if not status and not agent and not payload_json and not meta_json:
        raise TaskNotFoundError(task_id)
    elif not all([status, agent, payload_json, pickups, retries, meta_json]):
        fields = {
            "status": status,
            "agent": agent,
            "payload": payload_json,
            "pickups": pickups,
            "retries": retries,
            "metadata": meta_json,
        }
        missing_fields = [field for field, value in fields.items() if not value]
        raise CorruptedTaskDataError(task_id, missing_fields)

    task_data: dict[str, Any] = {
        "id": task_id,
        "status": decode(status),
        "agent": decode(agent),
        "payload": json.loads(decode(payload_json)),
        "pickups": int(decode(pickups)),
        "retries": int(decode(retries)),
        "metadata": json.loads(decode(meta_json)),
    }

    return task_data


async def get_task_status(
    redis_client: redis.Redis, namespace: str, task_id: str
) -> TaskStatus:
    if not is_valid_task_id(task_id):
        raise InvalidTaskIdError(task_id)

    status: str | bytes = await redis_client.hget(
        TASK_STATUS.format(namespace=namespace), task_id
    )  # type: ignore
    if not status:
        raise TaskNotFoundError(task_id)

    return TaskStatus(decode(status))


async def get_task_agent(
    redis_client: redis.Redis, namespace: str, task_id: str
) -> str:
    if not is_valid_task_id(task_id):
        raise InvalidTaskIdError(task_id)

    agent: str | bytes = await redis_client.hget(
        TASK_AGENT.format(namespace=namespace), task_id
    )  # type: ignore
    if not agent:
        raise TaskNotFoundError(task_id)

    return decode(agent)


async def enqueue_task(
    redis_client: redis.Redis,
    namespace: str,
    agent: BaseAgent[Any],
    task: Task[ContextType],
) -> None:
    queue_main_key = QUEUE_MAIN.format(namespace=namespace, agent=agent.name)
    task_statuses_key = TASK_STATUS.format(namespace=namespace)
    task_agents_key = TASK_AGENT.format(namespace=namespace)
    task_payloads_key = TASK_PAYLOAD.format(namespace=namespace)
    task_metas_key = TASK_META.format(namespace=namespace)
    task_pickups_key = TASK_PICKUPS.format(namespace=namespace)
    task_retries_key = TASK_RETRIES.format(namespace=namespace)

    if not is_valid_task_id(task.id):
        raise InvalidTaskIdError(task.id)

    enqueue_script = await create_enqueue_task_script(redis_client)
    await enqueue_script.execute(
        agent_queue_key=queue_main_key,
        task_statuses_key=task_statuses_key,
        task_agents_key=task_agents_key,
        task_payloads_key=task_payloads_key,
        task_pickups_key=task_pickups_key,
        task_retries_key=task_retries_key,
        task_metas_key=task_metas_key,
        task_id=task.id,
        task_agent=agent.name,
        task_payload_json=task.payload.to_json(),
        task_pickups=0,
        task_retries=0,
        task_meta_json=task.metadata.to_json(),
    )


async def cancel_task(
    redis_client: redis.Redis,
    namespace: str,
    task_id: str,
    metrics_bucket_duration: int = 300,
    metrics_retention_duration: int = 86400,
) -> None:
    """Cancel a task using unified cancellation script"""
    agent_name = await get_task_agent(redis_client, namespace, task_id)

    bucket_id = int(time.time() / metrics_bucket_duration) * metrics_bucket_duration
    agent_metrics_bucket_key = AGENT_ACTIVITY_METRICS.format(
        namespace=namespace, agent=agent_name, bucket=bucket_id
    )
    global_metrics_bucket_key = GLOBAL_ACTIVITY_METRICS.format(
        namespace=namespace, bucket=bucket_id
    )
    queue_cancelled_key = QUEUE_CANCELLED.format(namespace=namespace, agent=agent_name)
    queue_backoff_key = QUEUE_BACKOFF.format(namespace=namespace, agent=agent_name)
    queue_orphaned_key = QUEUE_ORPHANED.format(namespace=namespace, agent=agent_name)
    pending_cancellations_key = TASK_CANCELLATIONS.format(namespace=namespace)
    task_statuses_key = TASK_STATUS.format(namespace=namespace)
    task_agents_key = TASK_AGENT.format(namespace=namespace)
    task_payloads_key = TASK_PAYLOAD.format(namespace=namespace)
    task_pickups_key = TASK_PICKUPS.format(namespace=namespace)
    task_retries_key = TASK_RETRIES.format(namespace=namespace)
    task_metas_key = TASK_META.format(namespace=namespace)
    pending_tool_results_key = PENDING_TOOL_RESULTS.format(
        namespace=namespace, agent=agent_name, task_id=task_id
    )

    cancel_script = await create_cancel_task_script(redis_client)
    result: CancelTaskScriptResult = await cancel_script.execute(
        queue_cancelled_key=queue_cancelled_key,
        queue_backoff_key=queue_backoff_key,
        queue_orphaned_key=queue_orphaned_key,
        pending_cancellations_key=pending_cancellations_key,
        task_statuses_key=task_statuses_key,
        task_agents_key=task_agents_key,
        task_payloads_key=task_payloads_key,
        task_pickups_key=task_pickups_key,
        task_retries_key=task_retries_key,
        task_metas_key=task_metas_key,
        pending_tool_results_key=pending_tool_results_key,
        agent_metrics_bucket_key=agent_metrics_bucket_key,
        global_metrics_bucket_key=global_metrics_bucket_key,
        task_id=task_id,
        metrics_ttl=metrics_retention_duration,
    )

    if not result.success:
        if not result.current_status:
            raise TaskNotFoundError(task_id)
        elif result.current_status in [
            TaskStatus.COMPLETED,
            TaskStatus.FAILED,
            TaskStatus.CANCELLED,
        ]:
            raise InactiveTaskError(task_id)
        else:
            raise Exception(result.message)


async def steer_task(
    redis_client: redis.Redis,
    namespace: str,
    task_id: str,
    messages: list[dict[str, Any]],
) -> None:
    """Steer a task"""
    task_status = await get_task_status(redis_client, namespace, task_id)
    if task_status in [TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED]:
        raise InactiveTaskError(task_id)

    task_steering_key = TASK_STEERING.format(namespace=namespace, task_id=task_id)
    # message id format: {timestamp_ms}_{random_hex} e.g. 1717234200000_a3f4b5c6
    message_mapping = {
        f"{int(time.time() * 1000)}_{secrets.token_hex(3)}": json.dumps(message)
        for message in messages
    }
    await redis_client.hset(task_steering_key, mapping=message_mapping)  # type: ignore


async def get_steering_messages(
    redis_client: redis.Redis,
    namespace: str,
    task_id: str,
) -> list[tuple[str, dict[str, Any]]]:
    """Get steering messages for a task"""
    await get_task_status(
        redis_client, namespace, task_id
    )  # Raise if task does not exist

    steering_key = TASK_STEERING.format(namespace=namespace, task_id=task_id)
    message_data: list[tuple[str, dict[str, Any]]] = []
    steering_messages = cast(
        dict[str | bytes, str | bytes],
        await redis_client.hgetall(steering_key),  # type: ignore
    )
    if not steering_messages:
        return []

    for message_id, message in steering_messages.items():
        message_id_str = decode(message_id)
        message_str = decode(message)
        message_data.append((message_id_str, json.loads(message_str)))

    return message_data


async def complete_deferred_tool(
    redis_client: redis.Redis,
    namespace: str,
    agent: BaseAgent[Any],
    task_id: str,
    tool_call_id: str,
    result: Any,
) -> bool:
    """Complete a deferred tool call"""
    task_data = await get_task_data(redis_client, namespace, task_id)
    task_status = TaskStatus(task_data["status"])
    if task_status in [TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED]:
        raise InactiveTaskError(task_id)

    # Check if task is marked for cancellation
    is_cancelled = await redis_client.sismember(
        TASK_CANCELLATIONS.format(namespace=namespace), task_id
    )  # type: ignore
    if is_cancelled:
        # Cancel the task instead of completing the tool call
        await cancel_task(redis_client, namespace, task_id)
        return False

    tool_results_key = PENDING_TOOL_RESULTS.format(
        namespace=namespace, agent=agent.name, task_id=task_id
    )
    agent_queue_key = QUEUE_MAIN.format(namespace=namespace, agent=agent.name)

    task_dict = await get_task_data(redis_client, namespace, task_id)
    if not task_dict:
        logger.error(f"Failed to process task {task_id}: Task data not found")
        return False

    try:
        task = Task.from_dict(task_dict, context_class=agent.context_class)
    except Exception as e:
        logger.error(
            f"Failed to process task {task_id}: Task data is invalid", exc_info=e
        )
        return False

    # Set the result
    await redis_client.hset(tool_results_key, tool_call_id, json.dumps(result))  # type: ignore

    # Get all results and check if any are still pending
    all_results = cast(
        dict[str | bytes, str | bytes],
        await redis_client.hgetall(tool_results_key),  # type: ignore
    )
    if not all_results:
        return False

    # Check if any results are still pending (sentinel value)
    completed_results: list[tuple[str, Any]] = []

    for tcid, result_json in all_results.items():
        tcid_str = decode(tcid)
        result_str = decode(result_json)

        if result_str == PENDING_SENTINEL:
            return False
        else:
            completed_results.append((tcid_str, json.loads(result_str)))

    # Update the task context with the completed results
    updated_context = agent.process_long_running_tool_results(
        task.payload, completed_results
    ).context

    # Move task back to queue
    tool_completion_script = await create_tool_completion_script(redis_client)
    success = await tool_completion_script.execute(
        queue_main_key=agent_queue_key,
        queue_orphaned_key=QUEUE_ORPHANED.format(namespace=namespace, agent=agent.name),
        pending_tool_results_key=tool_results_key,
        task_statuses_key=TASK_STATUS.format(namespace=namespace),
        task_agents_key=TASK_AGENT.format(namespace=namespace),
        task_payloads_key=TASK_PAYLOAD.format(namespace=namespace),
        task_pickups_key=TASK_PICKUPS.format(namespace=namespace),
        task_retries_key=TASK_RETRIES.format(namespace=namespace),
        task_metas_key=TASK_META.format(namespace=namespace),
        task_id=task.id,
        updated_task_context_json=updated_context.to_json(),
    )

    if success:
        return True
    else:
        return False


async def publish_update(
    redis_client: redis.Redis, namespace: str, owner_id: str, payload: dict[str, Any]
) -> None:
    user_update_channel = UPDATES_CHANNEL.format(namespace=namespace, owner_id=owner_id)
    await redis_client.publish(user_update_channel, json.dumps(payload))  # type: ignore


async def get_task_batch(
    batch_script: BatchPickupScript,
    namespace: str,
    agent: BaseAgent[Any],
    batch_size: int,
    metrics_bucket_duration: int,
    metrics_ttl: int,
) -> tuple[list[str], list[str]]:
    """
    Get batch of tasks atomically

    Returns:
        * List of task ids to process
        * List of task ids to cancel
    """
    timestamp = time.time()
    bucket_id = int(timestamp / metrics_bucket_duration) * metrics_bucket_duration

    queue_main_key = QUEUE_MAIN.format(namespace=namespace, agent=agent.name)
    queue_cancelled_key = QUEUE_CANCELLED.format(namespace=namespace, agent=agent.name)
    queue_orphaned_key = QUEUE_ORPHANED.format(namespace=namespace, agent=agent.name)
    processing_heartbeats_key = PROCESSING_HEARTBEATS.format(
        namespace=namespace, agent=agent.name
    )
    task_cancellations_key = TASK_CANCELLATIONS.format(namespace=namespace)
    task_statuses_key = TASK_STATUS.format(namespace=namespace)
    task_agents_key = TASK_AGENT.format(namespace=namespace)
    task_payloads_key = TASK_PAYLOAD.format(namespace=namespace)
    task_pickups_key = TASK_PICKUPS.format(namespace=namespace)
    task_retries_key = TASK_RETRIES.format(namespace=namespace)
    task_metas_key = TASK_META.format(namespace=namespace)
    agent_metrics_bucket_key = AGENT_ACTIVITY_METRICS.format(
        namespace=namespace, agent=agent.name, bucket=bucket_id
    )
    global_metrics_bucket_key = GLOBAL_ACTIVITY_METRICS.format(
        namespace=namespace, bucket=bucket_id
    )

    try:
        result: BatchPickupScriptResult = await batch_script.execute(
            queue_main_key=queue_main_key,
            queue_cancelled_key=queue_cancelled_key,
            queue_orphaned_key=queue_orphaned_key,
            task_statuses_key=task_statuses_key,
            task_agents_key=task_agents_key,
            task_payloads_key=task_payloads_key,
            task_pickups_key=task_pickups_key,
            task_retries_key=task_retries_key,
            task_metas_key=task_metas_key,
            task_cancellations_key=task_cancellations_key,
            processing_heartbeats_key=processing_heartbeats_key,
            agent_metrics_bucket_key=agent_metrics_bucket_key,
            global_metrics_bucket_key=global_metrics_bucket_key,
            batch_size=batch_size,
            metrics_ttl=metrics_ttl,
        )
        if result.orphaned_task_ids:
            logger.warning(
                f"⚠️ Found {len(result.orphaned_task_ids)} orphaned tasks: {result.orphaned_task_ids}"
            )
        if result.corrupted_task_ids:
            logger.warning(
                f"⚠️ Found {len(result.corrupted_task_ids)} corrupted tasks: {result.corrupted_task_ids}"
            )

        return result.tasks_to_process_ids, result.tasks_to_cancel_ids

    except Exception as e:
        logger.error(f"Failed to get task batch: {e}")
        await asyncio.sleep(0.15 + random.random() * 0.1)  # backoff with jitter
        return [], []


async def heartbeat_loop(
    redis_client: redis.Redis,
    namespace: str,
    task_id: str,
    agent: BaseAgent[Any],
    stop_event: asyncio.Event,
    interval: int,
) -> None:
    """Simple heartbeat loop that runs until stopped"""
    agent_heartbeats = PROCESSING_HEARTBEATS.format(
        namespace=namespace, agent=agent.name
    )

    try:
        while not stop_event.is_set():
            try:
                await redis_client.zadd(agent_heartbeats, {task_id: time.time()})  # type: ignore

                # Wait for next interval
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=interval)
                    break  # Stop event was set
                except asyncio.TimeoutError:
                    continue  # Normal timeout, send next heartbeat

            except Exception as e:
                # Log but don't crash - missing one heartbeat shouldn't kill the task
                logger.error(f"Heartbeat error for {task_id}: {e}")
                # Small backoff to avoid hammering Redis if it's having issues
                await asyncio.sleep(0.25 + random.random() * 0.5)
    except asyncio.CancelledError:
        # Task was cancelled, exit cleanly
        logger.debug(f"Heartbeat loop for {task_id} cancelled")
        raise


async def handle_cancelled_tasks(
    redis_client: redis.Redis,
    namespace: str,
    cancelled_task_ids: list[str],
    agent: BaseAgent[Any],
) -> None:
    """Send cancellation events for cancelled tasks concurrently"""

    async def cancel_task(task_id: str) -> None:
        task_data = await get_task_data(redis_client, namespace, task_id)
        if not task_data:
            logger.error(
                f"Failed to complete task cancellation for {task_id}: Task data not found"
            )
            return

        try:
            task = Task.from_dict(task_data, context_class=agent.context_class)
        except Exception as e:
            logger.error(
                f"Failed to process task {task_id}: Task data is invalid", exc_info=e
            )
            return

        event_publisher = EventPublisher(
            redis_client=redis_client,
            channel=UPDATES_CHANNEL.format(
                namespace=namespace, owner_id=task.metadata.owner_id
            ),
        )

        try:
            execution_ctx = ExecutionContext(
                task_id=task.id,
                owner_id=task.metadata.owner_id,
                retries=task.retries,
                iterations=task.payload.turn,
                events=event_publisher,
            )
            await agent.cancel(task.payload, execution_ctx)
            logger.info(f"🚫 Task cancelled {colored(f'[{task.id}]', 'dim')}")

            await event_publisher.publish_event(
                AgentEvent(
                    event_type="run_cancelled",
                    task_id=task.id,
                    owner_id=task.metadata.owner_id,
                    agent_name=agent.name,
                )
            )
        except Exception as e:
            logger.error(f"Error sending cancellation event for {task.id}", exc_info=e)

    tasks = [cancel_task(task_id) for task_id in cancelled_task_ids]
    await asyncio.gather(*tasks)


async def process_task(
    redis_client: redis.Redis,
    namespace: str,
    task_id: str,
    completion_script: TaskCompletionScript,
    steering_script: TaskSteeringScript,
    agent: BaseAgent[ContextType],
    max_retries: int,
    heartbeat_interval: int,
    task_timeout: int,
    metrics_bucket_duration: int,
    metrics_retention_duration: int,
) -> None:
    """Process a single task"""
    # logger.info(f"▶️  Task started   {colored(f'[{task_id}]', 'dim')}")
    bucket_id = int(time.time() / metrics_bucket_duration) * metrics_bucket_duration

    queue_main_key = QUEUE_MAIN.format(namespace=namespace, agent=agent.name)
    queue_completions_key = QUEUE_COMPLETIONS.format(
        namespace=namespace, agent=agent.name
    )
    queue_backoff_key = QUEUE_BACKOFF.format(namespace=namespace, agent=agent.name)
    queue_failed_key = QUEUE_FAILED.format(namespace=namespace, agent=agent.name)
    queue_orphaned_key = QUEUE_ORPHANED.format(namespace=namespace, agent=agent.name)
    processing_heartbeats_key = PROCESSING_HEARTBEATS.format(
        namespace=namespace, agent=agent.name
    )
    task_steering_key = TASK_STEERING.format(namespace=namespace, task_id=task_id)
    pending_tool_results_key = PENDING_TOOL_RESULTS.format(
        namespace=namespace, agent=agent.name, task_id=task_id
    )
    agent_metrics_bucket_key = AGENT_ACTIVITY_METRICS.format(
        namespace=namespace, agent=agent.name, bucket=bucket_id
    )
    global_metrics_bucket_key = GLOBAL_ACTIVITY_METRICS.format(
        namespace=namespace, bucket=bucket_id
    )
    agent_idle_gauge_key = GAUGE_IDLE.format(namespace=namespace, agent=agent.name)
    global_idle_gauge_key = GAUGE_IDLE_ALL.format(namespace=namespace)

    task_data = await get_task_data(redis_client, namespace, task_id)
    if not task_data:
        logger.error(f"Failed to process task {task_id}: Task data not found")
        return

    try:
        task = Task.from_dict(task_data, context_class=agent.context_class)
    except Exception as e:
        logger.error(
            f"Failed to process task {task_id}: Task data is invalid", exc_info=e
        )
        return

    stop_heartbeat = asyncio.Event()
    heartbeat_task = asyncio.create_task(
        heartbeat_loop(
            redis_client=redis_client,
            namespace=namespace,
            task_id=task_id,
            agent=agent,
            stop_event=stop_heartbeat,
            interval=heartbeat_interval,
        )
    )

    event_publisher = EventPublisher(
        redis_client=redis_client,
        channel=UPDATES_CHANNEL.format(
            namespace=namespace, owner_id=task.metadata.owner_id
        ),
    )

    task_failed = False
    try:
        if task.payload.turn == 0 and task.retries == 0:
            await event_publisher.publish_event(
                AgentEvent(
                    event_type="run_started",
                    task_id=task.id,
                    owner_id=task.metadata.owner_id,
                    agent_name=agent.name,
                )
            )

        execution_ctx = ExecutionContext(
            task_id=task.id,
            owner_id=task.metadata.owner_id,
            retries=task.retries,
            iterations=task.payload.turn,
            events=event_publisher,
        )

        steering_messages_data = await get_steering_messages(
            redis_client=redis_client,
            namespace=namespace,
            task_id=task.id,
        )

        def extract_timestamp(message_tuple: tuple[str, dict[str, Any]]) -> int:
            try:
                message_id = message_tuple[0]
                return int(message_id.split("_")[0])
            except (ValueError, IndexError):
                return 0

        sorted_data = sorted(steering_messages_data, key=extract_timestamp)
        steering_messages = [message for _, message in sorted_data]
        steering_message_ids = [message_id for message_id, _ in sorted_data]

        if steering_messages:
            steered_task = replace(task)
            steered_task.payload = await agent.steer(
                messages=steering_messages,
                agent_ctx=task.payload,
                execution_ctx=execution_ctx,
            )

            try:
                await steering_script.execute(
                    queue_orphaned_key=queue_orphaned_key,
                    task_statuses_key=TASK_STATUS.format(namespace=namespace),
                    task_agents_key=TASK_AGENT.format(namespace=namespace),
                    task_payloads_key=TASK_PAYLOAD.format(namespace=namespace),
                    task_pickups_key=TASK_PICKUPS.format(namespace=namespace),
                    task_retries_key=TASK_RETRIES.format(namespace=namespace),
                    task_metas_key=TASK_META.format(namespace=namespace),
                    steering_messages_key=task_steering_key,
                    steering_message_ids=steering_message_ids,
                    updated_task_payload_json=steered_task.payload.to_json(),
                )

                task = steered_task
                await event_publisher.publish_event(
                    AgentEvent(
                        event_type="run_steering_applied",
                        task_id=task.id,
                        owner_id=task.metadata.owner_id,
                        agent_name=agent.name,
                    )
                )
            except Exception as e:
                logger.error(f"Error updating task with steering messages: {e}")
                await event_publisher.publish_event(
                    AgentEvent(
                        event_type="run_steering_failed",
                        task_id=task.id,
                        owner_id=task.metadata.owner_id,
                        agent_name=agent.name,
                        error=str(e),
                    )
                )

        turn_completion = await asyncio.wait_for(
            agent.execute(task.payload, execution_ctx),
            timeout=task_timeout,
        )

        if turn_completion.pending_tool_call_ids:
            logger.info(
                f"This should not be happening? {turn_completion.pending_tool_call_ids}"
            )
            res = await completion_script.execute(
                queue_main_key=queue_main_key,
                queue_completions_key=queue_completions_key,
                queue_failed_key=queue_failed_key,
                queue_backoff_key=queue_backoff_key,
                queue_orphaned_key=queue_orphaned_key,
                task_statuses_key=TASK_STATUS.format(namespace=namespace),
                task_agents_key=TASK_AGENT.format(namespace=namespace),
                task_payloads_key=TASK_PAYLOAD.format(namespace=namespace),
                task_pickups_key=TASK_PICKUPS.format(namespace=namespace),
                task_retries_key=TASK_RETRIES.format(namespace=namespace),
                task_metas_key=TASK_META.format(namespace=namespace),
                processing_heartbeats_key=processing_heartbeats_key,
                pending_tool_results_key=pending_tool_results_key,
                agent_metrics_bucket_key=agent_metrics_bucket_key,
                global_metrics_bucket_key=global_metrics_bucket_key,
                agent_idle_gauge_key=agent_idle_gauge_key,
                global_idle_gauge_key=global_idle_gauge_key,
                task_id=task.id,
                action="pending_tool_call_results",
                updated_task_payload_json=task.payload.to_json(),
                metrics_ttl=metrics_retention_duration,
                pending_sentinel=PENDING_SENTINEL,
                pending_tool_call_ids_json=json.dumps(
                    turn_completion.pending_tool_call_ids
                ),
            )

            logger.info(
                f"⏳ Task awaiting tool results {colored(f'[{task.id}]', 'dim')}"
            )
            await event_publisher.publish_event(
                AgentEvent(
                    event_type="task_pending_tool_call_results",
                    task_id=task.id,
                    owner_id=task.metadata.owner_id,
                    agent_name=agent.name,
                )
            )

        elif turn_completion.is_done:
            res = await completion_script.execute(
                processing_heartbeats_key=processing_heartbeats_key,
                queue_main_key=queue_main_key,
                queue_completions_key=queue_completions_key,
                queue_failed_key=queue_failed_key,
                queue_backoff_key=queue_backoff_key,
                queue_orphaned_key=queue_orphaned_key,
                task_statuses_key=TASK_STATUS.format(namespace=namespace),
                task_agents_key=TASK_AGENT.format(namespace=namespace),
                task_payloads_key=TASK_PAYLOAD.format(namespace=namespace),
                task_pickups_key=TASK_PICKUPS.format(namespace=namespace),
                task_retries_key=TASK_RETRIES.format(namespace=namespace),
                task_metas_key=TASK_META.format(namespace=namespace),
                pending_tool_results_key=pending_tool_results_key,
                agent_metrics_bucket_key=agent_metrics_bucket_key,
                global_metrics_bucket_key=global_metrics_bucket_key,
                agent_idle_gauge_key=agent_idle_gauge_key,
                global_idle_gauge_key=global_idle_gauge_key,
                task_id=task.id,
                action="complete",
                updated_task_payload_json=task.payload.to_json(),
                metrics_ttl=metrics_retention_duration,
            )
            # logger.info(f"✅ Task completed {colored(f'[{task.id}]', 'dim')}")
            await event_publisher.publish_event(
                AgentEvent(
                    event_type="run_completed",
                    task_id=task.id,
                    owner_id=task.metadata.owner_id,
                    agent_name=agent.name,
                )
            )

        else:
            # Task needs to continue processing
            task.payload = turn_completion.context
            # logger.info(f"⏩ Task continued {colored(f'[{task.id}]', 'dim')}")

            await completion_script.execute(
                processing_heartbeats_key=processing_heartbeats_key,
                queue_main_key=queue_main_key,
                queue_completions_key=queue_completions_key,
                queue_failed_key=queue_failed_key,
                queue_backoff_key=queue_backoff_key,
                queue_orphaned_key=queue_orphaned_key,
                task_statuses_key=TASK_STATUS.format(namespace=namespace),
                task_agents_key=TASK_AGENT.format(namespace=namespace),
                task_payloads_key=TASK_PAYLOAD.format(namespace=namespace),
                task_pickups_key=TASK_PICKUPS.format(namespace=namespace),
                task_retries_key=TASK_RETRIES.format(namespace=namespace),
                task_metas_key=TASK_META.format(namespace=namespace),
                pending_tool_results_key=pending_tool_results_key,
                agent_metrics_bucket_key=agent_metrics_bucket_key,
                global_metrics_bucket_key=global_metrics_bucket_key,
                agent_idle_gauge_key=agent_idle_gauge_key,
                global_idle_gauge_key=global_idle_gauge_key,
                task_id=task.id,
                action="continue",
                updated_task_payload_json=task.payload.to_json(),
                metrics_ttl=metrics_retention_duration,
            )

    except asyncio.TimeoutError:
        task_failed = True
        logger.error(
            f"❌ Task timed out after {task_timeout} seconds {colored(f'[{task.id}]', 'dim')}"
        )

        await event_publisher.publish_event(
            QueueEvent(
                event_type="task_failed",
                task_id=task.id,
                owner_id=task.metadata.owner_id,
                agent_name=agent.name,
                error=f"Task {task.id} timed out after {task_timeout} seconds",
            )
        )

        action = "fail" if task.retries >= max_retries else "retry"
        res = await completion_script.execute(
            processing_heartbeats_key=processing_heartbeats_key,
            queue_main_key=queue_main_key,
            queue_completions_key=queue_completions_key,
            queue_failed_key=queue_failed_key,
            queue_backoff_key=queue_backoff_key,
            queue_orphaned_key=queue_orphaned_key,
            task_statuses_key=TASK_STATUS.format(namespace=namespace),
            task_agents_key=TASK_AGENT.format(namespace=namespace),
            task_payloads_key=TASK_PAYLOAD.format(namespace=namespace),
            task_pickups_key=TASK_PICKUPS.format(namespace=namespace),
            task_retries_key=TASK_RETRIES.format(namespace=namespace),
            task_metas_key=TASK_META.format(namespace=namespace),
            pending_tool_results_key=pending_tool_results_key,
            agent_metrics_bucket_key=agent_metrics_bucket_key,
            global_metrics_bucket_key=global_metrics_bucket_key,
            agent_idle_gauge_key=agent_idle_gauge_key,
            global_idle_gauge_key=global_idle_gauge_key,
            task_id=task.id,
            action=action,
            updated_task_payload_json=task.payload.to_json(),
            metrics_ttl=metrics_retention_duration,
        )

    except Exception as e:
        task_failed = True
        logger.error(f"❌ Task failed {colored(f'[{task.id}]', 'dim')}", exc_info=e)

        try:
            await event_publisher.publish_event(
                QueueEvent(
                    event_type="task_failed",
                    task_id=task.id,
                    owner_id=task.metadata.owner_id,
                    agent_name=agent.name,
                    error=str(e),
                )
            )
        except Exception as err:
            logger.error(f"Failed to send task failed event: {err}")

        if isinstance(e, tuple(RETRYABLE_EXCEPTIONS)):
            action = "fail" if task.retries >= max_retries else "backoff"
        else:
            action = "fail" if task.retries >= max_retries else "retry"

        res = await completion_script.execute(
            processing_heartbeats_key=processing_heartbeats_key,
            queue_main_key=queue_main_key,
            queue_completions_key=queue_completions_key,
            queue_failed_key=queue_failed_key,
            queue_backoff_key=queue_backoff_key,
            queue_orphaned_key=queue_orphaned_key,
            task_statuses_key=TASK_STATUS.format(namespace=namespace),
            task_agents_key=TASK_AGENT.format(namespace=namespace),
            task_payloads_key=TASK_PAYLOAD.format(namespace=namespace),
            task_pickups_key=TASK_PICKUPS.format(namespace=namespace),
            task_retries_key=TASK_RETRIES.format(namespace=namespace),
            task_metas_key=TASK_META.format(namespace=namespace),
            pending_tool_results_key=pending_tool_results_key,
            agent_metrics_bucket_key=agent_metrics_bucket_key,
            global_metrics_bucket_key=global_metrics_bucket_key,
            agent_idle_gauge_key=agent_idle_gauge_key,
            global_idle_gauge_key=global_idle_gauge_key,
            task_id=task.id,
            action=action,
            updated_task_payload_json=task.payload.to_json(),
            metrics_ttl=metrics_retention_duration,
        )
        if not res:
            logger.error(f"Failed to update task status: {res}")
    finally:
        # Stop heartbeat
        stop_heartbeat.set()
        heartbeat_task.cancel()

        # Only emit retry/failure events if the task actually failed
        if task_failed:
            if task.retries >= max_retries:
                logger.error(
                    f"❌ Task failed permanently {colored(f'[{task.id}]', 'dim')}"
                )
                await event_publisher.publish_event(
                    AgentEvent(
                        event_type="run_failed",
                        task_id=task.id,
                        owner_id=task.metadata.owner_id,
                        agent_name=agent.name,
                        error=f"Agent {agent.name} failed to complete task {task.id} due to max retries ({max_retries})",
                    )
                )
            else:
                logger.info(
                    f"🔄 Task set back for retry {colored(f'[{task.id}]', 'dim')}"
                )
                await event_publisher.publish_event(
                    QueueEvent(
                        event_type="task_retried",
                        task_id=task.id,
                        owner_id=task.metadata.owner_id,
                        agent_name=agent.name,
                    )
                )

        try:
            await heartbeat_task
        except asyncio.CancelledError:
            pass


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
    bucket_id = int(time.time() / metrics_bucket_duration) * metrics_bucket_duration

    queue_main_key = QUEUE_MAIN.format(namespace=namespace, agent=agent.name)
    queue_orphaned_key = QUEUE_ORPHANED.format(namespace=namespace, agent=agent.name)
    queue_failed_key = QUEUE_FAILED.format(namespace=namespace, agent=agent.name)
    processing_heartbeats_key = PROCESSING_HEARTBEATS.format(
        namespace=namespace, agent=agent.name
    )
    agent_metrics_bucket_key = AGENT_ACTIVITY_METRICS.format(
        namespace=namespace, agent=agent.name, bucket=bucket_id
    )
    global_metrics_bucket_key = GLOBAL_ACTIVITY_METRICS.format(
        namespace=namespace, bucket=bucket_id
    )

    try:
        result: StaleRecoveryScriptResult = await recovery_script.execute(
            queue_main_key=queue_main_key,
            queue_failed_key=queue_failed_key,
            queue_orphaned_key=queue_orphaned_key,
            task_statuses_key=TASK_STATUS.format(namespace=namespace),
            task_agents_key=TASK_AGENT.format(namespace=namespace),
            task_payloads_key=TASK_PAYLOAD.format(namespace=namespace),
            task_pickups_key=TASK_PICKUPS.format(namespace=namespace),
            task_retries_key=TASK_RETRIES.format(namespace=namespace),
            task_metas_key=TASK_META.format(namespace=namespace),
            processing_heartbeats_key=processing_heartbeats_key,
            agent_metrics_bucket_key=agent_metrics_bucket_key,
            global_metrics_bucket_key=global_metrics_bucket_key,
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
    queue_backoff_key = QUEUE_BACKOFF.format(namespace=namespace, agent=agent.name)
    queue_main_key = QUEUE_MAIN.format(namespace=namespace, agent=agent.name)

    try:
        backoff_recovery_script = await create_backoff_recovery_script(redis_client)
        recovered_task_ids = await backoff_recovery_script.execute(
            queue_backoff_key=queue_backoff_key,
            queue_main_key=queue_main_key,
            queue_orphaned_key=QUEUE_ORPHANED.format(
                namespace=namespace, agent=agent.name
            ),
            task_statuses_key=TASK_STATUS.format(namespace=namespace),
            task_agents_key=TASK_AGENT.format(namespace=namespace),
            task_payloads_key=TASK_PAYLOAD.format(namespace=namespace),
            task_pickups_key=TASK_PICKUPS.format(namespace=namespace),
            task_retries_key=TASK_RETRIES.format(namespace=namespace),
            task_metas_key=TASK_META.format(namespace=namespace),
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

    queue_completions_key = QUEUE_COMPLETIONS.format(
        namespace=namespace, agent=agent.name
    )
    queue_failed_key = QUEUE_FAILED.format(namespace=namespace, agent=agent.name)
    queue_cancelled_key = QUEUE_CANCELLED.format(namespace=namespace, agent=agent.name)
    queue_orphaned_key = QUEUE_ORPHANED.format(namespace=namespace, agent=agent.name)

    try:
        result: TaskExpirationScriptResult = await task_expiration_script.execute(
            queue_completions_key=queue_completions_key,
            queue_failed_key=queue_failed_key,
            queue_cancelled_key=queue_cancelled_key,
            queue_orphaned_key=queue_orphaned_key,
            task_statuses_key=TASK_STATUS.format(namespace=namespace),
            task_agents_key=TASK_AGENT.format(namespace=namespace),
            task_payloads_key=TASK_PAYLOAD.format(namespace=namespace),
            task_pickups_key=TASK_PICKUPS.format(namespace=namespace),
            task_retries_key=TASK_RETRIES.format(namespace=namespace),
            task_metas_key=TASK_META.format(namespace=namespace),
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


async def worker_loop(
    shutdown_event: asyncio.Event,
    redis_pool: redis.ConnectionPool,
    namespace: str,
    worker_id: str,
    agent: BaseAgent[Any],
    batch_size: int,
    max_retries: int,
    heartbeat_interval: int,
    task_timeout: int,
    metrics_bucket_duration: int,
    metrics_retention_duration: int,
) -> None:
    """Main worker loop"""
    redis_client = redis.Redis(connection_pool=redis_pool, decode_responses=True)
    batch_script = await create_batch_pickup_script(redis_client)
    completion_script = await create_task_completion_script(redis_client)
    steering_script = await create_task_steering_script(redis_client)

    logger.info(f"Worker {worker_id} started")
    current_tasks: list[asyncio.Task[Any]] = []

    try:
        while not shutdown_event.is_set():
            task_batch: tuple[list[str], list[str]] = await get_task_batch(
                batch_script=batch_script,
                agent=agent,
                batch_size=batch_size,
                metrics_bucket_duration=metrics_bucket_duration,
                metrics_ttl=metrics_retention_duration,
                namespace=namespace,
            )
            tasks_to_process_ids: list[str] = task_batch[0]
            tasks_to_cancel_ids: list[str] = task_batch[1]

            if tasks_to_process_ids:
                logger.info(
                    f"Worker {worker_id} got {len(tasks_to_process_ids)} tasks to process"
                )

                cancel_task = (
                    asyncio.create_task(
                        handle_cancelled_tasks(
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
                            max_retries=max_retries,
                            heartbeat_interval=heartbeat_interval,
                            task_timeout=task_timeout,
                            metrics_bucket_duration=metrics_bucket_duration,
                            metrics_retention_duration=metrics_retention_duration,
                        )
                    )
                    for task_id in tasks_to_process_ids
                ]

                all_tasks = current_tasks + ([cancel_task] if cancel_task else [])
                await asyncio.gather(*all_tasks, return_exceptions=True)
                current_tasks = []

                # Check if we should exit after completing batch
                if shutdown_event.is_set():
                    logger.info(
                        f"Worker {worker_id} shutting down after completing batch"
                    )
                    break
            else:
                if tasks_to_cancel_ids:
                    await handle_cancelled_tasks(
                        redis_client=redis_client,
                        namespace=namespace,
                        cancelled_task_ids=tasks_to_cancel_ids,
                        agent=agent,
                    )

                # No tasks, wait a bit but check for shutdown
                sleep_time = 0.25 + random.uniform(0, 0.5)  # 0.25s to 0.75s
                try:
                    await asyncio.wait_for(shutdown_event.wait(), timeout=sleep_time)
                    break
                except asyncio.TimeoutError:
                    pass

    except asyncio.CancelledError:
        # Cancel any running tasks
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
