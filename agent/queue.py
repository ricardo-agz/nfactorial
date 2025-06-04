import random
from typing import Any, cast
import uuid
import redis.asyncio as redis
import secrets
import time
import json
import asyncio
from dataclasses import replace

from agent.context import Task, TaskStatus, ContextType
from agent.agent import (
    BaseAgent,
    EventPublisher,
    AgentEvent,
    ExecutionContext,
)
from agent.events import QueueEvent
from agent.logging import get_logger, colored
from agent.scripts import (
    BatchMoveScript,
    TaskSteeringScript,
    TaskCompletionScript,
    StaleRecoveryScript,
    GarbageCollectionScript,
    create_batch_move_script,
    create_task_steering_script,
    create_task_completion_script,
    create_stale_recovery_script,
    create_garbage_collection_script,
    create_tool_completion_script,
    create_enqueue_task_script,
    BatchMoveScriptResult,
    StaleRecoveryScriptResult,
    GarbageCollectionScriptResult,
)
from agent.utils import decode


logger = get_logger("agent.queue")

# ***** REDIS KEY SPACE *****

# HASH: task data {id, status, retries, owner_id, payload}
TASKS_DATA = "tasks:{task_id}"

# ===== QUEUE MANAGEMENT =====
# LIST: task_ids waiting to be processed
QUEUE_MAIN = "queue:{agent}:main"
# ZSET: task_id -> timestamp of failure
QUEUE_FAILED = "queue:{agent}:failed"
# ZSET: task_id -> timestamp of completion
QUEUE_COMPLETIONS = "queue:{agent}:completed"
# ZSET: task_id -> timestamp of cancellation
QUEUE_CANCELLED = "queue:{agent}:cancelled"

# ===== ACTIVE TASK PROCESSING =====
# ZSET: task_id -> timestamp of last heartbeat
PROCESSING_HEARTBEATS = "processing:{agent}:heartbeats"

# ===== STEERING & CONTROL =====
# SET: task_ids marked for cancellation
TASK_CANCELLATIONS = "cancel:pending"
# HASH: message_id -> steering_message_json
TASK_STEERING = "steer:{task_id}:messages"

# ===== PENDING TOOL EXECUTION =====
# HASH: tool_call_id -> result_json or <|PENDING|>
PENDING_TOOL_RESULTS = "tool:{agent}:{task_id}:results"

# ===== COMMUNICATION =====
# PUBSUB: real-time updates to task owners
UPDATES_CHANNEL = "updates:{owner_id}"

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
    complete_async_tool(tool_call_id, result)
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


def is_valid_task_id(task_id: str) -> bool:
    try:
        uuid.UUID(task_id)
    except ValueError:
        return False
    return True


async def get_task_data(redis_client: redis.Redis, task_id: str) -> dict[str, Any]:
    task_data_key = TASKS_DATA.format(task_id=task_id)
    raw_task_data = cast(
        dict[str | bytes, str | bytes],
        await redis_client.hgetall(task_data_key),  # type: ignore
    )

    task_data: dict[str, Any] = {
        decode(key): decode(value) for key, value in raw_task_data.items()
    }

    return task_data


async def get_task_status(redis_client: redis.Redis, task_id: str) -> TaskStatus:
    if not is_valid_task_id(task_id):
        raise InvalidTaskIdError(task_id)

    task_status_raw: str | bytes = await redis_client.hget(
        TASKS_DATA.format(task_id=task_id), "status"
    )  # type: ignore
    if not task_status_raw:
        raise TaskNotFoundError(task_id)

    task_status = decode(task_status_raw)
    return TaskStatus(task_status)


async def enqueue_task(
    redis_client: redis.Redis, agent: BaseAgent[Any], task: Task[ContextType]
) -> None:
    queue_main_key = QUEUE_MAIN.format(agent=agent.name)
    tasks_data_key = TASKS_DATA.format(task_id=task.id)

    task.status = TaskStatus.QUEUED
    task_data = task.to_dict()

    if not is_valid_task_id(task.id):
        raise InvalidTaskIdError(task.id)

    enqueue_script = create_enqueue_task_script(redis_client)
    await enqueue_script.execute(
        queue_main_key=queue_main_key,
        tasks_data_key=tasks_data_key,
        task_id=task.id,
        task_data=json.dumps(task_data),
    )  # type: ignore


async def cancel_task(
    redis_client: redis.Redis,
    task_id: str,
) -> None:
    """Add a task to the cancellation set"""
    task_status = await get_task_status(redis_client, task_id)
    if task_status in [TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED]:
        raise InactiveTaskError(task_id)

    await redis_client.sadd(TASK_CANCELLATIONS, task_id)  # type: ignore


async def steer_task(
    redis_client: redis.Redis,
    task_id: str,
    messages: list[dict[str, Any]],
) -> None:
    """Steer a task"""
    task_status = await get_task_status(redis_client, task_id)
    if task_status in [TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED]:
        raise InactiveTaskError(task_id)

    task_steering_key = TASK_STEERING.format(task_id=task_id)
    # message id format: {timestamp_ms}_{random_hex} e.g. 1717234200000_a3f4b5c6
    message_mapping = {
        f"{int(time.time() * 1000)}_{secrets.token_hex(3)}": json.dumps(message)
        for message in messages
    }
    await redis_client.hset(task_steering_key, mapping=message_mapping)  # type: ignore


async def get_steering_messages(
    redis_client: redis.Redis,
    task_id: str,
) -> list[tuple[str, dict[str, Any]]]:
    """Get steering messages for a task"""
    await get_task_status(redis_client, task_id)  # Raise if task does not exist

    steering_key = TASK_STEERING.format(task_id=task_id)
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


async def complete_async_tool(
    redis_client: redis.Redis,
    agent: BaseAgent[Any],
    task_id: str,
    tool_call_id: str,
    result: Any,
) -> bool:
    """Complete a tool call"""
    task_status = await get_task_status(redis_client, task_id)
    if task_status in [TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED]:
        raise InactiveTaskError(task_id)

    tasks_key = TASKS_DATA.format(task_id=task_id)
    tool_results_key = PENDING_TOOL_RESULTS.format(agent=agent.name, task_id=task_id)
    agent_queue_key = QUEUE_MAIN.format(agent=agent.name)

    task_dict = await get_task_data(redis_client, task_id)
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
    tool_completion_script = create_tool_completion_script(redis_client)
    success = await tool_completion_script.execute(
        pending_tool_results_key=tool_results_key,
        queue_main_key=agent_queue_key,
        tasks_data_key=tasks_key,
        task_id=task.id,
        updated_task_context_json=updated_context.to_json(),
    )

    if success:
        return True
    else:
        return False


async def publish_update(
    redis_client: redis.Redis, owner_id: str, payload: dict[str, Any]
) -> None:
    user_update_channel = UPDATES_CHANNEL.format(owner_id=owner_id)
    await redis_client.publish(user_update_channel, json.dumps(payload))  # type: ignore


async def get_task_batch(
    batch_script: BatchMoveScript,
    agent: BaseAgent[Any],
    batch_size: int,
) -> tuple[list[str], list[str]]:
    """
    Get batch of tasks atomically

    Returns:
        * List of task ids to process
        * List of task ids to cancel
    """
    task_data_key_template = TASKS_DATA
    queue_main_key = QUEUE_MAIN.format(agent=agent.name)
    processing_heartbeats_key = PROCESSING_HEARTBEATS.format(agent=agent.name)
    task_cancellations_key = TASK_CANCELLATIONS
    queue_cancelled_key = QUEUE_CANCELLED.format(agent=agent.name)

    try:
        result: BatchMoveScriptResult = await batch_script.execute(
            task_data_key_template=task_data_key_template,
            queue_main_key=queue_main_key,
            queue_cancelled_key=queue_cancelled_key,
            processing_heartbeats_key=processing_heartbeats_key,
            task_cancellations_key=task_cancellations_key,
            batch_size=batch_size,
        )

        return result.tasks_to_process_ids, result.tasks_to_cancel_ids

    except Exception as e:
        logger.error(f"Failed to get task batch: {e}")
        await asyncio.sleep(0.15 + random.random() * 0.1)  # backoff with jitter
        return [], []


async def heartbeat_loop(
    redis_client: redis.Redis,
    task_id: str,
    agent: BaseAgent[Any],
    stop_event: asyncio.Event,
    interval: int,
) -> None:
    """Simple heartbeat loop that runs until stopped"""
    agent_heartbeats = PROCESSING_HEARTBEATS.format(agent=agent.name)

    try:
        while not stop_event.is_set():
            try:
                await redis_client.zadd(
                    agent_heartbeats,
                    {task_id: time.time()},
                )

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
    cancelled_task_ids: list[str],
    agent: BaseAgent[Any],
) -> None:
    """Send cancellation events for cancelled tasks concurrently"""

    async def cancel_task(task_id: str) -> None:
        task_data = await get_task_data(redis_client, task_id)
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
            channel=UPDATES_CHANNEL.format(owner_id=task.owner_id),
        )

        try:
            execution_ctx = ExecutionContext(
                task_id=task.id,
                owner_id=task.owner_id,
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
                    owner_id=task.owner_id,
                    agent_name=agent.name,
                )
            )
        except Exception as e:
            logger.error(f"Error sending cancellation event for {task.id}", exc_info=e)

    tasks = [cancel_task(task_id) for task_id in cancelled_task_ids]
    await asyncio.gather(*tasks)


async def process_task(
    redis_client: redis.Redis,
    task_id: str,
    completion_script: TaskCompletionScript,
    steering_script: TaskSteeringScript,
    agent: BaseAgent[ContextType],
    max_retries: int,
    heartbeat_interval: int,
    task_timeout: int = 90,
) -> None:
    """Process a single task"""
    logger.info(f"▶️  Task started   {colored(f'[{task_id}]', 'dim')}")

    tasks_data_key = TASKS_DATA.format(task_id=task_id)
    queue_main_key = QUEUE_MAIN.format(agent=agent.name)
    processing_heartbeats_key = PROCESSING_HEARTBEATS.format(agent=agent.name)
    queue_failed_key = QUEUE_FAILED.format(agent=agent.name)
    queue_completions_key = QUEUE_COMPLETIONS.format(agent=agent.name)
    task_steering_key = TASK_STEERING.format(task_id=task_id)
    pending_tool_results_key = PENDING_TOOL_RESULTS.format(
        agent=agent.name, task_id=task_id
    )

    task_data = await get_task_data(redis_client, task_id)
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
            task_id=task_id,
            agent=agent,
            stop_event=stop_heartbeat,
            interval=heartbeat_interval,
        )
    )

    event_publisher = EventPublisher(
        redis_client=redis_client,
        channel=UPDATES_CHANNEL.format(owner_id=task.owner_id),
    )

    task_failed = False
    try:
        if task.payload.turn == 0 and task.retries == 0:
            await event_publisher.publish_event(
                AgentEvent(
                    event_type="run_started",
                    task_id=task.id,
                    owner_id=task.owner_id,
                    agent_name=agent.name,
                )
            )

        execution_ctx = ExecutionContext(
            task_id=task.id,
            owner_id=task.owner_id,
            retries=task.retries,
            iterations=task.payload.turn,
            events=event_publisher,
        )

        steering_messages_data = await get_steering_messages(
            redis_client=redis_client,
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
                    task_data_key=tasks_data_key,
                    steering_messages_key=task_steering_key,
                    steering_message_ids=steering_message_ids,
                    updated_task_payload_json=steered_task.to_json(),
                )

                task = steered_task
                await event_publisher.publish_event(
                    AgentEvent(
                        event_type="run_steering_applied",
                        task_id=task.id,
                        owner_id=task.owner_id,
                        agent_name=agent.name,
                    )
                )
            except Exception as e:
                logger.error(f"Error updating task with steering messages: {e}")
                await event_publisher.publish_event(
                    AgentEvent(
                        event_type="run_steering_failed",
                        task_id=task.id,
                        owner_id=task.owner_id,
                        agent_name=agent.name,
                        error=str(e),
                    )
                )

        turn_completion = await asyncio.wait_for(
            agent.execute(task.payload, execution_ctx),
            timeout=task_timeout,
        )

        if turn_completion.pending_tool_call_ids:
            res = await completion_script.execute(
                tasks_data_key=tasks_data_key,
                processing_heartbeats_key=processing_heartbeats_key,
                queue_main_key=queue_main_key,
                queue_completions_key=queue_completions_key,
                queue_failed_key=queue_failed_key,
                pending_tool_results_key=pending_tool_results_key,
                task_id=task.id,
                action="pending_tool_call_results",
                updated_task_payload_json=task.payload.to_json(),
                pending_tool_call_result_sentinel=PENDING_SENTINEL,
                pending_tool_call_ids_json=json.dumps(
                    turn_completion.pending_tool_call_ids
                ),
            )
            logger.info(f"PENDING TOOL CALL IDS Completion script result: {res}")

            logger.info(
                f"⏳ Task awaiting tool results {colored(f'[{task.id}]', 'dim')}"
            )
            await event_publisher.publish_event(
                AgentEvent(
                    event_type="task_pending_tool_call_results",
                    task_id=task.id,
                    owner_id=task.owner_id,
                    agent_name=agent.name,
                )
            )

        elif turn_completion.is_done:
            res = await completion_script.execute(
                tasks_data_key=tasks_data_key,
                processing_heartbeats_key=processing_heartbeats_key,
                queue_main_key=queue_main_key,
                queue_completions_key=queue_completions_key,
                queue_failed_key=queue_failed_key,
                pending_tool_results_key=pending_tool_results_key,
                task_id=task.id,
                action="complete",
                updated_task_payload_json=task.payload.to_json(),
            )
            logger.info(f"✅ Task completed Completion script result: {res}")
            logger.info(f"✅ Task completed {colored(f'[{task.id}]', 'dim')}")
            await event_publisher.publish_event(
                AgentEvent(
                    event_type="run_completed",
                    task_id=task.id,
                    owner_id=task.owner_id,
                    agent_name=agent.name,
                )
            )

        else:
            # Task needs to continue processing
            task.payload = turn_completion.context
            logger.info(f"⏩ Task continued {colored(f'[{task.id}]', 'dim')}")

            await completion_script.execute(
                tasks_data_key=tasks_data_key,
                processing_heartbeats_key=processing_heartbeats_key,
                queue_main_key=queue_main_key,
                queue_completions_key=queue_completions_key,
                queue_failed_key=queue_failed_key,
                pending_tool_results_key=pending_tool_results_key,
                task_id=task.id,
                action="continue",
                updated_task_payload_json=task.payload.to_json(),
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
                owner_id=task.owner_id,
                agent_name=agent.name,
                error=f"Task {task.id} timed out after {task_timeout} seconds",
            )
        )

        action = "fail" if task.retries >= max_retries else "retry"
        res = await completion_script.execute(
            tasks_data_key=tasks_data_key,
            processing_heartbeats_key=processing_heartbeats_key,
            queue_main_key=queue_main_key,
            queue_completions_key=queue_completions_key,
            queue_failed_key=queue_failed_key,
            pending_tool_results_key=pending_tool_results_key,
            task_id=task.id,
            action=action,
            updated_task_payload_json=task.payload.to_json(),
        )
        logger.info(f"❌ Task failed due to timeout Completion script result: {res}")

    except Exception as e:
        task_failed = True
        logger.error(f"❌ Task failed {colored(f'[{task.id}]', 'dim')}", exc_info=e)

        try:
            await event_publisher.publish_event(
                QueueEvent(
                    event_type="task_failed",
                    task_id=task.id,
                    owner_id=task.owner_id,
                    agent_name=agent.name,
                    error=str(e),
                )
            )
        except Exception as e:
            logger.error(f"Failed to send task failed event: {e}")

        action = "fail" if task.retries >= max_retries else "retry"
        res = await completion_script.execute(
            tasks_data_key=tasks_data_key,
            processing_heartbeats_key=processing_heartbeats_key,
            queue_main_key=queue_main_key,
            queue_completions_key=queue_completions_key,
            queue_failed_key=queue_failed_key,
            pending_tool_results_key=pending_tool_results_key,
            task_id=task.id,
            action=action,
            updated_task_payload_json=task.payload.to_json(),
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
                logger.info(
                    f"❌ Task failed permanently {colored(f'[{task.id}]', 'dim')}"
                )
                await event_publisher.publish_event(
                    AgentEvent(
                        event_type="run_failed",
                        task_id=task.id,
                        owner_id=task.owner_id,
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
                        owner_id=task.owner_id,
                        agent_name=agent.name,
                    )
                )

        try:
            await heartbeat_task
        except asyncio.CancelledError:
            pass


async def recover_stale_tasks(
    recovery_script: StaleRecoveryScript,
    agent: BaseAgent[Any],
    heartbeat_timeout: int,
    max_retries: int,
    batch_size: int,
) -> int:
    """Atomically move stale tasks back to main queue"""
    cutoff_timestamp = time.time() - heartbeat_timeout

    task_data_key_template = TASKS_DATA
    queue_main_key = QUEUE_MAIN.format(agent=agent.name)
    processing_heartbeats_key = PROCESSING_HEARTBEATS.format(agent=agent.name)
    queue_failed_key = QUEUE_FAILED.format(agent=agent.name)

    try:
        result: StaleRecoveryScriptResult = await recovery_script.execute(
            task_data_key_template=task_data_key_template,
            queue_main_key=queue_main_key,
            processing_heartbeats_key=processing_heartbeats_key,
            queue_failed_key=queue_failed_key,
            cutoff_timestamp=cutoff_timestamp,
            max_recovery_batch=batch_size,
            max_retries=max_retries,
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


async def garbage_collect_expired_tasks(
    garbage_collection_script: GarbageCollectionScript,
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

    task_data_key_template = TASKS_DATA
    queue_completions_key = QUEUE_COMPLETIONS.format(agent=agent.name)
    queue_failed_key = QUEUE_FAILED.format(agent=agent.name)
    queue_cancelled_key = QUEUE_CANCELLED.format(agent=agent.name)

    try:
        result: GarbageCollectionScriptResult = await garbage_collection_script.execute(
            task_data_key_template=task_data_key_template,
            queue_completions_key=queue_completions_key,
            queue_failed_key=queue_failed_key,
            queue_cancelled_key=queue_cancelled_key,
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
                f"🗑️  Garbage collected {total_cleaned} expired tasks "
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
        logger.error(f"Error during garbage collection: {e}")
        return 0


async def maintenance_loop(
    shutdown_event: asyncio.Event,
    redis_pool: redis.ConnectionPool,
    agent: BaseAgent[Any],
    heartbeat_timeout: int,
    max_retries: int,
    batch_size: int,
    interval: int,
    task_ttl_config: Any,  # Will be TaskTTLConfig from manager.py
    max_cleanup_batch: int,
) -> None:
    """Background maintenance worker to periodically recover stale tasks and clean up expired tasks"""
    redis_client = redis.Redis(connection_pool=redis_pool, decode_responses=True)
    recovery_script = create_stale_recovery_script(redis_client)
    garbage_collection_script = create_garbage_collection_script(redis_client)

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
                )

                # Then, garbage collect expired finished tasks
                await garbage_collect_expired_tasks(
                    garbage_collection_script=garbage_collection_script,
                    agent=agent,
                    task_ttl_config=task_ttl_config,
                    max_cleanup_batch=max_cleanup_batch,
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
    worker_id: str,
    agent: BaseAgent[Any],
    batch_size: int,
    max_retries: int,
    heartbeat_interval: int,
    task_timeout: int = 90,
) -> None:
    """Main worker loop"""
    redis_client = redis.Redis(connection_pool=redis_pool, decode_responses=True)
    batch_script = create_batch_move_script(redis_client)
    completion_script = create_task_completion_script(redis_client)
    steering_script = create_task_steering_script(redis_client)

    logger.info(f"Worker {worker_id} started")
    current_tasks: list[asyncio.Task[Any]] = []

    try:
        while not shutdown_event.is_set():
            task_batch: tuple[list[str], list[str]] = await get_task_batch(
                batch_script=batch_script,
                agent=agent,
                batch_size=batch_size,
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
                            task_id=task_id,
                            completion_script=completion_script,
                            steering_script=steering_script,
                            agent=agent,
                            max_retries=max_retries,
                            heartbeat_interval=heartbeat_interval,
                            task_timeout=task_timeout,
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
