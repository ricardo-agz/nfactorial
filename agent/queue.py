import random
from typing import Any, cast
import redis.asyncio as redis
import secrets
import time
import json
import asyncio
from dataclasses import replace
from redis.commands.core import AsyncScript

from agent.context import Task
from agent.agent import (
    BaseAgent,
    EventPublisher,
    AgentEvent,
    ExecutionContext,
)
from agent.events import QueueEvent
from agent.logging import get_logger, colored

logger = get_logger("agent.queue")

# List: task_json
MAIN_QUEUE = "requests:{agent}:main"
# List: task_json
FAILED_QUEUE = "requests:{agent}:failed"
# Sorted set: processing_key -> timestamp
PROCESSING_HEARTBEATS = "requests:{agent}:processing:heartbeats"
# Hash: processing_key -> task_json
PROCESSING_TASKS = "requests:{agent}:processing:data"
# Set: task_ids
CANCELLED_TASKS = "cancellations"
# PubSub
UPDATE_CHANNEL = "updates:{owner_id}"
# Hash: message_id -> message
TASK_STEERING = "steering:{task_id}"

BATCH_SIZE = 25
MAX_RETRIES = 3
STALE_THRESHOLD = 120
HEARTBEAT_INTERVAL = 5  # seconds


"""
Workflow:

1. Worker pops a batch of tasks from the main queue and puts them in processing
    * processing times: task_id -> started_at timestamp
    * processing data: task_id -> task_json
2. Same worker processes tasks concurrently in a batch
3. Once tasks are completed:
    * Finished tasks get removed from processing
    * Failed tasks get moved back to the front of the main queue
    * Incomple tasks get moved back to the front of the main queue to be picked up again by another worker
5. Optional retry mechanism to move stale tasks back to the front of the main queue
    * Checks for task in processing sorted set that are older than X seconds
"""


def create_batch_move_script(redis_client: redis.Redis) -> AsyncScript:
    """
    Create atomic script for moving tasks from main queue to processing

    Returns:
        * List of tasks
        * List of tasks to cancel
    """
    return redis_client.register_script(
        """
        local main_queue = KEYS[1]
        local processing_heartbeats = KEYS[2]
        local processing_tasks = KEYS[3]
        local cancelled_set = KEYS[4]
        local batch_size = tonumber(ARGV[1])
        local worker_id = ARGV[2]
        local batch_id = ARGV[3]
        local timestamp = tonumber(ARGV[4])
        
        local tasks = {}
        local tasks_to_cancel = {}
        
        -- Try to get more tasks than requested to account for cancelled ones
        local attempts = batch_size * 2
        
        for i = 1, attempts do
            local task_json = redis.call('LPOP', main_queue)
            if not task_json then
                break
            end

            local task = cjson.decode(task_json)

            -- Check if cancelled
            if redis.call('SISMEMBER', cancelled_set, task.id) == 1 then
                -- Add to cancelled list with info needed for event
                table.insert(tasks_to_cancel, task_json)
                -- Remove from cancelled set
                redis.call('SREM', cancelled_set, task.id)

            else
                -- Not cancelled, add to batch
                table.insert(tasks, task_json)
                
                -- Generate processing key
                local task_index = #tasks
                local processing_key = worker_id .. ":" .. batch_id .. ":" .. task_index
                
                -- Add to processing structures
                redis.call('ZADD', processing_heartbeats, timestamp, processing_key)
                redis.call('HSET', processing_tasks, processing_key, task_json)
                
                -- Stop if we have enough tasks
                if #tasks >= batch_size then
                    break
                end
            end
        end
        
        return {tasks, tasks_to_cancel}
    """
    )


def create_heartbeat_script(redis_client: redis.Redis) -> AsyncScript:
    """Simple heartbeat update"""
    return redis_client.register_script(
        """
        local processing_heartbeats = KEYS[1]
        local processing_key = ARGV[1]
        local timestamp = tonumber(ARGV[2])
        
        -- Update heartbeat timestamp
        redis.call('ZADD', processing_heartbeats, timestamp, processing_key)
        return true
        """
    )


def create_steering_script(redis_client: redis.Redis) -> AsyncScript:
    """
    Create atomic script for to handle updating a task with steering messages.
    Updates a task with the updated task json and clears the steering messages.
    """
    return redis_client.register_script(
        """
        local processing_tasks = KEYS[1]
        local steering_messages = KEYS[2]
        local processing_key = ARGV[1]
        local steering_message_ids = ARGV[2]
        local updated_task_json = ARGV[3]
        
        -- Update task with steering message
        redis.call('HSET', processing_tasks, processing_key, updated_task_json)
        -- Delete each steering message
        for _, id in ipairs(cjson.decode(steering_message_ids)) do
            redis.call('HDEL', steering_messages, id)
        end
        """
    )


def create_task_completion_script(redis_client: redis.Redis) -> AsyncScript:
    """Create atomic script for handling task completion, continuation, or failure"""
    return redis_client.register_script(
        """
        local main_queue = KEYS[1]
        local processing_heartbeats = KEYS[2]
        local processing_tasks = KEYS[3]
        local failed_queue = KEYS[4]
        
        local processing_key = ARGV[1]
        local action = ARGV[2]  -- "complete", "continue", or "fail"
        local updated_task_json = ARGV[3]  -- Only used for continue/fail
        local max_retries = tonumber(ARGV[4])
        
        -- Verify the task exists in processing
        local task_exists = redis.call('HEXISTS', processing_tasks, processing_key)
        if task_exists == 0 then
            return {false, "task_not_found"}
        end
        
        -- Always remove from processing structures first
        redis.call('ZREM', processing_heartbeats, processing_key)
        redis.call('HDEL', processing_tasks, processing_key)
        
        if action == "complete" then
            -- Task finished successfully
            return {true, "completed"}
            
        elseif action == "continue" then
            -- Task needs more processing, put back at front of main queue
            redis.call('LPUSH', main_queue, updated_task_json)
            return {true, "continued"}
            
        elseif action == "fail" then
            -- Task failed, parse to check retry count
            local task = cjson.decode(updated_task_json)
            local retries = task.retries or 0
            
            if retries < max_retries then
                redis.call('LPUSH', main_queue, updated_task_json)
                return {true, "retried"}
            else
                redis.call('LPUSH', failed_queue, updated_task_json)
                return {true, "failed_permanently"}
            end
        else
            return {false, "invalid_action"}
        end
        """
    )


def create_stale_recovery_script(redis_client: redis.Redis) -> AsyncScript:
    """Create atomic script for recovering stale tasks with retry increment"""
    return redis_client.register_script(
        """
        local main_queue = KEYS[1]
        local processing_heartbeats = KEYS[2] 
        local processing_tasks = KEYS[3]
        local failed_queue = KEYS[4]
        local cutoff_timestamp = tonumber(ARGV[1])
        local max_recovery_batch = tonumber(ARGV[2] or 100)
        local max_retries = tonumber(ARGV[3])
        
        -- Get stale processing keys (limited batch to avoid long-running operations)
        local stale_keys = redis.call('ZRANGEBYSCORE', processing_heartbeats, 0, cutoff_timestamp, 'LIMIT', 0, max_recovery_batch)
        
        local recovered_count = 0
        local failed_count = 0
        local recovered_tasks = {}
        
        for i = 1, #stale_keys do
            local processing_key = stale_keys[i]
            
            -- Atomically check if task data still exists and remove from processing
            local task_json = redis.call('HGET', processing_tasks, processing_key)
            
            if task_json then
                -- Parse task to check/increment retries
                local task = cjson.decode(task_json)
                task.retries = (task.retries or 0) + 1
                
                -- Remove from processing structures first (safer order)
                redis.call('ZREM', processing_heartbeats, processing_key)
                redis.call('HDEL', processing_tasks, processing_key)
                
                -- Route based on retry count
                if task.retries < max_retries then
                    -- Re-encode with incremented retry count
                    local updated_task_json = cjson.encode(task)
                    redis.call('LPUSH', main_queue, updated_task_json)
                    recovered_count = recovered_count + 1
                else
                    -- Max retries exceeded, send to failed queue
                    local updated_task_json = cjson.encode(task)
                    redis.call('LPUSH', failed_queue, updated_task_json)
                    failed_count = failed_count + 1
                end
                
                -- Add to recovered tasks
                table.insert(recovered_tasks, processing_key)
                table.insert(recovered_tasks, task.retries)
            else
                -- Task data missing but key exists in sorted set - cleanup
                redis.call('ZREM', processing_heartbeats, processing_key)
            end
        end
        
        return {recovered_count, failed_count, recovered_tasks}
        """
    )


async def enqueue_task(
    redis_client: redis.Redis, task: Task[Any], agent: BaseAgent[Any]
) -> None:
    agent_main_queue = MAIN_QUEUE.format(agent=agent.name)
    await redis_client.lpush(agent_main_queue, task.to_json())  # type: ignore


async def cancel_task(
    redis_client: redis.Redis,
    task_id: str,
) -> None:
    """Add a task to the cancellation set"""
    await redis_client.sadd(CANCELLED_TASKS, task_id)  # type: ignore
    await redis_client.expire(CANCELLED_TASKS, 60 * 5)  # type: ignore


async def steer_task(
    redis_client: redis.Redis,
    task_id: str,
    messages: list[dict[str, Any]],
) -> None:
    """Steer a task"""
    steering_key = TASK_STEERING.format(task_id=task_id)
    message_mapping = {
        f"{int(time.time() * 1000)}_{secrets.token_hex(3)}": json.dumps(message)
        for message in messages
    }
    await redis_client.hset(steering_key, mapping=message_mapping)  # type: ignore


async def get_steering_messages(
    redis_client: redis.Redis,
    task_id: str,
) -> list[tuple[str, dict[str, Any]]]:
    """Get steering messages for a task"""
    steering_key = TASK_STEERING.format(task_id=task_id)
    message_data: list[tuple[str, dict[str, Any]]] = []
    steering_messages = await redis_client.hgetall(steering_key)  # type: ignore
    for message_id, message in steering_messages.items():
        message_id_str = (
            message_id.decode("utf-8") if isinstance(message_id, bytes) else message_id
        )
        message_str = message.decode("utf-8") if isinstance(message, bytes) else message
        message_data.append((message_id_str, json.loads(message_str)))

    return message_data


async def publish_update(
    redis_client: redis.Redis, owner_id: str, payload: dict[str, Any]
) -> None:
    user_update_channel = UPDATE_CHANNEL.format(owner_id=owner_id)
    await redis_client.publish(user_update_channel, json.dumps(payload))  # type: ignore


async def get_task_batch(
    batch_script: AsyncScript,
    worker_id: str,
    agent: BaseAgent[Any],
    batch_size: int,
) -> tuple[list[tuple[Task[Any], str]], list[Task[Any]]]:
    """
    Get batch of tasks atomically

    Returns:
        * List of (task, processing_key) tuples
        * List of tasks to cancel
    """
    batch_id = secrets.token_hex(3)
    timestamp = time.time()

    agent_main_queue = MAIN_QUEUE.format(agent=agent.name)
    agent_processing_heartbeats = PROCESSING_HEARTBEATS.format(agent=agent.name)
    agent_processing_tasks = PROCESSING_TASKS.format(agent=agent.name)
    cancelled_tasks = CANCELLED_TASKS

    try:
        result: tuple[list[str], list[str]] = await batch_script(
            keys=[
                agent_main_queue,
                agent_processing_heartbeats,
                agent_processing_tasks,
                cancelled_tasks,
            ],
            args=[batch_size, worker_id, batch_id, timestamp],
        )  # type: ignore
        if not result:
            return [], []

        task_jsons, tasks_to_cancel_jsons = result
        tasks = [agent.deserialize_task(task_json) for task_json in task_jsons]
        tasks_to_cancel = [
            agent.deserialize_task(task_json) for task_json in tasks_to_cancel_jsons
        ]
        tasks_with_processing_keys = [
            (task, f"{worker_id}:{batch_id}:{i + 1}") for i, task in enumerate(tasks)
        ]
        return tasks_with_processing_keys, tasks_to_cancel

    except Exception as e:
        logger.error(f"Failed to get task batch: {e}")
        await asyncio.sleep(0.15 + random.random() * 0.1)  # backoff with jitter
        return [], []


async def heartbeat_loop(
    heartbeat_script: AsyncScript,
    processing_key: str,
    agent: BaseAgent[Any],
    stop_event: asyncio.Event,
    interval: int,
) -> None:
    """Simple heartbeat loop that runs until stopped"""
    agent_heartbeats = PROCESSING_HEARTBEATS.format(agent=agent.name)

    try:
        while not stop_event.is_set():
            try:
                # Send heartbeat
                await heartbeat_script(
                    keys=[agent_heartbeats],
                    args=[processing_key, time.time()],
                )

                # Wait for next interval
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=interval)
                    break  # Stop event was set
                except asyncio.TimeoutError:
                    continue  # Normal timeout, send next heartbeat

            except Exception as e:
                # Log but don't crash - missing one heartbeat shouldn't kill the task
                logger.error(f"Heartbeat error for {processing_key}: {e}")
                # Small backoff to avoid hammering Redis if it's having issues
                await asyncio.sleep(0.25 + random.random() * 0.5)
    except asyncio.CancelledError:
        # Task was cancelled, exit cleanly
        logger.debug(f"Heartbeat loop for {processing_key} cancelled")
        raise


async def handle_cancelled_tasks(
    redis_client: redis.Redis,
    cancelled_tasks: list[Task[Any]],
    agent: BaseAgent[Any],
) -> None:
    """Send cancellation events for cancelled tasks concurrently"""

    async def handle_single_task(task: Task[Any]) -> None:
        event_publisher = EventPublisher(
            redis_client=redis_client,
            channel=UPDATE_CHANNEL.format(owner_id=task.owner_id),
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

    tasks = [handle_single_task(task) for task in cancelled_tasks]
    await asyncio.gather(*tasks)


async def process_task(
    redis_client: redis.Redis,
    task: Task[Any],
    processing_key: str,
    heartbeat_script: AsyncScript,
    completion_script: AsyncScript,
    steering_script: AsyncScript,
    agent: BaseAgent[Any],
    max_retries: int,
    heartbeat_interval: int,
    task_timeout: int = 90,
) -> None:
    """Process a single task"""
    logger.info(f"▶️  Task started   {colored(f'[{task.id}]', 'dim')}")

    agent_main_queue = MAIN_QUEUE.format(agent=agent.name)
    agent_processing_heartbeats = PROCESSING_HEARTBEATS.format(agent=agent.name)
    agent_processing_tasks = PROCESSING_TASKS.format(agent=agent.name)
    agent_failed_queue = FAILED_QUEUE.format(agent=agent.name)
    agent_steering_messages = TASK_STEERING.format(task_id=task.id)

    stop_heartbeat = asyncio.Event()
    heartbeat_task = asyncio.create_task(
        heartbeat_loop(
            heartbeat_script=heartbeat_script,
            processing_key=processing_key,
            agent=agent,
            stop_event=stop_heartbeat,
            interval=heartbeat_interval,
        )
    )

    event_publisher = EventPublisher(
        redis_client=redis_client,
        channel=UPDATE_CHANNEL.format(owner_id=task.owner_id),
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

        def extract_timestamp(message_tuple):
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
                await steering_script(
                    keys=[agent_processing_tasks, agent_steering_messages],
                    args=[
                        processing_key,
                        json.dumps(steering_message_ids),
                        steered_task.to_json(),
                    ],
                )  # type: ignore
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

        if turn_completion.is_done:
            await completion_script(
                keys=[
                    agent_main_queue,
                    agent_processing_heartbeats,
                    agent_processing_tasks,
                    agent_failed_queue,
                ],
                args=[processing_key, "complete", "", max_retries],
            )  # type: ignore

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

            await completion_script(
                keys=[
                    agent_main_queue,
                    agent_processing_heartbeats,
                    agent_processing_tasks,
                    agent_failed_queue,
                ],
                args=[processing_key, "continue", task.to_json(), max_retries],
            )  # type: ignore

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

        # Increment retry count for timeouts
        task.retries += 1

        await completion_script(
            keys=[
                agent_main_queue,
                agent_processing_heartbeats,
                agent_processing_tasks,
                agent_failed_queue,
            ],
            args=[processing_key, "fail", task.to_json(), max_retries],
        )  # type: ignore

    except Exception as e:
        task_failed = True
        logger.error(f"❌ Task failed {colored(f'[{task.id}]', 'dim')}", exc_info=e)

        await event_publisher.publish_event(
            QueueEvent(
                event_type="task_failed",
                task_id=task.id,
                owner_id=task.owner_id,
                agent_name=agent.name,
                error=str(e),
            )
        )

        # Increment retry count for failures
        task.retries += 1

        await completion_script(
            keys=[
                agent_main_queue,
                agent_processing_heartbeats,
                agent_processing_tasks,
                agent_failed_queue,
            ],
            args=[processing_key, "fail", task.to_json(), max_retries],
        )  # type: ignore
    finally:
        # Stop heartbeat
        stop_heartbeat.set()
        heartbeat_task.cancel()

        # Only emit retry/failure events if the task actually failed
        if task_failed:
            if task.retries >= max_retries:
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
    recovery_script: AsyncScript,
    agent: BaseAgent[Any],
    heartbeat_timeout: int,
    max_retries: int,
    batch_size: int,
) -> int:
    """Atomically move stale tasks back to main queue"""
    cutoff = time.time() - heartbeat_timeout

    agent_main_queue = MAIN_QUEUE.format(agent=agent.name)
    agent_processing_heartbeats = PROCESSING_HEARTBEATS.format(agent=agent.name)
    agent_processing_tasks = PROCESSING_TASKS.format(agent=agent.name)
    agent_failed_queue = FAILED_QUEUE.format(agent=agent.name)

    try:
        result = cast(
            tuple[int, int, list[Any]],
            await recovery_script(
                keys=[
                    agent_main_queue,
                    agent_processing_heartbeats,
                    agent_processing_tasks,
                    agent_failed_queue,
                ],
                args=[cutoff, batch_size, max_retries],
            ),
        )

        recovered_count, failed_count, recovered_tasks_flat = result

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

            # Parse the flat list back into key-value pairs
            for i in range(0, len(recovered_tasks_flat), 2):
                if i + 1 < len(recovered_tasks_flat):
                    task_key = recovered_tasks_flat[i]
                    retries = recovered_tasks_flat[i + 1]
                    logger.info(colored(f"    [{task_key}]: {retries} retries", "dim"))

        return recovered_count

    except Exception as e:
        logger.error(f"Error during stale task recovery: {e}")
        return 0


async def stale_task_monitor(
    shutdown_event: asyncio.Event,
    redis_pool: redis.ConnectionPool,
    agent: BaseAgent[Any],
    heartbeat_timeout: int,
    max_retries: int,
    batch_size: int,
    interval: int,
) -> None:
    """Background task to periodically recover stale tasks"""
    redis_client = redis.Redis(connection_pool=redis_pool)
    recovery_script = create_stale_recovery_script(redis_client)

    logger.info(
        f"Stale task monitor started (checking every {interval}s for heartbeats >{heartbeat_timeout}s old)"
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
                logger.error(f"Error in stale task monitor: {e}")
                # Wait a bit before retrying to avoid tight error loops
                try:
                    await asyncio.wait_for(
                        shutdown_event.wait(), timeout=interval * 1.5
                    )
                    break
                except asyncio.TimeoutError:
                    continue

    except asyncio.CancelledError:
        logger.info("Stale task monitor cancelled")
        raise
    finally:
        await redis_client.close()
        logger.info("Stale task monitor finished")


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
    redis_client = redis.Redis(connection_pool=redis_pool)
    batch_script = create_batch_move_script(redis_client)
    heartbeat_script = create_heartbeat_script(redis_client)
    completion_script = create_task_completion_script(redis_client)
    steering_script = create_steering_script(redis_client)

    logger.info(f"Worker {worker_id} started")
    current_tasks: list[asyncio.Task[Any]] = []

    try:
        while not shutdown_event.is_set():
            batch, tasks_to_cancel = await get_task_batch(
                batch_script=batch_script,
                worker_id=worker_id,
                agent=agent,
                batch_size=batch_size,
            )

            if batch:
                logger.info(f"Worker {worker_id} got {len(batch)} tasks")

                cancel_task = (
                    asyncio.create_task(
                        handle_cancelled_tasks(
                            redis_client=redis_client,
                            cancelled_tasks=tasks_to_cancel,
                            agent=agent,
                        )
                    )
                    if tasks_to_cancel
                    else None
                )

                current_tasks = [
                    asyncio.create_task(
                        process_task(
                            redis_client=redis_client,
                            task=task_data,
                            processing_key=processing_key,
                            heartbeat_script=heartbeat_script,
                            completion_script=completion_script,
                            steering_script=steering_script,
                            agent=agent,
                            max_retries=max_retries,
                            heartbeat_interval=heartbeat_interval,
                            task_timeout=task_timeout,
                        )
                    )
                    for task_data, processing_key in batch
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
                if tasks_to_cancel:
                    await handle_cancelled_tasks(
                        redis_client=redis_client,
                        cancelled_tasks=tasks_to_cancel,
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
