import traceback
from typing import Any
import redis.asyncio as redis
import secrets
import time
import json
import asyncio
from redis.commands.core import AsyncScript

from agent.context import Task, AgentContext
from agent.llms import MultiClient
from agent.agent import DummyAgent


MAIN_QUEUE = "requests:main"  # List: task_json
FAILED_QUEUE = "requests:failed"  # List: task_json
PROCESSING_TIMES = (
    "requests:processing:times"  # Sorted set: processing_key -> timestamp
)
PROCESSING_TASKS = "requests:processing:data"  # Hash: processing_key -> task_json
UPDATE_CHANNEL = "updates:{user_id}"

BATCH_SIZE = 25
MAX_RETRIES = 3
STALE_THRESHOLD = 120


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
    return redis_client.register_script(
        """
        local main_queue = KEYS[1]
        local processing_times = KEYS[2]
        local processing_tasks = KEYS[3]
        local batch_size = tonumber(ARGV[1])
        local worker_id = ARGV[2]
        local batch_id = ARGV[3]
        local timestamp = tonumber(ARGV[4])
        
        local tasks = {}
        
        for i = 1, batch_size do
            local task = redis.call('LPOP', main_queue)
            if task then
                table.insert(tasks, task)
                
                -- Generate processing key: worker_id:batch_id:task_index
                local processing_key = worker_id .. ":" .. batch_id .. ":" .. i
                
                -- Add to sorted set with processing_key and timestamp
                redis.call('ZADD', processing_times, timestamp, processing_key)
                -- Store task data in hash
                redis.call('HSET', processing_tasks, processing_key, task)
            else
                break
            end
        end
        
        return tasks
    """
    )


def create_task_completion_script(redis_client: redis.Redis) -> AsyncScript:
    """Create atomic script for handling task completion, continuation, or failure"""
    return redis_client.register_script(
        """
        local main_queue = KEYS[1]
        local processing_times = KEYS[2]
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
        redis.call('ZREM', processing_times, processing_key)
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
        local processing_times = KEYS[2] 
        local processing_tasks = KEYS[3]
        local failed_queue = KEYS[4]
        local cutoff_timestamp = tonumber(ARGV[1])
        local max_recovery_batch = tonumber(ARGV[2] or 100)
        local max_retries = tonumber(ARGV[3])  -- Add max retries param
        
        -- Get stale processing keys (limited batch to avoid long-running operations)
        local stale_keys = redis.call('ZRANGEBYSCORE', processing_times, 0, cutoff_timestamp, 'LIMIT', 0, max_recovery_batch)
        
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
                redis.call('ZREM', processing_times, processing_key)
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
                
                table.insert(recovered_tasks, {
                    key = processing_key,
                    retries = task.retries
                })
            else
                -- Task data missing but key exists in sorted set - cleanup
                redis.call('ZREM', processing_times, processing_key)
            end
        end
        
        return {recovered_count, failed_count, recovered_tasks}
        """
    )


async def enqueue_task(redis_client: redis.Redis, task: Task) -> None:
    await redis_client.lpush(MAIN_QUEUE, task.to_json())  # type: ignore


async def publish_update(
    redis_client: redis.Redis, user_id: str, payload: dict[str, Any]
) -> None:
    user_update_channel = UPDATE_CHANNEL.format(user_id=user_id)
    await redis_client.publish(user_update_channel, json.dumps(payload))  # type: ignore


async def get_task_batch(
    batch_script: AsyncScript, worker_id: str
) -> list[tuple[Task, str]]:
    """Get batch of tasks atomically"""
    batch_id = secrets.token_hex(3)
    timestamp = time.time()

    task_jsons: list[str] = await batch_script(
        keys=[MAIN_QUEUE, PROCESSING_TIMES, PROCESSING_TASKS],
        args=[BATCH_SIZE, worker_id, batch_id, timestamp],
    )  # type: ignore
    if not task_jsons:
        return []

    return [
        (Task.from_json(task_json), f"{worker_id}:{batch_id}:{i + 1}")
        for i, task_json in enumerate(task_jsons)
    ]


async def process_task(
    redis_client: redis.Redis,
    llm_client: MultiClient,
    task: Task,
    processing_key: str,
    completion_script: AsyncScript,
) -> None:
    """Process a single task"""
    print(f"Processing task: {task.id}")

    try:
        agent = DummyAgent(client=llm_client)
        is_done, updated_context = await agent.run_turn(task.payload)
        result: tuple[str, str]

        if is_done:
            # Task completed successfully
            await publish_update(
                redis_client,
                task.user_id,
                {
                    "task_id": task.id,
                    "event": "complete",
                    "content": updated_context.messages[-1]["content"],
                },
            )

            result = await completion_script(
                keys=[MAIN_QUEUE, PROCESSING_TIMES, PROCESSING_TASKS, FAILED_QUEUE],
                args=[processing_key, "complete", "", MAX_RETRIES],
            )  # type: ignore

            _, status = result
            print(f"Task {task.id} completed: {status}")

        else:
            # Task needs to continue processing
            task.payload = updated_context

            await publish_update(
                redis_client,
                task.user_id,
                {
                    "task_id": task.id,
                    "event": "progress",
                    "context": updated_context.to_dict(),
                },
            )

            result = await completion_script(
                keys=[MAIN_QUEUE, PROCESSING_TIMES, PROCESSING_TASKS, FAILED_QUEUE],
                args=[processing_key, "continue", task.to_json(), MAX_RETRIES],
            )  # type: ignore

            _, status = result
            print(f"Task {task.id} continued: {status}")

    except Exception as e:
        print(f"Task {task.id} failed with exception: {e}", traceback.format_exc())

        # Increment retry count for failures
        task.retries += 1

        result = await completion_script(
            keys=[MAIN_QUEUE, PROCESSING_TIMES, PROCESSING_TASKS, FAILED_QUEUE],
            args=[processing_key, "fail", task.to_json(), MAX_RETRIES],
        )  # type: ignore

        _, status = result
        print(f"Task {task.id} failed: {status}")


async def recover_stale_tasks(
    redis_client: redis.Redis, recovery_script: AsyncScript
) -> int:
    """Atomically move stale tasks back to main queue"""
    cutoff = time.time() - STALE_THRESHOLD
    max_batch = 100

    try:
        result: tuple[int, int, list[dict[str, Any]]] = await recovery_script(
            keys=[MAIN_QUEUE, PROCESSING_TIMES, PROCESSING_TASKS, FAILED_QUEUE],
            args=[cutoff, max_batch, MAX_RETRIES],
        )  # type: ignore

        recovered_count, _, recovered_tasks = result

        if recovered_count > 0:
            print(f"Recovered {recovered_count} stale tasks: {recovered_tasks}")

        return recovered_count

    except Exception as e:
        print(f"Error during stale task recovery: {e}")
        return 0


async def stale_task_monitor(
    shutdown_event: asyncio.Event, redis_pool: redis.ConnectionPool
) -> None:
    """Background task to periodically recover stale tasks"""
    redis_client = redis.Redis(connection_pool=redis_pool)
    recovery_script = create_stale_recovery_script(redis_client)

    print("Stale task monitor started")

    try:
        while not shutdown_event.is_set():
            try:
                await recover_stale_tasks(redis_client, recovery_script)

                # Wait before next check, but allow early exit on shutdown
                try:
                    await asyncio.wait_for(shutdown_event.wait(), timeout=30.0)
                    break  # Shutdown requested
                except asyncio.TimeoutError:
                    continue  # Normal timeout, continue monitoring

            except Exception as e:
                print(f"Error in stale task monitor: {e}")
                # Wait a bit before retrying to avoid tight error loops
                try:
                    await asyncio.wait_for(shutdown_event.wait(), timeout=10.0)
                    break
                except asyncio.TimeoutError:
                    continue

    except asyncio.CancelledError:
        print("Stale task monitor cancelled")
        raise
    finally:
        # Properly close the Redis client
        await redis_client.close()
        print("Stale task monitor finished")


async def worker_loop(
    shutdown_event: asyncio.Event,
    redis_pool: redis.ConnectionPool,
    llm_client: MultiClient,
    worker_id: str,
) -> None:
    """Main worker loop"""
    redis_client = redis.Redis(connection_pool=redis_pool)
    batch_script = create_batch_move_script(redis_client)
    completion_script = create_task_completion_script(redis_client)

    print(f"Worker {worker_id} started")

    try:
        while not shutdown_event.is_set():
            batch = await get_task_batch(batch_script, worker_id)

            if batch:
                print(f"Worker {worker_id} got {len(batch)} tasks")

                # Process all tasks in batch concurrently
                tasks = [
                    asyncio.create_task(
                        process_task(
                            redis_client=redis_client,
                            llm_client=llm_client,
                            task=task_data,
                            processing_key=processing_key,
                            completion_script=completion_script,
                        )
                    )
                    for task_data, processing_key in batch
                ]

                # Process tasks but check if we should shutdown while waiting
                _, pending = await asyncio.wait(
                    tasks, return_when=asyncio.FIRST_EXCEPTION
                )

                # Complete any remaining tasks
                if pending:
                    await asyncio.gather(*pending, return_exceptions=True)

                # Check if we should exit after completing batch
                if shutdown_event.is_set():
                    print(f"Worker {worker_id} shutting down after completing batch")
                    break
            else:
                # No tasks, wait a bit but check for shutdown
                try:
                    await asyncio.wait_for(shutdown_event.wait(), timeout=1.0)
                    # If we get here, shutdown was triggered
                    break
                except asyncio.TimeoutError:
                    # Normal timeout, continue loop
                    pass

    except asyncio.CancelledError:
        print(f"Worker {worker_id} cancelled")
        raise
    finally:
        # Properly close the Redis client connection
        await redis_client.close()
        print(f"Worker {worker_id} finished")
