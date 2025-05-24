import asyncio
import os
import signal
import redis.asyncio as redis

from agent.queue import stale_task_monitor, worker_loop
from agent.llms import MultiClient

NUM_WORKERS = 5


async def start_workers(shutdown_event: asyncio.Event) -> None:
    redis_pool = redis.ConnectionPool(
        host=os.getenv("REDIS_HOST", "localhost"), port=6379, db=0, max_connections=50
    )
    llm_client = MultiClient(
        openai_api_key=os.getenv("OPENAI_API_KEY"),
        xai_api_key=os.getenv("XAI_API_KEY"),
    )

    try:
        workers = [
            asyncio.create_task(
                worker_loop(shutdown_event, redis_pool, llm_client, f"worker-{i + 1}")
            )
            for i in range(NUM_WORKERS)
        ]

        monitor_task = asyncio.create_task(
            stale_task_monitor(shutdown_event, redis_pool)
        )
        workers += [monitor_task]

        print(f"Started {NUM_WORKERS} workers and stale task monitor")

        # Wait for all workers to complete
        done, pending = await asyncio.wait(workers, return_when=asyncio.FIRST_COMPLETED)

        # If any worker exits, something went wrong or we're shutting down
        if done and not shutdown_event.is_set():
            print("Worker exited unexpectedly")
            shutdown_event.set()

        # Wait for remaining workers with timeout
        if pending:
            print("Waiting for remaining workers to finish...")
            _, still_pending = await asyncio.wait(pending, timeout=30)

            if still_pending:
                print(
                    f"{len(still_pending)} workers didn't finish in time, cancelling..."
                )
                for worker in still_pending:
                    worker.cancel()
                await asyncio.gather(*still_pending, return_exceptions=True)

        print("All workers shut down")

    finally:
        await redis_pool.disconnect()
        print("Redis connection pool closed")


if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    shutdown_event = asyncio.Event()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, shutdown_event.set)

    try:
        loop.run_until_complete(start_workers(shutdown_event))
    finally:
        loop.close()
