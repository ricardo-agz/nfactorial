from dataclasses import dataclass, field
import asyncio
import os
import signal
import redis.asyncio as redis
from typing import Any

from agent.agent import BaseAgent
from agent.queue import enqueue_task, worker_loop, maintenance_loop
from agent.llms import MultiClient
from agent.utils import to_snake_case
from agent.logging import get_logger

logger = get_logger(__name__)


@dataclass
class AgentWorkerConfig:
    workers: int = 1
    batch_size: int = 25
    max_retries: int = 3
    heartbeat_interval: int = 5
    missed_heartbeats_threshold: int = 3
    missed_heartbeats_grace_period: int = 5
    turn_timeout: int = 90


@dataclass
class TaskTTLConfig:
    """TTL configuration for finished tasks (in seconds)"""

    completed_ttl: int = 3600  # 1 hour for completed tasks
    failed_ttl: int = 86400  # 24 hours for failed tasks (longer for debugging)
    cancelled_ttl: int = 1800  # 30 minutes for cancelled tasks


@dataclass
class MaintenanceWorkerConfig:
    """Configuration for the maintenance worker that handles both stale recovery and garbage collection"""

    interval: int = 10
    workers: int = 1
    task_ttl: TaskTTLConfig = field(default_factory=TaskTTLConfig)
    max_cleanup_batch: int = 100  # Maximum tasks to clean up per queue per run


class AgentRunner:
    def __init__(
        self,
        redis_pool: redis.ConnectionPool,
        llm_client: MultiClient,
        agent: BaseAgent[Any],
        agent_worker_config: AgentWorkerConfig,
        maintenance_worker_config: MaintenanceWorkerConfig,
    ):
        self.shutdown_event = asyncio.Event()
        self.redis_pool = redis_pool
        self.llm_client = llm_client
        self.agent = agent
        self.queue = to_snake_case(agent.__class__.__name__)
        self.agent_worker_config = agent_worker_config
        self.maintenance_worker_config = maintenance_worker_config

    def set_shutdown_event(self, shutdown_event: asyncio.Event):
        self.shutdown_event = shutdown_event

    def create_worker_tasks(
        self, shutdown_event: asyncio.Event
    ) -> list[asyncio.Task[Any]]:
        return [
            asyncio.create_task(
                worker_loop(
                    shutdown_event=shutdown_event,
                    redis_pool=self.redis_pool,
                    worker_id=f"{self.queue}-worker-{i + 1}",
                    agent=self.agent,
                    batch_size=self.agent_worker_config.batch_size,
                    max_retries=self.agent_worker_config.max_retries,
                    heartbeat_interval=self.agent_worker_config.heartbeat_interval,
                    task_timeout=self.agent_worker_config.turn_timeout,
                )
            )
            for i in range(self.agent_worker_config.workers)
        ] + [
            asyncio.create_task(
                maintenance_loop(
                    shutdown_event=shutdown_event,
                    redis_pool=self.redis_pool,
                    agent=self.agent,
                    heartbeat_timeout=self.agent_worker_config.heartbeat_interval
                    * self.agent_worker_config.missed_heartbeats_threshold
                    + self.agent_worker_config.missed_heartbeats_grace_period,
                    max_retries=self.agent_worker_config.max_retries,
                    batch_size=self.agent_worker_config.batch_size,
                    interval=self.maintenance_worker_config.interval,
                    task_ttl_config=self.maintenance_worker_config.task_ttl,
                    max_cleanup_batch=self.maintenance_worker_config.max_cleanup_batch,
                )
            )
            for _ in range(self.maintenance_worker_config.workers)
        ]


class ControlPlane:
    def __init__(
        self,
        redis_pool: redis.ConnectionPool | None = None,
        redis_host: str | None = None,
        redis_port: int = 6379,
        redis_db: int = 0,
        redis_max_connections: int = 50,
        openai_api_key: str | None = None,
        xai_api_key: str | None = None,
    ):
        self.shutdown_event = asyncio.Event()

        if redis_pool:
            self.redis_pool = redis_pool
        else:
            self.redis_pool = redis.ConnectionPool(
                host=redis_host or os.getenv("REDIS_HOST", "localhost"),
                port=redis_port,
                db=redis_db,
                max_connections=redis_max_connections,
            )

        self.llm_client = MultiClient(
            openai_api_key=openai_api_key or os.getenv("OPENAI_API_KEY"),
            xai_api_key=xai_api_key or os.getenv("XAI_API_KEY"),
        )

        self.runners: list[AgentRunner] = []

    def register_runner(
        self,
        agent: BaseAgent[Any],
        agent_worker_config: AgentWorkerConfig,
        maintenance_worker_config: MaintenanceWorkerConfig,
    ):
        runner = AgentRunner(
            redis_pool=self.redis_pool,
            llm_client=self.llm_client,
            agent=agent,
            agent_worker_config=agent_worker_config,
            maintenance_worker_config=maintenance_worker_config,
        )

        runner.set_shutdown_event(self.shutdown_event)
        self.runners.append(runner)

    async def start_workers(self, shutdown_event: asyncio.Event) -> None:
        try:
            workers: list[asyncio.Task[Any]] = []
            for runner in self.runners:
                workers += runner.create_worker_tasks(shutdown_event)

            logger.info(
                f"Started {len(workers)} total workers for {len(self.runners)} agents"
            )

            # Wait for all workers to complete or shutdown signal
            done, pending = await asyncio.wait(
                workers, return_when=asyncio.FIRST_COMPLETED
            )

            # If any worker exits, something went wrong or we're shutting down
            if done and not shutdown_event.is_set():
                logger.error("Worker exited unexpectedly")
                shutdown_event.set()

            # If shutdown was requested, wait for workers to finish gracefully
            if shutdown_event.is_set():
                logger.info(
                    "Shutdown requested, waiting for workers to finish gracefully..."
                )

                # Give workers more time to finish gracefully (60 seconds instead of 30)
                if pending:
                    _, still_pending = await asyncio.wait(pending, timeout=60)

                    if still_pending:
                        logger.warning(
                            f"{len(still_pending)} workers didn't finish in time, cancelling..."
                        )
                        for worker in still_pending:
                            worker.cancel()

                        # Wait for cancellation to complete
                        await asyncio.gather(*still_pending, return_exceptions=True)

            logger.info("All workers shut down")

        finally:
            await self.redis_pool.disconnect()
            logger.info("Redis connection pool closed")

    def run(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, self.shutdown_event.set)

        try:
            loop.run_until_complete(self.start_workers(self.shutdown_event))
        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt, shutting down...")
            self.shutdown_event.set()
        finally:
            # Cancel all remaining tasks before closing the loop
            pending_tasks = [
                task for task in asyncio.all_tasks(loop) if not task.done()
            ]
            if pending_tasks:
                logger.info(f"Cancelling {len(pending_tasks)} pending tasks...")
                for task in pending_tasks:
                    task.cancel()

                # Wait for all tasks to be cancelled
                loop.run_until_complete(
                    asyncio.gather(*pending_tasks, return_exceptions=True)
                )

            loop.close()
