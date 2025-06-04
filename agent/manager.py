from dataclasses import dataclass, field
import asyncio
import os
import signal
import redis.asyncio as redis
from typing import Any, Optional
import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

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


@dataclass
class ObservabilityConfig:
    """Configuration for the observability dashboard"""

    enabled: bool = True
    host: str = "0.0.0.0"
    port: int = 8080
    cors_origins: list[str] = field(default_factory=lambda: ["*"])


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
        observability_config: ObservabilityConfig | None = None,
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
        self.observability_config = observability_config or ObservabilityConfig()

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

    async def create_observability_app(self) -> FastAPI:
        """Create the observability FastAPI app (minimal, clean, robust)"""
        from agent.observability import add_observability_routes

        app = FastAPI(
            title="Observability Dashboard",
            description="Minimal real-time dashboard for distributed AI agent system",
            version="1.0.0",
        )

        app.add_middleware(
            CORSMiddleware,
            allow_origins=self.observability_config.cors_origins,
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

        redis_client = redis.Redis(
            connection_pool=self.redis_pool, decode_responses=True
        )
        agents = [runner.agent for runner in self.runners]

        # Get TTL config from the first runner (assuming all runners have the same TTL config)
        ttl_config = None
        if self.runners:
            ttl_config = self.runners[0].maintenance_worker_config.task_ttl

        add_observability_routes(app, redis_client, agents, ttl_config)

        @app.get("/")
        async def root():
            return {
                "message": "Observability dashboard",
                "dashboard": "/observability",
                "port": self.observability_config.port,
            }

        return app

    async def start_observability_server(self, shutdown_event: asyncio.Event) -> None:
        """Start the observability web server"""
        if not self.observability_config.enabled:
            return

        try:
            app = await self.create_observability_app()

            config = uvicorn.Config(
                app=app,
                host=self.observability_config.host,
                port=self.observability_config.port,
                log_level="info",
                access_log=False,  # Reduce noise
            )

            server = uvicorn.Server(config)

            logger.info(
                f"🌐 Observability dashboard available at http://{self.observability_config.host}:{self.observability_config.port}/observability"
            )

            # Run server until shutdown
            await server.serve()

        except Exception as e:
            logger.error(f"Error starting observability server: {e}")

    async def start_workers(self, shutdown_event: asyncio.Event) -> None:
        try:
            workers: list[asyncio.Task[Any]] = []
            for runner in self.runners:
                workers += runner.create_worker_tasks(shutdown_event)

            # Add observability server if enabled
            if self.observability_config.enabled:
                observability_task = asyncio.create_task(
                    self.start_observability_server(shutdown_event)
                )
                workers.append(observability_task)

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
