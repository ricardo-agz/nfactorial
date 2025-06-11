from dataclasses import dataclass, field
import asyncio
import os
import signal
import httpx
import redis.asyncio as redis
from typing import Any, Literal, AsyncGenerator
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager

from factorial.agent import BaseAgent
from factorial.queue import worker_loop, maintenance_loop, UPDATES_CHANNEL
from factorial.llms import MultiClient
from factorial.utils import to_snake_case
from factorial.logging import get_logger
from factorial.task import Task, ContextType

logger = get_logger(__name__)


@dataclass
class AgentWorkerConfig:
    workers: int = 1
    batch_size: int = 25
    max_retries: int = 5
    heartbeat_interval: int = 5
    missed_heartbeats_threshold: int = 5
    missed_heartbeats_grace_period: int = 5
    turn_timeout: int = 120


@dataclass
class TaskTTLConfig:
    """TTL configuration for finished tasks (in seconds)"""

    completed_ttl: int = 3600  # 1 hour for completed tasks
    failed_ttl: int = 86400  # 24 hours for failed tasks (longer for debugging)
    cancelled_ttl: int = 1800  # 30 minutes for cancelled tasks


@dataclass
class MetricsTimelineConfig:
    """Configuration for metrics timeline and bucketing"""

    # Timeline duration in seconds
    timeline_duration: int = 3600  # 1 hour default

    # Bucket size in seconds (auto-calculated if None)
    bucket_size: Literal["seconds", "minutes", "hours", "days"] = "minutes"

    # Retention multiplier - how long to keep metrics data relative to timeline
    retention_multiplier: float = 2.0  # Keep data for 2x the timeline duration

    def __post_init__(self):
        """Calculate bucket size if not provided"""
        # Validate that timeline duration provides enough buckets
        min_buckets = 50
        if self.bucket_size == "seconds":
            min_duration = min_buckets
        elif self.bucket_size == "minutes":
            min_duration = min_buckets * 60  # 50 minutes minimum
        elif self.bucket_size == "hours":
            min_duration = min_buckets * 60 * 60  # 50 hours minimum
        elif self.bucket_size == "days":
            min_duration = min_buckets * 60 * 60 * 24  # 50 days minimum
        else:
            raise ValueError(f"Invalid bucket_size: {self.bucket_size}")

        if self.timeline_duration < min_duration:
            raise ValueError(
                f"Timeline duration ({self.timeline_duration}s) is too short for bucket size '{self.bucket_size}'. "
                f"Minimum duration required: {min_duration}s to ensure at least {min_buckets} buckets."
            )

    @property
    def retention_duration(self) -> int:
        """Get the retention duration for metrics data"""
        return int(self.timeline_duration * self.retention_multiplier)

    @property
    def bucket_duration(self) -> int:
        if self.bucket_size == "seconds":
            return 1
        elif self.bucket_size == "minutes":
            return 60
        elif self.bucket_size == "hours":
            return 3600
        elif self.bucket_size == "days":
            return 86400
        else:
            raise ValueError(f"Invalid bucket_size: {self.bucket_size}")

    @property
    def display_name(self) -> str:
        """Get a human-readable display name for the timeline"""
        if self.timeline_duration < 3600:
            minutes = self.timeline_duration // 60
            return f"{minutes}m"
        elif self.timeline_duration < 86400:
            hours = self.timeline_duration // 3600
            return f"{hours}h"
        else:
            days = self.timeline_duration // 86400
            return f"{days}d"


@dataclass
class MaintenanceWorkerConfig:
    """Configuration for the maintenance worker that handles both stale recovery and garbage collection"""

    interval: int = 10
    workers: int = 1
    task_ttl: TaskTTLConfig = field(default_factory=TaskTTLConfig)
    max_cleanup_batch: int = 100  # Maximum tasks to clean up per queue per run
    metrics_timeline: MetricsTimelineConfig = field(
        default_factory=MetricsTimelineConfig
    )


@dataclass
class ObservabilityConfig:
    """Configuration for the observability dashboard"""

    enabled: bool = True
    host: str = "0.0.0.0"
    port: int = 8080
    cors_origins: list[str] = field(default_factory=lambda: ["*"])
    dashboard_name: str | None = None


class Runner:
    def __init__(
        self,
        redis_pool: redis.ConnectionPool,
        llm_client: MultiClient,
        agent: BaseAgent[Any],
        metrics_config: MetricsTimelineConfig,
        agent_worker_config: AgentWorkerConfig,
        maintenance_worker_config: MaintenanceWorkerConfig,
        namespace: str,
    ):
        agent.client = agent.client or llm_client

        self.shutdown_event = asyncio.Event()
        self.redis_pool = redis_pool
        self.llm_client = llm_client
        self.agent = agent
        self.queue = to_snake_case(agent.__class__.__name__)
        self.metrics_config = metrics_config
        self.agent_worker_config = agent_worker_config
        self.maintenance_worker_config = maintenance_worker_config
        self.namespace = namespace

    def set_shutdown_event(self, shutdown_event: asyncio.Event):
        self.shutdown_event = shutdown_event

    def create_worker_tasks(
        self, shutdown_event: asyncio.Event
    ) -> list[asyncio.Task[Any]]:
        heartbeat_timeout = (
            self.agent_worker_config.heartbeat_interval
            * self.agent_worker_config.missed_heartbeats_threshold
            + self.agent_worker_config.missed_heartbeats_grace_period
        )

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
                    metrics_bucket_duration=self.metrics_config.bucket_duration,
                    metrics_retention_duration=self.metrics_config.retention_duration,
                    namespace=self.namespace,
                )
            )
            for i in range(self.agent_worker_config.workers)
        ] + [
            asyncio.create_task(
                maintenance_loop(
                    shutdown_event=shutdown_event,
                    redis_pool=self.redis_pool,
                    agent=self.agent,
                    heartbeat_timeout=heartbeat_timeout,
                    max_retries=self.agent_worker_config.max_retries,
                    batch_size=self.agent_worker_config.batch_size,
                    interval=self.maintenance_worker_config.interval,
                    task_ttl_config=self.maintenance_worker_config.task_ttl,
                    max_cleanup_batch=self.maintenance_worker_config.max_cleanup_batch,
                    metrics_bucket_duration=self.maintenance_worker_config.metrics_timeline.bucket_duration,
                    metrics_retention_duration=self.maintenance_worker_config.metrics_timeline.retention_duration,
                    namespace=self.namespace,
                )
            )
            for _ in range(self.maintenance_worker_config.workers)
        ]


class Orchestrator:
    def __init__(
        self,
        redis_pool: redis.ConnectionPool | None = None,
        redis_host: str | None = None,
        redis_port: int = 6379,
        redis_db: int = 0,
        redis_max_connections: int = 50,
        openai_api_key: str | None = None,
        xai_api_key: str | None = None,
        observability_config: ObservabilityConfig = ObservabilityConfig(),
        metrics_config: MetricsTimelineConfig = MetricsTimelineConfig(),
        namespace: str | None = None,
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

        self.api_keys = {
            "openai_api_key": openai_api_key or os.getenv("OPENAI_API_KEY"),
            "xai_api_key": xai_api_key or os.getenv("XAI_API_KEY"),
        }
        self.runners: list[Runner] = []
        self.observability_config = observability_config
        self.metrics_config = metrics_config
        self._agents_by_name: dict[str, BaseAgent[Any]] = {}
        self.namespace = namespace or "factorial"

        if self.observability_config.dashboard_name is None:
            self.observability_config.dashboard_name = (
                f"{self.namespace.title()} Dashboard"
            )

    @property
    def namespace(self) -> str:
        return self._namespace

    @namespace.setter
    def namespace(self, value: str):
        self._namespace = value

    def get_updates_channel(self, owner_id: str) -> str:
        return UPDATES_CHANNEL.format(namespace=self.namespace, owner_id=owner_id)

    @asynccontextmanager
    async def _pubsub_context(self, owner_id: str):
        """Context manager for Redis pubsub with proper cleanup"""
        redis_client = await self.get_redis_client()
        pubsub = redis_client.pubsub()
        channel = self.get_updates_channel(owner_id=owner_id)

        try:
            await pubsub.subscribe(channel)
            yield redis_client, pubsub, channel
        finally:
            await pubsub.unsubscribe(channel)
            await pubsub.aclose()
            await redis_client.close()

    async def subscribe_to_updates(
        self,
        owner_id: str,
        timeout: float = 5.0,
        ignore_subscribe_messages: bool = True,
        task_ids: list[str] | None = None,
        event_types: list[str] | None = None,
        event_pattern: str | None = None,
    ) -> AsyncGenerator[dict[str, Any], None]:
        """
        Subscribe to updates for a specific owner_id and yield messages as they arrive.

        Args:
            owner_id: The owner ID to subscribe to updates for
            timeout: Timeout in seconds for waiting for messages (default: 5.0)
            ignore_subscribe_messages: Whether to ignore Redis subscription confirmation messages
            task_ids: Optional list of task IDs to filter for (only events for these tasks)
            event_types: Optional list of event types to filter for (e.g., ["run_completed", "run_failed"])
            event_pattern: Optional regex pattern to match against event_type (e.g., "run_.*" for all run events)

        Yields:
            dict: Parsed JSON message data from the updates channel (filtered based on criteria)

        Examples:
            # All updates for a user
            async for update in orchestrator.subscribe_to_updates(owner_id="user123"):
                print(f"Received update: {update}")

            # Only completion and failure events
            async for update in orchestrator.subscribe_to_updates(
                owner_id="user123",
                event_types=["run_completed", "run_failed"]
            ):
                print(f"Task finished: {update}")

            # Only events for specific tasks
            async for update in orchestrator.subscribe_to_updates(
                owner_id="user123",
                task_ids=["task-123", "task-456"]
            ):
                print(f"Specific task update: {update}")

            # All progress events using pattern
            async for update in orchestrator.subscribe_to_updates(
                owner_id="user123",
                event_pattern=r"progress_update_.*"
            ):
                print(f"Progress: {update}")
        """
        import json
        import re

        # Compile regex pattern if provided
        compiled_pattern = re.compile(event_pattern) if event_pattern else None

        def should_include_event(event_data: dict[str, Any]) -> bool:
            """Check if event matches the filter criteria"""

            # Filter by task_ids
            if task_ids is not None:
                event_task_id = event_data.get("task_id")
                if event_task_id not in task_ids:
                    return False

            # Filter by event_types
            if event_types is not None:
                event_type = event_data.get("event_type")
                if event_type not in event_types:
                    return False

            # Filter by event_pattern
            if compiled_pattern is not None:
                event_type = event_data.get("event_type", "")
                if not compiled_pattern.match(event_type):
                    return False

            return True

        async with self._pubsub_context(owner_id) as (redis_client, pubsub, channel):
            while True:
                try:
                    msg = await pubsub.get_message(
                        ignore_subscribe_messages=ignore_subscribe_messages,
                        timeout=timeout,
                    )

                    if msg and msg["type"] == "message":
                        data = msg["data"]
                        if isinstance(data, bytes):
                            data = data.decode("utf-8")

                        try:
                            event_data = json.loads(data)

                            # Apply filters
                            if should_include_event(event_data):
                                yield {"data": event_data}

                        except json.JSONDecodeError:
                            # If it's not valid JSON, yield raw message only if no filters
                            if (
                                task_ids is None
                                and event_types is None
                                and event_pattern is None
                            ):
                                yield {"raw": data}

                except asyncio.TimeoutError:
                    # Timeout is expected, continue listening
                    continue
                except Exception as e:
                    # Log error but don't break the loop
                    logger.error(f"Error receiving message from channel {channel}: {e}")
                    continue

    def register_runner(
        self,
        agent: BaseAgent[Any],
        agent_worker_config: AgentWorkerConfig = AgentWorkerConfig(),
        maintenance_worker_config: MaintenanceWorkerConfig = MaintenanceWorkerConfig(),
    ):
        num_connections = agent_worker_config.workers * agent_worker_config.batch_size
        http_client = httpx.AsyncClient(
            limits=httpx.Limits(
                max_connections=num_connections * 1.25,
                max_keepalive_connections=num_connections,
            ),
            timeout=httpx.Timeout(agent.request_timeout),
        )
        llm_client = MultiClient(
            http_client=http_client,
            **self.api_keys,
        )

        runner = Runner(
            redis_pool=self.redis_pool,
            llm_client=llm_client,
            agent=agent,
            agent_worker_config=agent_worker_config,
            maintenance_worker_config=maintenance_worker_config,
            metrics_config=self.metrics_config,
            namespace=self.namespace,
        )

        runner.set_shutdown_event(self.shutdown_event)
        self.runners.append(runner)
        self._agents_by_name[agent.name] = agent

    def get_agent(self, agent_name: str) -> BaseAgent[Any] | None:
        """Get an agent by name"""
        return self._agents_by_name.get(agent_name)

    async def get_redis_client(self) -> redis.Redis:
        """Get a Redis client from the pool"""
        return redis.Redis(connection_pool=self.redis_pool, decode_responses=True)

    async def enqueue_task(
        self,
        agent: BaseAgent[Any],
        task: Task[ContextType],
    ) -> None:
        """Enqueue a task using the control plane's configuration"""
        from factorial.queue import enqueue_task as q_enqueue_task

        redis_client = await self.get_redis_client()
        try:
            await q_enqueue_task(
                redis_client=redis_client,
                namespace=self.namespace,
                agent=agent,
                task=task,
            )
        finally:
            await redis_client.close()

    async def cancel_task(
        self,
        task_id: str,
    ) -> None:
        """Cancel a task using the control plane's configuration"""
        from factorial.queue import cancel_task as q_cancel_task

        redis_client = await self.get_redis_client()
        try:
            await q_cancel_task(
                redis_client=redis_client,
                namespace=self.namespace,
                task_id=task_id,
                metrics_bucket_duration=self.metrics_config.bucket_duration,
                metrics_retention_duration=self.metrics_config.retention_duration,
            )
        finally:
            await redis_client.close()

    async def steer_task(
        self,
        task_id: str,
        messages: list[dict[str, Any]],
    ) -> None:
        """Steer a task using the control plane's configuration"""
        from factorial.queue import steer_task as q_steer_task

        redis_client = await self.get_redis_client()
        try:
            await q_steer_task(
                redis_client=redis_client,
                namespace=self.namespace,
                task_id=task_id,
                messages=messages,
            )
        finally:
            await redis_client.close()

    async def complete_deferred_tool(
        self,
        task_id: str,
        tool_call_id: str,
        result: Any,
    ) -> bool:
        """Complete a deferred tool call

        Args:
            task_id: The ID of the task containing the deferred tool
            tool_call_id: The ID of the specific tool call to complete
            result: The result to provide for the tool call

        Returns:
            bool: True if the tool was completed successfully, False otherwise

        Raises:
            TaskNotFoundError: If the task doesn't exist
            InactiveTaskError: If the task is not active (completed, failed, or cancelled)
        """
        from factorial.queue import complete_deferred_tool, get_task_agent

        redis_client = await self.get_redis_client()
        try:
            # Get the agent for this task
            agent_name = await get_task_agent(redis_client, self.namespace, task_id)
            agent = self.get_agent(agent_name)

            if not agent:
                raise ValueError(f"Agent {agent_name} not found in orchestrator")

            return await complete_deferred_tool(
                redis_client=redis_client,
                namespace=self.namespace,
                agent=agent,
                task_id=task_id,
                tool_call_id=tool_call_id,
                result=result,
            )
        finally:
            await redis_client.close()

    async def get_task_status(self, task_id: str) -> Any:
        """Get task status using the control plane's configuration"""
        from factorial.queue import get_task_status as q_get_task_status

        redis_client = await self.get_redis_client()
        try:
            return await q_get_task_status(
                redis_client=redis_client, namespace=self.namespace, task_id=task_id
            )
        finally:
            await redis_client.close()

    async def get_task_data(self, task_id: str) -> dict[str, Any] | None:
        """Get task data using the control plane's configuration"""
        from factorial.queue import get_task_data as q_get_task_data

        redis_client = await self.get_redis_client()
        try:
            return await q_get_task_data(
                redis_client=redis_client, namespace=self.namespace, task_id=task_id
            )
        finally:
            await redis_client.close()

    async def get_task_agent(self, task_id: str) -> BaseAgent[Any] | None:
        """Get the agent that owns a specific task"""
        task_data = await self.get_task_data(task_id)
        if not task_data:
            return None

        agent_name = task_data.get("agent")
        if not agent_name:
            return None

        return self.get_agent(agent_name)

    def create_observability_app(self) -> FastAPI:
        """Create the observability FastAPI app (minimal, clean, robust)"""
        from factorial.dashboard.routes import add_observability_routes

        dashboard_name = (
            self.observability_config.dashboard_name
            or f"{self.namespace.title()} Dashboard"
        )

        app = FastAPI(
            title=dashboard_name,
            description="Minimal real-time dashboard for the Factorial orchestrator",
            version="1.0.0",
            redoc_url=None,
            docs_url=None,
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

        # Get metrics config from the first runner (assuming all runners have the same metrics config)
        metrics_config = None
        if self.runners:
            metrics_config = self.runners[0].maintenance_worker_config.metrics_timeline

        add_observability_routes(
            app=app,
            redis_client=redis_client,
            agents=agents,
            runners=self.runners,
            metrics_config=metrics_config,
            dashboard_name=dashboard_name,
            namespace=self.namespace,
        )

        @app.get("/")
        async def root():
            return {
                "message": "Observability dashboard",
                "dashboard": "/observability",
                "port": self.observability_config.port,
            }

        return app

    def start_observability_server(self) -> None:
        """Start the observability web server"""
        import uvicorn

        app = self.create_observability_app()
        logger.info(
            f"🌐 Observability dashboard available at http://{self.observability_config.host}:{self.observability_config.port}/observability"
        )
        uvicorn.run(
            app,
            host=self.observability_config.host,
            port=self.observability_config.port,
            log_level="info",
            access_log=False,  # Reduce noise
        )

    async def start_async_observability_server(self) -> None:
        """Start the observability web server asynchronously"""
        import uvicorn

        app = self.create_observability_app()
        config = uvicorn.Config(
            app,
            host=self.observability_config.host,
            port=self.observability_config.port,
            log_level="info",
            access_log=False,  # Reduce noise
        )
        server = uvicorn.Server(config)
        await server.serve()

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
                for task in done:
                    try:
                        # This will raise the exception if the task failed
                        task.result()
                    except asyncio.CancelledError:
                        logger.info(f"Worker {task.get_name()} cancelled")
                    except Exception as e:
                        logger.error(
                            f"Worker exited unexpectedly with error: {e}", exc_info=True
                        )
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
            for runner in self.runners:
                if hasattr(runner.llm_client, "close"):
                    await runner.llm_client.close()

            await self.redis_pool.disconnect()
            logger.info("Redis connection pool closed")

    def run(self, run_observability_server: bool = True):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, self.shutdown_event.set)

        try:
            # Run both workers and observability server concurrently
            if run_observability_server:
                observability_task = loop.create_task(
                    self.start_async_observability_server()
                )
                loop.run_until_complete(
                    asyncio.gather(
                        self.start_workers(self.shutdown_event), observability_task
                    )
                )
            else:
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
