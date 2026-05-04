import asyncio
import json
import os
import signal
import weakref
from collections.abc import AsyncGenerator, AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Literal, cast

import httpx
import redis.asyncio as redis
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from factorial._internal.compat import resolve_awaitable
from factorial._internal.orchestrator.runner import Runner
from factorial._internal.orchestrator.runtime import (
    build_wake_dispatch,
    default_maintenance_reason,
    resolve_runtime_mode,
    resolve_wake_transport,
)
from factorial._internal.queue.keys import RedisKeys
from factorial.agent import BaseAgent
from factorial.agent.context import ContextType
from factorial.ai.messages import Message, normalize_messages_input, system
from factorial.ai.models import MultiClient
from factorial.core.exceptions import (
    BatchNotFoundError,
    InactiveTaskError,
    TaskNotFoundError,
)
from factorial.core.logging import get_logger
from factorial.core.run_types import RunResult, RunStatus, UsageSummary
from factorial.execution.hooks import HookRecord, HookResolutionResult, PendingHook
from factorial.orchestrator.config import (
    AgentWorkerConfig,
    MaintenanceWorkerConfig,
    MetricsTimelineConfig,
    ObservabilityConfig,
    TaskTTLConfig as TaskTTLConfig,
)
from factorial.orchestrator.handles import (
    BatchHandle,
    BatchSnapshot,
    HookMode,
    InputWithContext,
    PendingHookSnapshot,
    TaskHandle,
    TaskSnapshot,
    TaskSnapshotStatus,
    WaitKind,
    WaitSnapshot,
)
from factorial.orchestrator.messaging import OrchestratorMessagingNamespace
from factorial.queue import Task, TaskStatus

from .wake_dispatch import WakeDispatch

logger = get_logger(__name__)

if TYPE_CHECKING:
    from factorial.platforms.vercel import VercelRuntimeSettings
    from factorial.queue.task import Batch


class Orchestrator:
    def __init__(
        self,
        redis_pool: redis.ConnectionPool | None = None,
        redis_host: str | None = None,
        redis_port: int | None = None,
        redis_db: int | None = None,
        redis_max_connections: int | None = None,
        openai_api_key: str | None = None,
        xai_api_key: str | None = None,
        observability_config: ObservabilityConfig | None = None,
        metrics_config: MetricsTimelineConfig | None = None,
        namespace: str | None = None,
        runtime_mode: Literal["process", "vercel"] | None = None,
        wake_transport: Literal["none", "vercel_queue"] | None = None,
        wake_dispatch: WakeDispatch | None = None,
    ):
        self.shutdown_event = asyncio.Event()

        if observability_config is None:
            observability_config = ObservabilityConfig()
        if metrics_config is None:
            metrics_config = MetricsTimelineConfig()

        resolved_redis_port = (
            redis_port
            if redis_port is not None
            else int(os.getenv("REDIS_PORT", "6379"))
        )
        resolved_redis_db = (
            redis_db if redis_db is not None else int(os.getenv("REDIS_DB", "0"))
        )
        resolved_redis_max_connections = (
            redis_max_connections
            if redis_max_connections is not None
            else int(os.getenv("REDIS_MAX_CONNECTIONS", "50"))
        )

        if redis_pool:
            self.redis_pool = redis_pool
        else:
            redis_url = os.getenv("REDIS_URL") or os.getenv("UPSTASH_REDIS_URL")
            if redis_url:
                if not redis_url.startswith(("redis://", "rediss://")):
                    raise RuntimeError(
                        "REDIS_URL must be a redis:// or rediss:// connection string. "
                        "REST endpoints are not supported by redis-py."
                    )
                self.redis_pool = redis.ConnectionPool.from_url(
                    redis_url,
                    max_connections=resolved_redis_max_connections,
                )
            else:
                resolved_redis_host = redis_host or os.getenv("REDIS_HOST")
                self.redis_pool = redis.ConnectionPool(
                    host=resolved_redis_host or "localhost",
                    port=resolved_redis_port,
                    db=resolved_redis_db,
                    max_connections=resolved_redis_max_connections,
                )
        self._redis_pool_connection_class = getattr(
            self.redis_pool, "connection_class", None
        )
        self._redis_pool_connection_kwargs = dict(
            getattr(self.redis_pool, "connection_kwargs", {})
        )
        self._redis_pool_max_connections = getattr(
            self.redis_pool,
            "max_connections",
            resolved_redis_max_connections,
        )
        self._loop_redis_pools: weakref.WeakKeyDictionary[
            asyncio.AbstractEventLoop, redis.ConnectionPool
        ] = weakref.WeakKeyDictionary()
        self._loop_scoped_pool_disabled = False

        self.api_keys = {
            "openai_api_key": openai_api_key or os.getenv("OPENAI_API_KEY"),
            "xai_api_key": xai_api_key or os.getenv("XAI_API_KEY"),
        }
        self.runners: list[Runner] = []
        self.observability_config = observability_config
        self.metrics_config = metrics_config
        self.agents_by_name: dict[str, BaseAgent[Any]] = {}
        self.namespace = namespace or "factorial"
        self.runtime_mode = resolve_runtime_mode(runtime_mode)
        self.wake_transport = resolve_wake_transport(
            runtime_mode=self.runtime_mode,
            wake_transport=wake_transport,
        )
        self.wake_dispatch: WakeDispatch = (
            wake_dispatch
            if wake_dispatch is not None
            else build_wake_dispatch(
                wake_transport=self.wake_transport,
                dispatch_topic=os.getenv(
                    "NFACTORIAL_DISPATCH_TOPIC",
                    "nfactorial-dispatch",
                ),
                namespace=self.namespace,
            )
        )
        self.messaging = OrchestratorMessagingNamespace(self)

        if self.observability_config.dashboard_name is None:
            self.observability_config.dashboard_name = (
                f"{self.namespace.title()} Dashboard"
            )

    @property
    def namespace(self) -> str:
        return self._namespace

    @namespace.setter
    def namespace(self, value: str) -> None:
        self._namespace = value

    @property
    def agents(self) -> list[BaseAgent[Any]]:
        return list(self.agents_by_name.values())

    @agents.setter
    def agents(self, value: list[BaseAgent[Any]]) -> None:
        self.agents_by_name = {agent.name: agent for agent in value}

    def get_updates_channel(self, owner_id: str) -> str:
        return RedisKeys.for_owner(
            namespace=self.namespace, owner_id=owner_id
        ).updates_channel

    @asynccontextmanager
    async def _pubsub_context(
        self, owner_id: str
    ) -> AsyncIterator[tuple[redis.Redis, Any, str]]:
        """Context manager for Redis pubsub with proper cleanup"""
        async with self.redis_client_context() as redis_client:
            pubsub = redis_client.pubsub()
            channel = self.get_updates_channel(owner_id=owner_id)
            try:
                await pubsub.subscribe(channel)
                yield redis_client, pubsub, channel
            finally:
                await pubsub.unsubscribe(channel)
                await pubsub.aclose()

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
            ignore_subscribe_messages: Whether to ignore Redis subscription
                confirmation messages
            task_ids: Optional list of task IDs to filter for (only events
                for these tasks)
            event_types: Optional list of event types to filter for
                (e.g., ["run_completed", "run_failed"])
            event_pattern: Optional regex pattern to match against event_type
                (e.g., "run_.*" for all run events)

        Yields:
            dict: Parsed JSON message data from the updates channel
                (filtered based on criteria)

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
                                yield event_data

                        except json.JSONDecodeError:
                            logger.error(f"Error decoding JSON: {data}")
                            continue

                except asyncio.TimeoutError:
                    # Timeout is expected, continue listening
                    continue
                except Exception as e:
                    # Log error but don't break the loop
                    logger.error(f"Error receiving message from channel {channel}: {e}")
                    continue

    def _register_agent_runner(
        self,
        agent: BaseAgent[Any],
        agent_worker_config: AgentWorkerConfig | None = None,
        maintenance_worker_config: MaintenanceWorkerConfig | None = None,
    ) -> None:
        if agent_worker_config is None:
            agent_worker_config = AgentWorkerConfig()
        if maintenance_worker_config is None:
            maintenance_worker_config = MaintenanceWorkerConfig()
        num_connections = agent_worker_config.workers * agent_worker_config.batch_size
        http_client = httpx.AsyncClient(
            limits=httpx.Limits(
                max_connections=int(num_connections * 1.25),
                max_keepalive_connections=num_connections,
            ),
            timeout=httpx.Timeout(agent.request_timeout),
        )
        llm_client = MultiClient(
            http_client=http_client,
            openai_api_key=self.api_keys.get("openai_api_key"),
            xai_api_key=self.api_keys.get("xai_api_key"),
            anthropic_api_key=self.api_keys.get("anthropic_api_key"),
            fireworks_api_key=self.api_keys.get("fireworks_api_key"),
            ai_gateway_api_key=self.api_keys.get("ai_gateway_api_key"),
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
        self.agents_by_name[agent.name] = agent

    def get_agent(self, agent_name: str) -> BaseAgent[Any] | None:
        """Get an agent by name"""
        return self.agents_by_name.get(agent_name)

    def _resolve_agent(self, agent: str | BaseAgent[Any]) -> BaseAgent[Any]:
        if isinstance(agent, BaseAgent):
            return agent
        resolved = self.get_agent(agent)
        if resolved is None:
            raise ValueError(
                f"Agent '{agent}' is not registered. Register it before enqueueing."
            )
        return resolved

    def register(
        self,
        agent: BaseAgent[Any],
        *,
        agent_worker_config: AgentWorkerConfig | None = None,
        maintenance_worker_config: MaintenanceWorkerConfig | None = None,
    ) -> BaseAgent[Any]:
        self._register_agent_runner(
            agent=agent,
            agent_worker_config=agent_worker_config,
            maintenance_worker_config=maintenance_worker_config,
        )
        return agent

    async def wake_agent(
        self,
        *,
        agent_name: str,
        reason: str,
        task_id: str | None = None,
    ) -> bool:
        try:
            await self.wake_dispatch.wake_agent(
                agent_name=agent_name,
                reason=reason,
                task_id=task_id,
            )
            return True
        except Exception as exc:
            logger.error(
                "Failed to dispatch wake for agent=%s reason=%s task_id=%s",
                agent_name,
                reason,
                task_id,
                exc_info=exc,
            )
            return False

    async def wake_agents(self, *, agent_names: list[str], reason: str) -> bool:
        try:
            await self.wake_dispatch.wake_agents(agent_names=agent_names, reason=reason)
            return True
        except Exception as exc:
            logger.error(
                "Failed to dispatch wakes for agents=%s reason=%s",
                agent_names,
                reason,
                exc_info=exc,
            )
            return False

    async def wake_maintenance(
        self,
        *,
        reason: str,
        delay_seconds: int | None = None,
        idempotency_key: str | None = None,
        retention_seconds: int | None = None,
    ) -> bool:
        try:
            await self.wake_dispatch.wake_maintenance(
                reason=reason,
                delay_seconds=delay_seconds,
                idempotency_key=idempotency_key,
                retention_seconds=retention_seconds,
            )
            return True
        except Exception as exc:
            logger.error(
                "Failed to dispatch maintenance wake reason=%s",
                reason,
                exc_info=exc,
            )
            return False

    async def run_maintenance_tick(
        self,
        *,
        reason: str | None = None,
        settings: "VercelRuntimeSettings | None" = None,
    ) -> dict[str, Any]:
        """Run one maintenance trigger/invocation using Vercel runtime semantics."""
        from factorial.platforms.vercel import (
            VercelRuntimeSettings,
            configure_orchestrator,
            trigger_maintenance_once,
        )

        runtime_settings = settings or VercelRuntimeSettings.from_env()
        configure_orchestrator(self, settings=runtime_settings)
        resolved_reason = reason or default_maintenance_reason()
        return await trigger_maintenance_once(
            orchestrator=self,
            settings=runtime_settings,
            reason=resolved_reason,
        )

    async def run_maintenance_cron_tick(
        self,
        *,
        settings: "VercelRuntimeSettings | None" = None,
    ) -> dict[str, Any]:
        """Run one maintenance tick with explicit cron reason semantics."""
        return await self.run_maintenance_tick(
            reason="cron_schedule",
            settings=settings,
        )

    def bootstrap_vercel_worker_app(
        self,
        *,
        settings: "VercelRuntimeSettings | None" = None,
    ) -> Any:
        """Return the Vercel worker callback app for this orchestrator."""
        from factorial.platforms.vercel import (
            VercelRuntimeSettings,
            create_worker,
        )

        runtime_settings = settings or VercelRuntimeSettings.from_env()
        return create_worker(self, settings=runtime_settings)

    def create_app(
        self,
        *,
        enable_ws: bool = False,
        cors_origins: list[str] | None = None,
    ) -> FastAPI:
        """Create an ASGI app for orchestrator control-plane APIs."""
        from factorial.api.app import create_control_plane_app

        return create_control_plane_app(
            self,
            enable_ws=enable_ws,
            cors_origins=cors_origins,
        )

    async def _wake_task_if_possible(self, *, task_id: str, reason: str) -> None:
        task_data = await self.get_task_data(task_id)
        if not task_data:
            return
        agent_name = task_data.get("agent")
        if isinstance(agent_name, str) and agent_name:
            await self.wake_agent(agent_name=agent_name, reason=reason, task_id=task_id)

    def _get_loop_scoped_redis_pool(
        self, loop: asyncio.AbstractEventLoop
    ) -> redis.ConnectionPool:
        if self._loop_scoped_pool_disabled:
            return self.redis_pool

        existing_pool = self._loop_redis_pools.get(loop)
        if existing_pool is not None:
            return existing_pool

        pool_kwargs = dict(self._redis_pool_connection_kwargs)
        pool_kwargs["decode_responses"] = True
        if self._redis_pool_connection_class is not None:
            pool_kwargs["connection_class"] = self._redis_pool_connection_class
        pool_kwargs["max_connections"] = self._redis_pool_max_connections

        try:
            loop_pool = redis.ConnectionPool(**pool_kwargs)
        except Exception as exc:
            self._loop_scoped_pool_disabled = True
            logger.warning(
                "Failed to create loop-scoped Redis pool; falling back to shared pool.",
                exc_info=exc,
            )
            return self.redis_pool
        self._loop_redis_pools[loop] = loop_pool
        return loop_pool

    async def _disconnect_loop_redis_pools(self) -> None:
        if not self._loop_redis_pools:
            return

        pools = list(self._loop_redis_pools.values())
        self._loop_redis_pools.clear()
        await asyncio.gather(
            *[pool.disconnect() for pool in pools],
            return_exceptions=True,
        )

    async def get_redis_client(self) -> redis.Redis:
        """Get a Redis client from the pool"""
        if self.runtime_mode == "vercel":
            loop = asyncio.get_running_loop()
            loop_pool = self._get_loop_scoped_redis_pool(loop)
            return redis.Redis(connection_pool=loop_pool, decode_responses=True)
        return redis.Redis(connection_pool=self.redis_pool, decode_responses=True)

    @asynccontextmanager
    async def redis_client_context(self) -> AsyncIterator[redis.Redis]:
        """Yield a Redis client and always close it."""
        redis_client = await self.get_redis_client()
        try:
            yield redis_client
        finally:
            await redis_client.close()

    def _task_handle_from_task(
        self,
        task: Task[Any],
    ) -> TaskHandle[Any, Any, Any]:
        return TaskHandle(
            orchestrator=self,
            task_id=task.id,
            agent_name=task.agent,
            owner_id=task.metadata.owner_id,
            batch_id=task.metadata.batch_id,
        )

    async def enqueue(
        self,
        agent: str | BaseAgent[Any],
        input: str | list[Any],
        *,
        owner_id: str,
        state: Any = None,
        metadata: Any = None,
        idempotency_key: str | None = None,
    ) -> TaskHandle[Any, Any, Any]:
        resolved_agent = self._resolve_agent(agent)
        payload = resolved_agent.build_context(
            input=input,
            state=state,
            metadata=metadata,
        )
        task = Task.create(
            owner_id=owner_id,
            agent=resolved_agent.name,
            payload=payload,
            max_turns=resolved_agent.max_turns,
        )
        task.id = await self.enqueue_task(
            resolved_agent,
            task,
            idempotency_key=idempotency_key,
        )
        return self._task_handle_from_task(task)

    async def enqueue_many(
        self,
        agent: str | BaseAgent[Any],
        inputs: list[str | list[Any] | InputWithContext[Any, Any]],
        *,
        owner_id: str,
        state: Any = None,
        metadata: Any = None,
        idempotency_key: str | None = None,
    ) -> BatchHandle[Any, Any, Any]:
        if not inputs:
            raise ValueError("enqueue_many requires at least one input")

        resolved_agent = self._resolve_agent(agent)
        tasks: list[Task[Any]] = []
        for item in inputs:
            if isinstance(item, InputWithContext):
                item_input = item.input
                item_state = item.state if item.state is not None else state
                item_metadata = item.metadata if item.metadata is not None else metadata
            else:
                item_input = item
                item_state = state
                item_metadata = metadata

            payload = resolved_agent.build_context(
                input=cast(str | list[Message], item_input),
                state=item_state,
                metadata=item_metadata,
            )
            tasks.append(
                Task.create(
                    owner_id=owner_id,
                    agent=resolved_agent.name,
                    payload=payload,
                    max_turns=resolved_agent.max_turns,
                )
            )

        batch = await self.enqueue_batch(
            resolved_agent,
            tasks,
            idempotency_key=idempotency_key,
        )
        return BatchHandle(
            orchestrator=self,
            batch_id=batch.id,
            agent_name=resolved_agent.name,
            owner_id=owner_id,
            task_ids=tuple(batch.task_ids),
        )

    async def enqueue_task(
        self,
        agent: BaseAgent[Any],
        task: Task[ContextType],
        idempotency_key: str | None = None,
    ) -> str:
        """Enqueue a task using the control plane's configuration"""
        from factorial._internal.queue.operations import enqueue_task as q_enqueue_task

        async with self.redis_client_context() as redis_client:
            task_id = await q_enqueue_task(
                redis_client=redis_client,
                namespace=self.namespace,
                agent=agent,
                task=task,
                idempotency_key=idempotency_key,
            )
            await self.wake_agent(
                agent_name=agent.name,
                reason="enqueue",
                task_id=task_id,
            )
            return task_id

    async def enqueue_batch(
        self,
        agent: BaseAgent[Any],
        tasks: list[Task[ContextType]],
        idempotency_key: str | None = None,
    ) -> "Batch":
        """Create and enqueue a batch using task objects."""
        from factorial._internal.queue.operations import (
            create_batch_and_enqueue as q_create_batch_and_enqueue,
        )

        if not tasks:
            raise ValueError("enqueue_batch requires at least one task")

        task_agents = {task.agent for task in tasks}
        if task_agents != {agent.name}:
            raise ValueError(
                "enqueue_batch requires all tasks to match the provided agent"
            )

        owner_ids = {task.metadata.owner_id for task in tasks}
        if len(owner_ids) != 1:
            raise ValueError(
                "enqueue_batch requires all tasks to have the same owner_id"
            )

        parent_ids = {task.metadata.parent_id for task in tasks}
        if len(parent_ids) != 1:
            raise ValueError(
                "enqueue_batch requires all tasks to have the same parent_id"
            )
        team_ids = {task.metadata.team_id for task in tasks}
        if len(team_ids) != 1:
            raise ValueError(
                "enqueue_batch requires all tasks to have the same team_id"
            )

        payloads = [task.payload for task in tasks]
        owner_id = tasks[0].metadata.owner_id
        parent_id = tasks[0].metadata.parent_id
        team_id = tasks[0].metadata.team_id

        task_ids: list[str] | None = None
        if idempotency_key is None:
            # Preserve caller-provided task ids for non-idempotent batches.
            task_ids = [task.id for task in tasks]

        async with self.redis_client_context() as redis_client:
            batch = await q_create_batch_and_enqueue(
                redis_client=redis_client,
                namespace=self.namespace,
                agent=agent,
                payloads=payloads,
                owner_id=owner_id,
                parent_id=parent_id,
                team_id=team_id,
                task_ids=task_ids,
                idempotency_key=idempotency_key,
            )
            await self.wake_agent(agent_name=agent.name, reason="enqueue_batch")
            return batch

    async def resume_task(
        self,
        task_id: str,
        messages: list[dict[str, Any]],
        idempotency_key: str | None = None,
    ) -> Task[Any]:
        """Resume a terminal task as a new queued task."""
        from factorial._internal.queue.operations import resume_task as q_resume_task
        from factorial._internal.queue.task_store import (
            get_task_data as q_get_task_data,
        )

        async with self.redis_client_context() as redis_client:
            source_task_data = await q_get_task_data(
                redis_client=redis_client,
                namespace=self.namespace,
                task_id=task_id,
            )
            source_agent_name = source_task_data["agent"]
            source_agent = self.agents_by_name.get(source_agent_name)
            if source_agent is None:
                raise ValueError(
                    f"Task {task_id} belongs to unregistered agent "
                    f"'{source_agent_name}'. Register the agent before resuming."
                )

            resumed_task = await q_resume_task(
                redis_client=redis_client,
                namespace=self.namespace,
                task_id=task_id,
                agent=source_agent,
                messages=messages,
                idempotency_key=idempotency_key,
            )
            await self.wake_agent(
                agent_name=source_agent_name,
                reason="resume",
                task_id=resumed_task.id,
            )
            return resumed_task

    async def branch_task(
        self,
        *,
        task_id: str,
        input: str | list[Any],
        state: Any = None,
        metadata: Any = None,
        idempotency_key: str | None = None,
    ) -> TaskHandle[Any, Any, Any]:
        source_task_data = await self.get_task_data(task_id)
        if source_task_data is None:
            raise TaskNotFoundError(task_id)

        source_status = TaskStatus(source_task_data["status"])
        if source_status not in {
            TaskStatus.COMPLETED,
            TaskStatus.FAILED,
            TaskStatus.CANCELLED,
        }:
            raise ValueError("branch() requires a terminal task")

        source_agent_name = str(source_task_data["agent"])
        source_agent = self._resolve_agent(source_agent_name)
        source_payload = source_agent.context_from_dict(source_task_data["payload"])
        source_metadata = Task.from_dict(
            source_task_data,
            payload_parser=source_agent.context_from_dict,
        ).metadata

        payload = source_agent.build_context(
            input=input,
            state=source_payload.state if state is None else state,
            metadata=source_payload.metadata if metadata is None else metadata,
        )
        task = Task.create(
            owner_id=source_metadata.owner_id,
            agent=source_agent.name,
            payload=payload,
            max_turns=source_agent.max_turns,
            team_id=source_metadata.team_id,
        )
        task.metadata.parent_id = task_id
        task.id = await self.enqueue_task(
            source_agent,
            task,
            idempotency_key=idempotency_key,
        )
        return self._task_handle_from_task(task)

    async def cancel_task(
        self,
        task_id: str,
    ) -> None:
        """Cancel a task using the control plane's configuration"""
        from factorial._internal.queue.operations import cancel_task as q_cancel_task

        async with self.redis_client_context() as redis_client:
            await q_cancel_task(
                redis_client=redis_client,
                namespace=self.namespace,
                task_id=task_id,
                agents_by_name=self.agents_by_name,
                metrics_retention_duration=self.metrics_config.retention_duration,
            )

    async def cancel_batch(self, batch_id: str) -> None:
        from factorial._internal.queue.operations.control import (
            cancel_batch as q_cancel_batch,
        )

        async with self.redis_client_context() as redis_client:
            await q_cancel_batch(
                redis_client=redis_client,
                namespace=self.namespace,
                batch_id=batch_id,
                agents_by_name=self.agents_by_name,
                metrics_retention_duration=self.metrics_config.retention_duration,
            )

    async def steer_task(
        self,
        task_id: str,
        messages: list[dict[str, Any]],
    ) -> None:
        """Steer a task using the control plane's configuration"""
        from factorial._internal.queue.operations import steer_task as q_steer_task
        from factorial._internal.queue.task_store import (
            get_task_data as q_get_task_data,
        )

        async with self.redis_client_context() as redis_client:
            await q_steer_task(
                redis_client=redis_client,
                namespace=self.namespace,
                task_id=task_id,
                messages=messages,
            )
            task_data = await q_get_task_data(
                redis_client=redis_client,
                namespace=self.namespace,
                task_id=task_id,
            )
            agent_name = task_data.get("agent") if task_data else None
            if isinstance(agent_name, str) and agent_name:
                await self.wake_agent(
                    agent_name=agent_name,
                    reason="steer",
                    task_id=task_id,
                )

    async def steer_task_input(
        self,
        *,
        task_id: str,
        input: str | list[Any],
    ) -> None:
        await self.steer_task(
            task_id=task_id,
            messages=cast(
                list[dict[str, Any]],
                normalize_messages_input(cast(Any, input)),
            ),
        )

    def _manual_wake_messages(
        self,
        *,
        wait_kind: WaitKind,
        input: str | list[Any] | None = None,
    ) -> list[dict[str, Any]]:
        messages: list[dict[str, Any]] = [
            cast(
                dict[str, Any],
                system(
                    "Runtime note: task was manually resumed and interrupted "
                    f"a {wait_kind.value} wait."
                ),
            )
        ]
        if input is not None:
            messages.extend(
                cast(
                    list[dict[str, Any]],
                    normalize_messages_input(cast(Any, input)),
                )
            )
        return messages

    async def wake_task(
        self,
        *,
        task_id: str,
        input: str | list[Any] | None = None,
    ) -> bool:
        snapshot = await self.snapshot_task(task_id)
        if snapshot.status is TaskSnapshotStatus.BACKOFF:
            raise ValueError("wake() cannot resume tasks that are in backoff")
        if snapshot.status is not TaskSnapshotStatus.WAITING:
            raise ValueError("wake() requires a task that is currently waiting")
        if snapshot.pending_hooks:
            raise ValueError("wake() does not apply to pending hooks")
        if snapshot.pending_child_task_ids:
            raise ValueError("wake() does not apply to pending child tasks")
        if snapshot.wait is None:
            raise ValueError("wake() requires a wakeable wait")

        wake_messages = self._manual_wake_messages(
            wait_kind=snapshot.wait.kind,
            input=input,
        )
        if snapshot.wait.kind is WaitKind.SIGNAL:
            from factorial._internal.queue.operations import (
                signal_task as q_signal_task,
            )

            if snapshot.wait.signal_id is None:
                raise ValueError("signal waits require a pending signal_id")

            async with self.redis_client_context() as redis_client:
                try:
                    await q_signal_task(
                        redis_client=redis_client,
                        namespace=self.namespace,
                        sender_task_id=task_id,
                        task_id=task_id,
                        signal_id=snapshot.wait.signal_id,
                        payload={"kind": "manual_wake"},
                    )
                except (InactiveTaskError, TaskNotFoundError):
                    return False

            await self.steer_task(task_id=task_id, messages=wake_messages)
            return True

        try:
            await self.steer_task(task_id=task_id, messages=wake_messages)
        except (InactiveTaskError, TaskNotFoundError):
            return False
        return True

    async def message_task(
        self,
        *,
        task_id: str,
        owner_id: str,
        content: str,
        data: Any = None,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        from factorial._internal.queue.operations import (
            messaging_human_send_direct as q_message_task,
        )

        async with self.redis_client_context() as redis_client:
            return await q_message_task(
                redis_client=redis_client,
                namespace=self.namespace,
                owner_id=owner_id,
                to_task_id=task_id,
                content=content,
                data=data,
                metadata=metadata,
            )

    async def message_group(
        self,
        *,
        owner_id: str,
        content: str,
        data: Any = None,
        group_id: str | None = None,
        group_name: str | None = None,
        task_id: str | None = None,
        team_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        from factorial._internal.queue.operations import (
            messaging_human_send_group as q_message_group,
        )

        async with self.redis_client_context() as redis_client:
            return await q_message_group(
                redis_client=redis_client,
                namespace=self.namespace,
                owner_id=owner_id,
                content=content,
                data=data,
                group_id=group_id,
                group_name=group_name,
                task_id=task_id,
                team_id=team_id,
                metadata=metadata,
            )

    async def register_pending_hook(
        self,
        *,
        task_id: str,
        tool_call_id: str,
        pending_hook: PendingHook[Any],
        tool_name: str,
        hook_param_name: str,
        session_id: str,
        mode: Literal["requires", "awaits"] = "requires",
        tool_args: dict[str, Any] | None = None,
        depends_on: tuple[str, ...] | None = None,
        hook_type_name: str | None = None,
    ) -> bool:
        """Register a pending hook ticket for a task/tool call."""
        from factorial._internal.queue.operations import (
            register_pending_hook as q_register_pending_hook,
        )

        async with self.redis_client_context() as redis_client:
            return await q_register_pending_hook(
                redis_client=redis_client,
                namespace=self.namespace,
                task_id=task_id,
                tool_call_id=tool_call_id,
                pending_hook=pending_hook,
                mode=mode,
                session_id=session_id,
                tool_name=tool_name,
                tool_args=tool_args,
                hook_param_name=hook_param_name,
                depends_on=depends_on,
                hook_type_name=hook_type_name,
            )

    async def resolve_hook(
        self,
        *,
        hook_id: str,
        payload: Any,
        token: str,
        idempotency_key: str | None = None,
    ) -> HookResolutionResult:
        """Resolve a hook by id using token-authenticated payload."""
        from factorial._internal.queue.operations import resolve_hook as q_resolve_hook

        async with self.redis_client_context() as redis_client:
            resolution = await q_resolve_hook(
                redis_client=redis_client,
                namespace=self.namespace,
                hook_id=hook_id,
                payload=payload,
                token=token,
                idempotency_key=idempotency_key,
            )
            if resolution.task_resumed:
                await self._wake_task_if_possible(
                    task_id=resolution.task_id,
                    reason="hook_resolved",
                )
            return resolution

    async def rotate_hook_token(
        self,
        *,
        hook_id: str,
        revoke_previous: bool = True,
    ) -> str:
        """Rotate token for a pending hook."""
        from factorial._internal.queue.operations import (
            rotate_hook_token as q_rotate_hook_token,
        )

        async with self.redis_client_context() as redis_client:
            return await q_rotate_hook_token(
                redis_client=redis_client,
                namespace=self.namespace,
                hook_id=hook_id,
                revoke_previous=revoke_previous,
            )

    async def get_task_status(self, task_id: str) -> Any:
        """Get task status using the control plane's configuration"""
        from factorial._internal.queue.task_store import (
            get_task_status as q_get_task_status,
        )

        async with self.redis_client_context() as redis_client:
            return await q_get_task_status(
                redis_client=redis_client, namespace=self.namespace, task_id=task_id
            )

    async def get_task_data(self, task_id: str) -> dict[str, Any] | None:
        """Get task data using the control plane's configuration"""
        from factorial._internal.queue.task_store import (
            get_task_data as q_get_task_data,
        )

        async with self.redis_client_context() as redis_client:
            try:
                return await q_get_task_data(
                    redis_client=redis_client,
                    namespace=self.namespace,
                    task_id=task_id,
                )
            except TaskNotFoundError:
                return None

    async def get_task_agent(self, task_id: str) -> BaseAgent[Any] | None:
        """Get the agent that owns a specific task"""
        task_data = await self.get_task_data(task_id)
        if not task_data:
            return None

        agent_name = task_data.get("agent")
        if not agent_name:
            return None

        return self.get_agent(agent_name)

    def _datetime_from_unix(self, value: Any) -> datetime | None:
        if value is None:
            return None
        try:
            return datetime.fromtimestamp(float(value), tz=timezone.utc)
        except (TypeError, ValueError):
            return None

    def _normalize_public_status(self, status: TaskStatus) -> TaskSnapshotStatus:
        if status is TaskStatus.QUEUED:
            return TaskSnapshotStatus.QUEUED
        if status in {TaskStatus.PROCESSING, TaskStatus.ACTIVE}:
            return TaskSnapshotStatus.RUNNING
        if status in {
            TaskStatus.PAUSED,
            TaskStatus.PENDING_TOOL_RESULTS,
            TaskStatus.PENDING_CHILD_TASKS,
        }:
            return TaskSnapshotStatus.WAITING
        if status is TaskStatus.BACKOFF:
            return TaskSnapshotStatus.BACKOFF
        if status is TaskStatus.COMPLETED:
            return TaskSnapshotStatus.COMPLETED
        if status is TaskStatus.FAILED:
            return TaskSnapshotStatus.FAILED
        if status is TaskStatus.CANCELLED:
            return TaskSnapshotStatus.CANCELLED
        raise ValueError(f"Unsupported task status: {status}")

    async def _task_wait_snapshot(
        self,
        *,
        redis_client: redis.Redis,
        task_id: str,
    ) -> WaitSnapshot | None:
        root_keys = RedisKeys.format(namespace=self.namespace)
        pipe = redis_client.pipeline(transaction=True)
        pipe.multi()
        pipe.hget(root_keys.signal_wait_meta, task_id)
        pipe.hget(root_keys.activity_wait_meta, task_id)
        pipe.hget(root_keys.scheduled_wait_meta, task_id)
        signal_wait_raw, activity_wait_raw, scheduled_wait_raw = await pipe.execute()

        if signal_wait_raw:
            payload = json.loads(signal_wait_raw)
            return WaitSnapshot(
                kind=WaitKind.SIGNAL,
                signal_id=payload.get("signal_id"),
                source_tool_call_ids=tuple(payload.get("source_tool_call_ids") or ()),
                data=payload.get("data"),
            )

        if activity_wait_raw:
            payload = json.loads(activity_wait_raw)
            return WaitSnapshot(
                kind=WaitKind.ACTIVITY,
                wake_at=self._datetime_from_unix(payload.get("deadline_at")),
                source_tool_call_ids=tuple(payload.get("source_tool_call_ids") or ()),
                data=payload.get("data"),
            )

        if scheduled_wait_raw:
            payload = json.loads(scheduled_wait_raw)
            kind = payload.get("kind")
            if kind == "cron":
                wait_kind = WaitKind.CRON
            elif kind == "sleep":
                wait_kind = WaitKind.SLEEP
            elif kind == "activity_timeout":
                wait_kind = WaitKind.ACTIVITY
            elif kind == "signal_timeout":
                wait_kind = WaitKind.SIGNAL
            else:
                return None
            return WaitSnapshot(
                kind=wait_kind,
                wake_at=self._datetime_from_unix(payload.get("wake_at")),
                signal_id=payload.get("signal_id"),
                source_tool_call_ids=tuple(payload.get("source_tool_call_ids") or ()),
                data=payload.get("data"),
            )

        return None

    async def _pending_hook_snapshots(
        self,
        *,
        redis_client: redis.Redis,
        task_id: str,
    ) -> tuple[PendingHookSnapshot, ...]:
        task_keys = RedisKeys.format(namespace=self.namespace, task_id=task_id)
        hook_ids: list[Any] = sorted(
            cast(
                set[Any],
                await resolve_awaitable(redis_client.smembers(task_keys.hooks_by_task)),
            )
        )
        if not hook_ids:
            return ()

        records_raw = cast(
            list[Any],
            await resolve_awaitable(
                redis_client.hmget(task_keys.hooks_index, hook_ids)
            ),
        )
        snapshots: list[PendingHookSnapshot] = []
        for record_raw in records_raw:
            if not record_raw:
                continue
            record = HookRecord.from_json(record_raw)
            if record.resolved_at is not None:
                continue
            snapshots.append(
                PendingHookSnapshot(
                    id=record.hook_id,
                    hook_type=record.hook_type,
                    mode=HookMode(record.mode),
                    title=cast(str | None, record.metadata.get("title")),
                    tool_name=record.tool_name,
                    param_name=record.hook_param_name,
                    expires_at=datetime.fromtimestamp(
                        record.expires_at,
                        tz=timezone.utc,
                    ),
                    metadata=dict(record.metadata),
                )
            )
        return tuple(snapshots)

    async def snapshot_task(self, task_id: str) -> TaskSnapshot[Any, Any]:
        task_data = await self.get_task_data(task_id)
        if task_data is None:
            raise TaskNotFoundError(task_id)

        agent_name = str(task_data["agent"])
        agent = self._resolve_agent(agent_name)
        task = Task.from_dict(task_data, payload_parser=agent.context_from_dict)
        public_status = self._normalize_public_status(task.status)

        async with self.redis_client_context() as redis_client:
            wait = await self._task_wait_snapshot(
                redis_client=redis_client,
                task_id=task_id,
            )
            pending_hooks = await self._pending_hook_snapshots(
                redis_client=redis_client,
                task_id=task_id,
            )
            task_keys = RedisKeys.format(
                namespace=self.namespace,
                task_id=task_id,
                agent=agent_name,
            )
            pending_child_members = cast(
                set[Any],
                await resolve_awaitable(
                    redis_client.smembers(task_keys.pending_child_wait_ids)
                ),
            )
            pending_child_task_ids = tuple(
                sorted(pending_child_members)
            )
            backoff_score = await redis_client.zscore(task_keys.queue_backoff, task_id)

        return TaskSnapshot(
            id=task.id,
            agent_name=task.agent,
            owner_id=task.metadata.owner_id,
            batch_id=task.metadata.batch_id,
            status=public_status,
            state=task.payload.state,
            metadata=task.payload.metadata,
            output=task.payload.output,
            retry_count=task.retries,
            turn_number=task.payload.turn_number,
            wait=wait,
            pending_hooks=pending_hooks,
            pending_child_task_ids=pending_child_task_ids,
            backoff_until=self._datetime_from_unix(backoff_score),
        )

    async def snapshot_batch(self, batch_id: str) -> BatchSnapshot:
        from factorial._internal.queue.task_store import (
            get_batch_data as q_get_batch_data,
        )

        async with self.redis_client_context() as redis_client:
            try:
                batch = await q_get_batch_data(
                    redis_client=redis_client,
                    namespace=self.namespace,
                    batch_id=batch_id,
                )
            except BatchNotFoundError:
                raise

        return BatchSnapshot(
            id=batch.id,
            owner_id=batch.metadata.owner_id,
            total_tasks=batch.metadata.total_tasks,
            remaining_tasks=len(batch.remaining_task_ids),
            progress=batch.progress,
            is_finished=batch.metadata.status != "active",
        )

    async def task_result(self, task_id: str) -> RunResult[Any, Any, Any]:
        task_data = await self.get_task_data(task_id)
        if task_data is None:
            raise TaskNotFoundError(task_id)

        agent_name = str(task_data["agent"])
        agent = self._resolve_agent(agent_name)
        task = Task.from_dict(task_data, payload_parser=agent.context_from_dict)
        snapshot = await self.snapshot_task(task_id)
        if task.status not in {
            TaskStatus.COMPLETED,
            TaskStatus.FAILED,
            TaskStatus.CANCELLED,
        }:
            raise ValueError("RunResult is only available for terminal tasks")

        if task.status is TaskStatus.COMPLETED:
            run_status = RunStatus.COMPLETED
        elif task.status is TaskStatus.FAILED:
            run_status = RunStatus.FAILED
        else:
            run_status = RunStatus.CANCELLED

        return RunResult(
            run_id=task.id,
            task_id=task.id,
            agent_name=task.agent,
            owner_id=task.metadata.owner_id,
            status=run_status,
            output=task.payload.output,
            state=task.payload.state,
            metadata=task.payload.metadata,
            messages=tuple(task.payload.messages),
            usage=UsageSummary.zero(),
            turn_count=snapshot.turn_number,
            last_turn=snapshot.last_turn,
            started_at=task.metadata.created_at,
            finished_at=datetime.now(timezone.utc),
        )

    def create_observability_app(self) -> FastAPI:
        """Create the observability FastAPI app (minimal, clean, robust)"""
        from factorial.observability.dashboard.routes import add_observability_routes

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
            cast(Any, CORSMiddleware),
            allow_origins=self.observability_config.cors_origins,
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

        redis_client = redis.Redis(
            connection_pool=self.redis_pool, decode_responses=True
        )
        agents = [runner.agent for runner in self.runners]

        # Get metrics config from first runner (all runners have same config)
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
        async def root() -> dict[str, Any]:
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
                workers += runner.create_worker_tasks(shutdown_event, self.agents)

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
                            f"{len(still_pending)} workers didn't finish "
                            "in time, cancelling..."
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

            try:
                await self.wake_dispatch.flush()
            except Exception as exc:
                logger.error("Failed to flush wake dispatch", exc_info=exc)

            await self._disconnect_loop_redis_pools()
            await self.redis_pool.disconnect()
            logger.info("Redis connection pool closed")

    def run(self, run_observability_server: bool = True) -> None:
        if self.runtime_mode == "vercel":
            raise RuntimeError(
                "Orchestrator.run() is for long-running process mode only. "
                "Use factorial.platforms.vercel.create_*_app helpers in Vercel mode."
            )
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
                        self.start_workers(self.shutdown_event),
                        observability_task,
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

