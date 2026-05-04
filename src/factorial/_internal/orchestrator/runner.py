import asyncio
from typing import Any

import redis.asyncio as redis

from factorial.agent import BaseAgent
from factorial.ai.models import MultiClient
from factorial.core.utils import to_snake_case
from factorial.orchestrator.config import (
    AgentWorkerConfig,
    MaintenanceWorkerConfig,
    MetricsTimelineConfig,
)
from factorial.platforms.process.maintenance_loop import maintenance_loop
from factorial.platforms.process.worker_loop import worker_loop


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

    def set_shutdown_event(self, shutdown_event: asyncio.Event) -> None:
        self.shutdown_event = shutdown_event

    def create_worker_tasks(
        self,
        shutdown_event: asyncio.Event,
        agents: list[BaseAgent[Any]],
    ) -> list[asyncio.Task[Any]]:
        heartbeat_timeout = (
            self.agent_worker_config.heartbeat_interval
            * self.agent_worker_config.missed_heartbeats_threshold
            + self.agent_worker_config.missed_heartbeats_grace_period
        )

        agents_by_name = {agent.name: agent for agent in agents}

        return [
            asyncio.create_task(
                worker_loop(
                    shutdown_event=shutdown_event,
                    redis_pool=self.redis_pool,
                    worker_id=f"{self.queue}-worker-{i + 1}",
                    agent=self.agent,
                    agents_by_name=agents_by_name,
                    batch_size=self.agent_worker_config.batch_size,
                    max_retries=self.agent_worker_config.max_retries,
                    heartbeat_interval=self.agent_worker_config.heartbeat_interval,
                    task_timeout=self.agent_worker_config.turn_timeout,
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
                    metrics_retention_duration=(
                        self.maintenance_worker_config.metrics_timeline.retention_duration
                    ),
                    namespace=self.namespace,
                )
            )
            for _ in range(self.maintenance_worker_config.workers)
        ]


__all__ = ["Runner"]
