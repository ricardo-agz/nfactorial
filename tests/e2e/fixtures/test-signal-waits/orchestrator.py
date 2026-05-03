from __future__ import annotations

import redis.asyncio as redis

from factorial import AgentWorkerConfig, MaintenanceWorkerConfig, Orchestrator

from .agents import signal_timeout_agent, signal_wait_agent


def _agent_worker_config() -> AgentWorkerConfig:
    return AgentWorkerConfig(
        workers=1,
        batch_size=10,
        max_retries=1,
        heartbeat_interval=1,
        missed_heartbeats_threshold=5,
        missed_heartbeats_grace_period=1,
        turn_timeout=30,
    )


def _maintenance_worker_config() -> MaintenanceWorkerConfig:
    return MaintenanceWorkerConfig(
        interval=1,
        workers=1,
    )


def build_orchestrator(
    *,
    redis_pool: redis.ConnectionPool | None = None,
    namespace: str | None = None,
) -> Orchestrator:
    orchestrator = Orchestrator(
        redis_pool=redis_pool,
        namespace=namespace,
    )
    orchestrator.register(
        signal_wait_agent,
        agent_worker_config=_agent_worker_config(),
        maintenance_worker_config=_maintenance_worker_config(),
    )
    orchestrator.register(
        signal_timeout_agent,
        agent_worker_config=_agent_worker_config(),
        maintenance_worker_config=_maintenance_worker_config(),
    )
    return orchestrator
