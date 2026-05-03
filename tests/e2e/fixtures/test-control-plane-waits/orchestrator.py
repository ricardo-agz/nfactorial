from __future__ import annotations

import redis.asyncio as redis

from factorial import AgentWorkerConfig, MaintenanceWorkerConfig, Orchestrator

from .agents import (
    activity_wait_agent,
    approval_wait_agent,
    cron_wait_agent,
    message_receiver_agent,
)


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
    # Keep scheduled waits responsive enough for CI without turning the fixture into
    # a timing race.
    return MaintenanceWorkerConfig(
        interval=1,
        workers=1,
    )


def _register_fixture_agent(orchestrator: Orchestrator, agent: object) -> None:
    orchestrator.register(
        agent,
        agent_worker_config=_agent_worker_config(),
        maintenance_worker_config=_maintenance_worker_config(),
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
    _register_fixture_agent(orchestrator, message_receiver_agent)
    _register_fixture_agent(orchestrator, activity_wait_agent)
    _register_fixture_agent(orchestrator, cron_wait_agent)
    _register_fixture_agent(orchestrator, approval_wait_agent)
    return orchestrator
