from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any

from factorial.core.logging import get_logger
from factorial.queue.keys import RedisKeys
from factorial.queue.worker.tick import WorkerTickContext, worker_tick

from .bootstrap import configure_orchestrator_for_vercel
from .maintenance_heartbeat import (
    dispatch_maintenance_continuation,
    ensure_maintenance_heartbeat,
)
from .maintenance_runner import run_maintenance_invocation
from .settings import VercelRuntimeSettings
from .wake_dispatcher import parse_wake_envelope

if TYPE_CHECKING:
    from factorial.orchestrator import Orchestrator, Runner

logger = get_logger(__name__)

_REGISTERED_WORKER_APPS: dict[tuple[int, str, str], Any] = {}


def create_worker(
    orchestrator: Orchestrator,
    *,
    settings: VercelRuntimeSettings | None = None,
):
    settings = settings or VercelRuntimeSettings.from_env()
    configure_orchestrator_for_vercel(orchestrator, settings=settings)
    registration_key = (
        id(orchestrator),
        settings.dispatch_topic,
        settings.dispatch_consumer,
    )
    app = _REGISTERED_WORKER_APPS.get(registration_key)
    if app is not None:
        return app

    worker_class = _resolve_vercel_worker_class()
    if worker_class is not None:
        worker = worker_class()

        @worker.receive(
            topic=settings.dispatch_topic,
            consumer=settings.dispatch_consumer,
        )
        async def _queue_worker(message: Any, metadata: Any) -> None:
            await _handle_queue_payload(
                orchestrator=orchestrator,
                settings=settings,
                payload=message,
                metadata=_normalize_worker_metadata(metadata),
            )

        app = worker.asgi_app()
        _REGISTERED_WORKER_APPS[registration_key] = app
        return app

    workers_runtime = _resolve_vercel_workers_runtime()
    if workers_runtime is None:
        raise RuntimeError(
            "Vercel runtime requires `vercel-workers`. "
            "Install with `pip install \"nfactorial[vercel]\"`."
        )

    subscribe, get_asgi_app = workers_runtime

    @subscribe(topic=settings.dispatch_topic, consumer=settings.dispatch_consumer)
    async def _queue_worker(message: Any, metadata: Any) -> None:
        await _handle_queue_payload(
            orchestrator=orchestrator,
            settings=settings,
            payload=message,
            metadata=_normalize_worker_metadata(metadata),
        )

    app = get_asgi_app()
    _REGISTERED_WORKER_APPS[registration_key] = app
    return app


async def _handle_queue_payload(
    *,
    orchestrator: Orchestrator,
    settings: VercelRuntimeSettings,
    payload: Any,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    metadata = metadata or {}
    envelope = parse_wake_envelope(payload)
    if envelope is None:
        logger.error("invalid_wake_payload: %s", payload)
        return {"ok": True, "dropped": "invalid_wake_payload"}

    if envelope.namespace != orchestrator.namespace:
        logger.warning(
            "Dropping wake payload for namespace '%s' (orchestrator namespace '%s')",
            envelope.namespace,
            orchestrator.namespace,
        )
        return {"ok": True, "dropped": "namespace_mismatch"}

    follow_up_dispatched = False
    if envelope.kind == "maintenance_tick":
        maintenance_summary = await run_maintenance_invocation(
            orchestrator=orchestrator,
            settings=settings,
            reason=envelope.reason,
        )
        if (
            maintenance_summary.needs_follow_up
            and orchestrator.wake_transport == "vercel_queue"
        ):
            follow_up_dispatched = await dispatch_maintenance_continuation(
                orchestrator=orchestrator,
                settings=settings,
            )
        tick_summary = maintenance_summary.to_dict()
    else:
        tick_summary = await _run_worker_invocation(
            orchestrator=orchestrator,
            settings=settings,
            agent_name=envelope.agent_name,
        )

    if orchestrator.wake_transport == "vercel_queue" and not follow_up_dispatched:
        await ensure_maintenance_heartbeat(
            orchestrator=orchestrator,
            settings=settings,
        )

    logger.info(
        "worker_callback_processed wake_id=%s topic=%s consumer=%s summary=%s",
        envelope.wake_id,
        metadata.get("topic"),
        metadata.get("consumer"),
        tick_summary,
    )
    return {"ok": True, "summary": tick_summary}


async def _run_worker_invocation(
    *,
    orchestrator: Orchestrator,
    settings: VercelRuntimeSettings,
    agent_name: str | None,
) -> dict[str, Any]:
    runners = _select_runners(orchestrator, agent_name=agent_name)
    if not runners:
        return {
            "processed_tasks": 0,
            "picked_tasks": 0,
            "cancelled_tasks": 0,
            "failed_tasks": 0,
            "runners": 0,
        }

    total_processed = 0
    total_picked = 0
    total_cancelled = 0
    total_failed = 0
    deadline = time.monotonic() + settings.worker_budget_s
    max_passes = max(1, settings.worker_max_batches)
    passes = 0

    while passes < max_passes and time.monotonic() < deadline:
        made_progress = False
        passes += 1
        for runner in runners:
            remaining_s = deadline - time.monotonic()
            if remaining_s <= 0:
                break

            redis_client = await orchestrator.get_redis_client()
            try:
                tick_context = await WorkerTickContext.create(
                    redis_client=redis_client,
                    namespace=orchestrator.namespace,
                    agent=runner.agent,
                    agents_by_name=orchestrator.agents_by_name,
                    batch_size=runner.agent_worker_config.batch_size,
                    max_retries=runner.agent_worker_config.max_retries,
                    heartbeat_interval=runner.agent_worker_config.heartbeat_interval,
                    task_timeout=runner.agent_worker_config.turn_timeout,
                    metrics_retention_duration=runner.metrics_config.retention_duration,
                    strict_batch_pickup_errors=True,
                )
                tick_result = await worker_tick(
                    tick_context,
                    max_batches=1,
                    max_tasks=settings.worker_max_tasks,
                    max_runtime_s=remaining_s,
                )
                total_processed += tick_result.processed_tasks
                total_picked += tick_result.picked_tasks
                total_cancelled += tick_result.cancelled_tasks_processed
                total_failed += tick_result.failed_tasks
                if (
                    tick_result.picked_tasks > 0
                    or tick_result.cancelled_tasks_processed > 0
                ):
                    made_progress = True
            finally:
                await redis_client.close()

        if not made_progress:
            break

    backlogged_agents = await _agents_with_queue_main_backlog(
        orchestrator=orchestrator,
        runners=runners,
    )
    if backlogged_agents and orchestrator.wake_transport == "vercel_queue":
        # One wake is enough because worker invocation scans all runners.
        await orchestrator.wake_agent(
            agent_name=backlogged_agents[0],
            reason="queue_backlog_continuation",
        )

    return {
        "processed_tasks": total_processed,
        "picked_tasks": total_picked,
        "cancelled_tasks": total_cancelled,
        "failed_tasks": total_failed,
        "runners": len(runners),
        "passes": passes,
        "backlogged_agents": backlogged_agents,
    }


def _select_runners(
    orchestrator: Orchestrator, *, agent_name: str | None
) -> list[Runner]:
    if agent_name is None:
        return list(orchestrator.runners)

    prioritized: list[Runner] = []
    remaining: list[Runner] = []
    for runner in orchestrator.runners:
        if runner.agent.name == agent_name:
            prioritized.append(runner)
        else:
            remaining.append(runner)
    return prioritized + remaining


async def _agents_with_queue_main_backlog(
    *,
    orchestrator: Orchestrator,
    runners: list[Runner],
) -> list[str]:
    redis_client = await orchestrator.get_redis_client()
    try:
        backlogged: list[str] = []
        for runner in runners:
            keys = RedisKeys.format(
                namespace=orchestrator.namespace,
                agent=runner.agent.name,
            )
            queue_main_len = await redis_client.llen(keys.queue_main)  # type: ignore[misc]
            if int(queue_main_len) > 0:
                backlogged.append(runner.agent.name)
        return sorted(set(backlogged))
    finally:
        await redis_client.close()


def _normalize_worker_metadata(metadata: Any) -> dict[str, Any]:
    if isinstance(metadata, dict):
        return metadata

    normalized: dict[str, Any] = {}
    fields: tuple[tuple[str, str], ...] = (
        ("topic", "topic"),
        ("topic_name", "topic"),
        ("consumer", "consumer"),
        ("consumer_group", "consumer"),
        ("message_id", "message_id"),
        ("delivery_count", "delivery_count"),
        ("region", "region"),
    )
    for attr, key in fields:
        value = getattr(metadata, attr, None)
        if value is not None and key not in normalized:
            normalized[key] = value
    return normalized


def _resolve_vercel_worker_class():
    try:
        from vercel.workers import Worker  # type: ignore

        return Worker
    except Exception:
        try:
            from vercel.workers.worker import Worker  # type: ignore

            return Worker
        except Exception:
            return None


def _resolve_vercel_workers_runtime():
    try:
        from vercel.workers import get_asgi_app, subscribe  # type: ignore

        return subscribe, get_asgi_app
    except Exception:
        try:
            from vercel.workers.client import get_asgi_app, subscribe  # type: ignore

            return subscribe, get_asgi_app
        except Exception:
            return None
