from __future__ import annotations

import hashlib
import time
import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import redis.asyncio as redis

from factorial._internal.compat import resolve_awaitable
from factorial._internal.queue.maintenance.tick import (
    MaintenanceTickContext,
    MaintenanceTickResult,
    maintenance_tick,
)
from factorial.core.logging import get_logger
from factorial.platforms.vercel.settings import VercelRuntimeSettings

if TYPE_CHECKING:
    from factorial.orchestrator import Orchestrator
    from factorial.orchestrator.core import Runner

logger = get_logger(__name__)

_LOCK_RELEASE_SCRIPT = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
"""


@dataclass(frozen=True)
class MaintenanceInvocationSummary:
    ok: bool
    agents_total: int
    agents_processed: int
    stale_recovered: int
    backoff_recovered: int
    scheduled_recovered: int
    pending_child_resumed: int
    expired_hooks: int
    expired_tasks_removed: int
    expired_batches_removed: int
    touched_agents: list[str]
    budget_exhausted: bool
    needs_follow_up: bool
    lock_acquired: bool
    reason: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "agents_total": self.agents_total,
            "agents_processed": self.agents_processed,
            "stale_recovered": self.stale_recovered,
            "backoff_recovered": self.backoff_recovered,
            "scheduled_recovered": self.scheduled_recovered,
            "pending_child_resumed": self.pending_child_resumed,
            "expired_hooks": self.expired_hooks,
            "expired_tasks_removed": self.expired_tasks_removed,
            "expired_batches_removed": self.expired_batches_removed,
            "touched_agents": self.touched_agents,
            "budget_exhausted": self.budget_exhausted,
            "needs_follow_up": self.needs_follow_up,
            "lock_acquired": self.lock_acquired,
            "reason": self.reason,
        }


async def run_maintenance_invocation(
    *,
    orchestrator: Orchestrator,
    settings: VercelRuntimeSettings,
    reason: str,
) -> MaintenanceInvocationSummary:
    if not orchestrator.runners:
        return MaintenanceInvocationSummary(
            ok=True,
            agents_total=0,
            agents_processed=0,
            stale_recovered=0,
            backoff_recovered=0,
            scheduled_recovered=0,
            pending_child_resumed=0,
            expired_hooks=0,
            expired_tasks_removed=0,
            expired_batches_removed=0,
            touched_agents=[],
            budget_exhausted=False,
            needs_follow_up=False,
            lock_acquired=False,
            reason=reason,
        )

    redis_client = await orchestrator.get_redis_client()
    lock_key = _maintenance_lock_key(orchestrator.namespace)
    cursor_key = _maintenance_cursor_key(orchestrator.namespace)
    token = str(uuid.uuid4())
    lock_ttl_s = max(int(settings.maintenance_budget_s) + 30, 60)

    lock_acquired = await resolve_awaitable(
        redis_client.set(lock_key, token, nx=True, ex=lock_ttl_s)
    )
    if not lock_acquired:
        await redis_client.close()
        return MaintenanceInvocationSummary(
            ok=True,
            agents_total=len(orchestrator.runners),
            agents_processed=0,
            stale_recovered=0,
            backoff_recovered=0,
            scheduled_recovered=0,
            pending_child_resumed=0,
            expired_hooks=0,
            expired_tasks_removed=0,
            expired_batches_removed=0,
            touched_agents=[],
            budget_exhausted=False,
            needs_follow_up=False,
            lock_acquired=False,
            reason=reason,
        )

    try:
        ordered_runners = await _ordered_runners(
            redis_client=redis_client,
            cursor_key=cursor_key,
            runners=orchestrator.runners,
        )
        if (
            settings.maintenance_max_agents_per_invocation > 0
            and len(ordered_runners) > settings.maintenance_max_agents_per_invocation
        ):
            ordered_runners = ordered_runners[
                : settings.maintenance_max_agents_per_invocation
            ]

        deadline = time.monotonic() + settings.maintenance_budget_s
        processed_runners = 0
        stale_recovered = 0
        backoff_recovered = 0
        scheduled_recovered = 0
        pending_child_resumed = 0
        expired_hooks = 0
        expired_tasks_removed = 0
        expired_batches_removed = 0
        touched_agents: list[str] = []
        saturation_detected = False

        for runner in ordered_runners:
            if time.monotonic() >= deadline:
                break
            tick_context = await MaintenanceTickContext.create(
                redis_client=redis_client,
                namespace=orchestrator.namespace,
                agent=runner.agent,
                heartbeat_timeout=(
                    runner.agent_worker_config.heartbeat_interval
                    * runner.agent_worker_config.missed_heartbeats_threshold
                    + runner.agent_worker_config.missed_heartbeats_grace_period
                ),
                max_retries=runner.agent_worker_config.max_retries,
                batch_size=runner.agent_worker_config.batch_size,
                task_ttl_config=runner.maintenance_worker_config.task_ttl,
                max_cleanup_batch=runner.maintenance_worker_config.max_cleanup_batch,
                metrics_retention_duration=(
                    runner.maintenance_worker_config.metrics_timeline.retention_duration
                ),
            )
            tick_result = await maintenance_tick(tick_context)
            processed_runners += 1
            stale_recovered += tick_result.stale_recovered
            backoff_recovered += tick_result.backoff_recovered
            scheduled_recovered += tick_result.scheduled_recovered
            pending_child_resumed += tick_result.pending_child_resumed
            expired_hooks += tick_result.expired_hooks
            expired_tasks_removed += tick_result.expired_tasks_removed
            expired_batches_removed += tick_result.expired_batches_removed
            touched_agents.extend(tick_result.touched_agents)

            if _tick_saturated(runner=runner, result=tick_result):
                saturation_detected = True

        budget_exhausted = processed_runners < len(ordered_runners)
        needs_follow_up = budget_exhausted or saturation_detected

        await _advance_cursor(
            redis_client=redis_client,
            cursor_key=cursor_key,
            runners=orchestrator.runners,
            processed=processed_runners,
        )

        unique_touched_agents = sorted(set(touched_agents))
        if unique_touched_agents:
            await orchestrator.wake_agents(
                agent_names=unique_touched_agents,
                reason="maintenance_recovery",
            )

        return MaintenanceInvocationSummary(
            ok=True,
            agents_total=len(orchestrator.runners),
            agents_processed=processed_runners,
            stale_recovered=stale_recovered,
            backoff_recovered=backoff_recovered,
            scheduled_recovered=scheduled_recovered,
            pending_child_resumed=pending_child_resumed,
            expired_hooks=expired_hooks,
            expired_tasks_removed=expired_tasks_removed,
            expired_batches_removed=expired_batches_removed,
            touched_agents=unique_touched_agents,
            budget_exhausted=budget_exhausted,
            needs_follow_up=needs_follow_up,
            lock_acquired=True,
            reason=reason,
        )
    finally:
        try:
            await resolve_awaitable(
                redis_client.eval(_LOCK_RELEASE_SCRIPT, 1, lock_key, token)
            )
        finally:
            await redis_client.close()


async def _ordered_runners(
    *,
    redis_client: redis.Redis,
    cursor_key: str,
    runners: list[Runner],
) -> list[Runner]:
    if not runners:
        return []
    cursor_raw = await resolve_awaitable(redis_client.get(cursor_key))
    try:
        cursor = int(cursor_raw) if cursor_raw is not None else 0
    except (TypeError, ValueError):
        cursor = 0
    cursor = max(cursor, 0) % len(runners)
    return runners[cursor:] + runners[:cursor]


async def _advance_cursor(
    *,
    redis_client: redis.Redis,
    cursor_key: str,
    runners: list[Runner],
    processed: int,
) -> None:
    if not runners:
        return
    current_raw = await resolve_awaitable(redis_client.get(cursor_key))
    try:
        current = int(current_raw) if current_raw is not None else 0
    except (TypeError, ValueError):
        current = 0
    next_cursor = (current + max(processed, 1)) % len(runners)
    await resolve_awaitable(redis_client.set(cursor_key, str(next_cursor)))


def _maintenance_lock_key(namespace: str) -> str:
    namespace_hash = hashlib.sha256(namespace.encode("utf-8")).hexdigest()[:12]
    return f"{namespace}:maintenance:lock:{namespace_hash}"


def _maintenance_cursor_key(namespace: str) -> str:
    namespace_hash = hashlib.sha256(namespace.encode("utf-8")).hexdigest()[:12]
    return f"{namespace}:maintenance:cursor:{namespace_hash}"


def _tick_saturated(*, runner: Runner, result: MaintenanceTickResult) -> bool:
    batch_size = max(1, runner.agent_worker_config.batch_size)
    max_cleanup_batch = max(1, runner.maintenance_worker_config.max_cleanup_batch)
    return any(
        [
            result.stale_recovered >= batch_size,
            result.backoff_recovered >= batch_size,
            result.scheduled_recovered >= batch_size,
            result.pending_child_resumed >= batch_size,
            result.expired_hooks >= max_cleanup_batch,
            result.expired_tasks_removed >= max_cleanup_batch,
            result.expired_batches_removed >= max_cleanup_batch,
        ]
    )
