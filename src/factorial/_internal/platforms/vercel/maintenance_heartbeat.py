from __future__ import annotations

import hashlib
import time
import uuid
from typing import TYPE_CHECKING

from factorial.core.logging import get_logger
from factorial.platforms.vercel.settings import VercelRuntimeSettings

if TYPE_CHECKING:
    from factorial.orchestrator import Orchestrator

logger = get_logger(__name__)

_LOCK_RELEASE_SCRIPT = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
"""


async def ensure_maintenance_heartbeat(
    *,
    orchestrator: Orchestrator,
    settings: VercelRuntimeSettings,
    reason: str = "maintenance_heartbeat",
) -> bool:
    """
    Ensure exactly one delayed maintenance tick is scheduled for the near future.

    This uses Redis NX+EX to dedupe heartbeat scheduling across concurrent workers
    and Vercel queue idempotency to reduce duplicate enqueue risk.
    """
    if orchestrator.wake_transport != "vercel_queue":
        return False

    redis_client = await orchestrator.get_redis_client()
    interval_s = max(1, int(settings.maintenance_heartbeat_interval_s))
    dedupe_ttl_s = max(interval_s + 1, int(settings.maintenance_heartbeat_dedupe_ttl_s))
    retention_s = max(
        int(settings.maintenance_message_retention_s),
        interval_s + dedupe_ttl_s + 5,
    )
    heartbeat_key = _maintenance_heartbeat_key(orchestrator.namespace)
    token = str(uuid.uuid4())
    idempotency_bucket = int(time.time() // interval_s)
    idempotency_key = (
        f"{orchestrator.namespace}:maintenance:heartbeat:{idempotency_bucket}"
    )

    lock_acquired = await redis_client.set(
        heartbeat_key,
        token,
        nx=True,
        ex=dedupe_ttl_s,
    )
    if not lock_acquired:
        await redis_client.close()
        return False

    try:
        dispatched = await orchestrator.wake_maintenance(
            reason=reason,
            delay_seconds=interval_s,
            idempotency_key=idempotency_key,
            retention_seconds=retention_s,
        )
        if not dispatched:
            await redis_client.eval(_LOCK_RELEASE_SCRIPT, 1, heartbeat_key, token)  # type: ignore[misc]
        return dispatched
    finally:
        await redis_client.close()


async def dispatch_maintenance_continuation(
    *,
    orchestrator: Orchestrator,
    settings: VercelRuntimeSettings,
    reason: str = "maintenance_continuation",
) -> bool:
    if orchestrator.wake_transport != "vercel_queue":
        return False

    delay_s = max(0, int(settings.maintenance_continuation_delay_s))
    retention_s = max(
        int(settings.maintenance_message_retention_s),
        delay_s + 60,
    )
    idempotency_window_s = max(1, delay_s)
    idempotency_bucket = int(time.time() // idempotency_window_s)
    idempotency_key = (
        f"{orchestrator.namespace}:maintenance:continuation:{idempotency_bucket}"
    )
    return await orchestrator.wake_maintenance(
        reason=reason,
        delay_seconds=delay_s,
        idempotency_key=idempotency_key,
        retention_seconds=retention_s,
    )


def _maintenance_heartbeat_key(namespace: str) -> str:
    namespace_hash = hashlib.sha256(namespace.encode("utf-8")).hexdigest()[:12]
    return f"{namespace}:maintenance:heartbeat:{namespace_hash}"
