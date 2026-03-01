from __future__ import annotations

import os
from dataclasses import dataclass


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        return float(raw)
    except ValueError:
        return default


@dataclass(frozen=True)
class VercelRuntimeSettings:
    dispatch_topic: str = "nfactorial-dispatch"
    dispatch_consumer: str = "default"
    worker_max_batches: int = 5
    worker_max_tasks: int = 250
    worker_budget_s: float = 20.0
    maintenance_budget_s: float = 20.0
    maintenance_max_agents_per_invocation: int = 0
    maintenance_continuation_delay_s: int = 1
    maintenance_heartbeat_interval_s: int = 30
    maintenance_heartbeat_dedupe_ttl_s: int = 90
    maintenance_message_retention_s: int = 300
    sse_keepalive_s: float = 15.0
    event_poll_timeout_s: float = 5.0

    @classmethod
    def from_env(cls) -> VercelRuntimeSettings:
        return cls(
            dispatch_topic=os.getenv(
                "NFACTORIAL_DISPATCH_TOPIC",
                "nfactorial-dispatch",
            ),
            dispatch_consumer=os.getenv("NFACTORIAL_DISPATCH_CONSUMER", "default"),
            worker_max_batches=max(1, _env_int("NFACTORIAL_WORKER_MAX_BATCHES", 5)),
            worker_max_tasks=max(1, _env_int("NFACTORIAL_WORKER_MAX_TASKS", 250)),
            worker_budget_s=max(1.0, _env_float("NFACTORIAL_WORKER_BUDGET_S", 20.0)),
            maintenance_budget_s=max(
                1.0, _env_float("NFACTORIAL_MAINTENANCE_BUDGET_S", 20.0)
            ),
            maintenance_max_agents_per_invocation=max(
                0,
                _env_int("NFACTORIAL_MAINTENANCE_MAX_AGENTS_PER_INVOCATION", 0),
            ),
            maintenance_continuation_delay_s=max(
                0,
                _env_int("NFACTORIAL_MAINTENANCE_CONTINUATION_DELAY_S", 1),
            ),
            maintenance_heartbeat_interval_s=max(
                1,
                _env_int("NFACTORIAL_MAINTENANCE_HEARTBEAT_INTERVAL_S", 30),
            ),
            maintenance_heartbeat_dedupe_ttl_s=max(
                1,
                _env_int("NFACTORIAL_MAINTENANCE_HEARTBEAT_DEDUPE_TTL_S", 90),
            ),
            maintenance_message_retention_s=max(
                60,
                _env_int("NFACTORIAL_MAINTENANCE_MESSAGE_RETENTION_S", 300),
            ),
            sse_keepalive_s=max(1.0, _env_float("NFACTORIAL_SSE_KEEPALIVE_S", 15.0)),
            event_poll_timeout_s=max(
                0.5, _env_float("NFACTORIAL_EVENT_POLL_TIMEOUT_S", 5.0)
            ),
        )
