from __future__ import annotations

from typing import Any

import pytest

from factorial.orchestrator import Orchestrator

try:
    import vercel.workers  # type: ignore  # noqa: F401
except Exception:
    try:
        import vercel.workers.client  # type: ignore  # noqa: F401
    except Exception:
        pytest.skip(
            "requires nfactorial[vercel] (vercel-workers)",
            allow_module_level=True,
        )

from factorial.platforms.vercel.maintenance_heartbeat import (
    dispatch_maintenance_continuation,
    ensure_maintenance_heartbeat,
)
from factorial.platforms.vercel.settings import VercelRuntimeSettings


class _FakeRedis:
    def __init__(self, *, lock_acquired: bool) -> None:
        self.lock_acquired = lock_acquired
        self.closed = False
        self.eval_calls: int = 0

    async def set(self, *args, **kwargs):  # type: ignore[no-untyped-def]
        return self.lock_acquired

    async def eval(self, *args, **kwargs):  # type: ignore[no-untyped-def]
        self.eval_calls += 1
        return 1

    async def close(self) -> None:
        self.closed = True


@pytest.mark.asyncio
async def test_ensure_maintenance_heartbeat_dispatches_delayed_wake(monkeypatch) -> None:
    orchestrator = Orchestrator()
    orchestrator.wake_transport = "vercel_queue"
    fake_redis = _FakeRedis(lock_acquired=True)
    wake_calls: list[dict[str, Any]] = []

    async def _fake_get_redis_client() -> _FakeRedis:
        return fake_redis

    async def _fake_wake_maintenance(**kwargs):  # type: ignore[no-untyped-def]
        wake_calls.append(kwargs)
        return True

    monkeypatch.setattr(orchestrator, "get_redis_client", _fake_get_redis_client)
    monkeypatch.setattr(orchestrator, "wake_maintenance", _fake_wake_maintenance)

    settings = VercelRuntimeSettings(
        maintenance_heartbeat_interval_s=12,
        maintenance_heartbeat_dedupe_ttl_s=20,
        maintenance_message_retention_s=180,
    )
    dispatched = await ensure_maintenance_heartbeat(
        orchestrator=orchestrator,
        settings=settings,
        reason="heartbeat_test",
    )

    assert dispatched is True
    assert len(wake_calls) == 1
    call = wake_calls[0]
    assert call["reason"] == "heartbeat_test"
    assert call["delay_seconds"] == 12
    assert call["retention_seconds"] >= 180
    assert "maintenance:heartbeat:" in call["idempotency_key"]
    assert fake_redis.closed is True
    assert fake_redis.eval_calls == 0


@pytest.mark.asyncio
async def test_ensure_maintenance_heartbeat_dedupes_when_lock_not_acquired(
    monkeypatch,
) -> None:
    orchestrator = Orchestrator()
    orchestrator.wake_transport = "vercel_queue"
    fake_redis = _FakeRedis(lock_acquired=False)
    wake_calls = {"count": 0}

    async def _fake_get_redis_client() -> _FakeRedis:
        return fake_redis

    async def _fake_wake_maintenance(**kwargs):  # type: ignore[no-untyped-def]
        wake_calls["count"] += 1
        return True

    monkeypatch.setattr(orchestrator, "get_redis_client", _fake_get_redis_client)
    monkeypatch.setattr(orchestrator, "wake_maintenance", _fake_wake_maintenance)

    dispatched = await ensure_maintenance_heartbeat(
        orchestrator=orchestrator,
        settings=VercelRuntimeSettings(),
    )

    assert dispatched is False
    assert wake_calls["count"] == 0
    assert fake_redis.closed is True


@pytest.mark.asyncio
async def test_ensure_maintenance_heartbeat_releases_lock_when_dispatch_fails(
    monkeypatch,
) -> None:
    orchestrator = Orchestrator()
    orchestrator.wake_transport = "vercel_queue"
    fake_redis = _FakeRedis(lock_acquired=True)

    async def _fake_get_redis_client() -> _FakeRedis:
        return fake_redis

    async def _fake_wake_maintenance(**kwargs):  # type: ignore[no-untyped-def]
        return False

    monkeypatch.setattr(orchestrator, "get_redis_client", _fake_get_redis_client)
    monkeypatch.setattr(orchestrator, "wake_maintenance", _fake_wake_maintenance)

    dispatched = await ensure_maintenance_heartbeat(
        orchestrator=orchestrator,
        settings=VercelRuntimeSettings(),
    )

    assert dispatched is False
    assert fake_redis.eval_calls == 1
    assert fake_redis.closed is True


@pytest.mark.asyncio
async def test_dispatch_maintenance_continuation_uses_delay_from_settings(
    monkeypatch,
) -> None:
    orchestrator = Orchestrator()
    orchestrator.wake_transport = "vercel_queue"
    wake_calls: list[dict[str, Any]] = []

    async def _fake_wake_maintenance(**kwargs):  # type: ignore[no-untyped-def]
        wake_calls.append(kwargs)
        return True

    monkeypatch.setattr(orchestrator, "wake_maintenance", _fake_wake_maintenance)
    settings = VercelRuntimeSettings(
        maintenance_continuation_delay_s=3,
        maintenance_message_retention_s=90,
    )

    dispatched = await dispatch_maintenance_continuation(
        orchestrator=orchestrator,
        settings=settings,
    )

    assert dispatched is True
    assert len(wake_calls) == 1
    call = wake_calls[0]
    assert call["reason"] == "maintenance_continuation"
    assert call["delay_seconds"] == 3
    assert call["retention_seconds"] == 90
    assert call["idempotency_key"].startswith(
        f"{orchestrator.namespace}:maintenance:continuation:"
    )
