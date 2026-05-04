from __future__ import annotations

import pytest

import factorial.orchestrator.core as orchestrator_module
from factorial.orchestrator import Orchestrator
from factorial.orchestrator.wake_dispatch import NoopWakeDispatch


def test_orchestrator_defaults_to_process_mode_when_not_on_vercel(
    monkeypatch,
) -> None:
    monkeypatch.delenv("VERCEL", raising=False)
    monkeypatch.delenv("NFACTORIAL_WAKE_TRANSPORT", raising=False)

    orchestrator = Orchestrator()
    assert orchestrator.runtime_mode == "process"
    assert orchestrator.wake_transport == "none"


def test_orchestrator_forces_vercel_mode_when_vercel_env_present(
    monkeypatch,
) -> None:
    monkeypatch.setenv("VERCEL", "1")
    monkeypatch.delenv("NFACTORIAL_WAKE_TRANSPORT", raising=False)
    orchestrator = Orchestrator(
        runtime_mode="process",
        wake_dispatch=NoopWakeDispatch(),
    )
    assert orchestrator.runtime_mode == "vercel"
    assert orchestrator.wake_transport == "vercel_queue"


def test_orchestrator_honors_explicit_wake_transport_override(monkeypatch) -> None:
    monkeypatch.setenv("VERCEL", "1")
    monkeypatch.setenv("NFACTORIAL_WAKE_TRANSPORT", "none")

    orchestrator = Orchestrator()
    assert orchestrator.runtime_mode == "vercel"
    assert orchestrator.wake_transport == "none"

    monkeypatch.delenv("NFACTORIAL_WAKE_TRANSPORT", raising=False)


def test_orchestrator_requires_vercel_workers_when_running_on_vercel(
    monkeypatch,
) -> None:
    monkeypatch.setenv("VERCEL", "1")
    monkeypatch.delenv("NFACTORIAL_WAKE_TRANSPORT", raising=False)
    monkeypatch.setattr(
        orchestrator_module,
        "build_wake_dispatch",
        lambda **_: (_ for _ in ()).throw(RuntimeError("vercel-workers")),
    )

    with pytest.raises(RuntimeError, match="vercel-workers"):
        Orchestrator()


def test_orchestrator_uses_redis_url_from_env(monkeypatch) -> None:
    monkeypatch.delenv("VERCEL", raising=False)
    monkeypatch.setenv("REDIS_URL", "redis://localhost:6380/2")
    monkeypatch.delenv("UPSTASH_REDIS_URL", raising=False)
    monkeypatch.delenv("REDIS_HOST", raising=False)
    monkeypatch.delenv("REDIS_PORT", raising=False)
    monkeypatch.delenv("REDIS_DB", raising=False)
    monkeypatch.setenv("REDIS_MAX_CONNECTIONS", "77")

    orchestrator = Orchestrator()
    kwargs = orchestrator.redis_pool.connection_kwargs
    assert kwargs["host"] == "localhost"
    assert kwargs["port"] == 6380
    assert kwargs["db"] == 2
    assert orchestrator.redis_pool.max_connections == 77


def test_orchestrator_rejects_non_redis_url(monkeypatch) -> None:
    monkeypatch.delenv("VERCEL", raising=False)
    monkeypatch.setenv("REDIS_URL", "https://example.com")

    with pytest.raises(RuntimeError, match="REDIS_URL must be a redis://"):
        Orchestrator()
