from __future__ import annotations

import pytest

from factorial.orchestrator import Orchestrator
from factorial.runtimes import vercel as vercel_runtime
from factorial.runtimes.vercel import (
    VercelRuntimeSettings,
    bootstrap as vercel_bootstrap,
    cron_service as vercel_cron_service,
    worker_service as vercel_worker_service,
)


def test_worker_service_requires_vercel_workers_on_vercel(monkeypatch) -> None:
    monkeypatch.setenv("VERCEL", "1")
    monkeypatch.setattr(
        vercel_worker_service,
        "_resolve_vercel_workers_runtime",
        lambda: None,
    )

    orchestrator = Orchestrator(wake_transport="none")
    with pytest.raises(RuntimeError, match="vercel-workers"):
        vercel_worker_service.create_worker(orchestrator)


def test_bootstrap_falls_back_to_none_locally_when_workers_missing(monkeypatch) -> None:
    monkeypatch.delenv("VERCEL", raising=False)
    monkeypatch.setattr(vercel_bootstrap, "_vercel_workers_available", lambda: False)

    orchestrator = Orchestrator()
    configured = vercel_bootstrap.configure_orchestrator_for_vercel(orchestrator)
    assert configured.wake_transport == "none"


def test_bootstrap_alias_configure_orchestrator(monkeypatch) -> None:
    monkeypatch.delenv("VERCEL", raising=False)
    monkeypatch.setattr(vercel_bootstrap, "_vercel_workers_available", lambda: False)
    orchestrator = Orchestrator()
    configured = vercel_bootstrap.configure_orchestrator(orchestrator)
    assert configured.wake_transport == "none"


def test_bootstrap_requires_workers_on_vercel_when_missing(monkeypatch) -> None:
    monkeypatch.setenv("VERCEL", "1")
    monkeypatch.setattr(vercel_bootstrap, "_vercel_workers_available", lambda: False)

    orchestrator = Orchestrator(wake_transport="none")
    with pytest.raises(RuntimeError, match="vercel-workers"):
        vercel_bootstrap.configure_orchestrator_for_vercel(orchestrator)


@pytest.mark.asyncio
async def test_cron_trigger_queues_maintenance_tick(monkeypatch) -> None:
    orchestrator = Orchestrator()
    orchestrator.wake_transport = "vercel_queue"

    wake_called = {"count": 0}

    async def _wake_maintenance(*, reason: str) -> bool:
        wake_called["count"] += 1
        assert reason == "cron_schedule"
        return True

    monkeypatch.setattr(orchestrator, "wake_maintenance", _wake_maintenance)
    result = await vercel_cron_service.trigger_maintenance_once(
        orchestrator=orchestrator,
        settings=VercelRuntimeSettings(),
        reason="cron_schedule",
    )

    assert result["ok"] is True
    assert result["mode"] == "queued"
    assert wake_called["count"] == 1


@pytest.mark.asyncio
async def test_cron_trigger_inline_fallback_when_queue_transport_disabled() -> None:
    orchestrator = Orchestrator()
    orchestrator.wake_transport = "none"

    result = await vercel_cron_service.trigger_maintenance_once(
        orchestrator=orchestrator,
        settings=VercelRuntimeSettings(),
        reason="manual_test",
    )

    assert result["ok"] is True
    assert result["mode"] == "inline"


@pytest.mark.asyncio
async def test_orchestrator_run_maintenance_tick_uses_runtime_helpers(
    monkeypatch,
) -> None:
    orchestrator = Orchestrator()
    calls: dict[str, object] = {}

    def _configure(orch: Orchestrator, *, settings=None):  # type: ignore[no-untyped-def]
        calls["configured"] = True
        return orch

    async def _trigger(*, orchestrator: Orchestrator, settings, reason: str):  # type: ignore[no-untyped-def]
        calls["reason"] = reason
        return {"ok": True, "reason": reason}

    monkeypatch.setattr(vercel_runtime, "configure_orchestrator", _configure)
    monkeypatch.setattr(vercel_runtime, "trigger_maintenance_once", _trigger)

    result = await orchestrator.run_maintenance_tick(reason="manual_test")
    assert result["ok"] is True
    assert result["reason"] == "manual_test"
    assert calls["configured"] is True


@pytest.mark.asyncio
async def test_orchestrator_run_maintenance_tick_infers_cron_reason(
    monkeypatch,
) -> None:
    monkeypatch.setenv("VERCEL_SERVICE_TYPE", "cron")
    orchestrator = Orchestrator()
    calls: dict[str, object] = {}

    def _configure(orch: Orchestrator, *, settings=None):  # type: ignore[no-untyped-def]
        calls["configured"] = True
        return orch

    async def _trigger(*, orchestrator: Orchestrator, settings, reason: str):  # type: ignore[no-untyped-def]
        calls["reason"] = reason
        return {"ok": True, "reason": reason}

    monkeypatch.setattr(vercel_runtime, "configure_orchestrator", _configure)
    monkeypatch.setattr(vercel_runtime, "trigger_maintenance_once", _trigger)

    result = await orchestrator.run_maintenance_tick()
    assert result["ok"] is True
    assert result["reason"] == "cron_schedule"
    assert calls["configured"] is True


@pytest.mark.asyncio
async def test_orchestrator_run_maintenance_cron_tick_uses_cron_reason(
    monkeypatch,
) -> None:
    orchestrator = Orchestrator()
    calls: dict[str, object] = {}

    def _configure(orch: Orchestrator, *, settings=None):  # type: ignore[no-untyped-def]
        calls["configured"] = True
        return orch

    async def _trigger(*, orchestrator: Orchestrator, settings, reason: str):  # type: ignore[no-untyped-def]
        calls["reason"] = reason
        return {"ok": True, "reason": reason}

    monkeypatch.setattr(vercel_runtime, "configure_orchestrator", _configure)
    monkeypatch.setattr(vercel_runtime, "trigger_maintenance_once", _trigger)

    result = await orchestrator.run_maintenance_cron_tick()
    assert result["ok"] is True
    assert result["reason"] == "cron_schedule"
    assert calls["configured"] is True


def test_orchestrator_bootstrap_vercel_worker_app_uses_runtime_helper(
    monkeypatch,
) -> None:
    orchestrator = Orchestrator()
    sentinel = object()
    calls: dict[str, object] = {}

    def _create_worker(orch: Orchestrator, *, settings=None):  # type: ignore[no-untyped-def]
        calls["orchestrator"] = orch
        calls["settings"] = settings
        return sentinel

    monkeypatch.setattr(vercel_runtime, "create_worker", _create_worker)

    result = orchestrator.bootstrap_vercel_worker_app()
    assert result is sentinel
    assert calls["orchestrator"] is orchestrator
    assert calls["settings"] is not None


def test_orchestrator_create_app_exposes_expected_routes() -> None:
    orchestrator = Orchestrator()
    app = orchestrator.create_app(
        enable_ws=True,
        cors_origins=["https://app.example.com"],
    )
    route_paths = {route.path for route in app.routes}
    assert "/api/enqueue" in route_paths
    assert "/api/tasks/{task_id}" in route_paths
    assert "/events/{owner_id}" in route_paths
    assert "/ws/{owner_id}" in route_paths
