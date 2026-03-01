from __future__ import annotations

from types import SimpleNamespace

import pytest
from httpx import ASGITransport, AsyncClient

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

import factorial.queue as queue_module
from factorial.engine import WorkerTickResult
from factorial.exceptions import TaskNotFoundError
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


def test_bootstrap_leaves_none_transport_locally(monkeypatch) -> None:
    monkeypatch.delenv("VERCEL", raising=False)

    orchestrator = Orchestrator()
    configured = vercel_bootstrap.configure_orchestrator_for_vercel(orchestrator)
    assert configured.wake_transport == "none"


def test_bootstrap_alias_configure_orchestrator(monkeypatch) -> None:
    monkeypatch.delenv("VERCEL", raising=False)
    orchestrator = Orchestrator()
    configured = vercel_bootstrap.configure_orchestrator(orchestrator)
    assert configured.wake_transport == "none"


def test_bootstrap_uses_vercel_queue_transport_on_vercel(monkeypatch) -> None:
    monkeypatch.setenv("VERCEL", "1")
    monkeypatch.delenv("NFACTORIAL_WAKE_TRANSPORT", raising=False)

    orchestrator = Orchestrator(wake_transport="none")
    configured = vercel_bootstrap.configure_orchestrator_for_vercel(orchestrator)
    assert configured.wake_transport == "vercel_queue"


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
    route_paths = {
        route.path for route in app.routes if hasattr(route, "path")
    }
    assert "/api/enqueue" in route_paths
    assert "/api/tasks/{task_id}" in route_paths
    assert "/api/tasks/{task_id}/message" in route_paths
    assert "/api/groups/message" in route_paths
    assert "/events/{owner_id}" in route_paths
    assert "/ws/{owner_id}" in route_paths


@pytest.mark.asyncio
async def test_orchestrator_get_task_data_returns_none_for_missing_task(
    monkeypatch,
) -> None:
    orchestrator = Orchestrator()

    async def _missing_task(*args, **kwargs):  # type: ignore[no-untyped-def]
        raise TaskNotFoundError("missing-task")

    monkeypatch.setattr(queue_module, "get_task_data", _missing_task)
    task_data = await orchestrator.get_task_data("missing-task")
    assert task_data is None


@pytest.mark.asyncio
async def test_create_app_get_task_route_returns_404_for_missing_task(
    monkeypatch,
) -> None:
    orchestrator = Orchestrator()
    app = orchestrator.create_app()

    async def _missing_task(*args, **kwargs):  # type: ignore[no-untyped-def]
        raise TaskNotFoundError("missing-task")

    monkeypatch.setattr(queue_module, "get_task_data", _missing_task)
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://testserver",
    ) as client:
        response = await client.get("/api/tasks/missing-task")

    assert response.status_code == 404
    assert response.json() == {"detail": "Task not found"}


@pytest.mark.asyncio
async def test_create_app_message_task_route_returns_delivery_report(
    monkeypatch,
) -> None:
    orchestrator = Orchestrator()
    app = orchestrator.create_app()

    async def _message_task(*args, **kwargs):  # type: ignore[no-untyped-def]
        assert kwargs["owner_id"] == "owner-1"
        assert kwargs["to_task_id"] == "task-123"
        assert kwargs["content"] == "hello"
        return {
            "team_id": "team-1",
            "to_task_id": "task-123",
            "thread_id": "human:owner-1:task-123",
            "thread_message_id": "1-0",
            "global_message_id": "2-0",
            "delivered_task_ids": ["task-123"],
            "skipped_inactive_task_ids": [],
            "failed_task_ids": [],
        }

    monkeypatch.setattr(queue_module, "messaging_human_send_direct", _message_task)
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://testserver",
    ) as client:
        response = await client.post(
            "/api/tasks/task-123/message",
            json={"owner_id": "owner-1", "content": "hello"},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["ok"] is True
    assert body["to_task_id"] == "task-123"
    assert body["thread_id"] == "human:owner-1:task-123"


@pytest.mark.asyncio
async def test_create_app_message_group_route_accepts_group_id_target(
    monkeypatch,
) -> None:
    orchestrator = Orchestrator()
    app = orchestrator.create_app()

    async def _message_group(*args, **kwargs):  # type: ignore[no-untyped-def]
        assert kwargs["owner_id"] == "owner-1"
        assert kwargs["group_id"] == "grp1.test"
        assert kwargs["content"] == "status update"
        return {
            "team_id": "team-1",
            "group_id": "grp1.test",
            "group_name": "research",
            "thread_id": "group:team-1:research",
            "thread_message_id": "1-0",
            "global_message_id": "2-0",
            "delivered_task_ids": ["task-a"],
            "skipped_inactive_task_ids": [],
            "failed_task_ids": [],
        }

    monkeypatch.setattr(queue_module, "messaging_human_send_group", _message_group)
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://testserver",
    ) as client:
        response = await client.post(
            "/api/groups/message",
            json={
                "owner_id": "owner-1",
                "content": "status update",
                "group_id": "grp1.test",
            },
        )

    assert response.status_code == 200
    body = response.json()
    assert body["ok"] is True
    assert body["group_id"] == "grp1.test"


@pytest.mark.asyncio
async def test_run_worker_invocation_reports_failed_tasks(monkeypatch) -> None:
    orchestrator = Orchestrator()
    runner = SimpleNamespace(
        agent=SimpleNamespace(name="test-agent"),
        agent_worker_config=SimpleNamespace(
            batch_size=1,
            max_retries=3,
            heartbeat_interval=1,
            turn_timeout=30,
        ),
        metrics_config=SimpleNamespace(retention_duration=3600),
    )
    orchestrator.runners = [runner]  # type: ignore[assignment]
    orchestrator.agents_by_name = {runner.agent.name: runner.agent}

    class _FakeRedisClient:
        async def close(self) -> None:
            return

    async def _fake_get_redis_client() -> _FakeRedisClient:
        return _FakeRedisClient()

    async def _fake_worker_tick(*args, **kwargs):  # type: ignore[no-untyped-def]
        return WorkerTickResult(
            processed_tasks=1,
            picked_tasks=2,
            cancelled_tasks_processed=0,
            failed_tasks=1,
        )

    async def _fake_tick_context_create(*args, **kwargs):  # type: ignore[no-untyped-def]
        return object()

    async def _fake_backlogged_agents(*args, **kwargs):  # type: ignore[no-untyped-def]
        return []

    monkeypatch.setattr(orchestrator, "get_redis_client", _fake_get_redis_client)
    monkeypatch.setattr(vercel_worker_service, "worker_tick", _fake_worker_tick)
    monkeypatch.setattr(
        vercel_worker_service.WorkerTickContext,
        "create",
        _fake_tick_context_create,
    )
    monkeypatch.setattr(
        vercel_worker_service,
        "_agents_with_queue_main_backlog",
        _fake_backlogged_agents,
    )

    summary = await vercel_worker_service._run_worker_invocation(
        orchestrator=orchestrator,
        settings=VercelRuntimeSettings(worker_max_batches=1, worker_budget_s=1.0),
        agent_name=None,
    )

    assert summary["processed_tasks"] == 1
    assert summary["picked_tasks"] == 2
    assert summary["failed_tasks"] == 1
