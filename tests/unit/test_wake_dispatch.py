from __future__ import annotations

import pytest

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

from factorial.runtimes.vercel import wake_dispatch as wake_dispatch_module
from factorial.runtimes.vercel.wake_dispatch import VercelQueueWakeDispatch


@pytest.mark.asyncio
async def test_vercel_queue_wake_dispatch_emits_agent_wake(monkeypatch) -> None:
    sent: list[tuple[str, dict[str, object]]] = []

    def _fake_send(topic: str, payload: dict[str, object]) -> None:
        sent.append((topic, payload))

    monkeypatch.setattr(
        wake_dispatch_module,
        "_resolve_vercel_send_callable",
        lambda: _fake_send,
    )

    dispatch = VercelQueueWakeDispatch(topic="dispatch-topic", namespace="factorial")
    await dispatch.wake_agent(
        agent_name="weather_agent",
        reason="enqueue",
        task_id="task-123",
    )

    assert len(sent) == 1
    topic, payload = sent[0]
    assert topic == "dispatch-topic"
    assert payload["kind"] == "wake_agent"
    assert payload["namespace"] == "factorial"
    assert payload["agent_name"] == "weather_agent"
    assert payload["task_id"] == "task-123"
    assert payload["reason"] == "enqueue"
    assert isinstance(payload.get("wake_id"), str)


@pytest.mark.asyncio
async def test_vercel_queue_wake_dispatch_emits_maintenance_tick(monkeypatch) -> None:
    sent: list[tuple[str, dict[str, object]]] = []

    def _fake_send(topic: str, payload: dict[str, object]) -> None:
        sent.append((topic, payload))

    monkeypatch.setattr(
        wake_dispatch_module,
        "_resolve_vercel_send_callable",
        lambda: _fake_send,
    )

    dispatch = VercelQueueWakeDispatch(topic="dispatch-topic", namespace="factorial")
    await dispatch.wake_maintenance(reason="cron_schedule")

    assert len(sent) == 1
    _, payload = sent[0]
    assert payload["kind"] == "maintenance_tick"
    assert payload["namespace"] == "factorial"
    assert payload["reason"] == "cron_schedule"
    assert "agent_name" not in payload
