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

from factorial.platforms.vercel.wake_dispatcher import parse_wake_envelope


def test_parse_wake_envelope_accepts_valid_payload() -> None:
    envelope = parse_wake_envelope(
        {
            "schema_version": 1,
            "kind": "wake_agent",
            "namespace": "factorial",
            "agent_name": "agent_a",
            "reason": "enqueue",
            "task_id": "task-123",
            "wake_id": "wake-1",
        }
    )
    assert envelope is not None
    assert envelope.agent_name == "agent_a"
    assert envelope.reason == "enqueue"


def test_parse_wake_envelope_rejects_invalid_kind() -> None:
    envelope = parse_wake_envelope(
        {
            "schema_version": 1,
            "kind": "unknown_kind",
            "namespace": "factorial",
            "reason": "enqueue",
        }
    )
    assert envelope is None


def test_parse_wake_envelope_rejects_invalid_schema_version() -> None:
    envelope = parse_wake_envelope(
        {
            "schema_version": "v1",
            "kind": "wake_agent",
            "namespace": "factorial",
            "reason": "enqueue",
        }
    )
    assert envelope is None


def test_parse_wake_envelope_accepts_maintenance_tick() -> None:
    envelope = parse_wake_envelope(
        {
            "schema_version": 1,
            "kind": "maintenance_tick",
            "namespace": "factorial",
            "reason": "cron_schedule",
            "wake_id": "wake-maint-1",
        }
    )
    assert envelope is not None
    assert envelope.kind == "maintenance_tick"
    assert envelope.agent_name is None
