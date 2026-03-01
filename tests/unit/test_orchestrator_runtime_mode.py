from __future__ import annotations

import pytest

import factorial.orchestrator as orchestrator_module
from factorial.orchestrator import Orchestrator


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
    monkeypatch.setattr(
        orchestrator_module,
        "_build_wake_dispatch",
        lambda **_: orchestrator_module.NoopWakeDispatch(),
    )

    orchestrator = Orchestrator(runtime_mode="process")
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
        "_build_wake_dispatch",
        lambda **_: (_ for _ in ()).throw(RuntimeError("vercel-workers")),
    )

    with pytest.raises(RuntimeError, match="vercel-workers"):
        Orchestrator()
