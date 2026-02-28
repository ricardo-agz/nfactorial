from __future__ import annotations

import os

from factorial.contracts import NoopWakeDispatch
from factorial.logging import get_logger
from factorial.orchestrator import Orchestrator

from .settings import VercelRuntimeSettings
from .wake_dispatcher import build_vercel_wake_dispatch

logger = get_logger(__name__)


def configure_orchestrator_for_vercel(
    orchestrator: Orchestrator,
    *,
    settings: VercelRuntimeSettings | None = None,
) -> Orchestrator:
    if os.getenv("VERCEL") != "1":
        logger.warning("VERCEL environment variable is not set; skipping Vercel runtime configuration.")
        return orchestrator

    settings = settings or VercelRuntimeSettings.from_env()
    orchestrator.runtime_mode = "vercel"

    wake_transport = (os.getenv("NFACTORIAL_WAKE_TRANSPORT") or "").strip().lower()
    if wake_transport not in {"none", "vercel_queue"}:
        wake_transport = "vercel_queue"
    if wake_transport == "vercel_queue" and not _vercel_workers_available():
        if os.getenv("VERCEL") == "1":
            raise RuntimeError(
                "Vercel runtime requires `vercel-workers` to be installed "
                "for queue dispatch and worker callbacks."
            )
        logger.warning(
            "`vercel-workers` is unavailable locally; falling back to "
            "NFACTORIAL_WAKE_TRANSPORT=none for inline maintenance/testing."
        )
        wake_transport = "none"
    orchestrator.wake_transport = wake_transport

    if wake_transport == "none":
        orchestrator.wake_dispatch = NoopWakeDispatch()
    elif wake_transport == "vercel_queue":
        orchestrator.wake_dispatch = build_vercel_wake_dispatch(
            settings=settings,
            namespace=orchestrator.namespace,
        )
    else:
        raise ValueError(
            "Unsupported wake transport for Vercel runtime: "
            f"{orchestrator.wake_transport!r}"
        )

    return orchestrator


def configure_orchestrator(
    orchestrator: Orchestrator,
    *,
    settings: VercelRuntimeSettings | None = None,
) -> Orchestrator:
    """Ergonomic alias for configuring an orchestrator for Vercel runtime."""
    return configure_orchestrator_for_vercel(orchestrator, settings=settings)


def _vercel_workers_available() -> bool:
    try:
        import vercel.workers  # type: ignore  # noqa: F401

        return True
    except Exception:
        return False
