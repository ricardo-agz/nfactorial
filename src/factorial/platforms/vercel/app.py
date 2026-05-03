from __future__ import annotations

from typing import Any

from factorial.orchestrator import Orchestrator

from .bootstrap import configure_orchestrator_for_vercel
from .settings import VercelRuntimeSettings
from .worker_service import create_worker


def create_vercel_apps(
    orchestrator: Orchestrator,
    *,
    settings: VercelRuntimeSettings | None = None,
    enable_ws: bool = False,
) -> dict[str, Any]:
    settings = settings or VercelRuntimeSettings.from_env()
    configure_orchestrator_for_vercel(orchestrator, settings=settings)
    return {
        "web": orchestrator.create_app(enable_ws=enable_ws),
        "worker": create_worker(orchestrator, settings=settings),
    }
