from __future__ import annotations

from typing import TYPE_CHECKING

from .maintenance_runner import run_maintenance_invocation
from .settings import VercelRuntimeSettings

if TYPE_CHECKING:
    from factorial.orchestrator import Orchestrator

async def trigger_maintenance_once(
    *,
    orchestrator: Orchestrator,
    settings: VercelRuntimeSettings,
    reason: str = "manual",
) -> dict[str, Any]:
    # Preferred architecture on Vercel:
    # cron only enqueues a maintenance tick trigger and exits quickly.
    if orchestrator.wake_transport == "vercel_queue":
        dispatched = await orchestrator.wake_maintenance(reason=reason)
        if not dispatched:
            raise RuntimeError("Failed to dispatch maintenance tick from cron trigger")
        return {
            "ok": True,
            "mode": "queued",
            "reason": reason,
        }

    # Local/dev fallback when wake transport is disabled.
    summary = await run_maintenance_invocation(
        orchestrator=orchestrator,
        settings=settings,
        reason=f"{reason}_inline_fallback",
    )
    return {
        "ok": summary.ok,
        "mode": "inline",
        **summary.to_dict(),
    }
