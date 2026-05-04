import os
from typing import Literal

from factorial.orchestrator.wake_dispatch import NoopWakeDispatch, WakeDispatch


def resolve_runtime_mode(
    runtime_mode: Literal["process", "vercel"] | None,
) -> Literal["process", "vercel"]:
    if os.getenv("VERCEL") == "1":
        return "vercel"
    if runtime_mode in {"process", "vercel"}:
        return runtime_mode
    return "process"


def resolve_wake_transport(
    *,
    runtime_mode: Literal["process", "vercel"],
    wake_transport: Literal["none", "vercel_queue"] | None,
) -> Literal["none", "vercel_queue"]:
    env_transport = os.getenv("NFACTORIAL_WAKE_TRANSPORT")
    selected = wake_transport or env_transport
    if selected == "none":
        return "none"
    if selected == "vercel_queue":
        return "vercel_queue"
    return "vercel_queue" if runtime_mode == "vercel" else "none"


def build_wake_dispatch(
    *,
    wake_transport: Literal["none", "vercel_queue"],
    dispatch_topic: str,
    namespace: str,
) -> WakeDispatch:
    if wake_transport == "none":
        return NoopWakeDispatch()
    if wake_transport == "vercel_queue":
        from factorial.platforms.vercel.wake_dispatch import VercelQueueWakeDispatch

        return VercelQueueWakeDispatch(topic=dispatch_topic, namespace=namespace)
    return NoopWakeDispatch()


def default_maintenance_reason() -> str:
    service_type = (os.getenv("VERCEL_SERVICE_TYPE") or "").strip().lower()
    if service_type == "cron":
        return "cron_schedule"
    return "manual"


__all__ = [
    "build_wake_dispatch",
    "default_maintenance_reason",
    "resolve_runtime_mode",
    "resolve_wake_transport",
]
