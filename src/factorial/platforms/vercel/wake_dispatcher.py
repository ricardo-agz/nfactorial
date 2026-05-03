from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

from factorial.core.logging import get_logger
from factorial.orchestrator.wake_dispatch import WakeDispatch

from .settings import VercelRuntimeSettings
from .wake_dispatch import VercelQueueWakeDispatch

logger = get_logger(__name__)


@dataclass(frozen=True)
class WakeEnvelope:
    kind: Literal["wake_agent", "maintenance_tick"]
    namespace: str
    agent_name: str | None
    reason: str
    task_id: str | None
    wake_id: str | None
    schema_version: int


def build_vercel_wake_dispatch(
    *,
    settings: VercelRuntimeSettings,
    namespace: str,
) -> WakeDispatch:
    return VercelQueueWakeDispatch(
        topic=settings.dispatch_topic,
        namespace=namespace,
    )


def parse_wake_envelope(message: Any) -> WakeEnvelope | None:
    if not isinstance(message, dict):
        logger.warning("Dropping wake payload: expected object, got %s", type(message))
        return None

    schema_version = message.get("schema_version")
    if schema_version is None:
        schema_version = 1
    if not isinstance(schema_version, int):
        logger.warning(
            "Dropping wake payload: invalid schema_version=%r",
            schema_version,
        )
        return None

    kind = message.get("kind")
    if kind not in {"wake_agent", "maintenance_tick"}:
        logger.warning("Dropping wake payload: unsupported kind=%r", kind)
        return None

    namespace = message.get("namespace")
    if not isinstance(namespace, str) or not namespace:
        logger.warning("Dropping wake payload: missing namespace")
        return None

    reason = message.get("reason")
    if not isinstance(reason, str) or not reason:
        logger.warning("Dropping wake payload: missing reason")
        return None

    agent_name = message.get("agent_name")
    if kind == "wake_agent":
        if not isinstance(agent_name, str) or not agent_name:
            logger.warning(
                "Dropping wake payload: wake_agent requires non-empty agent_name"
            )
            return None
    elif agent_name is not None and not isinstance(agent_name, str):
        logger.warning("Dropping wake payload: invalid agent_name=%r", agent_name)
        return None

    task_id = message.get("task_id")
    if task_id is not None and not isinstance(task_id, str):
        logger.warning("Dropping wake payload: invalid task_id=%r", task_id)
        return None

    wake_id = message.get("wake_id")
    if wake_id is not None and not isinstance(wake_id, str):
        wake_id = None

    return WakeEnvelope(
        kind=kind,
        namespace=namespace,
        agent_name=agent_name,
        reason=reason,
        task_id=task_id,
        wake_id=wake_id,
        schema_version=schema_version,
    )
