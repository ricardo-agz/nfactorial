from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol

from factorial.logging import get_logger

logger = get_logger(__name__)


class WakeDispatch(Protocol):
    async def wake_agent(
        self,
        agent_name: str,
        reason: str,
        task_id: str | None = None,
    ) -> None: ...

    async def wake_agents(self, agent_names: list[str], reason: str) -> None: ...

    async def wake_maintenance(self, reason: str) -> None: ...

    async def flush(self) -> None: ...


class NoopWakeDispatch:
    async def wake_agent(
        self,
        agent_name: str,
        reason: str,
        task_id: str | None = None,
    ) -> None:
        return

    async def wake_agents(self, agent_names: list[str], reason: str) -> None:
        return

    async def wake_maintenance(self, reason: str) -> None:
        return

    async def flush(self) -> None:
        return


@dataclass
class VercelQueueWakeDispatch:
    topic: str
    namespace: str = "factorial"

    async def wake_agent(
        self,
        agent_name: str,
        reason: str,
        task_id: str | None = None,
    ) -> None:
        payload = self._base_payload(kind="wake_agent", reason=reason)
        payload["agent_name"] = agent_name
        payload["task_id"] = task_id
        await self._send(payload)

    async def wake_agents(self, agent_names: list[str], reason: str) -> None:
        deduped = sorted({name for name in agent_names if name})
        for agent_name in deduped:
            await self.wake_agent(agent_name=agent_name, reason=reason)

    async def wake_maintenance(self, reason: str) -> None:
        payload = self._base_payload(kind="maintenance_tick", reason=reason)
        await self._send(payload)

    async def flush(self) -> None:
        return

    def _base_payload(self, *, kind: str, reason: str) -> dict[str, object]:
        return {
            "schema_version": 1,
            "kind": kind,
            "namespace": self.namespace,
            "reason": reason,
            "wake_id": str(uuid.uuid4()),
            "emitted_at": datetime.now(UTC).isoformat(),
        }

    async def _send(self, payload: dict[str, object]) -> None:
        send_callable = _resolve_vercel_send_callable()
        if send_callable is None:
            raise RuntimeError(
                "Vercel wake dispatch requires `vercel-workers` to be installed. "
                "Install the package or set NFACTORIAL_WAKE_TRANSPORT=none."
            )
        await asyncio.to_thread(send_callable, self.topic, payload)


def _resolve_vercel_send_callable():
    try:
        from vercel.workers import send  # type: ignore

        return send
    except Exception:
        pass

    try:
        from vercel.workers.client import send  # type: ignore

        return send
    except Exception as exc:
        logger.debug("Failed to resolve vercel workers send callable", exc_info=exc)
        return None
