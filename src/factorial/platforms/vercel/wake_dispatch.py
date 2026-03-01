from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, timezone

from vercel.workers.aio import send as send_async  # type: ignore

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

    async def flush(self) -> None:
        return

    def _base_payload(self, *, kind: str, reason: str) -> dict[str, object]:
        return {
            "schema_version": 1,
            "kind": kind,
            "namespace": self.namespace,
            "reason": reason,
            "wake_id": str(uuid.uuid4()),
            "emitted_at": datetime.now(timezone.utc).isoformat(),
        }

    async def wake_maintenance(
        self,
        reason: str,
        *,
        delay_seconds: int | None = None,
        idempotency_key: str | None = None,
        retention_seconds: int | None = None,
    ) -> None:
        payload = self._base_payload(kind="maintenance_tick", reason=reason)
        await self._send(
            payload,
            delay_seconds=delay_seconds,
            idempotency_key=idempotency_key,
            retention_seconds=retention_seconds,
        )

    async def _send(
        self,
        payload: dict[str, object],
        *,
        delay_seconds: int | None = None,
        idempotency_key: str | None = None,
        retention_seconds: int | None = None,
    ) -> None:
        await send_async(
            self.topic,
            payload,
            delay_seconds=delay_seconds,
            idempotency_key=idempotency_key,
            retention_seconds=retention_seconds,
        )
