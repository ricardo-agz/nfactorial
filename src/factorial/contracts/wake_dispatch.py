from __future__ import annotations

from typing import Protocol


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
