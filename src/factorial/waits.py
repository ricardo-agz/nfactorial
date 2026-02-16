from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal


@dataclass(frozen=True)
class WaitInstruction:
    """Serializable wait intent used by the namespace-style API."""

    kind: Literal["sleep", "cron", "children"]
    message: str | None = None
    timeout_s: float | None = None
    sleep_s: float | None = None
    cron: str | None = None
    timezone: str | None = None
    child_agent_name: str | None = None
    child_inputs: list[Any] | None = None


class WaitNamespace:
    """Namespace-style wait API (`wait.sleep`, `wait.cron`, `wait.children`)."""

    def sleep(self, seconds: float, *, message: str | None = None) -> WaitInstruction:
        return WaitInstruction(kind="sleep", sleep_s=float(seconds), message=message)

    def cron(
        self,
        expression: str,
        *,
        timezone: str = "UTC",
        message: str | None = None,
    ) -> WaitInstruction:
        return WaitInstruction(
            kind="cron",
            cron=expression,
            timezone=timezone,
            message=message,
        )

    def children(
        self,
        *,
        agent: Any,
        inputs: list[Any],
        timeout_s: float | None = None,
        message: str | None = None,
    ) -> WaitInstruction:
        return WaitInstruction(
            kind="children",
            child_agent_name=getattr(agent, "name", None),
            child_inputs=inputs,
            timeout_s=timeout_s,
            message=message,
        )


wait = WaitNamespace()

