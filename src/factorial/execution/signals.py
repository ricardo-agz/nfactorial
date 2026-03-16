from __future__ import annotations

from dataclasses import dataclass
from typing import Any, TypeVar, cast

from pydantic import BaseModel

from factorial.execution.context import ExecutionContext

T = TypeVar("T")


def _current_execution_context() -> ExecutionContext:
    try:
        return ExecutionContext.current()
    except LookupError as exc:  # pragma: no cover - defensive runtime guard
        raise RuntimeError(
            "signals can only be used during active task execution"
        ) from exc


@dataclass(frozen=True)
class SignalEnvelope:
    signal_id: str
    payload: Any
    sender_task_id: str | None
    sent_at: float | None
    seq: int | None
    wake_reason: str | None = None

    def payload_as(self, model: type[T]) -> T:
        if self.payload is None:
            raise ValueError("signal payload is empty")
        if isinstance(model, type) and issubclass(model, BaseModel):
            return cast(T, model.model_validate(self.payload))
        return cast(T, self.payload)


class SignalsNamespace:
    """Top-level signal wake namespace (`from factorial import signals`)."""

    def current(self) -> SignalEnvelope | None:
        ctx = _current_execution_context()
        payload = ctx.signals.current()
        if payload is None:
            return None
        signal_id = payload.get("signal_id")
        if not isinstance(signal_id, str) or not signal_id:
            return None
        sender_task_id = payload.get("sender_task_id")
        if not isinstance(sender_task_id, str):
            sender_task_id = None
        seq_value = payload.get("seq")
        seq: int | None
        if isinstance(seq_value, int):
            seq = seq_value
        elif isinstance(seq_value, str) and seq_value.isdigit():
            seq = int(seq_value)
        else:
            seq = None
        sent_at_value = payload.get("sent_at")
        sent_at: float | None
        if isinstance(sent_at_value, (int, float)):
            sent_at = float(sent_at_value)
        elif isinstance(sent_at_value, str):
            try:
                sent_at = float(sent_at_value)
            except Exception:
                sent_at = None
        else:
            sent_at = None

        return SignalEnvelope(
            signal_id=signal_id,
            payload=payload.get("payload"),
            sender_task_id=sender_task_id,
            sent_at=sent_at,
            seq=seq,
            wake_reason=ctx.signals.wake_reason(),
        )

    def wake_reason(self) -> str | None:
        ctx = _current_execution_context()
        return ctx.signals.wake_reason()


signals = SignalsNamespace()

__all__ = [
    "SignalEnvelope",
    "SignalsNamespace",
    "signals",
]
