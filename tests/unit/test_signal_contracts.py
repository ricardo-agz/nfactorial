"""Focused contracts for reading the current signal wake context."""

from __future__ import annotations

from typing import Any, cast

import pytest
from pydantic import BaseModel

from factorial.core.events import EventPublisher
from factorial.execution.context import (
    ExecutionContext,
    SignalsExecutionNamespace,
    SubagentsExecutionNamespace,
    execution_context,
)
from factorial.execution.signals import SignalEnvelope, signals


class _NoopEvents:
    async def publish_event(self, _event: Any) -> None:
        return None


class _VotePayload(BaseModel):
    round: int
    target: str


def test_signals_namespace_reads_current_envelope_and_wake_reason() -> None:
    ctx = ExecutionContext(
        task_id="child-task",
        owner_id="owner",
        retry_count=0,
        events=cast(EventPublisher, _NoopEvents()),
        signals=SignalsExecutionNamespace(
            current_signal={
                "signal_id": "day_vote_open:1",
                "payload": {"round": 1, "target": "p2"},
                "sender_task_id": "gm-task",
                "sent_at": 123.45,
                "seq": 17,
            },
            wake_reason_value="signal",
        ),
    )
    token = execution_context.set(ctx)
    try:
        envelope = signals.current()
        wake_reason = signals.wake_reason()
    finally:
        execution_context.reset(token)

    assert isinstance(envelope, SignalEnvelope)
    assert envelope.signal_id == "day_vote_open:1"
    assert envelope.sender_task_id == "gm-task"
    assert envelope.seq == 17
    assert envelope.wake_reason == "signal"
    assert envelope.payload_as(_VotePayload) == _VotePayload(round=1, target="p2")
    assert wake_reason == "signal"


def test_signals_namespace_returns_timeout_wake_without_current_signal() -> None:
    ctx = ExecutionContext(
        task_id="child-task",
        owner_id="owner",
        retry_count=0,
        events=cast(EventPublisher, _NoopEvents()),
        signals=SignalsExecutionNamespace(
            current_signal=None,
            wake_reason_value="timeout",
        ),
    )
    token = execution_context.set(ctx)
    try:
        envelope = signals.current()
        wake_reason = signals.wake_reason()
    finally:
        execution_context.reset(token)

    assert envelope is None
    assert wake_reason == "timeout"
