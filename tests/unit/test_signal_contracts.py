"""Contracts for signal wait and subagent signal helpers."""

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
from factorial.execution.subagents import SignalDeliveryReport, subagents
from factorial.execution.waits import wait


class _NoopEvents:
    async def publish_event(self, _event: Any) -> None:
        return None


class _VotePayload(BaseModel):
    round: int
    target: str


def test_wait_until_signal_builder_without_timeout() -> None:
    instruction = wait.until_signal("day_vote_open:2", data={"phase": "day_vote"})
    assert instruction.kind == "signal"
    assert instruction.signal_id == "day_vote_open:2"
    assert instruction.data == {"phase": "day_vote"}
    assert instruction.signal_timeout_kind is None


def test_wait_until_signal_builder_with_sleep_timeout() -> None:
    instruction = wait.until_signal(
        "night_action_open:3",
        timeout=wait.sleep(15.0),
        data={"phase": "night_action"},
    )
    assert instruction.kind == "signal"
    assert instruction.signal_id == "night_action_open:3"
    assert instruction.signal_timeout_kind == "sleep"
    assert instruction.signal_timeout_s == 15.0


def test_wait_until_signal_builder_with_cron_timeout() -> None:
    instruction = wait.until_signal(
        "night_action_open:3",
        timeout=wait.cron("*/2 * * * *", timezone="UTC"),
    )
    assert instruction.kind == "signal"
    assert instruction.signal_id == "night_action_open:3"
    assert instruction.signal_timeout_kind == "cron"
    assert instruction.signal_timeout_cron == "*/2 * * * *"
    assert instruction.signal_timeout_timezone == "UTC"


def test_wait_until_signal_rejects_invalid_inputs() -> None:
    with pytest.raises(ValueError, match="non-empty signal_id"):
        wait.until_signal("   ")
    with pytest.raises(ValueError, match="only supports wait.sleep"):
        wait.until_signal("x", timeout=wait.activity())


@pytest.mark.asyncio
async def test_subagents_signal_routes_single_target_callback() -> None:
    seen: list[tuple[str, str, Any]] = []

    async def _signal_child(task_id: str, signal_id: str, payload: Any) -> dict[str, Any]:
        seen.append((task_id, signal_id, payload))
        return {
            "signal_id": signal_id,
            "target_task_ids": [task_id],
            "signaled_task_ids": [task_id],
            "woken_task_ids": [task_id],
            "skipped_inactive_task_ids": [],
            "failed_task_ids": [],
        }

    ctx = ExecutionContext(
        task_id="parent-task",
        owner_id="owner",
        retries=0,
        iterations=0,
        events=cast(EventPublisher, _NoopEvents()),
        subagents=SubagentsExecutionNamespace(signal_callback=_signal_child),
    )
    token = execution_context.set(ctx)
    try:
        report = await subagents.signal(
            "child-task-1",
            signal_id="day_vote_open:1",
            payload={"round": 1},
        )
    finally:
        execution_context.reset(token)

    assert isinstance(report, SignalDeliveryReport)
    assert report.signal_id == "day_vote_open:1"
    assert report.target_task_ids == ["child-task-1"]
    assert report.signaled_task_ids == ["child-task-1"]
    assert report.woken_task_ids == ["child-task-1"]
    assert report.failed_task_ids == []
    assert seen == [("child-task-1", "day_vote_open:1", {"round": 1})]


@pytest.mark.asyncio
async def test_subagents_signal_routes_batch_callback_with_dedupe() -> None:
    seen_batches: list[tuple[list[str], str, Any]] = []

    async def _signal_children(
        task_ids: list[str],
        signal_id: str,
        payload: Any,
    ) -> dict[str, Any]:
        seen_batches.append((list(task_ids), signal_id, payload))
        return {
            "signal_id": signal_id,
            "target_task_ids": list(task_ids),
            "signaled_task_ids": list(task_ids),
            "woken_task_ids": [task_ids[0]],
            "skipped_inactive_task_ids": [task_ids[-1]],
            "failed_task_ids": [],
        }

    ctx = ExecutionContext(
        task_id="parent-task",
        owner_id="owner",
        retries=0,
        iterations=0,
        events=cast(EventPublisher, _NoopEvents()),
        subagents=SubagentsExecutionNamespace(signal_many_callback=_signal_children),
    )
    token = execution_context.set(ctx)
    try:
        report = await subagents.signal(
            ["child-a", "child-b", "child-a"],
            signal_id="night_action_open:5",
            payload={"round": 5},
        )
    finally:
        execution_context.reset(token)

    assert report.target_task_ids == ["child-a", "child-b"]
    assert report.signaled_task_ids == ["child-a", "child-b"]
    assert report.woken_task_ids == ["child-a"]
    assert report.skipped_inactive_task_ids == ["child-b"]
    assert seen_batches == [
        (["child-a", "child-b"], "night_action_open:5", {"round": 5})
    ]


def test_signals_namespace_reads_current_envelope_and_wake_reason() -> None:
    ctx = ExecutionContext(
        task_id="child-task",
        owner_id="owner",
        retries=0,
        iterations=0,
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
        retries=0,
        iterations=0,
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
