"""Contracts for wait instruction builders and cron parsing."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, cast

import pytest

from factorial.context import ExecutionContext, execution_context
from factorial.events import EventPublisher
from factorial.subagents import JobRef
from factorial.waits import WaitInstruction, next_cron_wake_timestamp, wait


class _NoopEvents:
    async def publish_event(self, _event: Any) -> None:
        return None


class _ModelDumpJobRef:
    def __init__(self, task_id: str, agent_name: str, parent_task_id: str):
        self._payload = {
            "task_id": task_id,
            "agent_name": agent_name,
            "parent_task_id": parent_task_id,
        }

    def model_dump(self) -> dict[str, str]:
        return dict(self._payload)


def test_next_cron_wake_timestamp_minute_step() -> None:
    now_ts = datetime(2026, 1, 1, 12, 2, 10, tzinfo=timezone.utc).timestamp()
    wake_ts = next_cron_wake_timestamp("*/5 * * * *", "UTC", now_ts=now_ts)
    wake_at = datetime.fromtimestamp(wake_ts, tz=timezone.utc)
    assert wake_at == datetime(2026, 1, 1, 12, 5, 0, tzinfo=timezone.utc)


def test_next_cron_wake_timestamp_honors_timezone() -> None:
    now_ts = datetime(2026, 1, 1, 13, 30, 0, tzinfo=timezone.utc).timestamp()
    wake_ts = next_cron_wake_timestamp(
        "0 9 * * *",
        "America/New_York",
        now_ts=now_ts,
    )
    wake_at_utc = datetime.fromtimestamp(wake_ts, tz=timezone.utc)
    assert wake_at_utc == datetime(2026, 1, 1, 14, 0, 0, tzinfo=timezone.utc)


def test_next_cron_wake_timestamp_rejects_invalid_expression() -> None:
    with pytest.raises(ValueError):
        next_cron_wake_timestamp("* * *", "UTC", now_ts=0)


def test_wait_namespace_exposes_jobs_not_children() -> None:
    assert hasattr(wait, "jobs")
    assert not hasattr(wait, "children")
    assert hasattr(wait, "activity")


def test_wait_namespace_builders_return_serializable_instructions() -> None:
    sleep_wait = wait.sleep(12.5, data="retry shortly")
    cron_wait = wait.cron("0 * * * *", timezone="UTC", data="hourly sync")
    activity_wait = wait.activity(data={"reason": "awaiting activity"})
    jobs_wait = wait.jobs(
        [
            {
                "task_id": "child-task-1",
                "agent_name": "child-research-agent",
                "parent_task_id": "parent-task",
                "key": "research",
            }
        ],
        data="waiting for child tasks",
    )

    assert isinstance(sleep_wait, WaitInstruction)
    assert sleep_wait.kind == "sleep"
    assert sleep_wait.sleep_s == 12.5
    assert sleep_wait.data == "retry shortly"

    assert isinstance(cron_wait, WaitInstruction)
    assert cron_wait.kind == "cron"
    assert cron_wait.cron == "0 * * * *"
    assert cron_wait.timezone == "UTC"
    assert cron_wait.data == "hourly sync"

    assert isinstance(activity_wait, WaitInstruction)
    assert activity_wait.kind == "activity"
    assert activity_wait.data == {"reason": "awaiting activity"}

    assert isinstance(jobs_wait, WaitInstruction)
    assert jobs_wait.kind == "jobs"
    assert jobs_wait.child_task_ids == ["child-task-1"]
    assert jobs_wait.data == "waiting for child tasks"


def test_wait_activity_timeout_sleep_builder() -> None:
    instruction = wait.activity(timeout=wait.sleep(12.0), data="awaiting input")
    assert instruction.kind == "activity"
    assert instruction.data == "awaiting input"
    assert instruction.activity_timeout_kind == "sleep"
    assert instruction.activity_timeout_s == 12.0
    assert instruction.activity_timeout_cron is None


def test_wait_activity_timeout_cron_builder() -> None:
    instruction = wait.activity(
        timeout=wait.cron("*/5 * * * *", timezone="UTC"),
        data={"mode": "tick"},
    )
    assert instruction.kind == "activity"
    assert instruction.data == {"mode": "tick"}
    assert instruction.activity_timeout_kind == "cron"
    assert instruction.activity_timeout_cron == "*/5 * * * *"
    assert instruction.activity_timeout_timezone == "UTC"


def test_wait_activity_timeout_rejects_non_scheduled_waits() -> None:
    with pytest.raises(ValueError, match="only supports wait.sleep"):
        wait.activity(timeout=wait.activity())


def test_wait_jobs_rejects_empty_job_list() -> None:
    with pytest.raises(ValueError, match="at least one job reference"):
        wait.jobs([])


def test_wait_jobs_builds_join_instruction() -> None:
    ctx = ExecutionContext(
        task_id="parent-task",
        owner_id="owner",
        retries=0,
        iterations=0,
        events=cast(EventPublisher, _NoopEvents()),
    )
    token = execution_context.set(ctx)
    try:
        jobs = [
            JobRef(
                task_id="child-1",
                agent_name="a1",
                parent_task_id="parent-task",
                key="research",
            ),
            JobRef(
                task_id="child-2",
                agent_name="a2",
                parent_task_id="parent-task",
                key="risk",
            ),
        ]
        instruction = wait.jobs(jobs, data="wait for all jobs")
    finally:
        execution_context.reset(token)

    assert instruction.kind == "jobs"
    assert instruction.child_task_ids == ["child-1", "child-2"]
    assert instruction.data == "wait for all jobs"


def test_wait_jobs_deduplicates_task_ids_preserving_order() -> None:
    instruction = wait.jobs(
        [
            {"task_id": "child-1", "agent_name": "a1"},
            {"task_id": "child-2", "agent_name": "a1"},
            {"task_id": "child-1", "agent_name": "a1"},
            {"task_id": "child-3", "agent_name": "a2"},
        ]
    )
    assert instruction.child_task_ids == ["child-1", "child-2", "child-3"]


def test_wait_jobs_accepts_model_dump_refs() -> None:
    ctx = ExecutionContext(
        task_id="parent-task",
        owner_id="owner",
        retries=0,
        iterations=0,
        events=cast(EventPublisher, _NoopEvents()),
    )
    token = execution_context.set(ctx)
    try:
        instruction = wait.jobs(
            [
                _ModelDumpJobRef(
                    task_id="child-1",
                    agent_name="a1",
                    parent_task_id="parent-task",
                )
            ]
        )
    finally:
        execution_context.reset(token)

    assert instruction.kind == "jobs"
    assert instruction.child_task_ids == ["child-1"]


def test_wait_jobs_rejects_foreign_parent_refs() -> None:
    ctx = ExecutionContext(
        task_id="parent-task",
        owner_id="owner",
        retries=0,
        iterations=0,
        events=cast(EventPublisher, _NoopEvents()),
    )
    token = execution_context.set(ctx)
    try:
        with pytest.raises(ValueError, match="belongs to a different parent task"):
            wait.jobs(
                [
                    JobRef(
                        task_id="child-1",
                        agent_name="a1",
                        parent_task_id="other-parent",
                    )
                ]
            )
    finally:
        execution_context.reset(token)


def test_wait_jobs_rejects_missing_parent_ref_in_execution_context() -> None:
    ctx = ExecutionContext(
        task_id="parent-task",
        owner_id="owner",
        retries=0,
        iterations=0,
        events=cast(EventPublisher, _NoopEvents()),
    )
    token = execution_context.set(ctx)
    try:
        with pytest.raises(ValueError, match="non-empty parent_task_id"):
            wait.jobs(
                [
                    {
                        "task_id": "child-1",
                        "agent_name": "a1",
                    }
                ]
            )
    finally:
        execution_context.reset(token)


def test_wait_cron_rejects_conflicting_timezone_aliases() -> None:
    with pytest.raises(ValueError, match="either 'timezone' or 'tz'"):
        wait.cron("0 * * * *", timezone="Europe/London", tz="America/New_York")

