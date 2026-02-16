from datetime import datetime, timezone
from typing import Any, cast

import pytest

from factorial.context import ExecutionContext, execution_context
from factorial.events import EventPublisher
from factorial.subagents import JobRef
from factorial.waits import next_cron_wake_timestamp, wait


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


class _NoopEvents:
    async def publish_event(self, _event: Any) -> None:
        return None


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
        instruction = wait.jobs(jobs, message="wait for all jobs")
    finally:
        execution_context.reset(token)

    assert instruction.kind == "jobs"
    assert instruction.child_task_ids == ["child-1", "child-2"]
    assert instruction.message == "wait for all jobs"


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
        with pytest.raises(ValueError):
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
        with pytest.raises(ValueError):
            wait.jobs(
                [
                    {
                        "task_id": "child-1",
                        "agent_name": "a1",
                        # Missing parent_task_id should be rejected while running.
                    }
                ]
            )
    finally:
        execution_context.reset(token)
