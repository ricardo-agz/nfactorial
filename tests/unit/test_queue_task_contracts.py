"""Focused internal contracts for queue task persistence."""

from __future__ import annotations

import uuid

import pytest

from factorial.agent.context import AgentContext
from factorial.core.exceptions import CorruptedTaskDataError
from factorial.queue.task import Task, TaskMetadata, TaskStatus, task_team_id


def test_task_status_rejects_invalid_value() -> None:
    with pytest.raises(ValueError):
        TaskStatus("not-a-real-status")


def test_task_roundtrip_preserves_payload_and_counters(
    agent_name: str,
    sample_context: AgentContext,
    sample_metadata: TaskMetadata,
) -> None:
    task = Task[AgentContext](
        id=str(uuid.uuid4()),
        status=TaskStatus.PROCESSING,
        agent=agent_name,
        payload=sample_context,
        metadata=sample_metadata,
        pickups=2,
        retries=1,
    )

    restored = Task.from_dict(task.to_dict(), payload_parser=AgentContext.from_dict)

    assert restored.id == task.id
    assert restored.status is TaskStatus.PROCESSING
    assert restored.pickups == 2
    assert restored.retries == 1
    assert restored.payload.messages == sample_context.messages


def test_task_from_dict_rejects_missing_team_id(
    agent_name: str,
    sample_metadata: TaskMetadata,
) -> None:
    corrupted_metadata = sample_metadata.to_dict()
    corrupted_metadata.pop("team_id", None)

    with pytest.raises(CorruptedTaskDataError, match="metadata.team_id"):
        Task.from_dict(
            {
                "id": str(uuid.uuid4()),
                "status": TaskStatus.QUEUED.value,
                "agent": agent_name,
                "payload": {"messages": [{"role": "user", "content": "hello"}]},
                "pickups": 0,
                "retries": 0,
                "metadata": corrupted_metadata,
            },
            payload_parser=AgentContext.from_dict,
        )


def test_task_team_id_requires_non_empty_team_id(task_id: str) -> None:
    with pytest.raises(CorruptedTaskDataError, match="metadata.team_id"):
        task_team_id(task_id=task_id, metadata={"owner_id": "owner-1", "team_id": None})
