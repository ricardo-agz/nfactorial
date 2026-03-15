"""Unit tests for Task, TaskMetadata, and TaskStatus."""

import json
import uuid
from typing import Any

import pytest

from factorial.agent.context import AgentContext
from factorial.queue.task import Task, TaskMetadata, TaskStatus


class TestTaskStatus:
    """Tests for TaskStatus enum - focused on contract validation."""

    def test_status_enum_contract(self) -> None:
        """
        Contract test: Verify TaskStatus has all required states.

        This test serves as documentation and catches accidental removal
        of status values. Update this test when adding/removing statuses.
        """
        # Core lifecycle states
        assert TaskStatus.QUEUED.value == "queued"
        assert TaskStatus.PROCESSING.value == "processing"
        assert TaskStatus.ACTIVE.value == "active"

        # Terminal states
        assert TaskStatus.COMPLETED.value == "completed"
        assert TaskStatus.FAILED.value == "failed"
        assert TaskStatus.CANCELLED.value == "cancelled"

        # Pending states
        assert TaskStatus.PENDING_TOOL_RESULTS.value == "pending_tool_results"
        assert TaskStatus.PENDING_CHILD_TASKS.value == "pending_child_tasks"

        # Recovery states
        assert TaskStatus.BACKOFF.value == "backoff"
        assert TaskStatus.PAUSED.value == "paused"

        # Ensure enum values are unique while allowing additive expansion.
        assert len({status.value for status in TaskStatus}) == len(TaskStatus)

    def test_invalid_status_raises(self) -> None:
        """Test that invalid status string raises ValueError."""
        with pytest.raises(ValueError):
            TaskStatus("invalid_status")


class TestTaskMetadata:
    """Tests for TaskMetadata dataclass."""

    def test_to_dict(self, sample_metadata: TaskMetadata) -> None:
        """Test TaskMetadata serialization to dict."""
        data = sample_metadata.to_dict()

        assert data["owner_id"] == sample_metadata.owner_id
        assert data["team_id"] == sample_metadata.team_id
        assert data["parent_id"] == sample_metadata.parent_id
        assert data["resumed_from_task_id"] == sample_metadata.resumed_from_task_id
        assert data["batch_id"] == sample_metadata.batch_id
        assert data["max_turns"] == sample_metadata.max_turns
        assert isinstance(data["created_at"], float)

    def test_from_dict(self, sample_metadata: TaskMetadata) -> None:
        """Test TaskMetadata deserialization from dict."""
        data = sample_metadata.to_dict()
        restored = TaskMetadata.from_dict(data)

        assert restored.owner_id == sample_metadata.owner_id
        assert restored.team_id == sample_metadata.team_id
        assert restored.parent_id == sample_metadata.parent_id
        assert restored.resumed_from_task_id == sample_metadata.resumed_from_task_id
        assert restored.batch_id == sample_metadata.batch_id
        assert restored.max_turns == sample_metadata.max_turns
        # Timestamps should be close (within 1 second)
        assert abs(
            (restored.created_at - sample_metadata.created_at).total_seconds()
        ) < 1

    def test_to_json(self, sample_metadata: TaskMetadata) -> None:
        """Test TaskMetadata serialization to JSON string."""
        json_str = sample_metadata.to_json()
        data = json.loads(json_str)

        assert data["owner_id"] == sample_metadata.owner_id
        assert isinstance(data["created_at"], float)

    def test_from_json(self, sample_metadata: TaskMetadata) -> None:
        """Test TaskMetadata deserialization from JSON string."""
        json_str = sample_metadata.to_json()
        restored = TaskMetadata.from_json(json_str)

        assert restored.owner_id == sample_metadata.owner_id
        assert restored.team_id == sample_metadata.team_id
        assert restored.max_turns == sample_metadata.max_turns

    def test_from_json_bytes(self, sample_metadata: TaskMetadata) -> None:
        """Test TaskMetadata deserialization from JSON bytes."""
        json_bytes = sample_metadata.to_json().encode("utf-8")
        restored = TaskMetadata.from_json(json_bytes)

        assert restored.owner_id == sample_metadata.owner_id
        assert restored.team_id == sample_metadata.team_id

    def test_optional_fields(self, owner_id: str) -> None:
        """Test TaskMetadata with optional fields as None."""
        metadata = TaskMetadata(
            owner_id=owner_id,
            team_id=None,
            parent_id=None,
            batch_id=None,
            max_turns=None,
        )
        data = metadata.to_dict()

        assert data["parent_id"] is None
        assert data["team_id"] is None
        assert data["resumed_from_task_id"] is None
        assert data["batch_id"] is None
        assert data["max_turns"] is None

        restored = TaskMetadata.from_dict(data)
        assert restored.parent_id is None
        assert restored.team_id is None
        assert restored.resumed_from_task_id is None
        assert restored.batch_id is None
        assert restored.max_turns is None

    def test_with_parent_and_batch(self, owner_id: str) -> None:
        """Test TaskMetadata with parent and batch IDs."""
        parent_id = str(uuid.uuid4())
        resumed_from_task_id = str(uuid.uuid4())
        batch_id = str(uuid.uuid4())

        metadata = TaskMetadata(
            owner_id=owner_id,
            team_id=owner_id,
            parent_id=parent_id,
            resumed_from_task_id=resumed_from_task_id,
            batch_id=batch_id,
            max_turns=5,
        )

        data = metadata.to_dict()
        restored = TaskMetadata.from_dict(data)

        assert restored.parent_id == parent_id
        assert restored.team_id == owner_id
        assert restored.resumed_from_task_id == resumed_from_task_id
        assert restored.batch_id == batch_id

    def test_from_dict_without_team_id_uses_default(self, owner_id: str) -> None:
        """Legacy metadata without team_id remains supported."""
        metadata_dict = {
            "owner_id": owner_id,
            "parent_id": None,
            "resumed_from_task_id": None,
            "batch_id": None,
            "created_at": 1234.0,
            "max_turns": None,
        }
        restored = TaskMetadata.from_dict(metadata_dict)
        assert restored.owner_id == owner_id
        assert restored.team_id is None


class TestTask:
    """Tests for Task dataclass."""

    def test_create_factory(self, owner_id: str, agent_name: str) -> None:
        """Test Task.create factory method."""
        context = AgentContext(messages=[{"role": "user", "content": "Test query"}])

        task = Task.create(
            owner_id=owner_id,
            agent=agent_name,
            payload=context,
            batch_id=None,
            max_turns=10,
        )

        assert task.status == TaskStatus.QUEUED
        assert task.agent == agent_name
        assert task.payload.messages == [{"role": "user", "content": "Test query"}]
        assert task.metadata.owner_id == owner_id
        assert task.metadata.max_turns == 10
        assert task.pickups == 0
        assert task.retries == 0
        # ID should be a valid UUID
        uuid.UUID(task.id)

    def test_to_dict(self, sample_task: Task[AgentContext]) -> None:
        """Test Task serialization to dict."""
        data = sample_task.to_dict()

        assert data["id"] == sample_task.id
        assert data["status"] == sample_task.status.value
        assert data["agent"] == sample_task.agent
        assert data["pickups"] == sample_task.pickups
        assert data["retries"] == sample_task.retries
        assert isinstance(data["payload"], dict)
        assert isinstance(data["metadata"], dict)

    def test_from_dict(self, sample_task: Task[AgentContext]) -> None:
        """Test Task deserialization from dict."""
        data = sample_task.to_dict()
        restored = Task.from_dict(data, payload_parser=AgentContext.from_dict)

        assert restored.id == sample_task.id
        assert restored.status == sample_task.status
        assert restored.agent == sample_task.agent
        assert restored.pickups == sample_task.pickups
        assert restored.retries == sample_task.retries
        assert restored.payload.messages == sample_task.payload.messages

    def test_to_json(self, sample_task: Task[AgentContext]) -> None:
        """Test Task serialization to JSON string."""
        json_str = sample_task.to_json()
        data = json.loads(json_str)

        assert data["id"] == sample_task.id
        assert data["status"] == sample_task.status.value

    def test_from_json(self, sample_task: Task[AgentContext]) -> None:
        """Test Task deserialization from JSON string."""
        json_str = sample_task.to_json()
        restored = Task.from_json(json_str, payload_parser=AgentContext.from_dict)

        assert restored.id == sample_task.id
        assert restored.status == sample_task.status
        assert restored.agent == sample_task.agent

    def test_from_json_bytes(self, sample_task: Task[AgentContext]) -> None:
        """Test Task deserialization from JSON bytes."""
        json_bytes = sample_task.to_json().encode("utf-8")
        restored = Task.from_json(json_bytes, payload_parser=AgentContext.from_dict)

        assert restored.id == sample_task.id

    def test_all_status_values_serialize(
        self,
        agent_name: str,
        sample_context: AgentContext,
        sample_metadata: TaskMetadata,
    ) -> None:
        """Test that tasks with all status values serialize/deserialize correctly."""
        for status in TaskStatus:
            task: Task[AgentContext] = Task(
                id=str(uuid.uuid4()),
                status=status,
                agent=agent_name,
                payload=sample_context,
                metadata=sample_metadata,
                pickups=0,
                retries=0,
            )
            data = task.to_dict()
            restored = Task.from_dict(data, payload_parser=AgentContext.from_dict)
            assert restored.status == status

    def test_with_pickups_and_retries(
        self,
        agent_name: str,
        sample_context: AgentContext,
        sample_metadata: TaskMetadata,
    ) -> None:
        """Test task with non-zero pickups and retries."""
        task: Task[AgentContext] = Task(
            id=str(uuid.uuid4()),
            status=TaskStatus.PROCESSING,
            agent=agent_name,
            payload=sample_context,
            metadata=sample_metadata,
            pickups=3,
            retries=2,
        )

        data = task.to_dict()
        restored = Task.from_dict(data, payload_parser=AgentContext.from_dict)

        assert restored.pickups == 3
        assert restored.retries == 2

    def test_payload_with_messages(
        self, agent_name: str, sample_metadata: TaskMetadata
    ) -> None:
        """Test task with context containing messages."""
        context = AgentContext(
            messages=[
                {"role": "user", "content": "Hello"},
                {"role": "assistant", "content": "Hi there!"},
            ],
            turn_number=2,
            output="Some output",
        )

        task: Task[AgentContext] = Task(
            id=str(uuid.uuid4()),
            status=TaskStatus.ACTIVE,
            agent=agent_name,
            payload=context,
            metadata=sample_metadata,
            pickups=1,
            retries=0,
        )

        data = task.to_dict()
        restored = Task.from_dict(data, payload_parser=AgentContext.from_dict)

        assert len(restored.payload.messages) == 2
        assert restored.payload.turn_number == 2
        assert restored.payload.output == "Some output"

    def test_minimal_payload_dict(
        self, agent_name: str, sample_metadata: TaskMetadata
    ) -> None:
        """Test task construction with minimal payload dict."""
        data: dict[str, Any] = {
            "id": str(uuid.uuid4()),
            "status": "queued",
            "agent": agent_name,
            "payload": {
                "messages": [{"role": "user", "content": "minimal query"}]
            },
            "pickups": 0,
            "retries": 0,
            "metadata": sample_metadata.to_dict(),
        }

        task = Task.from_dict(data, payload_parser=AgentContext.from_dict)
        assert task.payload is not None
        assert task.payload.messages == [{"role": "user", "content": "minimal query"}]
        assert task.payload.turn_number == 1

    def test_from_dict_defaults_missing_team_id_to_task_id(
        self,
        agent_name: str,
        sample_metadata: TaskMetadata,
    ) -> None:
        metadata = sample_metadata.to_dict()
        metadata.pop("team_id", None)
        task_id = str(uuid.uuid4())
        task = Task.from_dict(
            {
                "id": task_id,
                "status": "queued",
                "agent": agent_name,
                "payload": {"messages": [{"role": "user", "content": "hello"}]},
                "pickups": 0,
                "retries": 0,
                "metadata": metadata,
            },
            payload_parser=AgentContext.from_dict,
        )
        assert task.metadata.team_id == task_id
