"""Unit tests for Task, TaskMetadata, and TaskStatus."""

import json
import uuid
from typing import Any

import pytest

from factorial.context import AgentContext
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

        # Verify total count matches expectations (catches accidental additions)
        assert len(TaskStatus) == 10

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
        assert data["parent_id"] == sample_metadata.parent_id
        assert data["batch_id"] == sample_metadata.batch_id
        assert data["max_turns"] == sample_metadata.max_turns
        assert isinstance(data["created_at"], float)

    def test_from_dict(self, sample_metadata: TaskMetadata) -> None:
        """Test TaskMetadata deserialization from dict."""
        data = sample_metadata.to_dict()
        restored = TaskMetadata.from_dict(data)

        assert restored.owner_id == sample_metadata.owner_id
        assert restored.parent_id == sample_metadata.parent_id
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
        assert restored.max_turns == sample_metadata.max_turns

    def test_from_json_bytes(self, sample_metadata: TaskMetadata) -> None:
        """Test TaskMetadata deserialization from JSON bytes."""
        json_bytes = sample_metadata.to_json().encode("utf-8")
        restored = TaskMetadata.from_json(json_bytes)

        assert restored.owner_id == sample_metadata.owner_id

    def test_optional_fields(self, owner_id: str) -> None:
        """Test TaskMetadata with optional fields as None."""
        metadata = TaskMetadata(
            owner_id=owner_id,
            parent_id=None,
            batch_id=None,
            max_turns=None,
        )
        data = metadata.to_dict()

        assert data["parent_id"] is None
        assert data["batch_id"] is None
        assert data["max_turns"] is None

        restored = TaskMetadata.from_dict(data)
        assert restored.parent_id is None
        assert restored.batch_id is None
        assert restored.max_turns is None

    def test_with_parent_and_batch(self, owner_id: str) -> None:
        """Test TaskMetadata with parent and batch IDs."""
        parent_id = str(uuid.uuid4())
        batch_id = str(uuid.uuid4())

        metadata = TaskMetadata(
            owner_id=owner_id,
            parent_id=parent_id,
            batch_id=batch_id,
            max_turns=5,
        )

        data = metadata.to_dict()
        restored = TaskMetadata.from_dict(data)

        assert restored.parent_id == parent_id
        assert restored.batch_id == batch_id


class TestTask:
    """Tests for Task dataclass."""

    def test_create_factory(self, owner_id: str, agent_name: str) -> None:
        """Test Task.create factory method."""
        context = AgentContext(query="Test query")

        task = Task.create(
            owner_id=owner_id,
            agent=agent_name,
            payload=context,
            batch_id=None,
            max_turns=10,
        )

        assert task.status == TaskStatus.QUEUED
        assert task.agent == agent_name
        assert task.payload.query == "Test query"
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
        restored = Task.from_dict(data, context_class=AgentContext)

        assert restored.id == sample_task.id
        assert restored.status == sample_task.status
        assert restored.agent == sample_task.agent
        assert restored.pickups == sample_task.pickups
        assert restored.retries == sample_task.retries
        assert restored.payload.query == sample_task.payload.query

    def test_to_json(self, sample_task: Task[AgentContext]) -> None:
        """Test Task serialization to JSON string."""
        json_str = sample_task.to_json()
        data = json.loads(json_str)

        assert data["id"] == sample_task.id
        assert data["status"] == sample_task.status.value

    def test_from_json(self, sample_task: Task[AgentContext]) -> None:
        """Test Task deserialization from JSON string."""
        json_str = sample_task.to_json()
        restored = Task.from_json(json_str, context_class=AgentContext)

        assert restored.id == sample_task.id
        assert restored.status == sample_task.status
        assert restored.agent == sample_task.agent

    def test_from_json_bytes(self, sample_task: Task[AgentContext]) -> None:
        """Test Task deserialization from JSON bytes."""
        json_bytes = sample_task.to_json().encode("utf-8")
        restored = Task.from_json(json_bytes, context_class=AgentContext)

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
            restored = Task.from_dict(data, context_class=AgentContext)
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
        restored = Task.from_dict(data, context_class=AgentContext)

        assert restored.pickups == 3
        assert restored.retries == 2

    def test_payload_with_messages(
        self, agent_name: str, sample_metadata: TaskMetadata
    ) -> None:
        """Test task with context containing messages."""
        context = AgentContext(
            query="Test query",
            messages=[
                {"role": "user", "content": "Hello"},
                {"role": "assistant", "content": "Hi there!"},
            ],
            turn=1,
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
        restored = Task.from_dict(data, context_class=AgentContext)

        assert len(restored.payload.messages) == 2
        assert restored.payload.turn == 1
        assert restored.payload.output == "Some output"

    def test_minimal_payload_dict(
        self, agent_name: str, sample_metadata: TaskMetadata
    ) -> None:
        """Test task construction with minimal payload dict."""
        data: dict[str, Any] = {
            "id": str(uuid.uuid4()),
            "status": "queued",
            "agent": agent_name,
            "payload": {"query": "minimal query"},  # Minimal required fields
            "pickups": 0,
            "retries": 0,
            "metadata": sample_metadata.to_dict(),
        }

        task = Task.from_dict(data, context_class=AgentContext)
        assert task.payload is not None
        assert task.payload.query == "minimal query"
        assert task.payload.turn == 0  # Default value
