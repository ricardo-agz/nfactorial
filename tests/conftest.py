"""Shared test fixtures for queue tests."""

import uuid
from datetime import datetime, timezone
from typing import Any

import pytest

from factorial.context import AgentContext
from factorial.queue.task import Task, TaskMetadata, TaskStatus


@pytest.fixture
def namespace() -> str:
    """Unique namespace for test isolation."""
    return f"test:{uuid.uuid4().hex[:8]}"


@pytest.fixture
def owner_id() -> str:
    """Test owner ID."""
    return str(uuid.uuid4())


@pytest.fixture
def task_id() -> str:
    """Test task ID."""
    return str(uuid.uuid4())


@pytest.fixture
def agent_name() -> str:
    """Test agent name."""
    return "test_agent"


@pytest.fixture
def sample_context() -> AgentContext:
    """Create a sample AgentContext for testing."""
    return AgentContext(
        query="Test query",
        messages=[],
        turn=0,
        output=None,
    )


@pytest.fixture
def sample_metadata(owner_id: str) -> TaskMetadata:
    """Create sample task metadata for testing."""
    return TaskMetadata(
        owner_id=owner_id,
        parent_id=None,
        batch_id=None,
        created_at=datetime.now(timezone.utc),
        max_turns=10,
    )


@pytest.fixture
def sample_task(
    agent_name: str, sample_context: AgentContext, sample_metadata: TaskMetadata
) -> Task[AgentContext]:
    """Create a sample task for testing."""
    return Task(
        id=str(uuid.uuid4()),
        status=TaskStatus.QUEUED,
        agent=agent_name,
        payload=sample_context,
        metadata=sample_metadata,
        pickups=0,
        retries=0,
    )


@pytest.fixture
def sample_task_dict(sample_task: Task[AgentContext]) -> dict[str, Any]:
    """Get sample task as dictionary."""
    return sample_task.to_dict()
