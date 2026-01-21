"""Unit tests for RedisKeys key generation."""

import pytest

from factorial.queue.keys import (
    PENDING_SENTINEL,
    RedisKeys,
)


class TestRedisKeysFormat:
    """Tests for RedisKeys.format() method."""

    def test_namespace_only(self, namespace: str) -> None:
        """Test key generation with namespace only."""
        keys = RedisKeys.format(namespace=namespace)

        assert keys.task_status == f"{namespace}:tasks:status"
        assert keys.task_agent == f"{namespace}:tasks:agent"
        assert keys.task_payload == f"{namespace}:tasks:payload"
        assert keys.task_pickups == f"{namespace}:tasks:pickups"
        assert keys.task_retries == f"{namespace}:tasks:retries"
        assert keys.task_meta == f"{namespace}:tasks:meta"
        assert keys.task_cancellations == f"{namespace}:cancel:pending"
        assert keys.global_metrics_bucket == f"{namespace}:metrics:__all__"

    def test_with_agent(self, namespace: str, agent_name: str) -> None:
        """Test key generation with namespace and agent."""
        keys = RedisKeys.format(namespace=namespace, agent=agent_name)

        assert keys.queue_main == f"{namespace}:queue:{agent_name}:main"
        assert keys.queue_completions == f"{namespace}:queue:{agent_name}:completed"
        assert keys.queue_failed == f"{namespace}:queue:{agent_name}:failed"
        assert keys.queue_backoff == f"{namespace}:queue:{agent_name}:backoff"
        assert keys.queue_orphaned == f"{namespace}:queue:{agent_name}:orphaned"
        assert keys.queue_pending == f"{namespace}:queue:{agent_name}:pending"
        assert keys.queue_cancelled == f"{namespace}:queue:{agent_name}:cancelled"
        assert (
            keys.processing_heartbeats
            == f"{namespace}:processing:{agent_name}:heartbeats"
        )
        assert keys.agent_metrics_bucket == f"{namespace}:metrics:{agent_name}"

    def test_with_task_id(self, namespace: str, task_id: str) -> None:
        """Test key generation with namespace and task_id."""
        keys = RedisKeys.format(namespace=namespace, task_id=task_id)

        assert keys.task_steering == f"{namespace}:steer:{task_id}:messages"
        assert keys.pending_tool_results == f"{namespace}:pending:{task_id}:tools"
        assert (
            keys.pending_child_task_results
            == f"{namespace}:pending:{task_id}:children"
        )

    def test_with_owner_id(self, namespace: str, owner_id: str) -> None:
        """Test key generation with namespace and owner_id."""
        keys = RedisKeys.format(namespace=namespace, owner_id=owner_id)

        assert keys.updates_channel == f"{namespace}:updates:{owner_id}"

    def test_full_format(
        self, namespace: str, agent_name: str, task_id: str, owner_id: str
    ) -> None:
        """Test key generation with all parameters."""
        keys = RedisKeys.format(
            namespace=namespace,
            agent=agent_name,
            task_id=task_id,
            owner_id=owner_id,
        )

        # All keys should be accessible
        assert keys.task_status is not None
        assert keys.queue_main is not None
        assert keys.task_steering is not None
        assert keys.updates_channel is not None

    def test_batch_keys(self, namespace: str) -> None:
        """Test batch-related key generation."""
        keys = RedisKeys.format(namespace=namespace)

        assert keys.batch_meta == f"{namespace}:batches:metadata"
        assert keys.batch_tasks == f"{namespace}:batches:tasks"
        assert keys.batch_remaining_tasks == f"{namespace}:batches:remaining"
        assert keys.batch_progress == f"{namespace}:batches:progress"
        assert keys.batch_completed == f"{namespace}:batches:completed"


class TestRedisKeysAccessErrors:
    """Tests for RedisKeys access errors when required parameters not provided."""

    @pytest.mark.parametrize(
        "attr_name,error_match",
        [
            ("queue_main", "agent was not provided"),
            ("queue_completions", "agent was not provided"),
            ("queue_backoff", "agent was not provided"),
            ("queue_failed", "agent was not provided"),
            ("queue_orphaned", "agent was not provided"),
            ("queue_pending", "agent was not provided"),
            ("queue_cancelled", "agent was not provided"),
            ("processing_heartbeats", "agent was not provided"),
            ("agent_metrics_bucket", "agent was not provided"),
            ("task_steering", "task_id was not provided"),
            ("pending_tool_results", "task_id was not provided"),
            ("pending_child_task_results", "task_id was not provided"),
            ("updates_channel", "owner_id was not provided"),
        ],
    )
    def test_missing_parameter_raises_value_error(
        self, namespace: str, attr_name: str, error_match: str
    ) -> None:
        """Test accessing keys without required parameters raises ValueError."""
        keys = RedisKeys.format(namespace=namespace)

        with pytest.raises(ValueError, match=error_match):
            getattr(keys, attr_name)


class TestRedisKeysConvenienceMethods:
    """Tests for RedisKeys convenience class methods."""

    def test_for_agent(self, namespace: str, agent_name: str) -> None:
        """Test RedisKeys.for_agent convenience method."""
        keys = RedisKeys.for_agent(namespace=namespace, agent=agent_name)

        assert keys.queue_main == f"{namespace}:queue:{agent_name}:main"
        assert keys.task_status == f"{namespace}:tasks:status"

    def test_for_task(self, namespace: str, task_id: str) -> None:
        """Test RedisKeys.for_task convenience method."""
        keys = RedisKeys.for_task(namespace=namespace, task_id=task_id)

        assert keys.task_steering == f"{namespace}:steer:{task_id}:messages"
        assert keys.pending_tool_results == f"{namespace}:pending:{task_id}:tools"

    def test_for_owner(self, namespace: str, owner_id: str) -> None:
        """Test RedisKeys.for_owner convenience method."""
        keys = RedisKeys.for_owner(namespace=namespace, owner_id=owner_id)

        assert keys.updates_channel == f"{namespace}:updates:{owner_id}"


class TestPendingSentinel:
    """Tests for pending sentinel value."""

    def test_sentinel_contract(self) -> None:
        """
        Contract test: PENDING_SENTINEL must be a unique string that cannot
        appear in valid JSON data (tool results, child outputs, etc).

        Uses special characters to ensure collision is extremely unlikely.
        """
        assert PENDING_SENTINEL == "<|*PENDING*|>"
        # Contains special chars that would need escaping in JSON strings
        assert "|" in PENDING_SENTINEL
        assert "*" in PENDING_SENTINEL
        assert "<" in PENDING_SENTINEL
