"""Unit tests for worker utility functions."""

import asyncio
import json

from factorial.core.exceptions import (
    FatalAgentError,
    InvalidLLMResponseError,
    RateLimitError,
    RetryableError,
)
from factorial.queue.worker import (
    CompletionAction,
    classify_failure,
    steering_message_sort_key,
)


class TestClassifyFailure:
    """Tests for classify_failure function."""

    def test_timeout_error_retries(self) -> None:
        """Test that asyncio.TimeoutError leads to RETRY."""
        exc = asyncio.TimeoutError("Task timed out")
        action, output = classify_failure(exc, retries=0, max_retries=3)

        assert action == CompletionAction.RETRY
        assert output is None

    def test_timeout_error_fails_after_max_retries(self) -> None:
        """Test that TimeoutError fails when max retries reached."""
        exc = asyncio.TimeoutError("Task timed out")
        action, output = classify_failure(exc, retries=3, max_retries=3)

        assert action == CompletionAction.FAIL
        assert output is not None
        data = json.loads(output)
        assert "error" in data
        assert "timed out" in data["error"].lower()

    def test_retryable_exception_backs_off(self) -> None:
        """Test that retryable exceptions lead to BACKOFF."""
        exc = RateLimitError("Rate limited")
        action, output = classify_failure(exc, retries=0, max_retries=3)

        assert action == CompletionAction.BACKOFF
        assert output is None

    def test_retryable_exception_fails_after_max_retries(self) -> None:
        """Test that retryable exceptions fail when max retries reached."""
        exc = RateLimitError("Rate limited")
        action, output = classify_failure(exc, retries=3, max_retries=3)

        assert action == CompletionAction.FAIL
        assert output is not None
        data = json.loads(output)
        assert "error" in data

    def test_invalid_llm_response_backs_off(self) -> None:
        """Test InvalidLLMResponseError leads to BACKOFF."""
        exc = InvalidLLMResponseError("Invalid response format")
        action, output = classify_failure(exc, retries=0, max_retries=3)

        assert action == CompletionAction.BACKOFF
        assert output is None

    def test_fatal_agent_error_fails_immediately(self) -> None:
        """Test FatalAgentError fails immediately without retry."""
        exc = FatalAgentError("Unrecoverable error")
        action, output = classify_failure(exc, retries=0, max_retries=3)

        assert action == CompletionAction.FAIL
        assert output is not None
        data = json.loads(output)
        assert "Unrecoverable error" in data["error"]

    def test_fatal_agent_error_ignores_retry_count(self) -> None:
        """Test FatalAgentError fails even with retries remaining."""
        exc = FatalAgentError("Critical failure")
        action, output = classify_failure(exc, retries=0, max_retries=10)

        assert action == CompletionAction.FAIL
        assert output is not None

    def test_generic_exception_retries(self) -> None:
        """Test that generic exceptions lead to RETRY."""
        exc = ValueError("Something went wrong")
        action, output = classify_failure(exc, retries=0, max_retries=3)

        assert action == CompletionAction.RETRY
        assert output is None

    def test_generic_exception_fails_after_max_retries(self) -> None:
        """Test generic exceptions fail when max retries reached."""
        exc = ValueError("Something went wrong")
        action, output = classify_failure(exc, retries=3, max_retries=3)

        assert action == CompletionAction.FAIL
        assert output is not None
        data = json.loads(output)
        assert "Something went wrong" in data["error"]

    def test_retryable_error_backs_off(self) -> None:
        """Test RetryableError leads to BACKOFF."""
        exc = RetryableError("Temporary failure")
        action, output = classify_failure(exc, retries=0, max_retries=3)

        assert action == CompletionAction.BACKOFF
        assert output is None

    def test_zero_max_retries_fails_immediately(self) -> None:
        """Test that zero max_retries fails immediately for retryable errors."""
        exc = ValueError("Some error")
        action, output = classify_failure(exc, retries=0, max_retries=0)

        assert action == CompletionAction.FAIL
        assert output is not None

    def test_error_message_preserved_in_output(self) -> None:
        """Test that original error message is preserved in failure output."""
        error_message = "Specific error details XYZ-123"
        exc = ValueError(error_message)
        action, output = classify_failure(exc, retries=3, max_retries=3)

        assert action == CompletionAction.FAIL
        assert output is not None
        data = json.loads(output)
        assert error_message in data["error"]

    def test_boundary_retry_count(self) -> None:
        """Test behavior at exact retry boundary."""
        exc = ValueError("Error")

        # At max_retries - 1, should still retry
        action1, output1 = classify_failure(exc, retries=2, max_retries=3)
        assert action1 == CompletionAction.RETRY
        assert output1 is None

        # At max_retries, should fail
        action2, output2 = classify_failure(exc, retries=3, max_retries=3)
        assert action2 == CompletionAction.FAIL
        assert output2 is not None

        # Above max_retries, should fail
        action3, output3 = classify_failure(exc, retries=5, max_retries=3)
        assert action3 == CompletionAction.FAIL
        assert output3 is not None


class TestSteeringOrdering:
    """Tests for steering message ordering helpers."""

    def test_steering_sort_key_orders_by_timestamp_then_sequence(self) -> None:
        message_ids = [
            "1700000000000_12",
            "1700000000000_2",
            "1699999999999_99",
            "1700000000001_1",
        ]
        sorted_ids = sorted(message_ids, key=steering_message_sort_key)
        assert sorted_ids == [
            "1699999999999_99",
            "1700000000000_2",
            "1700000000000_12",
            "1700000000001_1",
        ]

    def test_steering_sort_key_handles_nonstandard_ids(self) -> None:
        message_ids = [
            "bad",
            "1700000000000_x",
            "1700000000000_1",
        ]
        sorted_ids = sorted(message_ids, key=steering_message_sort_key)
        assert sorted_ids == [
            "bad",
            "1700000000000_x",
            "1700000000000_1",
        ]
