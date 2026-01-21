"""Unit tests for worker utility functions."""

import asyncio
import json

from factorial.exceptions import (
    FatalAgentError,
    InvalidLLMResponseError,
    RateLimitError,
    RetryableError,
)
from factorial.queue.worker import CompletionAction, classify_failure


class TestCompletionAction:
    """Tests for CompletionAction enum."""

    def test_all_actions_are_strings(self) -> None:
        """Verify all CompletionAction values are strings."""
        for action in CompletionAction:
            assert isinstance(action.value, str)

    def test_expected_actions_exist(self) -> None:
        """Verify all expected actions exist."""
        expected = [
            "continue",
            "pending_tool_call_results",
            "pending_child_task_results",
            "complete",
            "retry",
            "backoff",
            "fail",
        ]
        actual = [a.value for a in CompletionAction]
        assert set(expected) == set(actual)

    def test_action_from_string(self) -> None:
        """Test creating CompletionAction from string value."""
        assert CompletionAction("continue") == CompletionAction.CONTINUE
        assert CompletionAction("complete") == CompletionAction.COMPLETE
        assert CompletionAction("retry") == CompletionAction.RETRY
        assert CompletionAction("backoff") == CompletionAction.BACKOFF
        assert CompletionAction("fail") == CompletionAction.FAIL


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
