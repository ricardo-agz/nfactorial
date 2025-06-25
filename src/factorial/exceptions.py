from openai import (
    RateLimitError as OpenAIRateLimitError,
    InternalServerError as OpenAIInternalServerError,
    APITimeoutError as OpenAITimeoutError,
    APIConnectionError as OpenAIConnectionError,
)


class TaskNotFoundError(Exception):
    """Exception raised when a task is not found"""

    def __init__(self, task_id: str):
        self.task_id = task_id
        super().__init__(f"Task {task_id} not found")


class InvalidTaskIdError(Exception):
    """Exception raised when a task ID is invalid"""

    def __init__(self, task_id: str):
        self.task_id = task_id
        super().__init__(f"Invalid task ID format: {task_id}")


class InactiveTaskError(Exception):
    """Exception raised when trying to control a task that is not active"""

    def __init__(self, task_id: str):
        self.task_id = task_id
        super().__init__(f"Task {task_id} is not active")


class CorruptedTaskDataError(Exception):
    """Exception raised when task data is corrupted"""

    def __init__(self, task_id: str, missing_fields: list[str]):
        self.task_id = task_id
        super().__init__(
            f"Task {task_id} data is corrupted, missing fields: {missing_fields}"
        )


class RetryableError(Exception):
    """Error that should be retried"""

    pass


class RateLimitError(Exception):
    """Rate limit error"""

    pass


class FatalAgentError(Exception):
    """Exception raised to indicate an unrecoverable error which should fail the task immediately (no retry)."""

    pass


RETRYABLE_EXCEPTIONS = (
    OpenAIRateLimitError,
    OpenAIInternalServerError,
    OpenAIConnectionError,
    OpenAITimeoutError,
    RetryableError,
    RateLimitError,
)

__all__ = [
    "TaskNotFoundError",
    "InvalidTaskIdError",
    "InactiveTaskError",
    "CorruptedTaskDataError",
    "RetryableError",
    "RateLimitError",
    "RETRYABLE_EXCEPTIONS",
    "OpenAIRateLimitError",
    "OpenAIInternalServerError",
    "OpenAITimeoutError",
    "OpenAIConnectionError",
    "FatalAgentError",
]
