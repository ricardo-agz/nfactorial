from openai import (
    APIConnectionError as OpenAIConnectionError,
    APITimeoutError as OpenAITimeoutError,
    AuthenticationError as OpenAIAuthenticationError,
    BadRequestError as OpenAIBadRequestError,
    InternalServerError as OpenAIInternalServerError,
    PermissionDeniedError as OpenAIPermissionDeniedError,
    RateLimitError as OpenAIRateLimitError,
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


class HookNotFoundError(Exception):
    """Exception raised when a hook is not found."""

    def __init__(self, hook_id: str):
        self.hook_id = hook_id
        super().__init__(f"Hook {hook_id} not found")


class HookTokenValidationError(Exception):
    """Exception raised when a hook token is invalid."""

    def __init__(self, hook_id: str):
        self.hook_id = hook_id
        super().__init__(f"Hook token is invalid for hook {hook_id}")


class HookExpiredError(Exception):
    """Exception raised when a hook is already expired."""

    def __init__(self, hook_id: str):
        self.hook_id = hook_id
        super().__init__(f"Hook {hook_id} is expired")


class HookAlreadyResolvedError(Exception):
    """Exception raised when a hook was already resolved."""

    def __init__(self, hook_id: str):
        self.hook_id = hook_id
        super().__init__(f"Hook {hook_id} is already resolved")


class CorruptedTaskDataError(Exception):
    """Exception raised when task data is corrupted"""

    def __init__(self, task_id: str, missing_fields: list[str]):
        self.task_id = task_id
        super().__init__(
            f"Task {task_id} data is corrupted, missing fields: {missing_fields}"
        )


class BatchNotFoundError(Exception):
    """Exception raised when a batch is not found"""

    def __init__(self, batch_id: str):
        self.batch_id = batch_id
        super().__init__(f"Batch {batch_id} not found")


class RetryableError(Exception):
    """Error that should be retried"""

    pass


class RateLimitError(Exception):
    """Rate limit error"""

    pass


class InvalidLLMResponseError(Exception):
    """Exception raised when an LLM response is invalid"""

    pass


class FatalAgentError(Exception):
    """Exception for unrecoverable errors that should fail the task immediately.

    When this exception is raised, the task will fail without retry.
    """

    pass


RETRYABLE_EXCEPTIONS = (
    OpenAIBadRequestError,
    OpenAIAuthenticationError,
    OpenAIPermissionDeniedError,
    OpenAIRateLimitError,
    OpenAIInternalServerError,
    OpenAIConnectionError,
    OpenAITimeoutError,
    RetryableError,
    RateLimitError,
    InvalidLLMResponseError,
)

__all__ = [
    "TaskNotFoundError",
    "InvalidTaskIdError",
    "InactiveTaskError",
    "HookNotFoundError",
    "HookTokenValidationError",
    "HookExpiredError",
    "HookAlreadyResolvedError",
    "CorruptedTaskDataError",
    "BatchNotFoundError",
    "RetryableError",
    "RateLimitError",
    "RETRYABLE_EXCEPTIONS",
    "OpenAIRateLimitError",
    "OpenAIInternalServerError",
    "OpenAITimeoutError",
    "OpenAIConnectionError",
    "FatalAgentError",
    "InvalidLLMResponseError",
]
