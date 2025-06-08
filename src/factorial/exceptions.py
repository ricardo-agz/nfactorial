from openai import (
    RateLimitError as OpenAIRateLimitError,
    InternalServerError as OpenAIInternalServerError,
    APITimeoutError as OpenAITimeoutError,
    APIConnectionError as OpenAIConnectionError,
)


class RetryableError(Exception):
    """Error that should be retried"""

    pass


class RateLimitError(Exception):
    """Rate limit error"""

    pass


RETRYABLE_EXCEPTIONS = (
    OpenAIRateLimitError,
    OpenAIInternalServerError,
    OpenAIConnectionError,
    OpenAITimeoutError,
    RetryableError,
    RateLimitError,
)
