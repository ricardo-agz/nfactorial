from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Generic, TypeAlias, TypeVar

if False:  # pragma: no cover
    from factorial.ai.messages import Message


VerificationMetaT = TypeVar("VerificationMetaT")
OutputT = TypeVar("OutputT")
StateT = TypeVar("StateT")
MetadataT = TypeVar("MetadataT")


@dataclass(frozen=True)
class UsageSummary:
    input_tokens: int = 0
    output_tokens: int = 0
    total_tokens: int = 0

    @classmethod
    def zero(cls) -> UsageSummary:
        return cls()

    @classmethod
    def from_provider_usage(cls, usage: Any) -> UsageSummary:
        if usage is None:
            return cls.zero()

        input_tokens = int(
            getattr(usage, "prompt_tokens", 0)
            or getattr(usage, "input_tokens", 0)
            or 0
        )
        output_tokens = int(
            getattr(usage, "completion_tokens", 0)
            or getattr(usage, "output_tokens", 0)
            or 0
        )
        total_tokens = int(
            getattr(usage, "total_tokens", 0)
            or input_tokens + output_tokens
        )
        return cls(
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            total_tokens=total_tokens,
        )

    def add(self, other: UsageSummary) -> UsageSummary:
        return UsageSummary(
            input_tokens=self.input_tokens + other.input_tokens,
            output_tokens=self.output_tokens + other.output_tokens,
            total_tokens=self.total_tokens + other.total_tokens,
        )


@dataclass(frozen=True)
class TurnSummary:
    turn_number: int
    finish_reason: str
    status: str
    output: object | None
    usage: UsageSummary = field(default_factory=UsageSummary.zero)


class RunStatus(str, Enum):
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass(frozen=True)
class RunError:
    type: str
    message: str
    traceback: str | None = None

    @classmethod
    def from_exception(cls, exc: BaseException) -> RunError:
        return cls(type=type(exc).__name__, message=str(exc))


@dataclass(frozen=True)
class VerifierAccept(Generic[VerificationMetaT]):
    metadata: VerificationMetaT | None = None


@dataclass(frozen=True)
class VerifierRetry(Generic[VerificationMetaT]):
    message: str
    code: str | None = None
    metadata: VerificationMetaT | None = None


@dataclass(frozen=True)
class VerifierFail(Generic[VerificationMetaT]):
    message: str
    code: str | None = None
    metadata: VerificationMetaT | None = None


VerifierDecision: TypeAlias = (
    VerifierAccept[VerificationMetaT]
    | VerifierRetry[VerificationMetaT]
    | VerifierFail[VerificationMetaT]
)


@dataclass(frozen=True)
class VerificationSummary(Generic[VerificationMetaT]):
    status: str
    attempts_used: int
    code: str | None = None
    message: str | None = None
    metadata: VerificationMetaT | None = None


class verify:
    @staticmethod
    def accept(
        *,
        metadata: VerificationMetaT | None = None,
    ) -> VerifierAccept[VerificationMetaT]:
        return VerifierAccept(metadata=metadata)

    @staticmethod
    def retry(
        message: str,
        *,
        code: str | None = None,
        metadata: VerificationMetaT | None = None,
    ) -> VerifierRetry[VerificationMetaT]:
        return VerifierRetry(message=message, code=code, metadata=metadata)

    @staticmethod
    def fail(
        message: str,
        *,
        code: str | None = None,
        metadata: VerificationMetaT | None = None,
    ) -> VerifierFail[VerificationMetaT]:
        return VerifierFail(message=message, code=code, metadata=metadata)


@dataclass(frozen=True)
class RunResult(Generic[OutputT, StateT, MetadataT]):
    run_id: str
    task_id: str | None
    agent_name: str
    owner_id: str | None
    status: RunStatus
    output: OutputT | None
    state: StateT
    metadata: MetadataT
    messages: tuple[Message, ...]
    usage: UsageSummary
    turn_count: int
    last_turn: TurnSummary | None = None
    verification: VerificationSummary[Any] | None = None
    started_at: datetime = field(default_factory=datetime.utcnow)
    finished_at: datetime | None = None
    error: RunError | None = None


__all__ = [
    "RunError",
    "RunResult",
    "RunStatus",
    "TurnSummary",
    "UsageSummary",
    "VerificationSummary",
    "VerifierAccept",
    "VerifierDecision",
    "VerifierFail",
    "VerifierRetry",
    "verify",
]
