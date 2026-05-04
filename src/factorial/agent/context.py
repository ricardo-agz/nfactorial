from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Generic

from pydantic import TypeAdapter
from typing_extensions import TypeVar

from factorial._internal.serialization import serialize_data
from factorial.ai.messages import Message


@dataclass
class EmptyState:
    pass


@dataclass
class EmptyMetadata:
    pass


StateT = TypeVar("StateT")
MetadataT = TypeVar("MetadataT", default=EmptyMetadata)
ContextType = TypeVar("ContextType", bound="AgentContext[Any, Any]")


@dataclass
class VerificationState:
    attempts_used: int = 0
    last_candidate_hash: str | None = None
    last_outcome: str | None = None
    last_code: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "attempts_used": self.attempts_used,
            "last_candidate_hash": self.last_candidate_hash,
            "last_outcome": self.last_outcome,
            "last_code": self.last_code,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any] | None) -> VerificationState:
        if not isinstance(data, dict):
            return cls()
        return cls(
            attempts_used=int(data.get("attempts_used", 0)),
            last_candidate_hash=(
                str(data["last_candidate_hash"])
                if data.get("last_candidate_hash") is not None
                else None
            ),
            last_outcome=(
                str(data["last_outcome"])
                if data.get("last_outcome") is not None
                else None
            ),
            last_code=(
                str(data["last_code"]) if data.get("last_code") is not None else None
            ),
        )


def _default_value_for_type(
    target_type: Any,
    fallback_factory: Callable[[], Any],
) -> Any:
    if target_type in (Any, object, None):
        return fallback_factory()
    if isinstance(target_type, type):
        try:
            return target_type()
        except Exception:
            return fallback_factory()
    return fallback_factory()


def _coerce_typed_value(
    value: Any,
    target_type: Any,
    *,
    default_factory: Callable[[], Any],
) -> Any:
    if value is None:
        return default_factory()
    if target_type in (Any, object, None):
        return value
    if isinstance(target_type, type) and isinstance(value, target_type):
        return value
    try:
        return TypeAdapter(target_type).validate_python(value)
    except Exception:
        return value


@dataclass
class AgentContext(Generic[StateT, MetadataT]):
    messages: list[Message] = field(default_factory=list)
    turn_number: int = 1
    output: object | None = None
    attempt_number: int = 1
    state: StateT = field(default_factory=EmptyState)  # type: ignore[assignment]
    metadata: MetadataT = field(default_factory=EmptyMetadata)  # type: ignore[assignment]
    verification: VerificationState = field(default_factory=VerificationState)

    def to_dict(self) -> dict[str, Any]:
        return {
            "messages": serialize_data(self.messages),
            "turn_number": self.turn_number,
            "output": serialize_data(self.output),
            "attempt_number": self.attempt_number,
            "state": serialize_data(self.state),
            "metadata": serialize_data(self.metadata),
            "verification": self.verification.to_dict(),
        }

    @classmethod
    def from_dict(
        cls,
        data: dict[str, Any],
        *,
        state_type: Any = EmptyState,
        metadata_type: Any = EmptyMetadata,
    ) -> AgentContext[Any, Any]:
        raw_messages = data.get("messages", [])
        messages = raw_messages if isinstance(raw_messages, list) else []
        return cls(
            messages=list(messages),
            turn_number=int(data.get("turn_number", 1)),
            output=data.get("output"),
            attempt_number=int(data.get("attempt_number", 1)),
            state=_coerce_typed_value(
                data.get("state"),
                state_type,
                default_factory=lambda: _default_value_for_type(
                    state_type,
                    EmptyState,
                ),
            ),
            metadata=_coerce_typed_value(
                data.get("metadata"),
                metadata_type,
                default_factory=lambda: _default_value_for_type(
                    metadata_type,
                    EmptyMetadata,
                ),
            ),
            verification=VerificationState.from_dict(
                data.get("verification")
                if isinstance(data.get("verification"), dict)
                else None
            ),
        )

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False)

    @classmethod
    def from_json(
        cls,
        json_str: str,
        *,
        state_type: Any = EmptyState,
        metadata_type: Any = EmptyMetadata,
    ) -> AgentContext[Any, Any]:
        return cls.from_dict(
            json.loads(json_str),
            state_type=state_type,
            metadata_type=metadata_type,
        )

__all__ = [
    "AgentContext",
    "ContextType",
    "EmptyMetadata",
    "EmptyState",
    "MetadataT",
    "StateT",
    "VerificationState",
]
