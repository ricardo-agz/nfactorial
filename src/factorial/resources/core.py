from __future__ import annotations

import json
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Generic, Literal, Protocol, TypeGuard, TypeVar

R = TypeVar("R")
R_co = TypeVar("R_co", covariant=True)

_RESOURCE_LIFECYCLE_ATTR = "__factorial_resource_lifecycle__"
_RESOURCE_LIFECYCLES_BY_TYPE: dict[ResourceType[Any], type[Any]] = {}
_RESOURCE_LIFECYCLES_BY_KEY: dict[str, type[Any]] = {}
_RESOURCE_TYPES_BY_KEY: dict[str, ResourceType[Any]] = {}


class ResourceType(Protocol[R_co]):
    __module__: str
    __qualname__: str

    def __hash__(self) -> int: ...


@dataclass(frozen=True)
class ResourceCheckpoint:
    provider: str
    kind: str
    ref: str
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "provider": self.provider,
            "kind": self.kind,
            "ref": self.ref,
            "metadata": dict(self.metadata),
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), sort_keys=True)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ResourceCheckpoint:
        return cls(
            provider=str(data["provider"]),
            kind=str(data["kind"]),
            ref=str(data["ref"]),
            metadata=dict(data.get("metadata") or {}),
        )

    @classmethod
    def from_json(cls, raw: str | bytes) -> ResourceCheckpoint:
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        return cls.from_dict(json.loads(raw))


@dataclass(frozen=True)
class LiveResourceRef:
    provider: str
    kind: str
    ref: str
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "provider": self.provider,
            "kind": self.kind,
            "ref": self.ref,
            "metadata": dict(self.metadata),
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), sort_keys=True)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> LiveResourceRef:
        return cls(
            provider=str(data["provider"]),
            kind=str(data["kind"]),
            ref=str(data["ref"]),
            metadata=dict(data.get("metadata") or {}),
        )

    @classmethod
    def from_json(cls, raw: str | bytes) -> LiveResourceRef:
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        return cls.from_dict(json.loads(raw))


@dataclass(frozen=True)
class ResourceContext:
    task_id: str
    owner_id: str
    agent_name: str
    logical_name: str = "default"
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ResourceRequest(Generic[R]):
    resource_type: ResourceType[R]
    logical_name: str = "default"
    metadata: dict[str, Any] = field(default_factory=dict)
    binding_metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def resource_type_key(self) -> str:
        return resource_type_key(self.resource_type)


class ResourceLifecycle(Protocol, Generic[R]):
    @classmethod
    async def create(cls, ctx: ResourceContext, request: ResourceRequest[R]) -> R: ...

    @classmethod
    async def restore(
        cls,
        checkpoint: ResourceCheckpoint,
        ctx: ResourceContext,
        request: ResourceRequest[R],
    ) -> R: ...

    @classmethod
    async def checkpoint(
        cls,
        resource: R,
        ctx: ResourceContext,
        request: ResourceRequest[R],
    ) -> ResourceCheckpoint | None: ...

    @classmethod
    async def destroy(
        cls,
        resource: R,
        ctx: ResourceContext,
        request: ResourceRequest[R],
    ) -> None: ...


class LiveResourceLifecycle(ResourceLifecycle[R], Protocol, Generic[R]):
    @classmethod
    async def attach_live(
        cls,
        live_ref: LiveResourceRef,
        ctx: ResourceContext,
        request: ResourceRequest[R],
    ) -> R | None: ...

    @classmethod
    def capture_live_ref(
        cls,
        resource: R,
        ctx: ResourceContext,
        request: ResourceRequest[R],
    ) -> LiveResourceRef | None: ...


class CheckpointCleanupResourceLifecycle(ResourceLifecycle[R], Protocol, Generic[R]):
    @classmethod
    async def delete_checkpoint(
        cls,
        checkpoint: ResourceCheckpoint,
        ctx: ResourceContext,
        request: ResourceRequest[R],
    ) -> None: ...


ResourceBindingPhase = Literal[
    "fresh",
    "live",
    "checkpointed",
    "creating",
    "restoring",
    "attaching",
    "checkpointing",
    "destroying",
]

RESOURCE_PHASE_FRESH: ResourceBindingPhase = "fresh"
RESOURCE_PHASE_LIVE: ResourceBindingPhase = "live"
RESOURCE_PHASE_CHECKPOINTED: ResourceBindingPhase = "checkpointed"
RESOURCE_PHASE_CREATING: ResourceBindingPhase = "creating"
RESOURCE_PHASE_RESTORING: ResourceBindingPhase = "restoring"
RESOURCE_PHASE_ATTACHING: ResourceBindingPhase = "attaching"
RESOURCE_PHASE_CHECKPOINTING: ResourceBindingPhase = "checkpointing"
RESOURCE_PHASE_DESTROYING: ResourceBindingPhase = "destroying"
RESOURCE_INFLIGHT_PHASES: frozenset[ResourceBindingPhase] = frozenset(
    {
        RESOURCE_PHASE_CREATING,
        RESOURCE_PHASE_RESTORING,
        RESOURCE_PHASE_ATTACHING,
        RESOURCE_PHASE_CHECKPOINTING,
        RESOURCE_PHASE_DESTROYING,
    }
)


@dataclass(frozen=True)
class ResourceBindingRecovery:
    record: ResourceBindingRecord | None
    mutated: bool
    busy: bool


@dataclass
class ResourceBindingRecord:
    resource_type_key: str
    logical_name: str
    live_ref: LiveResourceRef | None = None
    checkpoint: ResourceCheckpoint | None = None
    binding_metadata: dict[str, Any] = field(default_factory=dict)
    phase: str = "fresh"
    owner_pickups: int | None = None
    operation_id: str | None = None
    updated_at: float = field(default_factory=time.time)

    def has_live_ref(self) -> bool:
        return self.live_ref is not None and bool(self.live_ref.ref)

    def has_checkpoint(self) -> bool:
        return self.checkpoint is not None and bool(self.checkpoint.ref)

    def checkpoint_expired_at(self, now: float) -> bool:
        if self.checkpoint is None:
            return False
        expires_at = self.checkpoint.metadata.get("expires_at")
        if expires_at is None:
            return False
        try:
            return float(expires_at) <= now
        except (TypeError, ValueError):
            return False

    def recover(
        self,
        *,
        now: float,
        operation_timeout_s: float,
    ) -> ResourceBindingRecovery:
        mutated = False
        if self.checkpoint_expired_at(now):
            self.checkpoint = None
            mutated = True

        previous_phase = self.phase
        if previous_phase in RESOURCE_INFLIGHT_PHASES:
            age = now - self.updated_at
            if age < operation_timeout_s:
                return ResourceBindingRecovery(
                    record=self,
                    mutated=mutated,
                    busy=True,
                )

            self.operation_id = None
            self.owner_pickups = None
            if self.has_live_ref() or previous_phase in {
                RESOURCE_PHASE_CHECKPOINTING,
                RESOURCE_PHASE_DESTROYING,
                RESOURCE_PHASE_LIVE,
            }:
                self.phase = RESOURCE_PHASE_LIVE
            elif self.has_checkpoint():
                self.phase = RESOURCE_PHASE_CHECKPOINTED
            else:
                return ResourceBindingRecovery(
                    record=None,
                    mutated=True,
                    busy=False,
                )
            self.updated_at = now
            mutated = True
        elif self.operation_id is not None:
            self.operation_id = None
            self.owner_pickups = None
            self.updated_at = now
            mutated = True

        if (
            self.phase != RESOURCE_PHASE_LIVE
            and not self.has_live_ref()
            and not self.has_checkpoint()
        ):
            return ResourceBindingRecovery(record=None, mutated=True, busy=False)

        return ResourceBindingRecovery(record=self, mutated=mutated, busy=False)

    def to_dict(self) -> dict[str, Any]:
        return {
            "resource_type_key": self.resource_type_key,
            "logical_name": self.logical_name,
            "live_ref": (
                self.live_ref.to_dict()
                if self.live_ref is not None
                else None
            ),
            "checkpoint": (
                self.checkpoint.to_dict()
                if self.checkpoint is not None
                else None
            ),
            "binding_metadata": dict(self.binding_metadata),
            "phase": self.phase,
            "owner_pickups": self.owner_pickups,
            "operation_id": self.operation_id,
            "updated_at": self.updated_at,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), sort_keys=True)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ResourceBindingRecord:
        live_ref_data = data.get("live_ref")
        checkpoint_data = data.get("checkpoint")
        return cls(
            resource_type_key=str(data["resource_type_key"]),
            logical_name=str(data["logical_name"]),
            live_ref=(
                LiveResourceRef.from_dict(live_ref_data)
                if isinstance(live_ref_data, dict)
                else None
            ),
            checkpoint=(
                ResourceCheckpoint.from_dict(checkpoint_data)
                if isinstance(checkpoint_data, dict)
                else None
            ),
            binding_metadata=dict(data.get("binding_metadata") or {}),
            phase=str(data.get("phase") or RESOURCE_PHASE_FRESH),
            owner_pickups=(
                int(data["owner_pickups"])
                if data.get("owner_pickups") is not None
                else None
            ),
            operation_id=(
                str(data["operation_id"])
                if data.get("operation_id") is not None
                else None
            ),
            updated_at=float(data.get("updated_at") or time.time()),
        )

    @classmethod
    def from_json(cls, raw: str | bytes) -> ResourceBindingRecord:
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        return cls.from_dict(json.loads(raw))


def resource_type_key(resource_type: ResourceType[Any]) -> str:
    return f"{resource_type.__module__}:{resource_type.__qualname__}"


def register_resource_lifecycle(
    resource_type: ResourceType[R],
    lifecycle_type: type[ResourceLifecycle[R]],
) -> type[ResourceLifecycle[R]]:
    key = resource_type_key(resource_type)
    _RESOURCE_LIFECYCLES_BY_TYPE[resource_type] = lifecycle_type
    _RESOURCE_LIFECYCLES_BY_KEY[key] = lifecycle_type
    _RESOURCE_TYPES_BY_KEY[key] = resource_type
    try:
        setattr(resource_type, _RESOURCE_LIFECYCLE_ATTR, lifecycle_type)
    except Exception:
        pass
    return lifecycle_type


def resource(
    resource_type: ResourceType[R],
) -> Callable[[type[ResourceLifecycle[R]]], type[ResourceLifecycle[R]]]:
    def decorator(
        lifecycle_type: type[ResourceLifecycle[R]],
    ) -> type[ResourceLifecycle[R]]:
        return register_resource_lifecycle(resource_type, lifecycle_type)

    return decorator


def get_resource_lifecycle(
    resource_type: ResourceType[R],
) -> type[ResourceLifecycle[R]] | None:
    lifecycle = _RESOURCE_LIFECYCLES_BY_TYPE.get(resource_type)
    if lifecycle is not None:
        return _coerce_resource_lifecycle(lifecycle)

    lifecycle_value = getattr(resource_type, _RESOURCE_LIFECYCLE_ATTR, None)
    if not _is_resource_lifecycle_type(lifecycle_value):
        return None

    _RESOURCE_LIFECYCLES_BY_TYPE[resource_type] = lifecycle_value
    _RESOURCE_LIFECYCLES_BY_KEY[resource_type_key(resource_type)] = lifecycle_value
    _RESOURCE_TYPES_BY_KEY[resource_type_key(resource_type)] = resource_type
    return _coerce_resource_lifecycle(lifecycle_value)


def get_resource_lifecycle_by_key(
    resource_type_key_value: str,
) -> type[ResourceLifecycle[Any]] | None:
    lifecycle = _RESOURCE_LIFECYCLES_BY_KEY.get(resource_type_key_value)
    if not _is_resource_lifecycle_type(lifecycle):
        return None
    return lifecycle


def get_resource_type_by_key(
    resource_type_key_value: str,
) -> ResourceType[Any] | None:
    return _RESOURCE_TYPES_BY_KEY.get(resource_type_key_value)


def has_resource_lifecycle(resource_type: Any) -> bool:
    return (
        isinstance(resource_type, type)
        and get_resource_lifecycle(resource_type) is not None
    )


def _is_resource_lifecycle_type(
    value: object,
) -> TypeGuard[type[ResourceLifecycle[Any]]]:
    return all(
        hasattr(value, attr)
        for attr in ("create", "restore", "checkpoint", "destroy")
    )


def _coerce_resource_lifecycle(
    lifecycle_type: type[Any],
) -> type[ResourceLifecycle[R]]:
    return lifecycle_type


def lifecycle_supports_live_refs(
    lifecycle_type: type[ResourceLifecycle[R]],
) -> TypeGuard[type[LiveResourceLifecycle[R]]]:
    return hasattr(lifecycle_type, "attach_live") and hasattr(
        lifecycle_type,
        "capture_live_ref",
    )


def lifecycle_supports_checkpoint_cleanup(
    lifecycle_type: type[ResourceLifecycle[R]],
) -> TypeGuard[type[CheckpointCleanupResourceLifecycle[R]]]:
    return hasattr(lifecycle_type, "delete_checkpoint")


def checkpoint_is_expired(checkpoint: ResourceCheckpoint) -> bool:
    expires_at = checkpoint.metadata.get("expires_at")
    if expires_at is None:
        return False
    try:
        return float(expires_at) <= time.time()
    except (TypeError, ValueError):
        return False


__all__ = [
    "CheckpointCleanupResourceLifecycle",
    "LiveResourceLifecycle",
    "LiveResourceRef",
    "RESOURCE_INFLIGHT_PHASES",
    "RESOURCE_PHASE_ATTACHING",
    "RESOURCE_PHASE_CHECKPOINTED",
    "RESOURCE_PHASE_CHECKPOINTING",
    "RESOURCE_PHASE_CREATING",
    "RESOURCE_PHASE_DESTROYING",
    "RESOURCE_PHASE_FRESH",
    "RESOURCE_PHASE_LIVE",
    "RESOURCE_PHASE_RESTORING",
    "ResourceBindingPhase",
    "ResourceBindingRecovery",
    "ResourceType",
    "ResourceBindingRecord",
    "ResourceCheckpoint",
    "ResourceContext",
    "ResourceLifecycle",
    "ResourceRequest",
    "checkpoint_is_expired",
    "get_resource_lifecycle",
    "get_resource_lifecycle_by_key",
    "get_resource_type_by_key",
    "has_resource_lifecycle",
    "lifecycle_supports_checkpoint_cleanup",
    "lifecycle_supports_live_refs",
    "register_resource_lifecycle",
    "resource",
    "resource_type_key",
]
