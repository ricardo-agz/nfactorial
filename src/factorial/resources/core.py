from __future__ import annotations

import json
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Generic, Protocol, TypeVar

R = TypeVar("R")

_RESOURCE_LIFECYCLE_ATTR = "__factorial_resource_lifecycle__"
_RESOURCE_LIFECYCLES_BY_TYPE: dict[type[Any], type[Any]] = {}
_RESOURCE_LIFECYCLES_BY_KEY: dict[str, type[Any]] = {}


@dataclass(frozen=True)
class ResourceCheckpoint:
    provider: str
    kind: str
    ref: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class LiveResourceRef:
    provider: str
    kind: str
    ref: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ResourceContext:
    task_id: str
    owner_id: str
    agent_name: str
    logical_name: str = "default"
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ResourceRequest(Generic[R]):
    resource_type: type[R]
    logical_name: str = "default"

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


@dataclass
class ResourceBindingRecord:
    resource_type_key: str
    logical_name: str
    live_ref: LiveResourceRef | None = None
    checkpoint: ResourceCheckpoint | None = None
    phase: str = "fresh"
    updated_at: float = field(default_factory=time.time)

    def to_dict(self) -> dict[str, Any]:
        return {
            "resource_type_key": self.resource_type_key,
            "logical_name": self.logical_name,
            "live_ref": (
                {
                    "provider": self.live_ref.provider,
                    "kind": self.live_ref.kind,
                    "ref": self.live_ref.ref,
                    "metadata": dict(self.live_ref.metadata),
                }
                if self.live_ref is not None
                else None
            ),
            "checkpoint": (
                {
                    "provider": self.checkpoint.provider,
                    "kind": self.checkpoint.kind,
                    "ref": self.checkpoint.ref,
                    "metadata": dict(self.checkpoint.metadata),
                }
                if self.checkpoint is not None
                else None
            ),
            "phase": self.phase,
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
                LiveResourceRef(
                    provider=str(live_ref_data["provider"]),
                    kind=str(live_ref_data["kind"]),
                    ref=str(live_ref_data["ref"]),
                    metadata=dict(live_ref_data.get("metadata") or {}),
                )
                if isinstance(live_ref_data, dict)
                else None
            ),
            checkpoint=(
                ResourceCheckpoint(
                    provider=str(checkpoint_data["provider"]),
                    kind=str(checkpoint_data["kind"]),
                    ref=str(checkpoint_data["ref"]),
                    metadata=dict(checkpoint_data.get("metadata") or {}),
                )
                if isinstance(checkpoint_data, dict)
                else None
            ),
            phase=str(data.get("phase") or "fresh"),
            updated_at=float(data.get("updated_at") or time.time()),
        )

    @classmethod
    def from_json(cls, raw: str | bytes) -> ResourceBindingRecord:
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        return cls.from_dict(json.loads(raw))


def resource_type_key(resource_type: type[Any]) -> str:
    return f"{resource_type.__module__}:{resource_type.__qualname__}"


def register_resource_lifecycle(
    resource_type: type[R],
    lifecycle_type: type[ResourceLifecycle[R]],
) -> type[ResourceLifecycle[R]]:
    key = resource_type_key(resource_type)
    _RESOURCE_LIFECYCLES_BY_TYPE[resource_type] = lifecycle_type
    _RESOURCE_LIFECYCLES_BY_KEY[key] = lifecycle_type
    try:
        setattr(resource_type, _RESOURCE_LIFECYCLE_ATTR, lifecycle_type)
    except Exception:
        pass
    return lifecycle_type


def resource(
    resource_type: type[R],
) -> Callable[[type[ResourceLifecycle[R]]], type[ResourceLifecycle[R]]]:
    def decorator(
        lifecycle_type: type[ResourceLifecycle[R]],
    ) -> type[ResourceLifecycle[R]]:
        return register_resource_lifecycle(resource_type, lifecycle_type)

    return decorator


def get_resource_lifecycle(
    resource_type: type[Any],
) -> type[ResourceLifecycle[Any]] | None:
    lifecycle = _RESOURCE_LIFECYCLES_BY_TYPE.get(resource_type)
    if lifecycle is not None:
        return lifecycle

    lifecycle = getattr(resource_type, _RESOURCE_LIFECYCLE_ATTR, None)
    if lifecycle is not None:
        _RESOURCE_LIFECYCLES_BY_TYPE[resource_type] = lifecycle
        _RESOURCE_LIFECYCLES_BY_KEY[resource_type_key(resource_type)] = lifecycle
    return lifecycle


def get_resource_lifecycle_by_key(
    resource_type_key_value: str,
) -> type[ResourceLifecycle[Any]] | None:
    return _RESOURCE_LIFECYCLES_BY_KEY.get(resource_type_key_value)


def has_resource_lifecycle(resource_type: Any) -> bool:
    return (
        isinstance(resource_type, type)
        and get_resource_lifecycle(resource_type) is not None
    )


def lifecycle_supports_live_refs(lifecycle_type: type[Any]) -> bool:
    return hasattr(lifecycle_type, "attach_live") and hasattr(
        lifecycle_type,
        "capture_live_ref",
    )


def checkpoint_is_expired(checkpoint: ResourceCheckpoint) -> bool:
    expires_at = checkpoint.metadata.get("expires_at")
    if expires_at is None:
        return False
    try:
        return float(expires_at) <= time.time()
    except (TypeError, ValueError):
        return False


__all__ = [
    "LiveResourceLifecycle",
    "LiveResourceRef",
    "ResourceBindingRecord",
    "ResourceCheckpoint",
    "ResourceContext",
    "ResourceLifecycle",
    "ResourceRequest",
    "checkpoint_is_expired",
    "get_resource_lifecycle",
    "get_resource_lifecycle_by_key",
    "has_resource_lifecycle",
    "lifecycle_supports_live_refs",
    "register_resource_lifecycle",
    "resource",
    "resource_type_key",
]
