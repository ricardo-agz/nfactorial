from __future__ import annotations

import asyncio
import os
import time
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any, Generic, TypeVar, overload

from factorial.core.logging import get_logger

from .core import (
    LiveResourceRef,
    ResourceCheckpoint,
    ResourceContext,
    ResourceLifecycle,
    ResourceRequest,
    ResourceType,
    get_resource_lifecycle,
    get_resource_type_by_key,
    lifecycle_supports_checkpoint_cleanup,
    lifecycle_supports_live_refs,
    resource_type_key,
)
from .sandbox.base import Sandbox
from .sandbox.guarded import GuardedSandbox
from .sandbox.providers import (
    SANDBOX_PROVIDER_SOURCE_EXPLICIT,
    make_sandbox_request_metadata,
    sandbox_requested_provider_alias,
    sandbox_requested_provider_source,
)
from .store import (
    InMemoryResourceBindingStore,
    RedisResourceBindingStore,
    ResourceBindingStore,
    ResourceDecision,
    ResourceLease,
    ResourceLeaseLostError,
    ResourceReservation,
)

logger = get_logger(__name__)

R = TypeVar("R")


def _configured_operation_timeout_s() -> float:
    raw = os.getenv("NFACTORIAL_RESOURCE_OPERATION_TIMEOUT_S")
    if raw is None or not raw.strip():
        return 15.0
    return float(raw)


def _configured_busy_wait_timeout_s() -> float:
    raw = os.getenv("NFACTORIAL_RESOURCE_BUSY_WAIT_TIMEOUT_S")
    if raw is None or not raw.strip():
        return 15.0
    return float(raw)


def _configured_busy_poll_interval_s() -> float:
    raw = os.getenv("NFACTORIAL_RESOURCE_BUSY_POLL_INTERVAL_S")
    if raw is None or not raw.strip():
        return 0.1
    return float(raw)


@dataclass
class _LiveBinding(Generic[R]):
    resource_type: ResourceType[R]
    lifecycle: type[ResourceLifecycle[R]]
    request: ResourceRequest[R]
    resource: R


@dataclass
class ResourceManager:
    store: ResourceBindingStore
    task_id: str
    owner_id: str
    agent_name: str
    metadata: dict[str, Any] = field(default_factory=dict)
    lease: ResourceLease = field(default_factory=ResourceLease.local)
    operation_timeout_s: float = field(default_factory=_configured_operation_timeout_s)
    busy_wait_timeout_s: float = field(default_factory=_configured_busy_wait_timeout_s)
    busy_poll_interval_s: float = field(
        default_factory=_configured_busy_poll_interval_s
    )
    _live_bindings: dict[tuple[str, str], _LiveBinding[Any]] = field(
        default_factory=dict
    )
    _locks: dict[tuple[str, str], asyncio.Lock] = field(default_factory=dict)

    def _resource_context(self, logical_name: str) -> ResourceContext:
        metadata = dict(self.metadata)
        if self.lease.processing_pickups is not None:
            metadata.setdefault("processing_pickups", self.lease.processing_pickups)
        metadata.setdefault("resource_lease_mode", self.lease.mode)
        return ResourceContext(
            task_id=self.task_id,
            owner_id=self.owner_id,
            agent_name=self.agent_name,
            logical_name=logical_name,
            metadata=metadata,
        )

    def _lock(
        self,
        resource_type_value: ResourceType[Any],
        logical_name: str,
    ) -> asyncio.Lock:
        key = (resource_type_key(resource_type_value), logical_name)
        lock = self._locks.get(key)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[key] = lock
        return lock

    def _binding_key(
        self,
        resource_type_value: ResourceType[Any],
        logical_name: str,
    ) -> tuple[str, str]:
        return (resource_type_key(resource_type_value), logical_name)

    def _reservation_key(self, reservation: ResourceReservation) -> tuple[str, str]:
        return (reservation.resource_type_key, reservation.logical_name)

    def _get_live_binding(
        self,
        resource_type_value: ResourceType[R],
        logical_name: str,
    ) -> _LiveBinding[R] | None:
        return self._live_bindings.get(
            self._binding_key(resource_type_value, logical_name)
        )

    async def assert_current_lease(self) -> None:
        if not await self.store.validate_lease(self.lease):
            raise ResourceLeaseLostError(
                f"Lost resource lease for task {self.task_id} ({self.lease.mode})"
            )

    async def get(
        self,
        resource_type_value: ResourceType[R],
        logical_name: str = "default",
        request_metadata: dict[str, Any] | None = None,
    ) -> R:
        if self.lease.is_system:
            raise RuntimeError("system resource managers cannot acquire live resources")

        async with self._lock(resource_type_value, logical_name):
            live_binding = self._get_live_binding(resource_type_value, logical_name)
            if live_binding is not None:
                if resource_type_value is Sandbox:
                    requested_provider = sandbox_requested_provider_alias(
                        request_metadata or {}
                    )
                    requested_source = sandbox_requested_provider_source(
                        request_metadata or {}
                    )
                    bound_provider = sandbox_requested_provider_alias(
                        live_binding.request.binding_metadata
                    ) or sandbox_requested_provider_alias(live_binding.request.metadata)
                    if (
                        requested_source == SANDBOX_PROVIDER_SOURCE_EXPLICIT
                        and requested_provider is not None
                        and bound_provider is not None
                        and requested_provider != bound_provider
                    ):
                        raise RuntimeError(
                            "Sandbox provider conflict: requested "
                            f"{requested_provider!r}, but sandbox is already bound to "
                            f"{bound_provider!r}."
                        )
                return live_binding.resource

            lifecycle = get_resource_lifecycle(resource_type_value)
            if lifecycle is None:
                raise TypeError(
                    "No lifecycle is registered for resource type "
                    f"{resource_type_value!r}. Use @resource(Type) on a lifecycle "
                    "class or import a built-in resource type."
                )

            request = ResourceRequest(
                resource_type=resource_type_value,
                logical_name=logical_name,
                metadata=dict(request_metadata or {}),
            )
            ctx = self._resource_context(logical_name)
            while True:
                decision = await self._wait_for_decision(
                    resource_type_value=resource_type_value,
                    logical_name=logical_name,
                    begin=lambda operation_id, now: self.store.begin_acquire(
                        resource_type_key_value=request.resource_type_key,
                        logical_name=logical_name,
                        binding_metadata=dict(request.metadata),
                        lease=self.lease,
                        operation_id=operation_id,
                        now=now,
                        operation_timeout_s=self.operation_timeout_s,
                    ),
                )
                if decision.outcome == "stale_owner":
                    raise ResourceLeaseLostError(
                        f"Cannot acquire {request.resource_type_key}/{logical_name}; "
                        "the task is no longer owned by this worker."
                    )
                if decision.outcome == "not_allowed":
                    raise RuntimeError(
                        f"Cannot acquire {request.resource_type_key}/{logical_name} "
                        f"with resource lease mode {self.lease.mode!r}."
                    )
                if decision.reservation is None:
                    raise RuntimeError(
                        "Resource acquire reservation missing for "
                        f"{request.resource_type_key}/{logical_name}."
                    )

                reservation = decision.reservation
                effective_request = self._request_with_binding_metadata(
                    request,
                    (
                        decision.record.binding_metadata
                        if decision.record is not None
                        else request.metadata
                    ),
                )
                resource: R | None = None
                should_destroy_on_failure = decision.outcome in {"create", "restore"}
                try:
                    if decision.outcome == "create":
                        resource = await lifecycle.create(ctx, effective_request)
                        checkpoint = None
                    elif decision.outcome == "restore":
                        if (
                            decision.record is None
                            or decision.record.checkpoint is None
                        ):
                            raise RuntimeError(
                                "Restore reservation did not include a checkpoint."
                            )
                        checkpoint = decision.record.checkpoint
                        resource = await lifecycle.restore(
                            checkpoint,
                            ctx,
                            effective_request,
                        )
                    elif decision.outcome == "attach":
                        if decision.record is None or decision.record.live_ref is None:
                            raise RuntimeError(
                                "Attach reservation did not include a live reference."
                            )
                        resource = await self._try_attach_live(
                            lifecycle,
                            decision.record.live_ref,
                            ctx,
                            effective_request,
                        )
                        checkpoint = (
                            decision.record.checkpoint
                            if decision.record is not None
                            else None
                        )
                        if resource is None:
                            await self._abort(reservation)
                            continue
                        should_destroy_on_failure = False
                    else:
                        raise RuntimeError(
                            f"Unexpected resource acquire outcome: {decision.outcome!r}"
                        )

                    wrapped_resource = self._wrap_resource(resource)
                    live_ref = self._capture_live_ref(
                        lifecycle,
                        wrapped_resource,
                        ctx,
                        effective_request,
                    )
                    commit_status = await self.store.commit_live(
                        reservation=reservation,
                        lease=self.lease,
                        live_ref=live_ref,
                        checkpoint=checkpoint,
                        now=time.time(),
                    )
                    if commit_status != "ok":
                        raise ResourceLeaseLostError(
                            "Failed to commit live resource binding: "
                            f"{commit_status} "
                            f"({request.resource_type_key}/{logical_name})"
                        )
                    return self._remember_live_resource(
                        resource_type=resource_type_value,
                        lifecycle=lifecycle,
                        request=effective_request,
                        resource=wrapped_resource,
                    )
                except Exception:
                    try:
                        await self._abort(reservation)
                    except Exception:
                        logger.exception(
                            "Failed to abort resource reservation for %s/%s",
                            request.resource_type_key,
                            logical_name,
                        )
                    if resource is not None and should_destroy_on_failure:
                        try:
                            await lifecycle.destroy(
                                resource,
                                ctx,
                                effective_request,
                            )
                        except Exception:
                            logger.exception(
                                "Failed to destroy partially acquired "
                                "resource for %s/%s",
                                request.resource_type_key,
                                logical_name,
                            )
                    raise

    async def checkpoint(
        self,
        resource_type_value: ResourceType[R],
        logical_name: str = "default",
    ) -> ResourceCheckpoint | None:
        async with self._lock(resource_type_value, logical_name):
            live_binding = self._get_live_binding(resource_type_value, logical_name)
            if live_binding is None:
                for record in await self.store.list_bindings():
                    if (
                        record.resource_type_key
                        == resource_type_key(resource_type_value)
                        and record.logical_name == logical_name
                    ):
                        return record.checkpoint
                return None

            request = live_binding.request
            ctx = self._resource_context(logical_name)
            decision = await self.store.begin_checkpoint(
                resource_type_key_value=request.resource_type_key,
                logical_name=logical_name,
                lease=self.lease,
                operation_id=uuid.uuid4().hex,
                now=time.time(),
                operation_timeout_s=self.operation_timeout_s,
            )
            if decision.outcome == "stale_owner":
                raise ResourceLeaseLostError(
                    f"Cannot checkpoint {request.resource_type_key}/{logical_name}; "
                    "the task is no longer owned by this worker."
                )
            if decision.outcome == "busy":
                raise RuntimeError(
                    f"Resource {request.resource_type_key}/{logical_name} is busy."
                )
            if decision.outcome != "ok" or decision.reservation is None:
                if decision.outcome == "missing":
                    return None
                raise RuntimeError(
                    f"Unable to checkpoint {request.resource_type_key}/{logical_name}: "
                    f"{decision.outcome}"
                )

            reservation = decision.reservation
            checkpoint: ResourceCheckpoint | None = None
            try:
                checkpoint = await live_binding.lifecycle.checkpoint(
                    live_binding.resource,
                    ctx,
                    request,
                )
            except Exception:
                await self._abort(reservation)
                raise

            destroy_error: Exception | None = None
            try:
                await live_binding.lifecycle.destroy(
                    live_binding.resource,
                    ctx,
                    request,
                )
            except Exception as exc:
                destroy_error = exc
                if checkpoint is None:
                    await self._abort(reservation)
                    raise

            commit_status = await self.store.commit_checkpoint(
                reservation=reservation,
                checkpoint=checkpoint,
                now=time.time(),
            )
            if commit_status != "ok":
                if checkpoint is not None:
                    await self._delete_checkpoint_best_effort(
                        lifecycle=live_binding.lifecycle,
                        checkpoint=checkpoint,
                        ctx=ctx,
                        request=request,
                    )
                raise ResourceLeaseLostError(
                    "Failed to commit checkpointed resource binding: "
                    f"{commit_status} ({request.resource_type_key}/{logical_name})"
                )

            self._live_bindings.pop(
                self._binding_key(resource_type_value, logical_name),
                None,
            )
            if destroy_error is not None:
                logger.warning(
                    "Resource destroy after checkpoint failed for %s/%s",
                    request.resource_type_key,
                    logical_name,
                    exc_info=destroy_error,
                )
            return checkpoint

    async def destroy(
        self,
        resource_type_value: ResourceType[R],
        logical_name: str = "default",
    ) -> None:
        async with self._lock(resource_type_value, logical_name):
            await self._destroy_locked(resource_type_value, logical_name)

    async def checkpoint_all(self) -> None:
        for resource_type_value, logical_name in self._iter_live_resource_keys():
            await self.checkpoint(resource_type_value, logical_name)

    async def destroy_all(self) -> None:
        for resource_type_value, logical_name in self._iter_live_resource_keys():
            try:
                await self.destroy(resource_type_value, logical_name)
            except Exception:
                logger.exception(
                    "Failed to destroy live resource binding for %s/%s",
                    resource_type_value,
                    logical_name,
                )

        remaining_records = await self.store.list_bindings()
        for record in remaining_records:
            resolved_resource_type = get_resource_type_by_key(record.resource_type_key)
            if resolved_resource_type is None:
                logger.warning(
                    "Leaving unknown persisted resource binding %s/%s in Redis",
                    record.resource_type_key,
                    record.logical_name,
                )
                continue
            async with self._lock(resolved_resource_type, record.logical_name):
                if (
                    self._get_live_binding(resolved_resource_type, record.logical_name)
                    is not None
                ):
                    continue
                try:
                    await self._destroy_locked(
                        resolved_resource_type,
                        record.logical_name,
                    )
                except Exception:
                    logger.exception(
                        "Failed to destroy persisted resource binding for %s/%s",
                        record.resource_type_key,
                        record.logical_name,
                    )

    async def delete_all_bindings(self) -> None:
        await self.store.clear_all()
        self._live_bindings.clear()

    def _iter_live_resource_keys(self) -> list[tuple[ResourceType[Any], str]]:
        keys: list[tuple[ResourceType[Any], str]] = []
        for (_, logical_name), live_binding in self._live_bindings.items():
            keys.append((live_binding.resource_type, logical_name))
        return keys

    async def _destroy_locked(
        self,
        resource_type_value: ResourceType[R],
        logical_name: str,
    ) -> None:
        lifecycle = get_resource_lifecycle(resource_type_value)
        if lifecycle is None:
            raise TypeError(
                f"No lifecycle is registered for resource type {resource_type_value!r}."
            )
        request = ResourceRequest(
            resource_type=resource_type_value,
            logical_name=logical_name,
        )
        ctx = self._resource_context(logical_name)
        live_binding = self._get_live_binding(resource_type_value, logical_name)

        decision = await self._wait_for_decision(
            resource_type_value=resource_type_value,
            logical_name=logical_name,
            begin=lambda operation_id, now: self.store.begin_destroy(
                resource_type_key_value=request.resource_type_key,
                logical_name=logical_name,
                lease=self.lease,
                operation_id=operation_id,
                now=now,
                operation_timeout_s=self.operation_timeout_s,
            ),
        )
        if decision.outcome in {"missing", "not_allowed"}:
            self._live_bindings.pop(
                self._binding_key(resource_type_value, logical_name),
                None,
            )
            return
        if decision.outcome == "stale_owner":
            raise ResourceLeaseLostError(
                f"Cannot destroy {request.resource_type_key}/{logical_name}; "
                "the task is no longer owned by this worker."
            )
        if decision.outcome == "busy" or decision.reservation is None:
            raise RuntimeError(
                f"Unable to destroy {request.resource_type_key}/{logical_name}: "
                f"{decision.outcome}"
            )

        reservation = decision.reservation
        record = decision.record
        if record is None:
            await self._abort(reservation)
            return

        effective_request = (
            live_binding.request
            if live_binding is not None
            else self._request_with_binding_metadata(request, record.binding_metadata)
        )
        cleanup_resource = live_binding.resource if live_binding is not None else None
        try:
            if cleanup_resource is None and record.live_ref is not None:
                cleanup_resource = await self._try_attach_live(
                    lifecycle,
                    record.live_ref,
                    ctx,
                    effective_request,
                )
                if cleanup_resource is not None:
                    cleanup_resource = self._wrap_resource(cleanup_resource)

            if cleanup_resource is not None:
                await lifecycle.destroy(cleanup_resource, ctx, effective_request)
            elif record.checkpoint is not None:
                await self._delete_checkpoint_best_effort(
                    lifecycle=lifecycle,
                    checkpoint=record.checkpoint,
                    ctx=ctx,
                    request=effective_request,
                    warn_on_skip=True,
                )

            commit_status = await self.store.commit_destroy(
                reservation=reservation,
                now=time.time(),
            )
            if commit_status not in {"ok", "missing"}:
                raise ResourceLeaseLostError(
                    "Failed to commit resource destruction: "
                    f"{commit_status} ({request.resource_type_key}/{logical_name})"
                )
            self._live_bindings.pop(
                self._binding_key(resource_type_value, logical_name),
                None,
            )
        except Exception:
            await self._abort(reservation)
            raise

    async def _wait_for_decision(
        self,
        *,
        resource_type_value: ResourceType[Any],
        logical_name: str,
        begin: Callable[[str, float], Awaitable[ResourceDecision]],
    ) -> ResourceDecision:
        deadline = time.monotonic() + self.busy_wait_timeout_s
        while True:
            decision = await begin(uuid.uuid4().hex, time.time())
            if decision.outcome != "busy":
                return decision
            if time.monotonic() >= deadline:
                raise TimeoutError(
                    "Timed out waiting for resource binding "
                    f"{resource_type_key(resource_type_value)}/{logical_name}"
                )
            await asyncio.sleep(self.busy_poll_interval_s)

    async def _try_attach_live(
        self,
        lifecycle: type[ResourceLifecycle[Any]],
        live_ref: LiveResourceRef,
        ctx: ResourceContext,
        request: ResourceRequest[Any],
    ) -> Any | None:
        if not lifecycle_supports_live_refs(lifecycle):
            return None
        try:
            return await lifecycle.attach_live(live_ref, ctx, request)
        except Exception:
            logger.warning(
                "Failed to attach live resource %s for %s",
                live_ref.ref,
                request.resource_type_key,
                exc_info=True,
            )
            return None

    def _capture_live_ref(
        self,
        lifecycle: type[ResourceLifecycle[R]],
        resource: R,
        ctx: ResourceContext,
        request: ResourceRequest[R],
    ) -> LiveResourceRef | None:
        if not lifecycle_supports_live_refs(lifecycle):
            return None
        return lifecycle.capture_live_ref(resource, ctx, request)

    async def _delete_checkpoint_best_effort(
        self,
        *,
        lifecycle: type[ResourceLifecycle[R]],
        checkpoint: ResourceCheckpoint,
        ctx: ResourceContext,
        request: ResourceRequest[R],
        warn_on_skip: bool = False,
    ) -> None:
        if not lifecycle_supports_checkpoint_cleanup(lifecycle):
            if warn_on_skip:
                logger.warning(
                    "Checkpoint cleanup is not implemented for %s; "
                    "dropping binding only for checkpoint %s",
                    request.resource_type_key,
                    checkpoint.ref,
                )
            return
        await lifecycle.delete_checkpoint(checkpoint, ctx, request)

    async def _abort(self, reservation: ResourceReservation) -> None:
        status = await self.store.abort_operation(
            reservation=reservation,
            now=time.time(),
        )
        if status not in {"ok", "missing"}:
            raise RuntimeError(
                "Failed to abort resource reservation "
                f"{reservation.resource_type_key}/{reservation.logical_name}: {status}"
            )

    def _request_with_binding_metadata(
        self,
        request: ResourceRequest[R],
        binding_metadata: dict[str, Any],
    ) -> ResourceRequest[R]:
        return ResourceRequest(
            resource_type=request.resource_type,
            logical_name=request.logical_name,
            metadata=dict(request.metadata),
            binding_metadata=dict(binding_metadata),
        )

    @overload
    def _wrap_resource(self, resource: Sandbox) -> Sandbox: ...

    @overload
    def _wrap_resource(self, resource: R) -> R: ...

    def _wrap_resource(self, resource: object) -> object:
        if isinstance(resource, Sandbox) and self.lease.is_worker:
            return GuardedSandbox(
                sandbox=resource,
                validator=self.assert_current_lease,
            )
        return resource

    def _remember_live_resource(
        self,
        *,
        resource_type: ResourceType[R],
        lifecycle: type[ResourceLifecycle[R]],
        request: ResourceRequest[R],
        resource: R,
    ) -> R:
        binding_key = (resource_type_key(resource_type), request.logical_name)
        self._live_bindings[binding_key] = _LiveBinding(
            resource_type=resource_type,
            lifecycle=lifecycle,
            request=request,
            resource=resource,
        )
        return resource


@dataclass
class ResourcesExecutionNamespace:
    manager: ResourceManager | None = None
    default_sandbox_provider: str | None = None

    def _require_manager(self) -> ResourceManager:
        manager = self.manager
        if manager is None:
            raise RuntimeError(
                "resources are not configured for this execution context"
            )
        return manager

    async def get_resource(
        self,
        resource_type_value: ResourceType[R],
        logical_name: str = "default",
        request_metadata: dict[str, Any] | None = None,
    ) -> R:
        if (
            resource_type_value is Sandbox
            and request_metadata is None
            and self.default_sandbox_provider is not None
        ):
            request_metadata = make_sandbox_request_metadata(
                self.default_sandbox_provider,
                explicit=False,
            )
        return await self._require_manager().get(
            resource_type_value,
            logical_name,
            request_metadata=request_metadata,
        )

    async def get_sandbox(
        self,
        logical_name: str = "default",
        provider: str | None = None,
    ) -> Sandbox:
        request_metadata = (
            make_sandbox_request_metadata(provider, explicit=True)
            if provider is not None
            else None
        )
        return await self.get_resource(
            Sandbox,
            logical_name,
            request_metadata=request_metadata,
        )

    async def checkpoint_resource(
        self,
        resource_type_value: ResourceType[R],
        logical_name: str = "default",
    ) -> ResourceCheckpoint | None:
        return await self._require_manager().checkpoint(
            resource_type_value,
            logical_name,
        )

    async def destroy_resource(
        self,
        resource_type_value: ResourceType[R],
        logical_name: str = "default",
    ) -> None:
        await self._require_manager().destroy(resource_type_value, logical_name)

    async def checkpoint_all(self) -> None:
        await self._require_manager().checkpoint_all()

    async def destroy_all(self) -> None:
        await self._require_manager().destroy_all()

    async def delete_all_bindings(self) -> None:
        await self._require_manager().delete_all_bindings()


__all__ = [
    "InMemoryResourceBindingStore",
    "RedisResourceBindingStore",
    "ResourceBindingStore",
    "ResourceDecision",
    "ResourceLease",
    "ResourceLeaseLostError",
    "ResourceManager",
    "ResourceReservation",
    "ResourcesExecutionNamespace",
]
