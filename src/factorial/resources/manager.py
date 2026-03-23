from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass, field
from typing import Any, Generic, Protocol, TypeVar, cast

import redis.asyncio as redis

from factorial.core.logging import get_logger
from factorial.core.utils import resolve_awaitable

from .core import (
    LiveResourceLifecycle,
    ResourceBindingRecord,
    ResourceCheckpoint,
    ResourceContext,
    ResourceLifecycle,
    ResourceRequest,
    checkpoint_is_expired,
    get_resource_lifecycle,
    lifecycle_supports_live_refs,
    resource_type_key,
)

logger = get_logger(__name__)

R = TypeVar("R")


class ResourceBindingStore(Protocol):
    async def load(
        self,
        resource_type_key_value: str,
        logical_name: str,
    ) -> ResourceBindingRecord | None: ...

    async def save(self, binding: ResourceBindingRecord) -> None: ...

    async def delete(self, resource_type_key_value: str, logical_name: str) -> None: ...

    async def delete_all(self) -> None: ...


class InMemoryResourceBindingStore:
    def __init__(self) -> None:
        self._bindings: dict[tuple[str, str], ResourceBindingRecord] = {}

    async def load(
        self,
        resource_type_key_value: str,
        logical_name: str,
    ) -> ResourceBindingRecord | None:
        return self._bindings.get((resource_type_key_value, logical_name))

    async def save(self, binding: ResourceBindingRecord) -> None:
        self._bindings[(binding.resource_type_key, binding.logical_name)] = binding

    async def delete(self, resource_type_key_value: str, logical_name: str) -> None:
        self._bindings.pop((resource_type_key_value, logical_name), None)

    async def delete_all(self) -> None:
        self._bindings.clear()


class RedisResourceBindingStore:
    def __init__(
        self,
        *,
        redis_client: redis.Redis,
        namespace: str,
        task_id: str,
    ) -> None:
        self.redis_client = redis_client
        from factorial.queue.keys import RedisKeys

        self.keys = RedisKeys.format(namespace=namespace, task_id=task_id)

    def _field_name(self, resource_type_key_value: str, logical_name: str) -> str:
        return f"{resource_type_key_value}:{logical_name}"

    async def load(
        self,
        resource_type_key_value: str,
        logical_name: str,
    ) -> ResourceBindingRecord | None:
        raw = await resolve_awaitable(
            self.redis_client.hget(
                self.keys.resource_bindings,
                self._field_name(resource_type_key_value, logical_name),
            )
        )
        if raw is None:
            return None
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        return ResourceBindingRecord.from_dict(json.loads(str(raw)))

    async def save(self, binding: ResourceBindingRecord) -> None:
        await resolve_awaitable(
            self.redis_client.hset(
                self.keys.resource_bindings,
                self._field_name(binding.resource_type_key, binding.logical_name),
                binding.to_json(),
            )
        )

    async def delete(self, resource_type_key_value: str, logical_name: str) -> None:
        await resolve_awaitable(
            self.redis_client.hdel(
                self.keys.resource_bindings,
                self._field_name(resource_type_key_value, logical_name),
            )
        )

    async def delete_all(self) -> None:
        await resolve_awaitable(self.redis_client.delete(self.keys.resource_bindings))


@dataclass
class _LiveBinding(Generic[R]):
    resource_type: type[R]
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
    _live_bindings: dict[tuple[str, str], _LiveBinding[Any]] = field(
        default_factory=dict
    )
    _locks: dict[tuple[str, str], asyncio.Lock] = field(default_factory=dict)

    def _resource_context(self, logical_name: str) -> ResourceContext:
        return ResourceContext(
            task_id=self.task_id,
            owner_id=self.owner_id,
            agent_name=self.agent_name,
            logical_name=logical_name,
            metadata=dict(self.metadata),
        )

    def _lock(self, resource_type_value: type[Any], logical_name: str) -> asyncio.Lock:
        key = (resource_type_key(resource_type_value), logical_name)
        lock = self._locks.get(key)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[key] = lock
        return lock

    async def get(
        self,
        resource_type_value: type[R],
        logical_name: str = "default",
    ) -> R:
        binding_key = (resource_type_key(resource_type_value), logical_name)
        async with self._lock(resource_type_value, logical_name):
            live_binding = self._live_bindings.get(binding_key)
            if live_binding is not None:
                return cast(R, live_binding.resource)

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
            )
            ctx = self._resource_context(logical_name)
            persisted = await self.store.load(binding_key[0], logical_name)

            if persisted is not None and persisted.live_ref is not None:
                attached = await self._try_attach_live(
                    lifecycle,
                    persisted.live_ref,
                    ctx,
                    request,
                )
                if attached is not None:
                    return await self._remember_live_resource(
                        resource_type=resource_type_value,
                        lifecycle=cast(type[ResourceLifecycle[R]], lifecycle),
                        request=request,
                        resource=attached,
                        checkpoint=persisted.checkpoint,
                    )
                persisted.live_ref = None
                persisted.updated_at = time.time()
                persisted.phase = "broken"
                await self.store.save(persisted)

            if persisted is not None and persisted.checkpoint is not None:
                if checkpoint_is_expired(persisted.checkpoint):
                    await self.store.delete(binding_key[0], logical_name)
                else:
                    restored = await cast(
                        type[ResourceLifecycle[R]],
                        lifecycle,
                    ).restore(
                        persisted.checkpoint,
                        ctx,
                        request,
                    )
                    return await self._remember_live_resource(
                        resource_type=resource_type_value,
                        lifecycle=cast(type[ResourceLifecycle[R]], lifecycle),
                        request=request,
                        resource=restored,
                        checkpoint=persisted.checkpoint,
                    )

            created = await cast(type[ResourceLifecycle[R]], lifecycle).create(
                ctx,
                request,
            )
            return await self._remember_live_resource(
                resource_type=resource_type_value,
                lifecycle=cast(type[ResourceLifecycle[R]], lifecycle),
                request=request,
                resource=created,
                checkpoint=None,
            )

    async def checkpoint(
        self,
        resource_type_value: type[R],
        logical_name: str = "default",
    ) -> ResourceCheckpoint | None:
        binding_key = (resource_type_key(resource_type_value), logical_name)
        async with self._lock(resource_type_value, logical_name):
            live_binding = self._live_bindings.get(binding_key)
            if live_binding is None:
                persisted = await self.store.load(binding_key[0], logical_name)
                return None if persisted is None else persisted.checkpoint

            ctx = self._resource_context(logical_name)
            checkpoint = await live_binding.lifecycle.checkpoint(
                live_binding.resource,
                ctx,
                live_binding.request,
            )
            await live_binding.lifecycle.destroy(
                live_binding.resource,
                ctx,
                live_binding.request,
            )
            self._live_bindings.pop(binding_key, None)

            if checkpoint is None:
                await self.store.delete(binding_key[0], logical_name)
                return None

            await self.store.save(
                ResourceBindingRecord(
                    resource_type_key=binding_key[0],
                    logical_name=logical_name,
                    live_ref=None,
                    checkpoint=checkpoint,
                    phase="checkpointed",
                    updated_at=time.time(),
                )
            )
            return checkpoint

    async def destroy(
        self,
        resource_type_value: type[R],
        logical_name: str = "default",
    ) -> None:
        binding_key = (resource_type_key(resource_type_value), logical_name)
        async with self._lock(resource_type_value, logical_name):
            live_binding = self._live_bindings.pop(binding_key, None)
            if live_binding is not None:
                ctx = self._resource_context(logical_name)
                await live_binding.lifecycle.destroy(
                    live_binding.resource,
                    ctx,
                    live_binding.request,
                )
            await self.store.delete(binding_key[0], logical_name)

    async def checkpoint_all(self) -> None:
        for resource_type_value, logical_name in self._iter_live_resource_keys():
            await self.checkpoint(resource_type_value, logical_name)

    async def destroy_all(self) -> None:
        for resource_type_value, logical_name in self._iter_live_resource_keys():
            try:
                await self.destroy(resource_type_value, logical_name)
            except Exception:
                logger.exception(
                    "Failed to destroy resource binding for %s/%s",
                    resource_type_value,
                    logical_name,
                )
        await self.store.delete_all()
        self._live_bindings.clear()

    async def delete_all_bindings(self) -> None:
        await self.store.delete_all()
        self._live_bindings.clear()

    def _iter_live_resource_keys(self) -> list[tuple[type[Any], str]]:
        keys: list[tuple[type[Any], str]] = []
        for (_, logical_name), live_binding in self._live_bindings.items():
            keys.append((live_binding.resource_type, logical_name))
        return keys

    async def _try_attach_live(
        self,
        lifecycle: type[ResourceLifecycle[Any]],
        live_ref: Any,
        ctx: ResourceContext,
        request: ResourceRequest[Any],
    ) -> Any | None:
        if not lifecycle_supports_live_refs(lifecycle):
            return None
        try:
            live_lifecycle = cast(type[LiveResourceLifecycle[Any]], lifecycle)
            attach_live = live_lifecycle.attach_live
            return await attach_live(live_ref, ctx, request)
        except Exception:
            logger.warning(
                "Failed to attach live resource %s for %s",
                live_ref.ref,
                request.resource_type_key,
                exc_info=True,
            )
            return None

    async def _remember_live_resource(
        self,
        *,
        resource_type: type[R],
        lifecycle: type[ResourceLifecycle[R]],
        request: ResourceRequest[R],
        resource: R,
        checkpoint: ResourceCheckpoint | None,
    ) -> R:
        binding_key = (resource_type_key(resource_type), request.logical_name)
        self._live_bindings[binding_key] = _LiveBinding(
            resource_type=resource_type,
            lifecycle=lifecycle,
            request=request,
            resource=resource,
        )

        if lifecycle_supports_live_refs(lifecycle):
            live_lifecycle = cast(type[LiveResourceLifecycle[R]], lifecycle)
            capture_live_ref = live_lifecycle.capture_live_ref
            live_ref = capture_live_ref(
                resource,
                self._resource_context(request.logical_name),
                request,
            )
        else:
            live_ref = None

        if live_ref is not None or checkpoint is not None:
            await self.store.save(
                ResourceBindingRecord(
                    resource_type_key=binding_key[0],
                    logical_name=request.logical_name,
                    live_ref=live_ref,
                    checkpoint=checkpoint,
                    phase="live",
                    updated_at=time.time(),
                )
            )
        return resource


@dataclass
class ResourcesExecutionNamespace:
    manager: ResourceManager | None = None

    def _require_manager(self) -> ResourceManager:
        manager = self.manager
        if manager is None:
            raise RuntimeError(
                "resources are not configured for this execution context"
            )
        return manager

    async def get_resource(
        self,
        resource_type_value: type[R],
        logical_name: str = "default",
    ) -> R:
        return await self._require_manager().get(resource_type_value, logical_name)

    async def checkpoint_resource(
        self,
        resource_type_value: type[R],
        logical_name: str = "default",
    ) -> ResourceCheckpoint | None:
        return await self._require_manager().checkpoint(
            resource_type_value,
            logical_name,
        )

    async def destroy_resource(
        self,
        resource_type_value: type[R],
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
    "ResourceManager",
    "ResourcesExecutionNamespace",
]
