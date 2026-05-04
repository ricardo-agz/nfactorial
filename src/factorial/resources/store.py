from __future__ import annotations

import json
from collections.abc import Awaitable
from dataclasses import dataclass
from typing import Any, Literal, Protocol

import redis.asyncio as redis

from factorial.core.logging import get_logger
from factorial.core.utils import decode, resolve_awaitable

from .core import (
    RESOURCE_PHASE_ATTACHING,
    RESOURCE_PHASE_CHECKPOINTED,
    RESOURCE_PHASE_CHECKPOINTING,
    RESOURCE_PHASE_CREATING,
    RESOURCE_PHASE_DESTROYING,
    RESOURCE_PHASE_FRESH,
    RESOURCE_PHASE_LIVE,
    RESOURCE_PHASE_RESTORING,
    LiveResourceRef,
    ResourceBindingRecord,
    ResourceCheckpoint,
)
from .scripts import (
    ResourceAttachUnavailableScript,
    ResourceAttachUnavailableScriptInput,
    ResourceBeginMode,
    ResourceBeginScript,
    ResourceBeginScriptInput,
    ResourceCommitLiveScript,
    ResourceCommitLiveScriptInput,
    ResourceFinishScript,
    ResourceFinishScriptInput,
    create_resource_attach_unavailable_script,
    create_resource_begin_script,
    create_resource_commit_live_script,
    create_resource_finish_script,
)

logger = get_logger(__name__)


class ResourceLeaseLostError(RuntimeError):
    """Raised when the current worker no longer owns the task's resource lease."""


@dataclass(frozen=True)
class ResourceLease:
    mode: Literal["local", "worker", "system"] = "local"
    processing_pickups: int | None = None

    @classmethod
    def local(cls) -> ResourceLease:
        return cls(mode="local")

    @classmethod
    def worker(cls, processing_pickups: int) -> ResourceLease:
        return cls(mode="worker", processing_pickups=processing_pickups)

    @classmethod
    def system(cls) -> ResourceLease:
        return cls(mode="system")

    @property
    def is_worker(self) -> bool:
        return self.mode == "worker"

    @property
    def is_system(self) -> bool:
        return self.mode == "system"


@dataclass(frozen=True)
class ResourceReservation:
    resource_type_key: str
    logical_name: str
    operation_id: str


@dataclass(frozen=True)
class ResourceDecision:
    outcome: str
    record: ResourceBindingRecord | None = None
    reservation: ResourceReservation | None = None


class ResourceBindingStore(Protocol):
    async def begin_acquire(
        self,
        *,
        resource_type_key_value: str,
        logical_name: str,
        binding_metadata: dict[str, Any],
        lease: ResourceLease,
        operation_id: str,
        now: float,
        operation_timeout_s: float,
    ) -> ResourceDecision: ...

    async def begin_checkpoint(
        self,
        *,
        resource_type_key_value: str,
        logical_name: str,
        lease: ResourceLease,
        operation_id: str,
        now: float,
        operation_timeout_s: float,
    ) -> ResourceDecision: ...

    async def begin_destroy(
        self,
        *,
        resource_type_key_value: str,
        logical_name: str,
        lease: ResourceLease,
        operation_id: str,
        now: float,
        operation_timeout_s: float,
    ) -> ResourceDecision: ...

    async def commit_live(
        self,
        *,
        reservation: ResourceReservation,
        lease: ResourceLease,
        live_ref: LiveResourceRef | None,
        checkpoint: ResourceCheckpoint | None,
        now: float,
    ) -> str: ...

    async def commit_attach_unavailable(
        self,
        *,
        reservation: ResourceReservation,
        lease: ResourceLease,
        now: float,
    ) -> str: ...

    async def commit_checkpoint(
        self,
        *,
        reservation: ResourceReservation,
        checkpoint: ResourceCheckpoint | None,
        now: float,
    ) -> str: ...

    async def commit_destroy(
        self,
        *,
        reservation: ResourceReservation,
        now: float,
    ) -> str: ...

    async def abort_operation(
        self,
        *,
        reservation: ResourceReservation,
        now: float,
    ) -> str: ...

    async def list_bindings(self) -> list[ResourceBindingRecord]: ...

    async def clear_all(self) -> None: ...

    async def validate_lease(self, lease: ResourceLease) -> bool: ...


class InMemoryResourceBindingStore:
    def __init__(self) -> None:
        self._bindings: dict[tuple[str, str], ResourceBindingRecord] = {}

    def _key(self, resource_type_key_value: str, logical_name: str) -> tuple[str, str]:
        return (resource_type_key_value, logical_name)

    def _recover_record(
        self,
        record: ResourceBindingRecord | None,
        *,
        now: float,
        operation_timeout_s: float,
    ) -> ResourceBindingRecord | None:
        if record is None:
            return None
        return record.recover(
            now=now,
            operation_timeout_s=operation_timeout_s,
        ).record

    def _binding_metadata_for_create(
        self,
        record: ResourceBindingRecord | None,
        binding_metadata: dict[str, Any],
    ) -> dict[str, Any]:
        if record is not None and (
            record.binding_metadata or not binding_metadata
        ):
            return dict(record.binding_metadata)
        return dict(binding_metadata)

    async def begin_acquire(
        self,
        *,
        resource_type_key_value: str,
        logical_name: str,
        binding_metadata: dict[str, Any],
        lease: ResourceLease,
        operation_id: str,
        now: float,
        operation_timeout_s: float,
    ) -> ResourceDecision:
        if lease.is_system:
            return ResourceDecision(outcome="not_allowed")
        key = self._key(resource_type_key_value, logical_name)
        record = self._recover_record(
            self._bindings.get(key),
            now=now,
            operation_timeout_s=operation_timeout_s,
        )
        if record is not None and record.operation_id is not None:
            return ResourceDecision(outcome="busy", record=record)

        reservation = ResourceReservation(
            resource_type_key=resource_type_key_value,
            logical_name=logical_name,
            operation_id=operation_id,
        )
        if record is None:
            self._bindings[key] = ResourceBindingRecord(
                resource_type_key=resource_type_key_value,
                logical_name=logical_name,
                binding_metadata=self._binding_metadata_for_create(
                    record,
                    binding_metadata,
                ),
                phase=RESOURCE_PHASE_CREATING,
                owner_pickups=lease.processing_pickups,
                operation_id=operation_id,
                updated_at=now,
            )
            return ResourceDecision(outcome="create", reservation=reservation)

        if record.has_live_ref():
            record.phase = RESOURCE_PHASE_ATTACHING
            record.owner_pickups = lease.processing_pickups
            record.operation_id = operation_id
            record.updated_at = now
            self._bindings[key] = record
            return ResourceDecision(
                outcome="attach",
                record=ResourceBindingRecord.from_dict(record.to_dict()),
                reservation=reservation,
            )

        if record.has_checkpoint():
            record.phase = RESOURCE_PHASE_RESTORING
            record.owner_pickups = lease.processing_pickups
            record.operation_id = operation_id
            record.updated_at = now
            self._bindings[key] = record
            return ResourceDecision(
                outcome="restore",
                record=ResourceBindingRecord.from_dict(record.to_dict()),
                reservation=reservation,
            )

        self._bindings[key] = ResourceBindingRecord(
            resource_type_key=resource_type_key_value,
            logical_name=logical_name,
            binding_metadata=self._binding_metadata_for_create(
                record,
                binding_metadata,
            ),
            phase=RESOURCE_PHASE_CREATING,
            owner_pickups=lease.processing_pickups,
            operation_id=operation_id,
            updated_at=now,
        )
        return ResourceDecision(outcome="create", reservation=reservation)

    async def begin_checkpoint(
        self,
        *,
        resource_type_key_value: str,
        logical_name: str,
        lease: ResourceLease,
        operation_id: str,
        now: float,
        operation_timeout_s: float,
    ) -> ResourceDecision:
        if lease.is_system:
            return ResourceDecision(outcome="not_allowed")
        key = self._key(resource_type_key_value, logical_name)
        record = self._recover_record(
            self._bindings.get(key),
            now=now,
            operation_timeout_s=operation_timeout_s,
        )
        if record is None or not record.has_live_ref():
            return ResourceDecision(outcome="missing", record=record)
        if record.operation_id is not None:
            return ResourceDecision(outcome="busy", record=record)
        record.phase = RESOURCE_PHASE_CHECKPOINTING
        record.owner_pickups = lease.processing_pickups
        record.operation_id = operation_id
        record.updated_at = now
        self._bindings[key] = record
        return ResourceDecision(
            outcome="ok",
            record=ResourceBindingRecord.from_dict(record.to_dict()),
            reservation=ResourceReservation(
                resource_type_key=resource_type_key_value,
                logical_name=logical_name,
                operation_id=operation_id,
            ),
        )

    async def begin_destroy(
        self,
        *,
        resource_type_key_value: str,
        logical_name: str,
        lease: ResourceLease,
        operation_id: str,
        now: float,
        operation_timeout_s: float,
    ) -> ResourceDecision:
        key = self._key(resource_type_key_value, logical_name)
        record = self._recover_record(
            self._bindings.get(key),
            now=now,
            operation_timeout_s=operation_timeout_s,
        )
        if record is None:
            return ResourceDecision(outcome="missing")
        if record.operation_id is not None:
            return ResourceDecision(outcome="busy", record=record)
        record.phase = RESOURCE_PHASE_DESTROYING
        record.owner_pickups = lease.processing_pickups
        record.operation_id = operation_id
        record.updated_at = now
        self._bindings[key] = record
        return ResourceDecision(
            outcome="ok",
            record=ResourceBindingRecord.from_dict(record.to_dict()),
            reservation=ResourceReservation(
                resource_type_key=resource_type_key_value,
                logical_name=logical_name,
                operation_id=operation_id,
            ),
        )

    async def commit_live(
        self,
        *,
        reservation: ResourceReservation,
        lease: ResourceLease,
        live_ref: LiveResourceRef | None,
        checkpoint: ResourceCheckpoint | None,
        now: float,
    ) -> str:
        key = self._key(reservation.resource_type_key, reservation.logical_name)
        record = self._bindings.get(key)
        if record is None:
            return "missing"
        if record.operation_id != reservation.operation_id:
            return "operation_conflict"
        record.phase = RESOURCE_PHASE_LIVE
        record.live_ref = live_ref
        if checkpoint is not None:
            record.checkpoint = checkpoint
        record.owner_pickups = lease.processing_pickups
        record.operation_id = None
        record.updated_at = now
        self._bindings[key] = record
        return "ok"

    async def commit_attach_unavailable(
        self,
        *,
        reservation: ResourceReservation,
        lease: ResourceLease,
        now: float,
    ) -> str:
        del lease
        key = self._key(reservation.resource_type_key, reservation.logical_name)
        record = self._bindings.get(key)
        if record is None:
            return "missing"
        if record.operation_id != reservation.operation_id:
            return "operation_conflict"
        record.live_ref = None
        record.owner_pickups = None
        record.operation_id = None
        record.updated_at = now
        record.phase = (
            RESOURCE_PHASE_CHECKPOINTED
            if record.has_checkpoint()
            else RESOURCE_PHASE_FRESH
        )
        self._bindings[key] = record
        return "ok"

    async def commit_checkpoint(
        self,
        *,
        reservation: ResourceReservation,
        checkpoint: ResourceCheckpoint | None,
        now: float,
    ) -> str:
        key = self._key(reservation.resource_type_key, reservation.logical_name)
        record = self._bindings.get(key)
        if record is None:
            return "missing"
        if record.operation_id != reservation.operation_id:
            return "operation_conflict"
        if checkpoint is None:
            self._bindings.pop(key, None)
            return "ok"
        record.phase = RESOURCE_PHASE_CHECKPOINTED
        record.live_ref = None
        record.checkpoint = checkpoint
        record.owner_pickups = None
        record.operation_id = None
        record.updated_at = now
        self._bindings[key] = record
        return "ok"

    async def commit_destroy(
        self,
        *,
        reservation: ResourceReservation,
        now: float,
    ) -> str:
        del now
        key = self._key(reservation.resource_type_key, reservation.logical_name)
        record = self._bindings.get(key)
        if record is None:
            return "missing"
        if record.operation_id != reservation.operation_id:
            return "operation_conflict"
        self._bindings.pop(key, None)
        return "ok"

    async def abort_operation(
        self,
        *,
        reservation: ResourceReservation,
        now: float,
    ) -> str:
        key = self._key(reservation.resource_type_key, reservation.logical_name)
        record = self._bindings.get(key)
        if record is None:
            return "missing"
        if record.operation_id != reservation.operation_id:
            return "operation_conflict"
        record.operation_id = None
        record.owner_pickups = None
        record.updated_at = now
        if record.has_live_ref():
            record.phase = RESOURCE_PHASE_LIVE
            self._bindings[key] = record
            return "ok"
        if record.has_checkpoint():
            record.phase = RESOURCE_PHASE_CHECKPOINTED
            self._bindings[key] = record
            return "ok"
        self._bindings.pop(key, None)
        return "ok"

    async def list_bindings(self) -> list[ResourceBindingRecord]:
        return [
            ResourceBindingRecord.from_dict(record.to_dict())
            for record in self._bindings.values()
        ]

    async def clear_all(self) -> None:
        self._bindings.clear()

    async def validate_lease(self, lease: ResourceLease) -> bool:
        del lease
        return True


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

        self.task_id = task_id
        self.keys = RedisKeys.format(namespace=namespace, task_id=task_id)
        self.root_keys = RedisKeys.format(namespace=namespace)
        self._begin_script: ResourceBeginScript | None = None
        self._commit_live_script: ResourceCommitLiveScript | None = None
        self._attach_unavailable_script: ResourceAttachUnavailableScript | None = None
        self._finish_script: ResourceFinishScript | None = None

    def _field_name(self, resource_type_key_value: str, logical_name: str) -> str:
        return f"{resource_type_key_value}:{logical_name}"

    async def _get_begin_script(self) -> ResourceBeginScript:
        if self._begin_script is None:
            self._begin_script = await create_resource_begin_script(self.redis_client)
        return self._begin_script

    async def _get_commit_live_script(self) -> ResourceCommitLiveScript:
        if self._commit_live_script is None:
            self._commit_live_script = await create_resource_commit_live_script(
                self.redis_client
            )
        return self._commit_live_script

    async def _get_attach_unavailable_script(self) -> ResourceAttachUnavailableScript:
        if self._attach_unavailable_script is None:
            self._attach_unavailable_script = (
                await create_resource_attach_unavailable_script(self.redis_client)
            )
        return self._attach_unavailable_script

    async def _get_finish_script(self) -> ResourceFinishScript:
        if self._finish_script is None:
            self._finish_script = await create_resource_finish_script(self.redis_client)
        return self._finish_script

    def _reservation(
        self,
        resource_type_key_value: str,
        logical_name: str,
        operation_id: str,
    ) -> ResourceReservation:
        return ResourceReservation(
            resource_type_key=resource_type_key_value,
            logical_name=logical_name,
            operation_id=operation_id,
        )

    async def begin_acquire(
        self,
        *,
        resource_type_key_value: str,
        logical_name: str,
        binding_metadata: dict[str, Any],
        lease: ResourceLease,
        operation_id: str,
        now: float,
        operation_timeout_s: float,
    ) -> ResourceDecision:
        if not lease.is_worker:
            return ResourceDecision(outcome="not_allowed")
        result = await (await self._get_begin_script()).execute(
            ResourceBeginScriptInput(
                task_statuses_key=self.root_keys.task_status,
                task_pickups_key=self.root_keys.task_pickups,
                resource_bindings_key=self.keys.resource_bindings,
                mode="acquire",
                task_id=self.task_id,
                resource_field=self._field_name(resource_type_key_value, logical_name),
                binding_metadata_json=(
                    json.dumps(binding_metadata, sort_keys=True)
                    if binding_metadata
                    else None
                ),
                expected_pickups=lease.processing_pickups,
                operation_id=operation_id,
                now_timestamp=now,
                operation_timeout_s=operation_timeout_s,
            )
        )
        if result.status in {"create", "restore", "attach"}:
            return ResourceDecision(
                outcome=result.status,
                record=result.binding,
                reservation=self._reservation(
                    resource_type_key_value,
                    logical_name,
                    operation_id,
                ),
            )
        return ResourceDecision(outcome=result.status, record=result.binding)

    async def begin_checkpoint(
        self,
        *,
        resource_type_key_value: str,
        logical_name: str,
        lease: ResourceLease,
        operation_id: str,
        now: float,
        operation_timeout_s: float,
    ) -> ResourceDecision:
        if not lease.is_worker:
            return ResourceDecision(outcome="not_allowed")
        result = await (await self._get_begin_script()).execute(
            ResourceBeginScriptInput(
                task_statuses_key=self.root_keys.task_status,
                task_pickups_key=self.root_keys.task_pickups,
                resource_bindings_key=self.keys.resource_bindings,
                mode="checkpoint",
                task_id=self.task_id,
                resource_field=self._field_name(resource_type_key_value, logical_name),
                binding_metadata_json=None,
                expected_pickups=lease.processing_pickups,
                operation_id=operation_id,
                now_timestamp=now,
                operation_timeout_s=operation_timeout_s,
            )
        )
        if result.status == "ok":
            return ResourceDecision(
                outcome="ok",
                record=result.binding,
                reservation=self._reservation(
                    resource_type_key_value,
                    logical_name,
                    operation_id,
                ),
            )
        return ResourceDecision(outcome=result.status, record=result.binding)

    async def begin_destroy(
        self,
        *,
        resource_type_key_value: str,
        logical_name: str,
        lease: ResourceLease,
        operation_id: str,
        now: float,
        operation_timeout_s: float,
    ) -> ResourceDecision:
        mode: ResourceBeginMode = "destroy" if lease.is_worker else "system_destroy"
        result = await (await self._get_begin_script()).execute(
            ResourceBeginScriptInput(
                task_statuses_key=self.root_keys.task_status,
                task_pickups_key=self.root_keys.task_pickups,
                resource_bindings_key=self.keys.resource_bindings,
                mode=mode,
                task_id=self.task_id,
                resource_field=self._field_name(resource_type_key_value, logical_name),
                binding_metadata_json=None,
                expected_pickups=lease.processing_pickups,
                operation_id=operation_id,
                now_timestamp=now,
                operation_timeout_s=operation_timeout_s,
            )
        )
        if result.status == "ok":
            return ResourceDecision(
                outcome="ok",
                record=result.binding,
                reservation=self._reservation(
                    resource_type_key_value,
                    logical_name,
                    operation_id,
                ),
            )
        return ResourceDecision(outcome=result.status, record=result.binding)

    async def commit_live(
        self,
        *,
        reservation: ResourceReservation,
        lease: ResourceLease,
        live_ref: LiveResourceRef | None,
        checkpoint: ResourceCheckpoint | None,
        now: float,
    ) -> str:
        if not lease.is_worker or lease.processing_pickups is None:
            return "not_allowed"
        checkpoint_json = checkpoint.to_json() if checkpoint is not None else None
        result = await (await self._get_commit_live_script()).execute(
            ResourceCommitLiveScriptInput(
                task_statuses_key=self.root_keys.task_status,
                task_pickups_key=self.root_keys.task_pickups,
                resource_bindings_key=self.keys.resource_bindings,
                task_id=self.task_id,
                resource_field=self._field_name(
                    reservation.resource_type_key,
                    reservation.logical_name,
                ),
                expected_pickups=lease.processing_pickups,
                operation_id=reservation.operation_id,
                now_timestamp=now,
                live_ref_json=live_ref.to_json() if live_ref is not None else None,
                checkpoint_json=checkpoint_json,
            )
        )
        return result.status

    async def commit_attach_unavailable(
        self,
        *,
        reservation: ResourceReservation,
        lease: ResourceLease,
        now: float,
    ) -> str:
        if not lease.is_worker or lease.processing_pickups is None:
            return "not_allowed"
        result = await (await self._get_attach_unavailable_script()).execute(
            ResourceAttachUnavailableScriptInput(
                task_statuses_key=self.root_keys.task_status,
                task_pickups_key=self.root_keys.task_pickups,
                resource_bindings_key=self.keys.resource_bindings,
                task_id=self.task_id,
                resource_field=self._field_name(
                    reservation.resource_type_key,
                    reservation.logical_name,
                ),
                expected_pickups=lease.processing_pickups,
                operation_id=reservation.operation_id,
                now_timestamp=now,
            )
        )
        return result.status

    async def commit_checkpoint(
        self,
        *,
        reservation: ResourceReservation,
        checkpoint: ResourceCheckpoint | None,
        now: float,
    ) -> str:
        result = await (await self._get_finish_script()).execute(
            ResourceFinishScriptInput(
                resource_bindings_key=self.keys.resource_bindings,
                mode="commit_checkpoint",
                resource_field=self._field_name(
                    reservation.resource_type_key,
                    reservation.logical_name,
                ),
                operation_id=reservation.operation_id,
                now_timestamp=now,
                checkpoint_json=(
                    checkpoint.to_json() if checkpoint is not None else None
                ),
            )
        )
        return result.status

    async def commit_destroy(
        self,
        *,
        reservation: ResourceReservation,
        now: float,
    ) -> str:
        result = await (await self._get_finish_script()).execute(
            ResourceFinishScriptInput(
                resource_bindings_key=self.keys.resource_bindings,
                mode="commit_destroy",
                resource_field=self._field_name(
                    reservation.resource_type_key,
                    reservation.logical_name,
                ),
                operation_id=reservation.operation_id,
                now_timestamp=now,
            )
        )
        return result.status

    async def abort_operation(
        self,
        *,
        reservation: ResourceReservation,
        now: float,
    ) -> str:
        result = await (await self._get_finish_script()).execute(
            ResourceFinishScriptInput(
                resource_bindings_key=self.keys.resource_bindings,
                mode="abort",
                resource_field=self._field_name(
                    reservation.resource_type_key,
                    reservation.logical_name,
                ),
                operation_id=reservation.operation_id,
                now_timestamp=now,
            )
        )
        return result.status

    async def list_bindings(self) -> list[ResourceBindingRecord]:
        raw_values_result = self.redis_client.hvals(self.keys.resource_bindings)
        if isinstance(raw_values_result, Awaitable):
            raw_values = await raw_values_result
        else:
            raw_values = raw_values_result
        records: list[ResourceBindingRecord] = []
        for raw in raw_values:
            text = decode(raw)
            if not text:
                continue
            try:
                records.append(ResourceBindingRecord.from_json(text))
            except Exception:
                logger.warning(
                    "Ignoring invalid resource binding payload for task %s",
                    self.task_id,
                    exc_info=True,
                )
        return records

    async def clear_all(self) -> None:
        await resolve_awaitable(self.redis_client.delete(self.keys.resource_bindings))

    async def validate_lease(self, lease: ResourceLease) -> bool:
        if not lease.is_worker or lease.processing_pickups is None:
            return True
        pipe = self.redis_client.pipeline(transaction=True)
        pipe.hget(self.root_keys.task_status, self.task_id)
        pipe.hget(self.root_keys.task_pickups, self.task_id)
        status_raw, pickups_raw = await pipe.execute()
        if status_raw is None or pickups_raw is None:
            return False
        try:
            return (
                decode(status_raw) == "processing"
                and int(decode(pickups_raw)) == lease.processing_pickups
            )
        except (TypeError, ValueError):
            return False


__all__ = [
    "InMemoryResourceBindingStore",
    "RedisResourceBindingStore",
    "ResourceBindingStore",
    "ResourceDecision",
    "ResourceLease",
    "ResourceLeaseLostError",
    "ResourceReservation",
]
