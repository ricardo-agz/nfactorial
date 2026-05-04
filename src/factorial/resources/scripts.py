from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, ClassVar, Literal, Protocol

import redis.asyncio as redis
from redis.commands.core import AsyncScript

from factorial.core.lua import (
    LuaScriptContract,
    _execute_contract,
    get_cached_script,
)
from factorial.core.utils import decode

from .core import ResourceBindingRecord

ResourceBeginMode = Literal["acquire", "checkpoint", "destroy", "system_destroy"]
ResourceFinishMode = Literal["abort", "commit_checkpoint", "commit_destroy"]


class LuaScriptInput(Protocol):
    def to_lua_values(self) -> Mapping[str, Any]: ...


class _TypedLuaScript(AsyncScript):
    _CONTRACT: ClassVar[LuaScriptContract]

    async def _execute_input(self, input_value: LuaScriptInput) -> Any:
        return await _execute_contract(
            self,
            self._CONTRACT,
            input_value.to_lua_values(),
        )


def _decode_binding_record(raw: str | bytes | None) -> ResourceBindingRecord | None:
    if raw is None:
        return None
    text = decode(raw)
    if not text:
        return None
    return ResourceBindingRecord.from_json(text)


@dataclass(frozen=True)
class ResourceBeginScriptResult:
    status: str
    binding: ResourceBindingRecord | None


@dataclass(frozen=True)
class ResourceBeginScriptInput:
    task_statuses_key: str
    task_pickups_key: str
    resource_bindings_key: str
    mode: ResourceBeginMode
    task_id: str
    resource_field: str
    binding_metadata_json: str | None
    expected_pickups: int | None
    operation_id: str
    now_timestamp: float
    operation_timeout_s: float

    def to_lua_values(self) -> Mapping[str, Any]:
        return {
            "task_statuses_key": self.task_statuses_key,
            "task_pickups_key": self.task_pickups_key,
            "resource_bindings_key": self.resource_bindings_key,
            "mode": self.mode,
            "task_id": self.task_id,
            "resource_field": self.resource_field,
            "binding_metadata_json": self.binding_metadata_json,
            "expected_pickups": (
                str(self.expected_pickups)
                if self.expected_pickups is not None
                else None
            ),
            "operation_id": self.operation_id,
            "now_timestamp": repr(self.now_timestamp),
            "operation_timeout_s": repr(self.operation_timeout_s),
        }


class ResourceBeginScript(_TypedLuaScript):
    _CONTRACT = LuaScriptContract(
        script_name="ResourceBeginScript.execute",
        key_fields=(
            "task_statuses_key",
            "task_pickups_key",
            "resource_bindings_key",
        ),
        arg_fields=(
            "mode",
            "task_id",
            "resource_field",
            "binding_metadata_json",
            "expected_pickups",
            "operation_id",
            "now_timestamp",
            "operation_timeout_s",
        ),
        optional_arg_fields=frozenset({"binding_metadata_json", "expected_pickups"}),
    )

    async def execute(
        self,
        input_value: ResourceBeginScriptInput,
    ) -> ResourceBeginScriptResult:
        result: tuple[str | bytes, str | bytes | None] = await self._execute_input(
            input_value
        )
        return ResourceBeginScriptResult(
            status=decode(result[0]),
            binding=_decode_binding_record(result[1]),
        )


@dataclass(frozen=True)
class ResourceCommitLiveScriptResult:
    status: str


@dataclass(frozen=True)
class ResourceAttachUnavailableScriptResult:
    status: str


@dataclass(frozen=True)
class ResourceCommitLiveScriptInput:
    task_statuses_key: str
    task_pickups_key: str
    resource_bindings_key: str
    task_id: str
    resource_field: str
    expected_pickups: int
    operation_id: str
    now_timestamp: float
    live_ref_json: str | None
    checkpoint_json: str | None = None

    def to_lua_values(self) -> Mapping[str, Any]:
        return {
            "task_statuses_key": self.task_statuses_key,
            "task_pickups_key": self.task_pickups_key,
            "resource_bindings_key": self.resource_bindings_key,
            "task_id": self.task_id,
            "resource_field": self.resource_field,
            "expected_pickups": str(self.expected_pickups),
            "operation_id": self.operation_id,
            "now_timestamp": repr(self.now_timestamp),
            "live_ref_json": self.live_ref_json,
            "checkpoint_json": self.checkpoint_json,
        }


class ResourceCommitLiveScript(_TypedLuaScript):
    _CONTRACT = LuaScriptContract(
        script_name="ResourceCommitLiveScript.execute",
        key_fields=(
            "task_statuses_key",
            "task_pickups_key",
            "resource_bindings_key",
        ),
        arg_fields=(
            "task_id",
            "resource_field",
            "expected_pickups",
            "operation_id",
            "now_timestamp",
            "live_ref_json",
            "checkpoint_json",
        ),
        optional_arg_fields=frozenset({"live_ref_json", "checkpoint_json"}),
    )

    async def execute(
        self,
        input_value: ResourceCommitLiveScriptInput,
    ) -> ResourceCommitLiveScriptResult:
        result: str | bytes = await self._execute_input(input_value)
        return ResourceCommitLiveScriptResult(status=decode(result))


@dataclass(frozen=True)
class ResourceAttachUnavailableScriptInput:
    task_statuses_key: str
    task_pickups_key: str
    resource_bindings_key: str
    task_id: str
    resource_field: str
    expected_pickups: int
    operation_id: str
    now_timestamp: float

    def to_lua_values(self) -> Mapping[str, Any]:
        return {
            "task_statuses_key": self.task_statuses_key,
            "task_pickups_key": self.task_pickups_key,
            "resource_bindings_key": self.resource_bindings_key,
            "task_id": self.task_id,
            "resource_field": self.resource_field,
            "expected_pickups": str(self.expected_pickups),
            "operation_id": self.operation_id,
            "now_timestamp": repr(self.now_timestamp),
        }


class ResourceAttachUnavailableScript(_TypedLuaScript):
    _CONTRACT = LuaScriptContract(
        script_name="ResourceAttachUnavailableScript.execute",
        key_fields=(
            "task_statuses_key",
            "task_pickups_key",
            "resource_bindings_key",
        ),
        arg_fields=(
            "task_id",
            "resource_field",
            "expected_pickups",
            "operation_id",
            "now_timestamp",
        ),
    )

    async def execute(
        self,
        input_value: ResourceAttachUnavailableScriptInput,
    ) -> ResourceAttachUnavailableScriptResult:
        result: str | bytes = await self._execute_input(input_value)
        return ResourceAttachUnavailableScriptResult(status=decode(result))


@dataclass(frozen=True)
class ResourceFinishScriptResult:
    status: str


@dataclass(frozen=True)
class ResourceFinishScriptInput:
    resource_bindings_key: str
    mode: ResourceFinishMode
    resource_field: str
    operation_id: str
    now_timestamp: float
    checkpoint_json: str | None = None

    def to_lua_values(self) -> Mapping[str, Any]:
        return {
            "resource_bindings_key": self.resource_bindings_key,
            "mode": self.mode,
            "resource_field": self.resource_field,
            "operation_id": self.operation_id,
            "now_timestamp": repr(self.now_timestamp),
            "checkpoint_json": self.checkpoint_json,
        }


class ResourceFinishScript(_TypedLuaScript):
    _CONTRACT = LuaScriptContract(
        script_name="ResourceFinishScript.execute",
        key_fields=("resource_bindings_key",),
        arg_fields=(
            "mode",
            "resource_field",
            "operation_id",
            "now_timestamp",
            "checkpoint_json",
        ),
        optional_arg_fields=frozenset({"checkpoint_json"}),
    )

    async def execute(
        self,
        input_value: ResourceFinishScriptInput,
    ) -> ResourceFinishScriptResult:
        result: str | bytes = await self._execute_input(input_value)
        return ResourceFinishScriptResult(status=decode(result))


async def create_resource_begin_script(
    redis_client: redis.Redis,
) -> ResourceBeginScript:
    return get_cached_script(redis_client, "resource_begin", ResourceBeginScript)


async def create_resource_commit_live_script(
    redis_client: redis.Redis,
) -> ResourceCommitLiveScript:
    return get_cached_script(
        redis_client,
        "resource_commit_live",
        ResourceCommitLiveScript,
    )


async def create_resource_attach_unavailable_script(
    redis_client: redis.Redis,
) -> ResourceAttachUnavailableScript:
    return get_cached_script(
        redis_client,
        "resource_attach_unavailable",
        ResourceAttachUnavailableScript,
    )


async def create_resource_finish_script(
    redis_client: redis.Redis,
) -> ResourceFinishScript:
    return get_cached_script(redis_client, "resource_finish", ResourceFinishScript)


__all__ = [
    "LuaScriptInput",
    "ResourceBeginMode",
    "ResourceBeginScript",
    "ResourceBeginScriptInput",
    "ResourceBeginScriptResult",
    "ResourceAttachUnavailableScript",
    "ResourceAttachUnavailableScriptInput",
    "ResourceAttachUnavailableScriptResult",
    "ResourceCommitLiveScript",
    "ResourceCommitLiveScriptInput",
    "ResourceCommitLiveScriptResult",
    "ResourceFinishMode",
    "ResourceFinishScript",
    "ResourceFinishScriptInput",
    "ResourceFinishScriptResult",
    "create_resource_attach_unavailable_script",
    "create_resource_begin_script",
    "create_resource_commit_live_script",
    "create_resource_finish_script",
]
