import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, TypeVar, cast
from weakref import WeakKeyDictionary

import redis.asyncio as redis
from redis.commands.core import AsyncScript

from factorial.core.utils import decode

T = TypeVar("T", bound=AsyncScript)

_SCRIPTS_DIR = Path(__file__).parent / "scripts"
_SCRIPT_CONTENT: dict[str, str] = {}
_SHARED_SCRIPT_CONTENT: str | None = None
_SCRIPT_INSTANCES: WeakKeyDictionary[redis.Redis, dict[str, Any]] = (
    WeakKeyDictionary()
)
_SCRIPT_INSTANCES_BY_ID: dict[tuple[int, str], Any] = {}
_ALLOWED_EXECUTE_LOCALS = frozenset({"self"})


def _get_script_path(script_name: str) -> Path:
    return _SCRIPTS_DIR / f"{script_name}.lua"


def _load_shared_script_content() -> str:
    global _SHARED_SCRIPT_CONTENT
    if _SHARED_SCRIPT_CONTENT is None:
        shared_script_path = _SCRIPTS_DIR / "shared.lua"
        if not shared_script_path.exists():
            raise FileNotFoundError(f"Shared script not found: {shared_script_path}")
        _SHARED_SCRIPT_CONTENT = shared_script_path.read_text(encoding="utf-8")
    return _SHARED_SCRIPT_CONTENT


def _load_script(name: str) -> str:
    if name not in _SCRIPT_CONTENT:
        script_path = _get_script_path(name)
        if not script_path.exists():
            raise FileNotFoundError(f"Lua script not found: {script_path}")
        script_content = script_path.read_text(encoding="utf-8")
        _SCRIPT_CONTENT[name] = f"{_load_shared_script_content()}\n\n{script_content}"

    return _SCRIPT_CONTENT[name]


def _get_cached_script_by_id(client: redis.Redis, name: str, cls: type[T]) -> T:
    key = (id(client), name)
    if key not in _SCRIPT_INSTANCES_BY_ID:
        script_content = _load_script(name)
        base_script = client.register_script(script_content)
        inst = cls(registered_client=client, script=script_content)
        inst.__dict__.update(base_script.__dict__)
        _SCRIPT_INSTANCES_BY_ID[key] = inst
    return cast(T, _SCRIPT_INSTANCES_BY_ID[key])


def get_cached_script(client: redis.Redis, name: str, cls: type[T]) -> T:
    try:
        scripts_by_name = _SCRIPT_INSTANCES.get(client)
    except TypeError:
        # Some client implementations may not support weak references.
        return _get_cached_script_by_id(client, name, cls)

    if scripts_by_name is None:
        scripts_by_name = {}
        try:
            _SCRIPT_INSTANCES[client] = scripts_by_name
        except TypeError:
            return _get_cached_script_by_id(client, name, cls)
    if name not in scripts_by_name:
        script_content = _load_script(name)
        base_script = client.register_script(script_content)
        inst = cls(registered_client=client, script=script_content)
        inst.__dict__.update(base_script.__dict__)
        scripts_by_name[name] = inst
    return cast(T, scripts_by_name[name])


@dataclass(frozen=True)
class LuaScriptContract:
    script_name: str
    key_fields: tuple[str, ...]
    arg_fields: tuple[str, ...]
    optional_key_fields: frozenset[str] = field(default_factory=frozenset)
    optional_arg_fields: frozenset[str] = field(default_factory=frozenset)
    _key_field_set: frozenset[str] = field(init=False, repr=False)
    _arg_field_set: frozenset[str] = field(init=False, repr=False)
    _binding_field_set: frozenset[str] = field(init=False, repr=False)

    def __post_init__(self) -> None:
        key_field_set = frozenset(self.key_fields)
        arg_field_set = frozenset(self.arg_fields)
        shared = key_field_set.intersection(arg_field_set)
        if shared:
            names = ", ".join(sorted(shared))
            raise ValueError(
                f"{self.script_name}: field(s) cannot be both KEYS and ARGV: {names}"
            )
        unknown_optional_keys = self.optional_key_fields.difference(key_field_set)
        if unknown_optional_keys:
            names = ", ".join(sorted(unknown_optional_keys))
            raise ValueError(
                f"{self.script_name}: optional_key_fields "
                f"contain unknown key(s): {names}"
            )
        unknown_optional_args = self.optional_arg_fields.difference(arg_field_set)
        if unknown_optional_args:
            names = ", ".join(sorted(unknown_optional_args))
            raise ValueError(
                f"{self.script_name}: optional_arg_fields "
                f"contain unknown arg(s): {names}"
            )
        object.__setattr__(self, "_key_field_set", key_field_set)
        object.__setattr__(self, "_arg_field_set", arg_field_set)
        object.__setattr__(
            self,
            "_binding_field_set",
            key_field_set.union(arg_field_set),
        )

    def materialize(
        self,
        values: Mapping[str, Any],
        *,
        allowed_extras: frozenset[str] = frozenset(),
    ) -> tuple[list[Any], list[Any]]:
        unknown = sorted(
            set(values).difference(self._binding_field_set).difference(allowed_extras)
        )
        if unknown:
            names = ", ".join(f"'{name}'" for name in unknown)
            raise KeyError(f"{self.script_name}: unexpected binding(s): {names}")
        keys = [
            self._resolve(
                values=values,
                field_name=field_name,
                is_key=True,
                optional=field_name in self.optional_key_fields,
            )
            for field_name in self.key_fields
        ]
        args = [
            self._resolve(
                values=values,
                field_name=field_name,
                is_key=False,
                optional=field_name in self.optional_arg_fields,
            )
            for field_name in self.arg_fields
        ]
        return keys, args

    def _resolve(
        self,
        *,
        values: Mapping[str, Any],
        field_name: str,
        is_key: bool,
        optional: bool,
    ) -> Any:
        kind = "KEYS" if is_key else "ARGV"
        if field_name not in values:
            raise KeyError(f"{self.script_name}: missing {kind} binding '{field_name}'")

        value = values[field_name]
        if value is None:
            if optional:
                return ""
            raise ValueError(
                f"{self.script_name}: required {kind} binding "
                f"'{field_name}' cannot be None"
            )

        if is_key and not isinstance(value, str):
            raise TypeError(
                f"{self.script_name}: key '{field_name}' must be str, "
                f"got {type(value).__name__}"
            )
        return value


async def _execute_contract(
    script: AsyncScript,
    contract: LuaScriptContract,
    values: Mapping[str, Any],
) -> Any:
    keys, args = contract.materialize(values, allowed_extras=_ALLOWED_EXECUTE_LOCALS)
    return await script.__call__(keys=keys, args=args)


def _decode_json_string_list(raw: str | bytes | None) -> list[str]:
    if raw is None:
        return []
    text = decode(raw)
    if not text:
        return []
    try:
        parsed = json.loads(text)
    except Exception:
        return []
    if not isinstance(parsed, list):
        return []
    return [str(item) for item in parsed]
