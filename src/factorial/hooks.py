from __future__ import annotations

import inspect
import json
import secrets
import uuid
from collections.abc import Awaitable, Callable, Coroutine
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from types import UnionType
from typing import (
    Annotated,
    Any,
    Generic,
    Literal,
    TypeAlias,
    TypeVar,
    cast,
    get_args,
    get_origin,
    get_type_hints,
)

from pydantic import BaseModel, ConfigDict

from factorial.context import AgentContext, ExecutionContext
from factorial.utils import decode

HookT = TypeVar("HookT", bound="Hook")
HookRequestBuilder = Callable[..., "PendingHook[Any] | Awaitable[PendingHook[Any]]"]
HookParamName: TypeAlias = str


class HookCompilationError(ValueError):
    """Raised when hook dependency metadata cannot be compiled."""


class HookDependencyResolutionError(HookCompilationError):
    """Raised when request-builder dependencies cannot be resolved."""


class HookTypeMismatchError(HookCompilationError):
    """Raised when hook dependency type annotations are incompatible."""


class HookDependencyCycleError(HookCompilationError):
    """Raised when inferred hook dependencies contain a cycle."""


@dataclass
class HookRequestContext:
    """Context passed to hook request-builder functions."""

    task_id: str
    owner_id: str
    agent_name: str
    tool_name: str
    tool_call_id: str
    args: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)
    hooks_public_base_url: str | None = None


@dataclass
class PendingHook(Generic[HookT]):
    hook_id: str
    submit_url: str | None
    token: str
    expires_at: datetime
    hook_type: type[HookT]
    title: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def auth_headers(self) -> dict[str, str]:
        return {"X-NFactorial-Hook-Token": self.token}

    def auth_query(self) -> dict[str, str]:
        return {"token": self.token}


class Hook(BaseModel):
    """Base class for typed hook payloads."""

    model_config = ConfigDict(extra="forbid")

    @classmethod
    def pending(
        cls: type[HookT],
        *,
        ctx: HookRequestContext,
        title: str | None = None,
        body: str | None = None,
        channel: str | None = None,
        timeout_s: float = 300.0,
        metadata: dict[str, Any] | None = None,
        submit_url: str | None = None,
        hook_id: str | None = None,
        token: str | None = None,
        **extra: Any,
    ) -> PendingHook[HookT]:
        resolved_hook_id = hook_id or str(uuid.uuid4())
        resolved_token = token or secrets.token_urlsafe(32)
        expires_at = datetime.now(timezone.utc) + timedelta(seconds=float(timeout_s))

        resolved_submit_url = submit_url
        if resolved_submit_url is None and ctx.hooks_public_base_url:
            base = ctx.hooks_public_base_url.rstrip("/")
            resolved_submit_url = f"{base}/hooks/{resolved_hook_id}/submit"

        resolved_metadata = dict(metadata or {})
        if body is not None:
            resolved_metadata.setdefault("body", body)
        if channel is not None:
            resolved_metadata.setdefault("channel", channel)
        if extra:
            resolved_metadata.update(extra)

        return PendingHook(
            hook_id=resolved_hook_id,
            submit_url=resolved_submit_url,
            token=resolved_token,
            expires_at=expires_at,
            hook_type=cls,
            title=title,
            metadata=resolved_metadata,
        )


@dataclass(frozen=True)
class HookDependency:
    mode: Literal["requires", "awaits"]
    request: HookRequestBuilder


@dataclass(frozen=True)
class HookNodeSpec:
    name: HookParamName
    hook_type: type[Hook]
    mode: Literal["requires", "awaits"]
    request_builder: HookRequestBuilder
    depends_on: tuple[HookParamName, ...]
    order_index: int


@dataclass(frozen=True)
class HookExecutionPlan:
    tool_name: str
    tool_args: tuple[str, ...]
    hook_order: tuple[HookParamName, ...]
    stages: tuple[tuple[HookParamName, ...], ...]
    nodes: dict[HookParamName, HookNodeSpec]


@dataclass(frozen=True)
class HookResolutionResult:
    hook_id: str
    task_id: str
    tool_call_id: str
    status: Literal["resolved", "idempotent"]
    task_resumed: bool


@dataclass
class HookRecord:
    """Persisted hook session node state."""

    hook_id: str
    task_id: str
    tool_call_id: str
    agent_name: str
    owner_id: str
    hook_type: str
    mode: Literal["requires", "awaits"]
    tool_name: str | None = None
    tool_args: dict[str, Any] = field(default_factory=dict)
    hook_param_name: str | None = None
    depends_on: tuple[str, ...] = ()
    session_id: str | None = None
    status: Literal["requested", "resolved", "expired", "failed"] = "requested"
    submit_url: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    created_at: float = 0.0
    expires_at: float = 0.0
    resolved_at: float | None = None
    payload: Any = None
    token_hashes: list[str] = field(default_factory=list)
    token_version: int = 1

    def to_dict(self) -> dict[str, Any]:
        return {
            "hook_id": self.hook_id,
            "task_id": self.task_id,
            "tool_call_id": self.tool_call_id,
            "agent_name": self.agent_name,
            "owner_id": self.owner_id,
            "hook_type": self.hook_type,
            "mode": self.mode,
            "tool_name": self.tool_name,
            "tool_args": self.tool_args,
            "hook_param_name": self.hook_param_name,
            "depends_on": list(self.depends_on),
            "session_id": self.session_id,
            "status": self.status,
            "submit_url": self.submit_url,
            "metadata": self.metadata,
            "created_at": self.created_at,
            "expires_at": self.expires_at,
            "resolved_at": self.resolved_at,
            "payload": self.payload,
            "token_hashes": self.token_hashes,
            "token_version": self.token_version,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> HookRecord:
        return cls(
            hook_id=str(data["hook_id"]),
            task_id=str(data["task_id"]),
            tool_call_id=str(data["tool_call_id"]),
            agent_name=str(data["agent_name"]),
            owner_id=str(data["owner_id"]),
            hook_type=str(data.get("hook_type", "Hook")),
            mode=cast(
                Literal["requires", "awaits"],
                data.get("mode", "requires"),
            ),
            tool_name=(
                str(data["tool_name"]) if data.get("tool_name") is not None else None
            ),
            tool_args=dict(data.get("tool_args") or {}),
            hook_param_name=(
                str(data["hook_param_name"])
                if data.get("hook_param_name") is not None
                else None
            ),
            depends_on=tuple(str(dep) for dep in data.get("depends_on", [])),
            session_id=(
                str(data["session_id"]) if data.get("session_id") is not None else None
            ),
            status=cast(
                Literal["requested", "resolved", "expired", "failed"],
                data.get("status", "requested"),
            ),
            submit_url=(
                str(data["submit_url"]) if data.get("submit_url") is not None else None
            ),
            metadata=dict(data.get("metadata") or {}),
            created_at=float(data.get("created_at", 0.0)),
            expires_at=float(data.get("expires_at", 0.0)),
            resolved_at=(
                float(data["resolved_at"])
                if data.get("resolved_at") is not None
                else None
            ),
            payload=data.get("payload"),
            token_hashes=[
                str(token_hash)
                for token_hash in data.get("token_hashes", [])
                if isinstance(token_hash, (str, bytes))
            ],
            token_version=int(data.get("token_version", 1)),
        )

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), default=str)

    @classmethod
    def from_json(cls, json_str: str | bytes) -> HookRecord:
        return cls.from_dict(json.loads(decode(json_str)))


@dataclass
class HookSessionNode:
    """Per-parameter node state for a persisted hook session."""

    param_name: str
    mode: Literal["requires", "awaits"]
    hook_type: str
    depends_on: tuple[str, ...]
    status: Literal["unrequested", "requested", "resolved", "expired", "failed"] = (
        "unrequested"
    )
    hook_id: str | None = None
    payload: Any = None
    requested_at: float | None = None
    resolved_at: float | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "param_name": self.param_name,
            "mode": self.mode,
            "hook_type": self.hook_type,
            "depends_on": list(self.depends_on),
            "status": self.status,
            "hook_id": self.hook_id,
            "payload": self.payload,
            "requested_at": self.requested_at,
            "resolved_at": self.resolved_at,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> HookSessionNode:
        return cls(
            param_name=str(data["param_name"]),
            mode=cast(Literal["requires", "awaits"], data["mode"]),
            hook_type=str(data["hook_type"]),
            depends_on=tuple(str(dep) for dep in data.get("depends_on", [])),
            status=cast(
                Literal["unrequested", "requested", "resolved", "expired", "failed"],
                data.get("status", "unrequested"),
            ),
            hook_id=str(data["hook_id"]) if data.get("hook_id") is not None else None,
            payload=data.get("payload"),
            requested_at=(
                float(data["requested_at"])
                if data.get("requested_at") is not None
                else None
            ),
            resolved_at=(
                float(data["resolved_at"])
                if data.get("resolved_at") is not None
                else None
            ),
        )


@dataclass
class HookSessionRecord:
    """Persisted DAG session for one tool call."""

    session_id: str
    task_id: str
    tool_call_id: str
    tool_name: str
    tool_args: dict[str, Any]
    nodes: dict[str, HookSessionNode]
    status: Literal["active", "completed", "failed", "expired"] = "active"
    created_at: float = 0.0
    updated_at: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "session_id": self.session_id,
            "task_id": self.task_id,
            "tool_call_id": self.tool_call_id,
            "tool_name": self.tool_name,
            "tool_args": self.tool_args,
            "nodes": {name: node.to_dict() for name, node in self.nodes.items()},
            "status": self.status,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> HookSessionRecord:
        nodes_raw = cast(dict[str, dict[str, Any]], data.get("nodes", {}))
        return cls(
            session_id=str(data["session_id"]),
            task_id=str(data["task_id"]),
            tool_call_id=str(data["tool_call_id"]),
            tool_name=str(data["tool_name"]),
            tool_args=dict(data.get("tool_args") or {}),
            nodes={
                str(name): HookSessionNode.from_dict(node_data)
                for name, node_data in nodes_raw.items()
            },
            status=cast(
                Literal["active", "completed", "failed", "expired"],
                data.get("status", "active"),
            ),
            created_at=float(data.get("created_at", 0.0)),
            updated_at=float(data.get("updated_at", 0.0)),
        )

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), default=str)

    @classmethod
    def from_json(cls, json_str: str | bytes) -> HookSessionRecord:
        return cls.from_dict(json.loads(decode(json_str)))


def build_request_builder_kwargs(
    *,
    request_builder: HookRequestBuilder,
    request_ctx: HookRequestContext,
    tool_args: dict[str, Any],
    resolved_hook_payloads: dict[str, Any],
) -> dict[str, Any]:
    """Build DI kwargs for a hook request-builder at runtime."""
    sig = inspect.signature(request_builder)
    hints = get_type_hints(request_builder, include_extras=True)
    kwargs: dict[str, Any] = {}

    for param_name, param in sig.parameters.items():
        param_ann = hints.get(param_name, param.annotation)
        if _is_request_context_param(param_name, param_ann):
            kwargs[param_name] = request_ctx
            continue
        if param_name in tool_args:
            kwargs[param_name] = tool_args[param_name]
            continue
        if param_name in resolved_hook_payloads:
            resolved_value = resolved_hook_payloads[param_name]
            if (
                isinstance(param_ann, type)
                and issubclass(param_ann, BaseModel)
                and isinstance(resolved_value, dict)
            ):
                resolved_value = param_ann(**resolved_value)
            kwargs[param_name] = resolved_value
            continue
        if param.default is not inspect.Parameter.empty:
            continue
        raise HookDependencyResolutionError(
            f"Cannot resolve required request-builder parameter '{param_name}'. "
            f"Expected ctx, tool args, resolved hook payloads, or optional default."
        )

    return kwargs


class HookNamespace:
    """Namespace-style hook API (`hook.requires`, `hook.awaits`)."""

    def requires(self, request: HookRequestBuilder) -> HookDependency:
        if not callable(request):
            raise TypeError("hook.requires(...) expects a callable request builder")
        return HookDependency(mode="requires", request=request)

    def awaits(self, request: HookRequestBuilder) -> HookDependency:
        if not callable(request):
            raise TypeError("hook.awaits(...) expects a callable request builder")
        return HookDependency(mode="awaits", request=request)


hook = HookNamespace()


def _is_union(origin: Any) -> bool:
    return origin is UnionType or str(origin) == "typing.Union"


def _strip_optional(annotation: Any) -> Any:
    origin = get_origin(annotation)
    if _is_union(origin):
        args = [arg for arg in get_args(annotation) if arg is not type(None)]
        if len(args) == 1:
            return args[0]
    return annotation


def _is_hook_type(annotation: Any) -> bool:
    annotation = _strip_optional(annotation)
    return isinstance(annotation, type) and issubclass(annotation, Hook)


def _callable_name(func: Callable[..., Any]) -> str:
    return getattr(func, "__name__", func.__class__.__name__)


def _extract_hook_dependency(
    annotation: Any,
) -> tuple[type[Hook], HookDependency] | None:
    if get_origin(annotation) is not Annotated:
        return None

    args = get_args(annotation)
    if not args:
        return None

    base_type = args[0]
    metadata = args[1:]

    hook_deps = [meta for meta in metadata if isinstance(meta, HookDependency)]
    if not hook_deps:
        return None
    if len(hook_deps) > 1:
        raise HookCompilationError(
            "A hook parameter can only have one hook.requires/awaits dependency marker."
        )

    if not (isinstance(base_type, type) and issubclass(base_type, Hook)):
        raise HookCompilationError(
            "Hook dependency parameters must be Annotated[YourHookType, "
            "hook.requires(...)]."
        )

    return base_type, hook_deps[0]


def is_hook_dependency_annotation(annotation: Any) -> bool:
    """Return True when annotation carries hook.requires/awaits metadata."""
    return _extract_hook_dependency(annotation) is not None


def _is_execution_injected_tool_param(name: str, annotation: Any) -> bool:
    if name in {"agent_ctx", "execution_ctx"}:
        return True
    if isinstance(annotation, type) and (
        issubclass(annotation, AgentContext) or issubclass(annotation, ExecutionContext)
    ):
        return True
    return False


def _is_request_context_param(name: str, annotation: Any) -> bool:
    if name == "ctx":
        return True
    annotation = _strip_optional(annotation)
    return annotation is HookRequestContext


def _unwrap_awaitable_return(annotation: Any) -> Any:
    current = annotation
    while True:
        origin = get_origin(current)
        if origin in {Awaitable}:
            args = get_args(current)
            if args:
                current = args[0]
                continue
        if str(origin) in {"collections.abc.Awaitable", "typing.Awaitable"}:
            args = get_args(current)
            if args:
                current = args[0]
                continue
        if origin in {Coroutine}:
            args = get_args(current)
            if args:
                current = args[-1]
                continue
        if str(origin) in {"typing.Coroutine", "collections.abc.Coroutine"}:
            args = get_args(current)
            if args:
                current = args[-1]
                continue
        break
    return current


def _validate_request_return_type(
    *,
    hook_param_name: str,
    expected_hook_type: type[Hook],
    request_builder: HookRequestBuilder,
    request_hints: dict[str, Any],
) -> None:
    request_name = _callable_name(request_builder)
    ret_ann = request_hints.get("return", inspect.Signature.empty)
    if ret_ann is inspect.Signature.empty:
        return

    inner_ret = _unwrap_awaitable_return(ret_ann)
    origin = get_origin(inner_ret)

    if inner_ret is PendingHook:
        return

    if origin is PendingHook:
        args = get_args(inner_ret)
        if not args:
            return
        declared_hook_type = _strip_optional(args[0])
        if isinstance(declared_hook_type, type) and issubclass(
            declared_hook_type, Hook
        ):
            if not issubclass(declared_hook_type, expected_hook_type):
                raise HookTypeMismatchError(
                    f"Request builder '{request_name}' for hook parameter "
                    f"'{hook_param_name}' must return "
                    f"PendingHook[{expected_hook_type.__name__}] "
                    f"but is annotated as PendingHook[{declared_hook_type.__name__}]."
                )
            return
        return

    raise HookTypeMismatchError(
        f"Request builder '{request_name}' for hook parameter "
        f"'{hook_param_name}' must return PendingHook[{expected_hook_type.__name__}] "
        f"(or Awaitable thereof)."
    )


def _validate_hook_reference_annotation(
    *,
    request_builder: HookRequestBuilder,
    request_param_name: str,
    request_param_annotation: Any,
    referenced_hook_param_name: str,
    referenced_hook_type: type[Hook],
) -> None:
    request_name = _callable_name(request_builder)
    ann = _strip_optional(request_param_annotation)
    if ann in (inspect.Parameter.empty, Any):
        return

    if not (isinstance(ann, type) and issubclass(ann, Hook)):
        raise HookTypeMismatchError(
            f"Request builder '{request_name}' parameter "
            f"'{request_param_name}' references hook parameter "
            f"'{referenced_hook_param_name}' but has incompatible annotation '{ann}'. "
            f"Expected a Hook payload type."
        )

    if not issubclass(referenced_hook_type, ann):
        raise HookTypeMismatchError(
            f"Request builder '{request_name}' parameter "
            f"'{request_param_name}' expects '{ann.__name__}', but referenced hook "
            f"'{referenced_hook_param_name}' is '{referenced_hook_type.__name__}'."
        )


def _build_stages(
    hook_order: tuple[HookParamName, ...], nodes: dict[HookParamName, HookNodeSpec]
) -> tuple[tuple[HookParamName, ...], ...]:
    in_degree: dict[HookParamName, int] = {}
    outgoing: dict[HookParamName, list[HookParamName]] = {
        name: [] for name in hook_order
    }

    for name, node in nodes.items():
        in_degree[name] = len(node.depends_on)
        for parent in node.depends_on:
            outgoing[parent].append(name)

    remaining = set(hook_order)
    stages: list[tuple[HookParamName, ...]] = []

    while remaining:
        ready = tuple(
            name for name in hook_order if name in remaining and in_degree[name] == 0
        )
        if not ready:
            cycle_nodes = ", ".join(name for name in hook_order if name in remaining)
            raise HookDependencyCycleError(
                f"Cyclic hook dependencies detected: {cycle_nodes}"
            )

        stages.append(ready)
        for name in ready:
            remaining.remove(name)
            for child in outgoing[name]:
                in_degree[child] -= 1

    return tuple(stages)


def compile_hook_plan(func: Callable[..., Any]) -> HookExecutionPlan | None:
    """Compile hook dependency metadata for a tool function.

    Returns None when no hook-annotated parameters are present.
    Raises HookCompilationError subclasses on validation failures.
    """

    sig = inspect.signature(func)
    type_hints = get_type_hints(func, include_extras=True)

    hook_order: list[HookParamName] = []
    tool_args: list[str] = []
    nodes: dict[HookParamName, HookNodeSpec] = {}

    for idx, (param_name, param) in enumerate(sig.parameters.items()):
        ann = type_hints.get(param_name, param.annotation)
        if _is_execution_injected_tool_param(param_name, ann):
            continue

        extracted = _extract_hook_dependency(ann)
        if extracted is None:
            tool_args.append(param_name)
            continue

        hook_type, dep = extracted
        hook_order.append(param_name)
        nodes[param_name] = HookNodeSpec(
            name=param_name,
            hook_type=hook_type,
            mode=dep.mode,
            request_builder=dep.request,
            depends_on=(),
            order_index=idx,
        )

    if not hook_order:
        return None

    tool_arg_names = set(tool_args)
    for hook_param_name in hook_order:
        node = nodes[hook_param_name]
        request_name = _callable_name(node.request_builder)
        request_sig = inspect.signature(node.request_builder)
        request_hints = get_type_hints(node.request_builder, include_extras=True)

        depends_on: list[HookParamName] = []
        for req_param_name, req_param in request_sig.parameters.items():
            req_ann = request_hints.get(req_param_name, req_param.annotation)
            if _is_request_context_param(req_param_name, req_ann):
                continue
            if req_param_name in tool_arg_names:
                continue
            if req_param_name in nodes:
                _validate_hook_reference_annotation(
                    request_builder=node.request_builder,
                    request_param_name=req_param_name,
                    request_param_annotation=req_ann,
                    referenced_hook_param_name=req_param_name,
                    referenced_hook_type=nodes[req_param_name].hook_type,
                )
                depends_on.append(req_param_name)
                continue

            if req_param.default is not inspect.Parameter.empty:
                continue

            if _is_hook_type(req_ann):
                raise HookDependencyResolutionError(
                    f"Request builder '{request_name}' for hook "
                    f"'{hook_param_name}' references hook payload parameter "
                    f"'{req_param_name}' by type only. Hook dependencies must "
                    f"reference "
                    f"other hook parameters by name from the tool signature."
                )

            raise HookDependencyResolutionError(
                f"Cannot resolve required parameter '{req_param_name}' in "
                f"request builder '{request_name}' for hook '{hook_param_name}'. "
                f"Supported injections are: ctx, tool args by name, "
                f"previously resolved "
                f"hook params by name, or optional parameters with defaults."
            )

        _validate_request_return_type(
            hook_param_name=hook_param_name,
            expected_hook_type=node.hook_type,
            request_builder=node.request_builder,
            request_hints=request_hints,
        )

        nodes[hook_param_name] = HookNodeSpec(
            name=node.name,
            hook_type=node.hook_type,
            mode=node.mode,
            request_builder=node.request_builder,
            depends_on=tuple(dict.fromkeys(depends_on)),
            order_index=node.order_index,
        )

    stages = _build_stages(tuple(hook_order), nodes)
    return HookExecutionPlan(
        tool_name=getattr(func, "__name__", "tool"),
        tool_args=tuple(tool_args),
        hook_order=tuple(hook_order),
        stages=stages,
        nodes=nodes,
    )

