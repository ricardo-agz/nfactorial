from __future__ import annotations

import inspect
from typing import Annotated, Any, get_args, get_origin

from factorial.agent.context import AgentContext
from factorial.execution.resources import Resources, Sandboxes, resources, sandboxes
from factorial.resources.core import has_resource_lifecycle
from factorial.resources.sandbox.base import Sandbox

from .context import ExecutionContext


def unwrap_runtime_annotation(annotation: Any) -> Any:
    if get_origin(annotation) is Annotated:
        annotated_args = get_args(annotation)
        if annotated_args:
            return unwrap_runtime_annotation(annotated_args[0])

    origin = get_origin(annotation)
    if origin in {None, list, dict, tuple, set}:
        return annotation

    args = [arg for arg in get_args(annotation) if arg is not type(None)]
    if len(args) == 1:
        return unwrap_runtime_annotation(args[0])
    return annotation


def _is_subclass(annotation: Any, target: type[Any]) -> bool:
    return isinstance(annotation, type) and issubclass(annotation, target)


def is_agent_context_annotation(annotation: Any) -> bool:
    return _is_subclass(unwrap_runtime_annotation(annotation), AgentContext)


def is_execution_context_annotation(annotation: Any) -> bool:
    return _is_subclass(unwrap_runtime_annotation(annotation), ExecutionContext)


def is_runtime_injected_annotation(name: str, annotation: Any) -> bool:
    normalized = unwrap_runtime_annotation(annotation)
    if name in {"agent_ctx", "execution_ctx"}:
        return True
    if is_agent_context_annotation(normalized):
        return True
    if is_execution_context_annotation(normalized):
        return True
    if normalized in {Resources, Sandboxes, Sandbox}:
        return True
    return isinstance(normalized, type) and has_resource_lifecycle(normalized)


async def resolve_runtime_injected_value(
    *,
    name: str,
    annotation: Any,
    agent_ctx: AgentContext[Any, Any],
    execution_ctx: ExecutionContext,
) -> tuple[bool, Any]:
    normalized = unwrap_runtime_annotation(annotation)

    if name == "agent_ctx" or is_agent_context_annotation(normalized):
        return True, agent_ctx
    if name == "execution_ctx" or is_execution_context_annotation(normalized):
        return True, execution_ctx
    if normalized is Resources:
        return True, resources
    if normalized is Sandboxes:
        return True, sandboxes
    if normalized is Sandbox:
        return True, await execution_ctx.resources.get_sandbox("default")
    if isinstance(normalized, type) and has_resource_lifecycle(normalized):
        return True, await execution_ctx.resources.get_resource(normalized, "default")

    return False, None


async def inject_runtime_kwargs(
    *,
    func: Any,
    existing_kwargs: dict[str, Any],
    agent_ctx: AgentContext[Any, Any],
    execution_ctx: ExecutionContext,
    start_at: int = 0,
) -> dict[str, Any]:
    try:
        from typing import get_type_hints

        resolved_hints = get_type_hints(func, include_extras=True)
    except Exception:
        resolved_hints = {}

    updated = dict(existing_kwargs)
    params = list(inspect.signature(func).parameters.values())
    for param in params[start_at:]:
        if param.name in updated:
            continue
        if param.kind in (
            inspect.Parameter.VAR_POSITIONAL,
            inspect.Parameter.VAR_KEYWORD,
        ):
            continue

        annotation = resolved_hints.get(param.name, param.annotation)
        is_injected, value = await resolve_runtime_injected_value(
            name=param.name,
            annotation=annotation,
            agent_ctx=agent_ctx,
            execution_ctx=execution_ctx,
        )
        if is_injected:
            updated[param.name] = value
    return updated


__all__ = [
    "inject_runtime_kwargs",
    "is_agent_context_annotation",
    "is_execution_context_annotation",
    "is_runtime_injected_annotation",
    "resolve_runtime_injected_value",
    "unwrap_runtime_annotation",
]
