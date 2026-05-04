from typing import Any, get_args, get_origin

from pydantic import TypeAdapter

from factorial.agent.context import AgentContext, EmptyMetadata, EmptyState


def resolve_state_and_metadata_types(agent: Any) -> tuple[Any, Any]:
    original = getattr(agent, "__orig_class__", None)
    if original is None:
        return EmptyState, EmptyMetadata

    original_args = get_args(original)
    if len(original_args) >= 2:
        return original_args[0], original_args[1]

    if len(original_args) == 1:
        first_arg = original_args[0]
        context_origin = get_origin(first_arg)
        context_args = get_args(first_arg)
        if context_origin is AgentContext:
            if len(context_args) >= 2:
                return context_args[0], context_args[1]
            if len(context_args) == 1:
                return context_args[0], EmptyMetadata
            return EmptyState, EmptyMetadata
        if isinstance(first_arg, type) and issubclass(first_arg, AgentContext):
            return EmptyState, EmptyMetadata
        return first_arg, EmptyMetadata

    return EmptyState, EmptyMetadata


def default_typed_payload(target_type: Any, *, label: str) -> Any:
    if target_type in (Any, object, None, EmptyState, EmptyMetadata):
        return EmptyState() if label == "state" else EmptyMetadata()
    if isinstance(target_type, type):
        try:
            return target_type()
        except Exception as exc:
            raise ValueError(
                f"{label} must be provided because {target_type!r} "
                "does not have a default constructor"
            ) from exc
    raise ValueError(f"{label} must be provided")


def coerce_typed_payload(
    value: Any,
    target_type: Any,
    *,
    label: str,
    agent_name: str,
) -> Any:
    if value is None:
        return default_typed_payload(target_type, label=label)
    if target_type in (Any, object, None):
        return value
    if isinstance(target_type, type) and isinstance(value, target_type):
        return value
    try:
        return TypeAdapter(target_type).validate_python(value)
    except Exception as exc:
        raise TypeError(f"Invalid {label} for agent {agent_name}: {exc}") from exc


__all__ = [
    "coerce_typed_payload",
    "default_typed_payload",
    "resolve_state_and_metadata_types",
]
