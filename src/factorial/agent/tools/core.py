import inspect
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from enum import Enum
from typing import (
    Any,
    Generic,
    TypeVar,
    Union,
    get_args,
    get_origin,
    get_type_hints,
    overload,
)

from pydantic import BaseModel

from factorial._internal.execution.dependencies import is_runtime_injected_annotation
from factorial.agent.context import AgentContext
from factorial.execution.hooks import (
    HookExecutionPlan,
    compile_hook_plan,
    is_hook_dependency_annotation,
)

ContextT = TypeVar("ContextT", bound=AgentContext)
F = Callable[..., Any]


class _HiddenType:
    """Sentinel for ``Annotated[T, Hidden]``.

    Fields annotated with ``Hidden`` are excluded from the model context
    but included in client/event payloads.

    Usage::

        from factorial import Hidden
        from typing import Annotated
        from pydantic import BaseModel

        class EditResult(BaseModel):
            summary: str
            new_code: Annotated[str, Hidden]
    """

    def __repr__(self) -> str:
        return "Hidden"


Hidden = _HiddenType()
"""Sentinel to exclude a BaseModel field from the model context.

Usage::

    new_code: Annotated[str, Hidden]
"""


def has_hidden_annotation(annotation: Any) -> bool:
    """Return True if *annotation* is ``Annotated[T, Hidden]``."""
    if get_origin(annotation) is not None:
        for arg in get_args(annotation):
            if isinstance(arg, _HiddenType):
                return True
    return False


def serialize_for_model(result: BaseModel) -> dict[str, Any]:
    """Serialize a BaseModel for the model context, excluding Hidden fields."""
    try:
        hints = get_type_hints(type(result), include_extras=True)
    except Exception:
        return result.model_dump()

    visible_fields = {
        name for name, hint in hints.items() if not has_hidden_annotation(hint)
    }
    return {
        name: value
        for name, value in result.model_dump().items()
        if name in visible_fields
    }


def serialize_for_client(result: BaseModel) -> dict[str, Any]:
    """Serialize a BaseModel for client/event payloads (all fields)."""
    return result.model_dump()


ToolAction = Callable[..., Any] | Callable[..., Awaitable[Any]]


@dataclass
class ToolDefinition(Generic[ContextT]):
    """Canonical tool definition used by ``@tool``."""

    name: str
    description: str
    params_json_schema: dict[str, Any]
    on_invoke_tool: ToolAction
    strict_json_schema: bool = True
    is_enabled: bool | Callable[[ContextT], bool] = True
    hook_plan: HookExecutionPlan | None = None

    def to_openai_tool_schema(self) -> dict[str, Any]:
        return {
            "type": "function",
            "function": {
                "name": self.name,
                "description": self.description,
                "parameters": self.params_json_schema,
                "strict": self.strict_json_schema,
            },
        }


def _python_type_to_json_schema(python_type: type) -> dict[str, Any]:
    """Convert a Python type (including Annotated/Pydantic) to JSON schema."""

    if hasattr(python_type, "__metadata__"):
        annotated_args = get_args(python_type)
        if not annotated_args:
            python_type = str
        else:
            base_type = annotated_args[0]
            metadata = annotated_args[1:]

            base_schema = _python_type_to_json_schema(base_type)

            for meta in metadata:
                if isinstance(meta, str):
                    base_schema["description"] = meta
                elif hasattr(meta, "description") and meta.description:
                    base_schema["description"] = meta.description

                if hasattr(meta, "enum") and meta.enum is not None:
                    base_schema["enum"] = list(meta.enum)

            return base_schema

    origin = get_origin(python_type)

    if origin is Union:
        args = get_args(python_type)
        non_none_types = [arg for arg in args if arg is not type(None)]
        if non_none_types:
            return _python_type_to_json_schema(non_none_types[0])

    if origin is list:
        args = get_args(python_type)
        if args:
            item_type = args[0]
            return {"type": "array", "items": _python_type_to_json_schema(item_type)}
        return {"type": "array", "items": {"type": "string"}}

    if origin is dict:
        return {"type": "object"}

    if isinstance(python_type, type) and issubclass(python_type, Enum):
        enum_values = [member.value for member in python_type]
        json_type = (
            "integer" if all(isinstance(v, int) for v in enum_values) else "string"
        )
        return {
            "type": json_type,
            "enum": enum_values,
            "description": getattr(python_type, "__doc__", None) or None,
        }

    if isinstance(python_type, type) and issubclass(python_type, BaseModel):
        schema = python_type.model_json_schema()
        if schema.get("type") == "object":
            schema["additionalProperties"] = False
        return schema

    if python_type is str:
        return {"type": "string"}
    if python_type is int:
        return {"type": "integer"}
    if python_type is float:
        return {"type": "number"}
    if python_type is bool:
        return {"type": "boolean"}
    return {"type": "string"}


def _function_to_json_schema(func: F) -> tuple[dict[str, Any], bool]:
    """Convert a Python function to JSON schema for its parameters."""

    sig = inspect.signature(func)
    type_hints = get_type_hints(func)
    type_hints_with_extras = get_type_hints(func, include_extras=True)

    properties: dict[str, Any] = {}
    required: list[str] = []
    has_optional_param = False

    for param_name, param in sig.parameters.items():
        full_annotation = type_hints_with_extras.get(param_name, param.annotation)
        if is_hook_dependency_annotation(full_annotation):
            continue

        if is_runtime_injected_annotation(param_name, full_annotation):
            continue

        param_type = type_hints.get(param_name, str)
        properties[param_name] = _python_type_to_json_schema(param_type)

        origin = get_origin(param_type)
        args = get_args(param_type)
        is_optional_annotation = origin is Union and type(None) in args

        if param.default is inspect.Parameter.empty and not is_optional_annotation:
            required.append(param_name)
        else:
            has_optional_param = True

    schema: dict[str, Any] = {
        "type": "object",
        "properties": properties,
        "additionalProperties": False,
    }
    if required:
        schema["required"] = required
    return schema, has_optional_param


def _tool_factory(
    func: F | None = None,
    *,
    name: str | None = None,
    description: str | None = None,
    strict_json_schema: bool = True,
    is_enabled: bool | Callable[[ContextT], bool] = True,
) -> ToolDefinition[Any] | Callable[[F], ToolDefinition[Any]]:
    """Create a tool decorator or convert a callable to a tool definition."""

    def _create(the_func: F) -> ToolDefinition[Any]:
        func_name = getattr(the_func, "__name__", "tool")
        tool_name = name or func_name

        if description is None:
            if the_func.__doc__:
                tool_description = the_func.__doc__.strip().split("\n")[0]
            else:
                tool_description = f"Execute {func_name}"
        else:
            tool_description = description

        params_schema, has_optional = _function_to_json_schema(the_func)
        effective_strict_json_schema = strict_json_schema and not has_optional
        hook_plan = compile_hook_plan(the_func)

        return ToolDefinition(
            name=tool_name,
            description=tool_description,
            params_json_schema=params_schema,
            on_invoke_tool=the_func,
            strict_json_schema=effective_strict_json_schema,
            is_enabled=is_enabled,
            hook_plan=hook_plan,
        )

    if func is not None:
        return _create(func)
    return _create


class _ToolDecorator:
    """The ``tool`` decorator."""

    @overload
    def __call__(self, func: F) -> ToolDefinition[Any]: ...

    @overload
    def __call__(
        self,
        func: None = None,
        *,
        name: str | None = None,
        description: str | None = None,
        strict_json_schema: bool = True,
        is_enabled: bool | Callable[[ContextT], bool] = True,
    ) -> Callable[[F], ToolDefinition[Any]]: ...

    def __call__(
        self,
        func: F | None = None,
        *,
        name: str | None = None,
        description: str | None = None,
        strict_json_schema: bool = True,
        is_enabled: bool | Callable[[ContextT], bool] = True,
    ) -> ToolDefinition[Any] | Callable[[F], ToolDefinition[Any]]:
        return _tool_factory(
            func,
            name=name,
            description=description,
            strict_json_schema=strict_json_schema,
            is_enabled=is_enabled,
        )


tool = _ToolDecorator()


def convert_tools_list(
    tools: Sequence[ToolDefinition[ContextT] | F],
) -> tuple[list[ToolDefinition[ContextT]], dict[str, ToolAction]]:
    """Convert mixed list of tool definitions and callables to schemas."""
    tool_schemas: list[ToolDefinition[ContextT]] = []
    tool_actions: dict[str, ToolAction] = {}

    for tool_like in tools:
        tool_instance = (
            tool_like if isinstance(tool_like, ToolDefinition) else tool(tool_like)
        )
        tool_schemas.append(tool_instance)
        tool_actions[tool_instance.name] = tool_instance.on_invoke_tool

    return tool_schemas, tool_actions
