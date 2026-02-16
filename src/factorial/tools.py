import asyncio
import inspect
import warnings
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from enum import Enum  # local import to avoid unnecessary global import
from functools import wraps
from typing import (
    Any,
    Generic,
    Literal,
    TypeVar,
    Union,
    get_args,
    get_origin,
    get_type_hints,
    overload,
)

from openai.types.chat import ChatCompletionMessageToolCall
from pydantic import BaseModel, ConfigDict

from factorial.context import AgentContext, ExecutionContext
from factorial.hooks import (
    HookExecutionPlan,
    compile_hook_plan,
    is_hook_dependency_annotation,
)

ContextT = TypeVar("ContextT", bound=AgentContext)
F = Callable[..., Any]
T = TypeVar("T")


class ToolResult(BaseModel):
    """Structured tool outcome for the namespace-style API (`tool.ok/fail/error`)."""

    # Allow pydantic to accept Exception and other arbitrary types
    model_config = ConfigDict(arbitrary_types_allowed=True)

    output_str: str
    output_data: Any
    error: Exception | None = None
    tool_call: ChatCompletionMessageToolCall | None = None
    pending_result: bool = False
    pending_child_task_ids: list[str] = []
    status: Literal["ok", "fail", "error"] = "ok"


# Backward-compatible alias. Prefer `ToolResult` in new code.
FunctionToolActionResult = ToolResult

ToolActionReturn = Any | ToolResult | tuple[str, Any]
ToolAction = Callable[..., ToolActionReturn] | Callable[
    ..., Awaitable[ToolActionReturn]
]

# Backward-compatible type aliases. Prefer `ToolAction*` in new code.
FunctionToolActionReturn = ToolActionReturn
FunctionToolAction = ToolAction


@dataclass
class ToolDefinition(Generic[ContextT]):
    """Canonical tool definition used by `@tool`."""

    name: str
    """The name of the tool, as shown to the LLM."""

    description: str
    """A description of the tool, as shown to the LLM."""

    params_json_schema: dict[str, Any]
    """The JSON schema for the tool's parameters."""

    on_invoke_tool: ToolAction
    """The function that implements the tool."""

    strict_json_schema: bool = True
    """Whether the JSON schema is in strict mode."""

    is_enabled: bool | Callable[[ContextT], bool] = True
    """Whether the tool is enabled."""

    hook_plan: HookExecutionPlan | None = None
    """Compiled hook dependency metadata, if any."""

    def to_openai_tool_schema(self) -> dict[str, Any]:
        """Convert this tool definition to OpenAI tool schema format."""
        return {
            "type": "function",
            "function": {
                "name": self.name,
                "description": self.description,
                "parameters": self.params_json_schema,
                "strict": self.strict_json_schema,
            },
        }


# Backward-compatible alias. Prefer `ToolDefinition` in new code.
FunctionTool = ToolDefinition


def _python_type_to_json_schema(python_type: type) -> dict[str, Any]:
    """Convert a Python type (including Annotated/Pydantic) to JSON schema."""

    # ------------------------------------------------------------------
    # Handle ``typing.Annotated`` so we can capture metadata such as
    # descriptions coming from ``pydantic.Field`` or simple strings.
    # ------------------------------------------------------------------
    if hasattr(python_type, "__metadata__"):
        # Annotated types have their metadata stored in ``__metadata__`` and
        # the underlying type as the first argument of ``get_args``.
        annotated_args = get_args(python_type)
        if not annotated_args:
            # Fallback – treat as plain string
            python_type = str
        else:
            base_type = annotated_args[0]
            metadata = annotated_args[1:]

            # Recursively build the base schema first
            base_schema = _python_type_to_json_schema(base_type)

            # Merge in metadata – we only look for ``description`` and ``enum``
            # for now, but this can be extended easily.
            for meta in metadata:
                # Case 1: Simple string is treated as a description
                if isinstance(meta, str):
                    base_schema["description"] = meta
                # Case 2: Pydantic Field or FieldInfo-like objects
                elif hasattr(meta, "description") and meta.description:
                    base_schema["description"] = meta.description

                # Enum values (if provided via Field(..., enum=[...]))
                if hasattr(meta, "enum") and meta.enum is not None:
                    base_schema["enum"] = list(meta.enum)

            return base_schema

    origin = get_origin(python_type)

    # Handle Union types (including Optional)
    if origin is Union:
        args = get_args(python_type)
        non_none_types = [arg for arg in args if arg is not type(None)]
        if non_none_types:
            return _python_type_to_json_schema(non_none_types[0])

    # Handle List types
    if origin is list:
        args = get_args(python_type)
        if args:
            # Get the item type from the list type annotation
            item_type = args[0]
            return {"type": "array", "items": _python_type_to_json_schema(item_type)}
        else:
            # Fallback for untyped lists
            return {"type": "array", "items": {"type": "string"}}

    # Handle Dict types
    if origin is dict:
        return {"type": "object"}

    # Handle Python ``enum.Enum`` subclasses. We map them to a JSON schema with
    # an ``enum`` list containing all member *values* and set the type based on
    # the value type (string vs integer).
    if isinstance(python_type, type) and issubclass(python_type, Enum):
        enum_values = [member.value for member in python_type]

        # Determine JSON type – if **all** enum values are ints → integer, else → string
        json_type = (
            "integer" if all(isinstance(v, int) for v in enum_values) else "string"
        )

        return {
            "type": json_type,
            "enum": enum_values,
            "description": getattr(python_type, "__doc__", None) or None,
        }

    # Handle Pydantic BaseModel classes
    if isinstance(python_type, type) and issubclass(python_type, BaseModel):
        schema = python_type.model_json_schema()
        # Ensure additionalProperties is false for all object schemas
        if schema.get("type") == "object":
            schema["additionalProperties"] = False
        return schema

    # Handle basic types
    if python_type is str:
        return {"type": "string"}
    elif python_type is int:
        return {"type": "integer"}
    elif python_type is float:
        return {"type": "number"}
    elif python_type is bool:
        return {"type": "boolean"}
    else:
        return {"type": "string"}  # Default fallback


def _function_to_json_schema(func: F) -> tuple[dict[str, Any], bool]:
    """Convert a Python function to JSON schema for its parameters.

    Returns a tuple of (schema, has_optional). ``has_optional`` is **True**
    if the function has *any* parameter that is optional (either via a
    default value **or** via ``Optional`` / ``Union[NoneType]``). This flag
    is later used to decide whether the tool can be in strict mode.
    """
    sig = inspect.signature(func)
    type_hints = get_type_hints(func)
    type_hints_with_extras = get_type_hints(func, include_extras=True)

    properties: dict[str, Any] = {}
    required: list[str] = []
    has_optional_param = False

    for param_name, param in sig.parameters.items():
        full_annotation = type_hints_with_extras.get(param_name, param.annotation)
        if is_hook_dependency_annotation(full_annotation):
            # Hook payload params are runtime-injected after hook resolution.
            continue

        # Skip 'agent_ctx' and 'execution_ctx' - they're injected by the agent
        if param_name == "agent_ctx" or (
            param.annotation != inspect.Parameter.empty
            and isinstance(param.annotation, type)
            and issubclass(param.annotation, AgentContext)
        ):
            continue
        if param_name == "execution_ctx" or (
            param.annotation != inspect.Parameter.empty
            and isinstance(param.annotation, type)
            and issubclass(param.annotation, ExecutionContext)
        ):
            continue

        param_type = type_hints.get(param_name, str)
        json_schema = _python_type_to_json_schema(param_type)

        properties[param_name] = json_schema

        origin = get_origin(param_type)
        args = get_args(param_type)
        is_optional_annotation = origin is Union and type(None) in args

        # A parameter is required **only** when all of the following are true:
        #   1. No default value provided, *and*
        #   2. The type annotation is **not** Optional/Union[..., None]
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


@overload
def function_tool(
    func: F,
    *,
    name: str | None = None,
    description: str | None = None,
    strict_json_schema: bool = True,
    is_enabled: bool | Callable[[ContextT], bool] = True,
) -> ToolDefinition[Any]: ...


@overload
def function_tool(
    func: None = None,
    *,
    name: str | None = None,
    description: str | None = None,
    strict_json_schema: bool = True,
    is_enabled: bool | Callable[[ContextT], bool] = True,
) -> Callable[[F], ToolDefinition[Any]]: ...


def function_tool(
    func: F | None = None,
    *,
    name: str | None = None,
    description: str | None = None,
    strict_json_schema: bool = True,
    is_enabled: bool | Callable[[ContextT], bool] = True,
) -> ToolDefinition[Any] | Callable[[F], ToolDefinition[Any]]:
    """Legacy alias for `@tool` / `tool(...)`.

    Prefer `@tool` in new code. This alias is kept for backward compatibility.
    """
    warnings.warn(
        "`function_tool` is deprecated. Prefer `tool` / `@tool`.",
        DeprecationWarning,
        stacklevel=2,
    )
    return _tool_factory(
        func,
        name=name,
        description=description,
        strict_json_schema=strict_json_schema,
        is_enabled=is_enabled,
    )


class ToolNamespace:
    """Namespace-style tool API used by the redesign (`tool.*`)."""

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

    def ok(self, message: str = "", data: Any = None) -> ToolResult:
        return ToolResult(status="ok", output_str=message, output_data=data)

    def fail(self, message: str, data: Any = None) -> ToolResult:
        return ToolResult(status="fail", output_str=message, output_data=data)

    def error(self, message: str, data: Any = None) -> ToolResult:
        return ToolResult(status="error", output_str=message, output_data=data)


tool = ToolNamespace()


def convert_tools_list(
    tools: list[ToolDefinition[ContextT] | F],
) -> tuple[list[ToolDefinition[ContextT]], dict[str, ToolAction]]:
    """Convert mixed list of tool definitions and callables to schemas."""
    tool_schemas: list[ToolDefinition[ContextT]] = []
    tool_actions: dict[str, ToolAction] = {}

    for tool_like in tools:
        if isinstance(tool_like, ToolDefinition):
            function_tool_instance = tool_like
        else:
            # Convert Python function via the primary interface.
            function_tool_instance = tool(tool_like)

        tool_schemas.append(function_tool_instance)
        tool_actions[function_tool_instance.name] = (
            function_tool_instance.on_invoke_tool
        )

    return tool_schemas, tool_actions


def create_final_output_tool(output_type: type[BaseModel]) -> dict[str, Any]:
    tool = {
        "type": "function",
        "function": {
            "name": "final_output",
            "description": "Complete the task and return the final output to the user",
            "parameters": output_type.model_json_schema(),
        },
    }
    return tool


def forking_tool(
    timeout: float,
) -> Callable[
    [Callable[..., T] | Callable[..., Awaitable[T]]], Callable[..., Awaitable[T]]
]:
    def decorator(
        func: Callable[..., T] | Callable[..., Awaitable[T]],
    ) -> Callable[..., Awaitable[T]]:
        """Wrap tool function so it can be awaited regardless of sync/async."""

        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> T:
            if asyncio.iscoroutinefunction(func):
                # Async tool — await normally
                return await func(*args, **kwargs)
            # Sync tool — run directly
            return func(*args, **kwargs)  # type: ignore[return-value,arg-type,no-any-return]

        wrapper.forking_tool = True  # type: ignore[attr-defined]
        wrapper.timeout = timeout  # type: ignore[attr-defined]
        return wrapper

    return decorator
