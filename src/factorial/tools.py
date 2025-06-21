from typing import (
    Callable,
    Any,
    Awaitable,
    TypeVar,
    Union,
    Generic,
    Annotated,
    get_type_hints,
    get_origin,
    get_args,
    overload,
)
from dataclasses import dataclass
from pydantic import BaseModel, ConfigDict, Field
from functools import wraps
from openai.types.chat import ChatCompletionMessageToolCall
import inspect
import asyncio
from enum import Enum  # local import to avoid unnecessary global import

from factorial.context import AgentContext, ExecutionContext


ContextT = TypeVar("ContextT", bound=AgentContext)
FunctionToolActionReturn = Union[Any, "FunctionToolActionResult", tuple[str, Any]]
FunctionToolAction = Union[
    Callable[..., FunctionToolActionReturn],
    Callable[..., Awaitable[FunctionToolActionReturn]],
]
F = Callable[..., Any]
T = TypeVar("T")


class FunctionToolActionResult(BaseModel):
    # Allow pydantic to accept Exception and other arbitrary types
    model_config = ConfigDict(arbitrary_types_allowed=True)

    output_str: str
    output_data: Any
    error: Exception | None = None
    tool_call: ChatCompletionMessageToolCall | None = None
    pending_result: bool = False
    pending_child_task_ids: list[str] = []


@dataclass
class FunctionTool(Generic[ContextT]):
    """A tool that wraps a function."""

    name: str
    """The name of the tool, as shown to the LLM."""

    description: str
    """A description of the tool, as shown to the LLM."""

    params_json_schema: dict[str, Any]
    """The JSON schema for the tool's parameters."""

    on_invoke_tool: FunctionToolAction
    """The function that implements the tool."""

    strict_json_schema: bool = True
    """Whether the JSON schema is in strict mode."""

    is_enabled: bool | Callable[[ContextT], bool] = True
    """Whether the tool is enabled."""

    def to_openai_tool_schema(self) -> dict[str, Any]:
        """Convert this FunctionTool to OpenAI tool schema format."""
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
    """Convert a Python type (including Annotated and Pydantic types) to JSON schema object."""

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
            python_type = str  # type: ignore[assignment]
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
                elif hasattr(meta, "description") and getattr(meta, "description"):
                    base_schema["description"] = getattr(meta, "description")

                # Enum values (if provided via Field(..., enum=[...]))
                if hasattr(meta, "enum") and getattr(meta, "enum") is not None:
                    base_schema["enum"] = list(getattr(meta, "enum"))  # type: ignore[arg-type]

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
        enum_values = [member.value for member in python_type]  # type: ignore[arg-type]

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
        return python_type.model_json_schema()

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

    properties: dict[str, Any] = {}
    required: list[str] = []
    has_optional_param = False

    for param_name, param in sig.parameters.items():
        # Skip 'agent_ctx' and 'execution_ctx' parameters as they're injected by the agent
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


@overload
def function_tool(
    func: F,
) -> FunctionTool[Any]: ...


@overload
def function_tool(
    func: None = None,
    *,
    name: str | None = None,
    description: str | None = None,
    strict_json_schema: bool = True,
    is_enabled: bool | Callable[[ContextT], bool] = True,
) -> Callable[[F], FunctionTool[Any]]: ...


def function_tool(
    func: F | None = None,
    *,
    name: str | None = None,
    description: str | None = None,
    strict_json_schema: bool = True,
    is_enabled: bool | Callable[[ContextT], bool] = True,
) -> FunctionTool[ContextT] | Callable[[F], FunctionTool[ContextT]]:
    """Convert a Python function to a FunctionTool.

    Can be used as a decorator with or without arguments:

    @function_tool
    def my_tool(param: str) -> str:
        '''Tool description'''
        return f"Result: {param}"

    @function_tool(name="custom_name", description="Custom description")
    def my_tool(param: str) -> str:
        return f"Result: {param}"
    """

    def _create_function_tool(the_func: F) -> FunctionTool[ContextT]:
        tool_name = name or the_func.__name__

        # Extract description from docstring if not provided
        if description is None:
            if the_func.__doc__:
                tool_description = the_func.__doc__.strip().split("\n")[0]
            else:
                tool_description = f"Execute {the_func.__name__}"
        else:
            tool_description = description

        params_schema, has_optional = _function_to_json_schema(the_func)

        # ------------------------------------------------------------------
        # Strict-mode logic: if *any* parameter is optional the function **cannot**
        # be in strict mode because strict mode mandates all properties are
        # required.
        # ------------------------------------------------------------------
        effective_strict_json_schema = strict_json_schema and not has_optional

        return FunctionTool(
            name=tool_name,
            description=tool_description,
            params_json_schema=params_schema,
            on_invoke_tool=the_func,
            strict_json_schema=effective_strict_json_schema,
            is_enabled=is_enabled,
        )

    # If func is provided, we were used as @function_tool (no parentheses)
    if func is not None:
        return _create_function_tool(func)

    # Otherwise, we were used as @function_tool(...), so return a decorator
    return _create_function_tool


def convert_tools_list(
    tools: list[Union[FunctionTool[ContextT], F]],
) -> tuple[list[FunctionTool[ContextT]], dict[str, FunctionToolAction]]:
    """Convert a mixed list of FunctionTool instances and Python functions to OpenAI tool schemas and tool actions."""
    tool_schemas: list[FunctionTool[ContextT]] = []
    tool_actions: dict[str, FunctionToolAction] = {}

    for tool in tools:
        if isinstance(tool, FunctionTool):
            function_tool_instance = tool
        else:
            # Convert Python function to FunctionTool
            function_tool_instance = function_tool(tool)

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


def deferred_result(
    timeout: float,
) -> Callable[
    [Callable[..., T] | Callable[..., Awaitable[T]]], Callable[..., Awaitable[T]]
]:
    def decorator(
        func: Callable[..., T] | Callable[..., Awaitable[T]],
    ) -> Callable[..., Awaitable[T]]:
        """Wrap the tool function so it can be awaited regardless of being sync or async.

        The orchestrator always awaits the result of the tool wrapper.  If the wrapped
        function is synchronous we simply call it and return the value; if it is a
        coroutine function we *await* it.  This prevents the "NoneType can't be used in
        'await' expression" error when developers create synchronous deferred tools.
        """

        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> T:
            if asyncio.iscoroutinefunction(func):
                # Async tool — await normally
                return await func(*args, **kwargs)  # type: ignore[arg-type]
            # Sync tool — run directly
            return func(*args, **kwargs)  # type: ignore[return-value,arg-type]

        wrapper.deferred_result = True  # type: ignore[attr-defined]
        wrapper.timeout = timeout  # type: ignore[attr-defined]
        return wrapper

    return decorator


def forking_tool(
    timeout: float,
) -> Callable[
    [Callable[..., T] | Callable[..., Awaitable[T]]], Callable[..., Awaitable[T]]
]:
    def decorator(
        func: Callable[..., T] | Callable[..., Awaitable[T]],
    ) -> Callable[..., Awaitable[T]]:
        """Wrap the tool function so it can be awaited regardless of being sync or async.

        The orchestrator always awaits the result of the tool wrapper.  If the wrapped
        function is synchronous we simply call it and return the value; if it is a
        coroutine function we *await* it.  This prevents the "NoneType can't be used in
        'await' expression" error when developers create synchronous deferred tools.
        """

        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> T:
            if asyncio.iscoroutinefunction(func):
                # Async tool — await normally
                return await func(*args, **kwargs)  # type: ignore[arg-type]
            # Sync tool — run directly
            return func(*args, **kwargs)  # type: ignore[return-value,arg-type]

        wrapper.forking_tool = True  # type: ignore[attr-defined]
        wrapper.timeout = timeout  # type: ignore[attr-defined]
        return wrapper

    return decorator
