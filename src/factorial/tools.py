from typing import (
    Callable,
    Any,
    Awaitable,
    TypeVar,
    Union,
    Generic,
    get_type_hints,
    get_origin,
    get_args,
    overload,
)
from dataclasses import dataclass
from pydantic import BaseModel
from openai.types.chat import ChatCompletionMessageToolCall
import inspect

from factorial.context import AgentContext, ExecutionContext


ContextT = TypeVar("ContextT", bound=AgentContext)
FunctionToolActionReturn = Union[Any, "FunctionToolActionResult", tuple[str, Any]]
FunctionToolAction = Union[
    Callable[..., FunctionToolActionReturn],
    Callable[..., Awaitable[FunctionToolActionReturn]],
]
F = Callable[..., Any]


class FunctionToolActionResult(BaseModel):
    output_str: str
    output_data: Any
    tool_call: ChatCompletionMessageToolCall | None = None
    pending_result: bool = False


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
            },
            "strict": self.strict_json_schema,
        }


def _python_type_to_json_schema(python_type: type) -> dict[str, Any]:
    """Convert a Python type to JSON schema object."""
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


def _function_to_json_schema(func: F) -> dict[str, Any]:
    """Convert a Python function to JSON schema for its parameters."""
    sig = inspect.signature(func)
    type_hints = get_type_hints(func)

    properties: dict[str, Any] = {}
    required: list[str] = []

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

        # Check if parameter is required (no default value and not Optional)
        if param.default is inspect.Parameter.empty:
            origin = get_origin(param_type)
            args = get_args(param_type)
            is_optional = origin is Union and type(None) in args

            if not is_optional:
                required.append(param_name)

    schema: dict[str, Any] = {
        "type": "object",
        "properties": properties,
        "additionalProperties": False,
    }

    if required:
        schema["required"] = required

    return schema


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

        params_schema = _function_to_json_schema(the_func)

        return FunctionTool(
            name=tool_name,
            description=tool_description,
            params_json_schema=params_schema,
            on_invoke_tool=the_func,
            strict_json_schema=strict_json_schema,
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
    return {
        "type": "function",
        "function": {
            "name": "final_output",
            "description": "Complete the task and return the final output to the user",
            "parameters": output_type.model_json_schema(),
        },
    }
