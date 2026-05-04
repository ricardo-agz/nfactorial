from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
from datetime import datetime, timezone
from typing import Annotated, Any, TypeVar, cast, get_args, get_origin, get_type_hints

from openai.types.chat import ChatCompletionMessageToolCall
from pydantic import BaseModel

from factorial._internal.agent.helpers import invoke_callable_non_blocking, retry
from factorial._internal.agent.tools.types import _ToolResultInternal
from factorial._internal.agent.types import ToolExecutionResults
from factorial._internal.execution.dependencies import inject_runtime_kwargs
from factorial._internal.serialization import serialize_data
from factorial.agent.context import AgentContext
from factorial.agent.tools.core import (
    serialize_for_client,
    serialize_for_model,
)
from factorial.agent.types import TurnCompletion
from factorial.ai.messages import Message, assistant, tool_result
from factorial.core.events import ToolFinishEvent, ToolStartEvent
from factorial.core.exceptions import FatalAgentError
from factorial.core.logging import get_logger
from factorial.execution.context import ExecutionContext
from factorial.execution.hooks import (
    HookRequestContext,
    HookSessionNode,
    HookSessionRecord,
    PendingHook,
    build_request_builder_kwargs,
)
from factorial.execution.waits import WaitInstruction

logger = get_logger(__name__)

ContextT = TypeVar("ContextT", bound=AgentContext[Any, Any])


def stringify_for_model(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    serialized = serialize_data(value)
    if isinstance(serialized, str):
        return serialized
    try:
        return json.dumps(serialized, ensure_ascii=False, sort_keys=True)
    except (TypeError, ValueError):
        return str(serialized)


def wait_model_output(wait_instr: WaitInstruction) -> str:
    if wait_instr.data is not None:
        if isinstance(wait_instr.data, BaseModel):
            return stringify_for_model(serialize_for_model(wait_instr.data))
        return stringify_for_model(wait_instr.data)
    if wait_instr.kind == "sleep":
        return f"Waiting for {wait_instr.sleep_s or 0}s"
    if wait_instr.kind == "cron":
        expr = wait_instr.cron or "<cron>"
        tz = wait_instr.timezone or "UTC"
        return f"Waiting for next cron tick '{expr}' ({tz})"
    if wait_instr.kind == "jobs":
        return "Waiting for spawned jobs"
    if wait_instr.kind == "activity":
        return "Waiting for activity"
    if wait_instr.kind == "signal":
        return "Waiting for signal"
    return "Waiting"


def normalize_tool_result(
    result: Any,
    tool_call: ChatCompletionMessageToolCall,
    pending_child_task_ids: list[str] | None = None,
) -> _ToolResultInternal:
    if isinstance(result, _ToolResultInternal):
        result.tool_call = tool_call
        if pending_child_task_ids:
            existing = result.pending_child_task_ids or []
            result.pending_child_task_ids = list(
                dict.fromkeys([*existing, *pending_child_task_ids])
            )
        return result

    if isinstance(result, WaitInstruction):
        return _ToolResultInternal(
            tool_call=tool_call,
            model_output=wait_model_output(result),
            client_output=result,
            pending_child_task_ids=pending_child_task_ids,
        )

    if isinstance(result, BaseModel):
        return _ToolResultInternal(
            tool_call=tool_call,
            model_output=stringify_for_model(serialize_for_model(result)),
            client_output=serialize_for_client(result),
            pending_child_task_ids=pending_child_task_ids,
        )

    if result is None:
        return _ToolResultInternal(
            tool_call=tool_call,
            model_output="",
            client_output=None,
            pending_child_task_ids=pending_child_task_ids,
        )

    return _ToolResultInternal(
        tool_call=tool_call,
        model_output=stringify_for_model(result),
        client_output=result,
        pending_child_task_ids=pending_child_task_ids,
    )


def _coerce_tool_argument_models(
    action: Any,
    parsed_tool_args: dict[str, Any],
) -> None:
    try:
        resolved_hints = get_type_hints(action, include_extras=True)
    except Exception:
        resolved_hints = {}

    for param_name, param in inspect.signature(action).parameters.items():
        if param_name not in parsed_tool_args:
            continue
        expected = resolved_hints.get(param_name, param.annotation)
        if expected is inspect.Parameter.empty:
            continue

        try:
            if get_origin(expected) is Annotated:
                annotated_args = get_args(expected)
                if annotated_args:
                    expected = annotated_args[0]
            origin = get_origin(expected)
            if (
                isinstance(expected, type)
                and issubclass(expected, BaseModel)
                and isinstance(parsed_tool_args[param_name], dict)
            ):
                parsed_tool_args[param_name] = expected(**parsed_tool_args[param_name])
            elif origin is list:
                item_type = get_args(expected)[0] if get_args(expected) else None
                if (
                    item_type
                    and isinstance(item_type, type)
                    and issubclass(item_type, BaseModel)
                    and isinstance(parsed_tool_args[param_name], list)
                ):
                    parsed_tool_args[param_name] = [
                        item_type(**item) if not isinstance(item, item_type) else item
                        for item in parsed_tool_args[param_name]
                    ]
        except Exception as exc:
            logger.debug(
                "Failed to coerce argument '%s' to %s: %s",
                param_name,
                expected,
                exc,
            )


async def _request_hook_dependencies(
    agent: Any,
    *,
    tool_name: str,
    tool_call: ChatCompletionMessageToolCall,
    hook_plan: Any,
    raw_tool_args: dict[str, Any],
    parsed_tool_args: dict[str, Any],
    execution_ctx: ExecutionContext,
) -> _ToolResultInternal | None:
    hook_param_names = list(hook_plan.hook_order)
    present_hook_params = [
        param_name for param_name in hook_param_names if param_name in parsed_tool_args
    ]
    if present_hook_params and len(present_hook_params) != len(hook_param_names):
        raise ValueError(
            f"Tool '{tool_name}' continuation received partial hook payloads: "
            f"{present_hook_params}. Expected all of {hook_param_names}."
        )

    if present_hook_params:
        return None

    request_tool_args = {
        key: parsed_tool_args[key]
        for key in raw_tool_args.keys()
        if key in parsed_tool_args
    }
    serialized_tool_args = cast(dict[str, Any], serialize_data(request_tool_args))
    now_ts = datetime.now(timezone.utc).timestamp()
    session_seed = f"{execution_ctx.task_id}:{tool_call.id}:{tool_name}"
    session_id = hashlib.sha256(session_seed.encode("utf-8")).hexdigest()
    session_nodes: dict[str, HookSessionNode] = {}
    for hook_param_name in hook_plan.hook_order:
        node_spec = hook_plan.nodes[hook_param_name]
        session_nodes[hook_param_name] = HookSessionNode(
            param_name=hook_param_name,
            mode=node_spec.mode,
            hook_type=node_spec.hook_type.__name__,
            depends_on=node_spec.depends_on,
        )

    first_stage = hook_plan.stages[0] if hook_plan.stages else ()
    if not first_stage:
        raise ValueError(f"Hook plan for tool '{tool_name}' has no requestable stage.")

    request_ctx = HookRequestContext(
        task_id=execution_ctx.task_id,
        owner_id=execution_ctx.owner_id,
        agent_name=agent.name,
        tool_name=tool_name,
        tool_call_id=tool_call.id,
        args=serialized_tool_args,
    )
    requested_hooks: list[dict[str, Any]] = []
    for hook_param_name in first_stage:
        node_spec = hook_plan.nodes[hook_param_name]
        request_kwargs = build_request_builder_kwargs(
            request_builder=node_spec.request_builder,
            request_ctx=request_ctx,
            tool_args=request_tool_args,
            resolved_hook_payloads={},
        )
        pending_hook = await invoke_callable_non_blocking(
            node_spec.request_builder,
            **request_kwargs,
        )
        if not isinstance(pending_hook, PendingHook):
            raise TypeError(
                f"Hook request builder for '{hook_param_name}' must return "
                "PendingHook[...]"
            )

        stable_hook_id = f"{session_id}:{hook_param_name}"
        pending_hook.hook_id = stable_hook_id

        node_state = session_nodes[hook_param_name]
        node_state.status = "requested"
        node_state.hook_id = stable_hook_id
        node_state.requested_at = now_ts

        requested_hooks.append(
            {
                "param_name": hook_param_name,
                "mode": node_spec.mode,
                "hook_type": node_spec.hook_type.__name__,
                "depends_on": list(node_spec.depends_on),
                "hook_id": stable_hook_id,
                "submit_url": pending_hook.submit_url,
                "token": pending_hook.token,
                "expires_at": pending_hook.expires_at.timestamp(),
                "title": pending_hook.title,
                "metadata": pending_hook.metadata,
            }
        )

    session = HookSessionRecord(
        session_id=session_id,
        task_id=execution_ctx.task_id,
        tool_call_id=tool_call.id,
        tool_name=tool_name,
        tool_args=serialized_tool_args,
        nodes=session_nodes,
        status="active",
        created_at=now_ts,
        updated_at=now_ts,
    )

    await execution_ctx.persist_hook_session(
        {
            "kind": "hook_session_init",
            "session": session.to_dict(),
            "requested_hooks": requested_hooks,
        }
    )
    return _ToolResultInternal(
        tool_call=tool_call,
        model_output="Awaiting hook dependency resolution",
        client_output={
            "kind": "hook_session_pending",
            "session_id": session_id,
            "requested_hooks": requested_hooks,
        },
        pending_result=True,
    )


def _parse_tool_arguments(tool_name: str, raw_args: str) -> dict[str, Any]:
    parsed = json.loads(raw_args)
    if not isinstance(parsed, dict):
        raise TypeError(
            f"Tool '{tool_name}' arguments must decode to a JSON object, "
            f"got {type(parsed).__name__}"
        )
    return parsed


async def tool_action(
    agent: Any,
    tool_call: ChatCompletionMessageToolCall,
    agent_ctx: ContextT,
) -> _ToolResultInternal:
    tool_name = tool_call.function.name
    action = agent.tool_actions.get(tool_name)
    tool_def = next((tool for tool in agent.tools if tool.name == tool_name), None)
    hook_plan = tool_def.hook_plan if tool_def else None
    execution_ctx = ExecutionContext.current()

    if action is None:
        raise ValueError(f"Agent {agent.name} has no tool action for {tool_name}")

    raw_tool_args = _parse_tool_arguments(tool_name, tool_call.function.arguments)
    parsed_tool_args = await inject_runtime_kwargs(
        func=action,
        existing_kwargs=raw_tool_args,
        agent_ctx=agent_ctx,
        execution_ctx=execution_ctx,
    )
    _coerce_tool_argument_models(action, parsed_tool_args)

    if hook_plan is not None:
        pending_hook_result = await _request_hook_dependencies(
            agent,
            tool_name=tool_name,
            tool_call=tool_call,
            hook_plan=hook_plan,
            raw_tool_args=raw_tool_args,
            parsed_tool_args=parsed_tool_args,
            execution_ctx=execution_ctx,
        )
        if pending_hook_result is not None:
            return pending_hook_result

    result = await invoke_callable_non_blocking(action, **parsed_tool_args)
    return normalize_tool_result(result, tool_call)


@retry(max_attempts=3, delay=0.5)
async def _tool_action_with_retry(
    agent: Any,
    tool_call: ChatCompletionMessageToolCall,
    agent_ctx: ContextT,
) -> _ToolResultInternal:
    return await tool_action(agent, tool_call, agent_ctx)


async def execute_tools(
    agent: Any,
    tool_calls: list[ChatCompletionMessageToolCall],
    agent_ctx: ContextT,
) -> ToolExecutionResults:
    new_messages: list[Message] = []
    pending_tool_call_ids: list[str] = []
    all_pending_child_task_ids: list[str] = []
    tool_call_results: list[
        tuple[ChatCompletionMessageToolCall, Any | BaseException]
    ] = []
    resolved_results: list[
        tuple[ChatCompletionMessageToolCall, _ToolResultInternal | BaseException]
    ] = []
    execution_ctx = ExecutionContext.current()

    for tool_call in tool_calls:
        await agent._emit_event(
            ToolStartEvent(
                task_id=execution_ctx.task_id,
                owner_id=execution_ctx.owner_id,
                agent_name=agent.name,
                turn=agent_ctx.turn_number,
                tool_name=tool_call.function.name,
                tool_call_id=tool_call.id,
            ),
            agent_ctx,
            execution_ctx,
        )

    results = await asyncio.gather(
        *[
            _tool_action_with_retry(agent, tool_call, agent_ctx)
            for tool_call in tool_calls
        ],
        return_exceptions=True,
    )

    for tool_call, result in zip(tool_calls, results, strict=True):
        resolved_results.append(
            (tool_call, cast(_ToolResultInternal | BaseException, result))
        )
        tool_call_results.append(
            (
                tool_call,
                result if isinstance(result, BaseException) else result.client_output,
            )
        )

        if isinstance(result, _ToolResultInternal) and result.pending_child_task_ids:
            all_pending_child_task_ids.extend(result.pending_child_task_ids)

        if isinstance(result, BaseException):
            logger.error(
                "Tool %s failed: %s",
                tool_call.function.name,
                result,
                exc_info=result,
            )
            new_messages.append(
                tool_result(
                    tool_call.id,
                    str(result),
                    tool_name=tool_call.function.name,
                    is_error=True,
                    model_output=str(result),
                )
            )
            await agent._emit_event(
                ToolFinishEvent(
                    task_id=execution_ctx.task_id,
                    owner_id=execution_ctx.owner_id,
                    agent_name=agent.name,
                    turn=agent_ctx.turn_number,
                    tool_name=tool_call.function.name,
                    tool_call_id=tool_call.id,
                    output=str(result),
                    is_error=True,
                ),
                agent_ctx,
                execution_ctx,
            )
        elif result.pending_result:
            pending_tool_call_ids.append(tool_call.id)
            await agent._emit_event(
                ToolFinishEvent(
                    task_id=execution_ctx.task_id,
                    owner_id=execution_ctx.owner_id,
                    agent_name=agent.name,
                    turn=agent_ctx.turn_number,
                    tool_name=tool_call.function.name,
                    tool_call_id=tool_call.id,
                    output=result.client_output,
                    is_error=False,
                ),
                agent_ctx,
                execution_ctx,
            )
        else:
            new_messages.append(
                tool_result(
                    tool_call.id,
                    result.client_output,
                    tool_name=tool_call.function.name,
                    is_error=False,
                    model_output=result.model_output,
                )
            )
            await agent._emit_event(
                ToolFinishEvent(
                    task_id=execution_ctx.task_id,
                    owner_id=execution_ctx.owner_id,
                    agent_name=agent.name,
                    turn=agent_ctx.turn_number,
                    tool_name=tool_call.function.name,
                    tool_call_id=tool_call.id,
                    output=result.client_output,
                    is_error=False,
                ),
                agent_ctx,
                execution_ctx,
            )

        if isinstance(result, FatalAgentError):
            raise result

    return ToolExecutionResults(
        new_messages=new_messages,
        tool_call_results=tool_call_results,
        resolved_results=resolved_results,
        pending_tool_call_ids=pending_tool_call_ids,
        pending_child_task_ids=all_pending_child_task_ids,
    )


def process_deferred_tool_results(
    agent_ctx: ContextT,
    tool_call_results: list[tuple[str, Any | BaseException]],
) -> TurnCompletion[ContextT]:
    updated_messages = list(agent_ctx.messages)
    for tool_call_id, result in tool_call_results:
        if isinstance(result, BaseException):
            updated_messages.append(
                tool_result(
                    tool_call_id,
                    str(result),
                    is_error=True,
                    model_output=str(result),
                )
            )
        else:
            updated_messages.append(
                tool_result(
                    tool_call_id,
                    result,
                    model_output=stringify_for_model(result),
                )
            )
    agent_ctx.messages = updated_messages
    return TurnCompletion(is_done=False, context=agent_ctx)


def format_child_task_result(
    child_task_id: str,
    result: Any | BaseException,
) -> str:
    if isinstance(result, BaseException):
        return (
            f'<sub_task_error sub_task_id="{child_task_id}">\n'
            f"Error running sub task:\n{result}\n</sub_task_error>"
        )
    return (
        f'<sub_task_result sub_task_id="{child_task_id}">\n'
        f"{str(result)}\n</sub_task_result>"
    )


def process_child_task_results(
    agent_ctx: ContextT,
    child_task_results: list[tuple[str, Any | BaseException]],
) -> TurnCompletion[ContextT]:
    updated_messages = list(agent_ctx.messages)
    formatted_results = [
        format_child_task_result(child_task_id, result)
        for child_task_id, result in child_task_results
    ]
    if formatted_results:
        updated_messages.append(
            assistant(
                "<sub_task_results>\n"
                + "\n\n".join(formatted_results)
                + "\n</sub_task_results>"
            )
        )
    agent_ctx.messages = updated_messages
    return TurnCompletion(is_done=False, context=agent_ctx)
