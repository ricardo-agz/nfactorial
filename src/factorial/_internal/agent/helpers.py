from __future__ import annotations

import asyncio
import inspect
import random
from collections.abc import Awaitable, Callable
from functools import wraps
from typing import Any, cast, overload

from typing_extensions import TypeVar

from factorial._internal.execution.dependencies import (
    inject_runtime_kwargs,
    is_runtime_injected_annotation,
)
from factorial.agent.context import AgentContext
from factorial.agent.types import PrepareTurnHook, Turn
from factorial.core.events import BaseEvent
from factorial.core.exceptions import RETRYABLE_EXCEPTIONS, FatalAgentError
from factorial.core.run_types import VerificationSummary
from factorial.execution.context import ExecutionContext

T = TypeVar("T")


async def invoke_callable_non_blocking(
    fn: Callable[..., Any],
    *args: Any,
    **kwargs: Any,
) -> Any:
    if asyncio.iscoroutinefunction(fn):
        async_fn = cast(Callable[..., Awaitable[Any]], fn)
        return await async_fn(*args, **kwargs)
    return await asyncio.to_thread(fn, *args, **kwargs)


@overload
def retry(
    func: Callable[..., Awaitable[T]],
) -> Callable[..., Awaitable[T]]: ...


@overload
def retry(
    func: None = None,
    *,
    max_attempts: int = 3,
    delay: float = 1.0,
    max_delay: float = 60.0,
    exponential_base: float = 2.0,
    jitter: bool = True,
) -> Callable[[Callable[..., Awaitable[T]]], Callable[..., Awaitable[T]]]: ...


def retry(
    func: Callable[..., Awaitable[T]] | None = None,
    *,
    max_attempts: int = 3,
    delay: float = 1.0,
    max_delay: float = 60.0,
    exponential_base: float = 2.0,
    jitter: bool = True,
) -> (
    Callable[..., Awaitable[T]]
    | Callable[[Callable[..., Awaitable[T]]], Callable[..., Awaitable[T]]]
):
    def _create_retry_decorator(
        the_func: Callable[..., Awaitable[T]],
    ) -> Callable[..., Awaitable[T]]:
        sig = inspect.signature(the_func)

        ctx_param_name: str | None = None
        ctx_pos: int | None = None
        for idx, param in enumerate(sig.parameters.values()):
            ann = param.annotation
            if (
                ann is not inspect.Parameter.empty
                and isinstance(ann, type)
                and issubclass(ann, AgentContext)
            ):
                ctx_param_name = param.name
                ctx_pos = idx - 1
                break

        if ctx_param_name is None and "agent_ctx" in sig.parameters:
            ctx_param_name = "agent_ctx"
            ctx_pos = list(sig.parameters).index("agent_ctx") - 1

        @wraps(the_func)
        async def wrapper(self: Any, *args: Any, **kwargs: Any) -> T:
            if max_attempts <= 0:
                raise ValueError("max_attempts must be greater than 0")

            agent_ctx_obj: Any | None = None
            if ctx_param_name is not None:
                if ctx_param_name in kwargs:
                    agent_ctx_obj = kwargs[ctx_param_name]
                elif ctx_pos is not None and ctx_pos < len(args):
                    agent_ctx_obj = args[ctx_pos]

            last_exception: Exception | None = None
            for attempt_index in range(max_attempts):
                if isinstance(agent_ctx_obj, AgentContext):
                    agent_ctx_obj.attempt_number = attempt_index + 1

                try:
                    return await the_func(self, *args, **kwargs)
                except Exception as exc:
                    if isinstance(exc, RETRYABLE_EXCEPTIONS):
                        last_exception = exc
                        if attempt_index < max_attempts - 1:
                            backoff_delay = min(
                                delay * (exponential_base**attempt_index),
                                max_delay,
                            )
                            if jitter:
                                backoff_delay *= random.uniform(0.5, 1.5)
                            await asyncio.sleep(backoff_delay)
                            continue
                    raise

            raise last_exception or RuntimeError("Retry failed unexpectedly")

        return wrapper

    if func is not None:
        return _create_retry_decorator(func)
    return _create_retry_decorator


async def _maybe_call_prepare_turn(
    func: PrepareTurnHook | None,
    turn: Turn[Any],
    agent_ctx: AgentContext[Any, Any],
    execution_ctx: ExecutionContext,
) -> Turn[Any]:
    if func is None:
        return turn
    signature = inspect.signature(func)
    params = list(signature.parameters.values())
    if not params:
        raise TypeError("prepare_turn must accept at least one argument for turn")

    first = params[0]
    if first.kind not in (
        inspect.Parameter.POSITIONAL_ONLY,
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
    ):
        raise TypeError("prepare_turn must accept the turn as its first parameter")

    injected_kwargs = await inject_runtime_kwargs(
        func=func,
        existing_kwargs={},
        agent_ctx=agent_ctx,
        execution_ctx=execution_ctx,
        start_at=1,
    )
    args: list[Any] = [turn]
    kwargs: dict[str, Any] = {}
    for param in params[1:]:
        if param.kind in (
            inspect.Parameter.VAR_POSITIONAL,
            inspect.Parameter.VAR_KEYWORD,
        ):
            continue

        if param.name in injected_kwargs:
            injected_value = injected_kwargs[param.name]
            if param.kind in (
                inspect.Parameter.POSITIONAL_ONLY,
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
            ):
                args.append(injected_value)
            else:
                kwargs[param.name] = injected_value
            continue

        if param.default is not inspect.Parameter.empty:
            continue

        annotation = param.annotation
        if is_runtime_injected_annotation(param.name, annotation):
            raise RuntimeError(
                f"Failed to resolve prepare_turn parameter '{param.name}'."
            )

        raise TypeError(
            f"Unsupported required prepare_turn parameter '{param.name}'. "
            "Only turn (first arg), agent_ctx, execution_ctx, and other "
            "runtime-injected dependencies are supported."
        )

    result = func(*args, **kwargs)
    if inspect.isawaitable(result):
        result = await cast(Awaitable[Any], result)
    if isinstance(result, Turn):
        return result
    return turn


def chain_prepare_turn(
    *functions: PrepareTurnHook,
) -> PrepareTurnHook:
    async def _chained(
        turn: Turn[Any],
        agent_ctx: AgentContext[Any, Any],
        execution_ctx: ExecutionContext,
    ) -> Turn[Any]:
        current = turn
        for function in functions:
            current = await _maybe_call_prepare_turn(
                function,
                current,
                agent_ctx,
                execution_ctx,
            )
        return current

    return _chained


class _DirectEventPublisher:
    def __init__(self, sink: Callable[[BaseEvent], Awaitable[None]] | None = None):
        self._sink = sink

    async def publish_event(self, event: BaseEvent) -> None:
        if self._sink is not None:
            await self._sink(event)


class _RunFailureError(FatalAgentError):
    def __init__(
        self,
        message: str,
        *,
        verification_summary: VerificationSummary[Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.verification_summary = verification_summary


__all__ = [
    "chain_prepare_turn",
    "retry",
]
