from __future__ import annotations

import inspect
from collections.abc import Awaitable
from typing import cast, overload

from typing_extensions import TypeVar

T = TypeVar("T")


@overload
async def resolve_awaitable(value: Awaitable[T]) -> T: ...


@overload
async def resolve_awaitable(value: T) -> T: ...


async def resolve_awaitable(value: T | Awaitable[T]) -> T:
    if inspect.isawaitable(value):
        return await cast(Awaitable[T], value)
    return cast(T, value)


__all__ = ["resolve_awaitable"]
