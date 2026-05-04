from __future__ import annotations

from typing import TypeVar

from factorial.execution.context import ExecutionContext
from factorial.resources import (
    ResourceCheckpoint,
    ResourcesExecutionNamespace,
    ResourceType,
)
from factorial.resources.sandbox.base import Sandbox, SandboxCheckpoint

R = TypeVar("R")


def _resource_namespace() -> ResourcesExecutionNamespace:
    return ExecutionContext.current().resources


class Resources:
    async def get(self, resource_type: ResourceType[R], name: str = "default") -> R:
        return await _resource_namespace().get_resource(resource_type, name)


class Sandboxes:
    async def get(
        self,
        name: str = "default",
        *,
        provider: str | None = None,
    ) -> Sandbox:
        return await _resource_namespace().get_sandbox(name, provider=provider)

    async def checkpoint(self, name: str = "default") -> SandboxCheckpoint | None:
        checkpoint = await _resource_namespace().checkpoint_resource(Sandbox, name)
        if checkpoint is None:
            return None
        return SandboxCheckpoint.from_resource_checkpoint(checkpoint)

    async def destroy(self, name: str = "default") -> None:
        await _resource_namespace().destroy_resource(Sandbox, name)


resources = Resources()
sandboxes = Sandboxes()


__all__ = [
    "Resources",
    "Sandboxes",
    "resources",
    "sandboxes",
    "ResourceCheckpoint",
]
