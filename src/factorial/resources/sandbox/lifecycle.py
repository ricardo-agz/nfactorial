from __future__ import annotations

from ..core import (
    CheckpointCleanupResourceLifecycle,
    LiveResourceRef,
    ResourceCheckpoint,
    ResourceContext,
    ResourceRequest,
    register_resource_lifecycle,
)
from .base import Sandbox
from .providers import (
    SANDBOX_PROVIDER_SOURCE_EXPLICIT,
    SandboxProvider,
    resolve_sandbox_provider,
    sandbox_requested_provider_alias,
    sandbox_requested_provider_source,
)


def _bound_sandbox_provider_alias(
    request: ResourceRequest[Sandbox],
    *,
    live_ref: LiveResourceRef | None = None,
    checkpoint: ResourceCheckpoint | None = None,
) -> str | None:
    if live_ref is not None and live_ref.provider:
        return live_ref.provider
    if checkpoint is not None and checkpoint.provider:
        return checkpoint.provider
    return sandbox_requested_provider_alias(request.binding_metadata)


def _resolve_provider(
    request: ResourceRequest[Sandbox],
    *,
    live_ref: LiveResourceRef | None = None,
    checkpoint: ResourceCheckpoint | None = None,
) -> tuple[str, SandboxProvider]:
    requested_alias = sandbox_requested_provider_alias(request.metadata)
    requested_source = sandbox_requested_provider_source(request.metadata)
    bound_alias = _bound_sandbox_provider_alias(
        request,
        live_ref=live_ref,
        checkpoint=checkpoint,
    )
    if (
        requested_source == SANDBOX_PROVIDER_SOURCE_EXPLICIT
        and requested_alias is not None
        and bound_alias is not None
        and requested_alias != bound_alias
    ):
        raise RuntimeError(
            "Sandbox provider conflict: requested "
            f"{requested_alias!r}, but sandbox is already bound to {bound_alias!r}."
        )
    if bound_alias is not None:
        return resolve_sandbox_provider(bound_alias)
    if requested_alias is not None:
        return resolve_sandbox_provider(requested_alias)
    return resolve_sandbox_provider(None)


def _checkpoint_with_provider_alias(
    checkpoint: ResourceCheckpoint,
    *,
    provider_alias: str,
) -> ResourceCheckpoint:
    if checkpoint.provider == provider_alias:
        return checkpoint
    return ResourceCheckpoint(
        provider=provider_alias,
        kind=checkpoint.kind,
        ref=checkpoint.ref,
        metadata=dict(checkpoint.metadata),
    )


def _live_ref_with_provider_alias(
    live_ref: LiveResourceRef,
    *,
    provider_alias: str,
) -> LiveResourceRef:
    if live_ref.provider == provider_alias:
        return live_ref
    return LiveResourceRef(
        provider=provider_alias,
        kind=live_ref.kind,
        ref=live_ref.ref,
        metadata=dict(live_ref.metadata),
    )


class SandboxLifecycle(CheckpointCleanupResourceLifecycle[Sandbox]):
    @classmethod
    async def create(
        cls,
        ctx: ResourceContext,
        request: ResourceRequest[Sandbox],
    ) -> Sandbox:
        del cls
        _, provider = _resolve_provider(request)
        return await provider.create(ctx, request)

    @classmethod
    async def restore(
        cls,
        checkpoint: ResourceCheckpoint,
        ctx: ResourceContext,
        request: ResourceRequest[Sandbox],
    ) -> Sandbox:
        del cls
        _, provider = _resolve_provider(request, checkpoint=checkpoint)
        return await provider.restore(checkpoint, ctx, request)

    @classmethod
    async def checkpoint(
        cls,
        resource: Sandbox,
        ctx: ResourceContext,
        request: ResourceRequest[Sandbox],
    ) -> ResourceCheckpoint | None:
        del cls
        provider_alias, provider = _resolve_provider(request)
        checkpoint = await provider.checkpoint(resource, ctx, request)
        if checkpoint is None:
            return None
        return _checkpoint_with_provider_alias(
            checkpoint,
            provider_alias=provider_alias,
        )

    @classmethod
    async def destroy(
        cls,
        resource: Sandbox,
        ctx: ResourceContext,
        request: ResourceRequest[Sandbox],
    ) -> None:
        del cls
        _, provider = _resolve_provider(request)
        await provider.destroy(resource, ctx, request)

    @classmethod
    async def attach_live(
        cls,
        live_ref: LiveResourceRef,
        ctx: ResourceContext,
        request: ResourceRequest[Sandbox],
    ) -> Sandbox | None:
        del cls
        _, provider = _resolve_provider(request, live_ref=live_ref)
        return await provider.attach_live(live_ref, ctx, request)

    @classmethod
    def capture_live_ref(
        cls,
        resource: Sandbox,
        ctx: ResourceContext,
        request: ResourceRequest[Sandbox],
    ) -> LiveResourceRef | None:
        del cls
        provider_alias, provider = _resolve_provider(request)
        live_ref = provider.capture_live_ref(resource, ctx, request)
        if live_ref is None:
            return None
        return _live_ref_with_provider_alias(live_ref, provider_alias=provider_alias)

    @classmethod
    async def delete_checkpoint(
        cls,
        checkpoint: ResourceCheckpoint,
        ctx: ResourceContext,
        request: ResourceRequest[Sandbox],
    ) -> None:
        del cls
        _, provider = _resolve_provider(request, checkpoint=checkpoint)
        await provider.delete_checkpoint(checkpoint, ctx, request)


register_resource_lifecycle(Sandbox, SandboxLifecycle)


__all__ = ["SandboxLifecycle"]
