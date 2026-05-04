from __future__ import annotations

import importlib
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any, Literal, Protocol, runtime_checkable

from ..core import LiveResourceRef, ResourceCheckpoint, ResourceContext, ResourceRequest
from .base import Sandbox

SandboxProviderSource = Literal["default", "explicit"]
SandboxProviderLoader = Callable[[], "SandboxProvider"]

SANDBOX_PROVIDER_ALIAS_METADATA_KEY = "sandbox_provider"
SANDBOX_PROVIDER_SOURCE_METADATA_KEY = "sandbox_provider_source"
SANDBOX_PROVIDER_SOURCE_DEFAULT: SandboxProviderSource = "default"
SANDBOX_PROVIDER_SOURCE_EXPLICIT: SandboxProviderSource = "explicit"


@runtime_checkable
class SandboxProvider(Protocol):
    async def create(
        self,
        ctx: ResourceContext,
        request: ResourceRequest[Sandbox],
    ) -> Sandbox: ...

    async def restore(
        self,
        checkpoint: ResourceCheckpoint,
        ctx: ResourceContext,
        request: ResourceRequest[Sandbox],
    ) -> Sandbox: ...

    async def checkpoint(
        self,
        resource: Sandbox,
        ctx: ResourceContext,
        request: ResourceRequest[Sandbox],
    ) -> ResourceCheckpoint | None: ...

    async def destroy(
        self,
        resource: Sandbox,
        ctx: ResourceContext,
        request: ResourceRequest[Sandbox],
    ) -> None: ...

    async def attach_live(
        self,
        live_ref: LiveResourceRef,
        ctx: ResourceContext,
        request: ResourceRequest[Sandbox],
    ) -> Sandbox | None: ...

    def capture_live_ref(
        self,
        resource: Sandbox,
        ctx: ResourceContext,
        request: ResourceRequest[Sandbox],
    ) -> LiveResourceRef | None: ...

    async def delete_checkpoint(
        self,
        checkpoint: ResourceCheckpoint,
        ctx: ResourceContext,
        request: ResourceRequest[Sandbox],
    ) -> None: ...


class SandboxProviderRegistryError(RuntimeError):
    """Base class for sandbox provider registry errors."""


class UnknownSandboxProviderError(SandboxProviderRegistryError):
    """Raised when a sandbox provider alias is unknown."""


class SandboxProviderNotConfiguredError(SandboxProviderRegistryError):
    """Raised when no unambiguous sandbox provider can be resolved."""


class SandboxProviderLoadError(SandboxProviderRegistryError):
    """Raised when a registered sandbox provider cannot be loaded."""


@dataclass
class _SandboxProviderEntry:
    alias: str
    provider: SandboxProvider | None = None
    loader: SandboxProviderLoader | str | None = None
    extra_name: str | None = None


class SandboxProviderRegistry:
    def __init__(self) -> None:
        self._entries: dict[str, _SandboxProviderEntry] = {}

    def register(
        self,
        provider: SandboxProvider,
        *,
        alias: str,
        allow_replace: bool = False,
    ) -> None:
        self._register(
            _SandboxProviderEntry(alias=alias, provider=provider),
            allow_replace=allow_replace,
        )

    def register_lazy(
        self,
        *,
        alias: str,
        loader: SandboxProviderLoader | str,
        extra_name: str | None = None,
        allow_replace: bool = False,
    ) -> None:
        self._register(
            _SandboxProviderEntry(
                alias=alias,
                loader=loader,
                extra_name=extra_name,
            ),
            allow_replace=allow_replace,
        )

    def _register(
        self,
        entry: _SandboxProviderEntry,
        *,
        allow_replace: bool,
    ) -> None:
        existing = self._entries.get(entry.alias)
        if existing is not None and not allow_replace:
            raise SandboxProviderRegistryError(
                f"Sandbox provider alias {entry.alias!r} is already registered."
            )
        self._entries[entry.alias] = entry

    def has(self, alias: str) -> bool:
        return alias in self._entries

    def aliases(self) -> list[str]:
        return sorted(self._entries)

    def get(self, alias: str) -> SandboxProvider:
        entry = self._entries.get(alias)
        if entry is None:
            raise UnknownSandboxProviderError(
                f"Unknown sandbox provider alias {alias!r}."
            )
        if entry.provider is None:
            entry.provider = self._load_entry_provider(entry)
        return entry.provider

    def resolve(self, alias: str | None) -> tuple[str, SandboxProvider]:
        if alias is None:
            aliases = self.aliases()
            if len(aliases) == 1:
                alias = aliases[0]
            elif not aliases:
                raise SandboxProviderNotConfiguredError(
                    "No sandbox providers are registered."
                )
            else:
                names = ", ".join(sorted(aliases))
                raise SandboxProviderNotConfiguredError(
                    "Sandbox provider is ambiguous. Configure an agent default "
                    f"or pass provider=... explicitly. Registered providers: {names}."
                )
        return alias, self.get(alias)

    def _load_entry_provider(self, entry: _SandboxProviderEntry) -> SandboxProvider:
        loader = entry.loader
        if loader is None:
            raise SandboxProviderLoadError(
                f"Sandbox provider alias {entry.alias!r} has no loader."
            )
        try:
            if isinstance(loader, str):
                module_name, _, attr_name = loader.partition(":")
                if not module_name or not attr_name:
                    raise ValueError(
                        "Sandbox provider loader paths must be 'module:attribute'."
                    )
                module = importlib.import_module(module_name)
                target = getattr(module, attr_name)
                provider = target() if callable(target) else target
            else:
                provider = loader()
        except ImportError as exc:
            if entry.extra_name:
                raise SandboxProviderLoadError(
                    f"Sandbox provider {entry.alias!r} requires the optional extra "
                    f"`nfactorial[{entry.extra_name}]`."
                ) from exc
            raise SandboxProviderLoadError(
                f"Failed to import sandbox provider {entry.alias!r}."
            ) from exc
        except Exception as exc:
            raise SandboxProviderLoadError(
                f"Failed to load sandbox provider {entry.alias!r}: {exc}"
            ) from exc
        if not isinstance(provider, SandboxProvider):
            raise SandboxProviderLoadError(
                f"Loaded sandbox provider {entry.alias!r} does not implement the "
                "SandboxProvider protocol."
            )
        return provider


_REGISTRY = SandboxProviderRegistry()


def register_sandbox_provider(
    provider: SandboxProvider,
    *,
    alias: str,
    allow_replace: bool = False,
) -> None:
    _REGISTRY.register(provider, alias=alias, allow_replace=allow_replace)


def register_lazy_sandbox_provider(
    *,
    alias: str,
    loader: SandboxProviderLoader | str,
    extra_name: str | None = None,
    allow_replace: bool = False,
) -> None:
    _REGISTRY.register_lazy(
        alias=alias,
        loader=loader,
        extra_name=extra_name,
        allow_replace=allow_replace,
    )


def get_sandbox_provider(alias: str) -> SandboxProvider:
    return _REGISTRY.get(alias)


def resolve_sandbox_provider(
    alias: str | None,
) -> tuple[str, SandboxProvider]:
    return _REGISTRY.resolve(alias)


def registered_sandbox_provider_aliases() -> list[str]:
    return _REGISTRY.aliases()


def make_sandbox_request_metadata(
    provider: str,
    *,
    explicit: bool,
) -> dict[str, str]:
    return {
        SANDBOX_PROVIDER_ALIAS_METADATA_KEY: provider,
        SANDBOX_PROVIDER_SOURCE_METADATA_KEY: (
            SANDBOX_PROVIDER_SOURCE_EXPLICIT
            if explicit
            else SANDBOX_PROVIDER_SOURCE_DEFAULT
        ),
    }


def sandbox_requested_provider_alias(
    metadata: Mapping[str, Any],
) -> str | None:
    raw = metadata.get(SANDBOX_PROVIDER_ALIAS_METADATA_KEY)
    return raw if isinstance(raw, str) and raw else None


def sandbox_requested_provider_source(
    metadata: Mapping[str, Any],
) -> SandboxProviderSource | None:
    raw = metadata.get(SANDBOX_PROVIDER_SOURCE_METADATA_KEY)
    if raw == SANDBOX_PROVIDER_SOURCE_DEFAULT:
        return SANDBOX_PROVIDER_SOURCE_DEFAULT
    if raw == SANDBOX_PROVIDER_SOURCE_EXPLICIT:
        return SANDBOX_PROVIDER_SOURCE_EXPLICIT
    return None


register_lazy_sandbox_provider(
    alias="vercel",
    loader="factorial.resources.sandbox.vercel:get_provider",
    extra_name="vercel",
)


__all__ = [
    "SANDBOX_PROVIDER_ALIAS_METADATA_KEY",
    "SANDBOX_PROVIDER_SOURCE_DEFAULT",
    "SANDBOX_PROVIDER_SOURCE_EXPLICIT",
    "SANDBOX_PROVIDER_SOURCE_METADATA_KEY",
    "SandboxProvider",
    "SandboxProviderLoadError",
    "SandboxProviderNotConfiguredError",
    "SandboxProviderRegistry",
    "SandboxProviderRegistryError",
    "SandboxProviderSource",
    "UnknownSandboxProviderError",
    "get_sandbox_provider",
    "make_sandbox_request_metadata",
    "register_lazy_sandbox_provider",
    "register_sandbox_provider",
    "registered_sandbox_provider_aliases",
    "resolve_sandbox_provider",
    "sandbox_requested_provider_alias",
    "sandbox_requested_provider_source",
]
