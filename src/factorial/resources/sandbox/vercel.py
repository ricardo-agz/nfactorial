from __future__ import annotations

import asyncio
import importlib
import os
from contextlib import suppress
from dataclasses import dataclass
from typing import Literal, Protocol, TypedDict, TypeGuard

from ..core import (
    LiveResourceRef,
    ResourceCheckpoint,
    ResourceContext,
    ResourceRequest,
)
from .base import (
    Sandbox,
    SandboxCheckpoint,
    SandboxExecResult,
    SandboxProcess,
    SandboxWriteFile,
    encode_sandbox_file_content,
)
from .providers import SandboxProvider

_DEFAULT_EXPOSED_PORTS = [
    3000,
    3001,
    4173,
    5173,
    8000,
    8001,
    8080,
    8081,
    8888,
    9000,
]


class _VercelSnapshotSource(TypedDict):
    type: Literal["snapshot"]
    snapshot_id: str


class _VercelWriteFile(TypedDict):
    path: str
    content: bytes


class _VercelSnapshotLike(Protocol):
    @property
    def snapshot_id(self) -> str: ...

    @property
    def source_sandbox_id(self) -> str: ...

    @property
    def expires_at(self) -> int: ...

    async def delete(self) -> None: ...


class _VercelCommandResultLike(Protocol):
    @property
    def cmd_id(self) -> str: ...

    @property
    def exit_code(self) -> int: ...

    async def stdout(self) -> str: ...

    async def stderr(self) -> str: ...


class _VercelCommandLike(_VercelCommandResultLike, Protocol):
    async def wait(self) -> _VercelCommandResultLike: ...

    async def output(self, stream: str = "both") -> str: ...

    async def kill(self, signal: int = 15) -> None: ...


class _VercelAsyncSandboxLike(Protocol):
    @property
    def sandbox_id(self) -> str: ...

    @property
    def status(self) -> str: ...

    async def wait_for_status(
        self,
        status: str,
        *,
        timeout: float = 30.0,
        poll_interval: float = 0.5,
    ) -> None: ...

    async def run_command(
        self,
        cmd: str,
        args: list[str] | None = None,
        *,
        cwd: str | None = None,
        env: dict[str, str] | None = None,
        sudo: bool = False,
    ) -> _VercelCommandResultLike: ...

    async def run_command_detached(
        self,
        cmd: str,
        args: list[str] | None = None,
        *,
        cwd: str | None = None,
        env: dict[str, str] | None = None,
        sudo: bool = False,
    ) -> _VercelCommandLike: ...

    async def mk_dir(self, path: str, *, cwd: str | None = None) -> None: ...

    async def read_file(self, path: str, *, cwd: str | None = None) -> bytes | None: ...

    async def write_files(self, files: list[_VercelWriteFile]) -> None: ...

    def domain(self, port: int) -> str: ...

    async def snapshot(self) -> _VercelSnapshotLike: ...

    async def stop(self) -> None: ...


class _VercelAsyncSandboxFactory(Protocol):
    @staticmethod
    async def create(
        *,
        source: _VercelSnapshotSource | None = None,
        ports: list[int] | None = None,
        timeout: int | None = None,
        resources: dict[str, object] | None = None,
        runtime: str | None = None,
        token: str | None = None,
        project_id: str | None = None,
        team_id: str | None = None,
        interactive: bool = False,
        env: dict[str, str] | None = None,
        network_policy: object | None = None,
    ) -> _VercelAsyncSandboxLike: ...

    @staticmethod
    async def get(
        *,
        sandbox_id: str,
        token: str | None = None,
        project_id: str | None = None,
        team_id: str | None = None,
    ) -> _VercelAsyncSandboxLike: ...


class _VercelAsyncSnapshotFactory(Protocol):
    @staticmethod
    async def get(
        *,
        snapshot_id: str,
        token: str | None = None,
        project_id: str | None = None,
        team_id: str | None = None,
    ) -> _VercelSnapshotLike: ...


def _configured_ports() -> list[int]:
    raw = os.getenv("NFACTORIAL_SANDBOX_PORTS")
    if raw is None or not raw.strip():
        return list(_DEFAULT_EXPOSED_PORTS)

    parsed: list[int] = []
    for part in raw.split(","):
        stripped = part.strip()
        if not stripped:
            continue
        port = int(stripped)
        if port <= 0:
            raise ValueError("NFACTORIAL_SANDBOX_PORTS must contain positive integers")
        if port not in parsed:
            parsed.append(port)
    return parsed or list(_DEFAULT_EXPOSED_PORTS)


def _configured_timeout_ms() -> int:
    raw = os.getenv("NFACTORIAL_SANDBOX_TIMEOUT_MS")
    if raw is None or not raw.strip():
        return 300_000
    return int(raw)


def _configured_ready_timeout_s() -> float:
    raw = os.getenv("NFACTORIAL_SANDBOX_READY_TIMEOUT_S")
    if raw is None or not raw.strip():
        return 30.0
    return float(raw)


def _configured_runtime() -> str | None:
    raw = os.getenv("NFACTORIAL_SANDBOX_RUNTIME")
    if raw is None or not raw.strip():
        return None
    return raw.strip()


def _is_vercel_async_sandbox_factory(
    value: object,
) -> TypeGuard[_VercelAsyncSandboxFactory]:
    create = getattr(value, "create", None)
    get = getattr(value, "get", None)
    return callable(create) and callable(get)


def _load_vercel_async_sandbox() -> _VercelAsyncSandboxFactory:
    try:
        module = importlib.import_module("vercel.sandbox")
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError(
            "Sandbox support requires the `vercel` package. "
            'Install it with `pip install "nfactorial[vercel]"`.'
        ) from exc
    async_sandbox: object = getattr(module, "AsyncSandbox", None)
    if not _is_vercel_async_sandbox_factory(async_sandbox):
        raise RuntimeError(
            "Installed `vercel` package does not expose `vercel.sandbox.AsyncSandbox`."
        )
    return async_sandbox


def _is_vercel_async_snapshot_factory(
    value: object,
) -> TypeGuard[_VercelAsyncSnapshotFactory]:
    get = getattr(value, "get", None)
    return callable(get)


def _load_vercel_async_snapshot() -> _VercelAsyncSnapshotFactory:
    try:
        module = importlib.import_module("vercel.sandbox.snapshot")
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError(
            "Sandbox checkpoint cleanup requires the `vercel` package. "
            'Install it with `pip install "nfactorial[vercel]"`.'
        ) from exc
    async_snapshot: object = getattr(module, "AsyncSnapshot", None)
    if not _is_vercel_async_snapshot_factory(async_snapshot):
        raise RuntimeError(
            "Installed `vercel` package does not expose "
            "`vercel.sandbox.snapshot.AsyncSnapshot`."
        )
    return async_snapshot


def _is_not_found_error(exc: Exception) -> bool:
    status_code = getattr(exc, "status_code", None)
    if status_code is None:
        response = getattr(exc, "response", None)
        status_code = getattr(response, "status_code", None)
    if status_code is None:
        return False
    try:
        return int(status_code) == 404
    except (TypeError, ValueError):
        return False


async def _ensure_running(native_sandbox: _VercelAsyncSandboxLike) -> None:
    if native_sandbox.status == "running":
        return
    await native_sandbox.wait_for_status(
        "running",
        timeout=_configured_ready_timeout_s(),
    )


async def _command_to_result(command: _VercelCommandResultLike) -> SandboxExecResult:
    return SandboxExecResult(
        command_id=command.cmd_id,
        exit_code=command.exit_code,
        stdout_text=await command.stdout(),
        stderr_text=await command.stderr(),
    )


@dataclass
class VercelSandboxProcess(SandboxProcess):
    command: _VercelCommandLike

    @property
    def id(self) -> str:
        return str(self.command.cmd_id)

    async def wait(self) -> SandboxExecResult:
        finished = await self.command.wait()
        return await _command_to_result(finished)

    async def output(self, stream: str = "both") -> str:
        return await self.command.output(stream)

    async def stdout(self) -> str:
        return await self.command.stdout()

    async def stderr(self) -> str:
        return await self.command.stderr()

    async def kill(self, signal: int = 15) -> None:
        await self.command.kill(signal=signal)


@dataclass
class VercelSandboxHandle(Sandbox):
    sandbox: _VercelAsyncSandboxLike

    @property
    def id(self) -> str:
        return str(self.sandbox.sandbox_id)

    @property
    def provider(self) -> str:
        return "vercel"

    @property
    def native(self) -> _VercelAsyncSandboxLike:
        return self.sandbox

    async def exec(
        self,
        *args: str,
        cwd: str | None = None,
        env: dict[str, str] | None = None,
        timeout_s: float | None = None,
        sudo: bool = False,
    ) -> SandboxExecResult:
        if not args:
            raise ValueError("sandbox.exec requires at least one command argument")
        run = self.sandbox.run_command(
            args[0],
            list(args[1:]) or None,
            cwd=cwd,
            env=env,
            sudo=sudo,
        )
        finished = (
            await asyncio.wait_for(run, timeout=timeout_s)
            if timeout_s is not None
            else await run
        )
        return await _command_to_result(finished)

    async def spawn(
        self,
        *args: str,
        cwd: str | None = None,
        env: dict[str, str] | None = None,
        timeout_s: float | None = None,
        sudo: bool = False,
    ) -> SandboxProcess:
        if not args:
            raise ValueError("sandbox.spawn requires at least one command argument")
        spawn = self.sandbox.run_command_detached(
            args[0],
            list(args[1:]) or None,
            cwd=cwd,
            env=env,
            sudo=sudo,
        )
        command = (
            await asyncio.wait_for(spawn, timeout=timeout_s)
            if timeout_s is not None
            else await spawn
        )
        return VercelSandboxProcess(command=command)

    async def read_file(self, path: str) -> bytes | None:
        return await self.sandbox.read_file(path)

    async def write_files(self, files: list[SandboxWriteFile]) -> None:
        payload: list[_VercelWriteFile] = [
            {
                "path": file.path,
                "content": encode_sandbox_file_content(file.content),
            }
            for file in files
        ]
        await self.sandbox.write_files(payload)

    async def mkdir(self, path: str, *, parents: bool = True) -> None:
        del parents
        await self.sandbox.mk_dir(path)

    async def url(self, port: int) -> str:
        return self.sandbox.domain(port)

    async def checkpoint(self) -> SandboxCheckpoint:
        snapshot = await self.sandbox.snapshot()
        return SandboxCheckpoint(
            provider="vercel",
            kind="snapshot",
            ref=str(snapshot.snapshot_id),
            metadata={
                "expires_at": snapshot.expires_at,
                "source_sandbox_id": snapshot.source_sandbox_id,
            },
        )


class VercelSandboxProvider(SandboxProvider):
    async def create(
        self,
        ctx: ResourceContext,
        request: ResourceRequest[Sandbox],
    ) -> Sandbox:
        del self, ctx, request
        async_sandbox = _load_vercel_async_sandbox()
        sandbox = await async_sandbox.create(
            ports=_configured_ports(),
            timeout=_configured_timeout_ms(),
            runtime=_configured_runtime(),
        )
        await _ensure_running(sandbox)
        return VercelSandboxHandle(sandbox=sandbox)

    async def restore(
        self,
        checkpoint: ResourceCheckpoint,
        ctx: ResourceContext,
        request: ResourceRequest[Sandbox],
    ) -> Sandbox:
        del self, ctx, request
        async_sandbox = _load_vercel_async_sandbox()
        source: _VercelSnapshotSource = {
            "type": "snapshot",
            "snapshot_id": checkpoint.ref,
        }
        sandbox = await async_sandbox.create(
            source=source,
            ports=_configured_ports(),
            timeout=_configured_timeout_ms(),
            runtime=_configured_runtime(),
        )
        await _ensure_running(sandbox)
        return VercelSandboxHandle(sandbox=sandbox)

    async def checkpoint(
        self,
        resource: Sandbox,
        ctx: ResourceContext,
        request: ResourceRequest[Sandbox],
    ) -> ResourceCheckpoint | None:
        del self, ctx, request
        return await resource.checkpoint()

    async def destroy(
        self,
        resource: Sandbox,
        ctx: ResourceContext,
        request: ResourceRequest[Sandbox],
    ) -> None:
        del self, ctx, request
        native = resource.native
        stop = getattr(native, "stop", None)
        if callable(stop):
            with suppress(Exception):
                await stop()

    async def attach_live(
        self,
        live_ref: LiveResourceRef,
        ctx: ResourceContext,
        request: ResourceRequest[Sandbox],
    ) -> Sandbox | None:
        del self, ctx, request
        async_sandbox = _load_vercel_async_sandbox()
        try:
            sandbox = await async_sandbox.get(sandbox_id=live_ref.ref)
        except Exception as exc:
            if _is_not_found_error(exc):
                return None
            raise
        if sandbox.status not in {"running", "pending"}:
            return None
        await _ensure_running(sandbox)
        return VercelSandboxHandle(sandbox=sandbox)

    def capture_live_ref(
        self,
        resource: Sandbox,
        ctx: ResourceContext,
        request: ResourceRequest[Sandbox],
    ) -> LiveResourceRef | None:
        del self, ctx, request
        return LiveResourceRef(
            provider="vercel",
            kind="sandbox",
            ref=resource.id,
        )

    async def delete_checkpoint(
        self,
        checkpoint: ResourceCheckpoint,
        ctx: ResourceContext,
        request: ResourceRequest[Sandbox],
    ) -> None:
        del self, ctx, request
        async_snapshot = _load_vercel_async_snapshot()
        with suppress(Exception):
            snapshot = await async_snapshot.get(snapshot_id=checkpoint.ref)
            await snapshot.delete()

_VERCEL_PROVIDER: VercelSandboxProvider | None = None


def get_provider() -> SandboxProvider:
    global _VERCEL_PROVIDER
    if _VERCEL_PROVIDER is None:
        _VERCEL_PROVIDER = VercelSandboxProvider()
    return _VERCEL_PROVIDER


__all__ = [
    "VercelSandboxHandle",
    "VercelSandboxProvider",
    "VercelSandboxProcess",
    "get_provider",
]
