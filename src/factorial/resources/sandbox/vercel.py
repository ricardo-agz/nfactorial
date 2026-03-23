from __future__ import annotations

import asyncio
import os
from contextlib import suppress
from dataclasses import dataclass
from typing import Any

from ..core import (
    LiveResourceRef,
    ResourceCheckpoint,
    ResourceContext,
    ResourceRequest,
    register_resource_lifecycle,
)
from .base import (
    Sandbox,
    SandboxCheckpoint,
    SandboxExecResult,
    SandboxProcess,
    SandboxWriteFile,
    encode_sandbox_file_content,
)

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


def _load_vercel_async_sandbox() -> Any:
    try:
        from vercel.sandbox import AsyncSandbox
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError(
            "Sandbox support requires the `vercel` package. "
            'Install it with `pip install "nfactorial[vercel]"`.'
        ) from exc
    return AsyncSandbox


async def _ensure_running(native_sandbox: Any) -> None:
    if getattr(native_sandbox, "status", None) == "running":
        return
    wait_for_status = getattr(native_sandbox, "wait_for_status", None)
    if callable(wait_for_status):
        await wait_for_status("running", timeout=_configured_ready_timeout_s())


async def _command_to_result(command: Any) -> SandboxExecResult:
    return SandboxExecResult(
        command_id=getattr(command, "cmd_id", None),
        exit_code=int(getattr(command, "exit_code", 0)),
        stdout_text=await command.stdout(),
        stderr_text=await command.stderr(),
    )


@dataclass
class VercelSandboxProcess(SandboxProcess):
    command: Any

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
    sandbox: Any

    @property
    def id(self) -> str:
        return str(self.sandbox.sandbox_id)

    @property
    def provider(self) -> str:
        return "vercel"

    @property
    def native(self) -> object:
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
        await self.sandbox.write_files(
            [
                {
                    "path": file.path,
                    "content": encode_sandbox_file_content(file.content),
                }
                for file in files
            ]
        )

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


class VercelSandboxLifecycle:
    @classmethod
    async def create(
        cls,
        ctx: ResourceContext,
        request: ResourceRequest[Sandbox],
    ) -> Sandbox:
        del ctx, request
        async_sandbox = _load_vercel_async_sandbox()
        sandbox = await async_sandbox.create(
            ports=_configured_ports(),
            timeout=_configured_timeout_ms(),
            runtime=_configured_runtime(),
        )
        await _ensure_running(sandbox)
        return VercelSandboxHandle(sandbox=sandbox)

    @classmethod
    async def restore(
        cls,
        checkpoint: ResourceCheckpoint,
        ctx: ResourceContext,
        request: ResourceRequest[Sandbox],
    ) -> Sandbox:
        del ctx, request
        async_sandbox = _load_vercel_async_sandbox()
        sandbox = await async_sandbox.create(
            source={"type": "snapshot", "snapshot_id": checkpoint.ref},
            ports=_configured_ports(),
            timeout=_configured_timeout_ms(),
            runtime=_configured_runtime(),
        )
        await _ensure_running(sandbox)
        return VercelSandboxHandle(sandbox=sandbox)

    @classmethod
    async def checkpoint(
        cls,
        resource: Sandbox,
        ctx: ResourceContext,
        request: ResourceRequest[Sandbox],
    ) -> ResourceCheckpoint | None:
        del ctx, request
        return await resource.checkpoint()

    @classmethod
    async def destroy(
        cls,
        resource: Sandbox,
        ctx: ResourceContext,
        request: ResourceRequest[Sandbox],
    ) -> None:
        del ctx, request
        native = getattr(resource, "native", None)
        stop = getattr(native, "stop", None)
        if callable(stop):
            with suppress(Exception):
                await stop()

    @classmethod
    async def attach_live(
        cls,
        live_ref: LiveResourceRef,
        ctx: ResourceContext,
        request: ResourceRequest[Sandbox],
    ) -> Sandbox | None:
        del ctx, request
        async_sandbox = _load_vercel_async_sandbox()
        with suppress(Exception):
            sandbox = await async_sandbox.get(sandbox_id=live_ref.ref)
            if getattr(sandbox, "status", None) not in {"running", "pending"}:
                return None
            await _ensure_running(sandbox)
            return VercelSandboxHandle(sandbox=sandbox)
        return None

    @classmethod
    def capture_live_ref(
        cls,
        resource: Sandbox,
        ctx: ResourceContext,
        request: ResourceRequest[Sandbox],
    ) -> LiveResourceRef | None:
        del ctx, request
        return LiveResourceRef(
            provider="vercel",
            kind="sandbox",
            ref=resource.id,
        )


register_resource_lifecycle(Sandbox, VercelSandboxLifecycle)


__all__ = [
    "VercelSandboxHandle",
    "VercelSandboxLifecycle",
    "VercelSandboxProcess",
]
