from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from .base import (
    Sandbox,
    SandboxCheckpoint,
    SandboxExecResult,
    SandboxProcess,
    SandboxWriteFile,
)

LeaseValidator = Callable[[], Awaitable[None]]


@dataclass
class GuardedNativeHandle:
    native: object
    validator: LeaseValidator

    def __getattr__(self, name: str) -> Any:
        value = getattr(self.native, name)
        if not callable(value):
            return value

        async def _guarded_call(*args: Any, **kwargs: Any) -> Any:
            await self.validator()
            result = value(*args, **kwargs)
            if inspect.isawaitable(result):
                return await result
            return result

        return _guarded_call


@dataclass
class GuardedSandboxProcess(SandboxProcess):
    process: SandboxProcess
    validator: LeaseValidator

    @property
    def id(self) -> str:
        return self.process.id

    async def wait(self) -> SandboxExecResult:
        await self.validator()
        return await self.process.wait()

    async def output(self, stream: str = "both") -> str:
        await self.validator()
        return await self.process.output(stream)

    async def stdout(self) -> str:
        await self.validator()
        return await self.process.stdout()

    async def stderr(self) -> str:
        await self.validator()
        return await self.process.stderr()

    async def kill(self, signal: int = 15) -> None:
        await self.validator()
        await self.process.kill(signal=signal)


@dataclass
class GuardedSandbox(Sandbox):
    sandbox: Sandbox
    validator: LeaseValidator

    @property
    def id(self) -> str:
        return self.sandbox.id

    @property
    def provider(self) -> str:
        return self.sandbox.provider

    @property
    def native(self) -> object:
        return GuardedNativeHandle(self.sandbox.native, self.validator)

    async def exec(
        self,
        *args: str,
        cwd: str | None = None,
        env: dict[str, str] | None = None,
        timeout_s: float | None = None,
        sudo: bool = False,
    ) -> SandboxExecResult:
        await self.validator()
        return await self.sandbox.exec(
            *args,
            cwd=cwd,
            env=env,
            timeout_s=timeout_s,
            sudo=sudo,
        )

    async def spawn(
        self,
        *args: str,
        cwd: str | None = None,
        env: dict[str, str] | None = None,
        timeout_s: float | None = None,
        sudo: bool = False,
    ) -> SandboxProcess:
        await self.validator()
        process = await self.sandbox.spawn(
            *args,
            cwd=cwd,
            env=env,
            timeout_s=timeout_s,
            sudo=sudo,
        )
        return GuardedSandboxProcess(process=process, validator=self.validator)

    async def read_file(self, path: str) -> bytes | None:
        await self.validator()
        return await self.sandbox.read_file(path)

    async def write_files(self, files: list[SandboxWriteFile]) -> None:
        await self.validator()
        await self.sandbox.write_files(files)

    async def mkdir(self, path: str, *, parents: bool = True) -> None:
        await self.validator()
        await self.sandbox.mkdir(path, parents=parents)

    async def url(self, port: int) -> str:
        await self.validator()
        return await self.sandbox.url(port)

    async def checkpoint(self) -> SandboxCheckpoint:
        await self.validator()
        return await self.sandbox.checkpoint()


__all__ = [
    "GuardedNativeHandle",
    "GuardedSandbox",
    "GuardedSandboxProcess",
    "LeaseValidator",
]
