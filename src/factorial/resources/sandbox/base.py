from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass

from ..core import ResourceCheckpoint


@dataclass(frozen=True)
class SandboxWriteFile:
    path: str
    content: str | bytes


@dataclass(frozen=True)
class SandboxExecResult:
    command_id: str | None
    exit_code: int
    stdout_text: str
    stderr_text: str


@dataclass(frozen=True)
class SandboxCheckpoint(ResourceCheckpoint):
    @classmethod
    def from_resource_checkpoint(
        cls,
        checkpoint: ResourceCheckpoint,
    ) -> SandboxCheckpoint:
        if isinstance(checkpoint, cls):
            return checkpoint
        return cls(
            provider=checkpoint.provider,
            kind=checkpoint.kind,
            ref=checkpoint.ref,
            metadata=dict(checkpoint.metadata),
        )


class SandboxProcess(ABC):
    @property
    @abstractmethod
    def id(self) -> str: ...

    @abstractmethod
    async def wait(self) -> SandboxExecResult: ...

    @abstractmethod
    async def output(self, stream: str = "both") -> str: ...

    @abstractmethod
    async def stdout(self) -> str: ...

    @abstractmethod
    async def stderr(self) -> str: ...

    @abstractmethod
    async def kill(self, signal: int = 15) -> None: ...


class Sandbox(ABC):
    @property
    @abstractmethod
    def id(self) -> str: ...

    @property
    @abstractmethod
    def provider(self) -> str: ...

    @property
    @abstractmethod
    def native(self) -> object: ...

    @abstractmethod
    async def exec(
        self,
        *args: str,
        cwd: str | None = None,
        env: dict[str, str] | None = None,
        timeout_s: float | None = None,
        sudo: bool = False,
    ) -> SandboxExecResult: ...

    @abstractmethod
    async def spawn(
        self,
        *args: str,
        cwd: str | None = None,
        env: dict[str, str] | None = None,
        timeout_s: float | None = None,
        sudo: bool = False,
    ) -> SandboxProcess: ...

    @abstractmethod
    async def read_file(self, path: str) -> bytes | None: ...

    async def write_file(self, path: str, content: str | bytes) -> None:
        await self.write_files([SandboxWriteFile(path=path, content=content)])

    @abstractmethod
    async def write_files(self, files: list[SandboxWriteFile]) -> None: ...

    @abstractmethod
    async def mkdir(self, path: str, *, parents: bool = True) -> None: ...

    @abstractmethod
    async def url(self, port: int) -> str: ...

    @abstractmethod
    async def checkpoint(self) -> SandboxCheckpoint: ...


def encode_sandbox_file_content(content: str | bytes) -> bytes:
    if isinstance(content, bytes):
        return content
    return content.encode("utf-8")


__all__ = [
    "Sandbox",
    "SandboxCheckpoint",
    "SandboxExecResult",
    "SandboxProcess",
    "SandboxWriteFile",
    "encode_sandbox_file_content",
]
