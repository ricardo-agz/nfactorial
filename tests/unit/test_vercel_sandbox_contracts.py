from __future__ import annotations

import json
from dataclasses import dataclass, field

import fakeredis.aioredis
import pytest

from factorial import ResourceContext, ResourceRequest, Sandbox
from factorial._internal.queue.keys import RedisKeys
from factorial.resources import (
    RedisResourceBindingStore,
    ResourceLease,
    ResourceLeaseLostError,
    ResourceManager,
)
from factorial.resources.core import resource_type_key
from factorial.resources.sandbox.providers import make_sandbox_request_metadata
from factorial.resources.sandbox.vercel import get_provider


@dataclass
class _FakeCommand:
    cmd_id: str
    stdout_text: str = ""
    stderr_text: str = ""
    exit_code: int = 0
    killed_signals: list[int] = field(default_factory=list)

    async def wait(self) -> _FakeCommand:
        return self

    async def output(self, stream: str = "both") -> str:
        if stream == "stdout":
            return self.stdout_text
        if stream == "stderr":
            return self.stderr_text
        return self.stdout_text + self.stderr_text

    async def stdout(self) -> str:
        return self.stdout_text

    async def stderr(self) -> str:
        return self.stderr_text

    async def kill(self, signal: int = 15) -> None:
        self.killed_signals.append(signal)


@dataclass
class _FakeSnapshot:
    snapshot_id: str
    source_sandbox_id: str
    expires_at: int = 9999999999


@dataclass
class _FakeRemoteSnapshot:
    snapshot_id: str

    async def delete(self) -> None:
        _FakeAsyncSnapshotApi.deleted_ids.append(self.snapshot_id)


class _FakeAsyncSnapshotApi:
    deleted_ids: list[str] = []

    @classmethod
    def reset(cls) -> None:
        cls.deleted_ids = []

    @classmethod
    async def get(cls, *, snapshot_id: str):
        return _FakeRemoteSnapshot(snapshot_id=snapshot_id)


class _FakeAsyncSandbox:
    created_kwargs: list[dict] = []
    get_ids: list[str] = []
    instances: dict[str, _FakeAsyncSandbox] = {}
    snapshot_sources: dict[str, str] = {}
    counter: int = 0

    def __init__(self, sandbox_id: str, *, status: str = "running") -> None:
        self.sandbox_id = sandbox_id
        self.status = status
        self.files: dict[str, bytes] = {}
        self.directories: list[str] = []
        self.commands: list[tuple[str, list[str], str | None]] = []
        self.detached_commands: list[tuple[str, list[str], str | None]] = []
        self.stop_calls = 0
        _FakeAsyncSandbox.instances[sandbox_id] = self

    @classmethod
    def reset(cls) -> None:
        cls.created_kwargs = []
        cls.get_ids = []
        cls.instances = {}
        cls.snapshot_sources = {}
        cls.counter = 0

    @classmethod
    async def create(cls, **kwargs):
        cls.created_kwargs.append(dict(kwargs))
        cls.counter += 1
        sandbox = cls(f"sb-{cls.counter}")
        if kwargs.get("source"):
            sandbox.source_snapshot_id = kwargs["source"]["snapshot_id"]
        else:
            sandbox.source_snapshot_id = None
        return sandbox

    @classmethod
    async def get(cls, *, sandbox_id: str):
        cls.get_ids.append(sandbox_id)
        sandbox = cls.instances.get(sandbox_id)
        if sandbox is None:
            raise RuntimeError("missing sandbox")
        return sandbox

    async def wait_for_status(self, status: str, *, timeout: float) -> None:
        del timeout
        self.status = status

    async def run_command(
        self,
        cmd: str,
        args: list[str] | None = None,
        *,
        cwd: str | None = None,
        env: dict[str, str] | None = None,
        sudo: bool = False,
    ) -> _FakeCommand:
        del env, sudo
        args = args or []
        self.commands.append((cmd, list(args), cwd))
        return _FakeCommand(
            cmd_id=f"{self.sandbox_id}-cmd-{len(self.commands)}",
            stdout_text=f"ran {cmd} {' '.join(args)}".strip(),
            stderr_text="",
            exit_code=0,
        )

    async def run_command_detached(
        self,
        cmd: str,
        args: list[str] | None = None,
        *,
        cwd: str | None = None,
        env: dict[str, str] | None = None,
        sudo: bool = False,
    ) -> _FakeCommand:
        del env, sudo
        args = args or []
        self.detached_commands.append((cmd, list(args), cwd))
        return _FakeCommand(
            cmd_id=f"{self.sandbox_id}-detached-{len(self.detached_commands)}",
            stdout_text="detached stdout",
            stderr_text="detached stderr",
            exit_code=0,
        )

    async def read_file(self, path: str):
        return self.files.get(path)

    async def write_files(self, files: list[dict]) -> None:
        for file in files:
            self.files[str(file["path"])] = bytes(file["content"])

    async def mk_dir(self, path: str) -> None:
        self.directories.append(path)

    def domain(self, port: int) -> str:
        return f"https://{self.sandbox_id}-{port}.example.test"

    async def snapshot(self) -> _FakeSnapshot:
        snapshot_id = f"snap-{self.sandbox_id}"
        self.status = "stopped"
        self.snapshot_sources[snapshot_id] = self.sandbox_id
        return _FakeSnapshot(
            snapshot_id=snapshot_id,
            source_sandbox_id=self.sandbox_id,
        )

    async def stop(self) -> None:
        self.stop_calls += 1
        self.status = "stopped"


async def _seed_task_lease(
    redis_client,
    *,
    namespace: str,
    task_id: str,
    status: str = "processing",
    pickups: int = 1,
) -> None:
    keys = RedisKeys.format(namespace=namespace)
    await redis_client.hset(keys.task_status, task_id, status)
    await redis_client.hset(keys.task_pickups, task_id, pickups)


def _vercel_request_metadata() -> dict[str, str]:
    return make_sandbox_request_metadata("vercel", explicit=False)


@pytest.mark.asyncio
async def test_vercel_sandbox_handle_wraps_common_operations(monkeypatch) -> None:
    _FakeAsyncSandbox.reset()
    monkeypatch.setattr(
        "factorial.resources.sandbox.vercel._load_vercel_async_sandbox",
        lambda: _FakeAsyncSandbox,
    )
    monkeypatch.setenv("NFACTORIAL_SANDBOX_PORTS", "7000,8000")
    monkeypatch.setenv("NFACTORIAL_SANDBOX_TIMEOUT_MS", "123000")

    sandbox = await get_provider().create(
        ResourceContext(task_id="task-1", owner_id="owner-1", agent_name="agent-1"),
        ResourceRequest(
            resource_type=Sandbox,
            logical_name="default",
            metadata=_vercel_request_metadata(),
        ),
    )

    await sandbox.write_file("README.md", "# hello")
    await sandbox.mkdir("notes")
    exec_result = await sandbox.exec("python", "-V", cwd="/work")
    process = await sandbox.spawn("python", "-m", "http.server", "8000")
    checkpoint = await sandbox.checkpoint()

    assert _FakeAsyncSandbox.created_kwargs[0]["ports"] == [7000, 8000]
    assert _FakeAsyncSandbox.created_kwargs[0]["timeout"] == 123000
    assert await sandbox.read_file("README.md") == b"# hello"
    assert exec_result.command_id == "sb-1-cmd-1"
    assert exec_result.stdout_text == "ran python -V"
    assert await process.stdout() == "detached stdout"
    await process.kill(signal=9)
    assert process.command.killed_signals == [9]
    assert await sandbox.url(8000) == "https://sb-1-8000.example.test"
    assert checkpoint.ref == "snap-sb-1"
    assert checkpoint.metadata["source_sandbox_id"] == "sb-1"


@pytest.mark.asyncio
async def test_resource_manager_attaches_existing_live_vercel_sandbox(
    monkeypatch,
) -> None:
    _FakeAsyncSandbox.reset()
    _FakeAsyncSnapshotApi.reset()
    monkeypatch.setattr(
        "factorial.resources.sandbox.vercel._load_vercel_async_sandbox",
        lambda: _FakeAsyncSandbox,
    )
    monkeypatch.setattr(
        "factorial.resources.sandbox.vercel._load_vercel_async_snapshot",
        lambda: _FakeAsyncSnapshotApi,
    )

    redis_client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    try:
        await _seed_task_lease(
            redis_client,
            namespace="test",
            task_id="task-attach",
            pickups=1,
        )
        manager1 = ResourceManager(
            store=RedisResourceBindingStore(
                redis_client=redis_client,
                namespace="test",
                task_id="task-attach",
            ),
            task_id="task-attach",
            owner_id="owner-1",
            agent_name="agent-1",
            lease=ResourceLease.worker(1),
        )
        sandbox1 = await manager1.get(
            Sandbox,
            request_metadata=_vercel_request_metadata(),
        )

        manager2 = ResourceManager(
            store=RedisResourceBindingStore(
                redis_client=redis_client,
                namespace="test",
                task_id="task-attach",
            ),
            task_id="task-attach",
            owner_id="owner-1",
            agent_name="agent-1",
            lease=ResourceLease.worker(1),
        )
        sandbox2 = await manager2.get(
            Sandbox,
            request_metadata=_vercel_request_metadata(),
        )

        assert sandbox1.id == sandbox2.id
        assert _FakeAsyncSandbox.get_ids == [sandbox1.id]
    finally:
        await redis_client.aclose()


@pytest.mark.asyncio
async def test_resource_manager_restores_checkpointed_vercel_sandbox(
    monkeypatch,
) -> None:
    _FakeAsyncSandbox.reset()
    _FakeAsyncSnapshotApi.reset()
    monkeypatch.setattr(
        "factorial.resources.sandbox.vercel._load_vercel_async_sandbox",
        lambda: _FakeAsyncSandbox,
    )
    monkeypatch.setattr(
        "factorial.resources.sandbox.vercel._load_vercel_async_snapshot",
        lambda: _FakeAsyncSnapshotApi,
    )

    redis_client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    try:
        await _seed_task_lease(
            redis_client,
            namespace="test",
            task_id="task-restore",
            pickups=1,
        )
        manager1 = ResourceManager(
            store=RedisResourceBindingStore(
                redis_client=redis_client,
                namespace="test",
                task_id="task-restore",
            ),
            task_id="task-restore",
            owner_id="owner-1",
            agent_name="agent-1",
            lease=ResourceLease.worker(1),
        )
        sandbox1 = await manager1.get(
            Sandbox,
            request_metadata=_vercel_request_metadata(),
        )
        await manager1.checkpoint_all()

        await _seed_task_lease(
            redis_client,
            namespace="test",
            task_id="task-restore",
            pickups=2,
        )

        manager2 = ResourceManager(
            store=RedisResourceBindingStore(
                redis_client=redis_client,
                namespace="test",
                task_id="task-restore",
            ),
            task_id="task-restore",
            owner_id="owner-1",
            agent_name="agent-1",
            lease=ResourceLease.worker(2),
        )
        sandbox2 = await manager2.get(
            Sandbox,
            request_metadata=_vercel_request_metadata(),
        )

        assert sandbox1.id == "sb-1"
        assert sandbox2.id == "sb-2"
        assert _FakeAsyncSandbox.created_kwargs[-1]["source"] == {
            "type": "snapshot",
            "snapshot_id": "snap-sb-1",
        }
    finally:
        await redis_client.aclose()


@pytest.mark.asyncio
async def test_unavailable_live_sandbox_ref_restores_checkpoint(
    monkeypatch,
) -> None:
    _FakeAsyncSandbox.reset()
    _FakeAsyncSnapshotApi.reset()
    monkeypatch.setattr(
        "factorial.resources.sandbox.vercel._load_vercel_async_sandbox",
        lambda: _FakeAsyncSandbox,
    )
    monkeypatch.setattr(
        "factorial.resources.sandbox.vercel._load_vercel_async_snapshot",
        lambda: _FakeAsyncSnapshotApi,
    )

    redis_client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    try:
        await _seed_task_lease(
            redis_client,
            namespace="test",
            task_id="task-stale-live-with-checkpoint",
            pickups=1,
        )
        manager1 = ResourceManager(
            store=RedisResourceBindingStore(
                redis_client=redis_client,
                namespace="test",
                task_id="task-stale-live-with-checkpoint",
            ),
            task_id="task-stale-live-with-checkpoint",
            owner_id="owner-1",
            agent_name="agent-1",
            lease=ResourceLease.worker(1),
        )
        sandbox1 = await manager1.get(
            Sandbox,
            request_metadata=_vercel_request_metadata(),
        )
        await manager1.checkpoint_all()

        await _seed_task_lease(
            redis_client,
            namespace="test",
            task_id="task-stale-live-with-checkpoint",
            pickups=2,
        )
        manager2 = ResourceManager(
            store=RedisResourceBindingStore(
                redis_client=redis_client,
                namespace="test",
                task_id="task-stale-live-with-checkpoint",
            ),
            task_id="task-stale-live-with-checkpoint",
            owner_id="owner-1",
            agent_name="agent-1",
            lease=ResourceLease.worker(2),
        )
        sandbox2 = await manager2.get(
            Sandbox,
            request_metadata=_vercel_request_metadata(),
        )
        _FakeAsyncSandbox.instances[sandbox2.id].status = "stopped"

        manager3 = ResourceManager(
            store=RedisResourceBindingStore(
                redis_client=redis_client,
                namespace="test",
                task_id="task-stale-live-with-checkpoint",
            ),
            task_id="task-stale-live-with-checkpoint",
            owner_id="owner-1",
            agent_name="agent-1",
            lease=ResourceLease.worker(2),
        )
        sandbox3 = await manager3.get(
            Sandbox,
            request_metadata=_vercel_request_metadata(),
        )

        assert sandbox1.id == "sb-1"
        assert sandbox2.id == "sb-2"
        assert sandbox3.id == "sb-3"
        assert _FakeAsyncSandbox.get_ids[-1] == "sb-2"
        assert _FakeAsyncSandbox.created_kwargs[-1]["source"] == {
            "type": "snapshot",
            "snapshot_id": "snap-sb-1",
        }
    finally:
        await redis_client.aclose()


@pytest.mark.asyncio
async def test_unavailable_live_sandbox_ref_without_checkpoint_creates_fresh(
    monkeypatch,
) -> None:
    _FakeAsyncSandbox.reset()
    _FakeAsyncSnapshotApi.reset()
    monkeypatch.setattr(
        "factorial.resources.sandbox.vercel._load_vercel_async_sandbox",
        lambda: _FakeAsyncSandbox,
    )
    monkeypatch.setattr(
        "factorial.resources.sandbox.vercel._load_vercel_async_snapshot",
        lambda: _FakeAsyncSnapshotApi,
    )

    redis_client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    try:
        await _seed_task_lease(
            redis_client,
            namespace="test",
            task_id="task-stale-live-fresh",
            pickups=1,
        )
        manager1 = ResourceManager(
            store=RedisResourceBindingStore(
                redis_client=redis_client,
                namespace="test",
                task_id="task-stale-live-fresh",
            ),
            task_id="task-stale-live-fresh",
            owner_id="owner-1",
            agent_name="agent-1",
            lease=ResourceLease.worker(1),
        )
        sandbox1 = await manager1.get(
            Sandbox,
            request_metadata=_vercel_request_metadata(),
        )
        _FakeAsyncSandbox.instances[sandbox1.id].status = "stopped"

        manager2 = ResourceManager(
            store=RedisResourceBindingStore(
                redis_client=redis_client,
                namespace="test",
                task_id="task-stale-live-fresh",
            ),
            task_id="task-stale-live-fresh",
            owner_id="owner-1",
            agent_name="agent-1",
            lease=ResourceLease.worker(1),
        )
        sandbox2 = await manager2.get(
            Sandbox,
            request_metadata=_vercel_request_metadata(),
        )

        task_keys = RedisKeys.format(namespace="test", task_id="task-stale-live-fresh")
        bindings = await redis_client.hgetall(task_keys.resource_bindings)
        binding = json.loads(next(iter(bindings.values())))

        assert sandbox1.id == "sb-1"
        assert sandbox2.id == "sb-2"
        assert _FakeAsyncSandbox.get_ids == ["sb-1"]
        assert _FakeAsyncSandbox.created_kwargs[-1].get("source") is None
        assert binding["binding_metadata"] == _vercel_request_metadata()
    finally:
        await redis_client.aclose()


@pytest.mark.asyncio
async def test_attach_unavailable_commit_is_operation_fenced(
    monkeypatch,
) -> None:
    _FakeAsyncSandbox.reset()
    _FakeAsyncSnapshotApi.reset()
    monkeypatch.setattr(
        "factorial.resources.sandbox.vercel._load_vercel_async_sandbox",
        lambda: _FakeAsyncSandbox,
    )
    monkeypatch.setattr(
        "factorial.resources.sandbox.vercel._load_vercel_async_snapshot",
        lambda: _FakeAsyncSnapshotApi,
    )

    redis_client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    try:
        await _seed_task_lease(
            redis_client,
            namespace="test",
            task_id="task-attach-conflict",
            pickups=1,
        )
        manager = ResourceManager(
            store=RedisResourceBindingStore(
                redis_client=redis_client,
                namespace="test",
                task_id="task-attach-conflict",
            ),
            task_id="task-attach-conflict",
            owner_id="owner-1",
            agent_name="agent-1",
            lease=ResourceLease.worker(1),
        )
        sandbox = await manager.get(
            Sandbox,
            request_metadata=_vercel_request_metadata(),
        )

        store = RedisResourceBindingStore(
            redis_client=redis_client,
            namespace="test",
            task_id="task-attach-conflict",
        )
        decision = await store.begin_acquire(
            resource_type_key_value=resource_type_key(Sandbox),
            logical_name="default",
            binding_metadata=_vercel_request_metadata(),
            lease=ResourceLease.worker(1),
            operation_id="attach-op",
            now=1.0,
            operation_timeout_s=15.0,
        )
        assert decision.outcome == "attach"
        assert decision.reservation is not None

        task_keys = RedisKeys.format(namespace="test", task_id="task-attach-conflict")
        resource_field = f"{resource_type_key(Sandbox)}:default"
        binding = json.loads(
            await redis_client.hget(task_keys.resource_bindings, resource_field)
        )
        binding["operation_id"] = "other-op"
        await redis_client.hset(
            task_keys.resource_bindings,
            resource_field,
            json.dumps(binding),
        )

        status = await store.commit_attach_unavailable(
            reservation=decision.reservation,
            lease=ResourceLease.worker(1),
            now=2.0,
        )
        persisted = json.loads(
            await redis_client.hget(task_keys.resource_bindings, resource_field)
        )

        assert status == "operation_conflict"
        assert persisted["live_ref"]["ref"] == sandbox.id
        assert persisted["operation_id"] == "other-op"
    finally:
        await redis_client.aclose()


@pytest.mark.asyncio
async def test_guarded_sandbox_rejects_stale_worker_after_lease_loss(
    monkeypatch,
) -> None:
    _FakeAsyncSandbox.reset()
    _FakeAsyncSnapshotApi.reset()
    monkeypatch.setattr(
        "factorial.resources.sandbox.vercel._load_vercel_async_sandbox",
        lambda: _FakeAsyncSandbox,
    )
    monkeypatch.setattr(
        "factorial.resources.sandbox.vercel._load_vercel_async_snapshot",
        lambda: _FakeAsyncSnapshotApi,
    )

    redis_client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    try:
        await _seed_task_lease(
            redis_client,
            namespace="test",
            task_id="task-stale",
            pickups=1,
        )
        manager = ResourceManager(
            store=RedisResourceBindingStore(
                redis_client=redis_client,
                namespace="test",
                task_id="task-stale",
            ),
            task_id="task-stale",
            owner_id="owner-1",
            agent_name="agent-1",
            lease=ResourceLease.worker(1),
        )
        sandbox = await manager.get(
            Sandbox,
            request_metadata=_vercel_request_metadata(),
        )

        await _seed_task_lease(
            redis_client,
            namespace="test",
            task_id="task-stale",
            status="active",
            pickups=1,
        )

        with pytest.raises(ResourceLeaseLostError):
            await sandbox.write_file("stale.txt", "nope")
    finally:
        await redis_client.aclose()


@pytest.mark.asyncio
async def test_worker_destroy_all_deletes_persisted_checkpointed_sandbox(
    monkeypatch,
) -> None:
    _FakeAsyncSandbox.reset()
    _FakeAsyncSnapshotApi.reset()
    monkeypatch.setattr(
        "factorial.resources.sandbox.vercel._load_vercel_async_sandbox",
        lambda: _FakeAsyncSandbox,
    )
    monkeypatch.setattr(
        "factorial.resources.sandbox.vercel._load_vercel_async_snapshot",
        lambda: _FakeAsyncSnapshotApi,
    )

    redis_client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    try:
        await _seed_task_lease(
            redis_client,
            namespace="test",
            task_id="task-cleanup",
            pickups=1,
        )
        manager1 = ResourceManager(
            store=RedisResourceBindingStore(
                redis_client=redis_client,
                namespace="test",
                task_id="task-cleanup",
            ),
            task_id="task-cleanup",
            owner_id="owner-1",
            agent_name="agent-1",
            lease=ResourceLease.worker(1),
        )
        await manager1.get(
            Sandbox,
            request_metadata=_vercel_request_metadata(),
        )
        await manager1.checkpoint_all()

        await _seed_task_lease(
            redis_client,
            namespace="test",
            task_id="task-cleanup",
            pickups=2,
        )
        manager2 = ResourceManager(
            store=RedisResourceBindingStore(
                redis_client=redis_client,
                namespace="test",
                task_id="task-cleanup",
            ),
            task_id="task-cleanup",
            owner_id="owner-1",
            agent_name="agent-1",
            lease=ResourceLease.worker(2),
        )
        await manager2.destroy_all()

        task_keys = RedisKeys.format(namespace="test", task_id="task-cleanup")
        assert await redis_client.exists(task_keys.resource_bindings) == 0
        assert _FakeAsyncSnapshotApi.deleted_ids == ["snap-sb-1"]
    finally:
        await redis_client.aclose()


@pytest.mark.asyncio
async def test_system_destroy_all_cleans_persisted_live_sandbox(
    monkeypatch,
) -> None:
    _FakeAsyncSandbox.reset()
    _FakeAsyncSnapshotApi.reset()
    monkeypatch.setattr(
        "factorial.resources.sandbox.vercel._load_vercel_async_sandbox",
        lambda: _FakeAsyncSandbox,
    )
    monkeypatch.setattr(
        "factorial.resources.sandbox.vercel._load_vercel_async_snapshot",
        lambda: _FakeAsyncSnapshotApi,
    )

    redis_client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    try:
        await _seed_task_lease(
            redis_client,
            namespace="test",
            task_id="task-system-cleanup",
            pickups=1,
        )
        manager1 = ResourceManager(
            store=RedisResourceBindingStore(
                redis_client=redis_client,
                namespace="test",
                task_id="task-system-cleanup",
            ),
            task_id="task-system-cleanup",
            owner_id="owner-1",
            agent_name="agent-1",
            lease=ResourceLease.worker(1),
        )
        await manager1.get(
            Sandbox,
            request_metadata=_vercel_request_metadata(),
        )

        await _seed_task_lease(
            redis_client,
            namespace="test",
            task_id="task-system-cleanup",
            status="cancelled",
            pickups=1,
        )
        manager2 = ResourceManager(
            store=RedisResourceBindingStore(
                redis_client=redis_client,
                namespace="test",
                task_id="task-system-cleanup",
            ),
            task_id="task-system-cleanup",
            owner_id="owner-1",
            agent_name="agent-1",
            lease=ResourceLease.system(),
        )
        await manager2.destroy_all()

        task_keys = RedisKeys.format(namespace="test", task_id="task-system-cleanup")
        assert await redis_client.exists(task_keys.resource_bindings) == 0
        assert _FakeAsyncSandbox.instances["sb-1"].stop_calls == 1
    finally:
        await redis_client.aclose()
