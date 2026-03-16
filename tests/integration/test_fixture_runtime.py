from __future__ import annotations

import asyncio
import uuid
from pathlib import Path

import fakeredis.aioredis
import pytest
from httpx import ASGITransport, AsyncClient

from tests.e2e.app import build_fixture_app
from tests.e2e.loader import load_fixture_bundle, resolve_orchestrator
from tests.e2e.run_probes import run_fixture_probes

FIXTURES_ROOT = (
    Path(__file__).resolve().parents[2]
    / "tests"
    / "e2e"
    / "fixtures"
)
FIXTURE_PATHS = sorted(
    path
    for path in FIXTURES_ROOT.iterdir()
    if path.is_dir()
    and (path / "orchestrator.py").exists()
    and (path / "probes.py").exists()
)

if not FIXTURE_PATHS:
    pytest.skip(
        "No e2e fixture folders found under tests/e2e/fixtures",
        allow_module_level=True,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("fixture_path", FIXTURE_PATHS, ids=lambda path: path.name)
async def test_fixture_probes_pass(fixture_path: Path) -> None:
    redis_client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    bundle = load_fixture_bundle(fixture_path)
    orchestrator = resolve_orchestrator(
        bundle,
        redis_pool=redis_client.connection_pool,
        namespace=f"fixture-test:{bundle.name}:{uuid.uuid4().hex[:8]}",
    )
    app = build_fixture_app(bundle, orchestrator=orchestrator, manage_workers=False)
    try:
        async with app.router.lifespan_context(app):
            worker_task = asyncio.create_task(
                orchestrator.start_workers(orchestrator.shutdown_event),
                name=f"fixture-runtime-workers:{bundle.name}",
            )

            try:
                async with AsyncClient(
                    transport=ASGITransport(app=app),
                    base_url="http://testserver",
                    timeout=10.0,
                ) as client:
                    outcomes = await run_fixture_probes(
                        bundle,
                        client=client,
                        base_url="http://testserver",
                    )
                assert outcomes
                assert all(outcome.ok for outcome in outcomes), outcomes
            finally:
                orchestrator.shutdown_event.set()
                await asyncio.wait_for(worker_task, timeout=10.0)
    finally:
        try:
            await redis_client.aclose()
        except Exception:
            pass
