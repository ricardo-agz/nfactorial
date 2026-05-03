from __future__ import annotations

import argparse
import asyncio
import traceback
from dataclasses import dataclass
from time import perf_counter

import httpx

from .loader import FixtureBundle, load_fixture_bundle
from .sdk import ProbeContext, discover_probes, maybe_await


@dataclass(frozen=True)
class ProbeOutcome:
    name: str
    ok: bool
    duration_s: float
    error: str | None = None


async def run_fixture_probes(
    bundle: FixtureBundle,
    *,
    client: httpx.AsyncClient,
    base_url: str,
) -> list[ProbeOutcome]:
    definitions = discover_probes(bundle.probes_module)
    if not definitions:
        raise ValueError(f"Fixture '{bundle.name}' does not define any probes")

    outcomes: list[ProbeOutcome] = []
    for definition in definitions:
        started_at = perf_counter()
        try:
            ctx = ProbeContext(
                fixture_name=bundle.name,
                probe_name=definition.name,
                base_url=base_url,
                client=client,
            )
            await asyncio.wait_for(
                maybe_await(definition.func(ctx)),
                timeout=definition.timeout_s,
            )
            duration_s = perf_counter() - started_at
            outcomes.append(
                ProbeOutcome(
                    name=definition.name,
                    ok=True,
                    duration_s=duration_s,
                )
            )
        except Exception:
            duration_s = perf_counter() - started_at
            outcomes.append(
                ProbeOutcome(
                    name=definition.name,
                    ok=False,
                    duration_s=duration_s,
                    error=traceback.format_exc(),
                )
            )
    return outcomes


async def _main_async(args: argparse.Namespace) -> int:
    bundle = load_fixture_bundle(args.fixture)

    async with httpx.AsyncClient(
        base_url=args.base_url,
        timeout=args.http_timeout,
    ) as client:
        outcomes = await run_fixture_probes(
            bundle,
            client=client,
            base_url=args.base_url,
        )

    failed = [outcome for outcome in outcomes if not outcome.ok]
    for outcome in outcomes:
        status = "PASS" if outcome.ok else "FAIL"
        print(f"[{status}] {outcome.name} ({outcome.duration_s:.2f}s)")
        if outcome.error:
            print(outcome.error.rstrip())

    print(
        f"\nFixture '{bundle.name}': "
        f"{len(outcomes) - len(failed)}/{len(outcomes)} probes passed"
    )
    return 1 if failed else 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run all probes for one fixture folder."
    )
    parser.add_argument("fixture", help="Path to the fixture directory")
    parser.add_argument(
        "--base-url",
        default="http://127.0.0.1:8000",
        help="Base URL for the running fixture app",
    )
    parser.add_argument(
        "--http-timeout",
        type=float,
        default=30.0,
        help="HTTP client timeout in seconds",
    )
    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    raise SystemExit(asyncio.run(_main_async(args)))


if __name__ == "__main__":
    main()
