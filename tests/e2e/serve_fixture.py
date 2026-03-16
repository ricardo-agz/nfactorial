from __future__ import annotations

import argparse
from pathlib import Path

import uvicorn

from .app import build_fixture_app
from .loader import load_fixture_bundle


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Serve a fixture app with probe routes."
    )
    parser.add_argument("fixture", help="Path to the fixture directory")
    parser.add_argument("--host", default="0.0.0.0", help="Host to bind")
    parser.add_argument("--port", type=int, default=8000, help="Port to bind")
    parser.add_argument(
        "--namespace",
        default=None,
        help="Override the orchestrator namespace for this fixture run",
    )
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    bundle = load_fixture_bundle(Path(args.fixture))
    app = build_fixture_app(bundle, namespace=args.namespace)
    uvicorn.run(app, host=args.host, port=args.port, log_level="info")


if __name__ == "__main__":
    main()
