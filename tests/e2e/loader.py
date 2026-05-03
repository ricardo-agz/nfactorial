from __future__ import annotations

import hashlib
import importlib.util
import sys
import types
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType
from typing import Any

from factorial import Orchestrator


@dataclass(frozen=True)
class FixtureBundle:
    name: str
    path: Path
    package_name: str
    orchestrator_module: ModuleType
    probes_module: ModuleType
    agents_module: ModuleType | None = None


def _fixture_package_name(path: Path) -> str:
    digest = hashlib.sha1(str(path.resolve()).encode("utf-8")).hexdigest()[:12]
    sanitized = path.name.replace("-", "_")
    return f"_nfactorial_fixture_{sanitized}_{digest}"


def _load_submodule(
    *,
    package_name: str,
    module_name: str,
    path: Path,
) -> ModuleType:
    qualified_name = f"{package_name}.{module_name}"
    spec = importlib.util.spec_from_file_location(qualified_name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not create module spec for '{path}'")

    module = importlib.util.module_from_spec(spec)
    sys.modules[qualified_name] = module
    spec.loader.exec_module(module)
    return module


def load_fixture_bundle(path: str | Path) -> FixtureBundle:
    fixture_path = Path(path).resolve()
    if not fixture_path.exists():
        raise FileNotFoundError(f"Fixture folder does not exist: {fixture_path}")
    if not fixture_path.is_dir():
        raise NotADirectoryError(f"Fixture path must be a directory: {fixture_path}")

    orchestrator_path = fixture_path / "orchestrator.py"
    probes_path = fixture_path / "probes.py"
    agents_path = fixture_path / "agents.py"

    if not orchestrator_path.exists():
        raise FileNotFoundError(
            f"Fixture '{fixture_path.name}' is missing orchestrator.py"
        )
    if not probes_path.exists():
        raise FileNotFoundError(f"Fixture '{fixture_path.name}' is missing probes.py")

    package_name = _fixture_package_name(fixture_path)
    package = types.ModuleType(package_name)
    package.__path__ = [str(fixture_path)]  # type: ignore[attr-defined]
    sys.modules[package_name] = package

    agents_module = None
    if agents_path.exists():
        agents_module = _load_submodule(
            package_name=package_name,
            module_name="agents",
            path=agents_path,
        )

    orchestrator_module = _load_submodule(
        package_name=package_name,
        module_name="orchestrator",
        path=orchestrator_path,
    )
    probes_module = _load_submodule(
        package_name=package_name,
        module_name="probes",
        path=probes_path,
    )

    return FixtureBundle(
        name=fixture_path.name,
        path=fixture_path,
        package_name=package_name,
        orchestrator_module=orchestrator_module,
        probes_module=probes_module,
        agents_module=agents_module,
    )


def resolve_orchestrator(
    bundle: FixtureBundle,
    *,
    redis_pool: Any = None,
    namespace: str | None = None,
) -> Orchestrator:
    module = bundle.orchestrator_module
    factory = getattr(module, "build_orchestrator", None)
    if callable(factory):
        orchestrator = factory(redis_pool=redis_pool, namespace=namespace)
        if not isinstance(orchestrator, Orchestrator):
            raise TypeError(
                f"{bundle.path / 'orchestrator.py'} build_orchestrator() "
                "must return an Orchestrator instance"
            )
        return orchestrator

    orchestrator = getattr(module, "orchestrator", None)
    if isinstance(orchestrator, Orchestrator):
        if redis_pool is not None or namespace is not None:
            raise ValueError(
                f"Fixture '{bundle.name}' exports a global orchestrator only; "
                "injectable redis_pool/namespace requires build_orchestrator()."
            )
        return orchestrator

    raise ValueError(
        f"Fixture '{bundle.name}' must export build_orchestrator() "
        "or an 'orchestrator' instance"
    )
