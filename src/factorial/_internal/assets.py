from __future__ import annotations

from importlib.resources import files
from pathlib import Path


def package_file(package: str, *parts: str) -> Path:
    """Return a concrete package asset path for integrations that need a filename."""
    resource = files(package)
    for part in parts:
        resource = resource.joinpath(part)
    return Path(str(resource))


__all__ = ["package_file"]
