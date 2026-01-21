from collections.abc import Callable
from typing import Any

from .glob import glob
from .grep import grep
from .ls import ls
from .tree import tree

search_tools: list[Callable[..., Any]] = [
    glob,
    grep,
    tree,
    ls,
]

__all__ = ["search_tools", "glob", "grep", "tree", "ls"]
