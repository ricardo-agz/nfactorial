from collections.abc import Callable
from typing import Any

from .delete import delete
from .edit_lines import edit_lines
from .find_replace import find_replace
from .multi_edit import multi_edit
from .read import read
from .write import write

file_tools: list[Callable[..., Any]] = [
    delete,
    read,
    edit_lines,
    multi_edit,
    write,
    find_replace,
]

__all__ = [
    "file_tools",
    "delete",
    "read",
    "edit_lines",
    "multi_edit",
    "write",
    "find_replace",
]
