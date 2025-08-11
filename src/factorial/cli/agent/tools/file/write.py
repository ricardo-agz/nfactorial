import os
from typing import Any
from ..utils import run_linter


def write(
    file_path: str, content: str, overwrite: bool = False
) -> tuple[str, dict[str, Any]]:
    """
    Writes a file to the file system with the given content.

    Arguments:
    file_path: The path to the file to create
    content: The content to write to the file
    overwrite: Whether to overwrite the file if it already exists

    Usage:
    * This tool will overwrite the file if it already exists unless the overwrite flag is set to False.
    * ALWAYS prefer editing existing files in the codebase. ONLY write new files when necessary.
    * NEVER proactively create documentation files or README files unless explicitly requested by the user.
    * Avoid using emojis in the file content unless asked by the user.
    * _CRITICAL_: DO NOT create empty files unless absolutely necessary. Even for files like __init__.py, include a comment with the file name.
    """
    file_exists = os.path.exists(file_path)

    if file_exists and not overwrite:
        return (
            f"Error: File {file_path} already exists. If you meant to edit the file use one of the available edit tools, if this was intentional, set the overwrite flag to True.",
            {},
        )

    parent_dir = os.path.dirname(file_path)
    if parent_dir and not os.path.exists(parent_dir):
        os.makedirs(parent_dir, exist_ok=True)

    with open(file_path, "w") as f:
        f.write(content)

    lint_errors = run_linter(file_path)

    line_count = content.count("\n") + 1 if content else 0
    char_count = len(content)

    metadata = {
        "overwrite": overwrite and file_exists,
        "new_file": not file_exists,
        "line_count": line_count,
        "char_count": char_count,
        "has_lint_errors": lint_errors is not None,
    }

    if lint_errors:
        msg = (
            f"File {file_path} created, but linter/type-checker reported issues:\n"
            f"{lint_errors}"
        )
    else:
        msg = f"File {file_path} created and passed linting/type checking"

    return msg, metadata
