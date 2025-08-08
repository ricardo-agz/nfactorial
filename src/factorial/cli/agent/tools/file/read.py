import os
from typing import Any
from ..utils import run_linter, with_line_numbers


DIAGNOSTICS_EXTENSIONS = [".py", ".js", ".ts", ".jsx", ".tsx", ".css"]


def read(file_path: str, show_line_numbers: bool = True) -> tuple[str, dict[str, Any]]:
    """
    Reads a file and returns its contents and diagnostics for js, jsx, ts, tsx, css, and py code files.

    Arguments:
    file_path: The path to the file to read
    show_line_numbers: Whether to show line numbers in the file contents
        - **true** → each line is prefixed like `[42] const foo = "bar"`. Useful to know exactly which line numbers to edit.
        - **false** → raw text with no prefixes (easier to copy-paste into edits).
    """
    if not os.path.exists(file_path):
        return f"Error: File {file_path} does not exist", {"error": "File not found"}

    with open(file_path, "r") as f:
        content = f.read()

    file_size = len(content)
    line_count = content.count("\n") + 1 if content else 0

    diagnostics = None
    if os.path.splitext(file_path)[1].lower() in DIAGNOSTICS_EXTENSIONS:
        diagnostics = run_linter(file_path)

    if show_line_numbers:
        content = with_line_numbers(content)

    if diagnostics:
        content = f"{content}\n\nDiagnostics:\n{diagnostics}"

    metadata = {
        "file_size": file_size,
        "line_count": line_count,
        "has_diagnostics": diagnostics is not None,
        "show_line_numbers": show_line_numbers,
    }

    return content, metadata
