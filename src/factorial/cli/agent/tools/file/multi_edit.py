import os
from pathlib import Path
from pydantic import BaseModel
from ..utils import run_linter, replace_block


def _apply_single(
    content: str,
    *,
    start_line: int,
    end_line: int | None,
    tolerance: int,
    old_string: str,
    new_string: str,
) -> str:
    """Helper that applies a single edit to *content* and returns new content."""
    lines = content.split("\n")
    total_lines = len(lines)

    if start_line < 1 or start_line > total_lines:
        raise ValueError("start_line out of range in multi_edit")

    end_line = end_line if end_line is not None else start_line
    if end_line < start_line:
        raise ValueError("end_line < start_line in multi_edit")
    if old_string == new_string:
        raise ValueError("new_string must differ from old_string (multi_edit)")
    if tolerance < 0:
        raise ValueError("tolerance must be >= 0")

    search_start = max(1, start_line - tolerance)
    search_end = min(total_lines, end_line + tolerance)

    slice_str = "\n".join(lines[search_start - 1 : search_end])
    if old_string not in slice_str:
        raise ValueError("old_string not found within specified range in multi_edit")

    new_slice, _ = replace_block(
        slice_str,
        old_string,
        new_string,
        replace_all=False,
        fuzzy=True,
    )
    new_lines = new_slice.split("\n")
    updated_lines = lines[: search_start - 1] + new_lines + lines[search_end:]
    return "\n".join(updated_lines)


class Edit(BaseModel):
    start_line: int
    end_line: int | None
    tolerance: int
    old_string: str
    new_string: str


def multi_edit(
    file_path: str,
    edits: list[Edit],
) -> tuple[str, dict[str, int]]:
    """
    Apply *edits* to *file_path* sequentially.

    Arguments:
    file_path: The absolute filepath to the file to edit
    edits: A list of edits to apply to the file. Each edit must follow the same requirements as the edit_lines tool.
    """
    file_path = os.path.abspath(file_path)
    if not edits:
        raise ValueError("edits array cannot be empty")

    p = Path(file_path)
    new_file = False
    if not p.exists():
        raise FileNotFoundError(file_path)
    else:
        if p.is_dir():
            raise IsADirectoryError(file_path)
        content = p.read_text(encoding="utf-8", errors="ignore")

    # Apply edits sequentially, keeping track of current content length so later
    # edits still refer to original coordinates.
    line_offset = 0
    total_lines_added = 0
    total_lines_removed = 0

    for idx, edit in enumerate(edits, 1):
        start = edit.start_line
        end = edit.end_line
        tol = edit.tolerance
        old = edit.old_string
        new = edit.new_string

        # Count line changes for this edit
        old_line_count = old.count("\n") + 1
        new_line_count = new.count("\n") + 1
        edit_lines_added = max(0, new_line_count - old_line_count)
        edit_lines_removed = max(0, old_line_count - new_line_count)

        total_lines_added += edit_lines_added
        total_lines_removed += edit_lines_removed

        # Adjust start/end by current offset so that original coordinates work.
        adj_start = start + line_offset
        adj_end = (end if end is not None else start) + line_offset

        content_before = content
        content = _apply_single(
            content,
            start_line=adj_start,
            end_line=adj_end,
            tolerance=tol,
            old_string=old,
            new_string=new,
        )
        # Update offset for next edits
        delta = content.count("\n") - content_before.count("\n")
        line_offset += delta

    # Ensure directory exists for new files
    if new_file:
        p.parent.mkdir(parents=True, exist_ok=True)

    p.write_text(content, encoding="utf-8")

    lint_errors = run_linter(file_path)

    msg = f"Successfully applied {len(edits)} edits to {file_path}." + (
        " (file created)" if new_file else ""
    )
    if lint_errors:
        msg += f" Linter warnings:\n{lint_errors}"

    metadata = {
        "num_edits": len(edits),
        "total_lines_added": total_lines_added,
        "total_lines_removed": total_lines_removed,
        "new_file": new_file,
    }

    return msg, metadata
