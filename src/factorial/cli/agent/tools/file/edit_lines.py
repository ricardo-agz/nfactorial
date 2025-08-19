import os
from pathlib import Path

from ..utils import run_linter, replace_block, build_preview, build_full_file_preview

__all__ = ["edit_lines"]


def edit_lines(
    file_path: str,
    *,
    start_line: int,
    end_line: int | None = None,
    old_string: str,
    new_string: str,
    tolerance: int = 1,
) -> tuple[str, dict[str, int]]:
    """Replace *old_string* with *new_string* in a given line window.

    **Important authoring guidelines**
    1. *old_string* **must match the text in the target file exactly** - copy it
       verbatim from the source (including indentation).
    2. *new_string* is inserted **verbatim** Do not introduce additional python-style escape sequences.
    3. Keep replacements minimal: only the changed lines should differ between *old_string* and *new_string*.
    4. `tolerance`: number of **extra lines** that the tool is allowed to look **above _and_ below** the `[start_line, end_line]` window when searching for `old_string`.
        * With at tolerance of 0, the start_line - end_line slice must fully encompass **every line** of `old_string`. If even one character is outside that window, the replacement fails.
        * For a more forgiving search, increase tolerance for the tool to automatically widen the search window.
        * The default tolerance is 1.
    5. The *start_line* / *end_line* coordinates refer to the ORIGINAL file.
       This function automatically adjusts subsequent edits as earlier ones
       change line numbers.

    Parameters
    ----------
    file_path: str
        Absolute path of the file to edit.
    start_line: int
        1-based line where the edit window starts.
    end_line: int | None
        Last line (inclusive).  Defaults to *start_line*.
    old_string: str
        Text to be replaced (must appear **fully** inside the window widened by
        *tolerance*).
    new_string: str
        Replacement text (must differ).
    tolerance: int
        Extra lines above and below the window that are searched as well.
    """

    file_path = os.path.abspath(file_path)

    try:
        start_line = int(start_line)
    except Exception as exc:
        raise TypeError("start_line must be an integer") from exc

    if end_line is not None:
        try:
            end_line = int(end_line)
        except Exception as exc:
            raise TypeError("end_line must be an integer when provided") from exc

    try:
        tolerance = int(tolerance)
    except Exception as exc:
        raise TypeError("tolerance must be an integer") from exc

    if start_line < 1:
        raise ValueError("start_line must be >= 1")
    if end_line is not None and end_line < start_line:
        raise ValueError("end_line cannot be before start_line")
    if old_string == new_string:
        raise ValueError("new_string must differ from old_string")
    if tolerance < 0:
        raise ValueError("tolerance must be >= 0")

    end_line = end_line or start_line

    p = Path(file_path)
    if not p.exists():
        raise FileNotFoundError(file_path)
    if p.is_dir():
        raise IsADirectoryError(file_path)

    lines = p.read_text(encoding="utf-8", errors="ignore").split("\n")
    total_lines = len(lines)
    if start_line > total_lines:
        raise ValueError("start_line exceeds file length")

    search_start = max(1, start_line - tolerance)
    search_end = min(total_lines, end_line + tolerance)

    slice_str = "\n".join(lines[search_start - 1 : search_end])
    if old_string not in slice_str:
        preview = build_preview(lines, start_line, padding=25)
        raise ValueError(
            f"old_string not found in lines {search_start}-{search_end}. "
            f"File context:\n{preview}"
        )

    # Count line changes
    old_line_count = old_string.count("\n") + 1
    new_line_count = new_string.count("\n") + 1
    lines_added = max(0, new_line_count - old_line_count)
    lines_removed = max(0, old_line_count - new_line_count)

    # Apply replacement (first occurrence only)
    new_slice_str, _ = replace_block(
        slice_str,
        old_string,
        new_string,
        replace_all=False,
        fuzzy=True,
    )

    new_lines = new_slice_str.split("\n")
    updated_lines = lines[: search_start - 1] + new_lines + lines[search_end:]

    p.write_text("\n".join(updated_lines), encoding="utf-8")

    lint_errors = run_linter(file_path)

    # Build preview of the final content
    final_lines = updated_lines
    FILE_PREVIEW_LENGTH = 1000
    if len(final_lines) > FILE_PREVIEW_LENGTH:
        # Show preview around the edited area
        preview = build_preview(final_lines, search_start, padding=25)
    else:
        # Show full file with line numbers
        preview = build_full_file_preview(final_lines)

    msg = (
        f"Successfully edited lines {search_start}-{search_end} in {file_path}. "
        f"Replaced 1 occurrence. New file contents:\n{preview}"
    )
    if lint_errors:
        msg += f"\nLinter warnings:\n{lint_errors}"

    metadata = {
        "lines_changed": 1,
        "lines_added": lines_added,
        "lines_removed": lines_removed,
        "start_line": search_start,
        "end_line": search_end,
    }

    return msg, metadata
