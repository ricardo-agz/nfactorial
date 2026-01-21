import os
from pathlib import Path

from ..utils import build_full_file_preview, build_preview, replace_block, run_linter


def find_replace(
    file_path: str,
    old_string: str,
    new_string: str,
    *,
    replace_all: bool = False,
    fuzzy: bool = True,
) -> tuple[str, dict[str, int]]:
    """Replace *old_string* with *new_string* in *file_path*.

    **Authoring guidelines**
    - *old_string* **must** be copied verbatim from the source - do not add
      escape sequences.
    - *new_string* is inserted **verbatim** Do not introduce additional
      python-style escape sequences.
    - Set *replace_all* to *True* only when you are confident that every match
      should be changed - otherwise the first occurrence is safer.
    - If *fuzzy* is *True*, whitespace-insensitive matching is used and minor
      formatting changes do not prevent a hit. When *replace_all* is *False*
      only the first occurrence is substituted.
    """
    file_path = os.path.abspath(file_path)
    if old_string == new_string:
        raise ValueError("new_string must differ from old_string")

    p = Path(file_path)
    if not p.exists():
        raise FileNotFoundError(file_path)
    if p.is_dir():
        raise IsADirectoryError(file_path)

    original = p.read_text(encoding="utf-8", errors="ignore")

    new_content, count = replace_block(
        original,
        old_string,
        new_string,
        replace_all=replace_all,
        fuzzy=fuzzy,
    )

    p.write_text(new_content, encoding="utf-8")

    lint_errors = run_linter(file_path)

    # Build preview of the final content
    final_lines = new_content.split("\n")
    FILE_PREVIEW_LENGTH = 1000
    if len(final_lines) > FILE_PREVIEW_LENGTH:
        # Find first occurrence of new_string to center preview around
        first_match_line = 1
        for i, line in enumerate(final_lines):
            if new_string in line:
                first_match_line = i + 1
                break
        preview = build_preview(final_lines, first_match_line, padding=25)
    else:
        # Show full file with line numbers
        preview = build_full_file_preview(final_lines)

    mode = "fuzzy" if fuzzy else "strict"
    msg = (
        f"Successfully replaced {count} occurrence{'s' if count != 1 else ''}"
        f" ({mode}) in {file_path}. New file contents:\n{preview}"
    )
    if lint_errors:
        msg += f"\nLinter warnings:\n{lint_errors}"

    metadata = {
        "occurrences_replaced": count,
        "replace_all": replace_all,
        "fuzzy": fuzzy,
    }

    return msg, metadata
