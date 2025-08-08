import os
from pathlib import Path

from ..utils import run_linter, replace_block


def find_replace(
    file_path: str,
    old_string: str,
    new_string: str,
    *,
    replace_all: bool = False,
    fuzzy: bool = True,
) -> tuple[str, dict[str, int]]:
    """Replace *old_string* with *new_string* in *file_path*.

    If *fuzzy* is *True*, whitespace-insensitive matching is used and minor
    formatting changes do not prevent a hit.  When *replace_all* is *False* only
    the first occurrence is substituted.
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

    msg = (
        f"Successfully replaced {count} occurrence{'s' if count != 1 else ''}"
        f" ({'fuzzy' if fuzzy else 'strict'}) in {file_path}."
    )
    if lint_errors:
        msg += f" Linter warnings:\n{lint_errors}"

    metadata = {
        "occurrences_replaced": count,
        "replace_all": replace_all,
        "fuzzy": fuzzy,
    }

    return msg, metadata
