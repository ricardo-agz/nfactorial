import os
import re

from .tree import DEFAULT_IGNORE_PATTERNS, _should_ignore

__all__ = ["grep"]


def grep(
    pattern: str,
    dir_path: str | None = None,
    *,
    max_matches: int = 100,
    ignore: list[str] | None = None,
) -> str:
    """Search for *pattern* (regular expression) in files under *dir_path*.

    Parameters
    ----------
    pattern:
        Regular expression to look for.
    dir_path:
        Absolute directory path to search in. Defaults to current working
        directory.
    max_matches:
        Stop after this many matches have been found. Defaults to 100 to keep the
        output manageable.
    ignore:
        Extra glob patterns to exclude (in addition to *DEFAULT_IGNORE_PATTERNS*).
    """

    dir_path = dir_path or os.getcwd()

    if not os.path.isdir(dir_path):
        raise ValueError(f"Not a directory: {dir_path}")

    try:
        regex = re.compile(pattern)
    except re.error as exc:
        return f"Invalid regular expression: {exc}"

    ignore_patterns = list(DEFAULT_IGNORE_PATTERNS)
    if ignore:
        ignore_patterns.extend(ignore)

    # Basic binary file detection based on extension.
    binary_exts = {
        ".png",
        ".jpg",
        ".jpeg",
        ".gif",
        ".ico",
        ".svg",
        ".pyc",
        ".o",
        ".so",
        ".dylib",
    }

    matches: list[str] = []

    for current_root, dirs, files in os.walk(dir_path, topdown=True):
        dirs[:] = [d for d in dirs if not _should_ignore(d, ignore_patterns)]
        for fname in files:
            if _should_ignore(fname, ignore_patterns):
                continue
            if any(fname.endswith(ext) for ext in binary_exts):
                continue

            file_path = os.path.join(current_root, fname)
            try:
                with open(file_path, "r", encoding="utf-8", errors="ignore") as fh:
                    for lineno, line in enumerate(fh, 1):
                        if regex.search(line):
                            rel_path = os.path.relpath(file_path, dir_path)
                            matches.append(f"{rel_path}:{lineno}: {line.rstrip()}")
                            if len(matches) >= max_matches:
                                result = "\n".join(matches)
                                return result
            except (OSError, UnicodeDecodeError):
                # Skip unreadable/binary files safely.
                continue

    return "\n".join(matches) if matches else f"No matches found for pattern: {pattern}"
