import fnmatch
import os

from .tree import DEFAULT_IGNORE_PATTERNS, _should_ignore

__all__ = ["glob"]


def glob(
    pattern: str,
    dir_path: str | None = None,
    *,
    ignore: list[str] | None = None,
) -> str:
    """Find files matching *pattern* under *dir_path* recursively.

    Parameters
    ----------
    pattern:
        POSIX-style glob pattern, as understood by :pymod:`fnmatch`.
    dir_path:
        Absolute directory to search. Defaults to the current working directory.
    ignore:
        Additional glob patterns to exclude besides *DEFAULT_IGNORE_PATTERNS*.
    """

    dir_path = dir_path or os.getcwd()

    if not os.path.isdir(dir_path):
        raise ValueError(f"Not a directory: {dir_path}")

    ignore_patterns = list(DEFAULT_IGNORE_PATTERNS)
    if ignore:
        ignore_patterns.extend(ignore)

    matches: list[str] = []

    for root, dirs, files in os.walk(dir_path, topdown=True):
        # Apply ignore rules – mutate *dirs* so walk skips them altogether
        dirs[:] = [d for d in dirs if not _should_ignore(d, ignore_patterns)]
        for fname in files:
            if _should_ignore(fname, ignore_patterns):
                continue
            rel_path = os.path.relpath(os.path.join(root, fname), dir_path)
            if fnmatch.fnmatch(rel_path, pattern):
                matches.append(rel_path)

    matches.sort()
    return "\n".join(matches)
