import os

from .tree import DEFAULT_IGNORE_PATTERNS, _should_ignore

__all__ = ["ls"]


def ls(
    dir_path: str | None = None,
    ignore: list[str] | None = None,
    recursive: bool = False,
) -> str:
    """List directory contents.

    Parameters
    ----------
    dir_path:
        Absolute path to the directory to inspect. Defaults to the current working
        directory.
    ignore:
        Extra glob patterns to exclude from the listing in addition to the default
        *DEFAULT_IGNORE_PATTERNS*.
    recursive:
        When *True*, list contents recursively (similar to `ls -R`). When
        *False* (default), only the direct children of *dir_path* are returned.
    """

    dir_path = dir_path or os.getcwd()

    if not os.path.isdir(dir_path):
        raise ValueError(f"Not a directory: {dir_path}")

    ignore_patterns = list(DEFAULT_IGNORE_PATTERNS)
    if ignore:
        ignore_patterns.extend(ignore)

    lines: list[str] = []

    if recursive:
        # Walk the directory tree and build relative paths for every entry.
        for current_root, dirs, files in os.walk(dir_path, topdown=True):
            # Respect ignore rules by mutating *dirs* in-place so os.walk skips them
            dirs[:] = [d for d in dirs if not _should_ignore(d, ignore_patterns)]
            files = [f for f in files if not _should_ignore(f, ignore_patterns)]

            rel_root = os.path.relpath(current_root, dir_path)
            rel_root = "" if rel_root == "." else rel_root
            prefix = f"{rel_root}{os.sep}" if rel_root else ""

            # Files first (alphabetical), then dirs with trailing slash for clarity
            for name in sorted(files):
                lines.append(prefix + name)
            for dname in sorted(dirs):
                lines.append(prefix + dname + os.sep)
    else:
        # Only list the immediate children of *dir_path*.
        entries = [
            entry.name + (os.sep if entry.is_dir() else "")
            for entry in os.scandir(dir_path)
            if not _should_ignore(entry.name, ignore_patterns)
        ]
        lines.extend(sorted(entries))

    return "\n".join(lines)
