import os
import fnmatch
import asyncio

from factorial.llms import MultiClient, gpt_5_nano
from ..utils import line_count


# Default patterns to ignore (common build artifacts and hidden directories)
DEFAULT_IGNORE_PATTERNS: list[str] = [
    "node_modules",
    ".git",
    "dist",
    "build",
    ".next",
    ".nuxt",
    "coverage",
    ".cache",
    "__pycache__",
    "target",
    "vendor",
    "bin",
    "obj",
    ".idea",
    ".vscode",
    "logs",
    ".venv",
    "venv",
    "env",
    "tmp",
    "temp",
]

_CODE_FILE_EXTENSIONS: set[str] = {".js", ".ts", ".jsx", ".tsx", ".py"}


async def _llm_description_from_snippet(snippet: str) -> str | None:
    """Generate a short description using *gpt_5_nano*."""
    try:
        client = MultiClient()
        resp = await client.completion(
            model=gpt_5_nano,
            messages=[
                {
                    "role": "system",
                    "content": "Your task is to analyze a source code file and generate a concise 10-15 word description of its purpose. Respond with only the description; no preamble or postamble.",
                },
                {
                    "role": "user",
                    "content": snippet,
                },
            ],
            temperature=0.3,
            max_completion_tokens=40,
        )
        return resp.choices[0].message.content.strip() if resp.choices else None
    except Exception:
        return None


def _should_ignore(name: str, patterns: list[str]) -> bool:
    """Return True if *name* matches one of the *patterns* (supports basic globs)."""
    return any(fnmatch.fnmatch(name, pattern) for pattern in patterns)


async def _description(file_path: str) -> str | None:
    """Generate a short 10-15 word description for *file_path* using GPT-5-nano.

    Falls back to *None* on any error.
    """
    try:
        with open(file_path, "r", encoding="utf-8", errors="ignore") as fh:
            snippet = fh.read(2500)

        if not snippet.strip():
            return None

        return await _llm_description_from_snippet(snippet)
    except Exception:
        return None


def _collect_code_files(dir_path: str, ignore_patterns: list[str]) -> list[str]:
    paths: list[str] = []
    for current_root, dirs, files in os.walk(dir_path, topdown=True):
        # Filter dirs in-place to respect ignore rules
        dirs[:] = [d for d in dirs if not _should_ignore(d, ignore_patterns)]
        for file_name in files:
            if _should_ignore(file_name, ignore_patterns):
                continue
            ext = os.path.splitext(file_name)[1].lower()
            if ext in _CODE_FILE_EXTENSIONS:
                paths.append(os.path.join(current_root, file_name))
    return paths


def _build_tree_lines(
    dir_path: str,
    ignore_patterns: list[str],
    descriptions: dict[str, str | None],
    prefix: str = "",
) -> list[str]:
    try:
        entries = [
            entry
            for entry in os.scandir(dir_path)
            if not _should_ignore(entry.name, ignore_patterns)
        ]
    except PermissionError:
        # Skip directories we cannot access
        return []

    # Directories first, then files, both sorted alphabetically
    entries.sort(key=lambda e: (not e.is_dir(), e.name.lower()))

    lines: list[str] = []

    for idx, entry in enumerate(entries):
        is_last = idx == len(entries) - 1
        connector = "└── " if is_last else "├── "
        new_prefix = prefix + ("    " if is_last else "│   ")
        full_path = entry.path

        if entry.is_dir():
            lines.append(f"{prefix}{connector}{entry.name}/")
            lines.extend(
                _build_tree_lines(full_path, ignore_patterns, descriptions, new_prefix)
            )
        else:
            line = f"{prefix}{connector}{entry.name}"
            ext = os.path.splitext(entry.name)[1].lower()
            if ext in _CODE_FILE_EXTENSIONS:
                comment_parts: list[str] = []
                line_cnt = line_count(full_path)
                if line_cnt:
                    comment_parts.append(f"{line_cnt} lines")
                desc = descriptions.get(full_path)
                if desc:
                    comment_parts.append(desc)
                if comment_parts:
                    line += f"       // {' - '.join(comment_parts)}"
            lines.append(line)
    return lines


async def tree(dir_path: str | None = None, ignore: list[str] | None = None) -> str:
    """Lists the directory contents in a visual tree format. It works similarly to the Unix `tree` command but with the following enhancements:
    * Appends a concise description (~15 words) after every js, ts, jsx, tsx, css, md, and py file.
    * Ignores common bulky or transient directories like `venv`, `node_modules`, and `__pycache__` or any other directories specified in the `.gitignore` file.

    Parameters
    ----------
    dir_path:
        Absolute path to the directory to inspect. Defaults to the current working directory.
    ignore:
        Additional glob patterns to exclude from the listing.
    """
    dir_path = dir_path or os.getcwd()

    if not os.path.isdir(dir_path):
        raise ValueError(f"Not a directory: {dir_path}")

    ignore_patterns = list(DEFAULT_IGNORE_PATTERNS)
    if ignore:
        ignore_patterns.extend(ignore)

    code_files = _collect_code_files(dir_path, ignore_patterns)
    descriptions = dict(
        zip(code_files, await asyncio.gather(*[_description(fp) for fp in code_files]))
    )

    root_line = dir_path.rstrip(os.sep) + os.sep
    tree_lines = [root_line]
    tree_lines.extend(_build_tree_lines(dir_path, ignore_patterns, descriptions))

    return "\n".join(tree_lines)
