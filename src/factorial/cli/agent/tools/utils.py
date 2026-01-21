import json
import os
import re
import subprocess
from typing import Any


def run_linter(file_path: str) -> str | None:
    """
    Run an appropriate linter or syntax checker for *file_path* based on its
    extension. Falls back to a no-op when a suitable tool is unavailable.

    This function does NOT edit the file - it only returns diagnostic information
    about lint errors, type errors, or syntax issues. Returns a string with
    lint / type errors, or ``None`` when the file passes or when linting is skipped.
    """

    ext = os.path.splitext(file_path)[1].lower()

    # Python – reuse the Pyright helper defined above
    if ext in {".py", ".pyw"}:
        return run_pyright(file_path)

    # TypeScript / JavaScript – try eslint first, then tsc / node check
    if ext in {".ts", ".tsx", ".js", ".jsx"}:
        lint_cmds = [
            ["npx", "--yes", "eslint", file_path],
            ["npx", "--yes", "tsc", "--noEmit", "--pretty", "false", file_path],
            ["node", "--check", file_path],
        ]
        for cmd in lint_cmds:
            try:
                result = subprocess.run(cmd, capture_output=True, text=True)
                if result.returncode == 0:
                    return None
                # If we have output, return it - otherwise continue to next tool
                error_output = result.stdout or result.stderr
                if error_output:
                    return error_output
            except FileNotFoundError:
                continue
            except Exception:
                # Log unexpected errors but continue to next tool
                continue
        return None

    # CSS – if stylelint is available
    if ext == ".css":
        try:
            result = subprocess.run(
                ["npx", "--yes", "stylelint", file_path],
                capture_output=True,
                text=True,
            )
            if result.returncode == 0:
                return None
            return result.stdout or result.stderr
        except FileNotFoundError:
            return None

    # Unsupported file type – skip linting
    return None


def run_pyright(file_path: str) -> str | None:
    """Run *pyright* on *file_path* and return the error report if issues are found.

    The function tries to execute the *pyright* CLI with ``--outputjson`` so we can
    parse and return a concise error summary. If *pyright* is not available, we
    fall back to a simple syntax-check via ``python -m py_compile``. The returned
    string is ``None`` when no problems are detected or when linting is
    unavailable.
    """

    try:
        result = subprocess.run(
            ["pyright", file_path, "--outputjson"], capture_output=True, text=True
        )

        if result.returncode == 0:
            return None  # no errors

        # Attempt to pretty-print first 10 diagnostics
        try:
            data = json.loads(result.stdout or result.stderr)
            diagnostics = data.get("generalDiagnostics", [])
            if diagnostics:
                summary_lines: list[str] = []
                max_items = 10
                for d in diagnostics[:max_items]:
                    msg = d.get("message", "")
                    line = d.get("range", {}).get("start", {}).get("line", 0) + 1
                    summary_lines.append(f"Line {line}: {msg}")
                if len(diagnostics) > max_items:
                    summary_lines.append("...")
                return "\n".join(summary_lines)
        except Exception:
            # If JSON parsing fails, fall back to raw output
            pass

        return result.stdout or result.stderr or "Unknown pyright error"
    except FileNotFoundError:
        # pyright not installed – do a quick syntax check instead
        try:
            subprocess.check_output(["python", "-m", "py_compile", file_path])
            return None
        except subprocess.CalledProcessError as exc:
            return exc.output.decode() if isinstance(exc.output, bytes) else str(exc)


def line_count(file_path: str) -> int:
    try:
        with open(file_path, encoding="utf-8", errors="ignore") as handle:
            return sum(1 for _ in handle)
    except Exception:
        return 0


def with_line_numbers(code: str) -> str:
    return "\n".join([f"[{i + 1}]{line}" for i, line in enumerate(code.split("\n"))])


def build_preview(lines: list[str], start_line: int, padding: int = 25) -> str:
    """Build a line-numbered preview around a specific line range."""
    total_lines = len(lines)
    if total_lines == 0:
        return "[1] "

    # Ensure start_line is within bounds (1-based)
    start_line = max(1, min(start_line, total_lines))

    # Calculate preview window
    preview_start = max(1, start_line - padding)
    preview_end = min(total_lines, start_line + padding)

    preview_lines = []
    for i in range(preview_start - 1, preview_end):
        line_num = i + 1
        content = lines[i] if i < len(lines) else ""
        preview_lines.append(f"[{line_num}] {content}")

    return "\n".join(preview_lines)


def build_full_file_preview(lines: list[str]) -> str:
    """Build a line-numbered preview of the entire file."""
    return "\n".join([f"[{i + 1}] {line}" for i, line in enumerate(lines)])


_WHITESPACE_RE = re.compile(r"(?:\\s|\\n|\\r|\\t)+")


def _fuzzy_pattern(old: str) -> re.Pattern[str]:
    """Return a regex that matches *old* ignoring whitespace differences."""
    escaped = re.escape(old)
    # Normalize whitespace so that any sequence of whitespace **or** escaped whitespace
    # (e.g. " ", "\n", "\t", "\r", or "\s") is matched by the flexible
    # regex token ``\s+``.  This prevents the pattern generation step from
    # inadvertently touching other backslash-escaped characters such as escaped
    # quotes (``\"``) that should remain untouched.
    #
    # First, collapse *escaped* whitespace sequences to ``\s+`` – this handles
    # cases like ``\n`` or ``\t`` that appear after ``re.escape``.
    pattern_src = _WHITESPACE_RE.sub(r"\\s+", escaped)

    # Second, replace any remaining *literal* whitespace characters (spaces,
    # newlines, tabs) with ``\s+`` as well.
    pattern_src = re.sub(r"\s+", r"\\s+", pattern_src)
    return re.compile(pattern_src, flags=re.MULTILINE | re.DOTALL)


def replace_block(
    content: str,
    old: str,
    new: str,
    *,
    replace_all: bool = False,
    fuzzy: bool = True,
) -> tuple[str, int]:
    r"""Return *(new_content, replacements_made)* after replacing *old* with *new*.

    When *fuzzy* is *True*, contiguous whitespace in *old* is treated as a
    wildcard (``\s+``) so that indentation / line breaks do not affect the
    match.
    """
    if fuzzy:
        pattern = _fuzzy_pattern(old)
        max_count = 0 if replace_all else 1
        # Use a lambda so that the *new* string is inserted **literally** and
        # backslashes inside it are **not** interpreted by ``re.sub``.
        new_content, n = pattern.subn(lambda _: new, content, count=max_count)
    else:
        if replace_all:
            new_content, n = content.replace(old, new), content.count(old)
        else:
            if old not in content:
                n = 0
                new_content = content
            else:
                new_content = content.replace(old, new, 1)
                n = 1
    if n == 0:
        raise ValueError("old_string not found")
    return new_content, n


def count_occurrences(content: str, substring: str) -> int:
    """Count non-overlapping occurrences of *substring* in *content*."""
    return content.count(substring)


def run(cmd: list[str], **kwargs: Any) -> str:
    """Wrapper around subprocess.run that captures stdout/stderr."""
    completed = subprocess.run(
        cmd, capture_output=True, text=True, check=True, **kwargs
    )
    entry = [f"$ {' '.join(cmd)}"]
    if completed.stdout:
        entry.append("stdout:\n" + completed.stdout.strip())
    if completed.stderr:
        entry.append("stderr:\n" + completed.stderr.strip())
    return "\n".join(entry)
