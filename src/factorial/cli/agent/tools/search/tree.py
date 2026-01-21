import asyncio
import fnmatch
import os
import re

from factorial.llms import MultiClient, gpt_41_nano
from factorial.logging import get_logger

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

_CODE_FILE_EXTENSIONS: set[str] = {
    ".js",
    ".ts",
    ".jsx",
    ".tsx",
    ".py",
    ".css",
    ".md",
    ".vue",
    ".svelte",
    ".rs",
    ".java",
    ".kt",
    ".go",
    ".sql",
    ".toml",
    ".ini",
    ".yaml",
    ".yml",
    ".html",
    ".sh",
    ".bash",
    ".zsh",
    ".json",
}


logger = get_logger(__name__)


async def _llm_description_from_snippet(snippet: str) -> str | None:
    """Generate a short description using *gpt_5_nano*."""
    try:
        client = MultiClient()
        resp = await client.completion(
            model=gpt_41_nano,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "Your task is to analyze a source code file and generate "
                        "a concise 10-15 word description of its purpose. "
                        "Respond with only the description; no preamble or postamble."
                    ),
                },
                {
                    "role": "user",
                    "content": snippet,
                },
            ],
            max_completion_tokens=100,
        )
        return resp.choices[0].message.content.strip()
    except Exception:
        return None
    finally:
        try:
            await client.close()  # type: ignore[possibly-undefined]
        except Exception:
            pass


def _should_ignore(name: str, patterns: list[str]) -> bool:
    """Return True if *name* matches one of the *patterns* (supports basic globs)."""
    return any(fnmatch.fnmatch(name, pattern) for pattern in patterns)


def _heuristic_description(file_path: str, snippet: str) -> str | None:
    """Generate a lightweight, local description when LLM is unavailable.

    The goal is to produce a short, useful summary without external dependencies.
    Returns None if nothing reasonable can be determined.
    """
    try:
        ext = os.path.splitext(file_path)[1].lower()
        filename = os.path.basename(file_path)
        text = snippet.replace("\r\n", "\n").replace("\r", "\n")

        if ext == ".md":
            for line in text.split("\n"):
                stripped = line.strip()
                if stripped.startswith("#"):
                    return stripped.strip()
            return None

        if ext in {".css", ".scss", ".sass", ".less", ".styl"}:
            descriptions = []

            # Check for CSS framework usage
            if any(
                framework in text.lower() for framework in ["tailwind", "@tailwind"]
            ):
                descriptions.append("Tailwind CSS")
            elif "bootstrap" in text.lower():
                descriptions.append("Bootstrap")
            elif "@media" in text:
                descriptions.append("responsive design")

            # Count rule blocks
            rule_count = text.count("{")
            if rule_count:
                descriptions.append(f"~{rule_count} rule blocks")

            if "@keyframes" in text or "animation:" in text:
                descriptions.append("animations")

            return " - ".join(descriptions) if descriptions else None

        if ext == ".py":
            descriptions = []

            docstring_match = re.search(
                r"^\s*([\"\']{3})([\s\S]*?)\1", text, re.MULTILINE
            )
            if docstring_match:
                summary = docstring_match.group(2).strip().split("\n")[0]
                if summary and len(summary) > 10:
                    return summary[:120]

            if "from django" in text or "import django" in text:
                descriptions.append("Django")
            elif "from flask" in text or "import flask" in text:
                descriptions.append("Flask")
            elif "from fastapi" in text or "import fastapi" in text:
                descriptions.append("FastAPI")
            elif "import asyncio" in text or "async def" in text:
                descriptions.append("async/await")
            elif "import pytest" in text or "def test_" in text:
                descriptions.append("tests")

            class_names = re.findall(
                r"^class\s+([A-Za-z_][A-Za-z0-9_]*)", text, re.MULTILINE
            )
            func_names = re.findall(
                r"^(?:async\s+)?def\s+([A-Za-z_][A-Za-z0-9_]*)", text, re.MULTILINE
            )

            if class_names:
                descriptions.append(f"classes: {', '.join(class_names[:3])}")
            if func_names:
                descriptions.append(f"functions: {', '.join(func_names[:3])}")

            return " | ".join(descriptions) if descriptions else None

        if ext in {".js", ".ts", ".jsx", ".tsx", ".vue", ".svelte"}:
            descriptions = []

            if (
                "import React" in text
                or "from 'react'" in text
                or ext in {".jsx", ".tsx"}
            ):
                descriptions.append("React")
                if re.search(r"use[A-Z]\w*", text):
                    descriptions.append("hooks")
                if re.search(
                    r"export\s+(?:default\s+)?(?:function|const)\s+[A-Z]", text
                ):
                    descriptions.append("component")
            elif "import Vue" in text or "from 'vue'" in text or ext == ".vue":
                descriptions.append("Vue")
            elif ext == ".svelte":
                descriptions.append("Svelte")
            elif "import express" in text or "require('express')" in text:
                descriptions.append("Express.js")
            elif "import next" in text or "from 'next'" in text:
                descriptions.append("Next.js")

            if ext in {".ts", ".tsx"}:
                if "interface " in text or "type " in text:
                    descriptions.append("types")

            main_exports = []
            for pattern in [
                r"export\s+default\s+(?:function\s+)?([A-Za-z_][A-Za-z0-9_]*)",
                r"export\s+(?:const|function)\s+([A-Za-z_][A-Za-z0-9_]*)",
                r"function\s+([A-Za-z_][A-Za-z0-9_]*)\s*\(",
                r"const\s+([A-Za-z_][A-Za-z0-9_]*)\s*=",
            ]:
                matches = re.findall(pattern, text)
                main_exports.extend(matches)
                if len(main_exports) >= 3:
                    break

            if main_exports:
                descriptions.append(f"exports: {', '.join(main_exports[:3])}")

            return " | ".join(descriptions) if descriptions else None

        if ext == ".sql":
            if re.search(r"CREATE\s+TABLE", text, re.IGNORECASE):
                return "Database schema definition"
            elif re.search(r"INSERT\s+INTO", text, re.IGNORECASE):
                return "Database data insertion"
            elif re.search(r"SELECT.*FROM", text, re.IGNORECASE):
                return "Database query"
            else:
                return "SQL script"

        if ext in {".toml", ".ini"}:
            if "pyproject.toml" in filename:
                return "Python project configuration"
            elif "cargo.toml" in filename:
                return "Rust project configuration"
            else:
                return f"{ext[1:].upper()} configuration"

        if ext == ".go":
            if "package main" in text:
                return "Go main package"
            else:
                package_match = re.search(r"package\s+(\w+)", text)
                if package_match:
                    return f"Go package: {package_match.group(1)}"
                return "Go source"

        if ext == ".rs":
            if "fn main()" in text:
                return "Rust main binary"
            else:
                return "Rust source"

        if ext in {".java", ".kt"}:
            class_match = re.search(r"(?:public\s+)?class\s+(\w+)", text)
            lang = "Java" if ext == ".java" else "Kotlin"
            if class_match:
                return f"{lang} class: {class_match.group(1)}"
            return f"{lang} source"

        return None
    except Exception:
        return None


async def _description(file_path: str) -> str | None:
    """Generate a short 10-15 word description for *file_path* using GPT-5-nano.

    Returns both AI description and heuristic description when available.
    Falls back to *None* on any error.
    """
    try:
        with open(file_path, encoding="utf-8", errors="ignore") as fh:
            snippet = fh.read(2500)

        if not snippet.strip():
            return None

        llm_desc = await _llm_description_from_snippet(snippet)
        heuristic_desc = _heuristic_description(file_path, snippet)

        descriptions = []
        if llm_desc:
            descriptions.append(llm_desc)
        if heuristic_desc:
            descriptions.append(heuristic_desc)

        return " | ".join(descriptions) if descriptions else None
    except Exception:
        return None


def _collect_code_files(dir_path: str, ignore_patterns: list[str]) -> list[str]:
    paths: list[str] = []
    for current_root, dirs, files in os.walk(dir_path, topdown=True):
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
        return []

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
    """Lists the directory contents in a visual tree format.

    Works similarly to the Unix `tree` command but with enhancements:
    * Appends a concise description (~15 words) after every js, ts, jsx, tsx,
      css, md, and py file.
    * Ignores common bulky or transient directories like `venv`, `node_modules`,
      and `__pycache__` or any other directories specified in `.gitignore`.

    Parameters
    ----------
    dir_path:
        Absolute path to the directory to inspect. Defaults to the current
        working directory.
    ignore:
        Additional glob patterns to exclude from the listing.
    """
    input_path = dir_path
    effective_path = os.path.abspath(input_path or os.getcwd())

    if not os.path.isdir(effective_path):
        raise ValueError(f"Not a directory: {effective_path}")

    ignore_patterns = list(DEFAULT_IGNORE_PATTERNS)
    if ignore:
        ignore_patterns.extend(ignore)

    code_files = _collect_code_files(effective_path, ignore_patterns)
    description_results = await asyncio.gather(*[_description(fp) for fp in code_files])
    descriptions = dict(zip(code_files, description_results, strict=True))

    display_root = (input_path or ".").rstrip(os.sep) + os.sep
    root_line = display_root
    tree_lines = [root_line]
    tree_lines.extend(_build_tree_lines(effective_path, ignore_patterns, descriptions))

    return "\n".join(tree_lines)
