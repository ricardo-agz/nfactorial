import json
import asyncio
from pathlib import Path
from factorial.llms import MultiClient, gpt_41_mini, grok_3_mini, claude_35_haiku
from factorial.logging import get_logger
from .tools.utils import with_line_numbers

logger = get_logger(__name__)


MAX_FILES = 15
MAX_FILE_CHARS = 40000


async def prefetch(
    client: MultiClient,
    cwd: Path,
    tree_str: str,
    prompt: str,
    *,
    max_preview_lines: int = 750,
) -> tuple[list[Path], str]:
    """
    Prefetch the files that are most relevant to the prompt.
    """
    file_paths = await _fetch_relevant_files(client, cwd, tree_str, prompt)
    relevant_paths = await _filter_relevant_files(
        client, cwd, tree_str, prompt, file_paths
    )

    def _read_first_n_lines(path: Path, n: int) -> str:
        try:
            lines: list[str] = []
            with open(path, "r", encoding="utf-8", errors="ignore") as handle:
                for idx, line in enumerate(handle):
                    if idx >= n:
                        break
                    lines.append(line.rstrip("\n"))
            return "\n".join(lines)
        except Exception:
            return ""

    sections: list[str] = []
    for p in relevant_paths:
        preview = _read_first_n_lines(p, max_preview_lines)
        numbered = with_line_numbers(preview)
        sections.append(
            f'<file path="{str(p.resolve())}" first_lines="{max_preview_lines}">\n{numbered}\n</file>'
        )
    preview_blob = "\n\n".join(sections)

    return relevant_paths, preview_blob


fetch_relevant_files_prompt = """
You are assisting an AI coding agent. Given the task and the project tree, select a SMALL, high-signal set of files that the agent should read BEFORE acting.

Strict requirements:
- Only return files that materially help understand or change the code to complete the task.
- Prefer source files, configs, schemas, tests touching the target area.
- Avoid build artifacts, lockfiles, large assets, and generated files.
- Cap the result to at most {MAX_FILES} files.
- Paths MUST be within the current working directory: {cwd}
- Return absolute paths that exist on disk. Do not include directories.

<task>
{prompt}
</task>

<project_tree cwd="{cwd}">
{tree_str}
</project_tree>
"""

filter_relevant_files_prompt = """
Decide if the file below should be INCLUDED in the agent's initial context for the task.

Inclusion criteria (answer True if ANY of the following apply):
- The file directly informs understanding of the task.
- The file is likely to be edited to complete the task.

Exclude if:
- It is unrelated boilerplate, generic utilities, build artifacts, vendored code, or large assets.
- It is only tangentially related but not necessary to read to complete the task.

Return True to include, False to exclude. Use the tool to answer.

<task>
{prompt}
</task>

<file path="{file_path}">
{file_content}
</file>
"""


async def _fetch_relevant_files(
    client: MultiClient, cwd: Path, tree_str: str, prompt: str
) -> list[Path]:
    """
    Fetch the files that are most relevant to the prompt.
    """
    response_tool = {
        "type": "function",
        "function": {
            "name": "output",
            "description": "Return a list of relevant ABSOLUTE file paths.",
            "parameters": {
                "type": "object",
                "properties": {
                    "file_paths": {
                        "type": "array",
                        "items": {
                            "type": "string",
                        },
                        "description": "The absolute file paths.",
                    },
                },
            },
        },
    }
    message_text = fetch_relevant_files_prompt.format(
        prompt=prompt, tree_str=tree_str, cwd=str(cwd.resolve()), MAX_FILES=MAX_FILES
    )

    async def _try_models() -> dict:
        for model in (gpt_41_mini, claude_35_haiku, grok_3_mini):
            try:
                response = await client.completion(
                    model=model,
                    messages=[{"role": "user", "content": message_text}],
                    tools=[response_tool],
                    tool_choice="required",
                )
                args = json.loads(
                    response.choices[0].message.tool_calls[0].function.arguments
                )
                return args
            except Exception:
                continue
        logger.warning("Failed to fetch relevant files")
        return {"file_paths": []}

    args = await _try_models()

    resolved_cwd = cwd.resolve()
    selected: list[Path] = []
    seen: set[str] = set()
    for fp in args.get("file_paths", []) or []:
        try:
            p = Path(fp)
            if not p.is_absolute():
                p = resolved_cwd / p
            p = p.resolve()
            if not str(p).startswith(str(resolved_cwd)):
                continue
            if p.is_file():
                sp = str(p)
                if sp not in seen:
                    seen.add(sp)
                    selected.append(p)
        except Exception:
            continue

    return selected[:MAX_FILES]


async def _filter_relevant_files(
    client: MultiClient, cwd: Path, tree_str: str, prompt: str, file_paths: list[Path]
) -> list[Path]:
    """
    Filter the files that are most relevant to the prompt.
    """
    response_tool = {
        "type": "function",
        "function": {
            "name": "output",
            "description": "Return a boolean indicating if the file is relevant to the prompt.",
            "parameters": {
                "type": "object",
                "properties": {
                    "is_relevant": {
                        "type": "boolean",
                        "description": "Whether the file is relevant to the prompt.",
                    },
                },
            },
        },
    }

    if not file_paths:
        return []

    semaphore = asyncio.Semaphore(8)

    async def _read_file_truncated(path: Path) -> str:
        try:
            text = path.read_text(encoding="utf-8", errors="ignore")
            if len(text) > MAX_FILE_CHARS:
                head = text[: MAX_FILE_CHARS // 2]
                tail = text[-MAX_FILE_CHARS // 2 :]
                return f"""[TRUNCATED START]\n{head}\n...\n[TRUNCATED END]\n{tail}"""
            return text
        except Exception:
            return ""

    async def _check(path: Path) -> tuple[Path, bool]:
        async with semaphore:
            file_content = await _read_file_truncated(path)
            message_text = filter_relevant_files_prompt.format(
                prompt=prompt, file_path=str(path), file_content=file_content
            )

            async def _try_models() -> bool:
                for model in (gpt_41_mini, claude_35_haiku, grok_3_mini):
                    try:
                        response = await client.completion(
                            model=model,
                            messages=[{"role": "user", "content": message_text}],
                            tools=[response_tool],
                            tool_choice="required",
                        )
                        args = json.loads(
                            response.choices[0].message.tool_calls[0].function.arguments
                        )
                        return bool(args.get("is_relevant", False))
                    except Exception:
                        continue
                return False

            is_rel = await _try_models()
            return (path, is_rel)

    tasks = [asyncio.create_task(_check(p)) for p in file_paths]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    relevant: list[Path] = []
    for res in results:
        if isinstance(res, Exception):
            continue
        path, is_rel = res
        if is_rel:
            relevant.append(path)

    return relevant
