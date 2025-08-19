import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

from .read import read


MAX_WORKERS = 8


def multi_read(
    file_paths: list[str],
    show_line_numbers: bool = True,
) -> tuple[str, dict[str, Any]]:
    """
    Read multiple files concurrently and return their contents and per-file metadata.

    Arguments:
    file_paths: A list of file paths to read
    show_line_numbers: Whether to show line numbers in each file's contents
        - true → each line is prefixed like "[42] const foo = \"bar\""
        - false → raw text with no prefixes (easier to copy-paste into edits)
    """

    if not file_paths:
        raise ValueError("file_paths cannot be empty")

    abs_paths: list[str] = [os.path.abspath(p) for p in file_paths]

    results: dict[int, tuple[str, dict[str, Any]]] = {}
    with ThreadPoolExecutor(max_workers=min(MAX_WORKERS, len(abs_paths))) as executor:
        future_to_index = {
            executor.submit(read, path, show_line_numbers): idx
            for idx, path in enumerate(abs_paths)
        }
        for future in as_completed(future_to_index):
            idx = future_to_index[future]
            try:
                content, metadata = future.result()
                results[idx] = (content, metadata)
            except Exception as exc:
                results[idx] = (
                    f"Error: Failed to read {abs_paths[idx]}: {exc}",
                    {"error": str(exc), "show_line_numbers": show_line_numbers},
                )

    sections: list[str] = []
    per_file_metadata: list[dict[str, Any]] = []

    for idx, path in enumerate(abs_paths):
        content, metadata = results[idx]
        sections.append(f'<file path="{path}">\n{content}\n</file>')
        per_file_metadata.append({"file_path": path, **(metadata or {})})

    combined_message = (
        f"Read {len(abs_paths)} file(s). Combined contents follow in per-file sections.\n\n"
        + "\n\n".join(sections)
    )

    combined_metadata: dict[str, Any] = {
        "num_files": len(abs_paths),
        "files": per_file_metadata,
        "show_line_numbers": show_line_numbers,
    }

    return combined_message, combined_metadata
