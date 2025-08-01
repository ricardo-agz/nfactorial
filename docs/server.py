from pathlib import Path
from typing import List

from fastapi import FastAPI, HTTPException, Response

app = FastAPI(
    title="Docs Aggregator",
    summary="Serve all project documentation in a single Markdown payload.",
    redoc_url=None,
    docs_url=None,
)

# Resolve the documentation directory ("code/docs/docs" relative to this file).
# Since this file lives at <repo_root>/docs/server.py, the docs live in
# <repo_root>/docs/docs/.
DOCS_DIR = (Path(__file__).resolve().parent / "docs").resolve()


def _gather_markdown_files(base_path: Path) -> List[Path]:
    """Return a sorted list of all markdown files under *base_path* (recursive)."""
    return sorted(base_path.rglob("*.md"))


def _aggregate_docs() -> str:
    """Read every markdown file and concatenate them into a single string.

    Each file is separated by an horizontal rule for clarity and is preceded
    by a level-1 heading containing its relative path (so readers can jump
    around easily).
    """
    if not DOCS_DIR.exists():
        raise FileNotFoundError(f"Documentation directory '{DOCS_DIR}' not found.")

    parts: List[str] = []
    for md_path in _gather_markdown_files(DOCS_DIR):
        relative_title = md_path.relative_to(DOCS_DIR)
        parts.append(f"# {relative_title}\n\n")
        parts.append(md_path.read_text(encoding="utf-8"))
        parts.append("\n\n---\n\n")

    return "".join(parts).rstrip()  # Trim the trailing separator/newlines


@app.get(
    "/docs",
    summary="Return the full documentation as Markdown",
    response_class=Response,
)
async def get_all_docs() -> Response:  # noqa: D401
    """Return everything under `docs/docs` concatenated into one Markdown file."""
    try:
        markdown = _aggregate_docs()
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    # FastAPI will set the correct Content-Type header for us via `media_type`.
    return Response(content=markdown, media_type="text/markdown")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8081)
