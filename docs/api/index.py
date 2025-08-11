from pathlib import Path
import yaml

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import Response


app = FastAPI(title="Docs + Agent Prompt API")


# Resolve directories relative to this file
BASE_DIR = Path(__file__).resolve().parent.parent
DOCS_DIR = (BASE_DIR / "docs").resolve()
PROMPTS_DIR = (BASE_DIR / "agent").resolve()


def _gather_markdown_files(base_path: Path) -> list[Path]:
    return sorted(base_path.rglob("*.md"))


def _aggregate_docs() -> str:
    if not DOCS_DIR.exists():
        raise FileNotFoundError(f"Documentation directory '{DOCS_DIR}' not found.")

    parts: list[str] = []
    for md_path in _gather_markdown_files(DOCS_DIR):
        relative_title = md_path.relative_to(DOCS_DIR)
        parts.append(f"# {relative_title}\n\n")
        parts.append(md_path.read_text(encoding="utf-8"))
        parts.append("\n\n---\n\n")

    return "".join(parts).rstrip()


def _render_prompt(model: str, agent_mode: str) -> str:
    base_path = PROMPTS_DIR / "agent-base.md"
    create_yaml = PROMPTS_DIR / "agent-create.yaml"
    edit_yaml = PROMPTS_DIR / "agent-edit.yaml"

    if not base_path.exists() or not create_yaml.exists() or not edit_yaml.exists():
        missing = [
            p.name for p in [base_path, create_yaml, edit_yaml] if not p.exists()
        ]
        raise FileNotFoundError(f"Missing required prompt files: {', '.join(missing)}")

    base_prompt = base_path.read_text(encoding="utf-8")

    with open(create_yaml, "r") as fh:
        create_vars = yaml.safe_load(fh) or {}
    with open(edit_yaml, "r") as fh:
        edit_vars = yaml.safe_load(fh) or {}

    create_prompt = base_prompt
    for key, value in create_vars.items():
        create_prompt = create_prompt.replace(f"{{{{{key.upper()}}}}}", str(value))

    edit_prompt = base_prompt
    for key, value in edit_vars.items():
        edit_prompt = edit_prompt.replace(f"{{{{{key.upper()}}}}}", str(value))

    if agent_mode == "create":
        instructions = create_prompt
    elif agent_mode == "edit":
        instructions = edit_prompt
    else:
        raise ValueError(f"Invalid agent mode: {agent_mode}")

    framework_docs = _aggregate_docs()
    instructions = instructions.replace("{{FRAMEWORK_DOCS}}", framework_docs)

    return instructions


@app.get(
    "/api/docs", summary="Return all documentation as Markdown", response_class=Response
)
async def get_all_docs() -> Response:
    try:
        markdown = _aggregate_docs()
        return Response(content=markdown, media_type="text/markdown")
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.get(
    "/api/agent-prompt",
    summary="Return the rendered agent prompt",
    response_class=Response,
)
async def get_agent_prompt(
    model: str = Query(..., description="LLM being used"),
    agent_mode: str = Query(..., description="Agent mode: 'create' or 'edit'"),
) -> Response:
    try:
        prompt = _render_prompt(model=model, agent_mode=agent_mode)
        return Response(content=prompt, media_type="text/markdown")
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


# Local debug
if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8081)
