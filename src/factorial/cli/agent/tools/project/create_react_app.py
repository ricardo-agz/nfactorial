import os
import subprocess

from ..utils import run


def create_react_app(
    project_name: str, language: str = "typescript", with_tailwind: bool = False
) -> str:
    """
    Scaffold a React application using create-react-app. Optionally installs and configures Tailwind CSS.

    Args:
        project_name: Name of the directory / project to generate.
        language: "typescript" (default) or "javascript".
        with_tailwind:
        * **True** → install and configure Tailwind CSS by running `npx tailwindcss init -p` and injecting Tailwind CSS directives into `src/index.css`
        * **False** → skip
    """
    if language not in ("typescript", "javascript"):
        return "Error: language must be 'typescript' or 'javascript'"

    if os.path.exists(project_name):
        return f"Error: Directory {project_name} already exists."

    template_arg = ["--template", "typescript"] if language == "typescript" else []

    # Collect logs from each command we run so we can surface them back to the caller
    logs: list[str] = []

    try:
        # 1. Scaffold the CRA project
        logs.append(run(["npx", "create-react-app", project_name, *template_arg]))

        if with_tailwind:
            # 2. Install Tailwind CSS and its peer dependencies
            logs.append(
                run(
                    ["npm", "install", "-D", "tailwindcss", "postcss", "autoprefixer"],
                    cwd=project_name,
                )
            )
            # 3. Initialise Tailwind configuration (generates tailwind.config.js & postcss.config.js)
            logs.append(run(["npx", "tailwindcss", "init", "-p"], cwd=project_name))

            # 4. Replace the main CSS file with Tailwind directives so the framework is active immediately
            index_css = os.path.join(project_name, "src", "index.css")
            with open(index_css, "w") as fh:
                fh.write(
                    "@tailwind base;\n@tailwind components;\n@tailwind utilities;\n"
                )
            logs.append(
                f"# Injected Tailwind CSS directives into {os.path.relpath(index_css)}"
            )

        # Build a verbose summary for the caller / downstream agents
        summary_lines = [
            f"React app '{project_name}' created successfully.",
        ]
        if with_tailwind:
            summary_lines.append(
                "Tailwind CSS was installed and configured (tailwind.config.js, postcss.config.js, and src/index.css updated)."
            )

        # Join logs, truncate if excessively long (>4000 chars) to avoid flooding the caller
        logs_text = "\n\n".join(logs)
        max_len = 4000
        if len(logs_text) > max_len:
            logs_text = logs_text[:max_len] + "\n... (truncated)"
        summary_lines.append("\n--- Command Logs ---\n" + logs_text)

        return "\n".join(summary_lines)

    except subprocess.CalledProcessError as exc:
        # We already captured the failing command's output in `logs`, so surface that too
        error_logs = "\n\n".join(logs)
        return f"Error during create-react-app execution: {exc}\n\n--- Partial Logs ---\n{error_logs}"
