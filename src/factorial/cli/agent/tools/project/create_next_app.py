import os
import subprocess

from ..utils import run


def create_next_app(
    project_name: str, language: str = "typescript", with_tailwind: bool = False
) -> str:
    """Scaffold a **Next.js** application.

    Usage:
    This should ONLY be used to initialize a NEW frontend project. Do NOT use this if the current project already has a frontend
    directory set up, work off of the existing frontend directory instead.

    Args:
        project_name: Name of the directory / project to generate.
        language: "typescript" (default) or "javascript".
        with_tailwind:
            * **True** → install and configure Tailwind CSS by running ``npx tailwindcss init -p``
              and injecting Tailwind CSS directives into ``styles/globals.css``.
            * **False** → skip Tailwind CSS setup.
    """

    if language not in ("typescript", "javascript"):
        return "Error: language must be 'typescript' or 'javascript'"

    if os.path.exists(project_name):
        return f"Error: Directory {project_name} already exists."

    lang_flag = "--typescript" if language == "typescript" else "--js"

    logs: list[str] = []

    try:
        # 1. Scaffold the Next.js project
        logs.append(
            run(
                [
                    "npx",
                    "create-next-app@latest",
                    project_name,
                    lang_flag,
                ]
            )
        )

        if with_tailwind:
            # 2. Install Tailwind & peer deps
            logs.append(
                run(
                    ["npm", "install", "-D", "tailwindcss", "postcss", "autoprefixer"],
                    cwd=project_name,
                )
            )
            # 3. Init Tailwind configuration
            logs.append(run(["npx", "tailwindcss", "init", "-p"], cwd=project_name))

            # 4. Inject Tailwind CSS directives into global stylesheet
            globals_css = os.path.join(project_name, "styles", "globals.css")
            if os.path.isfile(globals_css):
                with open(globals_css, "w", encoding="utf-8") as fh:
                    fh.write(
                        "@tailwind base;\n@tailwind components;\n@tailwind utilities;\n"
                    )
                logs.append(
                    f"# Injected Tailwind CSS directives into {os.path.relpath(globals_css)}"
                )

        # Compose summary
        summary_lines = [f"Next.js app '{project_name}' created successfully."]
        if with_tailwind:
            summary_lines.append(
                "Tailwind CSS was installed and configured (tailwind.config.js, postcss.config.js, and styles/globals.css updated)."
            )

        # Combine logs (truncate if >4000 chars)
        logs_text = "\n\n".join(logs)
        if len(logs_text) > 4000:
            logs_text = logs_text[:4000] + "\n... (truncated)"

        summary_lines.append("\n--- Command Logs ---\n" + logs_text)

        return "\n".join(summary_lines)

    except subprocess.CalledProcessError as exc:
        error_logs = "\n\n".join(logs)
        return f"Error during Next.js app creation: {exc}\n\n--- Partial Logs ---\n{error_logs}"
