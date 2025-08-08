import os
import subprocess

from ..utils import run


def create_vite_app(
    project_name: str, language: str = "typescript", with_tailwind: bool = False
) -> str:
    """Scaffold a React application using **Vite**.

    Args:
        project_name: Name of the directory / project to generate.
        language: "typescript" (default) or "javascript".
        with_tailwind:
            * **True** → install and configure Tailwind CSS v4+ with ``@tailwindcss/postcss`` plugin,
              create proper PostCSS configuration, and inject Tailwind CSS directives into ``src/index.css``.
            * **False** → skip Tailwind CSS setup.
    """

    if language not in ("typescript", "javascript"):
        return "Error: language must be 'typescript' or 'javascript'"

    if os.path.exists(project_name):
        return f"Error: Directory {project_name} already exists."

    vite_template = "react-ts" if language == "typescript" else "react"

    logs: list[str] = []

    try:
        # 1. Scaffold the Vite project
        logs.append(
            run(
                [
                    "npm",
                    "create",
                    "vite@latest",
                    project_name,
                    "--",
                    "--template",
                    vite_template,
                ]
            )
        )

        if with_tailwind:
            # 2. Install Tailwind CSS v4+ and its PostCSS plugin
            logs.append(
                run(
                    [
                        "npm",
                        "install",
                        "-D",
                        "tailwindcss",
                        "@tailwindcss/postcss",
                        "autoprefixer",
                    ],
                    cwd=project_name,
                )
            )

            # 3. Create Tailwind configuration (tailwind.config.js)
            logs.append(run(["npx", "tailwindcss", "init"], cwd=project_name))

            # 4. Create PostCSS configuration for Tailwind v4+
            postcss_config = os.path.join(project_name, "postcss.config.js")
            with open(postcss_config, "w", encoding="utf-8") as fh:
                fh.write("""export default {
  plugins: {
    '@tailwindcss/postcss': {},
    autoprefixer: {},
  },
}
""")
            logs.append(
                f"# Created PostCSS configuration at {os.path.relpath(postcss_config)}"
            )

            # 5. Inject Tailwind directives into the main CSS entry point
            index_css = os.path.join(project_name, "src", "index.css")
            if os.path.isfile(index_css):
                with open(index_css, "w", encoding="utf-8") as fh:
                    fh.write('@import "tailwindcss";\n')
                logs.append(
                    f"# Injected Tailwind CSS directives into {os.path.relpath(index_css)}"
                )

        # Compose human-readable summary
        summary_lines = [f"Vite app '{project_name}' created successfully."]
        if with_tailwind:
            summary_lines.append(
                "Tailwind CSS was installed and fully configured (tailwind.config.js, postcss.config.js, and src/index.css updated)."
            )

        # Concatenate logs and truncate if too long
        logs_text = "\n\n".join(logs)
        max_len = 4000
        if len(logs_text) > max_len:
            logs_text = logs_text[:max_len] + "\n... (truncated)"

        summary_lines.append("\n--- Command Logs ---\n" + logs_text)

        return "\n".join(summary_lines)

    except subprocess.CalledProcessError as exc:
        error_logs = "\n\n".join(logs)
        return f"Error during Vite app creation: {exc}\n\n--- Partial Logs ---\n{error_logs}"
