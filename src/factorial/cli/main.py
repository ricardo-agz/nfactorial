from __future__ import annotations

import sys
import os
import asyncio
import json
from pathlib import Path
from typing import Any, Dict

import click

from .constants import PROVIDERS
from .key_storage import key_storage
from .agent import create_agent, CLIAgentContext

# Individual model symbols remain for backward-compat logic, but we'll import llms to access all models
from factorial import gpt_41, claude_4_sonnet, grok_4, fallback_models
from factorial import llms as _llms  # type: ignore  # dynamic attribute inspection
from factorial.llms import MultiClient

try:
    import inquirer  # type: ignore
except ModuleNotFoundError:  # pragma: no cover
    inquirer = None  # type: ignore


def _prompt_provider(providers: list[str] | None = None) -> str:
    providers = providers or PROVIDERS

    if inquirer:
        questions = [
            inquirer.List(
                "provider",
                message="Select API provider",
                choices=providers,
            )
        ]
        answers = inquirer.prompt(questions)
        if not answers:
            click.echo("\nAborted!", err=True)
            sys.exit(1)
        return answers["provider"]

    click.echo("Select API provider:")
    for idx, p in enumerate(providers, 1):
        click.echo(f"{idx}) {p}")
    choice = click.prompt("Provider", type=click.IntRange(1, len(providers)))
    return providers[choice - 1]


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
def cli() -> None:
    """nfactorial command-line utility."""


@cli.group()
def setup() -> None:
    """Configure API keys and related settings."""


@setup.command("add-key")
@click.option(
    "--provider",
    type=click.Choice(PROVIDERS),
    help="API provider to configure",
)
def add_key(provider: str | None) -> None:
    """Add or update an API key."""
    if not provider:
        provider = _prompt_provider()
    key = click.prompt(f"Enter your {provider} API key", hide_input=True)
    key_storage.set_key(provider, key)
    click.echo(f"Successfully saved {provider} API key!")


@setup.command("remove-key")
@click.option(
    "--provider",
    type=click.Choice(PROVIDERS),
    help="API provider to remove",
)
def remove_key(provider: str | None) -> None:
    """Remove an API key."""
    configured = [p for p in PROVIDERS if key_storage.get_key(p)]
    if not configured:
        click.echo("No API keys configured.")
        return
    if not provider:
        provider = _prompt_provider(configured)
    key_storage.remove_key(provider)
    click.echo(f"Removed {provider} API key (if it existed).")


@setup.command("list-keys")
def list_keys() -> None:
    """List currently configured API keys."""
    click.echo("Configured API keys:")
    click.echo("-" * 40)
    for provider in PROVIDERS:
        key = key_storage.get_key(provider)
        if key:
            masked = f"{key[:4]}...{key[-4:]}"
            click.echo(f"{provider:10} {masked}")
        else:
            click.echo(f"{provider:10} Not configured")


@cli.command()
@click.argument("description", nargs=-1, required=True)
@click.option(
    "--path",
    "-p",
    "target_path",
    default=".",
    type=click.Path(file_okay=False, dir_okay=True, writable=True, path_type=Path),
    help="Directory where the project should be generated (default: current directory)",
)
@click.option(
    "--model",
    "-m",
    "model_name",
    default=None,
    help="Force a particular model (e.g. 'gpt-4.1', 'claude-4-sonnet').",
)
def create(
    description: tuple[str, ...], target_path: Path, model_name: str | None
) -> None:
    """Bootstrap a new nfactorial project."""
    prompt = " ".join(description).strip("'\"").strip()
    if not prompt:
        click.echo("Description cannot be empty", err=True)
        sys.exit(1)
    project_dir = target_path
    project_dir.mkdir(parents=True, exist_ok=True)

    # Run the create_agent to generate project files inside the target directory
    async def _run_agent() -> Any:
        # Change working directory so the agent generates files in the correct place
        cwd = os.getcwd()
        os.chdir(project_dir)
        try:
            # Ensure the agent has an LLM client with available API keys
            keys = key_storage.all_keys()
            create_agent.client = MultiClient(
                openai_api_key=keys.get("openai"),
                anthropic_api_key=keys.get("anthropic"),
                xai_api_key=keys.get("xai"),
            )

            # Build mapping of available model names → Model objects on first call
            def _build_model_lookup() -> dict[str, Any]:
                from factorial.llms import Model  # local import to avoid circulars

                lookup: dict[str, Any] = {}
                for attr in dir(_llms):
                    maybe = getattr(_llms, attr)
                    if isinstance(maybe, Model):
                        # Index by human-friendly name and provider id (both lower-cased)
                        lookup[maybe.name.lower()] = maybe
                        lookup[maybe.provider_model_id.lower()] = maybe

                return lookup

            model_lookup = _build_model_lookup()

            # Model preference: CLI flag → automatic based on keys → default
            if model_name:
                chosen_model = model_lookup.get(model_name.lower())
                if not chosen_model:
                    click.echo(
                        "Unknown model '{0}'. Available models: {1}".format(
                            model_name,
                            ", ".join(sorted(set(model_lookup.keys()))),
                        ),
                        err=True,
                    )
                    sys.exit(1)

                create_agent.model = chosen_model
            else:
                supported_models = ["gpt-4.1", "claude-4-sonnet", "grok-4"]
                available_models = [
                    model
                    for model in model_lookup.values()
                    if model.provider.value in keys.keys()
                    and model.name.lower() in supported_models
                ]
                if not available_models:
                    click.echo(
                        "No models available with configured API keys",
                        err=True,
                    )
                    sys.exit(1)

                create_agent.model = fallback_models(*available_models)

            agent_ctx = CLIAgentContext(query=prompt)

            async def _event_printer(event):  # type: ignore[arg-type]
                """Print concise logs for completed tool executions."""

                event_type: str = getattr(event, "event_type", "")

                if event_type == "progress_update_tool_action_completed":
                    # Attempt to extract tool name and key arguments (best-effort)
                    tool_name: str | None = None
                    file_path: str | None = None

                    try:
                        data = event.data or {}
                        args = data.get("args", [])

                        if args:
                            tool_call = args[0]

                            # Extract function meta depending on object/dict structure
                            if isinstance(tool_call, dict):
                                func_data = tool_call.get("function", {}) or {}
                                tool_name = func_data.get("name")
                                arguments = func_data.get("arguments")
                            else:
                                func_obj = getattr(tool_call, "function", None)
                                tool_name = getattr(func_obj, "name", None)
                                arguments = getattr(func_obj, "arguments", None)

                            # Parse JSON arguments when available to get file_path
                            if arguments:
                                if isinstance(arguments, str):
                                    try:
                                        arg_dict = json.loads(arguments)
                                    except json.JSONDecodeError:
                                        arg_dict = {}
                                elif isinstance(arguments, dict):
                                    arg_dict = arguments
                                else:
                                    arg_dict = {}

                                file_path = arg_dict.get("file_path")
                    except Exception:
                        pass  # graceful degradation – logging is best-effort

                    # Craft human-friendly log messages
                    if (
                        tool_name in {"edit_code", "read_file", "create_file"}
                        and file_path
                    ):
                        verb = (
                            "edited"
                            if tool_name == "edit_code"
                            else "read"
                            if tool_name == "read_file"
                            else "created"
                        )
                        click.echo(f"[agent] {verb} file '{file_path}'")
                    elif tool_name:
                        click.echo(f"[agent] ran tool: {tool_name}")
                    else:
                        click.echo("[agent] ran a tool (name unavailable)")
                elif event_type.endswith("_failed"):
                    click.echo(f"[agent] ERROR during {event_type}")

            completion = await create_agent.run_inline(
                agent_ctx, event_handler=_event_printer
            )
            return completion
        finally:
            os.chdir(cwd)

    click.echo("Running nfactorial agent to generate project...\n")
    run_completion = asyncio.run(_run_agent())

    # Ensure __init__.py exists so the directory is a package
    (project_dir / "__init__.py").touch(exist_ok=True)

    click.echo(f"\nProject generated at {project_dir.resolve()}")

    # Report any commands suggested by the agent (if using FinalOutput)
    output = getattr(run_completion, "output", None)
    try:
        # If output is a Pydantic model (FinalOutput), convert to dict for inspection
        if hasattr(output, "model_dump"):
            output_dict = output.model_dump()  # type: ignore[attr-defined]
        else:
            output_dict = output if isinstance(output, dict) else {}

        if output_dict.get("run_commands"):
            click.echo("Suggested next commands:")
            for cmd in output_dict["run_commands"]:
                click.echo(f"  {cmd}")
    except Exception:  # pragma: no cover – best-effort reporting
        pass


if __name__ == "__main__":
    cli()
