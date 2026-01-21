from __future__ import annotations

import asyncio
import os
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

import click

from factorial import (
    MODELS,
    Provider,
    claude_4_sonnet,
    claude_35_sonnet,
    claude_37_sonnet,
    fallback_models,
    gpt_5,
    gpt_41,
    grok_4,
)
from factorial.llms import Model, MultiClient

from .agent import CLIAgentContext, NFactorialAgent
from .event_printer import event_printer
from .key_storage import key_storage

try:
    import inquirer  # type: ignore
except ModuleNotFoundError:  # pragma: no cover
    inquirer = None  # type: ignore


from typing import NamedTuple

from pydantic import BaseModel

PROVIDERS: list[Provider] = list(Provider)


# Sorted in fallback order
DEFAULT_MODELS = [
    claude_4_sonnet,
    gpt_5,
    claude_37_sonnet,
    gpt_41,
    claude_35_sonnet,
    grok_4,
]


class ModelSetup(NamedTuple):
    client: MultiClient
    lookup: dict[str, Model]
    available_models: list[Model]
    configured_providers: list[Provider]


def _model_setup() -> ModelSetup:
    lookup: dict[str, Model] = {}
    configured_providers: set[Provider] = set()
    for model in MODELS:
        lookup[model.name.lower()] = model
        lookup[model.provider_model_id.lower()] = model
        lookup[f"{model.provider.value}@{model.name}".lower()] = model
        lookup[f"{model.provider.value}@{model.provider_model_id}".lower()] = model

    client_kwargs = {}
    for provider in PROVIDERS:
        key_name = f"{provider.value}_api_key"
        env_var = f"{provider.value.upper()}_API_KEY"
        client_kwargs[key_name] = key_storage.get_key(provider.value) or os.environ.get(
            env_var
        )
        if client_kwargs[key_name]:
            configured_providers.add(provider)

    available_models = [
        model for model in MODELS if model.provider in configured_providers
    ]

    return ModelSetup(
        client=MultiClient(
            openai_api_key=client_kwargs.get("openai_api_key"),
            xai_api_key=client_kwargs.get("xai_api_key"),
            anthropic_api_key=client_kwargs.get("anthropic_api_key"),
            fireworks_api_key=client_kwargs.get("fireworks_api_key"),
            ai_gateway_api_key=client_kwargs.get("ai_gateway_api_key"),
        ),
        lookup=lookup,
        available_models=available_models,
        configured_providers=list(configured_providers),
    )


model_setup = _model_setup()


def _prompt_provider(providers: list[str] | None = None) -> str:
    provider_list = providers or []
    if inquirer:
        questions = [
            inquirer.List(
                "provider",
                message="Select API provider",
                choices=provider_list,
            )
        ]
        answers = inquirer.prompt(questions)
        if not answers:
            click.echo("\nAborted!", err=True)
            sys.exit(1)
        return str(answers["provider"])

    click.echo("Select API provider:")
    for idx, p in enumerate(provider_list, 1):
        click.echo(f"{idx}) {p}")
    choice = click.prompt("Provider", type=click.IntRange(1, len(provider_list)))
    return str(provider_list[choice - 1])


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
def cli() -> None:
    """nfactorial command-line utility."""


@cli.group()
def setup() -> None:
    """Configure API keys and related settings."""


@setup.command("add-key")
@click.option(
    "--provider",
    type=click.Choice([p.value for p in PROVIDERS]),
    help="API provider to configure",
)
def add_key(provider: str | None) -> None:
    """Add or update an API key."""
    if not provider:
        provider = _prompt_provider([p.value for p in PROVIDERS])
    key = click.prompt(f"Enter your {provider} API key", hide_input=True)
    key_storage.set_key(provider, key)
    click.echo(f"Successfully saved {provider} API key!")


@setup.command("remove-key")
@click.option(
    "--provider",
    type=click.Choice([p.value for p in PROVIDERS]),
    help="API provider to remove",
)
def remove_key(provider: str | None) -> None:
    """Remove an API key."""
    if not model_setup.configured_providers:
        click.echo("No API keys configured.")
        return
    if not provider:
        provider = _prompt_provider([p.value for p in model_setup.configured_providers])
    stored_key = key_storage.get_key(provider)
    env_var = f"{provider.upper()}_API_KEY"
    env_key = os.environ.get(env_var)
    if stored_key:
        key_storage.remove_key(provider)
        click.echo(f"Removed {provider} API key from key storage.")
    elif env_key:
        click.echo(
            f"{provider} API key is provided via environment variable {env_var}.\n"
            "This CLI cannot remove environment variables from your shell.\n"
            "To remove it:\n"
            f"  - Temporarily (current session): 'unset {env_var}' or "
            f"'export {env_var}='\n"
            "  - Persistently: remove it from your shell profile "
            "(e.g., ~/.zshrc, ~/.bashrc) and restart your shell."
        )
    else:
        click.echo(f"No {provider} API key found in key storage or environment.")


@setup.command("list-keys")
def list_keys() -> None:
    """List currently configured API keys."""
    click.echo("Configured API keys:")
    click.echo("-" * 40)
    for provider in PROVIDERS:
        provider_name = provider.value
        key = key_storage.get_key(provider_name)
        env_var = f"{provider_name.upper()}_API_KEY"
        env_key = os.environ.get(env_var)
        if key:
            masked = f"{key[:4]}...{key[-4:]}"
            click.echo(f"{provider_name:10} {masked}")
        elif env_key:
            masked_env = f"{env_key[:4]}...{env_key[-4:]}"
            click.echo(f"{provider_name:10} {masked_env} (env)")
        else:
            click.echo(f"{provider_name:10} Not configured")


@cli.command()
@click.argument(
    "path",
    type=click.Path(file_okay=False, dir_okay=True, writable=True, path_type=Path),
)
@click.argument("description", nargs=-1, required=True)
@click.option(
    "--model",
    "-m",
    "model_name",
    default=None,
    help="Force a particular model (e.g. 'gpt-4.1', 'claude-4-sonnet').",
)
def create(path: Path, description: tuple[str, ...], model_name: str | None) -> None:
    """Bootstrap a new nfactorial project.

    PATH: Where to create the project - use '.' for current directory or provide
          a name for a new directory
    DESCRIPTION: Description of the project to generate"""
    prompt = " ".join(description).strip("'\"").strip()
    if not prompt:
        click.echo("Description cannot be empty", err=True)
        sys.exit(1)

    # Handle project directory creation
    if str(path) == ".":
        # Use current directory
        project_dir = Path.cwd()
    else:
        # Create new directory with the given path
        project_dir = path.resolve()
        project_dir.mkdir(parents=True, exist_ok=True)

    model_setup = _model_setup()
    if not model_setup.configured_providers:
        click.echo(
            "No API keys configured. Please set up API keys using "
            "'nfactorial setup add-key' command or environment variables.",
            err=True,
        )
        sys.exit(1)

    # Model preference: CLI flag → automatic based on keys → default
    model: Model | Callable[[CLIAgentContext], Model]
    if model_name:
        selected_model = model_setup.lookup.get(model_name.lower())
        if not selected_model:
            available = ", ".join([m.name for m in MODELS])
            click.echo(
                f"Unknown model '{model_name}'. Available models: {available}",
                err=True,
            )
            sys.exit(1)
        if selected_model.provider not in model_setup.configured_providers:
            provider_val = selected_model.provider.value
            env_var = f"{provider_val.upper()}_API_KEY"
            click.echo(
                f"Model '{selected_model.name}' requires provider '{provider_val}', "
                "but no API key is configured. Add a key via "
                f"'nfactorial setup add-key' or set the {env_var} "
                "environment variable.",
                err=True,
            )
            sys.exit(1)
        model = selected_model
    else:
        if not model_setup.available_models:
            click.echo(
                "No models available with configured API keys",
                err=True,
            )
            sys.exit(1)

        model = fallback_models(
            *[
                m
                for m in DEFAULT_MODELS
                if m in model_setup.available_models
            ]
        )

    agent = NFactorialAgent(mode="create", model=model, client=model_setup.client)

    async def _run_agent() -> Any:
        cwd = os.getcwd()
        try:
            os.chdir(project_dir)
            agent_ctx = CLIAgentContext(query=prompt)
            completion = await agent.run_inline(agent_ctx, event_handler=event_printer)
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
        if isinstance(output, BaseModel):
            output_dict = output.model_dump()
        elif isinstance(output, dict):
            output_dict = output
        else:
            output_dict = {}

        if output_dict.get("run_commands"):
            click.echo("Suggested next commands:")
            for cmd in output_dict["run_commands"]:
                click.echo(f"  {cmd}")
    except Exception:  # pragma: no cover – best-effort reporting
        pass


@cli.command()
@click.argument("prompt", nargs=-1, required=True)
@click.option(
    "--model",
    "-m",
    "model_name",
    default=None,
    help="Force a particular model (e.g. 'gpt-4.1', 'claude-4-sonnet').",
)
def agent(prompt: tuple[str, ...], model_name: str | None) -> None:
    """Work on an existing nfactorial project.

    PROMPT: Description of what you want to do in the current project"""
    prompt_text = " ".join(prompt).strip("'\"").strip()
    if not prompt_text:
        click.echo("Description cannot be empty", err=True)
        sys.exit(1)

    # Use current directory
    project_dir = Path.cwd()

    model_setup = _model_setup()
    if not model_setup.configured_providers:
        click.echo(
            "No API keys configured. Please set up API keys using "
            "'nfactorial setup add-key' command or environment variables.",
            err=True,
        )
        sys.exit(1)

    # Model preference: CLI flag → automatic based on keys → default
    agent_model: Model | Callable[[CLIAgentContext], Model]
    if model_name:
        selected_model = model_setup.lookup.get(model_name.lower())
        if not selected_model:
            available = ", ".join([m.name for m in MODELS])
            click.echo(
                f"Unknown model '{model_name}'. Available models: {available}",
                err=True,
            )
            sys.exit(1)
        agent_model = selected_model
    else:
        if not model_setup.available_models:
            click.echo(
                "No models available with configured API keys",
                err=True,
            )
            sys.exit(1)

        agent_model = fallback_models(
            *[
                m
                for m in DEFAULT_MODELS
                if m in model_setup.available_models
            ]
        )

    agent = NFactorialAgent(mode="edit", model=agent_model, client=model_setup.client)

    async def _run_agent() -> Any:
        cwd = os.getcwd()
        try:
            os.chdir(project_dir)
            agent_ctx = CLIAgentContext(query=prompt_text)
            completion = await agent.run_inline(agent_ctx, event_handler=event_printer)
            return completion
        finally:
            os.chdir(cwd)

    click.echo("Running nfactorial agent to work on project...\n")
    run_completion = asyncio.run(_run_agent())

    click.echo(f"\nCompleted work in {project_dir.resolve()}")

    # Report any commands suggested by the agent (if using FinalOutput)
    output = getattr(run_completion, "output", None)
    try:
        # If output is a Pydantic model (FinalOutput), convert to dict for inspection
        if isinstance(output, BaseModel):
            output_dict = output.model_dump()
        elif isinstance(output, dict):
            output_dict = output
        else:
            output_dict = {}

        if output_dict.get("run_commands"):
            click.echo("Suggested next commands:")
            for cmd in output_dict["run_commands"]:
                click.echo(f"  {cmd}")
    except Exception:  # pragma: no cover – best-effort reporting
        pass


if __name__ == "__main__":
    cli()
