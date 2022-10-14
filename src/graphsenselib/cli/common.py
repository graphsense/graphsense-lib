import sys

import click

from ..config import (
    config,
    default_environments,
    schema_types,
    supported_base_currencies,
)


def require_environment(required=True):
    def inner(function):
        function = click.option(
            "--env",
            "-e",
            type=click.Choice(default_environments, case_sensitive=False),
            help="Environment to work on",
            required=required,
        )(function)
        return function

    return inner


def require_schema_type(required=True):
    def inner(function):
        function = click.option(
            "--schema",
            "-s",
            type=click.Choice(schema_types, case_sensitive=False),
            help="Type of schema supported by graphsense.",
            required=required,
        )(function)
        return function

    return inner


def require_currency(required=True):
    def inner(function):
        function = click.option(
            "--currency",
            "-c",
            type=click.Choice(supported_base_currencies, case_sensitive=False),
            help="Currency to work on",
            required=required,
        )(function)
        return function

    return inner


def try_load_config():
    try:
        config.load()
    except Exception as e:
        click.echo("There are errors in you graphsenselib config:")
        click.echo("====== ERRORS:")
        click.echo(e)
        click.echo("====== HANDLING:")
        file_loc = " or ".join(config.Config.default_files)
        click.echo(
            "Maybe there is no config file specified."
            f"Please create one in {file_loc} or specify a custom config path"
            f" in the environment variable {config.Config.file_env_var}."
        )
        click.echo("A template file can be generated via looks like:")
        click.echo("================================================")
        click.echo(config.generate_yaml(DEBUG=False))
        sys.exit(10)
