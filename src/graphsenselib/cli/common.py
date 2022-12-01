import sys

import click

from ..config import (
    config,
    default_environments,
    schema_types,
    supported_base_currencies,
)
from ..utils.console import console


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


def out_file(required=True, append=False):
    def inner(function):
        function = click.option(
            "--out-file",
            "-o",
            type=click.File(
                mode="a" if append else "w",
                encoding=None,
                errors="strict",
                lazy=None,
                atomic=False,
            ),
            help="Output text file.",
            required=required,
        )(function)
        return function

    return inner


def try_load_config(filename: str):
    try:
        config.load(filename=filename)
        return config
    except Exception as e:
        console.print("There are errors in you graphsenselib config:")
        console.rule("Errors")
        console.print(e)
        console.rule("Suggestions")
        file_loc = " or ".join(config.Config.default_files)
        console.print(
            "Maybe there is no config file specified. "
            f"Please create one in {file_loc} or specify a custom config path"
            f" in the environment variable {config.Config.file_env_var}."
        )
        console.rule("Template")
        console.print(config.generate_yaml(DEBUG=False))
        sys.exit(10)
