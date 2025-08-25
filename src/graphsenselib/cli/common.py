import hashlib
import os
import sys

import click

from ..config import (
    default_environments,
    get_config,
    schema_types,
    supported_base_currencies,
)
import logging

logger = logging.getLogger(__name__)


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
    if (
        len(sys.argv) > 1
        and sys.argv[1][0] == "-"
        and sys.argv[1][1:] == "v" * len(sys.argv[1][1:])
    ):
        remaining_args = sys.argv[2:]
    else:
        remaining_args = sys.argv[1:]

    is_tagpack_tool = len(remaining_args) > 0 and remaining_args[0] in [
        "tagpack-tool",
        "tagstore",
    ]

    app_config = get_config()
    f = filename or app_config.underlying_file

    try:
        if is_tagpack_tool:
            success, errors = app_config.load_partial(filename=filename)

            if not success:
                logger.debug(
                    f"Partial config loading for {remaining_args[0]} with {len(errors)} issues:"
                )
                for error in errors:
                    logger.debug(f"  - {error}")
                logger.debug("Continuing with partial/default configuration...")
            else:
                logger.debug(f"Config created successfully for {remaining_args[0]}")
        else:
            # Use strict loading for other tools
            app_config.load(filename=filename)

        if f and os.path.exists(f):
            with open(f, "rb") as file:
                md5hash = hashlib.md5(file.read()).hexdigest()
        else:
            md5hash = "no-config-file"
            if not is_tagpack_tool:
                raise Exception("No config file loaded")

        return app_config, md5hash

    except Exception as e:
        if not f:
            logger.error("No config file specified or found in default locations")
        elif not os.path.exists(f):
            logger.error(f"Config file {f} does not exist")
            logger.error("Suggestions")
            file_loc = " or ".join(app_config.model_config["default_files"])
            logger.error(
                f"Error: {e} \n"
                f"Please create the file in {file_loc} or specify a custom config path"
                f" in the environment variable {app_config.model_config['file_env_var']}."
            )
        else:
            logger.error(e)
            logger.error(
                "If there are errors in your graphsenselib config, please check the template below:"
            )
            logger.error("Template")
            logger.debug(app_config.generate_yaml(DEBUG=False))

        sys.exit(10)
