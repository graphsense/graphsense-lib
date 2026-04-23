import hashlib
import os
import sys
import warnings

import click

from ..config import (
    default_environments,
    get_config,
    schema_types,
    supported_base_currencies,
)
from ..config.settings import Settings, set_settings
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
    # Surface DeprecationWarning on the CLI by default — Python silences
    # them otherwise, which would hide the legacy env-prefix and class
    # warnings emitted by the consolidated config.
    warnings.simplefilter("default", DeprecationWarning, append=True)

    remaining_args = sys.argv[1:]

    # Also sniff --env / -e so the new Settings loader can pick the
    # right per-env overlay file. Click parses it too, later, but the
    # Settings singleton needs to be built *before* the command body
    # runs — otherwise consumers that call get_settings() see the wrong
    # env. The sniff is best-effort; if missing, the overlay is skipped.
    global_options_with_values = {"--config-file", "--env", "-e"}
    top_level_command = None
    selected_env: str | None = None
    skip_next = False
    prev_arg: str | None = None
    for arg in remaining_args:
        if skip_next:
            if prev_arg in ("--env", "-e"):
                selected_env = arg
            skip_next = False
            prev_arg = arg
            continue
        # Handle --env=prod / --config-file=path form.
        if arg.startswith("--env="):
            selected_env = arg.split("=", 1)[1]
            prev_arg = arg
            continue
        if arg in global_options_with_values:
            skip_next = True
            prev_arg = arg
            continue
        if arg.startswith("-"):
            prev_arg = arg
            continue
        if top_level_command is None:
            top_level_command = arg
        prev_arg = arg

    is_optional_config_command = top_level_command in [
        "tagpack-tool",
        "tagstore",
        "web",
        "convert",
        "mcp",
    ]

    app_config = get_config()
    f = filename or app_config.underlying_file

    try:
        if is_optional_config_command:
            success, errors = app_config.load_partial(filename=filename)

            if not success:
                logger.debug(
                    f"Partial config loading for {top_level_command} with {len(errors)} issues:"
                )
                for error in errors:
                    logger.debug(f"  - {error}")
                logger.debug("Continuing with partial/default configuration...")
            else:
                logger.debug(f"Config created successfully for {top_level_command}")
        else:
            # Use strict loading for other tools
            app_config.load(filename=filename)

        if f and os.path.exists(f):
            with open(f, "rb") as file:
                md5hash = hashlib.md5(file.read()).hexdigest()
        else:
            md5hash = "no-config-file"
            if not is_optional_config_command:
                raise Exception("No config file loaded")

        # Mirror into the new Settings singleton so `gs config show
        # --resolved` (and any future Settings consumer) sees the same
        # YAML the legacy AppConfig is reading. Errors are non-fatal here:
        # the new model is partial-friendly and any consumer that needs a
        # required field will surface its own error.
        new_settings, settings_errors = Settings.try_load(filename=f, env=selected_env)
        if new_settings is not None:
            set_settings(new_settings)
        elif settings_errors:
            for err in settings_errors:
                logger.debug("Settings load issue: %s", err)

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
