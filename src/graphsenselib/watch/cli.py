import logging
import sys

import click
from filelock import FileLock
from filelock import Timeout as LockFileTimeout

from ..cli.common import require_currency, require_environment
from .factory import FlowWatcherFactory

logger = logging.getLogger(__name__)


@click.group()
def watch_cli():
    pass


@watch_cli.group()
def watch():
    """Commands for permanently watching cryptocurrency events."""
    pass


@watch.command("money-flows")
@require_environment()
@require_currency(required=True)
@click.option(
    "--state-file",
    type=str,
    required=True,
    help="File to store and read the current state of the watcher, "
    " to avoid dupicate notifications",
)
@click.option(
    "--watchpoints-file",
    type=str,
    required=True,
    help="File that defines the watched addresses.",
)
def watchflows(env, currency, state_file, watchpoints_file):
    """Watches for movements money flows and generates notifications form that.
    \f
    Args:
        env (str): Environment to work on
        currency (str): currency to work on.
    """
    lockfile_name = f"/tmp/gscli_{env}_{currency}_watcher.lock"
    logger.info(f"Try acquiring lockfile {lockfile_name}")
    try:
        with FileLock(lockfile_name, timeout=1):
            with FlowWatcherFactory().file_based_from_config(
                env, currency, state_file, watchpoints_file
            ) as watcher:
                watcher.watch()
    except LockFileTimeout:
        logger.error(
            f"Lockfile {lockfile_name} could not be acquired. "
            "Is another watcher running for the environment?"
            " If not delete the lockfile."
        )
        sys.exit(911)
