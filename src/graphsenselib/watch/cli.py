import logging
import sys

import click

from ..cli.common import require_currency, require_environment
from ..utils.locking import LockAcquisitionError, create_lock
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
    lock_name = f"gscli_{env}_{currency}_watcher"
    try:
        with create_lock(lock_name):
            with FlowWatcherFactory().file_based_from_config(
                env, currency, state_file, watchpoints_file
            ) as watcher:
                watcher.watch()
    except LockAcquisitionError as e:
        logger.error(str(e))
        sys.exit(911)
