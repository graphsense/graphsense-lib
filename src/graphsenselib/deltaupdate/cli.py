import click

from ..cli.common import require_currency, require_environment
from .deltaupdater import state, update, validate


@click.group()
def deltaupdate_cli():
    pass


@deltaupdate_cli.group("delta-update")
def delta():
    """Updating the transformed keyspace from the raw keyspace."""
    pass


@delta.command("update", short_help="Updates transformed from raw, if possible.")
@require_environment()
@require_currency()
@click.option(
    "--write-batch-size",
    type=int,
    show_default=True,
    default=10,
    help="Nr. of blocks to bundle in a write. Larger batches "
    "usually yields better performance but needs more memory.",
)
@click.option(
    "-s",
    "--start-block",
    type=int,
    default=None,
    help="Block at which to start the update. "
    "(default: start from latest transformed block)",
)
@click.option("--write-new/--no-write-new", default=True)
@click.option("--write-dirty/--no-write-dirty", default=True)
def deltaupdate(env, currency, start_block, write_new, write_dirty, write_batch_size):
    """Updates the transformend keyspace for new data in raw, if possible.
    \f
    Args:
        env (str): Env to work on
        currency (str): currency to work on
        start_block (int): start block
        write_new (bool): should I write new_address table?
        write_dirty (bool): should I write dirty_address table?
        write_batch_size (int): how many blocks at a time are written.
    """
    update(env, currency, start_block, write_new, write_dirty, write_batch_size)


@delta.command("status", help="Shows the status of the delta updater.")
@require_environment()
@require_currency()
def status(env, currency):
    state(env, currency)


@delta.command("validate")
@require_environment()
@require_currency()
def validatedelta(env: str, currency: str):
    """Validates the current delta update status and its history.
    \f
    Args:
        env (str): Env to work on
        currency (str): Currency to work on
    """
    validate(env, currency)
