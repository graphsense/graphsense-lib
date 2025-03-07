import click

from ..cli.common import require_currency, require_environment
from ..schema import GraphsenseSchemas
from .deltaupdater import patch_exchange_rates, state, update, validate


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
    "--start-block",
    type=int,
    default=None,
    help="Block at which to start the update (inclusive). "
    "(default: start from latest transformed block)",
)
@click.option(
    "--end-block",
    type=int,
    default=None,
    help="Block at which to end the update (inclusive). (default: until raw data ends)",
)
@click.option(
    "--updater-version",
    type=int,
    default=1,
    help="Which version of the delta-updater to use, 1: legacy (addresses only)"
    ", 2: full (default: 1)",
)
@click.option("--write-new/--no-write-new", default=True)
@click.option("--write-dirty/--no-write-dirty", default=True)
@click.option("--pedantic/--no-pedantic", default=False)
@click.option(
    "--create-schema",
    is_flag=True,
    help="Create database schema if it does not exist",
)
@click.option(
    "--forward-fill-rates",
    is_flag=True,
    help=(
        "When set the importer imports until the most recent block "
        "regardless if current exchange rates are available. "
        "If no rate for the current day is available it uses the last one available"
        "This allows the delta update provide close to real time updates "
        "since it does not have to wait for current exchange rates."
    ),
)
@click.option(
    "--disable-safety-checks",
    is_flag=True,
    help="Disables safety checks for the delta update.",
)
def deltaupdate(
    env,
    currency,
    start_block,
    end_block,
    write_new,
    write_dirty,
    write_batch_size,
    updater_version,
    pedantic,
    create_schema,
    forward_fill_rates,
    disable_safety_checks,
):
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
    if create_schema:
        GraphsenseSchemas().create_keyspace_if_not_exist(
            env, currency, keyspace_type="transformed"
        )

    update(
        env,
        currency,
        start_block,
        end_block,
        write_new,
        write_dirty,
        write_batch_size,
        updater_version,
        pedantic,
        forward_fill_rates,
        disable_safety_checks,
    )


@delta.command("status", help="Shows the status of the delta updater.")
@require_environment()
@require_currency()
def status(env, currency):
    state(env, currency)


@delta.command("validate")
@require_environment()
@require_currency()
@click.option(
    "--look-back-blocks",
    type=int,
    default=20,
    help="How may historic blocks to look at.",
)
def validatedelta(env: str, currency: str, look_back_blocks: int):
    """Validates the current delta update status and its history.
    \f

    Args:
        env (str): Env to work on
        currency (str): Currency to work on
        look_back_blocks (int): How many blocks to check
    """
    validate(env, currency, look_back_blocks)


@delta.command("patch-exchange-rates")
@require_environment()
@require_currency()
@click.option(
    "--block",
    type=int,
    required=True,
    help="Block to write the exchange rates for.",
)
def pexchangerates(env: str, currency: str, block: int):
    """Rewrites the transformed exchange rate at a specific block
    \f

    Args:
        env (str): Env to work on
        currency (str): Currency to work on
        block (int): block to patch
    """
    patch_exchange_rates(env, currency, block)
