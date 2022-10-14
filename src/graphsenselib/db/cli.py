import click

from ..cli.common import require_currency, require_environment
from ..config import supported_base_currencies
from .factory import DbFactory


@click.group()
def db_cli():
    pass


@db_cli.group()
def db():
    """DB-management related functions."""
    pass


@db.command("state")
@require_environment()
@require_currency(required=False)
def state(env, currency):
    """Summary
    Prints the current state of the graphsense database.
    \f
    Args:
        env (str): Environment to work on
        currency (str): currency to work on
    """
    currencies = supported_base_currencies if currency is None else [currency]
    for cur in currencies:
        click.echo(f"\n===== {cur}")
        with DbFactory().from_config(env, cur) as db:
            hb_ft = db.transformed.get_highest_block()
            hb_raw = db.raw.get_highest_block()
            start_block = db.transformed.get_highest_block_delta_updater() + 1
            latest_address_id = db.transformed.get_highest_address_id()
            click.echo(f"Last addr id:       {latest_address_id:12}")
            click.echo(f"Raw     Config:      {db.raw.get_configuration()}")
            click.echo(f"Transf. Config:      {db.transformed.get_configuration()}")
            end_block = db.raw.find_highest_block_with_exchange_rates()
            click.echo(f"Last delta-transform: {(start_block -1):10}")
            click.echo(f"Last raw block:       {hb_raw:10}")
            click.echo(f"Last raw block:       {end_block:10} (with exchanges rates).")
            click.echo(
                f"Transf. behind raw:   {(end_block - (start_block -1)):10} "
                "(delta-transform)"
            )
            click.echo(
                f"Transf. behind raw:   {(end_block - hb_ft):10} (full-transform)"
            )
