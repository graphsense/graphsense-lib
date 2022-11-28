import click

from ..cli.common import require_currency, require_environment
from ..config import supported_base_currencies
from ..utils.console import console
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
        console.rule(f"{cur}")
        with DbFactory().from_config(env, cur) as db:
            hb_ft = db.transformed.get_highest_block_fulltransform()
            hb_raw = db.raw.get_highest_block()
            start_block = db.transformed.get_highest_block_delta_updater() + 1
            latest_address_id = db.transformed.get_highest_address_id()
            latest_clstr_id = db.transformed.get_highest_cluster_id()
            console.print(f"Last addr id:       {latest_address_id:12}")
            if latest_clstr_id is not None:
                console.print(f"Last cltr id:       {latest_clstr_id:12}")
            console.print(f"Raw     Config:      {db.raw.get_configuration()}")
            console.print(f"Transf. Config:      {db.transformed.get_configuration()}")
            end_block = db.raw.find_highest_block_with_exchange_rates()
            console.print(f"Last delta-transform: {(start_block -1):10}")
            console.print(f"Last raw block:       {hb_raw:10}")
            console.print(
                f"Last raw block:       {end_block:10} (with exchanges rates)."
            )
            console.print(
                f"Transf. behind raw:   {(end_block - (start_block - 1)):10} "
                "(delta-transform)"
            )
            console.print(
                f"Transf. behind raw:   {(end_block - hb_ft):10} (full-transform)"
            )
