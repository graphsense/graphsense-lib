import logging
from datetime import datetime

import click

from ..cli.common import require_currency, require_environment
from ..config import currency_to_schema_type, supported_base_currencies
from ..datatypes.abi import decode_db_logs, decoded_log_to_str
from ..utils.console import console
from .factory import DbFactory

logger = logging.getLogger(__name__)


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
            console.print(
                f"Last delta-transform: {(start_block -1):10}"
                f" ({db.raw.get_block_timestamp(start_block - 1)})"
            )
            console.print(
                f"Last raw block:       {hb_raw:10}"
                f" ({db.raw.get_block_timestamp(hb_raw)})"
            )
            console.print(
                f"Last raw block:       {end_block:10}"
                f" ({db.raw.get_block_timestamp(end_block)}) "
                "(with exchanges rates)."
            )
            console.print(
                f"Transf. behind raw:   {(end_block - (start_block - 1)):10}"
                f" ({db.raw.get_block_timestamp(end_block - (start_block - 1))})"
                " (delta-transform)"
            )
            console.print(
                f"Transf. behind raw:   {(end_block - hb_ft):10}"
                f" ({db.raw.get_block_timestamp((end_block - hb_ft))})"
                " (full-transform)"
            )


@db.group()
def block():
    """Special db query functions regarding blocks."""
    pass


@block.command("get-ts")
@require_environment()
@require_currency(required=False)
@click.option(
    "-b",
    "--block",
    type=int,
    required=True,
    help="block to get the ts for",
)
def get_ts(env: str, currency: str, block: int):
    """Summary
    Prints timestamps for the given block nr
    \f

    Args:
        env (str): Environment to work on
        currency (str): currency to work on
        block_id (int): Block to query
    """
    currencies = supported_base_currencies if currency is None else [currency]
    multi = len(currencies) > 1
    for cur in currencies:
        with DbFactory().from_config(env, cur) as db:
            timestamp = db.raw.get_block_timestamp(block)
            if multi:
                console.print(f"{cur}={timestamp}")
            else:
                console.print(f"{timestamp}")


@block.command("get-nr")
@require_environment()
@require_currency(required=False)
@click.option(
    "--date",
    type=click.DateTime(formats=["%Y-%m-%d %H:%M:%S"]),
    required=True,
    help="Date to get the block nr for.",
)
def get_nr(env: str, currency: str, date: datetime):
    """Summary
    Prints last blocknumber to include the given date.
    \f

    Args:
        env (str): Environment to work on
        currency (str): currency to work on
        block_id (int): Block to query
    """
    currencies = supported_base_currencies if currency is None else [currency]
    multi = len(currencies) > 1
    for cur in currencies:
        with DbFactory().from_config(env, cur) as db:
            nr = db.raw.find_block_nr_for_date(date)
            if nr is not None:
                tsp1 = db.raw.get_block_timestamp(nr + 1)
                ts = db.raw.get_block_timestamp(nr)
                tsm1 = db.raw.get_block_timestamp(nr - 1)
                logger.info(f"{tsm1} (-1) - {ts} ({nr}) - {tsp1} (+1)")
            if multi:
                console.print(f"{cur}={nr}")
            else:
                console.print(f"{nr}")


@db.group()
def logs():
    """Special db query functions regarding logs."""
    pass


@logs.command("get-decodeable-logs")
@require_environment()
@require_currency()
@click.option(
    "-b",
    "--block",
    type=int,
    required=True,
    help="Block to fetch the decodable logs.",
)
def get_logs(env: str, currency: str, block: int):
    """Print all decodable logs for a given block
    Args:
        env (str): evironment
        currency (str): currency
        block (int): block
    """
    stype = currency_to_schema_type.get(currency, None)
    if stype == "account":
        with DbFactory().from_config(env, currency) as db:
            for log in decode_db_logs(db.raw.get_logs_in_block(block)):
                console.print(decoded_log_to_str(log))
    else:
        console.print(
            f"Unsupported schema type {stype} for "
            "currency {currency}. Only account is supported."
        )
