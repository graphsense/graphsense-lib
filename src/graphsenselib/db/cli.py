# flake8: noqa: T201
import logging
from datetime import datetime
from typing import Optional

import click
from eth_hash.auto import keccak

from ..cli.common import require_currency, require_environment
from ..config import currency_to_schema_type, supported_base_currencies
from ..datatypes.abi import decode_db_logs, decoded_log_to_str
from ..utils.accountmodel import hex_str_to_bytes, hex_to_bytes, is_hex_string, strip_0x
from ..utils.console import console
from .factory import DbFactory
from .trace import trace as trace_it

logger = logging.getLogger(__name__)


@click.group()
def db_cli():
    pass


@db_cli.group()
def db():
    """Query related functions."""
    pass


@db_cli.group()
def trace():
    """trace related functions."""
    pass


@db.command("state")
@require_environment()
@require_currency(required=False)
def state(env, currency):
    """Prints the current state of the graphsense database.
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
            start_block = (db.transformed.get_highest_block_delta_updater() or -1) + 1
            transformed_exists = db.transformed.exists()
            if transformed_exists:
                latest_address_id = db.transformed.get_highest_address_id()
                latest_clstr_id = db.transformed.get_highest_cluster_id()
            else:
                latest_address_id = 0
                latest_clstr_id = 0
            console.print(f"Last addr id:       {latest_address_id:12}")
            if latest_clstr_id is not None:
                console.print(f"Last cltr id:       {latest_clstr_id:12}")
            console.print(f"Raw     Config:      {db.raw.get_configuration()}")
            if transformed_exists:
                console.print(
                    f"Transf. Config:      {db.transformed.get_configuration()}"
                )
            end_block = db.raw.find_highest_block_with_exchange_rates()
            console.print(
                f"Last delta-transform: {(start_block - 1):10}"
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
            if transformed_exists:
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
    "--start-block",
    type=int,
    required=True,
    help="Block to start fetching the decodable logs.",
)
@click.option(
    "--end-block",
    type=int,
    required=True,
    help="Block to stop fetching the decodable logs.",
)
@click.option(
    "--topic0",
    type=str,
    required=None,
    help="Log topic to fetch.",
)
@click.option(
    "--contract",
    type=str,
    required=None,
    help="Filter for Contract that produced the log.",
)
def get_logs(
    env: str,
    currency: str,
    start_block: int,
    end_block: int,
    topic0: Optional[str],
    contract: Optional[str],
):
    """Print all decodable logs for a given block
    Args:
        env (str): evironment
        currency (str): currency
        block (int): block
    """
    stype = currency_to_schema_type.get(currency, None)
    if stype == "account" or stype == "account_trx":
        with DbFactory().from_config(env, currency) as db:
            if topic0 is not None and is_hex_string(topic0):
                topic0 = hex_str_to_bytes(topic0)
            elif topic0 is not None:
                topic0 = keccak(topic0.encode("utf-8"))

            if contract is not None:
                contract = hex_str_to_bytes(strip_0x(contract))
            for b in range(start_block, end_block):
                for dlog, log in decode_db_logs(
                    db.raw.get_logs_in_block(b, topic0=topic0, contract=contract)
                ):
                    dlog_str = decoded_log_to_str(dlog)
                    print(f"{b}|{log.log_index}|0x{log.tx_hash.hex()}|{dlog_str}")
    else:
        print(
            f"Unsupported schema type {stype} for "
            f"currency {currency}. Only account is supported."
        )


@trace.command("events")
@require_environment()
@require_currency()
@click.option(
    "--q",
    type=str,
    required=True,
    help="Transaction hash or address",
)
@click.option(
    "--contract",
    type=str,
    required=False,
    help="Address of the contract to look at",
)
@click.option(
    "--start",
    type=str,
    required=False,
    help="Block or Date",
)
@click.option(
    "--end",
    type=str,
    required=False,
    help="Block or Date",
)
@click.option(
    "--names-file",
    type=str,
    default="./names.csv",
    help="File containing names for addresses and hashes etc.",
)
@click.option(
    "--output-format",
    type=str,
    default="csv",
    help="How to print results (csv, table etc.)",
)
def trace_transaction(
    env: str,
    currency: str,
    q: str,
    contract: str,
    start: str,
    end: str,
    names_file: str,
    output_format: str,
):
    """Print logs and filter
    Args:
        env (str): evironment
        currency (str): currency
        tx (str): tx
    """
    stype = currency_to_schema_type.get(currency, None)
    if stype == "account":
        with DbFactory().from_config(env, currency) as db:
            import os.path

            if os.path.isfile(names_file):
                with open(names_file) as file:
                    kvl = [line.split(":") for line in file.readlines()]
                    names = {a.strip(): b.strip() for a, b, *rest in kvl}
            else:
                names = {}
            trace_it(
                db,
                q.split(","),
                hex_to_bytes(contract),
                start,
                end,
                names,
                output_format,
            )
    else:
        print(
            f"Unsupported schema type {stype} for "
            f"currency {currency}. Only account is supported."
        )
