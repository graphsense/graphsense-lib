import sys

import click

from ..cli.common import require_currency, require_environment
from ..config import config, currency_to_schema_type
from ..db import DbFactory
from ..schema import GraphsenseSchemas
from .account_streaming import ingest as ingest_eth


@click.group()
def ingest_cli():
    pass


@ingest_cli.group("ingest")
def ingesting():
    """Ingesting raw cryptocurrency data from nodes into the graphsense database"""
    pass


@ingesting.command("from-node")
@require_environment()
@require_currency(required=True)
@click.option(
    "--start-block",
    type=int,
    required=False,
    help="start block (default: continue from last ingested block)",
)
@click.option(
    "--end-block",
    type=int,
    required=False,
    help="end block (default: last available block)",
)
@click.option(
    "--batch-size",
    type=int,
    default=10,
    help="number of blocks to export at a time (default: 10)",
)
@click.option(
    "--timeout",
    type=int,
    required=False,
    default=3600,
    help="end block (default: last available block)",
)
@click.option(
    "--info",
    is_flag=True,
    help="display block information and exit",
)
@click.option(
    "-p",
    "--previous_day",
    is_flag=True,
    help="only ingest blocks up to the previous day, "
    "since currency exchange rates might not be "
    "available for the current day",
)
@click.option(
    "--create-schema",
    is_flag=True,
    help="Create database schema if it does not exist",
)
def ingest(
    env,
    currency,
    start_block,
    end_block,
    batch_size,
    timeout,
    info,
    previous_day,
    create_schema,
):
    """Ingests cryptocurrency data form the client/node to the graphsense db
    \f
    Args:
        env (str): Environment to work on
        currency (str): currency to work on
    """
    schema_type = currency_to_schema_type.get(currency, "")
    if schema_type != "account":
        click.secho(
            "Ingest is not implemented for non account model currencies.", fg="red"
        )
        sys.exit(101)
    else:
        provider = config.get_keyspace_config(env, currency).node_reference

        if create_schema:
            GraphsenseSchemas().create_keyspace_if_not_exist(
                env, currency, keyspace_type="raw"
            )

        with DbFactory().from_config(env, currency) as db:
            ingest_eth(
                db=db,
                provider_uri=provider,
                user_start_block=start_block,
                user_end_block=end_block,
                batch_size=batch_size,
                info=info,
                previous_day=previous_day,
                w3_timeout=timeout,
            )
