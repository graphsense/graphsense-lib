import click

from ..cli.common import require_currency, require_environment
from ..config import config
from ..db import DbFactory
from ..schema import GraphsenseSchemas
from .common import INGEST_SINKS
from .factory import IngestFactory


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
    "--sinks",
    type=click.Choice(INGEST_SINKS, case_sensitive=False),
    multiple=True,
    default=["cassandra"],
    help="Where the raw data is written to currently"
    " either cassandra, parquet or both (default: cassandra)",
)
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
    help="number of blocks to export (write) at a time (default: 10)"
    "esp. export to parquet benefits from larger values.",
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
    sinks,
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
    ks_config = config.get_keyspace_config(env, currency)
    provider = ks_config.ingest_config.node_reference
    parquet_file_sink = ks_config.ingest_config.raw_keyspace_file_sink_directory

    if create_schema:
        GraphsenseSchemas().create_keyspace_if_not_exist(
            env, currency, keyspace_type="raw"
        )

    def create_sink_config(sink):
        return {"output_directory": parquet_file_sink} if sink == "parquet" else {}

    with DbFactory().from_config(env, currency) as db:
        IngestFactory().from_config(env, currency).ingest(
            db=db,
            source=provider,
            sink_config={k: create_sink_config(k) for k in sinks},
            user_start_block=start_block,
            user_end_block=end_block,
            batch_size=batch_size,
            info=info,
            previous_day=previous_day,
            provider_timeout=timeout,
        )
