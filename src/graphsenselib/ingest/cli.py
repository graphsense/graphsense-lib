import logging
import sys

import click

from ..cli.common import require_currency, require_environment
from ..config import config, currency_to_schema_type
from ..db import DbFactory
from ..schema import GraphsenseSchemas
from .common import INGEST_SINKS
from .factory import IngestFactory
from .parquet import SCHEMA_MAPPING

logger = logging.getLogger(__name__)


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
    help="Web3 API timeout in seconds (default: 3600s)",
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
@click.option(
    "--mode",
    type=click.Choice(
        ["legacy", "utxo_with_tx_graph", "utxo_only_tx_graph"], case_sensitive=False
    ),
    help="Importer mode",
    default="legacy",
    multiple=False,
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
    mode,
):
    """Ingests cryptocurrency data form the client/node to the graphsense db
    \f
    Args:
        env (str): Environment to work on
        currency (str): currency to work on
    """
    ks_config = config.get_keyspace_config(env, currency)
    provider = ks_config.ingest_config.node_reference
    parquet_file_sink_config = ks_config.ingest_config.raw_keyspace_file_sinks.get(
        "parquet", None
    )

    if ks_config.schema_type == "account" and mode != "legacy":
        logger.error(
            "Only legacy mode is available for account type currencies. Exiting."
        )
        sys.exit(11)

    parquet_file_sink = (
        parquet_file_sink_config.directory
        if parquet_file_sink_config is not None
        else None
    )

    if create_schema:
        GraphsenseSchemas().create_keyspace_if_not_exist(
            env, currency, keyspace_type="raw"
        )

    def create_sink_config(sink, currency):
        schema_type = currency_to_schema_type[currency]
        return (
            {
                "output_directory": parquet_file_sink,
                "schema": SCHEMA_MAPPING[schema_type],
            }
            if sink == "parquet" and schema_type == "account"
            else {}
        )

    with DbFactory().from_config(env, currency) as db:
        IngestFactory().from_config(env, currency).ingest(
            db=db,
            currency=currency,
            source=provider,
            sink_config={k: create_sink_config(k, currency) for k in sinks},
            user_start_block=start_block,
            user_end_block=end_block,
            batch_size=batch_size,
            info=info,
            previous_day=previous_day,
            provider_timeout=timeout,
            mode=mode,
        )


@ingesting.command("to-csv")
@require_environment()
@require_currency(required=True)
@click.option(
    "--start-block",
    type=int,
    required=False,
    help="start block (default: 0)",
)
@click.option(
    "--end-block",
    type=int,
    required=False,
    help="end block (default: last available block)",
)
@click.option(
    "--continue",
    "continue_export",
    is_flag=True,
    help="continue from export from position",
)
@click.option(
    "--batch-size",
    type=int,
    default=10,
    help="number of blocks to export (write) at a time (default: 10)",
)
@click.option(
    "--file-batch-size",
    type=int,
    default=1000,
    help="number of blocks to export to a CSV file (default: 1000)",
)
@click.option(
    "--partition-batch-size",
    type=int,
    default=1_000_000,
    help="number of blocks to export in partition (default: 1_000_000)",
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
    "--info",
    is_flag=True,
    help="display block information and exit",
)
@click.option(
    "--timeout",
    type=int,
    required=False,
    default=3600,
    help="Web3 API timeout in seconds (default: 3600s)",
)
def export_csv(
    env,
    currency,
    start_block,
    end_block,
    continue_export,
    batch_size,
    file_batch_size,
    partition_batch_size,
    previous_day,
    info,
    timeout,
):
    """Exports raw cryptocurrency data to gziped csv files.
    \f
    Args:
        env (str): Environment to work on
        currency (str): currency to work on
    """

    if currency != "eth" and currency != "trx":
        logger.error("Csv export is only supported for eth/trx at the moment.")
        sys.exit(11)

    ks_config = config.get_keyspace_config(env, currency)
    provider = ks_config.ingest_config.node_reference
    csv_directory_config = ks_config.ingest_config.raw_keyspace_file_sinks.get(
        "csv", None
    )

    if csv_directory_config is None:
        logger.error(
            "Please provide an output directory in your "
            "config (raw_keyspace_file_sinks.csv.directory)"
        )
        sys.exit(11)

    csv_directory = csv_directory_config.directory

    from .account import export_csv

    with DbFactory().from_config(env, currency) as db:
        export_csv(
            db=db,
            currency=currency,
            provider_uri=provider,
            directory=csv_directory,
            user_start_block=start_block,
            user_end_block=end_block,
            continue_export=continue_export,
            batch_size=batch_size,
            file_batch_size=file_batch_size,
            partition_batch_size=partition_batch_size,
            info=info,
            previous_day=previous_day,
            provider_timeout=timeout,
        )
