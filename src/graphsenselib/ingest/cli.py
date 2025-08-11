# flake8: noqa: T201
import contextlib
import logging
import sys
import time
from typing import Dict

import click
from filelock import FileLock
from filelock import Timeout as LockFileTimeout

from graphsenselib.utils.DeltaTableConnector import DeltaTableConnector

from ..cli.common import require_currency, require_environment
from ..config import get_config
from ..db import DbFactory
from ..schema import GraphsenseSchemas
from ..utils import subkey_get
from .common import INGEST_SINKS
from .delta.sink import optimize_table, optimize_tables
from .dump import export_delta
from .factory import IngestFactory

logger = logging.getLogger(__name__)


def create_sink_config(sink: str, network: str, ks_config: Dict):
    sink_config = ks_config.ingest_config.model_dump().get(
        "raw_keyspace_file_sinks", None
    )
    if sink == "parquet":
        file_sink_dir = subkey_get(sink_config, f"{sink}.directory".split("."))
        if file_sink_dir is None:
            logger.warning(
                f"No {sink} file output directory "
                f"({sink}.directory) is configured for {network}. "
                "Ignoring sink."
            )
            return None

        sc = {"output_directory": file_sink_dir}

        return sc
    else:
        return {}


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
    " either cassandra, parquet, or multiple (default: cassandra)",
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
    help="number of blocks to export (write) at a time (default: 10)",
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
        [
            "legacy",
            "utxo_with_tx_graph",
            "utxo_only_tx_graph",
            "account_traces_only",
            "account_fees_only",
            "trx_update_transactions",
        ],
        case_sensitive=False,
    ),
    help="Importer mode",
    default="legacy",
    multiple=False,
)
@click.option(
    "--version",
    type=int,
    default=1,
    help="Which version of the ingest to use, 1: legacy (sequential), 2:parallel"
    "(default: 1)",
)
@click.option(
    "--no-file-lock",
    is_flag=True,
    help="Do not set file lock to avoid conflicting ingests.",
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
    version,
    no_file_lock,
):
    """Ingests cryptocurrency data form the client/node to the graphsense db
    \f
    Args:
        env (str): Environment to work on
        currency (str): currency to work on
    """
    config = get_config()
    ks_config = config.get_keyspace_config(env, currency)
    sources = ks_config.ingest_config.all_node_references

    if (
        (
            ks_config.schema_type in ["account", "account_trx"]
            and mode.startswith("utxo_")
        )
        or ks_config.schema_type == "utxo"
        and not mode.startswith("utxo_")
    ):
        logger.error(
            f"Mode {mode} is not available for "
            f"{ks_config.schema_type} type currencies. Exiting."
        )
        sys.exit(11)

    if create_schema:
        GraphsenseSchemas().create_keyspace_if_not_exist(
            env, currency, keyspace_type="raw"
        )

    sink_configs = [(k, create_sink_config(k, currency, ks_config)) for k in sinks]

    with DbFactory().from_config(env, currency) as db:
        try:
            lockfile_name = (
                f"/tmp/{db.raw.get_keyspace()}_{db.transformed.get_keyspace()}.lock"
            )
            if not no_file_lock:
                logger.info(f"Try acquiring lockfile {lockfile_name}")
            with (
                contextlib.nullcontext()
                if no_file_lock
                else FileLock(lockfile_name, timeout=1)
            ):
                IngestFactory().from_config(env, currency, version).ingest(
                    db=db,
                    currency=currency,
                    sources=sources,
                    sink_config={k: v for k, v in sink_configs if v is not None},
                    user_start_block=start_block,
                    user_end_block=end_block,
                    batch_size=batch_size,
                    info=info,
                    previous_day=previous_day,
                    provider_timeout=timeout,
                    mode=mode,
                )
        except LockFileTimeout:
            logger.error(
                f"Lockfile {lockfile_name} could not be acquired. "
                "Is another ingest running? If not delete the lockfile."
            )
            sys.exit(911)


@ingesting.group("delta-lake")
def deltalake():
    """Ingesting raw cryptocurrency data from nodes into the graphsense deltalake"""
    pass


@deltalake.command("ingest")
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
    "--timeout",
    type=int,
    required=False,
    default=3600,
    help="Web3 API timeout in seconds (default: 3600s)",
)
@click.option(
    "--write-mode",
    type=click.Choice(
        [
            "overwrite",
            "append",
            "merge",
        ],
        case_sensitive=False,
    ),
    help="Write mode for the parquet files overwrite/append/merge (default: overwrite)",
    default="overwrite",
    multiple=False,
)
@click.option(
    "--ignore-overwrite-safechecks",
    is_flag=True,
    help="Ignore check in the overwrite mode that only lets you start at the "
    "beginning of a partition",
)
def dump_rawdata(
    env,
    currency,
    start_block,
    end_block,
    timeout,
    write_mode,
    ignore_overwrite_safechecks,
):
    """Exports raw cryptocurrency data to parquet files either to s3
    or a local directory.
    \f
    Args:
        env (str): Environment to work on
        currency (str): currency to work on
    """
    config = get_config()
    logger.info(f"Dumping raw data for {currency} in {env}")

    ks_config = config.get_keyspace_config(env, currency)
    sources = ks_config.ingest_config.all_node_references
    parquet_directory_config = ks_config.ingest_config.raw_keyspace_file_sinks.get(
        "delta", None
    )
    s3_credentials = config.get_s3_credentials()

    if parquet_directory_config is None:
        logger.error(
            "Please provide an output directory in your "
            "config (raw_keyspace_file_sinks.delta.directory)"
        )
        sys.exit(11)

    parquet_directory = parquet_directory_config.directory

    export_delta(
        currency=currency,
        sources=sources,
        directory=parquet_directory,
        start_block=start_block,
        end_block=end_block,
        provider_timeout=timeout,
        s3_credentials=s3_credentials,
        write_mode=write_mode,
        ignore_overwrite_safechecks=ignore_overwrite_safechecks,
    )


# optimize deltalake
@deltalake.command("optimize")
@require_environment()
@require_currency(required=True)
@click.option(
    "--mode",
    type=click.Choice(
        [
            "both",
            "compact",
            "vacuum",
        ],
        case_sensitive=False,
    ),
    help="Optimization mode for the deltalake tables (default: both)",
    default="both",
    multiple=False,
)
@click.option(
    "--table",
    type=str,
    help="Specific table to optimize (default: all tables)",
    required=False,
)
def optimize_deltalake(env, currency, mode="both", table=None):
    """Optimize the deltalake tables
    \f
    Args:
        env (str): Environment to work on
        currency (str): currency to work on
    """
    config = get_config()
    ks_config = config.get_keyspace_config(env, currency)
    parquet_directory_config = ks_config.ingest_config.raw_keyspace_file_sinks.get(
        "delta", None
    )

    if parquet_directory_config is None:
        logger.error(
            "Please provide an output directory in your "
            "config (raw_keyspace_file_sinks.delta.directory)"
        )
        sys.exit(11)

    logger.info(f"Optimizing deltalake tables in {parquet_directory_config.directory}")
    parquet_directory = parquet_directory_config.directory
    s3_credentials = config.get_s3_credentials()
    if table is None:
        optimize_tables(currency, parquet_directory, s3_credentials, mode=mode)
        logger.info(f"Optimized deltalake tables in {parquet_directory}")
    else:
        optimize_table(parquet_directory, table, s3_credentials, mode=mode)
        logger.info(f"Optimized deltalake table {table} in {parquet_directory}")


# show data from the delta lake


# optimize deltalake
@deltalake.command("query")
@require_environment()
@require_currency(required=True)
@click.option(
    "--table",
    type=str,
    help="Specific table to optimize (default: all tables). Only the fee and trc10 "
    "tables of trx cant be queried since they do not have a block_id",
    required=False,
)
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
    "--outfile",
    type=str,
    help="Specify where to save the file, if not specified, it will be printed",
)
def query_deltalake(env, currency, table, start_block, end_block, outfile):
    """Query the deltalake tables
    \f
    Args:
        env (str): Environment to work on
        currency (str): currency to work on
    """
    config = get_config()
    ks_config = config.get_keyspace_config(env, currency)
    parquet_directory_config = ks_config.ingest_config.raw_keyspace_file_sinks.get(
        "delta", None
    )

    if parquet_directory_config is None:
        logger.error(
            "Please provide an output directory in your "
            "config (raw_keyspace_file_sinks.delta.directory)"
        )
        sys.exit(11)

    logger.info(f"Querying deltalake tables in {parquet_directory_config.directory}")
    parquet_directory = parquet_directory_config.directory
    s3_credentials = config.get_s3_credentials()

    block_ids = list(range(start_block, end_block + 1))

    dtc = DeltaTableConnector(parquet_directory, s3_credentials)
    time_start = time.time()

    data = dtc.get_items(table, block_ids)

    data = dtc.make_displayable(data)
    logger.debug(
        f"Queried deltalake table {table} in {parquet_directory} in"
        f" {time.time() - time_start} seconds"
    )

    print(data)

    if outfile:
        data.to_csv(outfile, index=False)
