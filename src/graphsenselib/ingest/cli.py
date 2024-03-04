import contextlib
import logging
import sys
from typing import Dict

import click
from filelock import FileLock
from filelock import Timeout as LockFileTimeout

from ..cli.common import require_currency, require_environment
from ..config import config, currency_to_schema_type
from ..db import DbFactory
from ..schema import GraphsenseSchemas
from ..utils import subkey_get
from .common import INGEST_SINKS
from .factory import IngestFactory
from .parquet import SCHEMA_MAPPING as PARQUET_SCHEMA_MAPPING

logger = logging.getLogger(__name__)


def create_sink_config(sink: str, network: str, ks_config: Dict):
    schema_type = currency_to_schema_type[network]
    sink_config = ks_config.ingest_config.dict().get("raw_keyspace_file_sinks", None)
    if (sink == "parquet" and schema_type.startswith("account")) or sink == "fs-cache":
        file_sink_dir = subkey_get(sink_config, f"{sink}.directory".split("."))
        if file_sink_dir is None:
            logger.warning(
                f"No {sink} file output directory "
                f"({sink}.directory) is configured for {network}. "
                "Ignoring sink."
            )
            return None

        sc = {"output_directory": file_sink_dir}

        if sink == "parquet":
            sc["schema"] = PARQUET_SCHEMA_MAPPING[schema_type]
        if sink == "fs-cache":
            sc["ignore_tables"] = ["trc10", "configuration"]
            if network == "trx":
                sc["key_by"] = {"fee": "tx_hash", "default": "block_id"}

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
    source_node_uri = ks_config.ingest_config.get_first_node_reference()
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
            provider_uri=source_node_uri,
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
