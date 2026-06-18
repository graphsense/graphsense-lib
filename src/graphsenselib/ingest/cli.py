# flake8: noqa: T201
import logging
import sys
import time
from contextlib import ExitStack
from datetime import date, timedelta
from typing import Optional

import click

from graphsenselib.utils.DeltaTableConnector import DeltaTableConnector
from graphsenselib.utils.date import parse_older_than_run_spec
from graphsenselib.utils.locking import LockAcquisitionError, create_lock

from ..cli.common import require_currency, require_environment
from ..config import get_config, supported_fiat_currencies
from ..config.config import EXCHANGE_RATES_PROVIDERS, KeyspaceConfig
from ..db import DbFactory
from ..monitoring.monitoring import check_raw_ingest_staleness
from ..schema import GraphsenseSchemas
from ..utils import subkey_get
from .common import INGEST_SINKS
from .delta.sink import optimize_table, optimize_tables
from .dump import NothingToIngestError, export_delta
from .factory import IngestFactory

logger = logging.getLogger(__name__)


def _require_ingest_config(ks_config: KeyspaceConfig, currency: str):
    """Return the IngestConfig or exit if not configured."""
    ic = ks_config.ingest_config
    if ic is None:
        logger.error(f"No ingest_config for {currency}. Check your graphsense.yaml.")
        sys.exit(11)
    return ic


def create_sink_config(sink: str, network: str, ks_config: KeyspaceConfig):
    ic = ks_config.ingest_config
    sink_config = ic.model_dump().get("raw_keyspace_file_sinks", None) if ic else None
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
    "--no-lock",
    is_flag=True,
    help="Do not acquire a lock to avoid conflicting processes.",
)
@click.option(
    "--no-file-lock",
    is_flag=True,
    hidden=True,
    help="Deprecated alias for --no-lock.",
)
@click.option(
    "--sinks",
    type=click.Choice(INGEST_SINKS + ["delta"], case_sensitive=False),
    multiple=True,
    default=["cassandra"],
    help="Sinks to write to (default: cassandra). "
    "Can be specified multiple times, e.g. --sinks cassandra --sinks delta.",
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
    help="[legacy only] number of blocks to export (write) at a time. "
    "Ignored by the new pipeline (file batch size is fixed per currency).",
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
    "--write-mode",
    type=click.Choice(
        ["overwrite", "append", "merge"],
        case_sensitive=False,
    ),
    help="Write mode for delta sink (default: append).",
    default="append",
    multiple=False,
)
@click.option(
    "--ignore-overwrite-safechecks",
    is_flag=True,
    help="Ignore check in the overwrite mode that only lets you start at the "
    "beginning of a partition.",
)
@click.option(
    "--auto-compact",
    type=str,
    default=None,
    help="Run autocompation after ingest (delta sink only). Parameter "
    "controls age since last run and day the compaction should be run on, "
    "e.g. 7d;sunday means run on sundays iif the last compaction was more "
    "than 7 days ago days ago",
)
@click.option(
    "--auto-compact-last-n",
    type=int,
    default=10,
    help="When --auto-compact triggers, restrict compaction to the most "
    "recent N partitions of each table (default: 10). Older partitions are "
    "immutable raw data and don't accumulate small files. Has no effect on "
    "tables without a partition column (e.g. trc10).",
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
    "(default: 1). Note: version 1 has been removed for account chains (ETH/TRX).",
)
@click.option(
    "--staleness-threshold",
    type=int,
    default=None,
    help="Threshold in hours for the post-ingest staleness check. Overrides "
    "ingest_config.raw_ingest_staleness_threshold from graphsense.yaml. "
    "The check is skipped if neither is set.",
)
@click.option(
    "--no-staleness-check",
    is_flag=True,
    help="Skip the post-ingest staleness check even if a threshold is configured.",
)
@click.option(
    "--staleness-topic",
    type=str,
    default="exceptions",
    show_default=True,
    help="Notification topic the staleness warning is sent to.",
)
@click.option(
    "--exchange-rates-provider",
    type=click.Choice(EXCHANGE_RATES_PROVIDERS, case_sensitive=False),
    default=None,
    help="Ingest the latest exchange rates from this provider before "
    "ingesting blocks (with abort-on-gaps semantics). Overrides "
    "ingest_config.exchange_rates_provider from graphsense.yaml. "
    "The step is skipped if neither is set.",
)
@click.option(
    "--no-exchange-rates",
    is_flag=True,
    help="Skip the pre-ingest exchange-rates step even if a provider is configured.",
)
def ingest(
    env,
    currency,
    no_file_lock,
    no_lock,
    sinks,
    start_block,
    end_block,
    batch_size,
    timeout,
    info,
    previous_day,
    create_schema,
    write_mode,
    ignore_overwrite_safechecks,
    auto_compact,
    auto_compact_last_n,
    mode,
    version,
    staleness_threshold,
    no_staleness_check,
    staleness_topic,
    exchange_rates_provider,
    no_exchange_rates,
):
    """Ingests cryptocurrency data form the client/node to the graphsense db
    \f
    Args:
        env (str): Environment to work on
        currency (str): currency to work on
    """
    config = get_config()
    ks_config = config.get_keyspace_config(env, currency)
    ingest_cfg = _require_ingest_config(ks_config, currency)
    sources = ingest_cfg.all_node_references

    if batch_size != 10:
        logger.warning(
            "--batch-size is ignored by the new ingest pipeline. "
            "File batch size is fixed per currency."
        )

    use_legacy = config.legacy_ingest

    LEGACY_ONLY_MODES = {
        "utxo_only_tx_graph",
        "account_traces_only",
        "account_fees_only",
        "trx_update_transactions",
    }
    if not use_legacy and mode in LEGACY_ONLY_MODES:
        logger.error(f"Mode '{mode}' requires GRAPHSENSE_LEGACY_INGEST=true.")
        sys.exit(11)

    # Mode validation only applies to legacy paths
    if use_legacy:
        if mode != "legacy" and (
            (
                ks_config.schema_type in ["account", "account_trx"]
                and mode.startswith("utxo_")
            )
            or (ks_config.schema_type == "utxo" and not mode.startswith("utxo_"))
        ):
            logger.error(
                f"Mode {mode} is not available for "
                f"{ks_config.schema_type} type currencies. Exiting."
            )
            sys.exit(11)
        # Account chains require --version 2 in legacy mode
        if ks_config.schema_type in ["account", "account_trx"]:
            version = 2

    use_cassandra = "cassandra" in sinks
    use_delta = "delta" in sinks

    if auto_compact and not use_delta:
        logger.error("--auto-compact requires --sinks delta.")
        sys.exit(11)

    schema_tools = GraphsenseSchemas()
    ks_type = "raw"
    if use_cassandra:
        if create_schema:
            schema_tools.create_keyspace_if_not_exist(
                env, currency, keyspace_type=ks_type
            )
        logger.info("Apply migrations to raw keyspace if necessary")
        schema_tools.apply_migrations(env, currency, keyspace_type=ks_type)

    if not info and not no_exchange_rates:
        _run_exchange_rates_ingest(
            env=env,
            currency=currency,
            ingest_cfg=ingest_cfg,
            use_cassandra=use_cassandra,
            provider=exchange_rates_provider,
        )

    lock_disabled = no_lock or no_file_lock
    nothing_to_ingest = False
    try:
        with ExitStack() as stack:
            db = None
            if use_cassandra:
                db = stack.enter_context(DbFactory().from_config(env, currency))

            if not use_legacy:
                _run_new_ingest(
                    config,
                    ks_config,
                    db,
                    currency,
                    sources,
                    sinks,
                    start_block,
                    end_block,
                    timeout,
                    lock_disabled,
                    previous_day=previous_day,
                    info=info,
                    batch_size=batch_size,
                    write_mode=write_mode,
                    ignore_overwrite_safechecks=ignore_overwrite_safechecks,
                )

                if auto_compact and use_delta:
                    _run_auto_compact(
                        config=config,
                        ks_config=ks_config,
                        currency=currency,
                        auto_compact=auto_compact,
                        auto_compact_last_n=auto_compact_last_n,
                        lock_disabled=lock_disabled,
                    )
            else:
                if db is None:
                    logger.error(
                        "Legacy ingest requires Cassandra. Add --sinks cassandra."
                    )
                    sys.exit(11)
                assert db is not None
                logger.warning(
                    "DEPRECATED: The legacy ingest pipeline is deprecated and will be "
                    "removed in a future release. Unset GRAPHSENSE_LEGACY_INGEST to "
                    "use the new pipeline."
                )
                lock_name = db.raw.get_keyspace()
                with create_lock(lock_name, disabled=lock_disabled):
                    sink_configs = [
                        (k, create_sink_config(k, currency, ks_config)) for k in sinks
                    ]
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
    except LockAcquisitionError as e:
        logger.warning(str(e))
        sys.exit(911)
    except NothingToIngestError:
        # Still run the staleness check below: a node that stopped syncing
        # produces "no new blocks" forever, which is exactly the condition
        # the check must alert on. Exit code 12 is preserved for wrappers.
        nothing_to_ingest = True

    if not info and not no_staleness_check:
        _run_staleness_check(
            env=env,
            currency=currency,
            ingest_cfg=ingest_cfg,
            use_cassandra=use_cassandra,
            staleness_threshold=staleness_threshold,
            staleness_topic=staleness_topic,
        )

    if nothing_to_ingest:
        sys.exit(12)


def _run_new_ingest(
    config,
    ks_config,
    db,
    currency,
    sources,
    sinks,
    start_block,
    end_block,
    timeout,
    lock_disabled=False,
    previous_day=False,
    info=False,
    batch_size=None,
    write_mode="append",
    ignore_overwrite_safechecks=False,
):
    """Route from-node to the IngestRunner-based pipeline."""
    ic = ks_config.ingest_config
    delta_directory = None
    s3_credentials = None
    if "delta" in sinks:
        if ic is None:
            logger.error("Delta sink requested but no ingest_config configured.")
            sys.exit(11)
        pdc = ic.raw_keyspace_file_sinks.get("delta", None)
        if pdc is None:
            logger.error(
                "Delta sink requested but no delta directory configured "
                "(raw_keyspace_file_sinks.delta.directory)"
            )
            sys.exit(11)
        delta_directory = pdc.directory
        s3_credentials = config.get_s3_credentials(pdc.s3_config)

    source_max_workers = ic.source_max_workers if ic is not None else None

    export_delta(
        currency=currency,
        sources=sources,
        directory=delta_directory,
        start_block=start_block,
        end_block=end_block,
        provider_timeout=timeout,
        s3_credentials=s3_credentials,
        write_mode=write_mode,
        ignore_overwrite_safechecks=ignore_overwrite_safechecks,
        db=db if "cassandra" in sinks else None,
        lock_disabled=lock_disabled,
        previous_day=previous_day,
        info=info,
        source_max_workers=source_max_workers,
    )


def _run_auto_compact(
    config,
    ks_config,
    currency: str,
    auto_compact: str,
    auto_compact_last_n: int,
    lock_disabled: bool = False,
):
    """Check the auto-compact run-spec and run optimize_tables if it triggers."""
    from graphsenselib.utils.locking import delta_ingest_lock_name

    ic = _require_ingest_config(ks_config, currency)
    pdc = ic.raw_keyspace_file_sinks.get("delta", None)
    if pdc is None:
        logger.error("--auto-compact requires raw_keyspace_file_sinks.delta.directory.")
        sys.exit(11)

    parquet_directory = pdc.directory
    s3_credentials = config.get_s3_credentials(pdc.s3_config)

    lock_name = delta_ingest_lock_name(parquet_directory, currency)
    with create_lock(lock_name, disabled=lock_disabled):
        logger.info("Running auto-compaction check")
        last_vaccum_time = DeltaTableConnector(
            parquet_directory, s3_credentials
        ).get_last_completed_vacuum_date("block")

        if parse_older_than_run_spec(auto_compact, last_vaccum_time):
            logger.info(
                f"Auto-compaction conditions met, last compaction was "
                f"{last_vaccum_time}, running compaction"
            )
            optimize_tables(
                currency,
                parquet_directory,
                s3_credentials,
                mode="both",
                full_vacuum=True,
                last_n_partitions=auto_compact_last_n,
            )
        else:
            logger.info(
                f"Auto-compaction conditions not met, last compaction was "
                f"{last_vaccum_time}, skipping compaction"
            )


def _run_exchange_rates_ingest(
    env: str,
    currency: str,
    ingest_cfg,
    use_cassandra: bool,
    provider: Optional[str],
):
    """Pre-ingest exchange-rates update, replaces a separate `exchange-rates
    <provider> ingest --abort-on-gaps` run before every ingest.

    Aborts the whole command (SystemExit, same exit codes as the standalone
    command) if rates have gaps that cannot be filled."""
    provider = (provider or ingest_cfg.exchange_rates_provider or "").lower() or None
    if provider is None:
        return
    if not use_cassandra:
        logger.warning(
            "Skipping pre-ingest exchange-rates step: it writes to the "
            "Cassandra raw keyspace, but the cassandra sink is not enabled."
        )
        return

    # local imports: keep pandas/requests out of the ingest CLI import path
    from ..rates.cli import get_api_key

    prev_day = date.today() - timedelta(days=1)
    prev_date = prev_day.strftime("%Y-%m-%d")
    if provider == "coingecko":
        from ..rates.coingecko import MIN_START
        from ..rates.coingecko import ingest as ingest_rates

        api_key_args = [get_api_key(provider)]
    elif provider == "coinmarketcap":
        from ..rates.coinmarketcap import MIN_START
        from ..rates.coinmarketcap import ingest as ingest_rates

        api_key_args = [get_api_key(provider)]
    elif provider == "cryptocompare":
        from ..rates.cryptocompare import MIN_START
        from ..rates.cryptocompare import ingest as ingest_rates

        # cryptocompare works on offset-aware datetimes
        prev_date = prev_day.strftime("%Y-%m-%dT00:00:00.000000+00:00")
        api_key_args = [get_api_key(provider)]
    else:
        logger.error(
            f"Unknown exchange rates provider '{provider}'. "
            f"Supported: {EXCHANGE_RATES_PROVIDERS}."
        )
        sys.exit(11)

    logger.info(f"Ingesting {provider} exchange rates for {currency} up to {prev_date}")
    ingest_rates(
        env,
        currency,
        list(supported_fiat_currencies),
        MIN_START,
        prev_date,
        "exchange_rates",
        False,  # force
        False,  # dry_run
        True,  # abort_on_gaps
        *api_key_args,
    )


def _run_staleness_check(
    env: str,
    currency: str,
    ingest_cfg,
    use_cassandra: bool,
    staleness_threshold: Optional[int],
    staleness_topic: str,
):
    """Post-ingest staleness check, replaces a separate `monitoring
    monitor-raw-ingest` run after every ingest."""
    threshold = (
        staleness_threshold
        if staleness_threshold is not None
        else ingest_cfg.raw_ingest_staleness_threshold
    )
    if threshold is None:
        return
    if not use_cassandra:
        logger.warning(
            "Skipping post-ingest staleness check: it reads the Cassandra raw "
            "keyspace, but the cassandra sink is not enabled."
        )
        return
    logger.info(
        f"Checking raw keyspace staleness for {currency} (threshold {threshold}h)"
    )
    check_raw_ingest_staleness(env, currency, threshold, topic=staleness_topic)


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
@click.option(
    "--auto-compact",
    type=str,
    default=None,
    help="Run autocompation, paramater controls age since last run and day the compaction should be run on, e.g. 7d;sunday means run on sundays iif the last compaction was more than 7 days ago days ago",
)
@click.option(
    "--auto-compact-last-n",
    type=int,
    default=10,
    help="When --auto-compact triggers, restrict compaction to the most "
    "recent N partitions of each table (default: 10). Older partitions are "
    "immutable raw data and don't accumulate small files. Has no effect on "
    "tables without a partition column (e.g. trc10).",
)
@click.option(
    "--sinks",
    type=click.Choice(["delta", "cassandra"], case_sensitive=False),
    multiple=True,
    default=["delta"],
    help="Sinks to write to (default: delta). "
    "Can be specified multiple times, e.g. --sinks delta --sinks cassandra.",
)
def dump_rawdata(
    env,
    currency,
    start_block,
    end_block,
    timeout,
    write_mode,
    ignore_overwrite_safechecks,
    auto_compact,
    auto_compact_last_n,
    sinks,
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
    ingest_cfg = _require_ingest_config(ks_config, currency)
    sources = ingest_cfg.all_node_references
    parquet_directory_config = ingest_cfg.raw_keyspace_file_sinks.get("delta", None)

    if parquet_directory_config is None:
        logger.error(
            "Please provide an output directory in your "
            "config (raw_keyspace_file_sinks.delta.directory)"
        )
        sys.exit(11)

    parquet_directory = parquet_directory_config.directory
    s3_credentials = config.get_s3_credentials(parquet_directory_config.s3_config)

    use_cassandra = "cassandra" in sinks
    use_delta = "delta" in sinks

    if not use_delta:
        logger.error("Delta sink is currently required.")
        sys.exit(11)

    if use_cassandra:
        schema_tools = GraphsenseSchemas()
        schema_tools.create_keyspace_if_not_exist(env, currency, keyspace_type="raw")
        logger.info("Apply migrations to raw keyspace if necessary")
        schema_tools.apply_migrations(env, currency, keyspace_type="raw")

    # Use a single lock for both ingest and auto-compact to prevent a
    # concurrent process from starting between the two operations.
    from graphsenselib.utils.locking import delta_ingest_lock_name

    lock_name = delta_ingest_lock_name(parquet_directory, currency)
    try:
        with create_lock(lock_name):
            with ExitStack() as stack:
                db = None
                if use_cassandra:
                    db = stack.enter_context(DbFactory().from_config(env, currency))

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
                    db=db,
                    lock_disabled=True,
                    source_max_workers=ingest_cfg.source_max_workers,
                )

            if auto_compact:
                # Compact only recently-touched partitions. Older partitions
                # are immutable raw data and don't accumulate small files
                # between runs.
                _run_auto_compact(
                    config=config,
                    ks_config=ks_config,
                    currency=currency,
                    auto_compact=auto_compact,
                    auto_compact_last_n=auto_compact_last_n,
                    lock_disabled=True,
                )
    except LockAcquisitionError as e:
        logger.warning(str(e))
        sys.exit(911)


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
@click.option(
    "--full-vacuum",
    is_flag=True,
    help="Perform a full vacuum of the deltalake tables (default: False)",
    required=False,
)
@click.option(
    "--last-n-partitions",
    type=int,
    default=None,
    help="Restrict compaction to the most recent N partitions of each table. "
    "Older partitions are immutable raw data and don't accumulate small files. "
    "Has no effect on tables without a partition column (e.g. trc10).",
    required=False,
)
def optimize_deltalake(
    env,
    currency,
    mode="both",
    table=None,
    full_vacuum=False,
    last_n_partitions=None,
):
    """Optimize the deltalake tables
    \f
    Args:
        env (str): Environment to work on
        currency (str): currency to work on
    """
    config = get_config()
    ks_config = config.get_keyspace_config(env, currency)
    ingest_cfg = _require_ingest_config(ks_config, currency)
    parquet_directory_config = ingest_cfg.raw_keyspace_file_sinks.get("delta", None)

    if parquet_directory_config is None:
        logger.error(
            "Please provide an output directory in your "
            "config (raw_keyspace_file_sinks.delta.directory)"
        )
        sys.exit(11)

    logger.info(f"Optimizing deltalake tables in {parquet_directory_config.directory}")
    parquet_directory = parquet_directory_config.directory
    s3_credentials = config.get_s3_credentials(parquet_directory_config.s3_config)
    from graphsenselib.utils.locking import delta_ingest_lock_name

    lock_name = delta_ingest_lock_name(parquet_directory, currency)
    try:
        with create_lock(lock_name):
            if table is None:
                optimize_tables(
                    currency,
                    parquet_directory,
                    s3_credentials,
                    mode=mode,
                    full_vacuum=full_vacuum,
                    last_n_partitions=last_n_partitions,
                )
                logger.info(f"Optimized deltalake tables in {parquet_directory}")
            else:
                optimize_table(
                    parquet_directory,
                    table,
                    s3_credentials,
                    mode=mode,
                    full_vacuum=full_vacuum,
                    last_n_partitions=last_n_partitions,
                )
                logger.info(f"Optimized deltalake table {table} in {parquet_directory}")
    except LockAcquisitionError as e:
        logger.warning(str(e))
        sys.exit(911)


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
    ingest_cfg = _require_ingest_config(ks_config, currency)
    parquet_directory_config = ingest_cfg.raw_keyspace_file_sinks.get("delta", None)

    if parquet_directory_config is None:
        logger.error(
            "Please provide an output directory in your "
            "config (raw_keyspace_file_sinks.delta.directory)"
        )
        sys.exit(11)

    logger.info(f"Querying deltalake tables in {parquet_directory_config.directory}")
    parquet_directory = parquet_directory_config.directory
    s3_credentials = config.get_s3_credentials(parquet_directory_config.s3_config)

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
