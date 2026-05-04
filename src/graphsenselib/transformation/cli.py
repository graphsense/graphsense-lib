"""CLI for PySpark-based Delta Lake → Cassandra transformation."""

import logging

import click
import pyspark  # noqa: F401 — trigger ImportError early if pyspark not installed

from graphsenselib.cli.common import require_currency, require_environment
from graphsenselib.schema import GraphsenseSchemas

logger = logging.getLogger(__name__)


@click.group()
def transformation_cli():
    pass


@transformation_cli.group("transformation")
def transformation():
    """Transform Delta Lake tables into Cassandra raw keyspace via PySpark."""
    pass


def _log_startup_banner(
    *,
    env,
    currency,
    delta_lake_path,
    s3_credentials,
    raw_keyspace,
    raw_keyspace_overridden,
    cassandra_nodes,
    start_block,
    end_block,
    top_block,
    local,
    patch=False,
):
    from urllib.parse import urlparse

    from graphsenselib.config import currency_to_schema_type

    # Parse bucket / scheme from delta_lake_path. s3://bucket/path → bucket.
    parsed = urlparse(delta_lake_path)
    if parsed.scheme in ("s3", "s3a"):
        bucket = parsed.netloc or "?"
        delta_loc = f"{parsed.scheme}://{bucket}{parsed.path}"
    else:
        bucket = None
        delta_loc = delta_lake_path
    s3_endpoint = (s3_credentials or {}).get("AWS_ENDPOINT_URL")

    keyspace_label = raw_keyspace + ("  (override)" if raw_keyspace_overridden else "")
    schema_type = currency_to_schema_type.get(currency, "?")

    lines = [
        "=" * 72,
        "PySpark Delta Lake -> Cassandra raw transformation",
        "=" * 72,
        f"  env              : {env}",
        f"  currency         : {currency}  (schema={schema_type})",
        f"  source delta     : {delta_loc}",
    ]
    if bucket is not None:
        lines.append(f"  s3 bucket        : {bucket}")
    if s3_endpoint:
        lines.append(f"  s3 endpoint      : {s3_endpoint}")
    lines += [
        f"  target keyspace  : {keyspace_label}",
        f"  cassandra nodes  : {', '.join(cassandra_nodes)}",
        f"  start block      : {start_block}",
        f"  end block        : {end_block}",
        f"  top block        : {top_block}",
        f"  spark mode       : {'local[*]' if local else 'cluster'}",
        f"  patch mode       : {'on' if patch else 'off'}",
        "=" * 72,
    ]
    logger.info("\n" + "\n".join(lines))


@transformation.command("run", short_help="Run Delta Lake → Cassandra transformation.")
@require_environment()
@require_currency()
@click.option(
    "--start-block",
    type=int,
    default=0,
    show_default=True,
    help="Start block (inclusive).",
)
@click.option(
    "--end-block",
    type=int,
    default=None,
    help="End block (inclusive). If omitted, auto-detected from Delta Lake.",
)
@click.option(
    "--create-schema",
    is_flag=True,
    help="Create Cassandra keyspace/tables if they do not exist.",
)
@click.option(
    "--raw-keyspace",
    "raw_keyspace_override",
    type=str,
    default=None,
    help=(
        "Override the target raw Cassandra keyspace name (default: from "
        "graphsense.yaml). Use to write into a fresh/dated keyspace while "
        "continuous ingest and delta-update keep using the YAML name."
    ),
)
@click.option(
    "--delta-lake-path",
    type=str,
    default=None,
    help="Override Delta Lake base path (default: from config).",
)
@click.option(
    "--local",
    is_flag=True,
    help="Run Spark in local mode with local[*].",
)
@click.option(
    "--debug-write-audit",
    is_flag=True,
    help=(
        "Before each Cassandra write, run an extra aggregation that logs "
        "per-Spark-partition row counts and partition-key skew. Use to "
        "diagnose stragglers. Adds one shuffle per write."
    ),
)
@click.option(
    "--patch",
    is_flag=True,
    help=(
        "Allow writing into a non-empty target keyspace. Existing rows in the "
        "[start-block, end-block] range are overwritten by PK upsert; rows "
        "outside the range are untouched. Account chains only (eth, trx); "
        "rejected for UTXO chains because their derived spend tables are not "
        "window-local."
    ),
)
def run_transformation(
    env,
    currency,
    start_block,
    end_block,
    create_schema,
    raw_keyspace_override,
    delta_lake_path,
    local,
    debug_write_audit,
    patch,
):
    """Run PySpark transformation from Delta Lake to Cassandra raw keyspace.

    For cluster mode, Spark workers must have Python >= 3.10 (matching the driver).
    Install via: uv python install 3.11 on each worker node, then set
    spark.pyspark.python in spark_config.
    \f
    """
    from graphsenselib.config import currency_to_schema_type, get_config

    config = get_config()
    env_config = config.get_environment(env)
    ks_config = config.get_keyspace_config(env, currency)

    raw_keyspace = raw_keyspace_override or ks_config.raw_keyspace_name

    schema_type = currency_to_schema_type.get(currency)
    if patch and schema_type not in ("account", "account_trx"):
        raise click.ClickException(
            f"--patch is only supported for account chains (got {currency}, "
            f"schema_type={schema_type}). UTXO derived tables "
            f"(transaction_spending, transaction_spent_in) are computed across "
            f"the full block range loaded by Spark; a partial rerun would "
            f"silently miss spend links whose two endpoints straddle the "
            f"window boundary. Re-run from a fresh keyspace instead."
        )

    # Schema creation runs BEFORE Spark (uses cassandra-driver, no Java needed)
    if create_schema:
        logger.info(f"Creating Cassandra schema for {raw_keyspace} if not exists...")
        GraphsenseSchemas().create_keyspace_if_not_exist(
            env,
            currency,
            keyspace_type="raw",
            keyspace_name_override=raw_keyspace_override,
        )
        GraphsenseSchemas().apply_migrations(
            env,
            currency,
            keyspace_type="raw",
            keyspace_name_override=raw_keyspace_override,
        )
    cassandra_nodes = env_config.cassandra_nodes
    cassandra_username = env_config.username
    cassandra_password = env_config.password

    # Safety check: verify the target keyspace block table is empty
    # to prevent accidental data corruption from mixing sources. Skipped
    # when --patch is set (account chains only — guarded above).
    if not patch:
        from cassandra.cluster import Cluster as CassCluster

        host, _, port = cassandra_nodes[0].partition(":")
        cass_port = int(port) if port else 9042
        auth_provider = None
        if cassandra_username and cassandra_password:
            from cassandra.auth import PlainTextAuthProvider

            auth_provider = PlainTextAuthProvider(
                username=cassandra_username, password=cassandra_password
            )
        with CassCluster(
            [host], port=cass_port, auth_provider=auth_provider
        ) as cluster:
            session = cluster.connect()
            rows = list(
                session.execute(
                    "SELECT table_name FROM system_schema.tables "
                    "WHERE keyspace_name = %s",
                    (raw_keyspace,),
                )
            )
            if rows:
                # Keyspace exists — check if block table has data
                block_row = session.execute(
                    f"SELECT block_id FROM {raw_keyspace}.block LIMIT 1"  # noqa: S608
                ).one()
                if block_row is not None:
                    raise click.ClickException(
                        f"Keyspace {raw_keyspace} already contains data "
                        f"(block table is not empty). Use a fresh keyspace, "
                        f"truncate existing tables, or pass --patch to "
                        f"overwrite the requested block range (account "
                        f"chains only)."
                    )

    # Resolve delta path and S3 credentials from config if not overridden
    s3_config_name = None
    if delta_lake_path is None:
        ingest_cfg = ks_config.ingest_config
        if ingest_cfg and ingest_cfg.raw_keyspace_file_sinks:
            delta_sink = ingest_cfg.raw_keyspace_file_sinks.get("delta")
            if delta_sink:
                delta_lake_path = delta_sink.directory
                s3_config_name = delta_sink.s3_config
        if delta_lake_path is None:
            raise click.UsageError(
                "No --delta-lake-path provided and no delta sink configured "
                f"for {currency} in environment {env}."
            )

    s3_credentials = config.get_s3_credentials(s3_config_name)
    spark_config = config.spark_config or {}

    from graphsenselib.ingest.delta.sink import delta_lake_highest_block
    from graphsenselib.utils.locking import create_lock, delta_ingest_lock_name

    delta_lock_name = delta_ingest_lock_name(delta_lake_path, currency)
    transformed_keyspace = ks_config.transformed_keyspace_name

    # Phase 1: pin a top-block snapshot under the delta-ingest lock so
    # concurrent ingest writes past this boundary cannot tear our read.
    # Block rows are committed last in each ingest batch, so any
    # block_id <= top is guaranteed to have its dependent rows committed.
    with create_lock(delta_lock_name):
        top_block = delta_lake_highest_block(delta_lake_path, s3_credentials)
    if top_block is None:
        raise click.ClickException(
            f"Cannot pin top-block: block Delta table at {delta_lake_path}/block is empty."
        )
    if end_block is None or end_block > top_block:
        end_block = top_block

    _log_startup_banner(
        env=env,
        currency=currency,
        delta_lake_path=delta_lake_path,
        s3_credentials=s3_credentials,
        raw_keyspace=raw_keyspace,
        raw_keyspace_overridden=raw_keyspace_override is not None,
        cassandra_nodes=cassandra_nodes,
        start_block=start_block,
        end_block=end_block,
        top_block=top_block,
        local=local,
        patch=patch,
    )

    # Deferred PySpark import
    from graphsenselib.transformation.factory import run as run_factory

    # Phase 2: hold the transformed-keyspace lock for the Spark run so
    # only one transformation writes to a given transformed keyspace at
    # a time. Ingest is not blocked: the delta-ingest lock from phase 1
    # has already been released.
    with create_lock(transformed_keyspace):
        run_factory(
            env=env,
            currency=currency,
            delta_lake_path=delta_lake_path,
            cassandra_nodes=cassandra_nodes,
            cassandra_username=cassandra_username,
            cassandra_password=cassandra_password,
            raw_keyspace=raw_keyspace,
            start_block=start_block,
            end_block=end_block,
            local=local,
            s3_credentials=s3_credentials,
            spark_config=spark_config,
            debug_write_audit=debug_write_audit,
        )


@transformation.command("cluster", short_help="Run one-off UTXO address clustering.")
@require_environment()
@require_currency()
@click.option(
    "--start-block",
    type=int,
    default=0,
    show_default=True,
    help="Start block (inclusive).",
)
@click.option(
    "--end-block",
    type=int,
    default=None,
    help="End block (inclusive). If omitted, auto-detected from the raw keyspace.",
)
@click.option(
    "--chunk-size",
    type=int,
    default=1000,
    show_default=True,
    help="Block-range chunk size for the Cassandra read/feed loop.",
)
@click.option(
    "--concurrency",
    type=int,
    default=100,
    show_default=True,
    help="Max in-flight Cassandra statements per chunk.",
)
@click.option(
    "--write-chunk",
    type=int,
    default=100_000,
    show_default=True,
    help="Number of mapping rows per Cassandra write slice.",
)
def run_clustering(
    env, currency, start_block, end_block, chunk_size, concurrency, write_chunk
):
    """Run one-off UTXO address clustering directly from the raw Cassandra keyspace.

    Reads transactions via point/range queries in ``--chunk-size``-block chunks,
    feeds them to the Rust clustering engine, and streams the resulting mapping
    back to ``fresh_address_cluster`` / ``fresh_cluster_addresses`` in the
    transformed keyspace.  No PySpark dependency.

    The transformed keyspace must already be seeded by the Scala transformation
    (or a prior run) so that ``summary_statistics.no_addresses`` is populated.
    \f
    """
    from graphsenselib.config import is_fresh_clustering_enabled
    from graphsenselib.db.factory import DbFactory
    from graphsenselib.schema.schema import GraphsenseSchemas
    from graphsenselib.transformation.clustering import (
        run_clustering_one_off_from_cassandra,
    )

    if not is_fresh_clustering_enabled():
        raise click.ClickException(
            "Fresh clustering is disabled. Set "
            "GRAPHSENSE_FRESH_CLUSTERING_ENABLED=true to enable."
        )

    # Ensure transformed keyspace schema is up to date
    GraphsenseSchemas().apply_migrations(env, currency, keyspace_type="transformed")

    with DbFactory().from_config(env, currency) as db:
        if end_block is None:
            end_block = db.raw.get_highest_block()
            if end_block is None:
                raise click.ClickException(
                    f"Cannot auto-detect end_block: raw keyspace for "
                    f"{currency} in environment {env} appears empty."
                )
            logger.info(f"Auto-detected end_block={end_block} from raw keyspace.")

        logger.info(
            f"Starting clustering: env={env}, currency={currency}, "
            f"blocks={start_block}-{end_block}, chunk_size={chunk_size}, "
            f"concurrency={concurrency}"
        )

        run_clustering_one_off_from_cassandra(
            db,
            start_block=start_block,
            end_block=end_block,
            chunk_size=chunk_size,
            concurrency=concurrency,
            write_chunk=write_chunk,
        )

    logger.info("One-off clustering complete.")
