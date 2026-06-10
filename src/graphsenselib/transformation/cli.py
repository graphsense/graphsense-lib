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
    "--s3-config",
    "s3_config_name",
    type=str,
    default=None,
    help=(
        "Name of the s3_configs entry to use for S3/MinIO credentials. "
        "Required when the Delta Lake path is on s3://."
    ),
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
    s3_config_name,
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

    # Resolve delta path from config if not overridden. S3 credentials are
    # selected explicitly via --s3-config (not auto-derived from the sink) so
    # the user picks read-time credentials independently of write-time config.
    if delta_lake_path is None:
        ingest_cfg = ks_config.ingest_config
        if ingest_cfg and ingest_cfg.raw_keyspace_file_sinks:
            delta_sink = ingest_cfg.raw_keyspace_file_sinks.get("delta")
            if delta_sink:
                delta_lake_path = delta_sink.directory
        if delta_lake_path is None:
            raise click.UsageError(
                "No --delta-lake-path provided and no delta sink configured "
                f"for {currency} in environment {env}."
            )

    is_s3_path = delta_lake_path.startswith("s3://") or delta_lake_path.startswith(
        "s3a://"
    )
    if is_s3_path and s3_config_name is None:
        available = sorted(config.s3_configs.keys())
        if not available:
            raise click.UsageError(
                f"Delta Lake path {delta_lake_path} is on S3 but no s3_configs "
                "are defined in the graphsense config. Add at least one named "
                "entry under s3_configs and pass --s3-config NAME."
            )
        raise click.UsageError(
            f"Delta Lake path {delta_lake_path} is on S3 but --s3-config was "
            f"not provided. Available s3_configs: {', '.join(available)}."
        )

    s3_credentials = config.get_s3_credentials(s3_config_name)
    spark_config = config.get_spark_config()
    spark_packages = config.get_spark_packages()

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
            spark_packages=spark_packages,
            debug_write_audit=debug_write_audit,
        )


@transformation.command("cluster", short_help="Run one-off UTXO address clustering.")
@require_environment()
@require_currency()
@click.option(
    "--local",
    is_flag=True,
    help="Run Spark in local mode (local[*]) instead of submitting to the cluster.",
)
@click.option(
    "--read-partitions",
    type=int,
    default=None,
    help=(
        "Partitions the edge-set DataFrame is coalesced to before streaming to the "
        "driver (one Arrow blob each). Raise it if a partition exceeds "
        "spark.driver.maxResultSize or executor memory is tight on large chains "
        "(default 64). Does NOT control join parallelism."
    ),
)
@click.option(
    "--end-block",
    type=int,
    default=None,
    help=(
        "Cluster the chain only up to this block (inclusive); transactions in "
        "later blocks are ignored. Omit to cluster the whole transaction table. "
        "There is no start bound — clustering is transitive over full history."
    ),
)
def run_clustering(env, currency, local, read_partitions, end_block):
    """Run one-off UTXO address clustering with PySpark.

    Bulk-reads raw.transaction and address_ids_by_address_prefix via parallel
    token-range scans, clusters multi-input transactions with the Rust Union-Find,
    and bulk-writes fresh_address_cluster / fresh_cluster_addresses /
    fresh_cluster_stats via the Spark Cassandra connector. Clusters the whole
    transaction table, or only blocks up to --end-block when given.

    The transformed keyspace must already be seeded (Scala transformation or a
    prior run) so summary_statistics.no_addresses is populated.
    \f
    """
    from graphsenselib.config import get_config, is_fresh_clustering_enabled
    from graphsenselib.db.factory import DbFactory
    from graphsenselib.schema.schema import GraphsenseSchemas
    from graphsenselib.transformation.clustering import run_clustering_spark
    from graphsenselib.transformation.spark import create_spark_session

    if not is_fresh_clustering_enabled():
        raise click.ClickException(
            "Fresh clustering is disabled. Set "
            "GRAPHSENSE_FRESH_CLUSTERING_ENABLED=true to enable."
        )

    GraphsenseSchemas().apply_migrations(env, currency, keyspace_type="transformed")

    with DbFactory().from_config(env, currency) as db:
        config = get_config()
        env_config = config.get_environment(env)
        ks_config = config.get_keyspace_config(env, currency)
        raw_keyspace = ks_config.raw_keyspace_name
        transformed_keyspace = ks_config.transformed_keyspace_name

        stats = db.transformed.get_summary_statistics()
        if stats is None or getattr(stats, "no_addresses", None) is None:
            raise click.ClickException(
                f"{transformed_keyspace}.summary_statistics.no_addresses is "
                "missing — seed the transformed keyspace before clustering."
            )
        max_address_id = int(stats.no_addresses)

        logger.info(
            f"Starting Spark clustering: env={env}, currency={currency}, "
            f"raw={raw_keyspace}, transformed={transformed_keyspace}"
        )
        spark_session = create_spark_session(
            app_name=f"graphsense-clustering-{currency}-{env}",
            local=local,
            cassandra_nodes=env_config.cassandra_nodes,
            cassandra_username=env_config.username,
            cassandra_password=env_config.password,
            spark_config=config.get_spark_config(),
            spark_packages=config.get_spark_packages(),
        )
        try:
            spark_kwargs = {}
            if read_partitions is not None:
                spark_kwargs["read_partitions"] = read_partitions
            run_clustering_spark(
                spark_session,
                raw_keyspace=raw_keyspace,
                transformed_keyspace=transformed_keyspace,
                max_address_id=max_address_id,
                end_block=end_block,
                **spark_kwargs,
            )
        finally:
            spark_session.stop()
            logger.info("SparkSession stopped.")
        logger.info("One-off clustering complete.")


@transformation.command(
    "recompute-cluster-stats",
    short_help="Recompute fresh_cluster_stats from address-level tables.",
)
@require_environment()
@require_currency()
@click.option(
    "--local",
    is_flag=True,
    help="Run Spark in local mode (local[*]) instead of submitting to the cluster.",
)
def recompute_cluster_stats(env, currency, local):
    """Recompute the full ``fresh_cluster_stats`` from the address-level tables.

    Aggregates ``address`` + ``address_incoming/outgoing_relations`` through the
    fresh ``address -> cluster`` membership into per-cluster size, totals,
    first/last tx, degrees and tx-counts, and rewrites ``fresh_cluster_stats``.
    Membership (``fresh_address_cluster`` / ``fresh_cluster_addresses``) is NOT
    touched. Intended as a weekly job: cluster-level stats lag the delta loop
    (which keeps only size + root live), so they are refreshed periodically.

    Holds the transformed-keyspace lock (the same lock the delta updater takes) so
    it never races a delta merge, and truncates ``fresh_cluster_stats`` first for a
    clean, self-healing rebuild (clears rows of clusters merged away since the last
    run). ``total_received_adj`` / ``total_spent_adj`` are the cluster totals
    minus intra-cluster flows (summed external-relation ``estimated_value``);
    validate against a real keyspace before REST reads them.
    \f
    """
    from graphsenselib.config import get_config, is_fresh_clustering_enabled
    from graphsenselib.db.factory import DbFactory
    from graphsenselib.schema.schema import GraphsenseSchemas
    from graphsenselib.transformation.clustering import recompute_fresh_cluster_stats
    from graphsenselib.transformation.spark import create_spark_session
    from graphsenselib.utils.locking import create_lock

    if not is_fresh_clustering_enabled():
        raise click.ClickException(
            "Fresh clustering is disabled. Set "
            "GRAPHSENSE_FRESH_CLUSTERING_ENABLED=true to enable."
        )

    GraphsenseSchemas().apply_migrations(env, currency, keyspace_type="transformed")

    with DbFactory().from_config(env, currency) as db:
        config = get_config()
        env_config = config.get_environment(env)
        transformed_keyspace = db.transformed.get_keyspace()

        logger.info(
            f"Recomputing cluster stats: env={env}, currency={currency}, "
            f"transformed={transformed_keyspace} (acquiring keyspace lock)"
        )
        with create_lock(transformed_keyspace):
            db.transformed.execute_raw_cql("TRUNCATE fresh_cluster_stats")
            spark_session = create_spark_session(
                app_name=f"graphsense-cluster-stats-{currency}-{env}",
                local=local,
                cassandra_nodes=env_config.cassandra_nodes,
                cassandra_username=env_config.username,
                cassandra_password=env_config.password,
                spark_config=config.get_spark_config(),
                spark_packages=config.get_spark_packages(),
            )
            try:
                n = recompute_fresh_cluster_stats(spark_session, transformed_keyspace)
            finally:
                spark_session.stop()
                logger.info("SparkSession stopped.")
        logger.info(f"Cluster-stat recompute complete: {n} clusters.")
