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
def run_transformation(
    env,
    currency,
    start_block,
    end_block,
    create_schema,
    delta_lake_path,
    local,
):
    """Run PySpark transformation from Delta Lake to Cassandra raw keyspace.

    For cluster mode, Spark workers must have Python >= 3.10 (matching the driver).
    Install via: uv python install 3.11 on each worker node, then set
    spark.pyspark.python in spark_config.
    \f
    """
    from graphsenselib.config import get_config

    config = get_config()
    env_config = config.get_environment(env)
    ks_config = config.get_keyspace_config(env, currency)

    # Schema creation runs BEFORE Spark (uses cassandra-driver, no Java needed)
    if create_schema:
        logger.info("Creating Cassandra schema if not exists...")
        GraphsenseSchemas().create_keyspace_if_not_exist(
            env, currency, keyspace_type="raw"
        )
        GraphsenseSchemas().apply_migrations(env, currency, keyspace_type="raw")

    raw_keyspace = ks_config.raw_keyspace_name
    cassandra_nodes = env_config.cassandra_nodes
    cassandra_username = env_config.username
    cassandra_password = env_config.password

    # Safety check: verify the target keyspace block table is empty
    # to prevent accidental data corruption from mixing sources
    from cassandra.cluster import Cluster as CassCluster

    host, _, port = cassandra_nodes[0].partition(":")
    cass_port = int(port) if port else 9042
    auth_provider = None
    if cassandra_username and cassandra_password:
        from cassandra.auth import PlainTextAuthProvider

        auth_provider = PlainTextAuthProvider(
            username=cassandra_username, password=cassandra_password
        )
    with CassCluster([host], port=cass_port, auth_provider=auth_provider) as cluster:
        session = cluster.connect()
        rows = list(
            session.execute(
                "SELECT table_name FROM system_schema.tables WHERE keyspace_name = %s",
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
                    f"(block table is not empty). Use a fresh keyspace or "
                    f"truncate existing tables before running transformation."
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

    logger.info(
        f"Starting transformation: env={env}, currency={currency}, "
        f"blocks={start_block}-{end_block}, delta={delta_lake_path}, "
        f"keyspace={raw_keyspace}, local={local}"
    )

    # Deferred PySpark import
    from graphsenselib.transformation.factory import run as run_factory

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
    from graphsenselib.db.factory import DbFactory
    from graphsenselib.schema.schema import GraphsenseSchemas
    from graphsenselib.transformation.clustering import (
        run_clustering_one_off_from_cassandra,
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
