"""CLI for PySpark-based Delta Lake → Cassandra transformation."""

import logging

import click

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
    required=True,
    help="End block (inclusive).",
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
    env, currency, start_block, end_block, create_schema, delta_lake_path, local
):
    """Run PySpark transformation from Delta Lake to Cassandra raw keyspace.
    \f
    Reads raw blockchain data from Delta Lake tables and writes it to the
    Cassandra raw keyspace using the spark-cassandra-connector.
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

    # Resolve delta path from config if not overridden
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

    s3_credentials = config.get_s3_credentials()

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
    )
