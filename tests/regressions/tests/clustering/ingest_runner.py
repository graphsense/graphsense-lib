"""Run ingest, Scala transformation, and Rust clustering for regression tests."""

import subprocess
from pathlib import Path

from tests.lib.config import SCHEMA_TYPE_MAP
from tests.lib.constants import TRANSFORMATION_TIMEOUT_S
from tests.lib.ingest import (
    build_gs_config,
    detect_no_lock_flag,
    make_cli_env,
    run_cli_ingest,
)
from tests.clustering.config import ClusteringConfig


def _block_bucket_size(currency: str) -> int:
    """Return the block bucket size matching the original transformation config."""
    return 100 if SCHEMA_TYPE_MAP.get(currency) == "utxo" else 1000


def run_ingest_delta_only(
    venv_dir: Path,
    config: ClusteringConfig,
    delta_directory: str,
    minio_endpoint: str,
    minio_access_key: str,
    minio_secret_key: str,
) -> None:
    """Run ingest to Delta Lake only."""
    gs_config = build_gs_config(
        currency=config.currency,
        node_url=config.node_url,
        secondary_node_references=config.secondary_node_references,
        delta_directory=delta_directory,
        minio_endpoint=minio_endpoint,
        minio_access_key=minio_access_key,
        minio_secret_key=minio_secret_key,
    )

    cli_bin = str(venv_dir / "bin" / "graphsense-cli")
    env = make_cli_env(venv_dir, "")
    no_lock_flag = detect_no_lock_flag(cli_bin, env)

    cmd_args = [
        "ingest", "from-node",
        "-e", "test",
        "-c", config.currency,
        "--start-block", str(config.start_block),
        "--end-block", str(config.end_block),
        "--write-mode", "overwrite",
        "--ignore-overwrite-safechecks",
        no_lock_flag,
        "--sinks", "delta",
    ]

    run_cli_ingest(
        venv_dir, gs_config, cmd_args, config_prefix="gsconfig-clust-delta-"
    )


def run_ingest_cassandra_raw(
    venv_dir: Path,
    config: ClusteringConfig,
    cassandra_host: str,
    cassandra_port: int,
    keyspace_name: str,
) -> None:
    """Run ingest directly to Cassandra raw keyspace."""
    gs_config = build_gs_config(
        currency=config.currency,
        node_url=config.node_url,
        secondary_node_references=config.secondary_node_references,
        cassandra_host=cassandra_host,
        cassandra_port=cassandra_port,
        keyspace_name=keyspace_name,
        create_keyspace_setup=True,
        block_bucket_size=_block_bucket_size(config.currency),
    )

    cli_bin = str(venv_dir / "bin" / "graphsense-cli")
    env = make_cli_env(venv_dir, "")
    no_lock_flag = detect_no_lock_flag(cli_bin, env)

    cmd_args = [
        "ingest", "from-node",
        "-e", "test",
        "-c", config.currency,
        "--start-block", str(config.start_block),
        "--end-block", str(config.end_block),
        "--create-schema",
        "--write-mode", "overwrite",
        "--ignore-overwrite-safechecks",
        no_lock_flag,
        "--sinks", "cassandra",
    ]

    run_cli_ingest(
        venv_dir, gs_config, cmd_args, config_prefix="gsconfig-clust-raw-"
    )


def _create_transformed_keyspace(cassandra_host: str, cassandra_port: int, keyspace: str):
    """Create the transformed keyspace with its schema so Scala can write to it."""
    from cassandra.cluster import Cluster

    with Cluster([cassandra_host], port=cassandra_port) as cluster:
        session = cluster.connect()
        session.execute(
            f"CREATE KEYSPACE IF NOT EXISTS {keyspace} "  # noqa: S608
            "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}"
        )
        # Load the transformed UTXO schema from the repo
        schema_path = (
            Path(__file__).resolve().parents[4]
            / "src" / "graphsenselib" / "schema" / "resources"
            / "transformed_utxo_schema.sql"
        )
        schema_sql = schema_path.read_text()
        # Replace placeholder keyspace name and remove USE statement
        for stmt in schema_sql.split(";"):
            stmt = stmt.strip()
            if not stmt or stmt.upper().startswith("CREATE KEYSPACE") or stmt.upper().startswith("USE "):
                continue
            # Prepend keyspace to CREATE TABLE/TYPE statements
            stmt = stmt.replace("CREATE TABLE ", f"CREATE TABLE {keyspace}.")
            stmt = stmt.replace("CREATE TYPE ", f"CREATE TYPE {keyspace}.")
            stmt = stmt.replace("CREATE TABLE IF NOT EXISTS ", f"CREATE TABLE IF NOT EXISTS {keyspace}.")
            stmt = stmt.replace("CREATE TYPE IF NOT EXISTS ", f"CREATE TYPE IF NOT EXISTS {keyspace}.")
            session.execute(stmt)


def run_exchange_rates_ingest(
    venv_dir: Path,
    config: ClusteringConfig,
    cassandra_host: str,
    cassandra_port: int,
    keyspace_name: str,
):
    """Ingest exchange rates via graphsense-cli exchange-rates cryptocompare ingest."""
    gs_config = build_gs_config(
        currency=config.currency,
        node_url=config.node_url,
        secondary_node_references=config.secondary_node_references,
        cassandra_host=cassandra_host,
        cassandra_port=cassandra_port,
        keyspace_name=keyspace_name,
    )

    cmd_args = [
        "exchange-rates", "cryptocompare", "ingest",
        "-e", "test",
        "-c", config.currency,
    ]

    run_cli_ingest(
        venv_dir, gs_config, cmd_args, config_prefix="gsconfig-clust-rates-"
    )


def run_scala_transformation(
    image_name: str,
    config: ClusteringConfig,
    cassandra_host: str,
    cassandra_port: int,
    raw_keyspace: str,
    transformed_keyspace: str,
) -> None:
    """Run Scala/Spark full transformation via Docker.

    This produces the transformed keyspace with cluster_addresses, address table, etc.
    The Scala image uses docker/submit.sh with env vars for spark-submit.
    """
    # The Scala TransformationJob expects the short currency code (btc, ltc, etc.)
    network_name = config.currency

    # We invoke spark-submit directly (rather than submit.sh) so we can pass
    # the non-default Cassandra port from the testcontainer.
    spark_packages = (
        "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,"
        "org.rogach:scallop_2.12:4.1.0,"
        "joda-time:joda-time:2.10.10,"
        "org.web3j:core:4.8.7,"
        "org.web3j:abi:4.8.7,"
        "graphframes:graphframes:0.8.3-spark3.4-s_2.12"
    )

    cmd = [
        "docker", "run", "--rm",
        "--network", "host",
        image_name,
        "/opt/spark/bin/spark-submit",
        "--class", "org.graphsense.TransformationJob",
        "--master", "local[*]",
        "--conf", f"spark.cassandra.connection.host={cassandra_host}",
        "--conf", f"spark.cassandra.connection.port={cassandra_port}",
        "--conf", "spark.driver.memory=4g",
        "--conf", "spark.executor.memory=4g",
        "--conf", "spark.driver.bindAddress=0.0.0.0",
        "--conf", "spark.sql.extensions=com.datastax.spark.connector.CassandraSparkExtensions",
        "--conf", "spark.sql.session.timeZone=UTC",
        "--conf", "spark.sql.adaptive.enabled=true",
        "--conf", "spark.serializer=org.apache.spark.serializer.KryoSerializer",
        "--conf", "spark.kryo.referenceTracking=false",
        "--conf", "spark.executor.extraJavaOptions=-XX:+UnlockExperimentalVMOptions -XX:hashCode=0",
        "--conf", "spark.driver.extraJavaOptions=-XX:+UnlockExperimentalVMOptions -XX:hashCode=0",
        "--packages", spark_packages,
        "graphsense-spark.jar",
        "--network", network_name,
        "--raw-keyspace", raw_keyspace,
        "--target-keyspace", transformed_keyspace,
    ]

    result = subprocess.run(
        cmd, capture_output=True, text=True, timeout=TRANSFORMATION_TIMEOUT_S
    )

    if result.stdout:
        print(f"\n    [scala stdout tail]: {result.stdout[-2000:]}")
    if result.stderr:
        important = [
            line for line in result.stderr.splitlines()
            if any(kw in line.lower() for kw in [
                "error", "exception", "writing", "wrote", "no rows",
                "schema=", "warning", "transform", "cluster",
            ])
        ]
        if important:
            print("    [scala stderr highlights]:")
            for line in important[-20:]:
                print(f"      {line}")

    if result.returncode != 0:
        raise RuntimeError(
            f"Scala transformation failed (exit {result.returncode}):\n"
            f"cmd: {' '.join(cmd)}\n"
            f"stdout: {result.stdout[-5000:]}\n"
            f"stderr: {result.stderr[-10000:]}"
        )


def read_scala_clusters(
    cassandra_host: str,
    cassandra_port: int,
    transformed_keyspace: str,
) -> dict[int, set[int]]:
    """Read cluster_addresses from the Scala-produced transformed keyspace.

    Returns: dict mapping cluster_id -> set of address_ids.
    """
    from cassandra.cluster import Cluster

    with Cluster([cassandra_host], port=cassandra_port) as cluster:
        session = cluster.connect()
        rows = session.execute(
            f"SELECT cluster_id, address_id FROM {transformed_keyspace}.cluster_addresses"  # noqa: S608
        )
        clusters: dict[int, set[int]] = {}
        for row in rows:
            clusters.setdefault(row.cluster_id, set()).add(row.address_id)
    return clusters


def read_address_id_mapping(
    cassandra_host: str,
    cassandra_port: int,
    transformed_keyspace: str,
) -> dict[str, int]:
    """Read address -> address_id mapping from the Scala-produced address table.

    Returns: dict mapping address string -> address_id int.
    """
    from cassandra.cluster import Cluster

    with Cluster([cassandra_host], port=cassandra_port) as cluster:
        session = cluster.connect()
        rows = session.execute(
            f"SELECT address_id, address FROM {transformed_keyspace}.address"  # noqa: S608
        )
        return {row.address: row.address_id for row in rows}


def read_raw_tx_inputs(
    cassandra_host: str,
    cassandra_port: int,
    raw_keyspace: str,
    min_block_id: int = 0,
    max_block_id: int | None = None,
) -> list[list[str]]:
    """Read transaction inputs from the raw keyspace.

    Args:
        min_block_id: Only include transactions from blocks >= this (inclusive).
        max_block_id: Only include transactions from blocks <= this (inclusive).
            If None, no upper bound.

    Returns: list of transactions, each transaction is a list of input address strings.
    Only includes transactions with >1 unique input address (multi-input heuristic).
    """
    from cassandra.cluster import Cluster

    with Cluster([cassandra_host], port=cassandra_port) as cluster:
        session = cluster.connect()
        rows = session.execute(
            f"SELECT block_id, coinbase, inputs FROM {raw_keyspace}.transaction"  # noqa: S608
        )
        tx_inputs = []
        for row in rows:
            if row.coinbase:
                continue
            if row.block_id < min_block_id:
                continue
            if max_block_id is not None and row.block_id > max_block_id:
                continue
            if not row.inputs:
                continue
            addresses = set()
            for inp in row.inputs:
                if inp.address:
                    for addr in inp.address:
                        if addr:
                            addresses.add(addr)
            if len(addresses) > 1:
                tx_inputs.append(sorted(addresses))
    return tx_inputs


def _resolve_tx_inputs(
    raw_inputs: list[list[str]],
    addr_to_id: dict[str, int],
) -> list[list[int]]:
    """Map address strings to address_ids, keeping only multi-input groups."""
    tx_input_ids = []
    for addrs in raw_inputs:
        ids = []
        for addr in addrs:
            aid = addr_to_id.get(addr)
            if aid is not None:
                ids.append(aid)
        if len(ids) > 1:
            tx_input_ids.append(ids)
    return tx_input_ids


def _mapping_from_clustering(clustering, addr_to_id: dict[str, int]) -> dict[int, int]:
    """Extract address_id -> cluster_id for existing addresses only."""
    batch = clustering.get_mapping()
    all_address_ids = batch.column("address_id").to_pylist()
    all_cluster_ids = batch.column("cluster_id").to_pylist()
    existing_ids = set(addr_to_id.values())
    return {
        aid: cid
        for aid, cid in zip(all_address_ids, all_cluster_ids)
        if aid in existing_ids
    }


def run_rust_clustering(
    cassandra_host: str,
    cassandra_port: int,
    raw_keyspace: str,
    transformed_keyspace: str,
    max_block_id: int | None = None,
) -> dict[int, int]:
    """Run Rust clustering from scratch on blocks [0, max_block_id].

    Returns: dict mapping address_id -> cluster_id for all known addresses.
    """
    from gs_clustering import Clustering

    addr_to_id = read_address_id_mapping(
        cassandra_host, cassandra_port, transformed_keyspace
    )
    if not addr_to_id:
        return {}

    raw_inputs = read_raw_tx_inputs(
        cassandra_host, cassandra_port, raw_keyspace,
        max_block_id=max_block_id,
    )
    tx_input_ids = _resolve_tx_inputs(raw_inputs, addr_to_id)

    max_address_id = max(addr_to_id.values())
    c = Clustering(max_address_id=max_address_id)
    if tx_input_ids:
        c.process_transactions(tx_input_ids)

    return _mapping_from_clustering(c, addr_to_id)


def run_rust_clustering_incremental(
    cassandra_host: str,
    cassandra_port: int,
    raw_keyspace: str,
    transformed_keyspace: str,
    existing_mapping: dict[int, int],
    min_block_id: int,
    max_block_id: int | None = None,
) -> dict[int, int]:
    """Run Rust incremental clustering: rebuild from existing mapping, add new blocks.

    Args:
        existing_mapping: address_id -> cluster_id from the initial clustering.
        min_block_id: First block of the new range (inclusive).
        max_block_id: Last block of the new range (inclusive).

    Returns: updated address_id -> cluster_id for all known addresses.
    """
    from gs_clustering import Clustering

    addr_to_id = read_address_id_mapping(
        cassandra_host, cassandra_port, transformed_keyspace
    )
    if not addr_to_id:
        return existing_mapping

    raw_inputs = read_raw_tx_inputs(
        cassandra_host, cassandra_port, raw_keyspace,
        min_block_id=min_block_id,
        max_block_id=max_block_id,
    )
    new_tx_input_ids = _resolve_tx_inputs(raw_inputs, addr_to_id)

    max_address_id = max(addr_to_id.values())
    c = Clustering(max_address_id=max_address_id)

    # Rebuild from existing state
    if existing_mapping:
        addr_ids = list(existing_mapping.keys())
        clus_ids = list(existing_mapping.values())
        c.rebuild_from_mapping(addr_ids, clus_ids)

    # Process new transactions
    if new_tx_input_ids:
        c.process_transactions(new_tx_input_ids)

    return _mapping_from_clustering(c, addr_to_id)
