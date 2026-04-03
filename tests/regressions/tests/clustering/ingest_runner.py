"""Run ingest, Scala transformation, and Rust clustering for regression tests."""

import subprocess

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
    network_name = {"btc": "bitcoin", "ltc": "litecoin", "bch": "bitcoin_cash", "zec": "zcash"}.get(
        config.currency, config.currency
    )

    cmd = [
        "docker", "run", "--rm",
        "--network", "host",
        "-e", f"CASSANDRA_HOST={cassandra_host}",
        "-e", f"RAW_KEYSPACE={raw_keyspace}",
        "-e", f"TGT_KEYSPACE={transformed_keyspace}",
        "-e", f"NETWORK={network_name}",
        "-e", "SPARK_MASTER=local[*]",
        "-e", "SPARK_DRIVER_MEMORY=2g",
        "-e", "SPARK_EXECUTOR_MEMORY=2g",
        "-e", f"TRANSFORM_BUCKET_SIZE={_block_bucket_size(config.currency)}",
        image_name,
        "bash", "submit.sh",
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
) -> list[list[str]]:
    """Read transaction inputs from the raw keyspace.

    Returns: list of transactions, each transaction is a list of input address strings.
    Only includes transactions with >1 unique input address (multi-input heuristic).
    """
    from cassandra.cluster import Cluster

    with Cluster([cassandra_host], port=cassandra_port) as cluster:
        session = cluster.connect()
        rows = session.execute(
            f"SELECT tx_id, coinbase, inputs FROM {raw_keyspace}.transaction"  # noqa: S608
        )
        tx_inputs = []
        for row in rows:
            if row.coinbase:
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


def run_rust_clustering(
    cassandra_host: str,
    cassandra_port: int,
    raw_keyspace: str,
    transformed_keyspace: str,
) -> dict[int, int]:
    """Run Rust clustering in-process using address IDs from Scala's address table.

    1. Reads address -> address_id mapping from Scala's transformed keyspace
    2. Reads raw transaction inputs from Cassandra
    3. Maps input addresses to address_ids
    4. Feeds multi-input groups to gs_clustering.Clustering
    5. Returns address_id -> cluster_id mapping

    Returns: dict mapping address_id -> cluster_id for all addresses that
    appear in at least one multi-input transaction.
    """
    from gs_clustering import Clustering

    # Step 1: Read address -> address_id mapping from Scala
    addr_to_id = read_address_id_mapping(
        cassandra_host, cassandra_port, transformed_keyspace
    )
    if not addr_to_id:
        return {}

    # Step 2: Read raw transaction inputs
    raw_inputs = read_raw_tx_inputs(cassandra_host, cassandra_port, raw_keyspace)

    # Step 3: Map addresses to address_ids, build transaction input groups
    max_address_id = max(addr_to_id.values())
    tx_input_ids = []
    for addrs in raw_inputs:
        ids = []
        for addr in addrs:
            aid = addr_to_id.get(addr)
            if aid is not None:
                ids.append(aid)
        if len(ids) > 1:
            tx_input_ids.append(ids)

    # Step 4: Run Rust clustering
    c = Clustering(max_address_id=max_address_id)
    if tx_input_ids:
        c.process_transactions(tx_input_ids)

    # Step 5: Extract mapping for addresses that actually exist
    batch = c.get_mapping()
    all_address_ids = batch.column("address_id").to_pylist()
    all_cluster_ids = batch.column("cluster_id").to_pylist()

    existing_ids = set(addr_to_id.values())
    return {
        aid: cid
        for aid, cid in zip(all_address_ids, all_cluster_ids)
        if aid in existing_ids
    }
