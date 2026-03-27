"""Run graphsense-cli ingest in a subprocess against a Cassandra container.

Supports three modes:
- ``legacy``: uses ``ingest from-node`` with old pipeline flags
- ``from-node``: uses ``ingest from-node --sinks cassandra`` (new pipeline)
- ``delta``: uses ``ingest delta-lake ingest --sinks delta --sinks cassandra``
"""

import shutil
import tempfile
from pathlib import Path

from tests.cassandra.config import CassandraTestConfig
from tests.lib.ingest import (
    build_gs_config,
    detect_no_lock_flag,
    make_cli_env,
    run_cli_ingest,
)


def run_cassandra_ingest(
    venv_dir: Path,
    config: CassandraTestConfig,
    cassandra_host: str,
    cassandra_port: int,
    keyspace_name: str | None = None,
    mode: str = "legacy",
) -> None:
    """Run Cassandra ingestion inside *venv_dir*."""
    if keyspace_name is None:
        keyspace_name = f"regtest_{config.currency}_{config.range_id}_raw"

    if mode == "delta":
        _run_delta_ingest(venv_dir, config, cassandra_host, cassandra_port, keyspace_name)
    elif mode == "from-node":
        _run_from_node_ingest(venv_dir, config, cassandra_host, cassandra_port, keyspace_name)
    else:
        _run_legacy_ingest(venv_dir, config, cassandra_host, cassandra_port, keyspace_name)


def _run_legacy_ingest(
    venv_dir: Path,
    config: CassandraTestConfig,
    cassandra_host: str,
    cassandra_port: int,
    keyspace_name: str,
) -> None:
    """Run legacy pipeline with --mode and --version flags."""
    gs_config = build_gs_config(
        currency=config.currency,
        node_url=config.node_url,
        secondary_node_references=config.secondary_node_references,
        cassandra_host=cassandra_host,
        cassandra_port=cassandra_port,
        keyspace_name=keyspace_name,
        create_keyspace_setup=True,
    )

    cli_bin = str(venv_dir / "bin" / "graphsense-cli")
    env = make_cli_env(venv_dir, "", extra={"GRAPHSENSE_LEGACY_INGEST": "true"})
    no_lock_flag = detect_no_lock_flag(cli_bin, env)

    is_utxo = config.schema_type == "utxo"
    ingest_mode = "utxo_with_tx_graph" if is_utxo else "legacy"
    version = "1" if is_utxo else "2"

    cmd_args = [
        "ingest", "from-node",
        "-e", "test",
        "-c", config.currency,
        "--start-block", str(config.start_block),
        "--end-block", str(config.end_block),
        "--batch-size", "10",
        "--sinks", "cassandra",
        "--create-schema",
        no_lock_flag,
        "--mode", ingest_mode,
        "--version", version,
    ]

    run_cli_ingest(
        venv_dir, gs_config, cmd_args,
        config_prefix="gsconfig-cassandra-",
        extra_env={"GRAPHSENSE_LEGACY_INGEST": "true"},
        label="Legacy Cassandra ingestion",
    )


def _run_from_node_ingest(
    venv_dir: Path,
    config: CassandraTestConfig,
    cassandra_host: str,
    cassandra_port: int,
    keyspace_name: str,
) -> None:
    """Run new pipeline: ``ingest from-node --sinks cassandra``."""
    gs_config = build_gs_config(
        currency=config.currency,
        node_url=config.node_url,
        secondary_node_references=config.secondary_node_references,
        cassandra_host=cassandra_host,
        cassandra_port=cassandra_port,
        keyspace_name=keyspace_name,
        create_keyspace_setup=True,
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
        "--sinks", "cassandra",
        "--create-schema",
        no_lock_flag,
    ]

    run_cli_ingest(
        venv_dir, gs_config, cmd_args,
        config_prefix="gsconfig-fromnode-",
        label="from-node Cassandra ingestion",
    )


def _run_delta_ingest(
    venv_dir: Path,
    config: CassandraTestConfig,
    cassandra_host: str,
    cassandra_port: int,
    keyspace_name: str,
) -> None:
    """Run dual-sink: ``ingest delta-lake ingest --sinks delta --sinks cassandra``."""
    delta_dir = tempfile.mkdtemp(prefix="gstest-delta-")

    gs_config = build_gs_config(
        currency=config.currency,
        node_url=config.node_url,
        secondary_node_references=config.secondary_node_references,
        cassandra_host=cassandra_host,
        cassandra_port=cassandra_port,
        keyspace_name=keyspace_name,
        create_keyspace_setup=True,
        delta_directory=delta_dir,
    )

    cmd_args = [
        "ingest", "delta-lake", "ingest",
        "-e", "test",
        "-c", config.currency,
        "--start-block", str(config.start_block),
        "--end-block", str(config.end_block),
        "--write-mode", "overwrite",
        "--ignore-overwrite-safechecks",
        "--sinks", "delta",
        "--sinks", "cassandra",
    ]

    try:
        run_cli_ingest(
            venv_dir, gs_config, cmd_args,
            config_prefix="gsconfig-delta-cass-",
            label="Delta dual-sink ingestion",
        )
    finally:
        shutil.rmtree(delta_dir, ignore_errors=True)
