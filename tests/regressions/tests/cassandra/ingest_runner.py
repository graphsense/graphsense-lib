"""Run graphsense-cli ingest in a subprocess against a Cassandra container.

Supports three modes:
- ``legacy``: uses ``ingest from-node`` with old pipeline flags
  (--mode utxo_with_tx_graph / --version 2)
- ``from-node``: uses ``ingest from-node --sinks cassandra``
  (new IngestRunner-based pipeline, UTXO chains)
- ``delta``: uses ``ingest delta-lake ingest --sinks delta --sinks cassandra``
  (new dual-sink pipeline, account chains)
"""

import os
import subprocess
import tempfile
from pathlib import Path

import yaml

from tests.cassandra.config import CassandraTestConfig


def _detect_no_lock_flag(cli_bin: str, env: dict) -> str:
    """Detect the correct no-lock CLI flag for this version.

    Older versions (v25.11.18) use ``--no-file-lock``, newer versions
    use ``--no-lock``.
    """
    result = subprocess.run(
        [cli_bin, "ingest", "from-node", "--help"],
        capture_output=True,
        text=True,
        env=env,
    )
    help_text = result.stdout + result.stderr
    if "--no-file-lock" in help_text:
        return "--no-file-lock"
    return "--no-lock"


def _detect_has_sinks_flag(cli_bin: str, env: dict) -> bool:
    """Check if ``ingest delta-lake ingest`` supports ``--sinks``."""
    result = subprocess.run(
        [cli_bin, "ingest", "delta-lake", "ingest", "--help"],
        capture_output=True,
        text=True,
        env=env,
    )
    help_text = result.stdout + result.stderr
    return "--sinks" in help_text


def _build_gs_config_legacy(
    config: CassandraTestConfig,
    cassandra_host: str,
    cassandra_port: int,
    keyspace_name: str,
) -> dict:
    """Build a minimal .graphsense.yaml config dict for legacy Cassandra ingestion."""
    currency = config.currency

    ingest_config = {
        "node_reference": config.node_url,
    }
    if config.secondary_node_references:
        ingest_config["secondary_node_references"] = config.secondary_node_references

    return {
        "environments": {
            "test": {
                "cassandra_nodes": [f"{cassandra_host}:{cassandra_port}"],
                "keyspaces": {
                    currency: {
                        "raw_keyspace_name": keyspace_name,
                        "transformed_keyspace_name": f"{keyspace_name}_transformed",
                        "schema_type": config.schema_type,
                        "ingest_config": ingest_config,
                        "keyspace_setup_config": {
                            "raw": {
                                "replication_config": (
                                    "{'class': 'SimpleStrategy',"
                                    " 'replication_factor': 1}"
                                ),
                                "data_configuration": {
                                    "id": keyspace_name,
                                    "block_bucket_size": 100,
                                    "tx_bucket_size": 25000,
                                    "tx_prefix_length": 5,
                                },
                            },
                        },
                    },
                },
            },
        },
    }


def _build_gs_config_delta(
    config: CassandraTestConfig,
    cassandra_host: str,
    cassandra_port: int,
    keyspace_name: str,
    delta_directory: str,
) -> dict:
    """Build config for delta-lake dual-sink ingestion (delta + cassandra)."""
    currency = config.currency

    ingest_config = {
        "node_reference": config.node_url,
        "raw_keyspace_file_sinks": {
            "delta": {
                "directory": delta_directory,
            },
        },
    }
    if config.secondary_node_references:
        ingest_config["secondary_node_references"] = config.secondary_node_references

    return {
        "environments": {
            "test": {
                "cassandra_nodes": [f"{cassandra_host}:{cassandra_port}"],
                "keyspaces": {
                    currency: {
                        "raw_keyspace_name": keyspace_name,
                        "transformed_keyspace_name": f"{keyspace_name}_transformed",
                        "schema_type": config.schema_type,
                        "ingest_config": ingest_config,
                        "keyspace_setup_config": {
                            "raw": {
                                "replication_config": (
                                    "{'class': 'SimpleStrategy',"
                                    " 'replication_factor': 1}"
                                ),
                                "data_configuration": {
                                    "id": keyspace_name,
                                    "block_bucket_size": 100,
                                    "tx_bucket_size": 25000,
                                    "tx_prefix_length": 5,
                                },
                            },
                        },
                    },
                },
            },
        },
    }


def run_cassandra_ingest(
    venv_dir: Path,
    config: CassandraTestConfig,
    cassandra_host: str,
    cassandra_port: int,
    keyspace_name: str | None = None,
    mode: str = "legacy",
) -> None:
    """Run Cassandra ingestion inside *venv_dir*.

    *mode* controls which pipeline is used:

    - ``"legacy"``: ``ingest from-node`` with old pipeline flags
    - ``"from-node"``: ``ingest from-node --sinks cassandra`` (new pipeline)
    - ``"delta"``: ``ingest delta-lake ingest --sinks delta --sinks cassandra``

    If *keyspace_name* is not given, defaults to
    ``regtest_{currency}_{range_id}_raw``.
    """
    if keyspace_name is None:
        keyspace_name = f"regtest_{config.currency}_{config.range_id}_raw"

    if mode == "delta":
        _run_delta_ingest(venv_dir, config, cassandra_host, cassandra_port, keyspace_name)
    elif mode == "from-node":
        _run_from_node_ingest(
            venv_dir, config, cassandra_host, cassandra_port, keyspace_name
        )
    else:
        _run_legacy_ingest(venv_dir, config, cassandra_host, cassandra_port, keyspace_name)


def _run_legacy_ingest(
    venv_dir: Path,
    config: CassandraTestConfig,
    cassandra_host: str,
    cassandra_port: int,
    keyspace_name: str,
) -> None:
    """Run ``graphsense-cli ingest from-node --sinks cassandra`` (legacy pipeline)."""
    gs_config = _build_gs_config_legacy(
        config, cassandra_host, cassandra_port, keyspace_name
    )

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".yaml", prefix="gsconfig-cassandra-", delete=False
    ) as f:
        yaml.dump(gs_config, f)
        config_path = f.name

    cli_bin = str(venv_dir / "bin" / "graphsense-cli")

    env = os.environ.copy()
    env.update({
        "GRAPHSENSE_CONFIG_YAML": config_path,
        "PATH": f"{venv_dir / 'bin'}:{os.environ.get('PATH', '/usr/bin:/bin')}",
    })

    is_utxo = config.schema_type == "utxo"
    ingest_mode = "utxo_with_tx_graph" if is_utxo else "legacy"
    # Account chains must use --version 2 (async) for correct transaction_type.
    # UTXO only supports --version 1.
    version = "1" if is_utxo else "2"

    no_lock_flag = _detect_no_lock_flag(cli_bin, env)

    cmd = [
        cli_bin,
        "ingest",
        "from-node",
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

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        env=env,
        timeout=600,
    )

    Path(config_path).unlink(missing_ok=True)

    if result.returncode != 0:
        raise RuntimeError(
            f"Legacy Cassandra ingestion failed (exit {result.returncode}):\n"
            f"cmd: {' '.join(cmd)}\n"
            f"stdout: {result.stdout[-2000:]}\n"
            f"stderr: {result.stderr[-2000:]}"
        )


def _run_from_node_ingest(
    venv_dir: Path,
    config: CassandraTestConfig,
    cassandra_host: str,
    cassandra_port: int,
    keyspace_name: str,
) -> None:
    """Run ``graphsense-cli ingest from-node --sinks cassandra`` (new pipeline).

    For UTXO chains, from-node now uses the IngestRunner-based pipeline
    by default (no --mode or --version flags needed).
    """
    gs_config = _build_gs_config_legacy(
        config, cassandra_host, cassandra_port, keyspace_name
    )

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".yaml", prefix="gsconfig-fromnode-", delete=False
    ) as f:
        yaml.dump(gs_config, f)
        config_path = f.name

    cli_bin = str(venv_dir / "bin" / "graphsense-cli")

    env = os.environ.copy()
    env.update({
        "GRAPHSENSE_CONFIG_YAML": config_path,
        "PATH": f"{venv_dir / 'bin'}:{os.environ.get('PATH', '/usr/bin:/bin')}",
    })

    no_lock_flag = _detect_no_lock_flag(cli_bin, env)

    cmd = [
        cli_bin,
        "ingest",
        "from-node",
        "-e", "test",
        "-c", config.currency,
        "--start-block", str(config.start_block),
        "--end-block", str(config.end_block),
        "--sinks", "cassandra",
        "--create-schema",
        no_lock_flag,
    ]

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        env=env,
        timeout=600,
    )

    Path(config_path).unlink(missing_ok=True)

    if result.returncode != 0:
        raise RuntimeError(
            f"from-node ingestion failed (exit {result.returncode}):\n"
            f"cmd: {' '.join(cmd)}\n"
            f"stdout: {result.stdout[-2000:]}\n"
            f"stderr: {result.stderr[-2000:]}"
        )


def _run_delta_ingest(
    venv_dir: Path,
    config: CassandraTestConfig,
    cassandra_host: str,
    cassandra_port: int,
    keyspace_name: str,
) -> None:
    """Run ``graphsense-cli ingest delta-lake ingest --sinks delta --sinks cassandra``."""
    delta_dir = tempfile.mkdtemp(prefix="gstest-delta-")

    gs_config = _build_gs_config_delta(
        config, cassandra_host, cassandra_port, keyspace_name, delta_dir
    )

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".yaml", prefix="gsconfig-delta-cass-", delete=False
    ) as f:
        yaml.dump(gs_config, f)
        config_path = f.name

    cli_bin = str(venv_dir / "bin" / "graphsense-cli")

    env = os.environ.copy()
    env.update({
        "GRAPHSENSE_CONFIG_YAML": config_path,
        "PATH": f"{venv_dir / 'bin'}:{os.environ.get('PATH', '/usr/bin:/bin')}",
    })

    cmd = [
        cli_bin,
        "ingest",
        "delta-lake",
        "ingest",
        "-e", "test",
        "-c", config.currency,
        "--start-block", str(config.start_block),
        "--end-block", str(config.end_block),
        "--write-mode", "overwrite",
        "--ignore-overwrite-safechecks",
        "--sinks", "delta",
        "--sinks", "cassandra",
    ]

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        env=env,
        timeout=600,
    )

    Path(config_path).unlink(missing_ok=True)
    # Clean up delta temp dir
    import shutil
    shutil.rmtree(delta_dir, ignore_errors=True)

    if result.returncode != 0:
        raise RuntimeError(
            f"Delta dual-sink ingestion failed (exit {result.returncode}):\n"
            f"cmd: {' '.join(cmd)}\n"
            f"stdout: {result.stdout[-2000:]}\n"
            f"stderr: {result.stderr[-2000:]}"
        )
