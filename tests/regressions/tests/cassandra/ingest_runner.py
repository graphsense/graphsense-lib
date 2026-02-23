"""Run graphsense-cli ingest from-node in a subprocess against a Cassandra container."""

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


def _build_gs_config(
    config: CassandraTestConfig,
    cassandra_host: str,
    cassandra_port: int,
    keyspace_name: str,
) -> dict:
    """Build a minimal .graphsense.yaml config dict for Cassandra ingestion."""
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


def run_cassandra_ingest(
    venv_dir: Path,
    config: CassandraTestConfig,
    cassandra_host: str,
    cassandra_port: int,
    keyspace_name: str | None = None,
) -> None:
    """Run ``graphsense-cli ingest from-node`` inside *venv_dir*.

    Creates a temporary ``.graphsense.yaml`` with the right Cassandra
    settings, points ``GRAPHSENSE_CONFIG_YAML`` at it, then executes the CLI.

    If *keyspace_name* is not given, defaults to
    ``regtest_{currency}_{range_id}_raw``.
    """
    if keyspace_name is None:
        keyspace_name = f"regtest_{config.currency}_{config.range_id}_raw"

    gs_config = _build_gs_config(
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

    # UTXO currencies require utxo_with_tx_graph mode;
    # account currencies use legacy mode.
    mode = (
        "utxo_with_tx_graph" if config.schema_type == "utxo" else "legacy"
    )

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
        "--mode", mode,
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
            f"Cassandra ingestion failed (exit {result.returncode}):\n"
            f"cmd: {' '.join(cmd)}\n"
            f"stdout: {result.stdout[-2000:]}\n"
            f"stderr: {result.stderr[-2000:]}"
        )
