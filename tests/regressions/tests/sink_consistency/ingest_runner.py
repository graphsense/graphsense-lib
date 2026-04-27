"""Run graphsense-cli ingest with different sink combinations.

All three modes use ``ingest from-node`` which routes to the same
IngestRunner-based pipeline. The only variable is which ``--sinks`` flags
are passed, isolating the test to sink-writing behaviour.
"""

import os
import subprocess
import tempfile
from pathlib import Path

import yaml

from tests.deltalake.config import SCHEMA_TYPE_MAP
from tests.sink_consistency.config import SinkConsistencyConfig


def _build_gs_config(
    config: SinkConsistencyConfig,
    cassandra_host: str | None = None,
    cassandra_port: int | None = None,
    keyspace_name: str | None = None,
    delta_directory: str | None = None,
    minio_endpoint: str | None = None,
    minio_access_key: str | None = None,
    minio_secret_key: str | None = None,
) -> dict:
    """Build a minimal .graphsense.yaml for the requested sink combination."""
    currency = config.currency
    schema_type = SCHEMA_TYPE_MAP.get(currency, "utxo")

    ingest_config: dict = {
        "node_reference": config.node_url,
    }
    if config.secondary_node_references:
        ingest_config["secondary_node_references"] = config.secondary_node_references

    if delta_directory:
        ingest_config["raw_keyspace_file_sinks"] = {
            "delta": {"directory": delta_directory},
        }

    raw_ks = keyspace_name or f"{currency}_raw_test"
    ks_config: dict = {
        "raw_keyspace_name": raw_ks,
        "transformed_keyspace_name": f"{raw_ks}_transformed",
        "schema_type": schema_type,
        "ingest_config": ingest_config,
    }

    if cassandra_host and keyspace_name:
        ks_config["keyspace_setup_config"] = {
            "raw": {
                "replication_config": (
                    "{'class': 'SimpleStrategy', 'replication_factor': 1}"
                ),
                "data_configuration": {
                    "id": keyspace_name,
                    "block_bucket_size": 100,
                    "tx_bucket_size": 25000,
                    "tx_prefix_length": 5,
                },
            },
        }

    gs_config: dict = {
        "environments": {
            "test": {
                "cassandra_nodes": [
                    f"{cassandra_host}:{cassandra_port}"
                    if cassandra_host
                    else "localhost"
                ],
                "keyspaces": {currency: ks_config},
            },
        },
    }

    if minio_endpoint:
        gs_config["s3_credentials"] = {
            "AWS_ENDPOINT_URL": minio_endpoint,
            "AWS_ACCESS_KEY_ID": minio_access_key or "",
            "AWS_SECRET_ACCESS_KEY": minio_secret_key or "",
            "AWS_REGION": "us-east-1",
            "AWS_ALLOW_HTTP": "true",
            "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
        }

    return gs_config


def _detect_no_lock_flag(cli_bin: str, env: dict) -> str:
    """Detect the correct no-lock CLI flag."""
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


def run_ingest(
    venv_dir: Path,
    config: SinkConsistencyConfig,
    sinks: list[str],
    cassandra_host: str | None = None,
    cassandra_port: int | None = None,
    keyspace_name: str | None = None,
    delta_directory: str | None = None,
    minio_endpoint: str | None = None,
    minio_access_key: str | None = None,
    minio_secret_key: str | None = None,
) -> None:
    """Run ``graphsense-cli ingest from-node`` with the given sinks.

    All modes use the same IngestRunner pipeline; only the --sinks
    flags differ.
    """
    gs_config = _build_gs_config(
        config,
        cassandra_host=cassandra_host,
        cassandra_port=cassandra_port,
        keyspace_name=keyspace_name,
        delta_directory=delta_directory,
        minio_endpoint=minio_endpoint,
        minio_access_key=minio_access_key,
        minio_secret_key=minio_secret_key,
    )

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".yaml", prefix="gsconfig-sink-", delete=False
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
        "--create-schema",
        "--write-mode", "overwrite",
        "--ignore-overwrite-safechecks",
        no_lock_flag,
    ]
    for sink in sinks:
        cmd.extend(["--sinks", sink])

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
            f"Ingestion failed (sinks={sinks}, exit {result.returncode}):\n"
            f"cmd: {' '.join(cmd)}\n"
            f"stdout: {result.stdout[-3000:]}\n"
            f"stderr: {result.stderr[-3000:]}"
        )
