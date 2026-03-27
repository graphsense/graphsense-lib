"""Shared ingest runner infrastructure.

Provides:
- build_gs_config(): unified .graphsense.yaml config builder
- detect_no_lock_flag(): CLI version-aware flag detection
- run_cli_ingest(): subprocess execution with error handling
"""

import os
import subprocess
import tempfile
from pathlib import Path

import yaml

from tests.lib.config import SCHEMA_TYPE_MAP
from tests.lib.constants import INGEST_TIMEOUT_S


def build_gs_config(
    currency: str,
    node_url: str,
    secondary_node_references: list[str] | None = None,
    environment: str = "test",
    # Cassandra settings (optional)
    cassandra_host: str | None = None,
    cassandra_port: int | None = None,
    keyspace_name: str | None = None,
    create_keyspace_setup: bool = False,
    block_bucket_size: int = 100,
    # Delta Lake settings (optional)
    delta_directory: str | None = None,
    # S3/MinIO settings (optional)
    minio_endpoint: str | None = None,
    minio_access_key: str | None = None,
    minio_secret_key: str | None = None,
) -> dict:
    """Build a .graphsense.yaml config dict for ingest commands.

    Supports any combination of Cassandra and/or Delta Lake sinks.
    Only includes config sections for the sinks actually requested.
    """
    schema_type = SCHEMA_TYPE_MAP.get(currency, "utxo")

    ingest_config: dict = {"node_reference": node_url}
    if secondary_node_references:
        ingest_config["secondary_node_references"] = secondary_node_references
    if delta_directory:
        ingest_config["raw_keyspace_file_sinks"] = {
            "delta": {"directory": delta_directory},
        }

    raw_ks = keyspace_name or f"{currency}_raw_{environment}"
    ks_config: dict = {
        "raw_keyspace_name": raw_ks,
        "transformed_keyspace_name": f"{raw_ks}_transformed",
        "schema_type": schema_type,
        "ingest_config": ingest_config,
    }

    if create_keyspace_setup and cassandra_host and keyspace_name:
        ks_config["keyspace_setup_config"] = {
            "raw": {
                "replication_config": (
                    "{'class': 'SimpleStrategy', 'replication_factor': 1}"
                ),
                "data_configuration": {
                    "id": keyspace_name,
                    "block_bucket_size": block_bucket_size,
                    "tx_bucket_size": 25000,
                    "tx_prefix_length": 5,
                },
            },
        }

    cassandra_node = (
        f"{cassandra_host}:{cassandra_port}" if cassandra_host else "localhost"
    )
    gs_config: dict = {
        "environments": {
            environment: {
                "cassandra_nodes": [cassandra_node],
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


def detect_no_lock_flag(cli_bin: str, env: dict) -> str:
    """Detect the correct no-lock CLI flag for this graphsense-cli version.

    Older versions (v25.11.18) use ``--no-file-lock``, newer use ``--no-lock``.
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


def make_cli_env(venv_dir: Path, config_path: str, extra: dict | None = None) -> dict:
    """Build an environment dict for subprocess calls to graphsense-cli."""
    env = os.environ.copy()
    env.update({
        "GRAPHSENSE_CONFIG_YAML": config_path,
        "PATH": f"{venv_dir / 'bin'}:{os.environ.get('PATH', '/usr/bin:/bin')}",
    })
    if extra:
        env.update(extra)
    return env


def run_cli_ingest(
    venv_dir: Path,
    gs_config: dict,
    cmd_args: list[str],
    timeout: int = INGEST_TIMEOUT_S,
    config_prefix: str = "gsconfig-",
    extra_env: dict | None = None,
    stderr_limit: int = 3000,
) -> subprocess.CompletedProcess:
    """Write gs_config to a temp file, run graphsense-cli with cmd_args, clean up.

    Raises ``RuntimeError`` on non-zero exit.
    Returns the CompletedProcess on success.
    """
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".yaml", prefix=config_prefix, delete=False
    ) as f:
        yaml.dump(gs_config, f)
        config_path = f.name

    cli_bin = str(venv_dir / "bin" / "graphsense-cli")
    cmd = [cli_bin] + cmd_args
    env = make_cli_env(venv_dir, config_path, extra_env)

    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, env=env, timeout=timeout
        )
    finally:
        Path(config_path).unlink(missing_ok=True)

    if result.returncode != 0:
        raise RuntimeError(
            f"Ingestion failed (exit {result.returncode}):\n"
            f"cmd: {' '.join(cmd)}\n"
            f"stdout: {result.stdout[-stderr_limit:]}\n"
            f"stderr: {result.stderr[-stderr_limit:]}"
        )

    return result
