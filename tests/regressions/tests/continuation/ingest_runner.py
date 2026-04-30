"""Run graphsense-cli ingest for continuation tests.

Provides a single function that runs ``ingest from-node`` via subprocess
with delta-only sinks. Used for both one-shot and split ingests.
"""

import os
import subprocess
import tempfile
from pathlib import Path

import yaml

from tests.deltalake.config import SCHEMA_TYPE_MAP
from tests.continuation.config import ContinuationConfig


def _build_gs_config(
    config: ContinuationConfig,
    delta_directory: str,
    minio_endpoint: str,
    minio_access_key: str,
    minio_secret_key: str,
) -> dict:
    """Build a minimal .graphsense.yaml for delta-only ingestion."""
    currency = config.currency
    schema_type = SCHEMA_TYPE_MAP.get(currency, "utxo")

    ingest_config: dict = {
        "node_reference": config.node_url,
        "raw_keyspace_file_sinks": {
            "delta": {"directory": delta_directory},
        },
    }
    if config.secondary_node_references:
        ingest_config["secondary_node_references"] = config.secondary_node_references

    return {
        "environments": {
            "test": {
                "cassandra_nodes": ["localhost"],
                "keyspaces": {
                    currency: {
                        "raw_keyspace_name": f"{currency}_raw_test",
                        "transformed_keyspace_name": f"{currency}_transformed_test",
                        "schema_type": schema_type,
                        "ingest_config": ingest_config,
                    },
                },
            },
        },
        "s3_credentials": {
            "AWS_ENDPOINT_URL": minio_endpoint,
            "AWS_ACCESS_KEY_ID": minio_access_key,
            "AWS_SECRET_ACCESS_KEY": minio_secret_key,
            "AWS_REGION": "us-east-1",
            "AWS_ALLOW_HTTP": "true",
            "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
        },
    }


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
    config: ContinuationConfig,
    delta_directory: str,
    start_block: int,
    end_block: int,
    write_mode: str,
    minio_endpoint: str,
    minio_access_key: str,
    minio_secret_key: str,
) -> None:
    """Run ``graphsense-cli ingest from-node --sinks delta``.

    Raises ``RuntimeError`` on non-zero exit code.
    """
    gs_config = _build_gs_config(
        config,
        delta_directory=delta_directory,
        minio_endpoint=minio_endpoint,
        minio_access_key=minio_access_key,
        minio_secret_key=minio_secret_key,
    )

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".yaml", prefix="gsconfig-cont-", delete=False
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
        "--start-block", str(start_block),
        "--end-block", str(end_block),
        "--sinks", "delta",
        "--write-mode", write_mode,
        "--ignore-overwrite-safechecks",
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
            f"Ingestion failed (exit {result.returncode}):\n"
            f"cmd: {' '.join(cmd)}\n"
            f"stdout: {result.stdout[-3000:]}\n"
            f"stderr: {result.stderr[-3000:]}"
        )
