"""Run graphsense-cli ingest in a specific virtual environment via subprocess."""

import os
import subprocess
import tempfile
from pathlib import Path

import yaml

from tests.deltalake.config import DeltaTestConfig


def _build_gs_config(
    currency: str,
    node_url: str,
    delta_directory: str,
    minio_endpoint: str,
    minio_access_key: str,
    minio_secret_key: str,
) -> dict:
    """Build a minimal .graphsense.yaml config dict for delta-lake ingestion."""
    schema_type_map = {
        "btc": "utxo",
        "ltc": "utxo",
        "bch": "utxo",
        "zec": "utxo",
        "eth": "account",
        "trx": "account_trx",
    }
    schema_type = schema_type_map.get(currency, "utxo")

    return {
        "environments": {
            "dev": {
                "cassandra_nodes": ["localhost"],
                "keyspaces": {
                    currency: {
                        "raw_keyspace_name": f"{currency}_raw_dev",
                        "transformed_keyspace_name": f"{currency}_transformed_dev",
                        "schema_type": schema_type,
                        "ingest_config": {
                            "node_reference": node_url,
                            "raw_keyspace_file_sinks": {
                                "delta": {
                                    "directory": delta_directory,
                                },
                            },
                        },
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


def run_ingest(
    venv_dir: Path,
    config: DeltaTestConfig,
    delta_directory: str,
    start_block: int,
    end_block: int,
    write_mode: str,
    minio_endpoint: str,
    minio_access_key: str,
    minio_secret_key: str,
) -> None:
    """Run ``graphsense-cli ingest delta-lake ingest`` inside *venv_dir*.

    Creates a temporary ``.graphsense.yaml`` with the right MinIO and node
    settings, points ``GRAPHSENSE_CONFIG_YAML`` at it, then executes the CLI.

    Raises ``RuntimeError`` on non-zero exit code.
    """
    gs_config = _build_gs_config(
        currency=config.currency,
        node_url=config.node_url,
        delta_directory=delta_directory,
        minio_endpoint=minio_endpoint,
        minio_access_key=minio_access_key,
        minio_secret_key=minio_secret_key,
    )

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".yaml", prefix="gsconfig-", delete=False
    ) as f:
        yaml.dump(gs_config, f)
        config_path = f.name

    cli_bin = str(venv_dir / "bin" / "graphsense-cli")
    cmd = [
        cli_bin,
        "ingest",
        "delta-lake",
        "ingest",
        "-e", "dev",
        "-c", config.currency,
        "--start-block", str(start_block),
        "--end-block", str(end_block),
        "--write-mode", write_mode,
    ]

    env = os.environ.copy()
    env.update({
        "GRAPHSENSE_CONFIG_YAML": config_path,
        "PATH": f"{venv_dir / 'bin'}:{os.environ.get('PATH', '/usr/bin:/bin')}",
    })

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            env=env,
            timeout=600,
        )
    finally:
        Path(config_path).unlink(missing_ok=True)

    if result.returncode != 0:
        raise RuntimeError(
            f"Ingestion failed (exit {result.returncode}):\n"
            f"cmd: {' '.join(cmd)}\n"
            f"stdout: {result.stdout[-2000:]}\n"
            f"stderr: {result.stderr[-2000:]}"
        )
