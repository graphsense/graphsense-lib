"""Run graphsense-cli ingest via subprocess."""

import os
import subprocess
import tempfile
import time
from pathlib import Path

import yaml

from tests.deltalake.config import SCHEMA_TYPE_MAP, DeltaTestConfig


def _build_gs_config(
    currency: str,
    node_url: str,
    delta_directory: str,
    minio_endpoint: str,
    minio_access_key: str,
    minio_secret_key: str,
    secondary_node_references: list[str] | None = None,
) -> dict:
    """Build a minimal .graphsense.yaml config dict for delta-lake ingestion."""
    schema_type = SCHEMA_TYPE_MAP.get(currency, "utxo")

    ingest_config: dict = {
        "node_reference": node_url,
        "raw_keyspace_file_sinks": {
            "delta": {
                "directory": delta_directory,
            },
        },
    }
    if secondary_node_references:
        ingest_config["secondary_node_references"] = secondary_node_references

    return {
        "environments": {
            "dev": {
                "cassandra_nodes": ["localhost"],
                "keyspaces": {
                    currency: {
                        "raw_keyspace_name": f"{currency}_raw_dev",
                        "transformed_keyspace_name": f"{currency}_transformed_dev",
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
        secondary_node_references=config.secondary_node_references,
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
    if write_mode == "overwrite":
        cmd.append("--ignore-overwrite-safechecks")

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


def timed_run_ingest(
    venv_dir: Path,
    config: DeltaTestConfig,
    delta_directory: str,
    start_block: int,
    end_block: int,
    write_mode: str,
    minio_endpoint: str,
    minio_access_key: str,
    minio_secret_key: str,
) -> float:
    """Run ingest via subprocess and return wall-clock seconds."""
    t0 = time.perf_counter()
    run_ingest(
        venv_dir=venv_dir,
        config=config,
        delta_directory=delta_directory,
        start_block=start_block,
        end_block=end_block,
        write_mode=write_mode,
        minio_endpoint=minio_endpoint,
        minio_access_key=minio_access_key,
        minio_secret_key=minio_secret_key,
    )
    return time.perf_counter() - t0


def copy_s3_prefix(s3_client, bucket: str, src_prefix: str, dst_prefix: str) -> None:
    """Copy all objects under src_prefix to dst_prefix within the same bucket."""
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=src_prefix):
        for obj in page.get("Contents", []):
            src_key = obj["Key"]
            dst_key = src_key.replace(src_prefix, dst_prefix, 1)
            s3_client.copy_object(
                Bucket=bucket,
                CopySource={"Bucket": bucket, "Key": src_key},
                Key=dst_key,
            )
