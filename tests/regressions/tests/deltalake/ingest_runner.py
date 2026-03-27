"""Run graphsense-cli ingest via subprocess."""

import time
from pathlib import Path

from tests.deltalake.config import DeltaTestConfig
from tests.lib.ingest import build_gs_config, run_cli_ingest


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
    """Run ``graphsense-cli ingest delta-lake ingest`` inside *venv_dir*."""
    gs_config = build_gs_config(
        currency=config.currency,
        node_url=config.node_url,
        secondary_node_references=config.secondary_node_references,
        environment="dev",
        delta_directory=delta_directory,
        minio_endpoint=minio_endpoint,
        minio_access_key=minio_access_key,
        minio_secret_key=minio_secret_key,
    )

    cmd_args = [
        "ingest", "delta-lake", "ingest",
        "-e", "dev",
        "-c", config.currency,
        "--start-block", str(start_block),
        "--end-block", str(end_block),
        "--write-mode", write_mode,
    ]
    if write_mode == "overwrite":
        cmd_args.append("--ignore-overwrite-safechecks")

    run_cli_ingest(
        venv_dir, gs_config, cmd_args, config_prefix="gsconfig-delta-"
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
