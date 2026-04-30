"""Run ingest and transformation steps for regression tests."""

import subprocess
import tempfile
from pathlib import Path

import yaml

from tests.lib.config import SCHEMA_TYPE_MAP
from tests.lib.constants import TRANSFORMATION_TIMEOUT_S
from tests.lib.ingest import (
    build_gs_config,
    detect_no_lock_flag,
    make_cli_env,
    run_cli_ingest,
)
from tests.transformation.config import TransformationConfig


def _block_bucket_size(currency: str) -> int:
    """Return the block bucket size matching the original transformation config."""
    return 100 if SCHEMA_TYPE_MAP.get(currency) == "utxo" else 1000


def run_ingest_cassandra_direct(
    venv_dir: Path,
    config: TransformationConfig,
    cassandra_host: str,
    cassandra_port: int,
    keyspace_name: str,
) -> None:
    """Run ingest directly to Cassandra (Path A)."""
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
        venv_dir, gs_config, cmd_args, config_prefix="gsconfig-xform-direct-"
    )


def run_ingest_delta_only(
    venv_dir: Path,
    config: TransformationConfig,
    delta_directory: str,
    minio_endpoint: str,
    minio_access_key: str,
    minio_secret_key: str,
) -> None:
    """Run ingest to Delta Lake only (Path B, step 1)."""
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
        venv_dir, gs_config, cmd_args, config_prefix="gsconfig-xform-delta-"
    )


def run_transformation(
    image_name: str,
    config: TransformationConfig,
    delta_directory: str,
    cassandra_host: str,
    cassandra_port: int,
    keyspace_name: str,
    minio_endpoint: str,
    minio_access_key: str,
    minio_secret_key: str,
    start_block: int | None = None,
    end_block: int | None = None,
    extra_args: list[str] | None = None,
) -> None:
    """Run PySpark transformation inside Docker container (Path B, step 2).

    `start_block` / `end_block` override `config.start_block` / `config.end_block`
    when provided (used by the patch-mode regression test to drive the
    transformation in two halves). `extra_args` are appended to the CLI invocation
    after the standard arguments (used to pass `--patch`).
    """
    gs_config = build_gs_config(
        currency=config.currency,
        node_url=config.node_url,
        secondary_node_references=config.secondary_node_references,
        cassandra_host=cassandra_host,
        cassandra_port=cassandra_port,
        keyspace_name=keyspace_name,
        create_keyspace_setup=True,
        block_bucket_size=_block_bucket_size(config.currency),
        delta_directory=delta_directory,
        minio_endpoint=minio_endpoint,
        minio_access_key=minio_access_key,
        minio_secret_key=minio_secret_key,
    )

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".yaml", prefix="gsconfig-xform-spark-", delete=False
    ) as f:
        yaml.dump(gs_config, f)
        config_path = f.name

    sb = config.start_block if start_block is None else start_block
    eb = config.end_block if end_block is None else end_block
    cmd = [
        "docker", "run", "--rm",
        "--network", "host",
        "-v", f"{config_path}:/config.yaml:ro",
        "-e", "GRAPHSENSE_CONFIG_YAML=/config.yaml",
        image_name,
        "graphsense-cli", "transformation", "run",
        "--local",
        "--create-schema",
        "-e", "test",
        "-c", config.currency,
        "--start-block", str(sb),
        "--end-block", str(eb),
        "--delta-lake-path", delta_directory,
    ]
    if extra_args:
        cmd.extend(extra_args)

    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=TRANSFORMATION_TIMEOUT_S
        )
    finally:
        Path(config_path).unlink(missing_ok=True)

    if result.stdout:
        print(f"\n    [spark stdout tail]: {result.stdout[-2000:]}")
    if result.stderr:
        important = [
            line for line in result.stderr.splitlines()
            if any(kw in line.lower() for kw in [
                "error", "exception", "writing", "wrote", "no rows",
                "schema=", "warning", "transform",
            ])
        ]
        if important:
            print("    [spark stderr highlights]:")
            for line in important[-20:]:
                print(f"      {line}")

    if result.returncode != 0:
        raise RuntimeError(
            f"Transformation failed (exit {result.returncode}):\n"
            f"cmd: {' '.join(cmd)}\n"
            f"stdout: {result.stdout[-5000:]}\n"
            f"stderr: {result.stderr[-10000:]}"
        )
