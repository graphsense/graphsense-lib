"""Run ingest and transformation steps for regression tests.

- run_ingest_cassandra_direct: ingest directly to Cassandra (venv subprocess)
- run_ingest_delta_only: ingest to Delta Lake via MinIO (venv subprocess)
- run_transformation: run PySpark transformation in Docker container
"""

import os
import subprocess
import tempfile
from pathlib import Path

import yaml

from tests.deltalake.config import SCHEMA_TYPE_MAP
from tests.transformation.config import TransformationConfig


def _build_gs_config(
    config: TransformationConfig,
    cassandra_host: str | None = None,
    cassandra_port: int | None = None,
    keyspace_name: str | None = None,
    delta_directory: str | None = None,
    minio_endpoint: str | None = None,
    minio_access_key: str | None = None,
    minio_secret_key: str | None = None,
) -> dict:
    """Build a minimal .graphsense.yaml for the requested configuration."""
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
                    "block_bucket_size": 100 if schema_type == "utxo" else 1000,
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


def run_ingest_cassandra_direct(
    venv_dir: Path,
    config: TransformationConfig,
    cassandra_host: str,
    cassandra_port: int,
    keyspace_name: str,
) -> None:
    """Run ingest directly to Cassandra (Path A)."""
    gs_config = _build_gs_config(
        config,
        cassandra_host=cassandra_host,
        cassandra_port=cassandra_port,
        keyspace_name=keyspace_name,
    )

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".yaml", prefix="gsconfig-xform-direct-", delete=False
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

    result = subprocess.run(cmd, capture_output=True, text=True, env=env, timeout=600)
    Path(config_path).unlink(missing_ok=True)

    if result.returncode != 0:
        raise RuntimeError(
            f"Direct Cassandra ingest failed (exit {result.returncode}):\n"
            f"cmd: {' '.join(cmd)}\n"
            f"stdout: {result.stdout[-3000:]}\n"
            f"stderr: {result.stderr[-3000:]}"
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
    gs_config = _build_gs_config(
        config,
        delta_directory=delta_directory,
        minio_endpoint=minio_endpoint,
        minio_access_key=minio_access_key,
        minio_secret_key=minio_secret_key,
    )

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".yaml", prefix="gsconfig-xform-delta-", delete=False
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

    result = subprocess.run(cmd, capture_output=True, text=True, env=env, timeout=600)
    Path(config_path).unlink(missing_ok=True)

    if result.returncode != 0:
        raise RuntimeError(
            f"Delta-only ingest failed (exit {result.returncode}):\n"
            f"cmd: {' '.join(cmd)}\n"
            f"stdout: {result.stdout[-3000:]}\n"
            f"stderr: {result.stderr[-3000:]}"
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
) -> None:
    """Run PySpark transformation inside Docker container (Path B, step 2)."""
    schema_type = SCHEMA_TYPE_MAP.get(config.currency, "utxo")

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
        mode="w", suffix=".yaml", prefix="gsconfig-xform-spark-", delete=False
    ) as f:
        yaml.dump(gs_config, f)
        config_path = f.name

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
        "--start-block", str(config.start_block),
        "--end-block", str(config.end_block),
        "--delta-lake-path", delta_directory,
    ]

    result = subprocess.run(cmd, capture_output=True, text=True, timeout=1200)
    Path(config_path).unlink(missing_ok=True)

    # Always print transformation output for debugging
    if result.stdout:
        print(f"\n    [spark stdout tail]: {result.stdout[-2000:]}")
    if result.stderr:
        # Filter out Spark noise, show important lines
        important = [
            line for line in result.stderr.splitlines()
            if any(kw in line.lower() for kw in [
                "error", "exception", "writing", "wrote", "no rows",
                "schema=", "warning", "transform",
            ])
        ]
        if important:
            print(f"    [spark stderr highlights]:")
            for line in important[-20:]:
                print(f"      {line}")

    if result.returncode != 0:
        raise RuntimeError(
            f"Transformation failed (exit {result.returncode}):\n"
            f"cmd: {' '.join(cmd)}\n"
            f"stdout: {result.stdout[-5000:]}\n"
            f"stderr: {result.stderr[-10000:]}"
        )
