"""Run graphsense-cli ingest with different sink combinations."""

from pathlib import Path

from tests.lib.ingest import (
    build_gs_config,
    detect_no_lock_flag,
    make_cli_env,
    run_cli_ingest,
)
from tests.sink_consistency.config import SinkConsistencyConfig


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
    """Run ``graphsense-cli ingest from-node`` with the given sinks."""
    has_cassandra = cassandra_host is not None and keyspace_name is not None
    gs_config = build_gs_config(
        currency=config.currency,
        node_url=config.node_url,
        secondary_node_references=config.secondary_node_references,
        cassandra_host=cassandra_host,
        cassandra_port=cassandra_port,
        keyspace_name=keyspace_name,
        create_keyspace_setup=has_cassandra,
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
        "--create-schema",
        "--write-mode", "overwrite",
        "--ignore-overwrite-safechecks",
        no_lock_flag,
    ]
    for sink in sinks:
        cmd_args.extend(["--sinks", sink])

    run_cli_ingest(
        venv_dir, gs_config, cmd_args, config_prefix="gsconfig-sink-"
    )
