"""Run graphsense-cli ingest variants for the catch-up regression test."""

from pathlib import Path
from typing import Optional

from tests.lib.ingest import (
    build_gs_config,
    detect_no_lock_flag,
    make_cli_env,
    run_cli_ingest,
)
from tests.sink_catchup.config import SinkCatchupConfig


def run_ingest(
    venv_dir: Path,
    config: SinkCatchupConfig,
    sinks: list[str],
    start_block: int,
    end_block: int,
    write_mode: str,
    cassandra_host: Optional[str] = None,
    cassandra_port: Optional[int] = None,
    keyspace_name: Optional[str] = None,
    delta_directory: Optional[str] = None,
    minio_endpoint: Optional[str] = None,
    minio_access_key: Optional[str] = None,
    minio_secret_key: Optional[str] = None,
    label: str = "catch-up ingest",
) -> None:
    """Invoke ``graphsense-cli ingest from-node`` with the given sinks/range."""
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
        "--start-block", str(start_block),
        "--end-block", str(end_block),
        "--create-schema",
        "--write-mode", write_mode,
        "--ignore-overwrite-safechecks",
        no_lock_flag,
    ]
    for sink in sinks:
        cmd_args.extend(["--sinks", sink])

    run_cli_ingest(
        venv_dir, gs_config, cmd_args,
        config_prefix="gsconfig-catchup-",
        label=label,
    )
