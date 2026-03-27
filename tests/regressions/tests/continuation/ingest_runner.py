"""Run graphsense-cli ingest for continuation tests."""

from pathlib import Path

from tests.continuation.config import ContinuationConfig
from tests.lib.ingest import (
    build_gs_config,
    detect_no_lock_flag,
    make_cli_env,
    run_cli_ingest,
)


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
    """Run ``graphsense-cli ingest from-node --sinks delta``."""
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
        "--start-block", str(start_block),
        "--end-block", str(end_block),
        "--sinks", "delta",
        "--write-mode", write_mode,
        "--ignore-overwrite-safechecks",
        no_lock_flag,
    ]

    run_cli_ingest(
        venv_dir, gs_config, cmd_args, config_prefix="gsconfig-cont-"
    )
