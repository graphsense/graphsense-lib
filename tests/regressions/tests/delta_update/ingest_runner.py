"""Run ingest, PySpark Delta->Cassandra, exchange-rates, and delta-update.

Each helper accepts a venv directory so the same step can be run with the
current local graphsense-lib or with a reference release (e.g. v2.12.3). The
delta-update helper measures wall-clock time, captures the per-section
timings emitted by the in-tree ``LoggerScope`` (``-vv`` -> DEBUG), and
returns both so the test can report per-version totals plus a breakdown of
which sections of the updater take the longest.
"""

import re
import subprocess
import tempfile
import time
from collections import defaultdict
from pathlib import Path

import yaml

from tests.lib.config import SCHEMA_TYPE_MAP
from tests.lib.constants import INGEST_TIMEOUT_S, TRANSFORMATION_TIMEOUT_S
from tests.lib.ingest import (
    build_gs_config,
    detect_no_lock_flag,
    make_cli_env,
    run_cli_ingest,
)
from tests.delta_update.config import DeltaUpdateConfig


DELTA_UPDATE_TIMEOUT_S = 1800

# Matches the LoggerScope exit line: "E - <message> - took 0.123s".
# Captures the message and the elapsed seconds. The leading log prefix
# (timestamp | subsystem |) is consumed by the non-anchored search.
_TOOK_RE = re.compile(r"E - (?P<msg>.+?) - took (?P<secs>\d+\.\d+)s")


def aggregate_section_timings(log_text: str) -> list[tuple[str, float, int]]:
    """Aggregate LoggerScope ``took Xs`` lines by section message.

    Returns a list of ``(section, total_seconds, occurrences)`` tuples
    sorted by total_seconds descending. Nested scopes are counted under
    their own section -- their time is *also* included in the parent's
    elapsed (LoggerScope reports wall time per scope), so totals do not sum
    to the run wall-clock and entries should be read as "this section was
    on the critical path for this much wall time across the run".
    """
    totals: dict[str, float] = defaultdict(float)
    counts: dict[str, int] = defaultdict(int)
    for m in _TOOK_RE.finditer(log_text):
        msg = m.group("msg").strip()
        totals[msg] += float(m.group("secs"))
        counts[msg] += 1
    return sorted(
        ((msg, totals[msg], counts[msg]) for msg in totals),
        key=lambda t: t[1],
        reverse=True,
    )

# UTXO bech32 HRPs as used by graphsense-lib's transformed data_configuration.
# bch / zec do not use bech32 in their address-decoding path -- the production
# config sets these to '' (empty string).
_UTXO_BECH32_PREFIX = {
    "btc": "bc",
    "ltc": "ltc1",
    "bch": "",
    "zec": "",
}


def _block_bucket_size(currency: str) -> int:
    return 100 if SCHEMA_TYPE_MAP.get(currency) == "utxo" else 1000


def _add_transformed_setup_utxo(
    gs_config: dict,
    environment: str,
    currency: str,
    transformed_keyspace_name: str,
) -> None:
    """Add a UTXO ``transformed`` keyspace_setup_config entry in-place.

    ``delta-update update --create-schema`` reads this section to materialise
    the transformed keyspace; ``build_gs_config`` only emits the ``raw`` half
    because the existing ingest tests don't need it.
    """
    ks_config = (
        gs_config["environments"][environment]["keyspaces"][currency]
    )
    setup = ks_config.setdefault("keyspace_setup_config", {})
    setup["transformed"] = {
        "replication_config": (
            "{'class': 'SimpleStrategy', 'replication_factor': 1}"
        ),
        "data_configuration": {
            "keyspace_name": transformed_keyspace_name,
            "address_prefix_length": 4,
            "bech_32_prefix": _UTXO_BECH32_PREFIX.get(currency, ""),
            "bucket_size": 5000,
            "coinjoin_filtering": True,
            "fiat_currencies": ["EUR", "USD"],
        },
    }


def run_ingest_delta_only(
    venv_dir: Path,
    config: DeltaUpdateConfig,
    delta_directory: str,
    minio_endpoint: str,
    minio_access_key: str,
    minio_secret_key: str,
) -> None:
    """Ingest blocks from the configured node into a Delta Lake on MinIO."""
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
        venv_dir, gs_config, cmd_args,
        config_prefix="gsconfig-deltaupdate-delta-",
        timeout=INGEST_TIMEOUT_S,
    )


def run_spark_transformation_to_raw(
    image_name: str,
    config: DeltaUpdateConfig,
    delta_directory: str,
    cassandra_host: str,
    cassandra_port: int,
    keyspace_name: str,
    minio_endpoint: str,
    minio_access_key: str,
    minio_secret_key: str,
) -> None:
    """Run ``graphsense-cli transformation run`` (Delta Lake -> Cassandra raw).

    Mirrors tests/transformation/ingest_runner.run_transformation but kept here
    so the delta_update suite is self-contained and can evolve independently.
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
        mode="w", suffix=".yaml", prefix="gsconfig-deltaupdate-spark-",
        delete=False,
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
        "--s3-config", "minio",
    ]

    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True,
            timeout=TRANSFORMATION_TIMEOUT_S,
        )
    finally:
        Path(config_path).unlink(missing_ok=True)

    if result.returncode != 0:
        raise RuntimeError(
            f"Spark transformation (raw) failed (exit {result.returncode}):\n"
            f"cmd: {' '.join(cmd)}\n"
            f"stdout: {result.stdout[-5000:]}\n"
            f"stderr: {result.stderr[-10000:]}"
        )


def run_exchange_rates_ingest(
    venv_dir: Path,
    config: DeltaUpdateConfig,
    cassandra_host: str,
    cassandra_port: int,
    keyspace_name: str,
) -> None:
    """Populate the raw keyspace's exchange_rates table via cryptocompare."""
    gs_config = build_gs_config(
        currency=config.currency,
        node_url=config.node_url,
        secondary_node_references=config.secondary_node_references,
        cassandra_host=cassandra_host,
        cassandra_port=cassandra_port,
        keyspace_name=keyspace_name,
    )

    cmd_args = [
        "exchange-rates", "cryptocompare", "ingest",
        "-e", "test",
        "-c", config.currency,
    ]

    run_cli_ingest(
        venv_dir, gs_config, cmd_args,
        config_prefix="gsconfig-deltaupdate-rates-",
    )


def run_delta_update(
    venv_dir: Path,
    config: DeltaUpdateConfig,
    cassandra_host: str,
    cassandra_port: int,
    raw_keyspace: str,
    transformed_keyspace: str,
    delta_directory: str,
    minio_endpoint: str,
    minio_access_key: str,
    minio_secret_key: str,
    write_batch_size: int = 10,
    label: str = "delta-update",
) -> tuple[float, list[tuple[str, float, int]]]:
    """Run ``graphsense-cli delta-update update`` with timing.

    Always uses ``--updater-version 2`` because the perf-targeted commit
    modifies the v2 (full) UTXO updater. ``--create-schema`` is set so each
    side initialises its own transformed keyspace from the schema shipped with
    *its* graphsense-lib version -- this isolates current vs baseline schemas.

    Returns:
        (wall_seconds, section_timings) where section_timings is a list of
        ``(section, total_seconds, occurrences)`` aggregated from the
        updater's own ``LoggerScope`` debug logs (sorted by total seconds
        desc). ``-vv`` is set on the CLI so DEBUG-level timings are emitted.
    """
    gs_config = build_gs_config(
        currency=config.currency,
        node_url=config.node_url,
        secondary_node_references=config.secondary_node_references,
        cassandra_host=cassandra_host,
        cassandra_port=cassandra_port,
        keyspace_name=raw_keyspace,
        transformed_keyspace_name=transformed_keyspace,
        create_keyspace_setup=True,
        block_bucket_size=_block_bucket_size(config.currency),
        # delta-update demands ingest_config.raw_keyspace_file_sinks.delta to
        # exist (get_deltaupdater_config returns None otherwise -> exit 11).
        # The same MinIO bucket is reused -- delta-update doesn't actually
        # read from it during the update step, but the presence is required.
        delta_directory=delta_directory,
        minio_endpoint=minio_endpoint,
        minio_access_key=minio_access_key,
        minio_secret_key=minio_secret_key,
    )
    _add_transformed_setup_utxo(
        gs_config,
        environment="test",
        currency=config.currency,
        transformed_keyspace_name=transformed_keyspace,
    )

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".yaml", prefix="gsconfig-deltaupdate-update-",
        delete=False,
    ) as f:
        yaml.dump(gs_config, f)
        config_path = f.name

    cli_bin = str(venv_dir / "bin" / "graphsense-cli")
    env = make_cli_env(venv_dir, config_path)

    # --start-block is intentionally omitted. After --create-schema the
    # transformed state initialises hb_du/hb_ft at 0, and find_import_range
    # rejects start_block <= last_block, so letting it default makes the
    # updater pick up at "last_block + 1" on both sides identically.
    # ``-vv`` enables DEBUG so LoggerScope emits per-scope ``took Xs`` lines.
    cmd = [
        cli_bin,
        "-vv",
        "delta-update", "update",
        "-e", "test",
        "-c", config.currency,
        "--end-block", str(config.end_block),
        "--updater-version", "2",
        "--write-batch-size", str(write_batch_size),
        "--create-schema",
        "--no-pedantic",
        "--disable-safety-checks",
    ]

    t0 = time.perf_counter()
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, env=env,
            timeout=DELTA_UPDATE_TIMEOUT_S,
        )
    finally:
        Path(config_path).unlink(missing_ok=True)
    elapsed = time.perf_counter() - t0

    if result.returncode != 0:
        raise RuntimeError(
            f"{label} failed (exit {result.returncode}):\n"
            f"cmd: {' '.join(cmd)}\n"
            f"stdout: {result.stdout[-5000:]}\n"
            f"stderr: {result.stderr[-5000:]}"
        )

    full_output = (result.stdout or "") + "\n" + (result.stderr or "")
    section_timings = aggregate_section_timings(full_output)
    return elapsed, section_timings
