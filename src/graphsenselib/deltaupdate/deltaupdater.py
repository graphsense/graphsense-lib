import logging
import sys
from contextlib import nullcontext
from typing import Optional

from graphsenselib.db.parallel import ParallelDbPool, init_worker

from ..config import get_config
from ..db import DbFactory
from ..utils import batch, get_cassandra_result_as_dateframe
from ..utils.console import console
from ..utils.locking import LockAcquisitionError, create_lock
from ..utils.signals import graceful_ctlc_shutdown
from .update import AbstractUpdateStrategy, Action, UpdaterFactory

logger = logging.getLogger(__name__)


def _recover_pending_writes(db, pedantic: bool, force_wal_replay: bool = False) -> None:
    """Replay any pending delta-update WAL record to completion before the run.

    Idempotent: replays the exact resolved (absolute-value) changes the crashed
    run staged, then clears the record. A torn (headerless) stage leaves nothing
    applied and is swept. Raises on a version-mismatched record so stale writes
    are never applied automatically.

    When ``force_wal_replay`` is set and a version-mismatched record is found,
    the operator is shown a loud warning and asked for an interactive yes/no
    confirmation before the version fence is overridden. Declining (or a
    non-interactive session) re-raises the mismatch and aborts the run.
    """
    import click

    from graphsenselib import __version__

    from .update.abstractupdater import make_run_id
    from .update.utxo.update import apply_changes
    from .wal import DeltaWal, WalVersionMismatch

    wal = DeltaWal(db.transformed, make_run_id(), __version__)
    wal.ensure_schema()

    def apply_fn(changes):
        apply_changes(db, changes, pedantic, try_atomic_writes=False)

    try:
        wal.recover(apply_fn)
    except WalVersionMismatch as e:
        if not force_wal_replay:
            raise
        logger.warning("%s", e)
        confirmed = click.confirm(
            "A pending WAL record was written by a different code version (see "
            "warning above). Forcing replay writes the previously staged "
            "ABSOLUTE values verbatim WITHOUT recomputing them. Only proceed if "
            "you are certain those staged values are correct under the current "
            "code. Force replay now?",
            default=False,
        )
        if not confirmed:
            logger.error("WAL force-replay declined; aborting run.")
            raise
        wal.recover(apply_fn, allow_version_mismatch=True)


def adjust_start_block(db, start_block) -> int:
    last_er = None
    for i in [start_block - i for i in range(1, 4)]:
        last_er = db.transformed.get_exchange_rates_by_block(i)
        if last_er is not None:
            return i + 1
    raise Exception("Could not find proper start block after full-transform")


def find_import_range(
    db,
    start_block_overwrite,
    end_block_overwrite=None,
    forward_fill_rates=False,
    disable_safety_checks=False,
):
    hb_ft = db.transformed.get_highest_block_fulltransform()
    hb_raw = db.raw.get_highest_block()
    hb_du = db.transformed.get_highest_block_delta_updater()
    # initialize to zero if du is run before full transform
    hb_du = 0 if hb_du is None else hb_du
    hb_ft = 0 if hb_ft is None else hb_ft

    if start_block_overwrite is not None:
        last_block = max([i for i in (hb_du, hb_ft) if i is not None])
        if start_block_overwrite <= last_block:
            raise Exception(
                f"Start block {start_block_overwrite} is before last "
                f"delta update {hb_du}."
                f" Or before last full transform {hb_ft}."
                f" This would corrupt the state of balances. "
                f"Also make sure the transformations starts at block 0. Exiting."
            )
        if (start_block_overwrite > last_block + 1) and not disable_safety_checks:
            raise Exception(
                f"Start block {start_block_overwrite} is in the future."
                f" It looks like blocks are left out in the transformation."
                f" Start block should be {last_block + 1}"
                f" Tried starting at {start_block_overwrite}. "
                f"Also make sure the transformations starts at block 0. Exiting."
            )

    start_block = hb_du + 1 if start_block_overwrite is None else start_block_overwrite
    latest_address_id = db.transformed.get_highest_address_id()
    latest_cluster_id = db.transformed.get_highest_cluster_id()

    # initialize to zero/one if du is run before full transform
    # cluster id is initialized to 1 since 0 is reserved for coinbase
    latest_address_id = 0 if latest_address_id is None else latest_address_id
    latest_cluster_id = 1 if latest_cluster_id is None else latest_cluster_id

    logger.info(f"Last addr id:       {latest_address_id:12}")
    if latest_cluster_id is not None:
        logger.info(f"Last cltr id:       {latest_cluster_id:12}")
    logger.info(f"Raw     Config:      {db.raw.get_configuration()}")
    logger.info(f"Transf. Config:      {db.transformed.get_configuration()}")

    # if ff_rates is enabled we always sync till the highest block
    # we fill the non available rates with the last available rate.
    end_block = (
        db.raw.find_highest_block_with_exchange_rates()
        if not forward_fill_rates
        else hb_raw
    )
    end_block = end_block if end_block_overwrite is None else end_block_overwrite
    logger.info(f"Last delta-transform: {(start_block - 1):10}")
    logger.info(f"Last raw block:       {hb_raw:10}")
    logger.info(f"Last raw block:       {end_block:10} (with exchanges rates).")
    logger.info(
        f"Transf. behind raw:   {(end_block - (start_block - 1)):10} (delta-transform)"
    )
    logger.info(f"Transf. behind raw:   {(end_block - hb_ft):10} (full-transform)")
    return (
        start_block,
        end_block,
        (end_block_overwrite is not None and end_block <= hb_du),
    )


def state(env, currency):
    with DbFactory().from_config(env, currency) as db:
        start_block, end_block, _ = find_import_range(db, None)
        logger.info(
            "Parameters would update the transformed "
            f"keyspace from {start_block}-{end_block}."
        )

        console.rule("Update history")
        res = sorted(
            db.transformed.get_delta_updater_history(),
            key=lambda r: r.last_synced_block,
        )
        console.print(get_cassandra_result_as_dateframe(res))


def validate(env, currency, look_back_blocks):
    with DbFactory().from_config(env, currency) as db:
        offset = look_back_blocks
        highest_block_before_delta = db.transformed.get_highest_block()
        highest_block_delta = db.transformed.get_highest_block_delta_updater()

        console.print(
            f"==== Checking imported exchange rates for gaps, "
            f"from {offset} blocks before delta from {highest_block_before_delta} "
            f"- {offset} to {highest_block_delta}"
        )
        errc = 0
        for b in range(highest_block_before_delta - offset, highest_block_delta + 1):
            er = db.transformed.get_exchange_rates_by_block(b)
            if er is None or er.fiat_values is None:
                errc += 1
                console.print(f"Missing exchange rates for block {b}!!!")

        if errc > 0:
            console.print(f"{errc} Gaps found in exchange rates.")
            sys.exit(92)
        else:
            console.print("No gaps found in exchange rates")


def update_transformed(
    start_block: int,
    end_block: int,
    updater: AbstractUpdateStrategy,
    batch_size=10,
):
    updater.prepare_database()

    with graceful_ctlc_shutdown() as shutdown_initialized:
        for b in batch(range(start_block, end_block + 1), n=batch_size):
            logger.info(
                f"Working on batch ({len(b)}) "
                f"from block {min(b)} to {max(b)}. "
                f"Done with {min(b) - start_block}, {end_block - min(b) + 1} to go."
            )
            updater.reset_timing()
            action = updater.process_batch(b)
            if action == Action.DATA_TO_PROCESS_NOT_FOUND:
                logger.warning(
                    f"First block in batch {min(b)} is empty. Finishing update."
                )
                raise Exception("Data to execute delta update not found. See log file.")
            updater.persist_updater_progress()

            blocks_processed = (updater.last_block_processed - start_block) + 1
            to_go = end_block - max(b)
            bps = blocks_processed / updater.elapsed_seconds_global
            bps_batch = len(b) / updater.elapsed_seconds_last_batch

            logger.info(
                f"Batch of {len(b)} blocks took "
                f"{updater.elapsed_seconds_last_batch:.3f} s that's "
                f"{bps_batch:.1f} blks/s. Approx. {((to_go / bps) / 60):.3f} "
                "minutes remaining."
            )

            _log_batch_timing(updater)

            if shutdown_initialized():
                logger.info(f"Got shutdown signal stopping at block {b[-1]}")
                return b[-1]

    return end_block


def _log_batch_timing(updater):
    timing = updater.timing_summary
    # Exclude breakdown dict from top-level sum
    batch_total = sum(v for k, v in timing.items() if isinstance(v, (int, float)))

    parts = []
    for name, value in timing.items():
        if isinstance(value, (int, float)) and value > 0:
            pct = (value / batch_total * 100) if batch_total > 0 else 0
            parts.append(f"{name}={value:.1f}s ({pct:.1f}%)")

    timing_str = ", ".join(parts) if parts else "no timing data"
    logger.info(f"Batch timing: {timing_str}")

    # DEBUG level: detailed Cassandra read breakdown
    if logger.isEnabledFor(logging.DEBUG):
        breakdown = timing.get("cassandra_read_breakdown", {})
        if breakdown:
            logger.debug(
                f"  Cassandra read breakdown: "
                f"check_existence={breakdown.get('check_existence', 0):.1f}s, "
                f"read_addresses={breakdown.get('read_addresses', 0):.1f}s, "
                f"query_relations={breakdown.get('query_relations', 0):.1f}s"
            )


def update(
    env: str,
    currency: str,
    start_block: Optional[int],
    end_block: Optional[int],
    write_new: bool,
    write_dirty: bool,
    write_batch_size: int,
    updater_version: int,
    pedantic: bool,
    forward_fill_rates: bool,
    disable_safety_checks: bool = False,
    parallel_workers: int = 1,
    enable_wal: Optional[bool] = None,
    force_wal_replay: bool = False,
):
    with DbFactory().from_config(env, currency) as db:
        config = get_config()
        # Flag overrides config; config (default True) decides when unset.
        wal_enabled = (
            enable_wal if enable_wal is not None else config.delta_updater_wal_enabled
        )
        logger.info(f"Delta update WAL: {'enabled' if wal_enabled else 'disabled'}")
        if force_wal_replay and not wal_enabled:
            logger.warning(
                "--force-wal-replay has no effect while the WAL is disabled; "
                "enable it with --enable-wal to recover a pending record."
            )
        if config.get_keyspace_config(env, currency).disable_delta_updates:
            logger.error(
                f"Delta updates are disabled for {env} - {currency} in the "
                f"configuration at {config.path()}"
            )
            sys.exit(125)

        raw_ks = db.raw.get_keyspace()
        transformed_ks = db.transformed.get_keyspace()
        # Pure auto-resume (no explicit range) is the only path that can
        # silently double-count or skip blocks if the previous run's
        # bookkeeping write was torn (e.g. a Cassandra outage). An explicit
        # --start-block / --end-block means the operator is in manual control.
        auto_resume = start_block is None and end_block is None
        try:
            # Acquisition order: raw -> transformed (matches transformation/cli.py).
            with create_lock(raw_ks), create_lock(transformed_ks):
                # Crash recovery: replay a torn batch's resolved writes (if any)
                # BEFORE reading any state. The next batch computes aggregate
                # deltas read-modify-write against the keyspace, so a
                # partially-applied batch must be completed first. Replaying the
                # stored absolute-value changes is idempotent; recomputing the
                # block would double-count. Only when the WAL is enabled — when
                # off, nothing is ever staged and the table is not created.
                if wal_enabled:
                    _recover_pending_writes(db, pedantic, force_wal_replay)

                start_block, end_block, patch_mode = find_import_range(
                    db,
                    start_block,
                    end_block,
                    forward_fill_rates=forward_fill_rates,
                    disable_safety_checks=disable_safety_checks,
                )

                if auto_resume and not disable_safety_checks:
                    hb_du = db.transformed.get_highest_block_delta_updater()
                    if (
                        hb_du
                        and not db.transformed.is_first_delta_update_run()
                        and not db.transformed.delta_updater_history_has_block(hb_du)
                    ):
                        raise Exception(
                            "Delta-updater state is inconsistent: "
                            f"summary_statistics is at block {hb_du} but "
                            "delta_updater_history has no matching row. The "
                            "last batch's bookkeeping write was likely torn "
                            "by a database outage, so data for the most "
                            "recent batch may be only partially applied. "
                            "Refusing to auto-resume to avoid silently "
                            "double-counting or skipping blocks. Reconcile "
                            "the affected block range (re-run the full "
                            "transform for it) and pass an explicit "
                            "--start-block to continue, or "
                            "--disable-safety-checks to override."
                        )

                if end_block >= start_block:
                    is_first_delta_run = db.transformed.is_first_delta_update_run()
                    if is_first_delta_run:
                        # Full transform set nr_blocks a bit different (currency dep).
                        # To be sure about the block we look at exchange rates table
                        start_block = adjust_start_block(db, start_block)
                    # else:
                    #     last_block_er = db.transformed.get_exchange_rates_by_block(
                    #         start_block - 1
                    #     )
                    #     if last_block_er is None:
                    #         raise Exception(
                    #             "Could not find exchange rate for start block "
                    #             f"{start_block} - 1, is this an error?"
                    #             )
                    logger.info(
                        "Start updating transformed "
                        f"Keyspace {start_block}-{end_block}."
                    )
                    if patch_mode:
                        logger.warning(
                            "Running in patch mode, no_block will not be updated."
                        )

                    if not db.transformed.is_configuration_populated():
                        config_defaults = (
                            config.get_keyspace_config(env, currency)
                            .keyspace_setup_config["transformed"]
                            .data_configuration
                        )
                        logger.warning(
                            "Config table in transformed not populated."
                            f" Setting default values {config_defaults}."
                        )
                        db.transformed.ingest("configuration", [config_defaults])

                    du_config = config.get_deltaupdater_config(env, currency)
                    if du_config is None:
                        logger.error(
                            f"Delta sink not configured for {currency} in {env}. "
                            "Cannot run delta update."
                        )
                        sys.exit(11)
                    pool_ctx = (
                        ParallelDbPool(parallel_workers, init_worker, (env, currency))
                        if parallel_workers > 1
                        else nullcontext(None)
                    )
                    with pool_ctx as parallel_pool:
                        update_transformed(
                            start_block,
                            end_block,
                            UpdaterFactory().get_updater(
                                du_config,
                                db,
                                updater_version,
                                write_new,
                                write_dirty,
                                pedantic,
                                write_batch_size,
                                patch_mode=patch_mode,
                                forward_fill_rates=forward_fill_rates,
                                parallel_pool=parallel_pool,
                                wal_enabled=wal_enabled,
                            ),
                            batch_size=write_batch_size,
                        )

                elif end_block == start_block or start_block - 1 == end_block:
                    logger.info("Nothing to do. Data is up to date.")
                else:
                    raise Exception(
                        "Transformed space is ahead of raw keyspace. "
                        "This should not happen. Call 911."
                        f"start {start_block}, end {end_block}"
                    )

        except LockAcquisitionError as e:
            logger.error(str(e))
            sys.exit(911)


def patch_exchange_rates(env: str, currency: str, block: int):
    with DbFactory().from_config(env, currency) as db:
        ers = db.raw.get_exchange_rates_for_block_batch([block])
        logger.info(
            f"Overwriting transformed exchange_rate for block {block} with {ers}"
        )

        db.transformed.ingest("exchange_rates", ers)
