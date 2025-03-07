import logging
import sys
from typing import Optional

from filelock import FileLock
from filelock import Timeout as LockFileTimeout

from ..config import get_config
from ..db import DbFactory
from ..utils import batch, get_cassandra_result_as_dateframe
from ..utils.console import console
from ..utils.signals import graceful_ctlc_shutdown
from .update import AbstractUpdateStrategy, Action, UpdaterFactory

logger = logging.getLogger(__name__)


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
    end_block_overwrite,
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

            if shutdown_initialized():
                logger.info(f"Got shutdown signal stopping at block {b[-1]}")
                return b[-1]

    return end_block


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
):
    try:
        with DbFactory().from_config(env, currency) as db:
            config = get_config()
            if config.get_keyspace_config(env, currency).disable_delta_updates:
                logger.error(
                    f"Delta updates are disabled for {env} - {currency} in the "
                    f"configuration at {config.path()}"
                )
                sys.exit(125)

            lockfile_name = (
                f"/tmp/{db.raw.get_keyspace()}_{db.transformed.get_keyspace()}.lock"
            )
            logger.info(f"Try acquiring lockfile {lockfile_name}")
            with FileLock(lockfile_name, timeout=1):
                start_block, end_block, patch_mode = find_import_range(
                    db,
                    start_block,
                    end_block,
                    forward_fill_rates=forward_fill_rates,
                    disable_safety_checks=disable_safety_checks,
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

    except LockFileTimeout:
        logger.error(
            f"Lockfile {lockfile_name} could not be acquired. "
            "Is another ingest running? If not delete the lockfile."
        )
        sys.exit(911)


def patch_exchange_rates(env: str, currency: str, block: int):
    with DbFactory().from_config(env, currency) as db:
        ers = db.raw.get_exchange_rates_for_block_batch([block])
        logger.info(
            f"Overwriting transformed exchange_rate for block {block} with {ers}"
        )

        db.transformed.ingest("exchange_rates", ers)
