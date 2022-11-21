import logging
import sys
from typing import Optional

from filelock import FileLock
from filelock import Timeout as LockFileTimeout

from ..config import config
from ..db import DbFactory
from ..utils import batch, get_cassandra_result_as_dateframe
from ..utils.console import console
from ..utils.signals import gracefull_ctlc_shutdown
from .update import AbstractUpdateStrategy, UpdaterFactory

logger = logging.getLogger(__name__)


def find_import_range(db, start_block_overwrite):
    hb_ft = db.transformed.get_highest_block_fulltransform()
    hb_raw = db.raw.get_highest_block()
    start_block = (
        db.transformed.get_highest_block_delta_updater()
        if start_block_overwrite is None
        else start_block_overwrite
    ) + 1
    latest_address_id = db.transformed.get_highest_address_id()
    latest_cluster_id = db.transformed.get_highest_cluster_id()
    logger.info(f"Last addr id:       {latest_address_id:12}")
    if latest_cluster_id is not None:
        logger.info(f"Last cltr id:       {latest_cluster_id:12}")
    logger.info(f"Raw     Config:      {db.raw.get_configuration()}")
    logger.info(f"Transf. Config:      {db.transformed.get_configuration()}")
    end_block = db.raw.find_highest_block_with_exchange_rates()
    logger.info(f"Last delta-transform: {(start_block -1):10}")
    logger.info(f"Last raw block:       {hb_raw:10}")
    logger.info(f"Last raw block:       {end_block:10} (with exchanges rates).")
    logger.info(
        f"Transf. behind raw:   {(end_block - (start_block -1)):10} (delta-transform)"
    )
    logger.info(f"Transf. behind raw:   {(end_block - hb_ft):10} (full-transform)")
    return start_block, end_block


def state(env, currency):
    with DbFactory().from_config(env, currency) as db:
        start_block, end_block = find_import_range(db, None)
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


def validate(env, currency):
    with DbFactory().from_config(env, currency) as db:
        offset = 20
        highest_block_before_delta = db.transformed.get_highest_block()
        highest_block_delta = db.transformed.get_highest_block_delta_updater()

        console.print(
            f"==== Checking imported exchange rates for gaps,"
            f"from {offset} blocks before delta from {highest_block_before_delta} "
            f"- {offset} to {highest_block_delta}"
        )
        errc = 0
        for b in range(highest_block_before_delta - offset, highest_block_delta):
            er = db.transformed.get_exchange_rates_by_block(b)
            if er is None:
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
    with gracefull_ctlc_shutdown() as shutdown_initialized:
        for b in batch(range(start_block, end_block), n=batch_size):

            logger.info(
                f"Working on batch ({batch_size}) "
                f"from block {min(b)} to {max(b)}. "
                f"Done with {min(b) - start_block}, {end_block - min(b)} to go."
            )
            updater.process_batch(b)
            updater.persist_updater_progress()

            blocks_processed = (updater.last_block_processed - start_block) + 1
            to_go = end_block - max(b)
            bps = blocks_processed / updater.elapsed_seconds_global
            logger.info(
                f"Batch of {batch_size} blocks took "
                f"{updater.elapsed_seconds_last_batch:.3f} s that's "
                f"{bps:.3f} blocks per second. Approx. {((to_go / bps) / 60):.3f} "
                "minutes remaining."
            )

            if shutdown_initialized():
                logger.info(f"Got shutdown signal stoping at block {b[-1]}")
                return b[-1]

    return end_block


def update(
    env: str,
    currency: str,
    start_block: Optional[int],
    write_new: bool,
    write_dirty: bool,
    write_batch_size: int,
    updater_version: int,
    pedantic: bool,
):
    try:
        with DbFactory().from_config(env, currency) as db:
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

                start_block, end_block = find_import_range(db, start_block)

                if end_block > start_block:
                    logger.info(
                        "Start updating transformed "
                        f"Keyspace {start_block}-{end_block}."
                    )
                    update_transformed(
                        start_block,
                        end_block,
                        UpdaterFactory().get_updater(
                            currency,
                            db,
                            updater_version,
                            write_new,
                            write_dirty,
                            pedantic,
                            write_batch_size,
                        ),
                        batch_size=write_batch_size,
                    )
                elif end_block == start_block:
                    logger.info("Nothing to do. Data is up to date.")
                else:
                    raise Exception(
                        "Transformed space is ahead of raw keyspace. "
                        "This should not happen. Call 911."
                    )

    except LockFileTimeout:
        logger.error(
            f"Lockfile {lockfile_name} could not be acquired. "
            "Is another ingest running? If not delete the lockfile."
        )
        sys.exit(911)
