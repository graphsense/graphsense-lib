import sys
from contextlib import ExitStack
from typing import Callable, Dict, List, Optional

from graphsenselib.db import AnalyticsDb
from graphsenselib.db.state import mark_ingest_complete
from graphsenselib.ingest.account import (
    BLOCK_BUCKET_SIZE,
    TX_HASH_PREFIX_LEN,
    ingest_configuration_cassandra,
    logger,
)
from graphsenselib.ingest.utxo import (
    BLOCK_BUCKET_SIZE as UTXO_BLOCK_BUCKET_SIZE,
    TX_HASH_PREFIX_LENGTH as UTXO_TX_HASH_PREFIX_LENGTH,
    TX_BUCKET_SIZE as UTXO_TX_BUCKET_SIZE,
    ingest_configuration_cassandra as ingest_configuration_cassandra_utxo,
    ingest_summary_statistics_cassandra as ingest_summary_statistics_cassandra_utxo,
)
from graphsenselib.ingest.cassandra.sink import CassandraSink
from graphsenselib.ingest.delta.sink import DeltaDumpSinkFactory
from graphsenselib.ingest.ingestrunner import IngestRunner
from graphsenselib.ingest.source import SourceETH, SourceTRX, SourceUTXO
from graphsenselib.ingest.transform import (
    TransformerETH,
    TransformerTRX,
    TransformerUTXO,
)
from graphsenselib.utils import first_or_default
from graphsenselib.utils.locking import create_lock

from ..config import get_reorg_backoff_blocks
from ..config.config import get_config

_DEFAULT_VERBOSITY = {"btc": 3, "bch": 3, "ltc": 2, "zec": 2}


def _create_trx(
    provider_uri,
    grpc_provider_uri,
    provider_timeout,
    partition_batch_size,
    source_max_workers=None,
    **kw,
):
    source = SourceTRX(
        provider_uri=provider_uri,
        grpc_provider_uri=grpc_provider_uri,
        provider_timeout=provider_timeout,
        max_workers=source_max_workers,
    )
    transformer = TransformerTRX(partition_batch_size, "trx")
    return source, transformer


def _create_eth(
    provider_uri, provider_timeout, partition_batch_size, source_max_workers=None, **kw
):
    source = SourceETH(
        provider_uri=provider_uri,
        provider_timeout=provider_timeout,
        max_workers=source_max_workers,
    )
    transformer = TransformerETH(partition_batch_size, "eth")
    return source, transformer


def _create_utxo(
    provider_uri,
    provider_timeout,
    partition_batch_size,
    currency,
    db,
    source_max_workers=None,
    **kw,
):
    config = get_config()
    use_cassandra_resolver = config.resolve_inputs_via_cassandra
    verbosity = 2 if use_cassandra_resolver else _DEFAULT_VERBOSITY[currency]
    resolve_inputs = not use_cassandra_resolver

    source = SourceUTXO(
        provider_uri=provider_uri,
        network=currency,
        provider_timeout=provider_timeout,
        verbosity=verbosity,
        resolve_inputs=resolve_inputs,
        max_workers=source_max_workers,
    )
    transformer = TransformerUTXO(
        partition_batch_size,
        currency,
        db=db,
        resolve_inputs_via_cassandra=use_cassandra_resolver,
        fill_unresolved_inputs=config.fill_unresolved_inputs,
    )
    return source, transformer


PIPELINE_REGISTRY: Dict[str, Callable] = {
    "trx": _create_trx,
    "eth": _create_eth,
    "btc": _create_utxo,
    "ltc": _create_utxo,
    "bch": _create_utxo,
    "zec": _create_utxo,
}


# These large filesizes are actually not necessary
# anymore since compaction takes care of that.
# Could potentially be lowered if performance doesnt take a hit.
FILESIZES = {
    "zec": 1000,
    "trx": 1000,
    "ltc": 100,
    "eth": 100,
    "btc": 100,
    "bch": 100,
}

PARTITIONSIZES = {
    "zec": 100000,
    "trx": 100000,
    "ltc": 10000,
    "eth": 10000,
    "btc": 10000,
    "bch": 10000,
}


def _abort_on_sink_divergence(currency, sink_heights):
    """Refuse to ingest if registered sinks disagree on their highest block.

    Empty sinks (None) and sinks at the same height are aligned. Any other
    combination is divergence: writing forward from one sink's value would
    leave the others with a gap or duplicate writes.
    """
    distinct = {h for _, h in sink_heights if h is not None}
    has_empty = any(h is None for _, h in sink_heights)
    aligned = len(distinct) <= 1 and not (distinct and has_empty)
    if aligned:
        return

    target = max(distinct)
    rows = "\n".join(
        f"  - {name}: " + (f"block {h:,}" if h is not None else "(empty)")
        for name, h in sink_heights
    )
    laggards = [(name, h) for name, h in sink_heights if h != target]
    recovery = "\n".join(
        f"  graphsense-cli ingest from-node --currency {currency} "
        f"--sinks {name} --start-block {h + 1 if h is not None else 0} "
        f"--end-block {target}"
        for name, h in laggards
    )
    logger.error(
        "Sink divergence detected — refusing to ingest:\n"
        f"{rows}\n\n"
        f"Highest is block {target:,}. To catch up the lagging sinks, "
        f"run each one alone:\n"
        f"{recovery}\n"
        "Then re-run this command."
    )
    sys.exit(13)


def export_delta(
    currency: str,
    sources: List[str],
    directory: Optional[str],
    start_block: Optional[int],
    end_block: Optional[int],
    provider_timeout: int,
    s3_credentials: Optional[dict] = None,
    write_mode: str = "overwrite",
    ignore_overwrite_safechecks: bool = False,
    db: Optional[AnalyticsDb] = None,
    lock_disabled: bool = False,
    previous_day: bool = False,
    info: bool = False,
    file_batch_size: Optional[int] = None,
    source_max_workers: Optional[int] = None,
):
    if currency not in PIPELINE_REGISTRY:
        raise ValueError(f"{currency} not supported by ingest module")

    file_batch_size = (
        file_batch_size if file_batch_size is not None else FILESIZES[currency]
    )
    partition_batch_size = PARTITIONSIZES[currency]

    if directory is not None:
        if (write_mode == "overwrite") and not ignore_overwrite_safechecks:
            assert start_block is not None
            is_start_of_partition = start_block % partition_batch_size == 0
            left_partition_start = start_block - (start_block % partition_batch_size)
            assert is_start_of_partition, (
                f"Start block ({start_block:,}) must be a multiple of partition_batch_size "
                f"({partition_batch_size:,}) for overwrite mode. "
                f"Try {left_partition_start:,} or use flag ignore-overwrite-safechecks "
                f" instead."
            )

        logger.info(f"Writing data as parquet to {directory}")

    if partition_batch_size % file_batch_size != 0:
        logger.error("Error: partition_batch_size is not a multiple of file_batch_size")
        sys.exit(1)

    provider_uri = first_or_default(sources, lambda x: x.startswith("http"))
    grpc_provider_uri = first_or_default(sources, lambda x: x.startswith("grpc"))

    runner = IngestRunner(partition_batch_size, file_batch_size)

    factory = PIPELINE_REGISTRY[currency]
    source, transformer = factory(
        provider_uri=provider_uri,
        grpc_provider_uri=grpc_provider_uri,
        provider_timeout=provider_timeout,
        partition_batch_size=partition_batch_size,
        currency=currency,
        db=db,
        source_max_workers=source_max_workers,
    )

    runner.addSource(source)
    runner.addTransformer(transformer)

    # Delta sink (optional — only when a directory is configured)
    delta_sink = None
    if directory is not None:
        delta_sink = DeltaDumpSinkFactory.create_writer(
            currency, s3_credentials, write_mode, directory
        )
        runner.addSink(delta_sink)

    # Cassandra sink (optional — only when db is provided)
    if db is not None:
        cassandra_sink = CassandraSink(db)
        runner.addSink(cassandra_sink)

    # Acquire locks from all sinks for the entire duration of the ingest
    with ExitStack() as lock_stack:
        for sink in runner.sinks:
            name = sink.lock_name()
            if name is not None:
                lock_stack.enter_context(create_lock(name, disabled=lock_disabled))

        backoff = get_reorg_backoff_blocks(currency)

        # Auto-detect start_block. In append mode every registered sink that
        # tracks resume state must agree on its highest block — otherwise
        # writing forward from any single sink's value silently leaves the
        # others with a gap (cassandra) or duplicate writes (delta).
        if write_mode == "append":
            sink_heights = [(sink.name, sink.highest_block()) for sink in runner.sinks]
            _abort_on_sink_divergence(currency, sink_heights)

            # Surface resume state so the user always sees where ingestion will
            # pick up — parity with the legacy pipeline's print_block_info.
            for name, h in sink_heights:
                shown = f"{h:,}" if h is not None else "None"
                logger.info(f"Sink '{name}' last ingested block: {shown}")

            agreed_height = next((h for _, h in sink_heights if h is not None), None)
            if agreed_height is not None:
                highest_block_node = source.get_last_synced_block_bo(backoff)
                logger.info(
                    f"Last synced block at node (with reorg backoff "
                    f"{backoff}): {highest_block_node:,}"
                )
                if agreed_height == highest_block_node:
                    logger.info(
                        f"Data already present up to highest block "
                        f"{agreed_height:,}, no need to append."
                    )
                    sys.exit(12)

                if start_block is None:
                    start_block = agreed_height + 1
                else:
                    assert start_block > agreed_height, (
                        f"Start block ({start_block:,}) must be higher than the "
                        f"highest block already written ({agreed_height:,})"
                    )
            else:
                assert start_block is not None, (
                    "Start block must be provided "
                    "for append mode if no data is present "
                    "yet."
                )

        start_block, end_block = source.validate_blockrange(
            start_block, end_block, backoff
        )

        if previous_day:
            last_block_yesterday = source.get_last_block_yesterday()
            if end_block > last_block_yesterday:
                logger.info(
                    f"--previous-day: capping end_block from {end_block:,} "
                    f"to {last_block_yesterday:,}"
                )
                end_block = last_block_yesterday
            if end_block < start_block:
                logger.info(
                    f"--previous-day: nothing to ingest "
                    f"(start_block {start_block:,} > last_block_yesterday {end_block:,})"
                )
                return

        if info:
            sink_names = [sink.name for sink in runner.sinks]
            logger.info(
                f"Would ingest block range {start_block:,} - {end_block:,} "
                f"({end_block - start_block + 1:,} blocks) into {sink_names}"
            )
            return

        logger.info(f"Writing data from {start_block} to {end_block}")
        logger.info(
            f"Partition batch size: {partition_batch_size}, "
            f"file batch size: {file_batch_size}, "
            f"source_max_workers: {source_max_workers or 10}"
        )

        # Pre-write UTXO configuration so TransformerUTXO can read bucket
        # sizes from the configuration table during the first-ever ingest.
        # The configuration contains only constants (bucket sizes), so
        # writing before and after is idempotent and safe.
        if db is not None and currency in ["btc", "ltc", "bch", "zec"]:
            ingest_configuration_cassandra_utxo(
                db,
                UTXO_BLOCK_BUCKET_SIZE,
                UTXO_TX_HASH_PREFIX_LENGTH,
                UTXO_TX_BUCKET_SIZE,
            )

        actual_last_block = runner.run(start_block, end_block)

        # Close source to release any held resources (e.g., gRPC channels)
        if hasattr(source, "close"):
            source.close()

        # Write/update Cassandra configuration and summary statistics AFTER
        # data so that stats reflect the actual ingested range.
        if db is not None and actual_last_block is not None:
            logger.info("Writing Cassandra configuration table...")
            if currency in ["btc", "ltc", "bch", "zec"]:
                ingest_configuration_cassandra_utxo(
                    db,
                    UTXO_BLOCK_BUCKET_SIZE,
                    UTXO_TX_HASH_PREFIX_LENGTH,
                    UTXO_TX_BUCKET_SIZE,
                )
                logger.info("Writing Cassandra summary statistics...")
                ingest_summary_statistics_cassandra_utxo(
                    db,
                    timestamp=transformer._last_block_ts,
                    total_blocks=actual_last_block + 1,
                    total_txs=transformer._next_tx_id,
                )
            else:
                ingest_configuration_cassandra(
                    db, BLOCK_BUCKET_SIZE, TX_HASH_PREFIX_LEN
                )
            # MUST stay last — see graphsenselib.db.state.mark_ingest_complete.
            mark_ingest_complete(db, "raw")
