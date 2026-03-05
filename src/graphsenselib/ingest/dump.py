import sys
from contextlib import ExitStack
from typing import List, Optional

from graphsenselib.db import AnalyticsDb
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

SUPPORTED = ["trx", "eth", "btc", "ltc", "bch", "zec"]

_DEFAULT_VERBOSITY = {"btc": 3, "bch": 3, "ltc": 2, "zec": 2}


# filesizes should be between 100 and 1000 MB and partitions > 1000MB
# therefore we try to write files that are between 100 and 1000 MB and
# partitions that are between 1000 and 10000 MB
# TODO check this is for the entire deltalake not per table. review
FILESIZES = {
    "zec": 10000,
    "trx": 10000,
    "ltc": 1000,
    "eth": 1000,
    "btc": 1000,
    "bch": 1000,
}

PARTITIONSIZES = {
    "zec": 100000,
    "trx": 100000,
    "ltc": 10000,
    "eth": 10000,
    "btc": 10000,
    "bch": 10000,
}


def export_delta(
    currency: str,
    sources: List[str],
    directory: Optional[str],
    start_block: Optional[int],
    end_block: Optional[int],
    provider_timeout: int,
    s3_credentials: Optional[str] = None,
    write_mode: str = "overwrite",
    ignore_overwrite_safechecks: bool = False,
    db: Optional[AnalyticsDb] = None,
    lock_disabled: bool = False,
    previous_day: bool = False,
    info: bool = False,
    file_batch_size: Optional[int] = None,
):
    if currency not in SUPPORTED:
        raise ValueError(f"{currency} not supported by ingest module")

    file_batch_size = (
        file_batch_size if file_batch_size is not None else FILESIZES[currency]
    )
    partition_batch_size = PARTITIONSIZES[currency]

    if directory is not None:
        if (write_mode == "overwrite") and not ignore_overwrite_safechecks:
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

    if currency == "trx":
        source = SourceTRX(
            provider_uri=provider_uri,
            grpc_provider_uri=grpc_provider_uri,
            provider_timeout=provider_timeout,
        )
        transformer = TransformerTRX(partition_batch_size, "trx")

    elif currency == "eth":
        source = SourceETH(provider_uri=provider_uri, provider_timeout=provider_timeout)
        transformer = TransformerETH(partition_batch_size, "eth")

    elif currency in ["btc", "ltc", "bch", "zec"]:
        config = get_config()
        use_cassandra_resolver = config.resolve_inputs_via_cassandra
        verbosity = 2 if use_cassandra_resolver else _DEFAULT_VERBOSITY[currency]
        # When Cassandra resolves inputs, skip RPC resolution in the exporter.
        # When verbosity=3 (BTC/BCH), prevout data is inline — no RPC needed.
        # When verbosity=2 (LTC/ZEC) without Cassandra, resolve via getrawtransaction.
        resolve_inputs = not use_cassandra_resolver

        source = SourceUTXO(
            provider_uri=provider_uri,
            network=currency,
            provider_timeout=provider_timeout,
            verbosity=verbosity,
            resolve_inputs=resolve_inputs,
        )
        transformer = TransformerUTXO(
            partition_batch_size,
            currency,
            db=db,
            resolve_inputs_via_cassandra=use_cassandra_resolver,
            fill_unresolved_inputs=config.fill_unresolved_inputs,
        )
    else:
        raise ValueError(f"{currency} not supported by ingest module")

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

        # Auto-detect start_block
        if write_mode == "append" and delta_sink is not None:
            highest_block = delta_sink.highest_block()
            highest_block_node = source.get_last_synced_block_bo(backoff)

            if highest_block is not None:
                if highest_block == highest_block_node:
                    logger.info(
                        f"Data already present up to highest block {highest_block:,}, "
                        f"no need to append."
                    )
                    sys.exit(12)

                if start_block is None:
                    start_block = highest_block + 1
                else:
                    assert start_block > highest_block, (
                        f"Start block ({start_block:,}) must be higher than the highest "
                        f"block already written ({highest_block:,})"
                    )
            else:
                assert start_block is not None, (
                    "Start block must be provided "
                    "for append mode if no data is present "
                    "yet."
                )
        elif delta_sink is None and start_block is None and db is not None:
            # No delta sink — auto-detect from Cassandra
            highest_block = db.raw.get_highest_block()
            start_block = (highest_block + 1) if highest_block is not None else 0

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
            logger.info(
                f"Block range: {start_block:,} - {end_block:,} "
                f"({end_block - start_block + 1:,} blocks)"
            )
            return

        logger.info(f"Writing data from {start_block} to {end_block}")
        logger.info(
            f"Partition batch size: {partition_batch_size}, "
            f"file batch size: {file_batch_size}"
        )

        # Write configuration BEFORE runner.run() — critical because UTXO
        # get_latest_tx_id_before_block calls get_block_bucket_size() which
        # reads from the configuration table.
        if db is not None:
            logger.info("Writing Cassandra configuration table...")
            if currency in ["btc", "ltc", "bch", "zec"]:
                ingest_configuration_cassandra_utxo(
                    db,
                    UTXO_BLOCK_BUCKET_SIZE,
                    UTXO_TX_HASH_PREFIX_LENGTH,
                    UTXO_TX_BUCKET_SIZE,
                )
            else:
                ingest_configuration_cassandra(
                    db, BLOCK_BUCKET_SIZE, TX_HASH_PREFIX_LEN
                )

        actual_last_block = runner.run(start_block, end_block)

        # Write summary statistics for UTXO chains after run
        if (
            actual_last_block is not None
            and db is not None
            and currency in ["btc", "ltc", "bch", "zec"]
        ):
            logger.info("Writing Cassandra summary statistics...")
            ingest_summary_statistics_cassandra_utxo(
                db,
                timestamp=transformer._last_block_ts,
                total_blocks=actual_last_block + 1,
                total_txs=transformer._next_tx_id,
            )
