import sys
from typing import List, Optional

from filelock import FileLock
from filelock import Timeout as LockFileTimeout

from graphsenselib.ingest.account import logger
from graphsenselib.ingest.delta.sink import DeltaDumpSinkFactory
from graphsenselib.ingest.ingestrunner import IngestRunner
from graphsenselib.ingest.source import SourceETH, SourceTRX, SourceUTXO
from graphsenselib.ingest.transform import (
    TransformerETH,
    TransformerTRX,
    TransformerUTXO,
)
from graphsenselib.utils import first_or_default

from ..config import get_reorg_backoff_blocks

SUPPORTED = ["trx", "eth", "btc", "ltc", "bch", "zec"]


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
    directory: str,
    start_block: Optional[int],
    end_block: Optional[int],
    provider_timeout: int,
    s3_credentials: Optional[str] = None,
    write_mode: str = "overwrite",
    ignore_overwrite_safechecks: bool = False,
):
    if currency not in SUPPORTED:
        raise ValueError(f"{currency} not supported by ingest module")

    file_batch_size = FILESIZES[currency]
    partition_batch_size = PARTITIONSIZES[currency]

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
        source = SourceUTXO(
            provider_uri=provider_uri,
            network=currency,
            provider_timeout=provider_timeout,
        )
        transformer = TransformerUTXO(partition_batch_size, currency)
    else:
        raise ValueError(f"{currency} not supported by ingest module")

    delta_sink = DeltaDumpSinkFactory.create_writer(
        currency, s3_credentials, write_mode, directory
    )

    runner.addSource(source)
    runner.addTransformer(transformer)
    runner.addSink(delta_sink)

    backoff = get_reorg_backoff_blocks(currency)

    if write_mode == "append":
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

    start_block, end_block = source.validate_blockrange(start_block, end_block, backoff)

    logger.info(f"Writing data from {start_block} to {end_block} in mode {write_mode}")
    logger.info(
        f"Partition batch size: {partition_batch_size}, "
        f"file batch size: {file_batch_size}"
    )

    lockfile_name = f"/tmp/rawdatadump_{currency}.lock"
    logger.info(f"Try acquiring lockfile {lockfile_name}")
    try:
        with FileLock(lockfile_name, timeout=1):
            logger.info(f"Lockfile {lockfile_name} acquired.")
            runner.run(start_block, end_block)
    except LockFileTimeout:
        logger.error(
            f"Lockfile {lockfile_name} could not be acquired. "
            "Is another ingest running? If not delete the lockfile."
        )
        sys.exit(911)
