import logging
import sys
from typing import List, Optional

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
    logger.setLevel(logging.INFO)

    if currency not in SUPPORTED:
        raise ValueError(f"{currency} not supported by ingest module")

    file_batch_size = FILESIZES[currency]
    partition_batch_size = 10 * file_batch_size

    if (write_mode == "overwrite") and not ignore_overwrite_safechecks:
        is_start_of_partition = start_block % partition_batch_size == 0
        left_partition_start = start_block - (start_block % partition_batch_size)
        assert is_start_of_partition, (
            f"Start block ({start_block:,}) must be a multiple of partition_batch_size "
            f"({partition_batch_size:,}) for overwrite mode. "
            f"Try {left_partition_start:,} "
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
        runner.addSource(
            source=SourceTRX(
                provider_uri=provider_uri,
                grpc_provider_uri=grpc_provider_uri,
                provider_timeout=provider_timeout,
            )
        )
        runner.addTransformer(TransformerTRX(partition_batch_size, "trx"))

    elif currency == "eth":
        runner.addSource(
            SourceETH(provider_uri=provider_uri, provider_timeout=provider_timeout)
        )
        runner.addTransformer(TransformerETH(partition_batch_size, "eth"))

    elif currency in ["btc", "ltc", "bch", "zec"]:
        runner.addSource(SourceUTXO(provider_uri=provider_uri, network=currency))
        runner.addTransformer(TransformerUTXO(partition_batch_size, currency))

    delta_sink = DeltaDumpSinkFactory.create_writer(
        currency, s3_credentials, write_mode, directory
    )

    if write_mode == "append":
        highest_block = delta_sink.highest_block()
        if highest_block is not None:
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

    logger.info(f"Writing data from {start_block} to {end_block} in mode {write_mode}")
    runner.addSink(delta_sink)
    runner.run(start_block, end_block)
