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
    "zec": 50000,
    "trx": 50000,
    "ltc": 2000,
    "eth": 2000,
    "btc": 400,
    "bch": 5000,
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
):
    logger.setLevel(logging.INFO)

    if currency not in SUPPORTED:
        raise ValueError(f"{currency} not supported by ingest module")

    file_batch_size = FILESIZES[currency]
    partition_batch_size = 10 * file_batch_size

    is_start_of_partition = start_block % partition_batch_size == 0
    if write_mode == "overwrite":
        assert is_start_of_partition, (
            "Start block must be a multiple of " "partition_batch_size"
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

    runner.addSink(
        DeltaDumpSinkFactory.create_writer(
            currency, s3_credentials, write_mode, directory
        )
    )
    runner.run(start_block, end_block)
