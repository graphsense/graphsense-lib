from datetime import datetime

from graphsenselib.ingest.common import Sink, Source, Transformer

from ..config import GRAPHSENSE_DEFAULT_DATETIME_FORMAT
from ..utils import parse_timestamp
from ..utils.signals import graceful_ctlc_shutdown
from .account import logger
from .source import split_blockrange

AVG_BLOCKTIME = {
    "eth": 12,
    "trx": 3,
    "ltc": int(2.5 * 60),
    "zec": int(1.25 * 60),
    "btc": 10 * 60,
    "bch": 10 * 60,
}


class IngestRunner:
    def __init__(self, partition_batch_size: int, file_batch_size: int):
        self.source = None
        self.transformers = []
        self.sinks = []
        self.partition_batch_size = partition_batch_size
        self.file_batch_size = file_batch_size

    def addSource(self, source: Source):
        self.source = source

    def addTransformer(self, transformer: Transformer):
        self.transformers.append(transformer)

    def addSink(self, sink: Sink):
        self.sinks.append(sink)

    def ingest_range(self, start_block, end_block):
        source = self.source
        logger.debug("Reading blockrange...")
        data = source.read_blockrange(start_block, end_block)

        logger.debug("Applying transformations...")
        for transformer in self.transformers:
            data = transformer.transform(data)
            logger.debug("Applied a transformation")

        logger.debug("Writing to sinks...")
        for sink in self.sinks:
            sink.write(data)
            logger.debug("Wrote to a sink")

        return data

    def ingest_blockindep(self):
        source = self.source
        data = source.read_blockindep()

        for transformer in self.transformers:
            data = transformer.transform_blockindep(data)

        for sink in self.sinks:
            sink.write(data)

        return data

    def run(self, start_block, end_block):
        partitions = split_blockrange(
            (start_block, end_block), self.partition_batch_size
        )
        avg_blocktime = AVG_BLOCKTIME[self.transformers[0].network]

        with graceful_ctlc_shutdown() as check_shutdown_initialized:
            for partition in partitions:
                file_chunks = split_blockrange(partition, self.file_batch_size)
                for file_chunk in file_chunks:
                    start_chunk_time = datetime.now()
                    data = self.ingest_range(file_chunk[0], file_chunk[1])

                    blocks = data.table_contents["block"]
                    last_block = sorted(blocks, key=lambda x: x["block_id"])[-1]
                    last_block_id = last_block["block_id"]
                    last_block_ts = last_block["timestamp"]
                    last_block_date = parse_timestamp(last_block_ts)

                    speed = (file_chunk[1] - file_chunk[0] + 1) / (
                        datetime.now() - start_chunk_time
                    ).total_seconds()

                    network_s_per_ingest_s = speed * avg_blocktime

                    logger.info(
                        f"Written blocks: {file_chunk[0]:,} - {file_chunk[1]:,} "
                        f"""[{
                            last_block_date.strftime(GRAPHSENSE_DEFAULT_DATETIME_FORMAT)
                        }] """
                        f"({speed:.1f} blks/s) ({network_s_per_ingest_s:.1f} "
                        f"network_s/s) "
                    )

                if check_shutdown_initialized():
                    break

                partition_start = (
                    partition[0] // self.partition_batch_size
                ) * self.partition_batch_size
                logger.info(
                    f"Processed partition {partition_start:,} - {partition[1]:,}"
                )

            logger.info(
                f"Processed block range "
                f"{start_block:,} - {last_block_id:,} "
                f" ({last_block_date.strftime(GRAPHSENSE_DEFAULT_DATETIME_FORMAT)})"
            )
        self.ingest_blockindep()

        logger.info("Ingested block independent data. Finished")
