from datetime import datetime

from graphsenselib.ingest.common import Sink, Source, Transformer

from ..config import GRAPHSENSE_DEFAULT_DATETIME_FORMAT
from ..utils import parse_timestamp
from ..utils.signals import graceful_ctlc_shutdown
from .account import logger
from .source import split_blockrange


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
        data = source.read_blockrange(start_block, end_block)

        for transformer in self.transformers:
            data = transformer.transform(data)

        for sink in self.sinks:
            sink.write(data)

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
        start_block, end_block = self.source.validate_blockrange(start_block, end_block)

        partitions = split_blockrange(
            (start_block, end_block), self.partition_batch_size
        )

        for partition in partitions:
            file_chunks = split_blockrange(partition, self.file_batch_size)
            for file_chunk in file_chunks:
                with graceful_ctlc_shutdown() as check_shutdown_initialized:
                    start_chunk_time = datetime.now()
                    data = self.ingest_range(file_chunk[0], file_chunk[1])

                    blocks = data.table_contents["block"]
                    last_block_ts = sorted(blocks, key=lambda x: x["block_id"])[-1][
                        "timestamp"
                    ]
                    last_block_date = parse_timestamp(last_block_ts)

                    speed = (file_chunk[1] - file_chunk[0] + 1) / (
                        datetime.now() - start_chunk_time
                    ).total_seconds()
                    logger.info(
                        f"Written blocks: {file_chunk[0]:,} - {file_chunk[1]:,} "
                        f"""[{last_block_date.strftime(
                            GRAPHSENSE_DEFAULT_DATETIME_FORMAT
                        )}] """
                        f"({speed:.1f} blks/s)"
                    )

                    if check_shutdown_initialized():
                        break

            logger.info(
                f"Processed block range "
                f"{start_block:,} - {end_block:,} "
                f" ({last_block_date.strftime(GRAPHSENSE_DEFAULT_DATETIME_FORMAT)})"
            )
        self.ingest_blockindep()

        logger.info("Ingested blockindependent data. Finished")
