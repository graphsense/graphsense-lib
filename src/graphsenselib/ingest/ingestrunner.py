from concurrent.futures import ThreadPoolExecutor
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
        self._sink_last_block: dict[str, int] = {}

    def addSource(self, source: Source):
        self.source = source

    def addTransformer(self, transformer: Transformer):
        self.transformers.append(transformer)

    def addSink(self, sink: Sink):
        self.sinks.append(sink)

    def ingest_range(self, start_block, end_block):
        source = self.source
        assert source is not None
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
        assert source is not None
        data = source.read_blockindep()

        for transformer in self.transformers:
            data = transformer.transform_blockindep(data)

        for sink in self.sinks:
            sink.write(data)

        return data

    def _transform_and_write(self, data, file_chunk):
        """Apply transformers and write to sinks.

        Tracks which sinks succeeded per chunk. If a sink fails, reports
        which sinks already committed so operators can diagnose inconsistencies.
        """
        for transformer in self.transformers:
            data = transformer.transform(data)

        committed_sinks = []
        for sink in self.sinks:
            sink_name = type(sink).__name__
            try:
                sink.write(data)
            except Exception:
                if committed_sinks:
                    logger.error(
                        f"Sink {sink_name} failed for blocks "
                        f"{file_chunk[0]:,}–{file_chunk[1]:,}. "
                        f"Already committed to: {committed_sinks}. "
                        f"Per-sink state: {self._sink_last_block}"
                    )
                raise
            committed_sinks.append(sink_name)
            self._sink_last_block[sink_name] = file_chunk[1]

        return data

    def run(self, start_block, end_block):
        assert self.source is not None
        source = self.source
        partitions = split_blockrange(
            (start_block, end_block), self.partition_batch_size
        )
        avg_blocktime = AVG_BLOCKTIME[self.transformers[0].network]

        last_block_id = start_block
        last_block_date = None

        with graceful_ctlc_shutdown() as check_shutdown_initialized:
            for partition in partitions:
                file_chunks = list(split_blockrange(partition, self.file_batch_size))

                # Prefetch: overlap source read of chunk N+1 with
                # transform+write of chunk N. Source reads are thread-safe
                # (HTTP clients use thread-local sessions, gRPC uses
                # multiplexed channels).
                with ThreadPoolExecutor(max_workers=1) as prefetch_executor:
                    prefetch_future = None

                    for i, file_chunk in enumerate(file_chunks):
                        start_chunk_time = datetime.now()

                        # Get data: from prefetch or read synchronously
                        if prefetch_future is not None:
                            data = prefetch_future.result()
                        else:
                            data = source.read_blockrange(file_chunk[0], file_chunk[1])

                        # Start prefetching next chunk while we
                        # transform + write the current one
                        if i + 1 < len(file_chunks):
                            next_chunk = file_chunks[i + 1]
                            prefetch_future = prefetch_executor.submit(
                                source.read_blockrange,
                                next_chunk[0],
                                next_chunk[1],
                            )
                        else:
                            prefetch_future = None

                        # Transform + write current chunk
                        data = self._transform_and_write(data, file_chunk)

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
                                last_block_date.strftime(
                                    GRAPHSENSE_DEFAULT_DATETIME_FORMAT
                                )
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

            if last_block_date is not None:
                logger.info(
                    f"Processed block range "
                    f"{start_block:,} - {last_block_id:,} "
                    f" ({last_block_date.strftime(GRAPHSENSE_DEFAULT_DATETIME_FORMAT)})"
                )
            else:
                logger.info("No blocks were processed.")

        if last_block_date is not None:
            self.ingest_blockindep()
            if self._sink_last_block:
                logger.info(f"Sink checkpoint: {self._sink_last_block}")
            logger.info("Ingested block independent data. Finished")
        else:
            logger.info("Skipping block-independent data (no blocks were processed).")

        return last_block_id if last_block_date is not None else None
