"""In-process timed ingest runner for performance measurement."""

import time

from tests.deltalake.config import DeltaTestConfig
from tests.deltalake.timing import ChunkTiming, IngestTimingResult


def run_timed_ingest(
    config: DeltaTestConfig,
    delta_directory: str,
    start_block: int,
    end_block: int,
    write_mode: str,
    s3_credentials: dict,
    table_filter: list[str] | None = None,
    provider_timeout: int = 300,
) -> IngestTimingResult:
    """Run ingest in-process with detailed timing instrumentation.

    Mirrors dump.py::export_delta() wiring but adds per-phase and per-table timing.

    Args:
        config: Test configuration with currency, node_url, etc.
        delta_directory: S3 or local path for Delta Lake output.
        start_block: First block to ingest.
        end_block: Last block to ingest.
        write_mode: "overwrite" or "append".
        s3_credentials: AWS/S3 credentials dict.
        table_filter: If set, only write these tables (others are skipped at sink).
        provider_timeout: Timeout for blockchain node requests.

    Returns:
        IngestTimingResult with wall-clock, per-chunk, and per-table timing.
    """
    from graphsenselib.ingest.delta.sink import DeltaDumpSinkFactory
    from graphsenselib.ingest.source import (
        SourceETH,
        SourceTRX,
        SourceUTXO,
        split_blockrange,
    )
    from graphsenselib.ingest.transform import (
        TransformerETH,
        TransformerTRX,
        TransformerUTXO,
    )
    from graphsenselib.utils import first_or_default

    FILESIZES = {
        "zec": 10000, "trx": 10000, "ltc": 1000,
        "eth": 1000, "btc": 1000, "bch": 1000,
    }
    PARTITIONSIZES = {
        "zec": 100000, "trx": 100000, "ltc": 10000,
        "eth": 10000, "btc": 10000, "bch": 10000,
    }

    currency = config.currency
    file_batch_size = FILESIZES[currency]
    partition_batch_size = PARTITIONSIZES[currency]

    sources = [config.node_url] + config.secondary_node_references
    provider_uri = first_or_default(sources, lambda x: x.startswith("http"))
    grpc_provider_uri = first_or_default(sources, lambda x: x.startswith("grpc"))

    # Create source
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
        raise ValueError(f"{currency} not supported")

    # Create sink
    delta_sink = DeltaDumpSinkFactory.create_writer(
        currency, s3_credentials, write_mode, delta_directory
    )

    # Apply table filter
    if table_filter:
        delta_sink.writers = {
            k: v for k, v in delta_sink.writers.items() if k in table_filter
        }

    # Run timed ingest
    chunk_timings = []

    overall_start = time.perf_counter()

    partitions = split_blockrange((start_block, end_block), partition_batch_size)
    for partition in partitions:
        file_chunks = split_blockrange(partition, file_batch_size)
        for file_chunk in file_chunks:
            chunk_start = file_chunk[0]
            chunk_end = file_chunk[1]
            num_blocks = chunk_end - chunk_start + 1

            # Source phase
            t0 = time.perf_counter()
            data = source.read_blockrange(chunk_start, chunk_end)
            source_s = time.perf_counter() - t0

            # Transform phase
            t0 = time.perf_counter()
            data = transformer.transform(data)
            transform_s = time.perf_counter() - t0

            # Sink phase — with per-table timing
            sink_start = time.perf_counter()
            delta_sink.write(data)
            sink_s = time.perf_counter() - sink_start

            chunk_timings.append(
                ChunkTiming(
                    start_block=chunk_start,
                    end_block=chunk_end,
                    num_blocks=num_blocks,
                    source_s=source_s,
                    transform_s=transform_s,
                    sink_s=sink_s,
                )
            )

    # Ingest block-independent data (e.g. trc10 tokens)
    blockindep_data = source.read_blockindep()
    blockindep_data = transformer.transform_blockindep(blockindep_data)
    delta_sink.write(blockindep_data)

    overall_s = time.perf_counter() - overall_start

    return IngestTimingResult(
        wall_clock_s=overall_s,
        currency=currency,
        start_block=start_block,
        end_block=end_block,
        chunk_timings=chunk_timings,
        table_write_timings=[],
    )
