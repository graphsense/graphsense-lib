import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Generator, Tuple

from graphsenselib.ingest.account import (
    WEB3_QUERY_BATCH_SIZE,
    WEB3_QUERY_WORKERS,
    EthStreamerAdapter,
    TronStreamerAdapter,
    get_connection_from_url,
    get_last_synced_block,
)
from graphsenselib.ingest.common import BlockRangeContent, Source
from graphsenselib.ingest.fast_traces import FastTraceExporter
from graphsenselib.ingest.fast_btc import FastBtcBlockExporter

logger = logging.getLogger(__name__)


def split_blockrange(
    blockrange: Tuple[int, int], chunk_size: int
) -> Generator[Tuple[int, int], None, None]:
    start, end = blockrange
    temp_end = start
    while start <= end:
        temp_end = min((start // chunk_size + 1) * chunk_size - 1, end)
        yield (start, temp_end)
        start = temp_end + 1


class SourceTRX(Source):
    def __init__(self, provider_uri, grpc_provider_uri, provider_timeout):
        self.provider_uri = provider_uri
        self.grpc_provider_uri = grpc_provider_uri
        self.provider_timeout = provider_timeout
        self.client = get_connection_from_url(provider_uri, provider_timeout)
        self.adapter = TronStreamerAdapter(
            self.client,
            grpc_endpoint=grpc_provider_uri,
            batch_size_blockstransactions=20,
            max_workers_blockstransactions=10,
            batch_size_receiptslogs=600,
            max_workers_receiptslogs=30,
        )

    def read_blockrange(self, start_block, end_block):
        if start_block == 0:
            start_block = 1
            logging.warning(
                "Start was set to 1 since genesis blocks "
                "don't have logs and cause issues."
            )
        logger.debug("Reading blocks and transactions...")
        blocks, txs = self.adapter.export_blocks_and_transactions(
            start_block, end_block
        )
        logger.debug("Reading receipts and logs...")
        receipts, logs = self.adapter.export_receipts_and_logs(txs)
        logger.debug("Reading traces and fees...")
        traces, fees = self.adapter.export_traces_parallel(start_block, end_block)
        logger.debug("Reading types...")
        hash_to_type = self.adapter.export_hash_to_type_mappings_parallel(
            blocks, block_id_name="number"
        )

        data = {
            "blocks": blocks,
            "txs": txs,
            "receipts": receipts,
            "logs": logs,
            "traces": traces,
            "fees": fees,
            "hash_to_type": hash_to_type,
        }
        logger.debug(f"Finished reading blockrange from {start_block} to {end_block}")
        return BlockRangeContent(
            table_contents=data, start_block=start_block, end_block=end_block
        )

    def read_blockindep(self):
        token_infos = self.adapter.get_trc10_token_infos()
        return BlockRangeContent(table_contents={"token_infos": token_infos})

    def get_last_synced_block(self):
        return get_last_synced_block(self.client)


class SourceETH(Source):
    def __init__(self, provider_uri, provider_timeout):
        self.provider_uri = provider_uri
        self.provider_timeout = provider_timeout
        self.client = get_connection_from_url(provider_uri, provider_timeout)
        self.adapter = EthStreamerAdapter(
            self.client,
            batch_size=WEB3_QUERY_BATCH_SIZE,
            max_workers=WEB3_QUERY_WORKERS,
        )
        self.fast_trace_exporter = FastTraceExporter(
            client=self.client,
            trace_batch_size=10,
            max_workers=20,
        )

    def read_blockrange(self, start_block, end_block):
        # ethereum-etl injects special traces for genesis and DAO fork blocks.
        # Fall back to the legacy exporter when those windows are included so
        # output stays byte-compatible with historical snapshots.
        includes_special_trace_windows = (
            start_block <= 0 <= end_block or start_block <= 1_920_000 <= end_block
        )

        if includes_special_trace_windows:
            blocks, txs = self.adapter.export_blocks_and_transactions(
                start_block, end_block
            )
            receipts, logs = self.adapter.export_receipts_and_logs(txs)
            traces, _ = self.adapter.export_traces(start_block, end_block, True, True)

            data = {
                "blocks": blocks,
                "txs": txs,
                "receipts": receipts,
                "logs": logs,
                "traces": traces,
            }

            return BlockRangeContent(
                table_contents=data, start_block=start_block, end_block=end_block
            )

        # Traces only depend on block range, not on tx data, so we can
        # fetch them concurrently with blocks+receipts.
        with ThreadPoolExecutor(max_workers=1) as executor:
            trace_future = executor.submit(
                self.fast_trace_exporter.export_traces, start_block, end_block
            )

            blocks, txs = self.adapter.export_blocks_and_transactions(
                start_block, end_block
            )
            receipts, logs = self.adapter.export_receipts_and_logs(txs)

            traces, _ = trace_future.result()

        data = {
            "blocks": blocks,
            "txs": txs,
            "receipts": receipts,
            "logs": logs,
            "traces": traces,
        }

        return BlockRangeContent(
            table_contents=data, start_block=start_block, end_block=end_block
        )

    def read_blockindep(self):
        return BlockRangeContent(table_contents={})

    def get_last_synced_block(self):
        return get_last_synced_block(self.client)


class SourceUTXO(Source):
    def __init__(self, provider_uri, network, provider_timeout):
        self.fast_exporter = FastBtcBlockExporter(
            provider_uri=provider_uri,
            max_workers=10,
            timeout=provider_timeout,
        )
        # Keep legacy adapter for get_last_synced_block (uses getblockcount)
        self._provider_uri = provider_uri
        self._provider_timeout = provider_timeout

    def read_blockrange(self, start_block, end_block):
        blocks, txs = self.fast_exporter.export_blocks_and_transactions(
            start_block, end_block
        )
        data = {"blocks": blocks, "txs": txs}

        return BlockRangeContent(
            table_contents=data, start_block=start_block, end_block=end_block
        )

    def read_blockindep(self):
        return BlockRangeContent(table_contents={})

    def get_last_synced_block(self):
        return self.fast_exporter.get_current_block_number()
