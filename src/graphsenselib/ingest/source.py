import logging
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Generator, Tuple

from graphsenselib.ingest.account import (
    EthStreamerAdapter,
    TronStreamerAdapter,
    get_connection_from_url,
    get_last_block_yesterday as account_get_last_block_yesterday,
    get_last_synced_block,
)
from graphsenselib.ingest.utxo import (
    get_last_block_yesterday as utxo_get_last_block_yesterday,
)
from graphsenselib.ingest.common import BlockRangeContent, Source
from graphsenselib.ingest.fast_btc import FastBtcBlockExporter
from graphsenselib.ingest.fast_traces import FastTraceExporter
from graphsenselib.ingest.tron.grpc_exporter import TronCombinedGrpcExporter

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
            batch_size_receiptslogs=100,
            max_workers_receiptslogs=20,
        )
        self.grpc_exporter = TronCombinedGrpcExporter(
            grpc_endpoint=grpc_provider_uri,
            max_workers=30,
        )

    def get_last_block_yesterday(self) -> int:
        return account_get_last_block_yesterday(self.client)

    def read_blockrange(self, start_block, end_block):
        if start_block == 0:
            start_block = 1
            logging.warning(
                "Start was set to 1 since genesis blocks "
                "don't have logs and cause issues."
            )

        # gRPC provides everything: txs, types, traces, fees, receipts, logs.
        # HTTP only needed for lightweight block headers (logs_bloom, state_root, etc.).
        #
        # Worker 1:     ALL tx data via gRPC combined
        # Main thread:  block headers via HTTP (detailed=false, ~0.3s)
        t_total = time.monotonic()

        with ThreadPoolExecutor(max_workers=1) as executor:
            # Start gRPC combined (txs + types + traces + fees + receipts + logs)
            grpc_future = executor.submit(
                self.grpc_exporter.export, start_block, end_block
            )

            # Block headers via HTTP lightweight (detailed=false) in main thread
            t0 = time.monotonic()
            blocks = self.adapter.export_block_headers(start_block, end_block)
            t_blocks = time.monotonic() - t0

            # Collect gRPC results
            t0 = time.monotonic()
            txs, hash_to_type, traces, fees, receipts, logs = grpc_future.result()
            t_grpc = time.monotonic() - t0

        # Fallback: if energy_price couldn't be derived from gRPC data
        # (no tx in the batch paid energy, e.g. genesis blocks), fetch
        # one block's receipts via HTTP to get the actual energy price.
        # Use the block of the first receipt (not start_block, which may
        # be empty).
        t_fallback = 0.0
        if receipts and receipts[0]["effective_gas_price"] == 0:
            t0 = time.monotonic()
            sample_block = receipts[0]["block_number"]
            http_rcpts, _ = self.adapter.export_receipts_and_logs_by_block(
                sample_block, sample_block
            )
            if http_rcpts and http_rcpts[0].get("effective_gas_price", 0) > 0:
                real_price = http_rcpts[0]["effective_gas_price"]
                for r in receipts:
                    r["effective_gas_price"] = real_price
                logger.info(
                    f"[source-timing] energy_price fallback: "
                    f"derived=0, http={real_price}"
                )
            t_fallback = time.monotonic() - t0

        t_source_total = time.monotonic() - t_total
        n_blocks = end_block - start_block + 1
        logger.info(
            f"[source-timing] TRX {n_blocks} blocks ({start_block}-{end_block}): "
            f"total={t_source_total:.2f}s  "
            f"block_headers={t_blocks:.2f}s  "
            f"grpc_all={t_grpc:.2f}s ({len(txs)} txs, {len(traces)} traces, "
            f"{len(fees) if fees else 0} fees, "
            f"{len(receipts)} rcpts, {len(logs)} logs)"
            + (f"  fallback={t_fallback:.2f}s" if t_fallback > 0 else "")
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
            batch_size_blockstransactions=20,
            max_workers_blockstransactions=10,
            batch_size_receiptslogs=100,
            max_workers_receiptslogs=10,
        )
        self.fast_trace_exporter = FastTraceExporter(
            client=self.client,
            trace_batch_size=10,
            max_workers=10,
        )

    def get_last_block_yesterday(self) -> int:
        return account_get_last_block_yesterday(self.client)

    def read_blockrange(self, start_block, end_block):
        # ethereum-etl injects special traces for genesis and DAO fork blocks.
        # Fall back to the legacy exporter when those windows are included so
        # output stays byte-compatible with historical snapshots.
        includes_special_trace_windows = (
            start_block <= 0 <= end_block or start_block <= 1_920_000 <= end_block
        )

        if includes_special_trace_windows:
            t_total = time.monotonic()

            with ThreadPoolExecutor(max_workers=2) as executor:
                trace_future = executor.submit(
                    self.adapter.export_traces,
                    start_block,
                    end_block,
                    True,
                    True,
                )
                receipt_future = executor.submit(
                    self.adapter.export_receipts_and_logs_by_block,
                    start_block,
                    end_block,
                )

                t0 = time.monotonic()
                blocks, txs = self.adapter.export_blocks_and_transactions(
                    start_block, end_block
                )
                t_blocks = time.monotonic() - t0

                t0 = time.monotonic()
                receipts, logs = receipt_future.result()
                t_receipts = time.monotonic() - t0

                t0 = time.monotonic()
                traces, _ = trace_future.result()
                t_traces = time.monotonic() - t0

            t_source_total = time.monotonic() - t_total
            n_blocks = end_block - start_block + 1
            logger.info(
                f"[source-timing] {n_blocks} blocks ({start_block}-{end_block}) "
                f"[special-trace path]: "
                f"total={t_source_total:.2f}s  "
                f"blocks={t_blocks:.2f}s  "
                f"receipts={t_receipts:.2f}s ({len(receipts)} rcpts, {len(logs)} logs)  "
                f"traces={t_traces:.2f}s ({len(traces)} traces)  "
                f"txs={len(txs)}"
            )

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

        # Blocks, receipts, and traces are all independent — run concurrently.
        # BatchRpcClient uses thread-local sessions so HTTP calls are safe.
        t_total = time.monotonic()

        with ThreadPoolExecutor(max_workers=2) as executor:
            trace_future = executor.submit(
                self.fast_trace_exporter.export_traces, start_block, end_block
            )
            receipt_future = executor.submit(
                self.adapter.export_receipts_and_logs_by_block,
                start_block,
                end_block,
            )

            t0 = time.monotonic()
            blocks, txs = self.adapter.export_blocks_and_transactions(
                start_block, end_block
            )
            t_blocks = time.monotonic() - t0

            t0 = time.monotonic()
            receipts, logs = receipt_future.result()
            t_receipts = time.monotonic() - t0

            t0 = time.monotonic()
            traces, _ = trace_future.result()
            t_traces_wait = time.monotonic() - t0

        t_source_total = time.monotonic() - t_total
        n_blocks = end_block - start_block + 1
        logger.info(
            f"[source-timing] {n_blocks} blocks ({start_block}-{end_block}): "
            f"total={t_source_total:.2f}s  "
            f"blocks={t_blocks:.2f}s  "
            f"receipts={t_receipts:.2f}s ({len(receipts)} rcpts, {len(logs)} logs)  "
            f"traces_wait={t_traces_wait:.2f}s ({len(traces)} traces)  "
            f"txs={len(txs)}"
        )

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

    def get_last_block_yesterday(self) -> int:
        return utxo_get_last_block_yesterday(
            self.fast_exporter, self.get_last_synced_block()
        )

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
