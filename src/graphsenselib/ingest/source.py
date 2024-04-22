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
from graphsenselib.ingest.utxo import get_stream_adapter


def split_blockrange(
    blockrange: Tuple[int, int], size: int
) -> Generator[Tuple[int, int], None, None]:
    current = blockrange[0]
    while current <= blockrange[1]:
        yield (current, min(current + size - 1, blockrange[1]))
        current += size


class SourceTRX(Source):
    def __init__(self, provider_uri, grpc_provider_uri, provider_timeout):
        self.provider_uri = provider_uri
        self.grpc_provider_uri = grpc_provider_uri
        self.provider_timeout = provider_timeout
        self.thread_proxy = get_connection_from_url(provider_uri, provider_timeout)
        self.adapter = TronStreamerAdapter(
            self.thread_proxy,
            grpc_endpoint=grpc_provider_uri,
            batch_size=WEB3_QUERY_BATCH_SIZE,
            max_workers=WEB3_QUERY_WORKERS,
        )

    def read_blockrange(self, start_block, end_block):
        blocks, txs = self.adapter.export_blocks_and_transactions(
            start_block, end_block
        )
        receipts, logs = self.adapter.export_receipts_and_logs(txs)
        traces, fees = self.adapter.export_traces(start_block, end_block, True, True)
        hash_to_type = self.adapter.export_hash_to_type_mappings(
            txs, blocks, block_id_name="number"
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
        return BlockRangeContent(
            table_contents={"token_infos": token_infos}
        )  # todo dirty, its not for a blockrange, but it also works

    def get_last_synced_block(self):
        return get_last_synced_block(self.thread_proxy)


class SourceETH(Source):
    def __init__(self, provider_uri, provider_timeout):
        self.provider_uri = provider_uri
        self.provider_timeout = provider_timeout
        self.thread_proxy = get_connection_from_url(provider_uri, provider_timeout)
        self.adapter = EthStreamerAdapter(
            self.thread_proxy,
            batch_size=WEB3_QUERY_BATCH_SIZE,
            max_workers=WEB3_QUERY_WORKERS,
        )

    def read_blockrange(self, start_block, end_block):
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

    def read_blockindep(self):
        return BlockRangeContent(table_contents={})

    def get_last_synced_block(self):
        return get_last_synced_block(self.thread_proxy)


class SourceUTXO(Source):
    def __init__(self, provider_uri, network):
        self.adapter = get_stream_adapter(network, provider_uri, batch_size=30)

    def read_blockrange(self, start_block, end_block):
        blocks, txs = self.adapter.export_blocks_and_transactions(
            start_block, end_block
        )
        data = {"blocks": blocks, "txs": txs}

        return BlockRangeContent(
            table_contents=data, start_block=start_block, end_block=end_block
        )

    def read_blockindep(self):
        return BlockRangeContent(table_contents={})

    def get_last_synced_block(self):
        return self.adapter.get_current_block_number()
