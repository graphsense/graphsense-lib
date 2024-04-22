from ethereumetl.streaming.enrich import enrich_transactions

from graphsenselib.ingest.account import (
    BLOCK_BUCKET_SIZE,
    TX_HASH_PREFIX_LEN,
    enrich_transactions_with_type,
    prepare_blocks_inplace_eth,
    prepare_blocks_inplace_trx,
    prepare_fees_inplace,
    prepare_logs_inplace,
    prepare_traces_inplace_eth,
    prepare_traces_inplace_trx,
    prepare_transactions_inplace_eth,
    prepare_transactions_inplace_trx,
    prepare_trc10_tokens_inplace,
    to_bytes,
)
from graphsenselib.ingest.common import BlockRangeContent, Transformer
from graphsenselib.ingest.utxo import (
    TX_HASH_PREFIX_LENGTH,
    enrich_txs,
    get_tx_refs,
    prepare_blocks_inplace,
    prepare_refs_inplace_parquet,
    prepare_transactions_inplace_parquet,
)
from graphsenselib.schema.resources.parquet.account import (
    BINARY_COL_CONVERSION_MAP_ACCOUNT,
)
from graphsenselib.schema.resources.parquet.account_trx import (
    BINARY_COL_CONVERSION_MAP_ACCOUNT_TRX,
)
from graphsenselib.utils import flatten


class TransformerUTXO(Transformer):
    def transform(self, block_range_content: BlockRangeContent) -> BlockRangeContent:
        data = block_range_content.table_contents

        blocks = data["blocks"]
        txs = data["txs"]

        tx_refs = flatten(
            [get_tx_refs(tx["hash"], tx["inputs"], TX_HASH_PREFIX_LENGTH) for tx in txs]
        )

        prepare_blocks_inplace(
            blocks, BLOCK_BUCKET_SIZE, process_fields=False, drop_fields=False
        )

        # until bitcoin-etl progresses
        # with https://github.com/blockchain-etl/bitcoin-etl/issues/43
        enrich_txs(
            txs,
            resolver=None,
            ignore_missing_outputs=True,
            input_reference_only=True,
        )

        prepare_transactions_inplace_parquet(txs, self.network)
        prepare_refs_inplace_parquet(tx_refs)

        partition = (
            blocks[0]["block_id"] // self.partition_batch_size
        )  # todo this can be a problem if the there are multiple partitions

        def with_partition(items: list) -> list:
            for item in items:
                item["partition"] = partition
            return items

        block_range_content.table_contents = {
            "block": with_partition(blocks),
            "transaction": with_partition(txs),
            "transaction_spending": with_partition(tx_refs),
        }
        return block_range_content

    def transform_blockindep(self, data):
        return BlockRangeContent(table_contents={})


class TransformerTRX(Transformer):
    def transform(self, block_range_content: BlockRangeContent) -> BlockRangeContent:
        data = block_range_content.table_contents

        blocks = data["blocks"]
        txs = data["txs"]
        receipts = data["receipts"]
        logs = data["logs"]
        traces = data["traces"]
        fees = data["fees"]
        hash_to_type = data["hash_to_type"]

        prepare_blocks_inplace_trx(blocks, BLOCK_BUCKET_SIZE, self.partition_batch_size)
        txs = enrich_transactions(txs, receipts)
        txs = enrich_transactions_with_type(txs, hash_to_type)
        # todo this can be a problem if the there are multiple partitions
        partition = blocks[0]["block_id"] // self.partition_batch_size
        prepare_fees_inplace(fees, TX_HASH_PREFIX_LEN, partition)
        prepare_transactions_inplace = prepare_transactions_inplace_trx
        prepare_traces_inplace = prepare_traces_inplace_trx

        # COMMON
        prepare_transactions_inplace(
            txs, TX_HASH_PREFIX_LEN, BLOCK_BUCKET_SIZE, self.partition_batch_size
        )
        prepare_traces_inplace(traces, BLOCK_BUCKET_SIZE, self.partition_batch_size)
        prepare_logs_inplace(logs, BLOCK_BUCKET_SIZE, self.partition_batch_size)

        txs = to_bytes(txs, BINARY_COL_CONVERSION_MAP_ACCOUNT_TRX["transaction"])
        blocks = to_bytes(blocks, BINARY_COL_CONVERSION_MAP_ACCOUNT_TRX["block"])
        traces = to_bytes(traces, BINARY_COL_CONVERSION_MAP_ACCOUNT_TRX["trace"])
        logs = to_bytes(logs, BINARY_COL_CONVERSION_MAP_ACCOUNT_TRX["log"])

        block_range_content.table_contents = {
            "block": blocks,
            "transaction": txs,
            "log": logs,
            "trace": traces,
            "fee": fees,
        }
        return block_range_content

    def transform_blockindep(
        self, block_range_content: BlockRangeContent
    ) -> BlockRangeContent:
        token_infos = block_range_content.table_contents["token_infos"]
        prepare_trc10_tokens_inplace(token_infos)
        return BlockRangeContent(table_contents={"trc10": token_infos})


class TransformerETH(Transformer):
    def transform(self, block_range_content: BlockRangeContent) -> BlockRangeContent:
        data = block_range_content.table_contents

        blocks = data["blocks"]
        txs = data["txs"]
        receipts = data["receipts"]
        logs = data["logs"]
        traces = data["traces"]

        prepare_blocks_inplace_eth(blocks, BLOCK_BUCKET_SIZE, self.partition_batch_size)
        txs = enrich_transactions(txs, receipts)
        prepare_transactions_inplace_eth(
            txs, TX_HASH_PREFIX_LEN, BLOCK_BUCKET_SIZE, self.partition_batch_size
        )
        prepare_traces_inplace_eth(traces, BLOCK_BUCKET_SIZE, self.partition_batch_size)
        prepare_logs_inplace(logs, BLOCK_BUCKET_SIZE, self.partition_batch_size)

        txs = to_bytes(txs, BINARY_COL_CONVERSION_MAP_ACCOUNT["transaction"])
        blocks = to_bytes(blocks, BINARY_COL_CONVERSION_MAP_ACCOUNT["block"])
        traces = to_bytes(traces, BINARY_COL_CONVERSION_MAP_ACCOUNT["trace"])
        logs = to_bytes(logs, BINARY_COL_CONVERSION_MAP_ACCOUNT["log"])

        block_range_content.table_contents = {
            "block": blocks,
            "transaction": txs,
            "log": logs,
            "trace": traces,
        }
        return block_range_content

    def transform_blockindep(self, data):
        return BlockRangeContent(table_contents={})
