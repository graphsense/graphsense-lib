try:
    from ethereumetl.streaming.enrich import enrich_transactions
except ImportError:
    _has_ingest_dependencies = False
else:
    _has_ingest_dependencies = True

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
    enrich_txs,
    prepare_blocks_inplace,
    prepare_transactions_inplace_parquet,
)
from graphsenselib.schema.resources.parquet.account import (
    BINARY_COL_CONVERSION_MAP_ACCOUNT,
)
from graphsenselib.schema.resources.parquet.account_trx import (
    BINARY_COL_CONVERSION_MAP_ACCOUNT_TRX,
)

# drop block_id_group column
from .utxo import drop_columns_from_list


class TransformerUTXO(Transformer):
    def transform(self, block_range_content: BlockRangeContent) -> BlockRangeContent:
        data = block_range_content.table_contents

        blocks = data["blocks"]
        txs = data["txs"]

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
        }
        return block_range_content

    def transform_blockindep(self, data):
        return BlockRangeContent(table_contents={})


class TransformerTRX(Transformer):
    def transform(self, block_range_content: BlockRangeContent) -> BlockRangeContent:
        if not _has_ingest_dependencies:
            raise ImportError(
                "Transform function needs ethereumetl installed. Please install gslib with ingest dependencies."
            )

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
        prepare_fees_inplace(
            fees,
            TX_HASH_PREFIX_LEN,
            partition,
            keep_block_ids=True,
            drop_tx_hash_prefix=True,
        )

        prepare_transactions_inplace = prepare_transactions_inplace_trx
        prepare_traces_inplace = prepare_traces_inplace_trx

        # COMMON
        prepare_transactions_inplace(
            txs, TX_HASH_PREFIX_LEN, BLOCK_BUCKET_SIZE, self.partition_batch_size
        )
        prepare_traces_inplace(traces, BLOCK_BUCKET_SIZE, self.partition_batch_size)
        prepare_logs_inplace(logs, BLOCK_BUCKET_SIZE, self.partition_batch_size)

        blocks = drop_columns_from_list(blocks, ["block_id_group"])
        txs = drop_columns_from_list(txs, ["block_id_group"])
        traces = drop_columns_from_list(traces, ["block_id_group"])
        logs = drop_columns_from_list(logs, ["block_id_group"])

        def fix_transferToAddress(item):
            if len(item["transferto_address"]) == 0:
                # for some very rare txs
                # e.g. f0b31777dcc58cbca074380ff6f25f8495898edba2da0c43b099b3f276ae3d74
                # transferTo_address is empty which does not mach our schema.
                # as a quick fix we set a dummy address for now.
                dummy_address = b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00ikna"  # noqa
                item["transferto_address"] = dummy_address
            return item

        traces = [fix_transferToAddress(trace) for trace in traces]

        txs = to_bytes(txs, BINARY_COL_CONVERSION_MAP_ACCOUNT_TRX["transaction"])
        blocks = to_bytes(blocks, BINARY_COL_CONVERSION_MAP_ACCOUNT_TRX["block"])
        traces = to_bytes(traces, BINARY_COL_CONVERSION_MAP_ACCOUNT_TRX["trace"])
        logs = to_bytes(logs, BINARY_COL_CONVERSION_MAP_ACCOUNT_TRX["log"])

        txs = sorted(txs, key=lambda x: (x["block_id"], x["transaction_index"]))
        blocks = sorted(blocks, key=lambda x: x["block_id"])
        traces = sorted(traces, key=lambda x: (x["block_id"], x["trace_index"]))
        logs = sorted(logs, key=lambda x: (x["block_id"], x["log_index"]))

        # fees should already be ordered coming out of the function called
        # export_hash_to_type_mappings_parallel
        # and would need the tx_hash from the transaction table for ordering

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

        blocks = drop_columns_from_list(blocks, ["block_id_group"])
        txs = drop_columns_from_list(txs, ["block_id_group"])
        traces = drop_columns_from_list(traces, ["block_id_group"])
        logs = drop_columns_from_list(logs, ["block_id_group"])

        txs = to_bytes(txs, BINARY_COL_CONVERSION_MAP_ACCOUNT["transaction"])
        blocks = to_bytes(blocks, BINARY_COL_CONVERSION_MAP_ACCOUNT["block"])
        traces = to_bytes(traces, BINARY_COL_CONVERSION_MAP_ACCOUNT["trace"])
        logs = to_bytes(logs, BINARY_COL_CONVERSION_MAP_ACCOUNT["log"])

        txs = sorted(txs, key=lambda x: (x["block_id"], x["transaction_index"]))
        blocks = sorted(blocks, key=lambda x: x["block_id"])
        traces = sorted(traces, key=lambda x: (x["block_id"], x["trace_index"]))
        logs = sorted(logs, key=lambda x: (x["block_id"], x["log_index"]))

        block_range_content.table_contents = {
            "block": blocks,
            "transaction": txs,
            "log": logs,
            "trace": traces,
        }
        return block_range_content

    def transform_blockindep(self, data):
        return BlockRangeContent(table_contents={})
