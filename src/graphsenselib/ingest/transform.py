import logging
import time

from graphsenselib.ingest.account import (
    BLOCK_BUCKET_SIZE,
    TX_HASH_PREFIX_LEN,
    enrich_transactions_with_type,
    enrich_txs_with_vrs,
    prepare_blocks_inplace_trx,
    prepare_fees_inplace,
    prepare_logs_inplace,
    prepare_traces_inplace_eth,
    prepare_traces_inplace_trx,
    prepare_transactions_inplace_eth,
    prepare_transactions_inplace_trx,
    prepare_trc10_tokens_inplace,
    prepare_blocks_inplace_eth,
    to_bytes,
)
from graphsenselib.ingest.common import BlockRangeContent, Transformer
from graphsenselib.ingest.utxo import (
    drop_columns_from_list,
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
from graphsenselib.utils.constants import TRON_DUMMY_REPLACEMENT_ADDRESS

logger = logging.getLogger(__name__)


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
        t_total = time.monotonic()
        data = block_range_content.table_contents

        blocks = data["blocks"]
        txs = data["txs"]
        receipts = data["receipts"]
        logs = data["logs"]
        traces = data["traces"]
        fees = data["fees"]
        hash_to_type = data["hash_to_type"]

        t0 = time.monotonic()
        prepare_blocks_inplace_trx(blocks, BLOCK_BUCKET_SIZE, self.partition_batch_size)
        t_prep_blocks = time.monotonic() - t0

        t0 = time.monotonic()
        txs = enrich_txs_with_vrs(txs, receipts)
        t_enrich_vrs = time.monotonic() - t0

        t0 = time.monotonic()
        txs = enrich_transactions_with_type(txs, hash_to_type)
        t_enrich_type = time.monotonic() - t0

        # todo this can be a problem if the there are multiple partitions
        partition = blocks[0]["block_id"] // self.partition_batch_size

        t0 = time.monotonic()
        prepare_fees_inplace(
            fees,
            TX_HASH_PREFIX_LEN,
            partition,
            keep_block_ids=True,
            drop_tx_hash_prefix=True,
        )
        t_prep_fees = time.monotonic() - t0

        t0 = time.monotonic()
        prepare_transactions_inplace_trx(
            txs, TX_HASH_PREFIX_LEN, BLOCK_BUCKET_SIZE, self.partition_batch_size
        )
        t_prep_txs = time.monotonic() - t0

        t0 = time.monotonic()
        prepare_traces_inplace_trx(traces, BLOCK_BUCKET_SIZE, self.partition_batch_size)
        t_prep_traces = time.monotonic() - t0

        t0 = time.monotonic()
        prepare_logs_inplace(logs, BLOCK_BUCKET_SIZE, self.partition_batch_size)
        t_prep_logs = time.monotonic() - t0

        t0 = time.monotonic()
        blocks = drop_columns_from_list(blocks, ["block_id_group"])
        txs = drop_columns_from_list(txs, ["block_id_group"])
        traces = drop_columns_from_list(traces, ["block_id_group"])
        logs = drop_columns_from_list(logs, ["block_id_group"])
        t_drop_cols = time.monotonic() - t0

        def fix_transferToAddress(item):
            if len(item["transferto_address"]) == 0:
                dummy_address = TRON_DUMMY_REPLACEMENT_ADDRESS  # noqa
                item["transferto_address"] = dummy_address
            return item

        t0 = time.monotonic()
        traces = [fix_transferToAddress(trace) for trace in traces]
        t_fix_addr = time.monotonic() - t0

        t0 = time.monotonic()
        txs = to_bytes(txs, BINARY_COL_CONVERSION_MAP_ACCOUNT_TRX["transaction"])
        blocks = to_bytes(blocks, BINARY_COL_CONVERSION_MAP_ACCOUNT_TRX["block"])
        traces = to_bytes(traces, BINARY_COL_CONVERSION_MAP_ACCOUNT_TRX["trace"])
        logs = to_bytes(logs, BINARY_COL_CONVERSION_MAP_ACCOUNT_TRX["log"])
        t_to_bytes = time.monotonic() - t0

        t0 = time.monotonic()
        txs = sorted(txs, key=lambda x: (x["block_id"], x["transaction_index"]))
        blocks = sorted(blocks, key=lambda x: x["block_id"])
        traces = sorted(traces, key=lambda x: (x["block_id"], x["trace_index"]))
        logs = sorted(logs, key=lambda x: (x["block_id"], x["log_index"]))
        t_sort = time.monotonic() - t0

        t_transform_total = time.monotonic() - t_total
        logger.info(
            f"[transform-timing] TRX: "
            f"total={t_transform_total:.3f}s  "
            f"prep_blocks={t_prep_blocks:.3f}s ({len(blocks)} blks)  "
            f"enrich_vrs={t_enrich_vrs:.3f}s  "
            f"enrich_type={t_enrich_type:.3f}s  "
            f"prep_fees={t_prep_fees:.3f}s ({len(fees) if fees else 0} fees)  "
            f"prep_txs={t_prep_txs:.3f}s ({len(txs)} txs)  "
            f"prep_traces={t_prep_traces:.3f}s ({len(traces)} traces)  "
            f"prep_logs={t_prep_logs:.3f}s ({len(logs)} logs)  "
            f"drop_cols={t_drop_cols:.3f}s  "
            f"fix_addr={t_fix_addr:.3f}s  "
            f"to_bytes={t_to_bytes:.3f}s  "
            f"sort={t_sort:.3f}s"
        )

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
        t_total = time.monotonic()
        data = block_range_content.table_contents

        blocks = data["blocks"]
        txs = data["txs"]
        receipts = data["receipts"]
        logs = data["logs"]
        traces = data["traces"]

        t0 = time.monotonic()
        prepare_blocks_inplace_eth(blocks, BLOCK_BUCKET_SIZE, self.partition_batch_size)
        t_prep_blocks = time.monotonic() - t0

        t0 = time.monotonic()
        txs = enrich_txs_with_vrs(txs, receipts)
        t_enrich_vrs = time.monotonic() - t0

        t0 = time.monotonic()
        prepare_transactions_inplace_eth(
            txs, TX_HASH_PREFIX_LEN, BLOCK_BUCKET_SIZE, self.partition_batch_size
        )
        t_prep_txs = time.monotonic() - t0

        t0 = time.monotonic()
        prepare_traces_inplace_eth(traces, BLOCK_BUCKET_SIZE, self.partition_batch_size)
        t_prep_traces = time.monotonic() - t0

        t0 = time.monotonic()
        prepare_logs_inplace(logs, BLOCK_BUCKET_SIZE, self.partition_batch_size)
        t_prep_logs = time.monotonic() - t0

        t0 = time.monotonic()
        blocks = drop_columns_from_list(blocks, ["block_id_group"])
        txs = drop_columns_from_list(txs, ["block_id_group"])
        traces = drop_columns_from_list(traces, ["block_id_group"])
        logs = drop_columns_from_list(logs, ["block_id_group"])
        t_drop_cols = time.monotonic() - t0

        t0 = time.monotonic()
        txs = to_bytes(txs, BINARY_COL_CONVERSION_MAP_ACCOUNT["transaction"])
        blocks = to_bytes(blocks, BINARY_COL_CONVERSION_MAP_ACCOUNT["block"])
        traces = to_bytes(traces, BINARY_COL_CONVERSION_MAP_ACCOUNT["trace"])
        logs = to_bytes(logs, BINARY_COL_CONVERSION_MAP_ACCOUNT["log"])
        t_to_bytes = time.monotonic() - t0

        t0 = time.monotonic()
        txs = sorted(txs, key=lambda x: (x["block_id"], x["transaction_index"]))
        blocks = sorted(blocks, key=lambda x: x["block_id"])
        traces = sorted(traces, key=lambda x: (x["block_id"], x["trace_index"]))
        logs = sorted(logs, key=lambda x: (x["block_id"], x["log_index"]))
        t_sort = time.monotonic() - t0

        t_transform_total = time.monotonic() - t_total
        logger.info(
            f"[transform-timing] ETH: "
            f"total={t_transform_total:.3f}s  "
            f"prep_blocks={t_prep_blocks:.3f}s ({len(blocks)} blks)  "
            f"enrich_vrs={t_enrich_vrs:.3f}s  "
            f"prep_txs={t_prep_txs:.3f}s ({len(txs)} txs)  "
            f"prep_traces={t_prep_traces:.3f}s ({len(traces)} traces)  "
            f"prep_logs={t_prep_logs:.3f}s ({len(logs)} logs)  "
            f"drop_cols={t_drop_cols:.3f}s  "
            f"to_bytes={t_to_bytes:.3f}s  "
            f"sort={t_sort:.3f}s"
        )

        block_range_content.table_contents = {
            "block": blocks,
            "transaction": txs,
            "log": logs,
            "trace": traces,
        }
        return block_range_content

    def transform_blockindep(self, data):
        return BlockRangeContent(table_contents={})
