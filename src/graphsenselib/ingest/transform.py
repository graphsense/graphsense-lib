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
)
from graphsenselib.ingest.common import BlockRangeContent, Transformer
from graphsenselib.ingest.utxo import (
    BLOCK_BUCKET_SIZE as UTXO_BLOCK_BUCKET_SIZE,
    TX_HASH_PREFIX_LENGTH,
    TX_BUCKET_SIZE,
    CassandraOutputResolver,
    enrich_txs,
    get_tx_refs,
    prepare_blocks_inplace,
    prepare_transactions_inplace,
    prepare_transactions_inplace_parquet,
    preprocess_block_transactions,
    preprocess_transaction_lookups,
)
from graphsenselib.utils import flatten
from graphsenselib.utils.constants import TRON_DUMMY_REPLACEMENT_ADDRESS

logger = logging.getLogger(__name__)


def _finalize_inplace(items, int_cols):
    """Drop block_id_group and convert int columns to big-endian bytes in one pass.

    Replaces separate drop_columns_from_list + to_bytes calls.
    """
    for item in items:
        item.pop("block_id_group", None)
        for col in int_cols:
            v = item[col]
            if v is not None:
                item[col] = v.to_bytes((v.bit_length() + 7) // 8, "big")


class TransformerUTXO(Transformer):
    def __init__(
        self,
        partition_batch_size,
        network,
        db=None,
        resolve_inputs_via_cassandra=False,
        fill_unresolved_inputs=False,
    ):
        super().__init__(partition_batch_size, network)
        self.db = db
        self._has_cassandra = db is not None
        self._resolve_via_cassandra = resolve_inputs_via_cassandra
        self._fill_unresolved = fill_unresolved_inputs
        if self._has_cassandra:
            self._resolver = CassandraOutputResolver(
                db,
                tx_bucket_size=TX_BUCKET_SIZE,
                tx_prefix_length=TX_HASH_PREFIX_LENGTH,
            )
            self._next_tx_id = None  # lazy init on first batch
            self._last_block_ts = None

    def transform(self, block_range_content: BlockRangeContent) -> BlockRangeContent:
        if self._has_cassandra:
            return self._transform_with_cassandra(block_range_content)
        return self._transform_delta_only(block_range_content)

    def _fill_unresolved_inputs(self, txs):
        """Fill unresolved inputs (type=None) with dummy values.

        When inputs can't be resolved (no txindex, mid-chain start, no
        Cassandra), this prevents the pipeline from crashing. Logs a
        warning for each unresolved input.
        """
        n_filled = 0
        for tx in txs:
            for inp in tx["inputs"]:
                if inp["type"] is None and inp["spent_transaction_hash"]:
                    logger.warning(
                        f"Unresolved input in tx {tx.get('hash')}: "
                        f"spent_tx={inp['spent_transaction_hash']}, "
                        f"idx={inp['spent_output_index']} — "
                        f"filling with dummy values"
                    )
                    inp["type"] = "nonstandard"
                    inp["value"] = 0
                    if not inp.get("addresses"):
                        inp["addresses"] = ["nonstandard" + "0" * 40]
                    n_filled += 1
        if n_filled > 0:
            logger.warning(
                f"Filled {n_filled} unresolved inputs with dummy values. "
                f"Data is incomplete — consider enabling txindex on the node "
                f"or using resolve_inputs_via_cassandra=true."
            )

    def _transform_delta_only(
        self, block_range_content: BlockRangeContent
    ) -> BlockRangeContent:
        t_total = time.monotonic()
        data = block_range_content.table_contents

        blocks = data["blocks"]
        txs = data["txs"]

        t0 = time.monotonic()
        prepare_blocks_inplace(
            blocks, BLOCK_BUCKET_SIZE, process_fields=False, drop_fields=False
        )
        t_prep_blocks = time.monotonic() - t0

        t0 = time.monotonic()
        enrich_txs(
            txs,
            resolver=None,
            ignore_missing_outputs=True,
            input_reference_only=True,
        )
        if self._fill_unresolved:
            self._fill_unresolved_inputs(txs)
        t_enrich = time.monotonic() - t0

        t0 = time.monotonic()
        prepare_transactions_inplace_parquet(txs, self.network)
        t_prep_txs = time.monotonic() - t0

        partition = blocks[0]["block_id"] // self.partition_batch_size
        assert partition == blocks[-1]["block_id"] // self.partition_batch_size

        t0 = time.monotonic()
        for item in blocks:
            item["partition"] = partition
        for item in txs:
            item["partition"] = partition
        t_partition = time.monotonic() - t0

        t_transform_total = time.monotonic() - t_total
        logger.info(
            f"[transform-timing] UTXO: "
            f"total={t_transform_total:.3f}s  "
            f"prep_blocks={t_prep_blocks:.3f}s ({len(blocks)} blks)  "
            f"enrich={t_enrich:.3f}s ({len(txs)} txs)  "
            f"prep_txs={t_prep_txs:.3f}s  "
            f"partition={t_partition:.3f}s"
        )

        block_range_content.table_contents = {
            "block": blocks,
            "transaction": txs,
        }
        return block_range_content

    def _transform_with_cassandra(
        self, block_range_content: BlockRangeContent
    ) -> BlockRangeContent:
        t_total = time.monotonic()
        data = block_range_content.table_contents

        blocks = data["blocks"]
        txs = data["txs"]

        # 1. get_tx_refs MUST run first, before any field renaming/modification
        t0 = time.monotonic()
        tx_refs = flatten(
            [get_tx_refs(tx["hash"], tx["inputs"], TX_HASH_PREFIX_LENGTH) for tx in txs]
        )
        t_refs = time.monotonic() - t0

        # 2. prepare_blocks_inplace — adds block_id_group, keeps all fields for parquet
        t0 = time.monotonic()
        prepare_blocks_inplace(blocks, UTXO_BLOCK_BUCKET_SIZE, drop_fields=False)
        t_prep_blocks = time.monotonic() - t0

        # 3. enrich_txs — resolves inputs via CassandraOutputResolver
        #    (skipped when inputs are already resolved at the source)
        t0 = time.monotonic()
        if self._resolve_via_cassandra:
            enrich_txs(
                txs,
                self._resolver,
                ignore_missing_outputs=False,
                input_reference_only=False,
            )
        else:
            enrich_txs(
                txs,
                resolver=None,
                ignore_missing_outputs=True,
                input_reference_only=True,
            )
            if self._fill_unresolved:
                self._fill_unresolved_inputs(txs)
        t_enrich = time.monotonic() - t0

        # 4. Init _next_tx_id from DB on first batch
        first_block_id = blocks[0]["block_id"]
        if self._next_tx_id is None:
            latest_tx_id = self.db.raw.get_latest_tx_id_before_block(first_block_id)
            self._next_tx_id = latest_tx_id + 1

        # 5. prepare_transactions_inplace — adds tx_id, tx_id_group, tx_prefix,
        #    coinjoin, tx_io_summary, casts blobs
        t0 = time.monotonic()
        prepare_transactions_inplace(
            txs, self._next_tx_id, TX_HASH_PREFIX_LENGTH, TX_BUCKET_SIZE
        )
        t_prep_txs = time.monotonic() - t0

        # 6. Update _next_tx_id
        self._next_tx_id = txs[-1]["tx_id"] + 1

        # 7. preprocess_block_transactions → block_transactions table
        t0 = time.monotonic()
        block_transactions = preprocess_block_transactions(txs, UTXO_BLOCK_BUCKET_SIZE)
        t_block_txs = time.monotonic() - t0

        # 8. preprocess_transaction_lookups → transaction_by_tx_prefix table
        t0 = time.monotonic()
        tx_lookups = preprocess_transaction_lookups(txs)
        t_lookups = time.monotonic() - t0

        # 9. Add partition to blocks and txs
        partition = blocks[0]["block_id"] // self.partition_batch_size
        assert partition == blocks[-1]["block_id"] // self.partition_batch_size
        t0 = time.monotonic()
        for item in blocks:
            item["partition"] = partition
        for item in txs:
            item["partition"] = partition
        t_partition = time.monotonic() - t0

        # 10. Store _last_block_ts for summary statistics
        self._last_block_ts = blocks[-1]["timestamp"]

        t_transform_total = time.monotonic() - t_total
        logger.info(
            f"[transform-timing] UTXO+cassandra: "
            f"total={t_transform_total:.3f}s  "
            f"tx_refs={t_refs:.3f}s  "
            f"prep_blocks={t_prep_blocks:.3f}s ({len(blocks)} blks)  "
            f"enrich={t_enrich:.3f}s ({len(txs)} txs)  "
            f"prep_txs={t_prep_txs:.3f}s  "
            f"block_txs={t_block_txs:.3f}s  "
            f"lookups={t_lookups:.3f}s  "
            f"partition={t_partition:.3f}s"
        )

        block_range_content.table_contents = {
            "block": blocks,
            "transaction": txs,
            "block_transactions": block_transactions,
            "transaction_by_tx_prefix": tx_lookups,
            "transaction_spent_in": tx_refs,
            "transaction_spending": tx_refs,
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

        partition = blocks[0]["block_id"] // self.partition_batch_size
        assert partition == blocks[-1]["block_id"] // self.partition_batch_size

        t0 = time.monotonic()
        prepare_fees_inplace(
            fees,
            TX_HASH_PREFIX_LEN,
            partition,
            keep_block_ids=True,
            drop_tx_hash_prefix=False,
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
        # Fix empty transferto_address (semantic data correction, not format conversion)
        dummy_address = TRON_DUMMY_REPLACEMENT_ADDRESS
        for trace in traces:
            if len(trace["transferto_address"]) == 0:
                trace["transferto_address"] = dummy_address
        t_fixup = time.monotonic() - t0

        t0 = time.monotonic()
        txs.sort(key=lambda x: (x["block_id"], x["transaction_index"]))
        blocks.sort(key=lambda x: x["block_id"])
        traces.sort(key=lambda x: (x["block_id"], x["trace_index"]))
        logs.sort(key=lambda x: (x["block_id"], x["log_index"]))
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
            f"fixup={t_fixup:.3f}s  "
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
        txs.sort(key=lambda x: (x["block_id"], x["transaction_index"]))
        blocks.sort(key=lambda x: x["block_id"])
        traces.sort(key=lambda x: (x["block_id"], x["trace_index"]))
        logs.sort(key=lambda x: (x["block_id"], x["log_index"]))
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
