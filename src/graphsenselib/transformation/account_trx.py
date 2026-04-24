"""Account TRX (Tron) transformation: Delta Lake → Cassandra raw keyspace.

TRX shares the EVM block / transaction / log schema with ETH (handled by
`AccountTransformationBase`); it diverges in the trace schema and adds the
`fee` and `trc10` tables.
"""

import logging

from graphsenselib.transformation.account import (
    AccountTransformationBase,
    _convert_varint_cols,
)

logger = logging.getLogger(__name__)


class AccountTrxTransformation(AccountTransformationBase):
    """TRX binding."""

    # block.gas_limit is varint on TRX (int on ETH)
    VARINT_COLS_BLOCK = frozenset({"difficulty", "total_difficulty", "gas_limit"})

    DELTA_ONLY_COLS_TRACE = frozenset({"partition"})
    VARINT_COLS_TRACE = frozenset({"call_value"})

    DELTA_ONLY_COLS_FEE = frozenset({"partition", "block_id"})

    DELTA_ONLY_COLS_TRC10 = frozenset(
        {"partition", "public_free_asset_net_usage", "order"}
    )
    # trc10 varint columns are stored as int64 in parquet — connector handles
    # int → varint natively, listed for completeness.
    VARINT_COLS_TRC10 = frozenset(
        {
            "total_supply",
            "trx_num",
            "num",
            "start_time",
            "end_time",
            "public_latest_free_net_time",
        }
    )

    TABLES = ("block", "transaction", "trace", "log", "fee", "trc10")

    def transform_trace(self, start_block, end_block):
        from pyspark.sql import functions as F

        df = self._read_delta("trace", start_block, end_block)
        df = df.withColumn(
            "block_id_group",
            F.floor(F.col("block_id") / self.block_bucket_size).cast("int"),
        )
        drop_cols = [c for c in self.DELTA_ONLY_COLS_TRACE if c in df.columns]
        df = df.drop(*drop_cols)
        if "internal_index" in df.columns:
            df = df.withColumn("internal_index", F.col("internal_index").cast("short"))
        if "call_info_index" in df.columns:
            df = df.withColumn(
                "call_info_index", F.col("call_info_index").cast("short")
            )
        df = _convert_varint_cols(df, self.VARINT_COLS_TRACE)
        df = df.repartitionByRange(2000, "block_id_group", "block_id")
        self._write_cassandra(df, "trace")

    def transform_fee(self, start_block, end_block):
        from pyspark.sql import functions as F

        df = self._read_delta("fee", start_block, end_block)
        drop_cols = [c for c in self.DELTA_ONLY_COLS_FEE if c in df.columns]
        df = df.drop(*drop_cols)
        # Derive tx_hash_prefix from tx_hash (Cassandra primary key)
        if "tx_hash_prefix" not in df.columns and "tx_hash" in df.columns:
            df = df.withColumn(
                "tx_hash_prefix",
                F.substring(
                    F.lower(F.hex(F.col("tx_hash"))), 1, self.tx_hash_prefix_len
                ),
            )
        self._write_cassandra(df, "fee")

    def transform_trc10(self, start_block, end_block):
        from pyspark.sql import functions as F

        df = self._read_delta("trc10", start_block, end_block)
        drop_cols = [c for c in self.DELTA_ONLY_COLS_TRC10 if c in df.columns]
        df = df.drop(*drop_cols)
        df = _convert_varint_cols(df, self.VARINT_COLS_TRC10)
        if "vote_score" in df.columns:
            df = df.withColumn("vote_score", F.col("vote_score").cast("short"))
        if "precision" in df.columns:
            df = df.withColumn("precision", F.col("precision").cast("short"))
        self._write_cassandra(df, "trc10")

    def _table_methods(self):
        return {
            **super()._table_methods(),
            "fee": self.transform_fee,
            "trc10": self.transform_trc10,
        }
