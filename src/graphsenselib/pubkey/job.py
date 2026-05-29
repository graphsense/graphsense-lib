"""PySpark job: incremental cross-chain pubkey → address materialisation.

Per-currency invocation. State and the intermediate "which chains have we
seen this pubkey on" set live in Delta Lake; the final
``pubkey.pubkey_by_address`` table lives in Cassandra.

Pipeline per run, all under one ``SparkSession``:

1. Read ``last_processed_block`` for this network from ``<pubkey_delta>/state``.
2. Read source delta ``transaction`` for ``(last, end_block]``, extract
   compressed pubkeys via a pandas UDF (UTXO or account variant chosen by
   currency), distinct.
3. MERGE the ``(pubkey, network)`` pairs into ``<pubkey_delta>/observed``
   (partitioned by network).
4. Find pubkeys seen on >= 2 networks and not yet in
   ``<pubkey_delta>/materialized``; derive their addresses for every chain
   that ``convert_pubkey_to_addresses`` supports via a pandas UDF; write
   the resulting ``(address, pubkey)`` rows to Cassandra.
5. Append the just-materialised pubkeys to ``<pubkey_delta>/materialized``
   and bump the state row.
"""

from __future__ import annotations

import logging
from typing import Iterable, Optional

logger = logging.getLogger(__name__)


PUBKEY_KEYSPACE = "pubkey"
PUBKEY_TABLE = "pubkey_by_address"

SINK_CASSANDRA = "cassandra"
SINK_DELTA = "delta"
VALID_SINKS = (SINK_CASSANDRA, SINK_DELTA)

UTXO_CURRENCIES = {"btc", "bch", "ltc", "zec"}
ACCOUNT_CURRENCIES = {"eth", "trx"}

# All chains supported by convert_pubkey_to_addresses. We materialise
# addresses for every one of them whenever a pubkey becomes cross-chain,
# regardless of which subset of chains the pubkey was actually observed on.
DERIVATION_CHAINS = ("btc", "doge", "ltc", "zec", "eth", "trx", "bch")


def _materialised_addresses_schema():
    """Return-type schema for the address-derivation pandas UDF."""
    from pyspark.sql.types import (
        ArrayType,
        StringType,
        StructField,
        StructType,
    )

    return ArrayType(StructType([StructField("address", StringType(), nullable=False)]))


def _extract_pubkeys_udf_utxo():
    """Pandas UDF: array<input-struct> -> array<binary>.

    Returns one row of compressed-pubkey blobs per input tx. Callers
    explode the result to one (pubkey, …) row per signing key.
    """
    from pyspark.sql import functions as F
    from pyspark.sql.types import ArrayType, BinaryType

    from graphsenselib.pubkey.extract import extract_pubkeys_utxo

    @F.udf(returnType=ArrayType(BinaryType()))
    def _udf(inputs):
        if inputs is None:
            return []
        as_dicts = [
            i.asDict(recursive=True) if hasattr(i, "asDict") else i for i in inputs
        ]
        return list(extract_pubkeys_utxo(as_dicts))

    return _udf


def _extract_pubkey_udf_account(currency: str):
    """Row-wise UDF: account tx struct -> compressed pubkey (or null).

    All signature-relevant fields are passed in as a single struct so the
    UDF stays Arrow-friendly.
    """
    from pyspark.sql import functions as F
    from pyspark.sql.types import BinaryType

    from graphsenselib.pubkey.extract import extract_pubkey_account

    @F.udf(returnType=BinaryType())
    def _udf(row):
        if row is None:
            return None
        as_dict = row.asDict(recursive=True) if hasattr(row, "asDict") else row
        return extract_pubkey_account(as_dict, currency=currency)

    return _udf


def _derive_addresses_udf(chains: Iterable[str]):
    """UDF: compressed-pubkey bytes -> array<struct<address>>.

    Calls ``convert_pubkey_to_addresses`` and flattens its nested
    ``{currency: {form: address}}`` shape into a single list of address
    strings. Errors (e.g. an off-curve key that snuck through) yield an
    empty list rather than failing the whole batch.
    """
    from pyspark.sql import functions as F

    from graphsenselib.utils.pubkey_to_address import convert_pubkey_to_addresses

    chain_list = list(chains)

    @F.udf(returnType=_materialised_addresses_schema())
    def _udf(pubkey_bytes):
        if pubkey_bytes is None:
            return []
        try:
            pk_hex = bytes(pubkey_bytes).hex()
            all_addrs = convert_pubkey_to_addresses(pk_hex, currencies=chain_list)
        except Exception:
            return []
        out = []
        seen = set()
        for forms in all_addrs.values():
            if not isinstance(forms, dict):
                continue
            for key, val in forms.items():
                if key == "error":
                    continue
                if not isinstance(val, str) or not val or val in seen:
                    continue
                seen.add(val)
                out.append({"address": val})
        return out

    return _udf


class PubkeyUpdate:
    """Per-currency incremental pubkey materialisation."""

    def __init__(
        self,
        spark,
        currency: str,
        source_path: str,
        sink_path: str,
        cassandra_keyspace: str = PUBKEY_KEYSPACE,
        sink_type: str = SINK_CASSANDRA,
    ) -> None:
        if sink_type not in VALID_SINKS:
            raise ValueError(
                f"sink_type must be one of {VALID_SINKS}, got {sink_type!r}"
            )
        self.spark = spark
        self.currency = currency
        # Spark/Hadoop uses s3a:// not s3://
        self.source_path = source_path.rstrip("/").replace("s3://", "s3a://")
        self.sink_path = sink_path.rstrip("/").replace("s3://", "s3a://")
        self.cassandra_keyspace = cassandra_keyspace
        self.sink_type = sink_type
        self.network = currency

        self.observed_path = f"{self.sink_path}/observed"
        self.materialised_path = f"{self.sink_path}/materialised"
        self.state_path = f"{self.sink_path}/state"
        self.pubkey_by_address_path = f"{self.sink_path}/{PUBKEY_TABLE}"

    # ------------------------------------------------------------------
    # State
    # ------------------------------------------------------------------

    def _read_state(self) -> int:
        from delta.tables import DeltaTable
        from pyspark.sql import functions as F

        if not DeltaTable.isDeltaTable(self.spark, self.state_path):
            return -1
        df = self.spark.read.format("delta").load(self.state_path)
        rows = (
            df.filter(F.col("network") == self.network)
            .select("last_processed_block")
            .collect()
        )
        if not rows:
            return -1
        return int(rows[0]["last_processed_block"])

    def _write_state(self, end_block: int) -> None:
        from delta.tables import DeltaTable
        from pyspark.sql.types import IntegerType, StringType, StructField, StructType

        schema = StructType(
            [
                StructField("network", StringType(), False),
                StructField("last_processed_block", IntegerType(), False),
            ]
        )
        new_row = self.spark.createDataFrame([(self.network, int(end_block))], schema)
        if not DeltaTable.isDeltaTable(self.spark, self.state_path):
            new_row.write.format("delta").save(self.state_path)
            return
        target = DeltaTable.forPath(self.spark, self.state_path)
        (
            target.alias("t")
            .merge(new_row.alias("s"), "t.network = s.network")
            .whenMatchedUpdate(set={"last_processed_block": "s.last_processed_block"})
            .whenNotMatchedInsertAll()
            .execute()
        )

    # ------------------------------------------------------------------
    # Per-chain extract
    # ------------------------------------------------------------------

    def _read_source_transactions(self, start_block: int, end_block: int):
        path = f"{self.source_path}/transaction"
        df = self.spark.read.format("delta").load(path)
        return df.filter((df["block_id"] > start_block) & (df["block_id"] <= end_block))

    def _extract_pubkeys_df(self, start_block: int, end_block: int):
        """Return a 1-column DataFrame ``pubkey: binary`` for this currency."""
        from pyspark.sql import functions as F

        tx_df = self._read_source_transactions(start_block, end_block)

        if self.currency in UTXO_CURRENCIES:
            udf = _extract_pubkeys_udf_utxo()
            # Ship only the two fields the extractor actually reads
            # (script_hex, txinwitness). The raw input struct also carries
            # spent_transaction_hash, addresses, value, type, … which would
            # bloat the JVM->Python Arrow batch and OOM the stdout writer on
            # consolidation txs with thousands of inputs.
            slim_inputs = F.transform(
                F.col("inputs"),
                lambda i: F.struct(
                    i["script_hex"].alias("script_hex"),
                    i["txinwitness"].alias("txinwitness"),
                ),
            )
            pubkeys = tx_df.select(F.explode(udf(slim_inputs)).alias("pubkey"))
        elif self.currency in ACCOUNT_CURRENCIES:
            sig_struct = F.struct(
                F.col("tx_hash"),
                F.col("nonce"),
                F.col("from_address"),
                F.col("to_address"),
                F.col("value"),
                F.col("gas"),
                F.col("gas_price"),
                F.col("input"),
                F.col("max_fee_per_gas"),
                F.col("max_priority_fee_per_gas"),
                F.col("max_fee_per_blob_gas"),
                F.col("blob_versioned_hashes"),
                F.col("access_list")
                if "access_list" in tx_df.columns
                else F.lit(None).alias("access_list"),
                F.col("transaction_type"),
                F.col("v"),
                F.col("r"),
                F.col("s"),
            )
            udf = _extract_pubkey_udf_account(self.currency)
            pubkeys = tx_df.select(udf(sig_struct).alias("pubkey")).filter(
                F.col("pubkey").isNotNull()
            )
        else:
            raise ValueError(
                f"Unsupported currency for pubkey extraction: {self.currency}"
            )

        return pubkeys.dropDuplicates(["pubkey"])

    # ------------------------------------------------------------------
    # Intermediate: merge into observed (pubkey, network)
    # ------------------------------------------------------------------

    def _merge_observed(self, pubkey_df) -> None:
        from delta.tables import DeltaTable
        from pyspark.sql import functions as F

        observed_df = pubkey_df.withColumn("network", F.lit(self.network))

        if not DeltaTable.isDeltaTable(self.spark, self.observed_path):
            (
                observed_df.write.format("delta")
                .partitionBy("network")
                .save(self.observed_path)
            )
            return

        target = DeltaTable.forPath(self.spark, self.observed_path)
        (
            target.alias("t")
            .merge(
                observed_df.alias("s"),
                "t.pubkey = s.pubkey AND t.network = s.network",
            )
            .whenNotMatchedInsertAll()
            .execute()
        )

    # ------------------------------------------------------------------
    # Detection + Cassandra write
    # ------------------------------------------------------------------

    def _detect_and_materialise_cross_chain(self) -> None:
        from delta.tables import DeltaTable
        from pyspark.sql import functions as F

        observed = self.spark.read.format("delta").load(self.observed_path)
        cross_chain = (
            observed.groupBy("pubkey")
            .agg(F.collect_set("network").alias("networks"))
            .filter(F.size("networks") >= 2)
            .select("pubkey")
        )

        if DeltaTable.isDeltaTable(self.spark, self.materialised_path):
            already = self.spark.read.format("delta").load(self.materialised_path)
            to_write = cross_chain.join(already, "pubkey", "left_anti")
        else:
            to_write = cross_chain

        # Persist once: we both materialise addresses to Cassandra and
        # append to the local "materialised" set, and both need the same
        # row set without recomputing the join.
        to_write = to_write.cache()
        count = to_write.count()
        if count == 0:
            logger.info("No newly cross-chain pubkeys to materialise.")
            to_write.unpersist()
            return
        logger.info(f"Materialising {count} newly cross-chain pubkey(s).")

        derive_udf = _derive_addresses_udf(DERIVATION_CHAINS)
        derived = to_write.select(
            F.col("pubkey"),
            F.explode(derive_udf(F.col("pubkey"))).alias("addr_struct"),
        )
        out_rows = derived.select(
            F.col("addr_struct.address").alias("address"),
            F.col("pubkey").alias("pubkey"),
        )
        if self.sink_type == SINK_CASSANDRA:
            (
                out_rows.write.format("org.apache.spark.sql.cassandra")
                .options(table=PUBKEY_TABLE, keyspace=self.cassandra_keyspace)
                .mode("append")
                .save()
            )
            logger.info(
                f"Wrote derived addresses to {self.cassandra_keyspace}.{PUBKEY_TABLE}"
            )
        else:
            out_rows.write.format("delta").mode("append").save(
                self.pubkey_by_address_path
            )
            logger.info(
                f"Wrote derived addresses to delta path {self.pubkey_by_address_path}"
            )

        # Append to the materialised set so later runs anti-join correctly.
        if DeltaTable.isDeltaTable(self.spark, self.materialised_path):
            to_write.write.format("delta").mode("append").save(self.materialised_path)
        else:
            to_write.write.format("delta").save(self.materialised_path)

        to_write.unpersist()

    # ------------------------------------------------------------------
    # Entry point
    # ------------------------------------------------------------------

    def run(self, start_block: Optional[int], end_block: int) -> None:
        last_done = self._read_state()
        effective_start = (
            start_block
            if start_block is not None and start_block > last_done
            else last_done
        )
        if end_block <= effective_start:
            logger.info(
                f"Nothing to do for {self.currency}: end_block={end_block} "
                f"<= last_processed_block={effective_start}"
            )
            return

        logger.info(
            f"PubkeyUpdate[{self.currency}]: extracting pubkeys for blocks "
            f"({effective_start}, {end_block}]"
        )
        pubkey_df = self._extract_pubkeys_df(effective_start, end_block)
        self._merge_observed(pubkey_df)
        self._detect_and_materialise_cross_chain()
        self._write_state(end_block)
        logger.info(f"PubkeyUpdate[{self.currency}] complete.")
