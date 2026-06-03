"""PySpark job: incremental cross-chain pubkey → address materialisation.

Per-currency invocation. State and the intermediate "which chains have we
seen this pubkey on" set live in Delta Lake; the final
``pubkey.pubkey_by_address`` table lives in Cassandra.

Pipeline per run, all under one ``SparkSession``:

1. Read ``last_processed_block`` for this network from ``<pubkey_delta>/state``.
2. Read source delta ``transaction`` for ``(last, end_block]``, extract
   compressed pubkeys via a pandas UDF (UTXO or account variant chosen by
   currency), distinct.
3. Append the ``(pubkey, network)`` pairs to ``<pubkey_delta>/observed``
   (partitioned by network). Append-only: duplicates from re-observed hot
   keys are tolerated by detection and removed periodically by
   ``compact_observed``.
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

from graphsenselib.config import chain_forks

logger = logging.getLogger(__name__)


# Default WRITE target for the pubkey-update job. Deliberately NOT "pubkey":
# that legacy keyspace may already hold a table written by an older, unrelated
# script, and this job appends, so writing there would mix incompatible data.
# A fresh keyspace keeps the new job isolated until it is validated. Readers
# pick their source independently via
# cassandra_async_config.cross_chain_pubkey_mapping_keyspace (which still
# defaults to the legacy "pubkey"); point it here once testing is done.
# Overridable per-env (PubkeyConfig.keyspace) or per-run (--pubkey-keyspace).
PUBKEY_KEYSPACE = "pubkey_v2"
PUBKEY_TABLE = "pubkey_by_address"

SINK_CASSANDRA = "cassandra"
SINK_DELTA = "delta"
VALID_SINKS = (SINK_CASSANDRA, SINK_DELTA)

UTXO_CURRENCIES = {"btc", "bch", "ltc", "zec"}
ACCOUNT_CURRENCIES = {"eth", "trx"}

# A fork currency's extraction defaults to starting at its fork block (see
# config.chain_forks) so the shared pre-fork history isn't re-extracted into
# trivial cross-chain collisions. Back-compat alias for the BTC/BCH fork height.
BCH_FORK_BLOCK = chain_forks["bch"]["fork_block"]

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


def _extract_pubkeys_udf_utxo_outputs():
    """Pandas UDF: array<output-struct> -> array<binary>.

    P2PK / bare-P2MS reveal their keys in the output script, so those are
    extracted from outputs in addition to the input-side keys.
    """
    from pyspark.sql import functions as F
    from pyspark.sql.types import ArrayType, BinaryType

    from graphsenselib.pubkey.extract import extract_pubkeys_utxo_outputs

    @F.udf(returnType=ArrayType(BinaryType()))
    def _udf(outputs):
        if outputs is None:
            return []
        as_dicts = [
            o.asDict(recursive=True) if hasattr(o, "asDict") else o for o in outputs
        ]
        return list(extract_pubkeys_utxo_outputs(as_dicts))

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
            in_udf = _extract_pubkeys_udf_utxo()
            out_udf = _extract_pubkeys_udf_utxo_outputs()
            # Ship only the fields each extractor reads (inputs: script_hex +
            # txinwitness; outputs: script_hex). The raw structs also carry
            # spent_transaction_hash, addresses, value, type, … which would
            # bloat the JVM->Python Arrow batch and OOM the stdout writer on
            # consolidation txs with thousands of inputs/outputs.
            slim_inputs = F.transform(
                F.col("inputs"),
                lambda i: F.struct(
                    i["script_hex"].alias("script_hex"),
                    i["txinwitness"].alias("txinwitness"),
                ),
            )
            slim_outputs = F.transform(
                F.col("outputs"),
                lambda o: F.struct(o["script_hex"].alias("script_hex")),
            )
            in_keys = tx_df.select(F.explode(in_udf(slim_inputs)).alias("pubkey"))
            out_keys = tx_df.select(F.explode(out_udf(slim_outputs)).alias("pubkey"))
            # Union input- and output-side keys; the trailing dropDuplicates
            # collapses keys that appear in both (e.g. a P2PK output later spent).
            pubkeys = in_keys.unionByName(out_keys)
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

    def _append_observed(self, pubkey_df) -> None:
        """Append this run's ``(pubkey, network)`` pairs to ``observed``.

        Append-only by design: hot keys (exchanges, miners) re-appear across
        runs, so an insert-only MERGE would pay a full target-partition scan
        every run just to suppress duplicates. Detection tolerates duplicate
        rows (``collect_set`` over network is idempotent), and
        ``compact_observed`` periodically rewrites the table as distinct.
        The per-run batch is already deduplicated in ``_extract_pubkeys_df``.
        """
        from pyspark.sql import functions as F

        observed_df = pubkey_df.withColumn("network", F.lit(self.network))
        (
            observed_df.write.format("delta")
            .mode("append")
            .partitionBy("network")
            .save(self.observed_path)
        )

    # ------------------------------------------------------------------
    # Detection + Cassandra write
    # ------------------------------------------------------------------

    def _detect_and_materialise_cross_chain(self) -> None:
        # Delegate to the currency-agnostic module-level function so the same
        # detection can also be run standalone (pubkey-detect) after a batch of
        # --skip-detect appends.
        detect_and_materialise_cross_chain(
            self.spark,
            self.sink_path,
            sink_type=self.sink_type,
            cassandra_keyspace=self.cassandra_keyspace,
        )

    # ------------------------------------------------------------------
    # Entry point
    # ------------------------------------------------------------------

    def run(
        self,
        start_block: Optional[int],
        end_block: int,
        skip_detect: bool = False,
    ) -> None:
        fork = chain_forks.get(self.currency)
        if fork is not None and start_block is None:
            start_block = fork["fork_block"]
            logger.warning(
                "%s shares %s history before the fork at block %d; defaulting "
                "start_block to %d to skip the shared pre-fork blocks (extracting "
                "them would trivially collide with %s and defeat the cross-chain "
                "gate). Pass --start-block explicitly to override.",
                self.currency,
                fork["base"],
                fork["fork_block"],
                fork["fork_block"],
                fork["base"],
            )
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
        self._append_observed(pubkey_df)
        if skip_detect:
            logger.info(
                "skip_detect=True: appended to observed but skipping cross-chain "
                "detection/materialisation. Run a detection pass (pubkey-detect, "
                "or a final invocation without --skip-detect) once after the batch."
            )
        else:
            self._detect_and_materialise_cross_chain()
        self._write_state(end_block)
        logger.info(f"PubkeyUpdate[{self.currency}] complete.")


def detect_and_materialise_cross_chain(
    spark,
    sink_path: str,
    sink_type: str = SINK_CASSANDRA,
    cassandra_keyspace: str = PUBKEY_KEYSPACE,
) -> None:
    """Detect pubkeys observed on >= 2 networks and not yet materialised, derive
    their addresses for every ``DERIVATION_CHAINS`` chain, and write them.

    Currency-agnostic: reads only the shared ``observed`` / ``materialised``
    Delta tables under ``sink_path``. ``PubkeyUpdate.run(skip_detect=True)``
    defers this step so a multi-chain backfill can run the full-table
    ``groupBy`` ONCE here (``pubkey-detect``) instead of once per chain. The
    anti-join against ``materialised`` keeps writes idempotent, so a standalone
    run after a batch of appends produces exactly the same result set as running
    detection inside the last update would.
    """
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F

    if sink_type not in VALID_SINKS:
        raise ValueError(f"sink_type must be one of {VALID_SINKS}, got {sink_type!r}")

    sink_path = sink_path.rstrip("/").replace("s3://", "s3a://")
    observed_path = f"{sink_path}/observed"
    materialised_path = f"{sink_path}/materialised"
    pubkey_by_address_path = f"{sink_path}/{PUBKEY_TABLE}"

    if not DeltaTable.isDeltaTable(spark, observed_path):
        logger.info(f"No observed table at {observed_path}; nothing to materialise.")
        return

    observed = spark.read.format("delta").load(observed_path)
    cross_chain = (
        observed.groupBy("pubkey")
        .agg(F.collect_set("network").alias("networks"))
        .filter(F.size("networks") >= 2)
        .select("pubkey")
    )

    if DeltaTable.isDeltaTable(spark, materialised_path):
        already = spark.read.format("delta").load(materialised_path)
        to_write = cross_chain.join(already, "pubkey", "left_anti")
    else:
        to_write = cross_chain

    # Persist once: we both materialise addresses to the sink and append to the
    # local "materialised" set, and both need the same row set without
    # recomputing the join.
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
    if sink_type == SINK_CASSANDRA:
        (
            out_rows.write.format("org.apache.spark.sql.cassandra")
            .options(table=PUBKEY_TABLE, keyspace=cassandra_keyspace)
            .mode("append")
            .save()
        )
        logger.info(f"Wrote derived addresses to {cassandra_keyspace}.{PUBKEY_TABLE}")
    else:
        out_rows.write.format("delta").mode("append").save(pubkey_by_address_path)
        logger.info(f"Wrote derived addresses to delta path {pubkey_by_address_path}")

    # Append to the materialised set so later runs anti-join correctly.
    if DeltaTable.isDeltaTable(spark, materialised_path):
        to_write.write.format("delta").mode("append").save(materialised_path)
    else:
        to_write.write.format("delta").save(materialised_path)

    to_write.unpersist()


def compact_observed(spark, sink_path: str) -> None:
    """Rewrite the ``observed`` table as distinct ``(pubkey, network)`` and compact.

    ``_append_observed`` is append-only, so re-observed hot keys accumulate
    duplicate rows over many runs. Detection stays correct regardless
    (``collect_set``), but this shrinks the table the detection groupBy must
    scan and bin-packs the many small append files. Safe to schedule between
    ``pubkey-update`` runs; serialise it against them via the same lock.
    """
    from delta.tables import DeltaTable

    sink_path = sink_path.rstrip("/").replace("s3://", "s3a://")
    observed_path = f"{sink_path}/observed"

    if not DeltaTable.isDeltaTable(spark, observed_path):
        logger.info(f"No observed table at {observed_path}; nothing to compact.")
        return

    df = spark.read.format("delta").load(observed_path)
    before = df.count()
    # Materialise the deduplicated snapshot before overwriting the same path,
    # so the overwrite reads from cache rather than the table it truncates.
    deduped = df.dropDuplicates(["pubkey", "network"]).persist()
    after = deduped.count()
    try:
        (
            deduped.write.format("delta")
            .mode("overwrite")
            .partitionBy("network")
            .save(observed_path)
        )
    finally:
        deduped.unpersist()
    logger.info(
        f"Compacted observed: {before} -> {after} rows "
        f"({before - after} duplicate observations removed)."
    )

    DeltaTable.forPath(spark, observed_path).optimize().executeCompaction()
    logger.info("OPTIMIZE complete on observed.")


def load_pubkey_to_cassandra(
    spark,
    sink_path: str,
    cassandra_keyspace: str,
    table: str = PUBKEY_TABLE,
) -> None:
    """Load the Delta ``pubkey_by_address`` table into Cassandra.

    Decouples the heavy extraction + cross-chain detection (run with
    ``sink_type=delta`` so it never touches production Cassandra) from the
    throttled Cassandra write: produce the dataset to Delta, inspect it, then
    load it here once it looks good. Reads a consistent Delta snapshot, so it is
    safe to run independently. Idempotent — the Cassandra table is keyed by
    ``address``, so a re-load upserts the same rows.
    """
    from delta.tables import DeltaTable

    sink_path = sink_path.rstrip("/").replace("s3://", "s3a://")
    delta_path = f"{sink_path}/{table}"
    if not DeltaTable.isDeltaTable(spark, delta_path):
        raise ValueError(
            f"No Delta {table!r} table at {delta_path}. Run a sink_type=delta "
            "pubkey-update first to produce it."
        )

    df = (
        spark.read.format("delta")
        .load(delta_path)
        .select("address", "pubkey")
        .dropDuplicates(["address", "pubkey"])
    )
    logger.info(f"Loading {delta_path} -> Cassandra {cassandra_keyspace}.{table}")
    (
        df.write.format("org.apache.spark.sql.cassandra")
        .options(table=table, keyspace=cassandra_keyspace)
        .mode("append")
        .save()
    )
    logger.info(f"Loaded {table} into {cassandra_keyspace}.")
