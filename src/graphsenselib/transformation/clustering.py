"""UTXO clustering I/O helpers and one-off batch clustering entrypoint.

The PySpark one-off (:func:`run_clustering_spark`) bulk-reads the raw tables and
derives multi-input edges with :func:`multi_input_address_id_sets`.
:func:`iter_multi_input_tx_inputs` is the single-driver equivalent that streams
those edges for a block range by reading the raw txs back; it feeds the
stand-alone range re-cluster (``UpdateStrategyUtxo.run_fresh_clustering``, used
for backfill / recovery).  The continuous delta path does not read back — it
harvests the same id sets from the txs it already holds.
"""

import logging
import time
from typing import Dict, Iterator, List, Optional, Set

from graphsenselib.utils.utxo import multi_input_address_set, resolve_address_id_sets

logger = logging.getLogger(__name__)

DEFAULT_BLOCK_CHUNK_SIZE = 1_000
DEFAULT_CASSANDRA_CONCURRENCY = 100


def iter_multi_input_tx_inputs(
    db,
    start_block: int,
    end_block: int,
    chunk_size: int = DEFAULT_BLOCK_CHUNK_SIZE,
    concurrency: int = DEFAULT_CASSANDRA_CONCURRENCY,
) -> Iterator[List[List[int]]]:
    """Yield address-id lists of multi-input transactions per block chunk.

    Reads ``raw.block_transactions`` for every block in ``[start_block,
    end_block]`` in ``chunk_size``-block chunks (parallel point reads), then
    per-bucket range-reads ``raw.transaction`` over ``[min_tx_id, max_tx_id]``
    for each chunk (parallel single-partition scans), resolves unique input
    addresses to ``address_id`` via
    ``transformed.address_ids_by_address_prefix`` (async batched), and yields
    one ``List[List[int]]`` per chunk containing the dense address-id lists
    for transactions with >=2 resolved inputs.

    Relies on the invariant that ``tx_id`` is assigned in block order, so
    every ``tx_id`` in ``[min_tx_id, max_tx_id]`` belongs to a block in
    ``[chunk_start, chunk_end]`` — the same invariant used by
    :meth:`RawDbUtxo.get_transactions_in_block`.

    Yields nothing for chunks with no transactions or no resolvable
    multi-input transactions.  Caller is expected to feed each chunk to the
    Rust Union-Find and discard it before reading the next.
    """
    rdb = db.raw
    tdb = db.transformed
    raw_ks = rdb.get_keyspace()
    block_bucket_size = rdb.get_block_bucket_size()
    tx_bucket_size = rdb.get_tx_bucket_size()

    bt_prep = rdb._db.get_prepared_statement(
        f"SELECT txs FROM {raw_ks}.block_transactions "
        "WHERE block_id_group=:block_id_group AND block_id=:block_id"
    )
    tx_prep = rdb._db.get_prepared_statement(
        f"SELECT coinbase, inputs FROM {raw_ks}.transaction "
        "WHERE tx_id_group=:tx_id_group "
        "AND tx_id>:tx_id_lower AND tx_id<=:tx_id_upper"
    )

    for chunk_start in range(start_block, end_block + 1, chunk_size):
        chunk_end = min(chunk_start + chunk_size - 1, end_block)

        bt_stmts = [
            bt_prep.bind(
                {
                    "block_id_group": rdb.get_id_group(b, block_bucket_size),
                    "block_id": b,
                }
            )
            for b in range(chunk_start, chunk_end + 1)
        ]
        tx_ids: List[int] = []
        for success, result in rdb._db.execute_statements_async(
            bt_stmts, concurrency=concurrency
        ):
            if not success:
                raise RuntimeError(
                    f"block_transactions read failed in chunk "
                    f"[{chunk_start},{chunk_end}]: {result}"
                )
            for row in result:
                if row.txs:
                    tx_ids.extend(tx.tx_id for tx in row.txs)

        if not tx_ids:
            continue

        min_tx_id = min(tx_ids)
        max_tx_id = max(tx_ids)
        min_bucket = rdb.get_id_group(min_tx_id, tx_bucket_size)
        max_bucket = rdb.get_id_group(max_tx_id, tx_bucket_size)

        tx_stmts = [
            tx_prep.bind(
                {
                    "tx_id_group": bucket,
                    "tx_id_lower": min_tx_id - 1,
                    "tx_id_upper": max_tx_id,
                }
            )
            for bucket in range(min_bucket, max_bucket + 1)
        ]

        tx_input_addr_sets: List[Set[str]] = []
        unique_addresses: Set[str] = set()
        for success, result in rdb._db.execute_statements_async(
            tx_stmts, concurrency=concurrency
        ):
            if not success:
                raise RuntimeError(
                    f"transaction read failed in chunk "
                    f"[{chunk_start},{chunk_end}]: {result}"
                )
            for row in result:
                addrs = multi_input_address_set(row)
                if addrs is None:
                    continue
                tx_input_addr_sets.append(addrs)
                unique_addresses.update(addrs)

        if not tx_input_addr_sets:
            continue

        addr_to_id: Dict[str, int] = {}
        for adr, exec_result in tdb.get_address_id_async_batch(list(unique_addresses)):
            row = exec_result.result_or_exc.one()
            if row is not None:
                addr_to_id[adr] = row.address_id

        tx_input_ids = resolve_address_id_sets(tx_input_addr_sets, addr_to_id.get)

        if tx_input_ids:
            logger.debug(
                f"  chunk [{chunk_start:,}-{chunk_end:,}] "
                f"{len(tx_ids):,} txs ({len(tx_input_ids):,} multi-input)"
            )
            yield tx_input_ids


# Caps how many transactions of a partition's Arrow column are handed to Rust
# per process_transactions_arrow() call. The slice is a zero-copy Arrow view
# (no Python materialization), so this just bounds per-call work, not driver
# memory; --read-partitions is the primary control. Only binds when a partition
# holds more rows than this.
DEFAULT_FEED_BATCH_SIZE = 2_000_000
DEFAULT_SPARK_WRITE_CHUNK = 5_000_000
# Shuffle width for the Spark clustering job. The multi-input edge set is far
# smaller than the full transaction table, so the default 200 shuffle
# partitions just spawn hundreds of tiny tasks/stages across distinct/join/
# groupBy and the Arrow collect.
DEFAULT_READ_PARTITIONS = 64


def multi_input_address_id_sets(tx_df, address_ids_df, end_block: Optional[int] = None):
    """Derive each multi-input transaction's distinct input ``address_id`` set.

    Pure DataFrame transform (no Cassandra I/O) so it can be unit-tested with
    synthetic frames. This is the Spark (DataFrame-ops) expression of the same
    rule as the canonical single-tx helper
    ``utils.utxo.multi_input_address_set`` used by the driver and delta paths —
    keep the two in sync:

      * ``tx_df`` has the ``raw.transaction`` shape: ``tx_id``, ``block_id``,
        ``coinbase``, and ``inputs`` = ``array<struct<address: array<string>,
        ...>>``;
      * if ``end_block`` is given, only transactions with ``block_id <=
        end_block`` are considered (cluster the chain as of that height);
      * coinbase transactions and null addresses are dropped;
      * input addresses are taken as a DISTINCT set per transaction, resolved
        to ``address_id`` via ``address_ids_df`` (``address``, ``address_id``);
      * only transactions with >= 2 distinct resolved ``address_id`` s survive.

    Returns a DataFrame with a single ``ids`` column (``array<address_id>``),
    each row an order-independent edge set for the Union-Find.
    """
    from pyspark.sql import functions as F

    if end_block is not None:
        tx_df = tx_df.filter(F.col("block_id") <= end_block)

    tx_address = (
        tx_df.select("tx_id", "coinbase", "inputs")
        .filter(~F.coalesce(F.col("coinbase"), F.lit(False)))
        .filter(F.col("inputs").isNotNull() & (F.size("inputs") >= 1))
        .select("tx_id", F.explode("inputs").alias("inp"))
        .select("tx_id", F.explode("inp.address").alias("address"))
        .filter(F.col("address").isNotNull())
        .distinct()
    )

    return (
        tx_address.join(address_ids_df.select("address", "address_id"), "address")
        .groupBy("tx_id")
        .agg(F.collect_set("address_id").alias("ids"))
        .filter(F.size("ids") >= 2)
        .select("ids")
    )


def backfill_fresh_cluster_stats(spark, transformed_keyspace: str) -> int:
    """(Re)compute ``fresh_cluster_stats`` from ``fresh_cluster_addresses``.

    Aggregates the membership reverse-index ``(cluster_id, address_id)`` into one
    ``(cluster_id, size, min_address_id)`` row per cluster via the Spark
    Cassandra connector (a distributed count+min, so no driver-side
    materialization of the full mapping). Called as the final step of
    :func:`run_clustering_spark`, so a one-off run — including a re-run to
    (re)populate stats for an already-clustered keyspace — writes them. The
    incremental delta clustering requires these rows to pick the larger survivor
    on a merge. Returns the number of cluster rows written.
    """
    from pyspark.sql import functions as F

    cass_format = "org.apache.spark.sql.cassandra"
    members = (
        spark.read.format(cass_format)
        .options(table="fresh_cluster_addresses", keyspace=transformed_keyspace)
        .load()
    )
    stats = members.groupBy("cluster_id").agg(
        F.count(F.lit(1)).cast("bigint").alias("size"),
        F.min("address_id").alias("min_address_id"),
    )
    stats.persist()
    n_clusters = stats.count()
    (
        stats.select("cluster_id", "size", "min_address_id")
        .write.format(cass_format)
        .options(table="fresh_cluster_stats", keyspace=transformed_keyspace)
        .mode("append")
        .save()
    )
    stats.unpersist()
    return n_clusters


# --------------------------------------------------------------------------- #
# Weekly cluster-stat recompute (pure DataFrame transforms + Cassandra I/O)
# --------------------------------------------------------------------------- #
# These reproduce the per-cluster stats the legacy `cluster` table carried, by
# aggregating the address-level tables up through the fresh address->cluster
# membership.  They are pure (no Cassandra I/O) so they unit-test with synthetic
# frames.  ``total_received_adj`` / ``total_spent_adj`` are the cluster totals
# adjusted to exclude intra-cluster flows — the summed ``estimated_value`` over
# external (inter-cluster) relations only.  No in-repo path ever produced them
# (only the Scala full-transform did), so validate against a real keyspace before
# REST reads them.


def _currency_sum_by(df, group_col: str, struct_col: str, out_name: str):
    """Group-wise element-wise sum of a ``currency`` struct (value + fiat_values).

    ``struct_col`` is ``struct<value: bigint, fiat_values: array<float>>``. The
    value is summed; the fiat array is summed element-wise (positionally, so all
    members must share the keyspace's fiat-currency order, which they do — it is
    fixed per keyspace).  Returns ``group_col`` + an ``out_name`` currency struct.
    """
    from pyspark.sql import functions as F

    value_sum = df.groupBy(group_col).agg(
        F.sum(F.col(f"{struct_col}.value")).cast("bigint").alias("_v")
    )
    fiat = (
        df.select(
            group_col,
            F.posexplode(F.col(f"{struct_col}.fiat_values")).alias("_pos", "_fv"),
        )
        .groupBy(group_col, "_pos")
        .agg(F.sum("_fv").alias("_s"))
        .groupBy(group_col)
        .agg(F.sort_array(F.collect_list(F.struct("_pos", "_s"))).alias("_arr"))
        .select(
            group_col,
            F.transform("_arr", lambda x: x["_s"].cast("float")).alias("_fiat"),
        )
    )
    return value_sum.join(fiat, group_col, "left").select(
        group_col,
        F.struct(
            F.col("_v").alias("value"),
            F.coalesce(F.col("_fiat"), F.array().cast("array<float>")).alias(
                "fiat_values"
            ),
        ).alias(out_name),
    )


def cluster_additive_stats(members_df, address_df):
    """Per-cluster stats that are pure aggregates of member addresses.

    ``members_df`` = ``fresh_address_cluster`` (``address_id``, ``cluster_id``);
    ``address_df`` = ``address`` (``address_id``, ``total_received``,
    ``total_spent``, ``first_tx_id``, ``last_tx_id``).  Returns one row per
    ``cluster_id`` with ``size``, ``min_address_id`` (the canonical id),
    ``first_tx_id`` (min), ``last_tx_id`` (max), ``total_received`` /
    ``total_spent`` (element-wise currency sums).

    Left-joins ``address`` onto membership so ``size`` counts every member even
    if its ``address`` row is missing (an orphan membership row), matching the
    membership-only count of :func:`backfill_fresh_cluster_stats`; such a member
    contributes nothing to the value/tx aggregates (its columns are null).
    """
    from pyspark.sql import functions as F

    joined = members_df.join(address_df, "address_id", "left")
    base = joined.groupBy("cluster_id").agg(
        F.count(F.lit(1)).cast("bigint").alias("size"),
        F.min("address_id").cast("int").alias("min_address_id"),
        F.min("first_tx_id").cast("bigint").alias("first_tx_id"),
        F.max("last_tx_id").cast("bigint").alias("last_tx_id"),
    )
    tr = _currency_sum_by(joined, "cluster_id", "total_received", "total_received")
    ts = _currency_sum_by(joined, "cluster_id", "total_spent", "total_spent")
    return base.join(tr, "cluster_id", "left").join(ts, "cluster_id", "left")


def cluster_relation_stats(members_df, in_rel_df, out_rel_df):
    """Per-cluster degrees, tx-counts and adjusted totals from address relations
    mapped to clusters.

    Maps each address relation's endpoints through ``members_df`` (an address
    absent from membership is a singleton cluster = its own id), drops
    intra-cluster (self) edges, then per cluster:
      * ``in_degree`` / ``out_degree`` = distinct neighbouring clusters;
      * ``no_incoming_txs`` / ``no_outgoing_txs`` = summed ``no_transactions``
        over the external edges;
      * ``total_received_adj`` / ``total_spent_adj`` = summed ``estimated_value``
        over the external edges — the cluster total adjusted to exclude
        intra-cluster flows (only value crossing the cluster boundary), the
        canonical GraphSense "adjusted" totals.

    This mirrors the legacy delta aggregation (``to_cluster_delta`` +
    ``compress``): address relations summed up to cluster level. Self-edges are
    excluded so a cluster is not its own neighbour. (Exact parity with the old
    `cluster` table on self-edge / multi-member-per-tx counting must be validated
    against a real keyspace before REST reads these — see migration notes.)
    """
    from pyspark.sql import functions as F

    def to_cluster(rel_df, addr_col, cl_col):
        m = members_df.select(
            F.col("address_id").alias(addr_col),
            F.col("cluster_id").alias(cl_col),
        )
        return rel_df.join(m, addr_col, "left").withColumn(
            cl_col, F.coalesce(F.col(cl_col), F.col(addr_col))
        )

    out_mapped = to_cluster(
        to_cluster(out_rel_df, "src_address_id", "src_cluster"),
        "dst_address_id",
        "dst_cluster",
    ).filter(F.col("src_cluster") != F.col("dst_cluster"))
    out_stats = (
        out_mapped.groupBy("src_cluster")
        .agg(
            F.countDistinct("dst_cluster").cast("int").alias("out_degree"),
            F.sum("no_transactions").cast("int").alias("no_outgoing_txs"),
        )
        .select(
            F.col("src_cluster").alias("cluster_id"), "out_degree", "no_outgoing_txs"
        )
    )
    out_adj = _currency_sum_by(
        out_mapped, "src_cluster", "estimated_value", "total_spent_adj"
    ).select(F.col("src_cluster").alias("cluster_id"), "total_spent_adj")
    out_stats = out_stats.join(out_adj, "cluster_id", "outer")

    in_mapped = to_cluster(
        to_cluster(in_rel_df, "dst_address_id", "dst_cluster"),
        "src_address_id",
        "src_cluster",
    ).filter(F.col("src_cluster") != F.col("dst_cluster"))
    in_stats = (
        in_mapped.groupBy("dst_cluster")
        .agg(
            F.countDistinct("src_cluster").cast("int").alias("in_degree"),
            F.sum("no_transactions").cast("int").alias("no_incoming_txs"),
        )
        .select(
            F.col("dst_cluster").alias("cluster_id"), "in_degree", "no_incoming_txs"
        )
    )
    in_adj = _currency_sum_by(
        in_mapped, "dst_cluster", "estimated_value", "total_received_adj"
    ).select(F.col("dst_cluster").alias("cluster_id"), "total_received_adj")
    in_stats = in_stats.join(in_adj, "cluster_id", "outer")

    return in_stats.join(out_stats, "cluster_id", "outer")


def compute_fresh_cluster_stats(members_df, address_df, in_rel_df, out_rel_df):
    """Full per-cluster stat frame: additive stats + relation-derived degrees.

    Left-joins :func:`cluster_relation_stats` onto :func:`cluster_additive_stats`
    (additive carries every cluster; degree/tx-counts default to 0 and
    ``total_*_adj`` to a zero currency for clusters with no external edges).
    """
    from pyspark.sql import functions as F

    base = cluster_additive_stats(members_df, address_df)
    rels = cluster_relation_stats(members_df, in_rel_df, out_rel_df)
    zero_currency = F.struct(
        F.lit(0).cast("bigint").alias("value"),
        F.array().cast("array<float>").alias("fiat_values"),
    )
    return (
        base.join(rels, "cluster_id", "left")
        .fillna(
            0,
            subset=["in_degree", "out_degree", "no_incoming_txs", "no_outgoing_txs"],
        )
        .withColumn(
            "total_received_adj",
            F.coalesce(F.col("total_received_adj"), zero_currency),
        )
        .withColumn(
            "total_spent_adj", F.coalesce(F.col("total_spent_adj"), zero_currency)
        )
    )


def recompute_fresh_cluster_stats(spark, transformed_keyspace: str) -> int:
    """Recompute the full ``fresh_cluster_stats`` from the address-level tables.

    Reads ``fresh_address_cluster`` (membership), ``address`` (per-address stats),
    and ``address_incoming/outgoing_relations`` via the Spark Cassandra connector,
    aggregates with :func:`compute_fresh_cluster_stats`, and writes every well-
    defined column back to ``fresh_cluster_stats`` (append/upsert; the caller
    truncates first under the keyspace lock for a clean rebuild). ``size`` and
    ``min_address_id`` are recomputed here too so the row is whole. Returns the
    number of cluster rows written.

    Does NOT relabel ``fresh_address_cluster`` — membership is already written
    ``min(address_id)``-labeled by the one-off (and the incremental path), so the
    stored ``cluster_id`` equals ``min_address_id``; this only refreshes the stat
    columns.
    """

    cass_format = "org.apache.spark.sql.cassandra"

    def read(table):
        return (
            spark.read.format(cass_format)
            .options(table=table, keyspace=transformed_keyspace)
            .load()
        )

    members = read("fresh_address_cluster").select("address_id", "cluster_id")
    address = read("address").select(
        "address_id", "total_received", "total_spent", "first_tx_id", "last_tx_id"
    )
    in_rel = read("address_incoming_relations").select(
        "dst_address_id", "src_address_id", "no_transactions", "estimated_value"
    )
    out_rel = read("address_outgoing_relations").select(
        "src_address_id", "dst_address_id", "no_transactions", "estimated_value"
    )

    stats = compute_fresh_cluster_stats(members, address, in_rel, out_rel).select(
        "cluster_id",
        "size",
        "min_address_id",
        "no_incoming_txs",
        "no_outgoing_txs",
        "in_degree",
        "out_degree",
        "first_tx_id",
        "last_tx_id",
        "total_received",
        "total_spent",
        "total_received_adj",
        "total_spent_adj",
    )
    stats.persist()
    n_clusters = stats.count()
    logger.info(f"Recomputed fresh_cluster_stats for {n_clusters:,} clusters; writing")
    (
        stats.write.format(cass_format)
        .options(table="fresh_cluster_stats", keyspace=transformed_keyspace)
        .mode("append")
        .save()
    )
    stats.unpersist()
    return n_clusters


def stream_spark_column_as_arrow_ipc(df, column: str, arrow_type=None):
    """Stream one column of a Spark DataFrame to the driver one partition at a
    time as Arrow IPC blobs.

    Each executor serialises its partition's ``column`` into a single Arrow IPC
    stream (``rdd.mapPartitions`` + ``pyarrow.ipc``); ``toLocalIterator(
    prefetchPartitions=True)`` pulls one blob per partition and overlaps the next
    partition's compute+ship with the caller consuming the current one. This is
    far faster than ``DataFrame.toLocalIterator`` (py4j, row-by-row) and bounds the
    driver transfer to ~one partition, so it never trips
    ``spark.driver.maxResultSize``. Reusable for any single-column Spark→driver
    Arrow hand-off; the caller decodes each blob with ``pyarrow.ipc.open_stream``.

    For best throughput the caller should ``persist()`` + ``count()`` ``df`` first
    so the scan runs once with full cluster parallelism and the per-partition pulls
    are cache reads. ``arrow_type`` defaults to ``list<uint32>`` (the clustering
    edge-set shape) and is built executor-side when omitted.

    Yields ``(partition_index, num_partitions, ipc_blob_bytes, wait_seconds)`` —
    ``wait_seconds`` is the time blocked pulling that partition (Spark compute +
    executor→driver ship).
    """
    element_type = arrow_type

    def _partition_to_ipc(rows):
        # Runs on the executor — import locally rather than relying on the closure
        # capturing the driver's `pa` module reference.
        import pyarrow as pa

        vals = [row[column] for row in rows]
        if not vals:
            return iter(())
        el_type = element_type if element_type is not None else pa.list_(pa.uint32())
        batch = pa.record_batch({column: pa.array(vals, type=el_type)})
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, batch.schema) as writer:
            writer.write_batch(batch)
        return iter([sink.getvalue().to_pybytes()])

    num_parts = df.rdd.getNumPartitions()
    blobs = df.rdd.mapPartitions(_partition_to_ipc)
    blob_iter = blobs.toLocalIterator(prefetchPartitions=True)
    idx = 0
    while True:
        w0 = time.perf_counter()
        try:
            blob = next(blob_iter)
        except StopIteration:
            break
        wait_s = time.perf_counter() - w0
        yield idx, num_parts, blob, wait_s
        idx += 1


def _read_edges_into_unionfind(
    spark,
    c,
    raw_keyspace: str,
    transformed_keyspace: str,
    end_block: Optional[int],
    read_partitions: int,
    feed_batch_size: int,
) -> Dict[str, float]:
    """PHASE 1: bulk-read the multi-input edge sets and feed them to the in-process
    Rust Union-Find ``c``, streaming one Spark partition at a time as Arrow IPC.

    Returns a timing dict (``denom``, ``spark_side``, ``feed_total``, ``t_deser``,
    ``t_rust``, ``total_multi_input``) for the run summary. Every partition is
    wall-clock instrumented at INFO: ``wait(spark)`` (blocked pulling the partition
    = Spark compute + executor→driver ship) vs ``feed`` (driver-side Arrow→Rust
    ingest + union-find) — the one split local benchmarks can't settle.
    """
    import pyarrow as pa

    cass_format = "org.apache.spark.sql.cassandra"

    # read_partitions only coalesces the FINAL edge-set DataFrame for bounded
    # streaming; it deliberately does NOT cap spark.sql.shuffle.partitions. The
    # address-resolution join is against the full address_ids table (hundreds of
    # millions of rows) and needs the session's full shuffle parallelism (Spark
    # default 200 + AQE coalescing) — capping it here would create a few huge join
    # partitions that spill. Tune join parallelism via spark_config instead.
    tx = (
        spark.read.format(cass_format)
        .options(table="transaction", keyspace=raw_keyspace)
        .load()
    )
    address_ids = (
        spark.read.format(cass_format)
        .options(table="address_ids_by_address_prefix", keyspace=transformed_keyspace)
        .load()
    )
    edge_df = multi_input_address_id_sets(tx, address_ids, end_block=end_block)
    if read_partitions:
        edge_df = edge_df.coalesce(read_partitions)

    # Materialize the scan+join+groupBy ONCE into the Spark cache with full cluster
    # parallelism (persist + count) so the per-partition pulls below read the cache
    # instead of driving the final reduce one partition at a time.
    mat_start = time.perf_counter()
    edge_df = edge_df.persist()
    total_known = edge_df.count()
    mat_secs = time.perf_counter() - mat_start
    num_parts = read_partitions if read_partitions else edge_df.rdd.getNumPartitions()
    logger.info(
        f"── PHASE 1/3: bulk read → multi-input edge sets "
        f"(streaming {num_parts} partitions) ──"
    )
    logger.info(
        f"  [read] persist+count: materialized {total_known:,} edge sets in "
        f"{mat_secs:.1f}s with full-cluster parallelism — the per-partition wait "
        f"below is then just cache reads."
    )

    total_multi_input = 0
    t_wait = 0.0  # blocked pulling partitions: Spark compute + executor->driver ship
    t_deser = 0.0  # Arrow IPC parse on the driver
    t_rust = 0.0  # Rust Arrow ingest + union-find (process_transactions_arrow)
    first_wait = 0.0  # partition 1 warm-up, excluded from the ETA extrapolation
    read_start = time.perf_counter()
    try:
        for idx, _np, blob, wait_s in stream_spark_column_as_arrow_ipc(edge_df, "ids"):
            t_wait += wait_s
            if idx == 0:
                first_wait = wait_s

            d0 = time.perf_counter()
            batches = list(pa.ipc.open_stream(pa.py_buffer(blob)))
            deser_s = time.perf_counter() - d0
            t_deser += deser_s

            part_rows = 0
            part_rust = 0.0
            for record_batch in batches:
                ids_col = record_batch.column(0)
                for off in range(0, len(ids_col), feed_batch_size):
                    chunk = ids_col.slice(off, feed_batch_size)
                    n = len(chunk)
                    if n == 0:
                        continue
                    # Hand the Arrow list<uint32> buffer straight to Rust: it reads
                    # the offsets+values in place (zero Python objects).
                    r0 = time.perf_counter()
                    c.process_transactions_arrow(chunk)
                    part_rust += time.perf_counter() - r0
                    part_rows += n
            t_rust += part_rust
            total_multi_input += part_rows

            # ETA from steady-state partitions only — partition 1 carries warm-up.
            done = idx + 1
            eta = ""
            if done >= 2 and num_parts:
                per_part = (time.perf_counter() - read_start - first_wait) / idx
                eta = f" | ETA ~{per_part * max(0, num_parts - done) / 60:.1f}m"
            logger.info(
                f"  [read] partition {done}/{num_parts}: {part_rows:,} rows | "
                f"wait(spark)={wait_s:.1f}s feed={deser_s + part_rust:.1f}s "
                f"(deser={deser_s:.2f} rust(arrow)={part_rust:.2f}) | "
                f"cum {total_multi_input:,}{eta}"
            )
    finally:
        edge_df.unpersist()

    read_secs = time.perf_counter() - read_start
    spark_side = t_wait + mat_secs
    feed_total = t_deser + t_rust
    denom = read_secs + mat_secs
    logger.info(
        f"  [read] DONE: {num_parts} partitions, {total_multi_input:,} edge sets in "
        f"{denom:.1f}s — Spark delivery {spark_side:.1f}s | driver feed "
        f"{feed_total:.1f}s (deser {t_deser:.1f}s + Rust union-find {t_rust:.1f}s)"
    )
    return {
        "denom": denom,
        "spark_side": spark_side,
        "feed_total": feed_total,
        "t_deser": t_deser,
        "t_rust": t_rust,
        "total_multi_input": total_multi_input,
    }


def _mapping_to_write_arrays(c, max_address_id: int, skip_singletons: bool):
    """PHASE 2: pull the address→cluster mapping out of Rust, relabel each cluster
    to its canonical ``min(address_id)``, and filter to the rows to write. Returns
    ``(aid_w, cid_w, write_rows, skipped, map_secs)``.

    The Rust union-find roots a component by rank, so ``get_mapping`` labels each
    cluster with an arbitrary member id. We relabel every cluster to the minimum
    address_id it contains, so the stored ``cluster_id`` is the canonical root
    ``== min(address_id) == fresh_cluster_stats.min_address_id`` — the same
    survivor rule the incremental delta path elects on a merge, so the one-off
    and incremental labels are identical (the system-wide invariant downstream
    consumers rely on).

    Drops the coinbase placeholder (address_id 0). With ``skip_singletons`` also
    drops SIZE-1 clusters — a singleton is size 1, NOT merely
    ``cluster_id == address_id`` (a real cluster's min-root satisfies that too):
    keep addresses whose cluster_id has a non-root member.
    """
    import numpy as np
    import pandas as pd

    logger.info("── PHASE 2/3: materialize address→cluster mapping from Rust ──")
    map_start = time.perf_counter()
    mapping_batch = c.get_mapping()
    total_rows = mapping_batch.num_rows
    map_secs = time.perf_counter() - map_start
    logger.info(f"  [map] get_mapping() → {total_rows:,} rows in {map_secs:.1f}s")

    # numpy views over the Arrow mapping (zero-copy; the mapping has no nulls).
    aid_all = mapping_batch.column("address_id").to_numpy()
    cid_all = mapping_batch.column("cluster_id").to_numpy()

    # Relabel each component to the minimum address_id it contains (canonical
    # cluster_id). uf_rush roots by rank, so cid_all holds an arbitrary member; a
    # per-root group-min (pandas, C-optimized) maps it to min(address_id). The
    # placeholder's own component (id 0) maps to 0 and is dropped below; real
    # components never contain address 0, so their min is a real (>0) id.
    relabel_start = time.perf_counter()
    group_min = pd.Series(aid_all).groupby(cid_all, sort=False).min()
    root_to_min = np.zeros(max_address_id + 1, dtype=cid_all.dtype)
    root_to_min[group_min.index.to_numpy()] = group_min.to_numpy()
    cid_all = root_to_min[cid_all]
    logger.info(
        f"  [map] relabelled to min(address_id) cluster_id in "
        f"{time.perf_counter() - relabel_start:.1f}s"
    )

    keep = aid_all != 0
    if skip_singletons:
        non_root = aid_all != cid_all
        nontrivial = np.zeros(max_address_id + 1, dtype=bool)
        nontrivial[cid_all[non_root]] = True
        keep &= nontrivial[cid_all]
    aid_w = aid_all[keep]
    cid_w = cid_all[keep]
    write_rows = int(aid_w.shape[0])
    skipped = int(aid_all.shape[0] - write_rows)
    return aid_w, cid_w, write_rows, skipped, map_secs


def _write_mapping_to_cassandra(
    spark,
    transformed_keyspace: str,
    aid_w,
    cid_w,
    write_rows: int,
    skipped: int,
    write_chunk: int,
    skip_singletons: bool,
):
    """PHASE 3: bulk-write the address→cluster mapping to
    ``fresh_address_cluster`` / ``fresh_cluster_addresses`` in ``write_chunk``
    slices. Returns ``(write_secs, t_fa, t_fc, rows_written)``.

    ``createDataFrame`` is fed int64 numpy for the Arrow fast path; uint32/int32
    miss it and fall back to a ~17x slower row-at-a-time path that OOMs the driver.
    Spark casts down to the tables' int32 columns.
    """
    import numpy as np
    import pandas as pd
    from pyspark.sql import functions as F

    cass_format = "org.apache.spark.sql.cassandra"
    logger.info(
        f"── PHASE 3/3: bulk write {write_rows:,} mappings to Cassandra "
        f"(slices of {write_chunk:,}; skipped {skipped:,} "
        f"{'singletons + placeholder' if skip_singletons else 'placeholder'}) ──"
    )

    def _write_slice(slice_df, table, cols):
        (
            slice_df.select(*cols)
            .write.format(cass_format)
            .options(table=table, keyspace=transformed_keyspace)
            .mode("append")
            .save()
        )

    t_build = 0.0
    t_fa = 0.0
    t_fc = 0.0
    rows_written = 0
    write_start = time.perf_counter()
    slice_idx = 0
    for offset in range(0, write_rows, write_chunk):
        length = min(write_chunk, write_rows - offset)

        bld0 = time.perf_counter()
        pdf = pd.DataFrame(
            {
                "address_id": aid_w[offset : offset + length].astype(np.int64),
                "cluster_id": cid_w[offset : offset + length].astype(np.int64),
            }
        )
        sdf = (
            spark.createDataFrame(pdf)
            .withColumn("address_id", F.col("address_id").cast("int"))
            .withColumn("cluster_id", F.col("cluster_id").cast("int"))
        )
        sdf.persist()
        slice_rows = sdf.count()  # build once so both writes read cache; timed here
        build_s = time.perf_counter() - bld0
        t_build += build_s

        f0 = time.perf_counter()
        _write_slice(sdf, "fresh_address_cluster", ["address_id", "cluster_id"])
        fa_s = time.perf_counter() - f0
        t_fa += fa_s

        g0 = time.perf_counter()
        _write_slice(sdf, "fresh_cluster_addresses", ["cluster_id", "address_id"])
        fc_s = time.perf_counter() - g0
        t_fc += fc_s

        sdf.unpersist()
        rows_written += slice_rows
        slice_idx += 1
        logger.info(
            f"  [write] slice {slice_idx}: {offset + length:,}/{write_rows:,} "
            f"({100 * (offset + length) / write_rows:.1f}%) | "
            f"build+count={build_s:.1f}s "
            f"fresh_address_cluster={fa_s:.1f}s fresh_cluster_addresses={fc_s:.1f}s"
        )

    write_secs = time.perf_counter() - write_start
    logger.info(
        f"  [write] DONE: {rows_written:,} rows in {write_secs:.1f}s — "
        f"build+count {t_build:.1f}s | "
        f"fresh_address_cluster {t_fa:.1f}s | fresh_cluster_addresses {t_fc:.1f}s"
    )
    return write_secs, t_fa, t_fc, rows_written


def run_clustering_spark(
    spark,
    raw_keyspace: str,
    transformed_keyspace: str,
    max_address_id: int,
    feed_batch_size: int = DEFAULT_FEED_BATCH_SIZE,
    write_chunk: int = DEFAULT_SPARK_WRITE_CHUNK,
    read_partitions: int = DEFAULT_READ_PARTITIONS,
    skip_singletons: bool = True,
    end_block: Optional[int] = None,
):
    """Full one-off UTXO clustering with PySpark bulk read and bulk write.

    This path:

      * **bulk-reads** the entire ``raw.transaction`` and
        ``transformed.address_ids_by_address_prefix`` tables via the Spark
        Cassandra connector (parallel token-range scans across the cluster),
        deriving, for every multi-input transaction, the set of input
        ``address_id`` s — the order-independent edge set the multi-input
        clustering heuristic needs;
      * streams those edge sets to the driver **one Spark partition at a time**
        as **Arrow IPC** blobs (each executor serializes its partition with
        pyarrow; ``rdd.toLocalIterator()`` pulls one blob per partition), so
        each transfer is bounded (~total/read_partitions) and never trips
        ``spark.driver.maxResultSize`` — far faster than
        ``DataFrame.toLocalIterator`` (py4j row-by-row, ~50k rows/s) and using
        only public APIs. The edge sets feed the in-process Rust Union-Find
        (``gs_clustering``) as zero-copy Arrow buffers
        (``process_transactions_arrow``, no Python materialization) in
        ``feed_batch_size`` slices;
      * **bulk-writes** the resulting ``address_id -> cluster_id`` mapping back
        to ``fresh_address_cluster`` / ``fresh_cluster_addresses`` in
        ``write_chunk``-sized slices via the Spark Cassandra connector, then
        aggregates ``fresh_cluster_stats`` (size + min_address_id per cluster)
        that the incremental delta clustering needs to elect a merge survivor.

    ``read_partitions`` sets the number of per-partition Arrow blobs streamed
    to the driver (the final edge-set DataFrame is coalesced to it): more
    partitions => smaller per-blob driver memory / result size (raise it if a
    partition exceeds ``spark.driver.maxResultSize`` or executor memory is
    tight on big chains); fewer => less per-partition overhead. It does NOT
    control the resolution-join parallelism (that is
    ``spark.sql.shuffle.partitions``).

    The read+join+groupBy is materialized ONCE into the Spark cache with full
    cluster parallelism (``persist()`` + ``count()``) before streaming, so
    ``toLocalIterator`` reads the cache instead of driving the final reduce one
    partition at a time.

    ``skip_singletons`` (default True) writes only addresses that belong to a
    multi-address cluster; an address absent from ``fresh_address_cluster`` is
    taken to have no cluster (it is its own). This drops the majority of rows on
    most chains. A singleton is a cluster of SIZE 1 — not merely
    ``cluster_id == address_id`` (a real cluster's root also satisfies that and
    is kept).

    The whole timed region emits an extensive per-phase / per-partition /
    per-write-slice breakdown at INFO so a real run states exactly where the
    wall-clock goes (Spark read+join+ship vs Rust Arrow ingest+union-find
    vs each Cassandra write).

    The write feeds ``createDataFrame`` int64 numpy (the Arrow fast path); Rust's
    native uint32 is rejected by Spark Arrow and falls back to a row-at-a-time
    driver path that is ~17x slower AND pickles every row into oversized task
    buffers (driver-heap OOM on large chains).

    By default clusters the whole transaction table (full clustering).
    ``end_block`` caps the read to transactions with ``block_id <= end_block``,
    clustering the chain as of that height (genesis-to-``end_block``); there is no
    start bound because clustering is transitive over the full history, so a
    sub-range start would not yield meaningful clusters. ``max_address_id`` sizes
    the Union-Find and must come from ``transformed.summary_statistics.no_addresses``
    (dense ``[1, no_addresses]`` invariant) — addresses first seen after
    ``end_block`` simply remain singletons.
    """
    from gs_clustering import Clustering

    logger.info(
        f"max_address_id={max_address_id:,}; bulk-reading multi-input "
        f"transactions from {raw_keyspace}.transaction"
        + (f" up to block {end_block:,}" if end_block is not None else "")
    )
    c = Clustering(max_address_id=max_address_id)
    overall_start = time.perf_counter()

    # PHASE 1: bulk read → multi-input edge sets → Rust Union-Find.
    read = _read_edges_into_unionfind(
        spark,
        c,
        raw_keyspace,
        transformed_keyspace,
        end_block,
        read_partitions,
        feed_batch_size,
    )

    # PHASE 2: pull the address→cluster mapping out of Rust (Arrow fast path needs
    # this conf for the createDataFrame in PHASE 3).
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    aid_w, cid_w, write_rows, skipped, map_secs = _mapping_to_write_arrays(
        c, max_address_id, skip_singletons
    )

    # PHASE 3: bulk write to fresh_address_cluster / fresh_cluster_addresses.
    write_secs, t_fa, t_fc, _rows = _write_mapping_to_cassandra(
        spark,
        transformed_keyspace,
        aid_w,
        cid_w,
        write_rows,
        skipped,
        write_chunk,
        skip_singletons,
    )

    # PHASE 4: cluster stats (size + min_address_id per cluster), aggregated from
    # the just-written fresh_cluster_addresses (distributed count+min) rather than
    # the driver-side mapping, to keep driver memory bound. Needed by the
    # incremental delta clustering to pick the larger survivor on a merge.
    stats_start = time.perf_counter()
    n_stats = backfill_fresh_cluster_stats(spark, transformed_keyspace)
    stats_secs = time.perf_counter() - stats_start
    logger.info(
        f"  [stats] fresh_cluster_stats: {n_stats:,} clusters in {stats_secs:.1f}s"
    )

    total_secs = time.perf_counter() - overall_start
    logger.info("══════════ CLUSTERING COMPLETE ══════════")
    logger.info(
        f"  read {read['denom']:.1f}s "
        f"[spark {read['spark_side']:.1f} | feed {read['feed_total']:.1f}]  |  "
        f"get_mapping {map_secs:.1f}s  |  write {write_secs:.1f}s "
        f"[fresh_address_cluster {t_fa:.1f} | fresh_cluster_addresses {t_fc:.1f}]"
    )
    logger.info(f"  TOTAL {total_secs:.1f}s ({total_secs / 60:.1f} min)")
