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
from typing import Dict, Iterator, List, Set

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

        tx_input_addr_lists: List[List[str]] = []
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
                if row.coinbase or not row.inputs:
                    continue
                addrs: Set[str] = set()
                for inp in row.inputs:
                    if inp.address:
                        addrs.update(inp.address)
                if len(addrs) >= 2:
                    addr_list = list(addrs)
                    tx_input_addr_lists.append(addr_list)
                    unique_addresses.update(addr_list)

        if not tx_input_addr_lists:
            continue

        addr_to_id: Dict[str, int] = {}
        for adr, exec_result in tdb.get_address_id_async_batch(list(unique_addresses)):
            row = exec_result.result_or_exc.one()
            if row is not None:
                addr_to_id[adr] = row.address_id

        tx_input_ids: List[List[int]] = []
        for addr_list in tx_input_addr_lists:
            ids = {addr_to_id[a] for a in addr_list if a in addr_to_id}
            if len(ids) >= 2:
                tx_input_ids.append(list(ids))

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


def multi_input_address_id_sets(tx_df, address_ids_df):
    """Derive each multi-input transaction's distinct input ``address_id`` set.

    Pure DataFrame transform (no Cassandra I/O) so it can be unit-tested with
    synthetic frames. Mirrors the single-driver semantics in
    :func:`iter_multi_input_tx_inputs`:

      * ``tx_df`` has the ``raw.transaction`` shape: ``tx_id``, ``coinbase``,
        and ``inputs`` = ``array<struct<address: array<string>, ...>>``;
      * coinbase transactions and null addresses are dropped;
      * input addresses are taken as a DISTINCT set per transaction, resolved
        to ``address_id`` via ``address_ids_df`` (``address``, ``address_id``);
      * only transactions with >= 2 distinct resolved ``address_id`` s survive.

    Returns a DataFrame with a single ``ids`` column (``array<address_id>``),
    each row an order-independent edge set for the Union-Find.
    """
    from pyspark.sql import functions as F

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


def run_clustering_spark(
    spark,
    raw_keyspace: str,
    transformed_keyspace: str,
    max_address_id: int,
    feed_batch_size: int = DEFAULT_FEED_BATCH_SIZE,
    write_chunk: int = DEFAULT_SPARK_WRITE_CHUNK,
    read_partitions: int = DEFAULT_READ_PARTITIONS,
    skip_singletons: bool = True,
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

    Clusters the whole transaction table (full clustering); block sub-ranges are
    not applied here. ``max_address_id`` sizes the Union-Find and must come from
    ``transformed.summary_statistics.no_addresses`` (dense ``[1, no_addresses]``
    invariant).
    """
    from gs_clustering import Clustering
    from pyspark.sql import functions as F

    cass_format = "org.apache.spark.sql.cassandra"

    # ---- BULK READ: multi-input transactions -> input address_id sets ----
    # read_partitions only coalesces the FINAL edge-set DataFrame for bounded
    # streaming (below); it deliberately does NOT cap spark.sql.shuffle.partitions.
    # The address-resolution join is against the full address_ids table
    # (hundreds of millions of rows) and needs the session's full shuffle
    # parallelism (Spark default 200 + AQE coalescing) — capping it to a small
    # number here would create a few huge join partitions that spill. Tune join
    # parallelism via spark_config (spark.sql.shuffle.partitions) if needed.
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
    tx_input_ids = multi_input_address_id_sets(tx, address_ids)
    if read_partitions:
        tx_input_ids = tx_input_ids.coalesce(read_partitions)

    logger.info(
        f"max_address_id={max_address_id:,}; bulk-reading multi-input "
        f"transactions from {raw_keyspace}.transaction"
    )
    c = Clustering(max_address_id=max_address_id)

    # ===================== TIMED REGION: read -> map -> write =================
    # Every phase below is wall-clock instrumented at INFO so a real run reports
    # exactly where time goes. The crucial split is per partition: `wait(spark)`
    # = time blocked in toLocalIterator (Spark compute + executor->driver ship)
    # vs `feed` = driver-side Arrow→Rust ingest + union-find. That split is the
    # one thing local benchmarks can't settle, and it answers "what is slow?".
    overall_start = time.perf_counter()

    import pyarrow as pa

    def _partition_to_ipc(rows):
        # Runs on the executor — import locally rather than relying on the
        # closure capturing the driver's `pa` module reference.
        import pyarrow as pa

        ids = [row["ids"] for row in rows]
        if not ids:
            return iter(())
        # uint32 so the driver can hand the Arrow buffer straight to Rust's
        # process_transactions_arrow (it reads a UInt32Array in place).
        batch = pa.record_batch({"ids": pa.array(ids, type=pa.list_(pa.uint32()))})
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, batch.schema) as writer:
            writer.write_batch(batch)
        return iter([sink.getvalue().to_pybytes()])

    # coalesce(read_partitions) makes this exactly read_partitions; otherwise ask
    # the RDD (structural, no Spark job).
    num_parts = (
        read_partitions if read_partitions else tx_input_ids.rdd.getNumPartitions()
    )
    logger.info(
        f"── PHASE 1/3: bulk read → multi-input edge sets "
        f"(streaming {num_parts} partitions) ──"
    )

    # Materialize the scan+join+groupBy ONCE into the Spark cache with full
    # cluster parallelism (persist + count) so toLocalIterator reads the cache
    # instead of driving the final reduce one partition at a time.
    mat_start = time.perf_counter()
    tx_input_ids = tx_input_ids.persist()
    total_known = tx_input_ids.count()
    mat_secs = time.perf_counter() - mat_start
    logger.info(
        f"  [read] persist+count: materialized {total_known:,} edge sets in "
        f"{mat_secs:.1f}s with full-cluster parallelism — the per-partition wait "
        f"below is then just cache reads."
    )

    edge_blobs = tx_input_ids.rdd.mapPartitions(_partition_to_ipc)

    # Per-phase wall-clock accumulators (seconds).
    total_multi_input = 0
    t_wait = 0.0  # blocked in toLocalIterator: Spark compute + executor->driver ship
    t_deser = 0.0  # Arrow IPC parse on the driver
    t_rust = 0.0  # Rust Arrow ingest + union-find (process_transactions_arrow)
    first_wait = 0.0  # partition 1 warm-up, excluded from the ETA extrapolation

    # prefetchPartitions=True overlaps the next partition's compute+ship with the
    # current blob's feed, hiding the otherwise serial executor->driver ship.
    read_start = time.perf_counter()
    edge_iter = edge_blobs.toLocalIterator(prefetchPartitions=True)
    part_idx = 0
    while True:
        w0 = time.perf_counter()
        try:
            blob = next(edge_iter)
        except StopIteration:
            break
        wait_s = time.perf_counter() - w0
        t_wait += wait_s
        if part_idx == 0:
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
                # the offsets+values in place (zero Python objects), replacing
                # the to_pylist() that dominated the feed at scale.
                r0 = time.perf_counter()
                c.process_transactions_arrow(chunk)
                dt = time.perf_counter() - r0
                part_rust += dt
                part_rows += n
                logger.debug(f"    flushed {n:,} txs | rust(arrow)={dt:.2f}s")

        t_rust += part_rust
        total_multi_input += part_rows
        feed_s = deser_s + part_rust

        # ETA from steady-state partitions only — partition 1 carries warm-up and
        # is excluded so it doesn't skew the estimate.
        done = part_idx + 1
        eta = ""
        if done >= 2 and num_parts:
            per_part = (time.perf_counter() - read_start - first_wait) / part_idx
            eta = f" | ETA ~{per_part * max(0, num_parts - done) / 60:.1f}m"
        logger.info(
            f"  [read] partition {done}/{num_parts}: {part_rows:,} rows | "
            f"wait(spark)={wait_s:.1f}s feed={feed_s:.1f}s "
            f"(deser={deser_s:.2f} rust(arrow)={part_rust:.2f}) | "
            f"cum {total_multi_input:,}{eta}"
        )
        part_idx += 1

    tx_input_ids.unpersist()

    read_secs = time.perf_counter() - read_start
    feed_total = t_deser + t_rust
    spark_side = t_wait + mat_secs
    denom = read_secs + mat_secs
    logger.info(
        f"  [read] DONE: {part_idx} partitions, {total_multi_input:,} edge sets in "
        f"{denom:.1f}s — Spark delivery {spark_side:.1f}s | driver feed "
        f"{feed_total:.1f}s (deser {t_deser:.1f}s + Rust union-find {t_rust:.1f}s)"
    )

    # ---- PHASE 2: address_id -> cluster_id mapping from Rust ----
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    logger.info("── PHASE 2/3: materialize address→cluster mapping from Rust ──")
    map_start = time.perf_counter()
    mapping_batch = c.get_mapping()
    total_rows = mapping_batch.num_rows
    map_secs = time.perf_counter() - map_start
    logger.info(f"  [map] get_mapping() → {total_rows:,} rows in {map_secs:.1f}s")

    # numpy views over the Arrow mapping (zero-copy; the mapping has no nulls).
    import numpy as np
    import pandas as pd

    aid_all = mapping_batch.column("address_id").to_numpy()
    cid_all = mapping_batch.column("cluster_id").to_numpy()

    # Drop coinbase placeholder (address_id 0); skip_singletons also drops size-1
    # clusters. A singleton is SIZE 1, NOT cluster_id == address_id (a cluster's
    # root satisfies that too): keep addresses whose cluster_id has a non-root member.
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

    # ---- PHASE 3: bulk write to Cassandra ----
    # createDataFrame is fed int64 numpy for the Arrow fast path; uint32/int32 miss
    # it and fall back to a ~17x slower row-at-a-time path that OOMs the driver.
    # Spark casts down to the tables' int32 columns below.
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

    # ---- PHASE 4: cluster stats (size + min_address_id per cluster) ----
    # Needed by the incremental delta clustering to pick the larger survivor on a
    # merge. Aggregated from the just-written fresh_cluster_addresses (distributed
    # count+min) rather than the driver-side mapping, to keep driver memory bound.
    stats_start = time.perf_counter()
    n_stats = backfill_fresh_cluster_stats(spark, transformed_keyspace)
    stats_secs = time.perf_counter() - stats_start
    logger.info(
        f"  [stats] fresh_cluster_stats: {n_stats:,} clusters in {stats_secs:.1f}s"
    )

    total_secs = time.perf_counter() - overall_start
    logger.info("══════════ CLUSTERING COMPLETE ══════════")
    logger.info(
        f"  read {denom:.1f}s [spark {spark_side:.1f} | feed {feed_total:.1f}]  |  "
        f"get_mapping {map_secs:.1f}s  |  write {write_secs:.1f}s "
        f"[fresh_address_cluster {t_fa:.1f} | fresh_cluster_addresses {t_fc:.1f}]"
    )
    logger.info(f"  TOTAL {total_secs:.1f}s ({total_secs / 60:.1f} min)")
