"""UTXO clustering I/O helpers and one-off batch clustering entrypoint.

The generator :func:`iter_multi_input_tx_inputs` streams the address-id lists
of multi-input transactions for a block range, and is shared by both the
one-off batch path (this module) and the delta-update incremental path
(``deltaupdate/update/utxo/update.py``).
"""

import logging
import time
from typing import Dict, Iterator, List, Set

logger = logging.getLogger(__name__)

DEFAULT_BLOCK_CHUNK_SIZE = 1_000
DEFAULT_CASSANDRA_CONCURRENCY = 100
DEFAULT_WRITE_CHUNK = 100_000


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


def run_clustering_one_off_from_cassandra(
    db,
    start_block: int,
    end_block: int,
    chunk_size: int = DEFAULT_BLOCK_CHUNK_SIZE,
    concurrency: int = DEFAULT_CASSANDRA_CONCURRENCY,
    write_chunk: int = DEFAULT_WRITE_CHUNK,
):
    """Run full one-off clustering from the raw Cassandra keyspace.

    Sizes the Union-Find from ``transformed.summary_statistics.no_addresses``
    (relies on the dense ``[1, no_addresses]`` invariant preserved by the
    Scala seeding and ``consume_address_id()`` in the delta updater), streams
    multi-input tx inputs via :func:`iter_multi_input_tx_inputs`, feeds each
    chunk to the Rust Union-Find, then streams the final mapping back to
    ``fresh_address_cluster`` / ``fresh_cluster_addresses`` in
    ``write_chunk``-sized slices.

    The transformed keyspace must already be seeded (Scala transformation or
    a prior run) so ``summary_statistics.no_addresses`` is populated.
    """
    from gs_clustering import Clustering

    rdb = db.raw
    tdb = db.transformed
    transformed_ks = tdb.get_keyspace()

    stats = tdb.get_summary_statistics()
    if stats is None or getattr(stats, "no_addresses", None) is None:
        raise RuntimeError(
            f"{transformed_ks}.summary_statistics.no_addresses is missing — "
            "the transformed keyspace must be seeded (Scala transformation "
            "or a prior run) before one-off clustering can run."
        )
    max_address_id = int(stats.no_addresses)
    logger.info(
        f"max_address_id={max_address_id:,} "
        f"(from {transformed_ks}.summary_statistics.no_addresses)"
    )

    c = Clustering(max_address_id=max_address_id)

    total_multi_input = 0
    feed_start = time.perf_counter()
    for tx_input_ids in iter_multi_input_tx_inputs(
        db, start_block, end_block, chunk_size=chunk_size, concurrency=concurrency
    ):
        c.process_transactions(tx_input_ids)
        total_multi_input += len(tx_input_ids)

    feed_secs = time.perf_counter() - feed_start
    logger.info(
        f"Fed {total_multi_input:,} multi-input txs to Rust in {feed_secs:.1f}s "
        f"for blocks [{start_block:,}-{end_block:,}]"
    )

    # get_mapping() still materializes a single RecordBatch sized
    # ~max_address_id; on BTC mainnet that's ~10 GB.  A future Rust
    # get_mapping_range(start, end) will let us stream it — for now the
    # writes below are streamed per write_chunk so at least the per-batch
    # memory stays bounded.
    logger.info("Generating final cluster mapping from Rust")
    mapping_batch = c.get_mapping()
    total_rows = mapping_batch.num_rows
    logger.info(f"Mapping has {total_rows:,} rows — streaming to Cassandra")

    fa_prep = rdb._db.get_prepared_statement(
        f"INSERT INTO {transformed_ks}.fresh_address_cluster "
        "(address_id, cluster_id) VALUES (?, ?)"
    )
    fc_prep = rdb._db.get_prepared_statement(
        f"INSERT INTO {transformed_ks}.fresh_cluster_addresses "
        "(cluster_id, address_id) VALUES (?, ?)"
    )

    aid_col = mapping_batch.column("address_id")
    cid_col = mapping_batch.column("cluster_id")

    write_start = time.perf_counter()
    for offset in range(0, total_rows, write_chunk):
        length = min(write_chunk, total_rows - offset)
        aids = aid_col.slice(offset, length).to_pylist()
        cids = cid_col.slice(offset, length).to_pylist()

        stmts = []
        for aid, cid in zip(aids, cids):
            if aid == 0:
                continue  # coinbase placeholder
            stmts.append(fa_prep.bind({"address_id": aid, "cluster_id": cid}))
            stmts.append(fc_prep.bind({"cluster_id": cid, "address_id": aid}))

        for success, result in rdb._db.execute_statements_async(
            stmts, concurrency=concurrency
        ):
            if not success:
                raise RuntimeError(f"clustering write failed: {result}")

        logger.info(
            f"  wrote {offset + length:,}/{total_rows:,} "
            f"({100 * (offset + length) / total_rows:.1f}%)"
        )

    write_secs = time.perf_counter() - write_start
    logger.info(
        f"Wrote {total_rows:,} mappings in {write_secs:.1f}s "
        f"(total runtime {time.perf_counter() - feed_start:.1f}s)"
    )


# Each batched tx is a tiny list of int32 address_ids (~150-300 B in Python
# incl. object overhead), so 2M txs is ~0.5 GB of driver memory. Larger
# batches just mean fewer Python->Rust process_transactions() crossings; push
# higher (e.g. 4-5M ≈ 1 GB) via --feed-batch-size if desired.
DEFAULT_FEED_BATCH_SIZE = 2_000_000
DEFAULT_SPARK_WRITE_CHUNK = 5_000_000


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


def run_clustering_spark(
    spark,
    raw_keyspace: str,
    transformed_keyspace: str,
    max_address_id: int,
    feed_batch_size: int = DEFAULT_FEED_BATCH_SIZE,
    write_chunk: int = DEFAULT_SPARK_WRITE_CHUNK,
):
    """Full one-off UTXO clustering with PySpark bulk read and bulk write.

    Unlike :func:`run_clustering_one_off_from_cassandra` (single-driver,
    block-chunked point/range reads through the CQL coordinator), this path:

      * **bulk-reads** the entire ``raw.transaction`` and
        ``transformed.address_ids_by_address_prefix`` tables via the Spark
        Cassandra connector (parallel token-range scans across the cluster),
        deriving, for every multi-input transaction, the set of input
        ``address_id`` s — this is the order-independent edge set the
        multi-input clustering heuristic needs;
      * feeds those edge sets to the in-process Rust Union-Find
        (``gs_clustering``) on the driver via ``toLocalIterator`` so driver
        memory stays bounded by ``feed_batch_size`` transactions at a time;
      * **bulk-writes** the resulting ``address_id -> cluster_id`` mapping back
        to ``fresh_address_cluster`` / ``fresh_cluster_addresses`` via the
        Spark Cassandra connector in ``write_chunk``-sized slices.

    Clusters the whole transaction table (full clustering); block sub-ranges are
    not applied here. ``max_address_id`` sizes the Union-Find and must come from
    ``transformed.summary_statistics.no_addresses`` (dense ``[1, no_addresses]``
    invariant), exactly as the single-driver path requires.
    """
    from gs_clustering import Clustering
    from pyspark.sql import functions as F

    cass_format = "org.apache.spark.sql.cassandra"

    # ---- BULK READ: multi-input transactions -> input address_id sets ----
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

    logger.info(
        f"max_address_id={max_address_id:,}; bulk-reading multi-input "
        f"transactions from {raw_keyspace}.transaction"
    )
    c = Clustering(max_address_id=max_address_id)

    feed_start = time.perf_counter()
    total_multi_input = 0
    batch: List[List[int]] = []
    for row in tx_input_ids.toLocalIterator():
        batch.append(list(row.ids))
        if len(batch) >= feed_batch_size:
            c.process_transactions(batch)
            total_multi_input += len(batch)
            logger.info(f"  fed {total_multi_input:,} multi-input txs to Rust")
            batch = []
    if batch:
        c.process_transactions(batch)
        total_multi_input += len(batch)

    feed_secs = time.perf_counter() - feed_start
    logger.info(
        f"Fed {total_multi_input:,} multi-input txs to Rust in {feed_secs:.1f}s"
    )

    # ---- BULK WRITE: address_id -> cluster_id mapping ----
    logger.info("Generating final cluster mapping from Rust")
    mapping_batch = c.get_mapping()
    total_rows = mapping_batch.num_rows
    logger.info(f"Mapping has {total_rows:,} rows — bulk-writing via Spark")

    aid_col = mapping_batch.column("address_id")
    cid_col = mapping_batch.column("cluster_id")

    write_start = time.perf_counter()
    for offset in range(0, total_rows, write_chunk):
        length = min(write_chunk, total_rows - offset)
        pdf = (
            aid_col.slice(offset, length)
            .to_pandas()
            .to_frame("address_id")
            .assign(cluster_id=cid_col.slice(offset, length).to_pandas())
        )
        # Drop the coinbase placeholder (address_id 0); fresh tables use int32.
        pdf = pdf[pdf["address_id"] != 0]
        sdf = (
            spark.createDataFrame(pdf)
            .withColumn("address_id", F.col("address_id").cast("int"))
            .withColumn("cluster_id", F.col("cluster_id").cast("int"))
        )
        sdf.persist()
        (
            sdf.select("address_id", "cluster_id")
            .write.format(cass_format)
            .options(table="fresh_address_cluster", keyspace=transformed_keyspace)
            .mode("append")
            .save()
        )
        (
            sdf.select("cluster_id", "address_id")
            .write.format(cass_format)
            .options(table="fresh_cluster_addresses", keyspace=transformed_keyspace)
            .mode("append")
            .save()
        )
        sdf.unpersist()
        logger.info(
            f"  wrote {offset + length:,}/{total_rows:,} "
            f"({100 * (offset + length) / total_rows:.1f}%)"
        )

    write_secs = time.perf_counter() - write_start
    logger.info(
        f"Bulk-wrote {total_rows:,} mappings in {write_secs:.1f}s "
        f"(total runtime {time.perf_counter() - feed_start:.1f}s)"
    )
