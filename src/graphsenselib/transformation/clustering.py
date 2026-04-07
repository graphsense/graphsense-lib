"""One-off clustering: read raw Cassandra, run Rust clustering, write to Cassandra."""

import logging
import time
from typing import Dict, List, Set

logger = logging.getLogger(__name__)

# Defaults for run_clustering_one_off_from_cassandra
DEFAULT_BLOCK_CHUNK_SIZE = 1_000
DEFAULT_CASSANDRA_CONCURRENCY = 100
DEFAULT_WRITE_CHUNK = 100_000


def run_clustering_one_off_from_cassandra(
    db,
    start_block: int,
    end_block: int,
    chunk_size: int = DEFAULT_BLOCK_CHUNK_SIZE,
    concurrency: int = DEFAULT_CASSANDRA_CONCURRENCY,
    write_chunk: int = DEFAULT_WRITE_CHUNK,
):
    """Run full one-off clustering from the raw Cassandra keyspace.

    No PySpark dependency.  Processes blocks in ``chunk_size``-block chunks.
    For each chunk:

      1. Parallel point reads of ``raw.block_transactions`` for every block
         in the chunk  ->  list of new tx_ids.
      2. Per-tx_id_group range reads of ``raw.transaction`` over
         [min_tx_id, max_tx_id] for the chunk  ->  multi-input address sets.
         One query per bucket, dispatched via ``execute_statements_async``
         with bounded concurrency (issues parallel single-partition scans,
         not one fan-out coordinator query).
      3. Async batched resolution of unique input addresses via
         ``transformed.address_ids_by_address_prefix``  ->  address_ids.
      4. Feed the resulting ``tx_input_ids`` to the Rust Union-Find
         (``Clustering.process_transactions``), which accumulates state
         across chunk calls.

    After all chunks are processed, ``get_mapping()`` is called once and the
    full mapping is streamed back to Cassandra in ``write_chunk``-sized
    slices via the driver's async concurrent execution.

    Args:
        db: open ``AnalyticsDb`` instance.
        start_block: first block to include (inclusive).
        end_block: last block to include (inclusive).
        chunk_size: number of blocks per read+feed batch (default 1,000).
        concurrency: max concurrent in-flight statements per batch
            (default 100).  Keeps the Cassandra driver from flooding the
            coordinator during large-chunk reads or the final mapping write.
        write_chunk: number of rows per write slice (default 100,000).

    Raises:
        RuntimeError on any failed statement or if
        ``summary_statistics.no_addresses`` is missing (the UF needs it to
        size itself).  Relies on the invariant that address_ids are dense
        in ``[1, no_addresses]``, which is preserved by both the Scala
        seeding and ``consume_address_id()`` in the delta updater.
    """
    from gs_clustering import Clustering

    rdb = db.raw
    tdb = db.transformed
    raw_ks = rdb.get_keyspace()
    transformed_ks = tdb.get_keyspace()

    # 1) Size the Union-Find from summary_statistics.no_addresses.
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

    # 2) Prepare statements once.
    block_bucket_size = rdb.get_block_bucket_size()
    tx_bucket_size = rdb.get_tx_bucket_size()

    bt_prep = rdb._db.get_prepared_statement(
        f"SELECT txs FROM {raw_ks}.block_transactions "
        "WHERE block_id_group=? AND block_id=?"
    )
    tx_prep = rdb._db.get_prepared_statement(
        f"SELECT coinbase, inputs FROM {raw_ks}.transaction "
        "WHERE tx_id_group=? AND tx_id>? AND tx_id<=?"
    )

    # 3) Walk the block range in chunks.  Each chunk is fully processed
    # (read -> feed to Rust) before the next is started — no prefetch, no
    # double-buffering.
    total_blocks = end_block - start_block + 1
    total_tx_rows = 0
    total_multi_input = 0
    feed_start = time.perf_counter()

    for chunk_start in range(start_block, end_block + 1, chunk_size):
        chunk_end = min(chunk_start + chunk_size - 1, end_block)
        chunk_t0 = time.perf_counter()

        # 3a) block_transactions -> tx_ids
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

        # 3b) transaction range reads, one statement per tx_id_group bucket.
        # Parallel single-partition scans: Cassandra-idiomatic.
        min_bucket = rdb.get_id_group(min_tx_id, tx_bucket_size)
        max_bucket = rdb.get_id_group(max_tx_id, tx_bucket_size)

        tx_stmts = [
            tx_prep.bind(
                {
                    "tx_id_group": bucket,
                    "tx_id_lower": min_tx_id - 1,  # > lower, inclusive
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
                        for addr in inp.address:
                            addrs.add(addr)
                if len(addrs) >= 2:
                    addr_list = list(addrs)
                    tx_input_addr_lists.append(addr_list)
                    unique_addresses.update(addr_list)

        if not tx_input_addr_lists:
            total_tx_rows += len(tx_ids)
            continue

        # 3c) Resolve unique input addresses to address_ids.
        addr_to_id: Dict[str, int] = {}
        for adr, exec_result in tdb.get_address_id_async_batch(list(unique_addresses)):
            row = exec_result.result_or_exc.one()
            if row is not None:
                addr_to_id[adr] = row.address_id

        # 3d) Build dense address_id lists per tx, drop any that shrink
        # to <2 after resolution (missing addresses).
        tx_input_ids: List[List[int]] = []
        for addr_list in tx_input_addr_lists:
            ids = set()
            for addr in addr_list:
                aid = addr_to_id.get(addr)
                if aid is not None:
                    ids.add(aid)
            if len(ids) >= 2:
                tx_input_ids.append(list(ids))

        # 3e) Feed to Rust.  UFRush accumulates across calls.
        if tx_input_ids:
            c.process_transactions(tx_input_ids)

        total_tx_rows += len(tx_ids)
        total_multi_input += len(tx_input_ids)
        chunk_secs = time.perf_counter() - chunk_t0
        blocks_done = chunk_end - start_block + 1
        pct = 100 * blocks_done / total_blocks
        logger.info(
            f"  chunk [{chunk_start:,}-{chunk_end:,}] "
            f"{len(tx_ids):,} txs ({len(tx_input_ids):,} multi-input) "
            f"in {chunk_secs:.2f}s  [{pct:.1f}%]"
        )

    feed_secs = time.perf_counter() - feed_start
    logger.info(
        f"Fed {total_tx_rows:,} tx rows "
        f"({total_multi_input:,} multi-input) to Rust in {feed_secs:.1f}s "
        f"for blocks [{start_block:,}-{end_block:,}]"
    )

    # 4) Final mapping.  WARNING: this is still a single RecordBatch of
    # size ``max_address_id+1`` — on BTC mainnet that's ~10 GB.  A future
    # Rust ``get_mapping_range(start, end)`` will let us stream it.  For
    # now the mapping lives in driver memory, but the writes are streamed
    # below in ``write_chunk``-sized slices so at least the per-write-batch
    # memory stays bounded.
    logger.info("Generating final cluster mapping from Rust")
    mapping_batch = c.get_mapping()
    total_rows = mapping_batch.num_rows
    logger.info(f"Mapping has {total_rows:,} rows — streaming to Cassandra")

    # 5) Stream-write the mapping to fresh_* tables.
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

    written = 0
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

        written += length
        logger.info(
            f"  wrote {written:,}/{total_rows:,} ({100 * written / total_rows:.1f}%)"
        )

    write_secs = time.perf_counter() - write_start
    logger.info(
        f"Wrote {total_rows:,} mappings in {write_secs:.1f}s "
        f"(total runtime {time.perf_counter() - feed_start:.1f}s)"
    )
