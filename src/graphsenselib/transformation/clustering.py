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
