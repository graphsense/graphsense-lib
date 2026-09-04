"""The v3 data access layer.

Every read here is one of the access patterns
``graphsense_v3.probe`` runs against a live keyspace, and the probe is the
reason they are shaped the way they are. Three of those shapes are not
negotiable, and getting any of them wrong produces empty results rather than
errors -- which is why each is stated at its method:

* **Buckets are ``crc32(entity) % n``** (:func:`graphsense_v3.codec.bucket`),
  not murmur3 and not Spark's ``hash``. The writer and this module must agree
  exactly; a mismatch addresses a partition that exists and is empty.
* **The bucketing constants come from the keyspace**, read once at
  :meth:`Dal.open` from its own ``configuration`` row. Hard-coding them means a
  keyspace built with different constants reads as empty.
* **Clustering restrictions must form a prefix.** Anything that has to be
  pushed down lives in the PARTITION key, so a logically-single read can be
  several partition reads. Each method says how many it costs.

What is deliberately absent: entities/clusters. Clustering is staged for a
later run (D9), the cluster tables are not in the schema yet, and a method that
returned an empty cluster would read as "no cluster" rather than "not built".
:class:`graphsense_v3.db.legacy.LegacyAdapter` raises for those instead.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Optional, Sequence

from graphsense_v3.codec import bucket, tx_id_range
from graphsense_v3.settings import assert_v3_keyspace

logger = logging.getLogger(__name__)

#: How many block partitions `block_below` walks before giving up. A gap wider
#: than this turns a point read back into the scan the table exists to avoid.
BLOCK_BELOW_MAX_GROUPS = 100

#: Columns of ``address_stats`` that are summable across epochs. Epoch 0 is the
#: compacted base and later epochs are deltas, so a read SUMS the slice --
#: reading epoch 0 alone silently drops everything the incremental path added.
SUMMABLE_STATS = (
    "no_incoming_txs",
    "no_outgoing_txs",
    "no_incoming_txs_zero_value",
    "no_outgoing_txs_zero_value",
)

#: Columns that exist only on the epoch-0 row and are NOT summable: degrees are
#: distinct counts, and the paging cursors are positions rather than amounts.
#: Summing either would produce a plausible, wrong number.
EPOCH_ZERO_ONLY = (
    "in_degree",
    "out_degree",
    "in_degree_zero_value",
    "out_degree_zero_value",
    "in_tx_page_max",
    "out_tx_page_max",
    "in_tx_ordinal_next",
    "out_tx_ordinal_next",
    "in_zero_tx_page_max",
    "out_zero_tx_page_max",
    "in_zero_tx_ordinal_next",
    "out_zero_tx_ordinal_next",
)


@dataclass(frozen=True)
class AddressTx:
    """One transaction of one address, in one direction."""

    tx_id: int
    value: int
    #: The address's balance in this row's asset AFTER the transaction. NULL on
    #: rows written before the balance column existed, and on the ingest tail,
    #: which cannot fill it without a read.
    balance: Optional[int] = None
    currency: Optional[str] = None
    is_outgoing: bool = False


@dataclass(frozen=True)
class Neighbor:
    """One edge of the address graph, already summed over its epochs."""

    address: bytes
    no_transactions: int
    value: Optional[int] = None


@dataclass(frozen=True)
class Stats:
    """``address_stats``, with the epoch slice already resolved."""

    address: bytes
    summed: dict
    epoch_zero: dict
    first_tx_id: Optional[int] = None
    last_tx_id: Optional[int] = None

    @property
    def no_transactions(self) -> int:
        return int(self.summed.get("no_incoming_txs") or 0) + int(
            self.summed.get("no_outgoing_txs") or 0
        )


class Dal:
    """Reads one v3 raw + derived keyspace pair.

    Async because the service layer is: a synchronous driver call on the event
    loop blocks every other in-flight request.
    """

    def __init__(self, session, raw: str, derived: str, config: dict) -> None:
        self.session = session
        self.raw = raw
        self.derived = derived
        self.config = config
        #: Set by `open`; None when a session was injected directly, which is
        #: how the tests drive this without a cluster.
        self.cluster = None

    # -- lifecycle --------------------------------------------------------

    @classmethod
    async def open(
        cls,
        nodes: Sequence[str],
        raw: str,
        derived: str,
        *,
        username: Optional[str] = None,
        password: Optional[str] = None,
    ) -> "Dal":
        """Connect and read the keyspace's own constants.

        The constants are read here, once, rather than defaulted: they are what
        every bucket and every partition key is computed from, and a keyspace
        written with different ones would otherwise read as uniformly empty.
        """
        from graphsense_v3.cassandra import connect_to

        for keyspace in (raw, derived):
            assert_v3_keyspace(keyspace)

        cluster = connect_to(list(nodes), username, password)
        session = await asyncio.to_thread(cluster.connect)
        dal = cls(session, raw, derived, {})
        dal.cluster = cluster
        rows = await dal._select(
            f"SELECT * FROM {derived}.configuration WHERE keyspace_name = %s",
            (derived,),
        )
        if not rows:
            # The raw keyspace always has one; a derived keyspace written before
            # the job emitted its own does not. Same NetworkConfig, one run.
            rows = await dal._select(
                f"SELECT * FROM {raw}.configuration WHERE keyspace_name = %s", (raw,)
            )
            logger.warning(
                "%s has no configuration row; using %s's constants", derived, raw
            )
        if not rows:
            raise LookupError(
                f"neither {derived} nor {raw} has a configuration row -- the "
                "backfill did not finish, or these are not v3 keyspaces"
            )
        dal.config = rows[0]._asdict()
        return dal

    async def close(self) -> None:
        if self.cluster is not None:
            await asyncio.to_thread(self.cluster.shutdown)

    async def is_complete(self) -> bool:
        """Whether the backfill finished. A reader without this is measuring
        missing data and cannot tell that it is."""
        rows = await self._select(
            f"SELECT value FROM {self.derived}.markers WHERE key = %s", ("complete",)
        )
        return bool(rows)

    # -- driver -----------------------------------------------------------

    async def _select(self, cql: str, params: tuple = ()) -> list:
        """One query, off the event loop.

        The driver's ``execute_async`` returns a ResponseFuture whose callbacks
        fire on the driver's reactor thread, so the result is handed back to the
        loop with ``call_soon_threadsafe`` rather than resolved in place.
        """
        loop = asyncio.get_running_loop()
        future: asyncio.Future = loop.create_future()
        response = self.session.execute_async(cql, params)

        def on_success(rows) -> None:
            loop.call_soon_threadsafe(
                lambda: None if future.done() else future.set_result(list(rows or []))
            )

        def on_error(exc) -> None:
            loop.call_soon_threadsafe(
                lambda: None if future.done() else future.set_exception(exc)
            )

        response.add_callbacks(on_success, on_error)
        return await future

    async def _gather(self, queries: Sequence[tuple]) -> list:
        """Several partition reads at once. The fan-out patterns are only
        tolerable concurrently -- 16 sequential round trips is 16x the latency
        for the same work."""
        results = await asyncio.gather(*(self._select(q, p) for q, p in queries))
        return [row for rows in results for row in rows]

    # -- keys -------------------------------------------------------------

    def entity_bucket(self, address: bytes) -> int:
        """``crc32(address) % entity_buckets``, mirroring the writer exactly."""
        return bucket(address, self.config["entity_buckets"])

    def relation_bucket(self, counterparty: bytes) -> int:
        """The bucket an edge lives in, computed from the FAR side -- which is
        what makes "is X a neighbour of Y" a point read instead of a scatter."""
        return bucket(counterparty, self.config["relation_buckets"])

    def block_group(self, block_id: int) -> int:
        return block_id // self.config["block_bucket_size"]

    def tx_group(self, tx_id: int) -> int:
        """The ``transaction`` partition for a tx_id.

        Arithmetic, not a lookup: the id is ``(block_id << 32) + index``, so the
        block -- and therefore the partition -- falls out of the id itself.
        """
        return (tx_id >> 32) // self.config["tx_block_bucket_size"]

    # -- address ----------------------------------------------------------

    async def stats(self, address: bytes) -> Optional[Stats]:
        """One partition read; the epoch rows are summed here.

        Epoch 0 is the compacted base and each later epoch is a delta, so the
        answer is the sum of the slice. The degrees and paging cursors live on
        epoch 0 only and are carried through untouched -- they are positions and
        distinct counts, and summing them would give a plausible wrong number.
        """
        rows = await self._select(
            f"SELECT * FROM {self.derived}.address_stats "
            f"WHERE address_bucket = %s AND address = %s",
            (self.entity_bucket(address), address),
        )
        if not rows:
            return None
        summed = {name: 0 for name in SUMMABLE_STATS}
        epoch_zero: dict = {}
        first_tx: Optional[int] = None
        last_tx: Optional[int] = None
        for row in rows:
            data = row._asdict()
            for name in SUMMABLE_STATS:
                summed[name] += int(data.get(name) or 0)
            if data.get("epoch") == 0:
                epoch_zero = {name: data.get(name) for name in EPOCH_ZERO_ONLY}
            # min-merge and max-merge, as the writer defines them.
            if data.get("first_tx_id") is not None:
                first_tx = min(first_tx or data["first_tx_id"], data["first_tx_id"])
            if data.get("last_tx_id") is not None:
                last_tx = max(last_tx or data["last_tx_id"], data["last_tx_id"])
        return Stats(address, summed, epoch_zero, first_tx, last_tx)

    async def balance(self, address: bytes) -> dict:
        """``{currency: amount}``, summed over epochs like the stats."""
        rows = await self._select(
            f"SELECT currency, balance FROM {self.derived}.balance "
            f"WHERE address_bucket = %s AND address = %s",
            (self.entity_bucket(address), address),
        )
        totals: dict = {}
        for row in rows:
            totals[row.currency] = totals.get(row.currency, 0) + int(row.balance or 0)
        return totals

    async def balance_at(
        self, address: bytes, currency: str, day: int
    ) -> Optional[int]:
        """The balance at the end of ``day`` (yyyymmdd), or None if the address
        had not moved by then.

        ``day DESC`` + ``LIMIT 1`` is the whole point of ``balance_history``:
        the balance ON a day is one row, not a sum over every active day since
        the address was created.
        """
        rows = await self._select(
            f"SELECT balance FROM {self.derived}.balance_history "
            f"WHERE address_bucket = %s AND address = %s AND currency = %s "
            f"AND day <= %s LIMIT 1",
            (self.entity_bucket(address), address, currency, day),
        )
        return int(rows[0].balance) if rows else None

    async def transactions(
        self,
        address: bytes,
        *,
        is_outgoing: Optional[bool] = None,
        include_zero_value: bool = False,
        page: Optional[int] = None,
        before_tx_id: Optional[int] = None,
        after_tx_id: Optional[int] = None,
        limit: int = 100,
    ) -> list:
        """An address's transactions, newest first.

        Costs one partition read per (direction x zero-ness x page) combination,
        because all three are in the partition key -- unbound direction is two
        reads, and including zero-value rows doubles that again. They are merged
        here, in ``tx_id`` order.

        ``page`` defaults to the address's HIGHEST page rather than 0. Pages are
        numbered by ascending ordinal, so page 0 holds the OLDEST transactions;
        a newest-first listing starts at ``*_tx_page_max`` from the epoch-0
        stats row and walks down.
        """
        directions = (False, True) if is_outgoing is None else (is_outgoing,)
        zero_flags = (False, True) if include_zero_value else (False,)

        if page is None:
            stats = await self.stats(address)
            if stats is None:
                return []
            page = max(
                (
                    int(stats.epoch_zero.get(name) or 0)
                    for name in ("in_tx_page_max", "out_tx_page_max")
                ),
                default=0,
            )

        # Both bounds are clustering restrictions on tx_id, so a range read.
        # `after_tx_id` is what a min_height filter becomes -- without it the
        # height is only a hint about which page to start on, and rows BELOW it
        # come back anyway.
        clause = ""
        extra: tuple = ()
        if before_tx_id is not None:
            clause += " AND tx_id < %s"
            extra += (before_tx_id,)
        if after_tx_id is not None:
            clause += " AND tx_id >= %s"
            extra += (after_tx_id,)

        # Gathered per (direction, zero-ness) rather than through `_gather`,
        # which flattens: the DIRECTION is not on the row, it is in the
        # partition key, so flattening loses it. A caller cannot re-derive it,
        # and v2 signs an outgoing value negative -- so a lost direction is a
        # wrong sign on every row of an unbounded listing.
        specs = [(outgoing, zero) for outgoing in directions for zero in zero_flags]
        results = await asyncio.gather(
            *(
                self._select(
                    f"SELECT tx_id, value, balance FROM "
                    f"{self.derived}.address_transactions "
                    f"WHERE address = %s AND is_outgoing = %s AND is_zero_value = %s "
                    f"AND tx_page = %s{clause} LIMIT {int(limit)}",
                    (address, outgoing, zero, page) + extra,
                )
                for outgoing, zero in specs
            )
        )
        merged = [
            AddressTx(
                tx_id=row.tx_id,
                value=int(row.value or 0),
                balance=None if row.balance is None else int(row.balance),
                is_outgoing=outgoing,
            )
            for (outgoing, _zero), rows in zip(specs, results)
            for row in rows
        ]
        merged.sort(key=lambda tx: tx.tx_id, reverse=True)
        return merged[:limit]

    async def page_for_tx(
        self, address: bytes, is_outgoing: bool, tx_id: int, *, zero_value: bool = False
    ) -> Optional[int]:
        """Which page holds ``tx_id`` -- the entry point for a height filter.

        Ordinal pages are not tx_id-aligned, so a range query cannot compute its
        page. It looks it up here first, which is the only reason this index
        exists.
        """
        rows = await self._select(
            f"SELECT tx_page FROM {self.derived}.address_tx_pages "
            f"WHERE address = %s AND is_outgoing = %s AND is_zero_value = %s "
            f"AND first_tx_id <= %s LIMIT 1",
            (address, is_outgoing, zero_value, tx_id),
        )
        return rows[0].tx_page if rows else None

    async def neighbors(self, address: bytes, *, is_outgoing: bool) -> list:
        """Every counterparty, summed over epochs.

        Costs ``relation_buckets`` partition reads, unconditionally: the bucket
        is derived from the FAR side, which is unknown here, and there is no
        watermark table to stop early. Issued concurrently for that reason.
        """
        table = (
            "address_outgoing_relations"
            if is_outgoing
            else ("address_incoming_relations")
        )
        near = "src_address" if is_outgoing else "dst_address"
        far = "dst_address" if is_outgoing else "src_address"
        buckets = self.config["relation_buckets"]
        rows = await self._gather(
            [
                (
                    f"SELECT * FROM {self.derived}.{table} "
                    f"WHERE {near} = %s AND rel_bucket = %s",
                    (address, index),
                )
                for index in range(buckets)
            ]
        )
        totals: dict = {}
        for row in rows:
            data = row._asdict()
            key = bytes(data[far])
            count, value = totals.get(key, (0, 0))
            totals[key] = (
                count + int(data.get("no_transactions") or 0),
                value + int((data.get("value") or {}).get("value", 0) or 0)
                if isinstance(data.get("value"), dict)
                else value,
            )
        return [
            Neighbor(address=key, no_transactions=count, value=value)
            for key, (count, value) in totals.items()
        ]

    async def neighbor(
        self, address: bytes, counterparty: bytes, *, is_outgoing: bool
    ) -> Optional[Neighbor]:
        """Whether one specific edge exists -- a POINT read, because the bucket
        is computed from the counterparty."""
        table = (
            "address_outgoing_relations"
            if is_outgoing
            else ("address_incoming_relations")
        )
        near, far = (
            ("src_address", "dst_address")
            if is_outgoing
            else ("dst_address", "src_address")
        )
        rows = await self._select(
            f"SELECT * FROM {self.derived}.{table} "
            f"WHERE {near} = %s AND rel_bucket = %s AND {far} = %s",
            (address, self.relation_bucket(counterparty), counterparty),
        )
        if not rows:
            return None
        total = sum(int(r._asdict().get("no_transactions") or 0) for r in rows)
        return Neighbor(address=counterparty, no_transactions=total)

    async def link_transactions(
        self, src: bytes, dst: bytes, *, limit: int = 100
    ) -> list:
        """The transactions on one edge. One partition, because the layout is
        per (source, bucket) -- this is the ``/links`` fix."""
        rows = await self._select(
            f"SELECT tx_id, input_value, output_value FROM "
            f"{self.derived}.address_link_transactions "
            f"WHERE src_address = %s AND dst_bucket = %s AND dst_address = %s "
            f"LIMIT {int(limit)}",
            (src, self.relation_bucket(dst), dst),
        )
        return [
            {
                "tx_id": r.tx_id,
                "input_value": int(r.input_value or 0),
                "output_value": int(r.output_value or 0),
            }
            for r in rows
        ]

    async def search_addresses(self, prefix: str, *, limit: int = 10) -> list:
        """Addresses starting with ``prefix``.

        The stored prefix is lowercased and has the network's dead leading run
        stripped (:func:`graphsense_v3.codec.search_prefix`), so the caller's
        string must go through the same function -- v2's index is neither.
        """
        rows = await self._select(
            f"SELECT address FROM {self.derived}.address_by_prefix "
            f"WHERE address_prefix = %s LIMIT {int(limit)}",
            (prefix,),
        )
        return [bytes(row.address) for row in rows]

    # -- transaction ------------------------------------------------------

    async def tx_id_by_hash(self, tx_hash: bytes, prefix: str) -> Optional[int]:
        """Hash -> id. The only lookup that needs the prefix index."""
        rows = await self._select(
            f"SELECT tx_id FROM {self.raw}.transaction_by_tx_prefix "
            f"WHERE tx_prefix = %s AND tx_hash = %s",
            (prefix, tx_hash),
        )
        return rows[0].tx_id if rows else None

    async def transaction(self, tx_id: int) -> Optional[dict]:
        rows = await self._select(
            f"SELECT * FROM {self.raw}.transaction "
            f"WHERE block_id_group = %s AND tx_id = %s",
            (self.tx_group(tx_id), tx_id),
        )
        return rows[0]._asdict() if rows else None

    async def transaction_io(
        self, tx_id: int, *, is_output: Optional[bool] = None
    ) -> list:
        """A transaction's inputs and outputs -- same partition key as the
        transaction itself, so it is one extra read rather than a lookup."""
        clause = "" if is_output is None else " AND is_output = %s"
        params: tuple = (self.tx_group(tx_id), tx_id)
        if is_output is not None:
            params += (is_output,)
        rows = await self._select(
            f"SELECT * FROM {self.raw}.transaction_io "
            f"WHERE block_id_group = %s AND tx_id = %s{clause}",
            params,
        )
        return [row._asdict() for row in rows]

    async def transaction_io_many(self, tx_ids: Sequence[int]) -> dict:
        """``{tx_id: [io rows]}`` for several transactions at once.

        A block's transactions each need their inputs and outputs; sequentially
        that is one round trip per transaction, which on a full block is
        hundreds of times the latency for the same work.
        """
        if not tx_ids:
            return {}
        queries = [
            (
                f"SELECT * FROM {self.raw}.transaction_io "
                f"WHERE block_id_group = %s AND tx_id = %s",
                (self.tx_group(tx_id), tx_id),
            )
            for tx_id in tx_ids
        ]
        grouped: dict = {}
        for row in await self._gather(queries):
            grouped.setdefault(row.tx_id, []).append(row._asdict())
        return grouped

    async def spent_in(self, tx_hash: bytes, prefix: str) -> list:
        """What spent this transaction's outputs."""
        rows = await self._select(
            f"SELECT * FROM {self.raw}.transaction_spent_in "
            f"WHERE spent_tx_prefix = %s AND spent_tx_hash = %s",
            (prefix, tx_hash),
        )
        return [row._asdict() for row in rows]

    async def spending(self, tx_hash: bytes, prefix: str) -> list:
        """What this transaction's inputs spent."""
        rows = await self._select(
            f"SELECT * FROM {self.raw}.transaction_spending "
            f"WHERE spending_tx_prefix = %s AND spending_tx_hash = %s",
            (prefix, tx_hash),
        )
        return [row._asdict() for row in rows]

    # -- block ------------------------------------------------------------

    async def block(self, height: int) -> Optional[dict]:
        rows = await self._select(
            f"SELECT * FROM {self.raw}.block "
            f"WHERE block_id_group = %s AND block_id = %s",
            (self.block_group(height), height),
        )
        return rows[0]._asdict() if rows else None

    async def block_below(self, height: int) -> Optional[dict]:
        """The highest block strictly below ``height``.

        v2 answers this with ``SELECT max(block_id) ... ALLOW FILTERING``, a
        full scan of the block table. Here the height names its own partition,
        so the usual case is ONE partition read: blocks are dense within a
        group, and only a height sitting on a group boundary pays for a second.

        The walk is bounded. A chain with a gap wider than
        ``BLOCK_BELOW_MAX_GROUPS`` groups would otherwise turn a point read back
        into the scan this table exists to avoid; returning None says "not
        found here" rather than reading the chain to prove it.
        """
        group_size = self.config["block_bucket_size"]
        group = height // group_size
        for _ in range(BLOCK_BELOW_MAX_GROUPS):
            if group < 0:
                break
            rows = await self._select(
                f"SELECT block_id, timestamp FROM {self.raw}.block "
                f"WHERE block_id_group = %s AND block_id < %s "
                f"ORDER BY block_id DESC LIMIT 1",
                (group, height),
            )
            if rows:
                return rows[0]._asdict()
            group -= 1
        return None

    async def block_transactions(self, height: int) -> list:
        """A block's transactions as a tx_id RANGE.

        The reason ``block_transactions`` is gone from the schema: the range is
        arithmetic from the height, so the block's transactions are a clustering
        slice of a partition the height already identifies.
        """
        # Inclusive on both ends, so the clustering restriction is <=.
        low, high = tx_id_range(height, height)
        rows = await self._select(
            f"SELECT * FROM {self.raw}.transaction "
            f"WHERE block_id_group = %s AND tx_id >= %s AND tx_id <= %s",
            (height // self.config["tx_block_bucket_size"], low, high),
        )
        return [row._asdict() for row in rows]

    async def transactions_by_ids(self, tx_ids: Sequence[int]) -> dict:
        """``{tx_id: row}`` for a set of ids, fetched concurrently.

        A tx_id names its own partition arithmetically, so this is one point
        read each with no index lookup -- which is what makes the fan-out
        affordable where v2 needs a `transaction_ids_by_transaction_id_group`
        hop first.
        """
        if not tx_ids:
            return {}
        queries = [
            (
                f"SELECT * FROM {self.raw}.transaction "
                f"WHERE block_id_group = %s AND tx_id = %s",
                (self.tx_group(tx_id), tx_id),
            )
            for tx_id in tx_ids
        ]
        # `_gather` already flattens across queries.
        return {row.tx_id: row._asdict() for row in await self._gather(queries)}

    async def blocks_on_day(self, day: int, *, limit: int = 100) -> list:
        """``day`` is yyyymmdd as an integer, per design rule 5."""
        rows = await self._select(
            f"SELECT block_id, timestamp FROM {self.raw}.block_by_date "
            f"WHERE day = %s LIMIT {int(limit)}",
            (day,),
        )
        return [row._asdict() for row in rows]

    # -- rates and meta ---------------------------------------------------

    async def rate(self, asset: str, block_id: int) -> Optional[dict]:
        """The fiat rates for one asset at one block. The merged table holds the
        native coin and every token, so this is one lookup for both."""
        rows = await self._select(
            f"SELECT fiat_values FROM {self.derived}.exchange_rates "
            f"WHERE asset = %s AND block_id_group = %s AND block_id = %s",
            (asset, self.block_group(block_id), block_id),
        )
        return dict(rows[0].fiat_values) if rows and rows[0].fiat_values else None

    async def statistics(self) -> Optional[dict]:
        rows = await self._select(
            f"SELECT * FROM {self.derived}.summary_statistics WHERE id = 0"
        )
        return rows[0]._asdict() if rows else None

    async def token_configuration(self) -> list:
        rows = await self._select(f"SELECT * FROM {self.derived}.token_configuration")
        return [row._asdict() for row in rows]
