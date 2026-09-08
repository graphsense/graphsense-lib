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
from dataclasses import dataclass, field
from typing import Any, Optional, Sequence

from graphsense_v3.codec import bucket, tx_id_range
from graphsense_v3.settings import assert_v3_keyspace

logger = logging.getLogger(__name__)

#: How many block partitions `block_below` walks before giving up. A gap wider
#: than this turns a point read back into the scan the table exists to avoid.
BLOCK_BELOW_MAX_GROUPS = 100

#: How many days `block_at_or_after` will walk forward before giving up. A
#: timestamp late in a day often has no block after it until the next one, and a
#: chain can pause; past this it has a gap the date index cannot answer around.
BLOCK_BY_DATE_MAX_DAYS = 30

#: Columns of ``address_stats`` that are summable across epochs. Epoch 0 is the
#: compacted base and later epochs are deltas, so a read SUMS the slice --
#: reading epoch 0 alone silently drops everything the incremental path added.
SUMMABLE_STATS = (
    "no_incoming_txs",
    "no_outgoing_txs",
    "no_incoming_txs_zero_value",
    "no_outgoing_txs_zero_value",
)

#: Summable too, but as ``currency`` UDTs rather than integers -- an amount
#: plus a POSITIONAL fiat list -- so they merge with `add_currency` instead of
#: `+`. `address_stats` has carried them since the schema was written; the
#: reader simply dropped them, and `address_from_row` subscripts
#: ``row["total_received"]`` and ``row["total_spent"]``, so every get_address
#: and every neighbour listing died on a KeyError raised inside the service.
CURRENCY_STATS = ("total_received", "total_spent")

#: The account-only pair, ``map<text, currency>``: union by asset, then
#: `add_currency` per asset -- see `add_token_values` for why it cannot be a
#: positional merge. Absent on a UTXO keyspace, which holds one asset.
TOKEN_STATS = ("total_tokens_received", "total_tokens_spent")

#: Carried from the epoch-0 row untouched. `is_contract` is account-only and a
#: PROPERTY, not a delta: an address that became a contract did so once.
EPOCH_ZERO_CARRIED = ("is_contract",)

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


class NotAvailable(NotImplementedError):
    """A v2 method whose data v3 does not (yet) hold.

    Distinct from a bug: the caller asked for something real, and the honest
    answer is that this backend cannot serve it -- not an empty result.

    Defined HERE rather than in the adapter because the DAL itself has cases --
    `AccountDal.link_transactions` on a multi-page edge -- and `legacy` imports
    `core`, so the other direction would be a cycle. `legacy` re-exports it, so
    every existing import still resolves.
    """


@dataclass(frozen=True)
class AddressTx:
    """One transaction of one address, in one direction."""

    tx_id: int
    value: int
    #: The address's balance in this row's asset AFTER the transaction. NULL on
    #: rows written before the balance column existed, and on the ingest tail,
    #: which cannot fill it without a read.
    balance: Optional[int] = None
    #: Account only: which asset this row moved. NULL on a UTXO keyspace, whose
    #: `address_transactions` has no such column.
    currency: Optional[str] = None
    #: Account only: the (trace_index, log_index) that identifies WHICH transfer
    #: within the transaction this row is. `normalize_address_transactions`
    #: reads it, and one tx_id can carry several rows on this family.
    tx_reference: Any = None
    is_outgoing: bool = False


@dataclass(frozen=True)
class Neighbor:
    """One edge of the address graph, already summed over its epochs."""

    address: bytes
    no_transactions: int
    value: Optional[int] = None
    #: The native amount's fiat, POSITIONAL and ordered by the keyspace's
    #: `configuration.fiat_currencies` -- the `currency` UDT's own order.
    fiat_values: tuple = ()
    #: ``{ticker: {"value": int, "fiat_values": [float]}}``, account only.
    #: A map, so summing epochs is a union by asset then an add per asset --
    #: the one place in the summable model where a merge is not a scalar add.
    token_values: Optional[dict] = None


def _field(value: Any, name: str, default: Any = None) -> Any:
    """One field of a ``currency`` UDT, however the driver handed it over.

    Registered UDTs arrive as objects and unregistered ones as namedtuples,
    while a test double is usually a dict. Reading it three ways here keeps
    that detail out of every caller -- and an isinstance check that silently
    fell through was what dropped these values in the first place.
    """
    if value is None:
        return default
    if isinstance(value, dict):
        return value.get(name, default)
    return getattr(value, name, default)


def add_currency(into: Optional[dict], value: Any) -> dict:
    """One epoch's ``currency`` added to a running total.

    The amount is a scalar add; the fiat list is added POSITIONALLY, because
    the UDT is positional and ordered by the keyspace's own
    `configuration.fiat_currencies`.
    """
    previous = into or {}
    amount = int(previous.get("value") or 0) + int(_field(value, "value", 0) or 0)

    running: list = [float(item or 0.0) for item in previous.get("fiat_values") or []]
    fiat: list = list(_field(value, "fiat_values", []) or [])
    if len(fiat) > len(running):
        running += [0.0] * (len(fiat) - len(running))
    for index, item in enumerate(fiat):
        running[index] += float(item or 0.0)
    return {"value": amount, "fiat_values": running}


def add_token_values(into: Optional[dict], values: Any) -> Optional[dict]:
    """One epoch's ``token_values`` map merged into a running total.

    Union by asset, then :func:`add_currency` per asset. An asset the edge
    only carried in one epoch must survive, so this cannot be a positional
    zip -- which is why it is the one merge in this model that is not an add.
    """
    if not values:
        return into
    total = dict(into or {})
    for ticker, amount in dict(values).items():
        total[ticker] = add_currency(total.get(ticker), amount)
    return total


def page_max(epoch_zero: dict, is_outgoing: bool, is_zero_value: bool) -> int:
    """The newest page of ONE partition class of ``address_transactions``.

    Pages are numbered per class, so there is one cursor per class and the four
    do not move together: an address can have two outgoing pages and one
    incoming. Reading them all at the highest of the four returns nothing for
    every class that has not reached it.
    """
    name = (
        f"{'out' if is_outgoing else 'in'}"
        f"{'_zero' if is_zero_value else ''}_tx_page_max"
    )
    return int(epoch_zero.get(name) or 0)


@dataclass(frozen=True)
class Stats:
    """``address_stats``, with the epoch slice already resolved."""

    address: bytes
    summed: dict
    epoch_zero: dict
    first_tx_id: Optional[int] = None
    last_tx_id: Optional[int] = None
    #: ``total_received``/``total_spent``, merged as `currency` UDTs.
    totals: dict = field(default_factory=dict)
    #: ``total_tokens_received``/``total_tokens_spent``, merged per asset.
    token_totals: dict = field(default_factory=dict)

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

    @property
    def network(self) -> str:
        """The network this keyspace serves, read off its own name.

        `settings.v3_keyspace` builds every name as ``<net>_<kind>_v3[_label]``
        and `assert_v3_keyspace` refuses anything else, so the prefix is the
        network by construction rather than by convention.
        """
        return self.derived.split("_", 1)[0].lower()

    @property
    def is_account(self) -> bool:
        """Whether this keyspace has the ACCOUNT shape.

        The DAL was written UTXO-shaped throughout, and several tables differ:
        `address_transactions` carries `currency` and `tx_reference` clustering
        columns that no UTXO keyspace has. Selecting them unconditionally fails
        on UTXO; not selecting them at all serves every token transfer as if it
        were the native coin -- a wrong answer that looks like data.
        """
        from graphsense_v3.schema.definitions import NETWORKS
        from graphsense_v3.schema.model import Family

        return NETWORKS.get(self.network) is Family.ACCOUNT

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
        # `dal_for`, not `cls`: `open` is inherited, so calling it on the base
        # would build a reader with no `transactions` at all. The family comes
        # from the keyspace name either way.
        dal = dal_for(session, raw, derived, {})
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
        totals: dict = {}
        token_totals: dict = {}
        epoch_zero: dict = {}
        first_tx: Optional[int] = None
        last_tx: Optional[int] = None
        for row in rows:
            data = row._asdict()
            for name in SUMMABLE_STATS:
                summed[name] += int(data.get(name) or 0)
            # Summed over the SAME epoch slice as the counts, and by the same
            # argument: epoch 0 is the compacted base and later epochs are
            # deltas, so reading one row would understate an address the
            # incremental path has touched since.
            for name in CURRENCY_STATS:
                if name in data:
                    totals[name] = add_currency(totals.get(name), data.get(name))
            for name in TOKEN_STATS:
                if data.get(name):
                    token_totals[name] = add_token_values(
                        token_totals.get(name), data.get(name)
                    )
            if data.get("epoch") == 0:
                epoch_zero = {name: data.get(name) for name in EPOCH_ZERO_ONLY}
                epoch_zero.update(
                    {name: data[name] for name in EPOCH_ZERO_CARRIED if name in data}
                )
            # min-merge and max-merge, as the writer defines them.
            if data.get("first_tx_id") is not None:
                first_tx = min(first_tx or data["first_tx_id"], data["first_tx_id"])
            if data.get("last_tx_id") is not None:
                last_tx = max(last_tx or data["last_tx_id"], data["last_tx_id"])
        return Stats(
            address, summed, epoch_zero, first_tx, last_tx, totals, token_totals
        )

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

    @staticmethod
    def paging_bounds(
        before_row: Optional[tuple],
        before_tx_id: Optional[int],
        after_tx_id: Optional[int],
    ) -> tuple:
        """``(clause, params, skip)`` for a resume cursor and the height bounds.

        The resume cursor is ``(tx_id, rows_of_it_already_delivered)`` and its
        tx_id bound is INCLUSIVE -- not the exclusive ``tx_id <`` a UTXO listing
        could get away with.

        On the account family one transaction produces SEVERAL rows: a trace and
        a log, or two assets, keyed ``(tx_id, tx_reference, currency)``. A page
        can therefore end in the middle of a transaction, and an exclusive
        ``tx_id <`` bound would silently drop whatever of it did not fit. Re-
        reading that transaction and skipping what was already sent cannot lose
        a row, and costs at most one transaction's worth of rows.

        THE ALTERNATIVE, and why it is not what runs
        --------------------------------------------
        Cassandra can express this as one bound, on the whole clustering
        prefix::

            WHERE address = ? AND is_outgoing = ? AND is_zero_value = ?
              AND tx_page = ?
              AND (tx_id, tx_reference, currency) < (?, ?, ?)

        That is strictly better on paper. It is exact in one round trip, it
        re-reads nothing, and the cursor is the row's own identity rather than a
        position plus a count -- so it cannot drift if the merge order ever
        changes. If the re-read ever costs real time -- a contract emitting
        thousands of transfers in ONE transaction would re-read all of them on
        every page boundary -- this is the fix.

        Three things have to be settled before it can be trusted, and none of
        them could be settled from a laptop:

        1. **Mixed clustering directions.** The account table is
           ``tx_id DESC, tx_reference DESC, currency ASC``. A multi-column
           comparison follows clustering order rather than value order, so with
           a direction change partway through the tuple, ``<`` does not mean
           what reading it suggests. Making ``currency`` DESC in
           `schema.definitions._txs_table` removes the question entirely and
           costs nothing while no account keyspace exists -- do that first.
        2. **Binding the UDT.** ``tx_reference`` is ``frozen<tx_reference>``
           and has to arrive as a bound parameter. That means registering the
           type on the session or passing the driver's tuple form, and getting
           it wrong is a query error rather than a wrong answer -- the least
           dangerous of the three, because it fails loudly.
        3. **A NULL in the tuple.** ``tx_reference`` is null on rows that are
           neither a trace nor a log. Comparison against a null clustering
           value is the case most likely to silently skip rows, and it is the
           one to write a test for FIRST.

        Until those are answered on a real account keyspace, this reads a
        transaction twice rather than risk a bound that looks right and quietly
        drops rows at every page boundary -- which is the exact bug being
        fixed, and the kind that surfaces months later as "some transactions
        are missing".
        """
        clause, params, skip = "", (), 0
        if before_row is not None:
            bound, skip = before_row
            clause += " AND tx_id <= %s"
            params += (bound,)
        elif before_tx_id is not None:
            clause += " AND tx_id < %s"
            params += (before_tx_id,)
        if after_tx_id is not None:
            clause += " AND tx_id >= %s"
            params += (after_tx_id,)
        return clause, params, int(skip or 0)

    # -- family-shaped, implemented by the subclasses ---------------------
    #
    # The reads whose shape is decided by the family rather than shared by it.
    # They are declared here so the contract is visible and the checker can see
    # it, and they RAISE: a base that guessed is what served every account
    # token transfer as the native coin. Use `dal_for`.
    #
    # The last four are the inputs/outputs and spending graph. They lived on
    # this class and queried `transaction_io` unconditionally, which is a table
    # only a UTXO keyspace has -- so `get_tx` and `list_block_txs` on eth died
    # with a CQL error naming a table rather than the family mismatch. UtxoDal
    # queries; AccountDal answers empty, because an account transaction having
    # no inputs is a fact about the family and not a missing feature.

    async def transactions(
        self,
        address: bytes,
        *,
        is_outgoing: Optional[bool] = None,
        include_zero_value: bool = False,
        page: Optional[int] = None,
        before_tx_id: Optional[int] = None,
        after_tx_id: Optional[int] = None,
        #: ``(tx_id, rows_of_it_already_delivered)`` -- see `paging_bounds`.
        before_row: Optional[tuple] = None,
        limit: int = 100,
    ) -> list:
        """An address's transactions, newest first. See the subclasses."""
        raise NotImplementedError(
            f"{type(self).__name__} does not know which family it is reading; "
            "build the reader with `dal_for`"
        )

    async def link_transactions(
        self, src: bytes, dst: bytes, *, limit: int = 100
    ) -> list:
        """The transactions on one edge. See the subclasses."""
        raise NotImplementedError(
            f"{type(self).__name__} does not know which family it is reading; "
            "build the reader with `dal_for`"
        )

    async def transaction_io(
        self, tx_id: int, *, is_output: Optional[bool] = None
    ) -> list:
        """A transaction's inputs and outputs. See the subclasses."""
        raise NotImplementedError(
            f"{type(self).__name__} does not know which family it is reading; "
            "build the reader with `dal_for`"
        )

    async def transaction_io_many(self, tx_ids: Sequence[int]) -> dict:
        """``{tx_id: [io rows]}`` for several transactions. See the subclasses."""
        raise NotImplementedError(
            f"{type(self).__name__} does not know which family it is reading; "
            "build the reader with `dal_for`"
        )

    async def spent_in(self, tx_hash: bytes, prefix: str) -> list:
        """What spent this transaction's outputs. See the subclasses."""
        raise NotImplementedError(
            f"{type(self).__name__} does not know which family it is reading; "
            "build the reader with `dal_for`"
        )

    async def spending(self, tx_hash: bytes, prefix: str) -> list:
        """What this transaction's inputs spent. See the subclasses."""
        raise NotImplementedError(
            f"{type(self).__name__} does not know which family it is reading; "
            "build the reader with `dal_for`"
        )

    async def traces_by_ref(self, refs: "Sequence[tuple]") -> dict:
        """The traces named by ``(block_id, trace_index)``. See AccountDal."""
        raise NotImplementedError(
            f"{type(self).__name__} does not know which family it is reading; "
            "build the reader with `dal_for`"
        )

    async def logs_by_ref(self, refs: "Sequence[tuple]") -> dict:
        """The logs named by ``(block_id, log_index)``. See AccountDal."""
        raise NotImplementedError(
            f"{type(self).__name__} does not know which family it is reading; "
            "build the reader with `dal_for`"
        )

    async def logs_in_block(
        self, block_id: int, *, topic0: Optional[bytes] = None
    ) -> list:
        """Every log of one block. See AccountDal."""
        raise NotImplementedError(
            f"{type(self).__name__} does not know which family it is reading; "
            "build the reader with `dal_for`"
        )

    async def page_for_tx(
        self, address: bytes, is_outgoing: bool, tx_id: int, *, zero_value: bool = False
    ) -> Optional[int]:
        """Which page holds ``tx_id`` -- the entry point for a height filter.

        Ordinal pages are not tx_id-aligned, so a range query cannot compute its
        page. It looks it up here first, which is the only reason this index
        exists.

        **No row means page 0**, not "unknown": the index is written only for
        addresses that span more than one page, because for everyone else the
        row would say exactly this. Returning None here would make a height
        filter on an ordinary address look unanswerable.
        """
        rows = await self._select(
            f"SELECT tx_page FROM {self.derived}.address_tx_pages "
            f"WHERE address = %s AND is_outgoing = %s AND is_zero_value = %s "
            f"AND first_tx_id <= %s LIMIT 1",
            (address, is_outgoing, zero_value, tx_id),
        )
        return rows[0].tx_page if rows else 0

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
        counts: dict = {}
        amounts: dict = {}
        tokens: dict = {}
        order: list = []
        for row in rows:
            data = row._asdict()
            key = bytes(data[far])
            if key not in counts:
                order.append(key)
                counts[key] = 0
            counts[key] += int(data.get("no_transactions") or 0)
            amounts[key] = add_currency(amounts.get(key), data.get("value"))
            merged = add_token_values(tokens.get(key), data.get("token_values"))
            if merged is not None:
                tokens[key] = merged
        return [
            Neighbor(
                address=key,
                no_transactions=counts[key],
                value=amounts[key]["value"],
                fiat_values=tuple(amounts[key]["fiat_values"]),
                token_values=tokens.get(key),
            )
            for key in order
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
        total = 0
        amount: Optional[dict] = None
        token: Optional[dict] = None
        for row in rows:
            data = row._asdict()
            total += int(data.get("no_transactions") or 0)
            amount = add_currency(amount, data.get("value"))
            token = add_token_values(token, data.get("token_values"))
        amount = amount or {"value": 0, "fiat_values": []}
        return Neighbor(
            address=counterparty,
            no_transactions=total,
            value=amount["value"],
            fiat_values=tuple(amount["fiat_values"]),
            token_values=token,
        )

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

    async def block_at_or_after(
        self,
        timestamp: int,
        *,
        inclusive: bool = True,
        max_days: int = BLOCK_BY_DATE_MAX_DAYS,
    ) -> Optional[dict]:
        """The first block at or after ``timestamp``, as ``{block_id, timestamp}``.

        ``block_by_date`` clusters by ``(timestamp, block_id)`` ASC, so the bound
        is pushed into CQL and the answer is ONE row from ONE partition -- which
        is the whole reason the table exists.

        It reads the day, then walks forward: a timestamp late in the day may
        have no block after it until the next one, and a chain can pause. The
        walk is bounded for the same reason `block_below`'s is -- past that the
        chain has a gap this table cannot answer around, and returning None says
        so rather than reading forever to prove it.

        ``inclusive=False`` makes the bound STRICT, which is what
        `/blocks/by_date` needs. The service turns this block into
        ``after_block`` and the one below it into ``before_block``
        (`blocks_service.py:169-180`), so an exact timestamp match has to fall
        on the BEFORE side -- see the adapter's `get_block_by_date_allow_filtering`
        for why that, and not this method's natural reading, is the answer the
        endpoint has to give.
        """
        from datetime import datetime, timedelta, timezone

        bound = ">=" if inclusive else ">"
        when = datetime.fromtimestamp(timestamp, tz=timezone.utc)
        for step in range(max_days):
            day = int((when + timedelta(days=step)).strftime("%Y%m%d"))
            rows = await self._select(
                f"SELECT block_id, timestamp FROM {self.raw}.block_by_date "
                f"WHERE day = %s AND timestamp {bound} %s LIMIT 1",
                (day, timestamp),
            )
            if rows:
                return rows[0]._asdict()
        return None

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


class UtxoDal(Dal):
    """The reader for a UTXO keyspace.

    A transaction touches an address once, so ``tx_id`` alone identifies a row
    and orders the listing.
    """

    # -- inputs, outputs and the spending graph ---------------------------
    #
    # UTXO-ONLY, and on this class rather than on `Dal` for that reason. An
    # account transaction has no inputs, no outputs and no spending edges,
    # and `transaction_io`, `transaction_spent_in` and `transaction_spending`
    # are not in an account keyspace AT ALL -- so inheriting these read a
    # table that does not exist and failed `get_tx` and `list_block_txs` on
    # eth with a CQL error naming a table rather than the family mismatch.
    # `AccountDal` answers the same four with empty results.

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

    async def transactions(
        self,
        address: bytes,
        *,
        is_outgoing: Optional[bool] = None,
        include_zero_value: bool = False,
        page: Optional[int] = None,
        before_tx_id: Optional[int] = None,
        after_tx_id: Optional[int] = None,
        #: ``(tx_id, rows_of_it_already_delivered)`` -- see `paging_bounds`.
        before_row: Optional[tuple] = None,
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

        That maximum is PER PARTITION CLASS, and one number for all four reads
        is wrong in both directions: a class whose max is lower than the number
        chosen reads a page it has no rows on and returns NOTHING, so a merged
        listing silently becomes single-direction. An address with two outgoing
        pages and one incoming page loses every incoming transaction, which
        reads as "these transactions do not exist" rather than as an error.
        `address_tx_pages` is keyed per class for the same reason.
        """
        directions = (False, True) if is_outgoing is None else (is_outgoing,)
        zero_flags = (False, True) if include_zero_value else (False,)

        cursors: dict = {}
        if page is None:
            stats = await self.stats(address)
            if stats is None:
                return []
            cursors = stats.epoch_zero

        # Both bounds are clustering restrictions on tx_id, so a range read.
        # `after_tx_id` is what a min_height filter becomes -- without it the
        # height is only a hint about which page to start on, and rows BELOW it
        # come back anyway.
        clause, extra, skip = self.paging_bounds(before_row, before_tx_id, after_tx_id)

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
                    f"AND tx_page = %s{clause} LIMIT {int(limit) + skip}",
                    (
                        address,
                        outgoing,
                        zero,
                        page if page is not None else page_max(cursors, outgoing, zero),
                    )
                    + extra,
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
        # `skip` rows of the boundary transaction were already delivered. The
        # merge is deterministic -- each partition arrives in clustering order
        # and the sort is stable over a fixed spec list -- so the same rows come
        # back in the same order and dropping the leading `skip` is exact.
        return merged[skip : skip + limit]

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


class AccountDal(Dal):
    """The reader for an ACCOUNT keyspace.

    The family differs in more than a column. One transaction can move value
    several times for one address -- a trace and a log, or two assets -- so the
    listing carries ``currency`` and ``tx_reference``, and the primary key is
    ``(tx_id, tx_reference, currency)`` rather than ``tx_id`` alone.
    """

    # -- the transfer behind a listing row --------------------------------
    #
    # An account listing row carries `tx_reference` -- a trace_index or a
    # log_index -- and NOT the two ends of the transfer. v2 fetches the trace
    # or the log for exactly this (`cassandra.py:5045-5060`), because the outer
    # transaction's from/to are the wrong counterparty for an internal call or
    # a token transfer. `/links` escapes this: the caller named both ends.
    #
    # Both tables are keyed (block_id_group) / block_id, index, so a reference
    # names its own partition arithmetically and these are point reads.

    def _block_group(self, block_id: int) -> int:
        return block_id // self.config["block_bucket_size"]

    async def traces_by_ref(self, refs: "Sequence[tuple]") -> dict:
        """``{(block_id, trace_index): row}`` for several references at once."""
        return await self._events_by_ref("trace", "trace_index", refs)

    async def logs_by_ref(self, refs: "Sequence[tuple]") -> dict:
        """``{(block_id, log_index): row}`` for several references at once."""
        return await self._events_by_ref("log", "log_index", refs)

    async def logs_in_block(
        self, block_id: int, *, topic0: Optional[bytes] = None
    ) -> list:
        """Every log of one block, oldest first -- ONE partition slice.

        The whole-block read, rather than `no_logs` range reads keyed off each
        transaction: a block listing wants them all, and the pointers exist to
        make a SINGLE transaction's logs cheap, not to make the block's
        expensive.

        ``topic0`` filters here rather than in CQL. It is a plain column since
        the re-key (see the `log` table comment), so restricting on it would
        need ALLOW FILTERING over the same partition this already reads.
        """
        rows = await self._select(
            f"SELECT * FROM {self.raw}.log WHERE block_id_group = %s AND block_id = %s",
            (self._block_group(block_id), block_id),
        )
        logs = [row._asdict() for row in rows]
        if topic0 is None:
            return logs
        return [log for log in logs if log.get("topic0") == topic0]

    async def _events_by_ref(self, table: str, column: str, refs) -> dict:
        wanted = {
            (int(block), int(index)) for block, index in refs if index is not None
        }
        if not wanted:
            return {}
        queries = [
            (
                f"SELECT * FROM {self.raw}.{table} "
                f"WHERE block_id_group = %s AND block_id = %s AND {column} = %s",
                (self._block_group(block), block, index),
            )
            for block, index in sorted(wanted)
        ]
        found = {}
        for row in await self._gather(queries):
            data = row._asdict()
            found[(data["block_id"], data[column])] = data
        return found

    # -- the UTXO-only reads, answered without a query --------------------
    #
    # EMPTY, not `NotAvailable`. An account transaction genuinely has no
    # inputs, outputs or spending edges -- that is a fact about the family,
    # not a feature v3 has yet to build -- and the three tables these would
    # read do not exist in an account keyspace. Raising would make every
    # `get_tx` on eth an error; querying made it a CQL failure naming a table.

    async def transaction_io(
        self, tx_id: int, *, is_output: Optional[bool] = None
    ) -> list:
        return []

    async def transaction_io_many(self, tx_ids: Sequence[int]) -> dict:
        return {}

    async def spent_in(self, tx_hash: bytes, prefix: str) -> list:
        return []

    async def spending(self, tx_hash: bytes, prefix: str) -> list:
        return []

    async def transactions(
        self,
        address: bytes,
        *,
        is_outgoing: Optional[bool] = None,
        include_zero_value: bool = False,
        page: Optional[int] = None,
        before_tx_id: Optional[int] = None,
        after_tx_id: Optional[int] = None,
        #: ``(tx_id, rows_of_it_already_delivered)`` -- see `paging_bounds`.
        before_row: Optional[tuple] = None,
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

        That maximum is PER PARTITION CLASS, and one number for all four reads
        is wrong in both directions: a class whose max is lower than the number
        chosen reads a page it has no rows on and returns NOTHING, so a merged
        listing silently becomes single-direction. An address with two outgoing
        pages and one incoming page loses every incoming transaction, which
        reads as "these transactions do not exist" rather than as an error.
        `address_tx_pages` is keyed per class for the same reason.
        """
        directions = (False, True) if is_outgoing is None else (is_outgoing,)
        zero_flags = (False, True) if include_zero_value else (False,)

        cursors: dict = {}
        if page is None:
            stats = await self.stats(address)
            if stats is None:
                return []
            cursors = stats.epoch_zero

        # Both bounds are clustering restrictions on tx_id, so a range read.
        # `after_tx_id` is what a min_height filter becomes -- without it the
        # height is only a hint about which page to start on, and rows BELOW it
        # come back anyway.
        clause, extra, skip = self.paging_bounds(before_row, before_tx_id, after_tx_id)

        # Gathered per (direction, zero-ness) rather than through `_gather`,
        # which flattens: the DIRECTION is not on the row, it is in the
        # partition key, so flattening loses it. A caller cannot re-derive it,
        # and v2 signs an outgoing value negative -- so a lost direction is a
        # wrong sign on every row of an unbounded listing.
        specs = [(outgoing, zero) for outgoing in directions for zero in zero_flags]
        results = await asyncio.gather(
            *(
                self._select(
                    f"SELECT tx_id, value, balance, currency, tx_reference FROM "
                    f"{self.derived}.address_transactions "
                    f"WHERE address = %s AND is_outgoing = %s AND is_zero_value = %s "
                    f"AND tx_page = %s{clause} LIMIT {int(limit) + skip}",
                    (
                        address,
                        outgoing,
                        zero,
                        page if page is not None else page_max(cursors, outgoing, zero),
                    )
                    + extra,
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
                currency=getattr(row, "currency", None),
                tx_reference=getattr(row, "tx_reference", None),
            )
            for (outgoing, _zero), rows in zip(specs, results)
            for row in rows
        ]
        merged.sort(key=lambda tx: tx.tx_id, reverse=True)
        # `skip` rows of the boundary transaction were already delivered. The
        # merge is deterministic -- each partition arrives in clustering order
        # and the sort is stable over a fixed spec list -- so the same rows come
        # back in the same order and dropping the leading `skip` is exact.
        return merged[skip : skip + limit]

    async def link_transactions(
        self, src: bytes, dst: bytes, *, limit: int = 100
    ) -> list:
        """The transactions on one edge, newest first.

        The account link table is keyed ``(src, dst, tx_page)`` where the page
        is the edge's own ordinal // `tx_page_size`. Ordinals ascend with
        tx_id, so page 0 holds the OLDEST transactions and the NEWEST are in
        `link_page_max` -- which is why that cursor has to be read first, and
        why reading page 0 would answer from the wrong end of the edge's
        history.

        The cursor lives on the relations row, and the bucket is computed from
        the counterparty, so finding it is a point read rather than a scan.
        A missing row means no such edge, and a missing cursor means an edge
        written before the backfill filled it -- page 0 is the right answer for
        both, since an edge that small has only one page anyway.
        """
        rows = await self._select(
            f"SELECT link_page_max FROM "
            f"{self.derived}.address_outgoing_relations "
            f"WHERE src_address = %s AND rel_bucket = %s AND dst_address = %s",
            (src, self.relation_bucket(dst), dst),
        )
        if not rows:
            return []
        page = max(
            (int(getattr(row, "link_page_max", 0) or 0) for row in rows), default=0
        )

        found: list = []
        # Walk DOWN from the newest page: a page holds tx_page_size rows, so a
        # limit smaller than that -- every real request -- stops on the first.
        while page >= 0 and len(found) < limit:
            slice_ = await self._select(
                f"SELECT tx_id, tx_reference, currency, value FROM "
                f"{self.derived}.address_link_transactions "
                f"WHERE src_address = %s AND dst_address = %s AND tx_page = %s "
                f"LIMIT {int(limit) - len(found)}",
                (src, dst, page),
            )
            found += [
                {
                    "tx_id": r.tx_id,
                    "tx_reference": r.tx_reference,
                    "currency": r.currency,
                    "value": int(r.value or 0),
                }
                for r in slice_
            ]
            page -= 1
        return found


def dal_for(session, raw: str, derived: str, config: dict) -> Dal:
    """The reader for whichever family ``derived`` belongs to.

    Constructing `Dal` directly gives a reader with no `transactions` at all,
    which is deliberate: the base cannot answer that question without knowing
    the family, and a base that guessed is what served every token transfer as
    the native coin.
    """
    from graphsense_v3.schema.definitions import NETWORKS
    from graphsense_v3.schema.model import Family

    network = derived.split("_", 1)[0].lower()
    kind = AccountDal if NETWORKS.get(network) is Family.ACCOUNT else UtxoDal
    return kind(session, raw, derived, config)
