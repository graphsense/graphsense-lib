"""Back-to-back comparison of the v2 and v3 backends, at the service level.

The question this answers is the only one that matters for v3: **does REST give
the same answer from either backend?** Not "do the tables look right" -- a
schema can be defensible and still change what a caller sees.

So the comparison runs where the REST data is actually shaped: the service
layer (`graphsenselib.db.asynchronous.services`). Two
:class:`~graphsenselib.web.dependencies.ServiceContainer` instances are built
over the same config, one holding the v2 DAL and one holding
:class:`graphsense_v3.db.legacy.LegacyAdapter`, and the same call is made
against both. Anything the service layer computes on top of the DAL -- rate
conversion, address canonicalisation, response assembly -- is therefore
included, which is the point of comparing here rather than at the DAL.

**The tagstore is deliberately absent.** Both containers get
``tagstore_db=None``, which substitutes ``MockTagstoreDb``, so tags cannot
contribute to either side. A difference in this report is a Cassandra
difference.

Three things this file refuses to do, each because the alternative would report
a parity that does not exist:

* **A call that cannot be made is `skipped`, never an agreement.** v3 has no
  cluster tables, so the adapter raises :class:`NotAvailable` for nine methods.
  Recording those as "no differences found" would claim parity for the single
  largest missing feature.
* **An exception is part of the answer.** If v2 raises ``AddressNotFound`` and
  v3 returns an empty address, that is a difference, not a pass. Both sides are
  run to an outcome -- a value or an exception type -- and the outcomes are
  compared.
* **Addresses are spelled per backend.** The LTC lake predates the 2026-06-15
  P2PK fix, so v3 holds ``1...`` where v2 holds ``L...`` for the same hash160.
  Passing one spelling to both would look up a nonexistent address on one side
  and report a difference that is an artifact of the fixture, not the backend.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Optional

from graphsense_v3 import compare

logger = logging.getLogger(__name__)

#: What a call needs from a fixture before it can run.
ADDRESS = "address"
TX_HASH = "tx_hash"
BLOCK = "block"
NOTHING = "nothing"


@dataclass
class Fixtures:
    """The concrete values the calls are made against.

    Addresses are held in **v3's spelling**; :func:`v2_spelling` converts on the
    way to the v2 side. Auto-selection reads them out of the v3 keyspace, so
    that is the spelling that arrives here.
    """

    network: str
    addresses: list = field(default_factory=list)
    tx_hashes: list = field(default_factory=list)
    blocks: list = field(default_factory=list)

    def values_for(self, needs: str) -> list:
        if needs is ADDRESS or needs == ADDRESS:
            return self.addresses
        if needs == TX_HASH:
            return self.tx_hashes
        if needs == BLOCK:
            return self.blocks
        return [None]


def v2_spelling(network: str, address: str) -> str:
    """``address`` as the v2 keyspace spells it.

    v2 was written by an ingest that already had the network-aware P2PK fix, so
    it holds the chain's own version byte; the v3 lake predates it. Re-versioning
    is a no-op for every address written after the fix, which is the vast
    majority -- it only moves the early-chain P2PK ones.
    """
    return compare.reversion_address(network, address)


@dataclass
class Call:
    """One service call, made against both backends."""

    label: str
    needs: str
    invoke: Callable[..., Awaitable]


#: How much of an exception message a difference line carries.
MESSAGE_LIMIT = 160


async def _outcome(coro: Awaitable) -> tuple:
    """``(kind, value, elapsed_ms)`` -- "ok" with the result, or "raised" with
    "Type: message".

    An exception is an answer: two backends that both raise
    ``AddressNotFoundException`` agree, and one that raises where the other
    returns does not.

    The MESSAGE is carried, not just the type. A report saying only
    ``raised:TypeError`` names a symptom a dozen unrelated causes share and
    sends the reader back to the cluster to find out which -- answering that in
    one run is the whole point of the harness. The traceback goes to the debug
    log for when the message alone is not enough.
    """
    from graphsense_v3.db.legacy import NotAvailable

    started = time.perf_counter()
    try:
        return "ok", await coro, (time.perf_counter() - started) * 1000
    except NotAvailable:
        raise
    except Exception as exc:  # noqa: BLE001 -- the exception IS the observation
        logger.debug("call raised", exc_info=True)
        message = str(exc).replace("\n", " ")[:MESSAGE_LIMIT]
        name = type(exc).__name__
        return (
            "raised",
            f"{name}: {message}" if message else name,
            (time.perf_counter() - started) * 1000,
        )


def to_plain(value: Any) -> Any:
    """A service response as plain data.

    The services return pydantic models; `compare` walks dicts and lists. Left
    alone, two models compare by identity and every call would "differ".
    """
    if hasattr(value, "model_dump"):
        return value.model_dump()
    if hasattr(value, "dict") and callable(value.dict):
        return value.dict()
    if isinstance(value, (list, tuple)):
        return [to_plain(item) for item in value]
    if isinstance(value, dict):
        return {key: to_plain(item) for key, item in value.items()}
    return value


async def run_call(
    call: Call, v2_services: Any, v3_services: Any, network: str, value: Any
) -> compare.Report:
    """One call against both backends, as one :class:`compare.Report`."""
    from graphsense_v3.db.legacy import NotAvailable

    label = f"{call.label}({value})" if value is not None else call.label
    v2_value = v2_spelling(network, value) if call.needs == ADDRESS else value
    try:
        left = await _outcome(call.invoke(v2_services, network, v2_value))
    except NotAvailable as exc:
        return compare.skipped(label, f"v2 side unavailable: {exc}")
    try:
        right = await _outcome(call.invoke(v3_services, network, value))
    except NotAvailable as exc:
        return compare.skipped(label, str(exc))

    left_kind, left_body, left_ms = left
    right_kind, right_body, right_ms = right
    if left_kind == "raised" or right_kind == "raised":
        # Compared as bare strings so that "both raised AddressNotFound" is an
        # agreement and "one raised" is a single, readable difference.
        report = compare.compare(
            label,
            f"{left_kind}:{left_body}" if left_kind == "raised" else "ok",
            f"{right_kind}:{right_body}" if right_kind == "raised" else "ok",
            network,
        )
    else:
        report = compare.compare(
            label, to_plain(left_body), to_plain(right_body), network
        )
    # Only time calls that BOTH sides completed: a raised call measures how
    # fast something failed, which would flatter whichever side broke earlier.
    if left_kind == "ok" and right_kind == "ok":
        report.left_ms, report.right_ms = left_ms, right_ms
    return report


async def run(
    v2_services: Any,
    v3_services: Any,
    fixtures: Fixtures,
    calls: Optional[list] = None,
) -> list:
    """Every call against every fixture it applies to."""
    reports = []
    for call in calls if calls is not None else CALLS:
        for value in fixtures.values_for(call.needs):
            reports.append(
                await run_call(call, v2_services, v3_services, fixtures.network, value)
            )
    return reports


# --------------------------------------------------------------------------
# The calls.
#
# Only what REST actually serves from Cassandra. Each is written against the
# SERVICE, not the DAL, so the response is the one a caller would receive.
# --------------------------------------------------------------------------

CALLS: list = [
    Call(
        "get_address",
        ADDRESS,
        lambda s, n, v: s.addresses_service.get_address(
            n, v, tagstore_groups=[], include_actors=False
        ),
    ),
    Call(
        "list_address_txs",
        ADDRESS,
        lambda s, n, v: s.addresses_service.list_address_txs(n, v, pagesize=20),
    ),
    Call(
        "list_address_txs_in",
        ADDRESS,
        lambda s, n, v: s.addresses_service.list_address_txs(
            n, v, direction="in", pagesize=20
        ),
    ),
    Call(
        "list_address_neighbors_out",
        ADDRESS,
        lambda s, n, v: s.addresses_service.list_address_neighbors(
            n,
            v,
            direction="out",
            tagstore_groups=[],
            include_labels=False,
            include_actors=False,
            pagesize=20,
        ),
    ),
    Call(
        "list_address_neighbors_in",
        ADDRESS,
        lambda s, n, v: s.addresses_service.list_address_neighbors(
            n,
            v,
            direction="in",
            tagstore_groups=[],
            include_labels=False,
            include_actors=False,
            pagesize=20,
        ),
    ),
    Call(
        "get_address_entity",
        ADDRESS,
        lambda s, n, v: s.addresses_service.get_address_entity(
            n, v, include_actors=False, tagstore_groups=[]
        ),
    ),
    Call("get_block", BLOCK, lambda s, n, v: s.blocks_service.get_block(n, v)),
    Call(
        "list_block_txs",
        BLOCK,
        lambda s, n, v: s.blocks_service.list_block_txs(n, v),
    ),
    Call(
        # include_io is not optional for a meaningful UTXO comparison: without
        # it the response carries no inputs or outputs at all, so two backends
        # would agree on a body that omits the part most likely to differ.
        "get_tx",
        TX_HASH,
        lambda s, n, v: s.txs_service.get_tx(
            n,
            v,
            include_io=True,
            include_nonstandard_io=True,
            include_io_index=True,
            tagstore_groups=[],
        ),
    ),
    Call(
        "get_currency_statistics",
        NOTHING,
        lambda s, n, v: s.stats_service.get_currency_statistics(n),
    ),
]


def rate_coverage_warning(
    session: Any, raw: str, derived: str, network: str, size: int
) -> Optional[str]:
    """A warning when the keyspace's tip has no exchange rate, else None.

    Worth its own check because of how it PRESENTS: every call that asks for
    current rates resolves the height to ``no_blocks - 1``, so one missing rate
    row at the tip becomes an identical `BlockNotFoundException` on
    `get_address` and on every neighbour listing -- N copies of one fact,
    which is the kind of noise a real finding hides in.
    """
    rows = list(session.execute(f"SELECT highest_block FROM {raw}.summary_statistics"))
    if not rows or rows[0].highest_block is None:
        return None
    tip = int(rows[0].highest_block)
    rated = rated_block(session, derived, network, tip, size)
    if rated is not None and rated >= tip:
        return None
    return (
        f"{derived} has no exchange rate for block {tip}, its own tip"
        + (f" (the last rated block is {rated})" if rated is not None else "")
        + ". Every call that asks for CURRENT rates resolves to that block and "
        "will fail with BlockNotFoundException on the v3 side -- get_address "
        "and both neighbour listings among them. This is a property of the "
        "keyspace, not a difference between the backends: a backfill carrying "
        "the last rate forward over the unrated tail fixes it."
    )


def sample_addresses(
    session: Any, derived: str, network: str, count: int, *, buckets: int
) -> list:
    """``count`` addresses spread across the keyspace, as strings.

    Two axes, because `address_stats` is ``PRIMARY KEY (address_bucket,
    address, epoch)`` -- the partition key is the BUCKET alone, and the address
    is a clustering column:

    * a random bucket, so the draw is spread over the ring;
    * a random floor WITHIN it, because taking each partition's first row would
      always return its lowest-sorting address. That is not a harmless bias:
      the encoded form starts with a type marker, so the lowest address in a
      bucket is systematically the same address TYPE, and the sample would
      quietly exclude the others.

    Each draw is one point read. Fewer than ``count`` may come back -- draws are
    independent, so duplicates happen and are dropped rather than re-drawn.
    """
    import random

    from graphsense_v3.codec import decode_address

    cql = (
        f"SELECT address FROM {derived}.address_stats "
        f"WHERE address_bucket = %s AND address >= %s LIMIT 1"
    )
    found: list = []
    seen: set = set()
    for _ in range(count):
        bucket = random.randrange(buckets)
        floor = bytes([random.randrange(256)])
        rows = list(session.execute(cql, (bucket, floor)))
        if not rows:
            # The floor landed past the end of this bucket; take it from the
            # start rather than losing the draw.
            rows = list(session.execute(cql, (bucket, b"")))
        for row in rows:
            raw = bytes(row.address)
            if raw in seen:
                continue
            seen.add(raw)
            found.append(decode_address(network, raw))
    return found


#: How far below an unrated block to look for a rated one, in rate partitions.
RATED_FIXTURE_MAX_GROUPS = 50


def rated_block(session: Any, derived: str, network: str, block_id: int, size: int):
    """``block_id``, or the highest rated block at or below it.

    A fixture in an unrated block is not comparable. The REST rates service
    raises ``BlockNotFoundException`` for a block with no rate rather than
    degrading, so every call touching it fails on the v3 side for a reason that
    has nothing to do with the row under test -- which is how a one-day rate lag
    turned into four "differences" in the first run.

    Rates arrive a day at a time, so the answer is normally in the block's own
    partition or the one below it.
    """
    asset = network.upper()
    group = block_id // size
    for _ in range(RATED_FIXTURE_MAX_GROUPS):
        if group < 0:
            break
        rows = list(
            session.execute(
                f"SELECT block_id FROM {derived}.exchange_rates "
                f"WHERE asset = %s AND block_id_group = %s AND block_id <= %s "
                f"ORDER BY block_id DESC LIMIT 1",
                (asset, group, block_id),
            )
        )
        if rows:
            return rows[0].block_id
        group -= 1
        # Ask the next partition down for its own top block, not the original.
        block_id = (group + 1) * size - 1
    return None


def fixtures_from_v3(session: Any, raw: str, derived: str, network: str) -> Fixtures:
    """Fixtures discovered in the v3 keyspace, in v3's spelling.

    Reuses the probe's discovery rather than repeating it: it already finds a
    real address, transaction and block by reading the keyspace, and a second
    implementation would be a second thing to keep true.

    v3 is the side that is asked first because it is the smaller and newer
    keyspace -- a fixture that exists in v3 exists in v2, while the reverse does
    not hold for a v3 run bounded below the chain tip.
    """
    from graphsense_v3 import probe as prober
    from graphsense_v3.codec import decode_address

    config = prober.configuration(session, derived, fallback=raw)
    found = prober.Prober(session, raw, derived, config).fixtures()
    addresses = [
        decode_address(network, bytes(a))
        for a in (found.address, found.busiest_address)
        if a
    ]
    block = found.block_id
    if block is not None:
        rated = rated_block(
            session, derived, network, block, config.get("block_bucket_size") or 100
        )
        if rated is None:
            logger.warning(
                "no rated block at or below %d; block fixtures will fail on the "
                "v3 side for want of an exchange rate, not for a data difference",
                block,
            )
        elif rated != block:
            logger.warning(
                "block %d has no exchange rate; using %d instead", block, rated
            )
            block = rated
    return Fixtures(
        network=network,
        # dict.fromkeys rather than set(): the order a report lists its calls in
        # should not change between runs on identical data.
        addresses=list(dict.fromkeys(addresses)),
        tx_hashes=[bytes(found.tx_hash).hex()] if found.tx_hash else [],
        blocks=[block] if block is not None else [],
    )


def with_port(nodes: list, port: Optional[int]) -> list:
    """Contact points carrying ``port``, for those that do not name their own.

    The v2 DAL takes the port from ``database.port`` while the v3 session takes
    it from the ``host:port`` contact string. Left unreconciled, a config with a
    non-default port sends the two sides to different endpoints -- and the
    comparison would be measuring two clusters, not two backends.
    """
    if not port:
        return list(nodes)
    return [node if ":" in node else f"{node}:{port}" for node in nodes]


def build_services(config: Any, db: Any, log: Any = None) -> Any:
    """A :class:`ServiceContainer` over ``db`` with **no tagstore**.

    ``tagstore_db=None`` substitutes ``MockTagstoreDb``, which is the isolation
    this comparison depends on: with a real tagstore attached, a tag difference
    would surface as a service difference and be read as a Cassandra bug.
    """
    from graphsenselib.db.asynchronous.services.tags_service import (
        MockConceptProtocol,
    )
    from graphsenselib.web.dependencies import ServiceContainer

    return ServiceContainer(
        config=config,
        db=db,
        tagstore_db=None,
        concepts_cache_service=MockConceptProtocol(),
        logger=log or logger,
    )
