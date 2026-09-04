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
    """``("ok", value)`` or ``("raised", "Type: message")``.

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

    try:
        return "ok", await coro
    except NotAvailable:
        raise
    except Exception as exc:  # noqa: BLE001 -- the exception IS the observation
        logger.debug("call raised", exc_info=True)
        message = str(exc).replace("\n", " ")[:MESSAGE_LIMIT]
        name = type(exc).__name__
        return "raised", f"{name}: {message}" if message else name


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

    left_kind, left_body = left
    right_kind, right_body = right
    if left_kind == "raised" or right_kind == "raised":
        # Compared as bare strings so that "both raised AddressNotFound" is an
        # agreement and "one raised" is a single, readable difference.
        return compare.compare(
            label,
            f"{left_kind}:{left_body}" if left_kind == "raised" else "ok",
            f"{right_kind}:{right_body}" if right_kind == "raised" else "ok",
            network,
        )
    return compare.compare(label, to_plain(left_body), to_plain(right_body), network)


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
    return Fixtures(
        network=network,
        # dict.fromkeys rather than set(): the order a report lists its calls in
        # should not change between runs on identical data.
        addresses=list(dict.fromkeys(addresses)),
        tx_hashes=[bytes(found.tx_hash).hex()] if found.tx_hash else [],
        blocks=[found.block_id] if found.block_id is not None else [],
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
