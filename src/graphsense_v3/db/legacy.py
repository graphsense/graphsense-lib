"""The v2 DAL surface, served from a v3 keyspace.

Exists for one purpose: running the existing service layer against v3 without
touching it, so the two backends can be compared on the REST output they
produce. It is an adapter, not a second DAL -- every read goes through
:class:`graphsense_v3.db.core.Dal`.

Three mismatches are structural, and none of them can be hidden:

* **v2 is id-based, v3 is byte-keyed.** v2 addresses an address by
  ``(address_id_group, address_id)``, a surrogate assigned at write time; v3
  keys on the address bytes and has no such id (dropping it is what removed the
  BTC int32 ceiling). The adapter therefore SYNTHESISES an id --
  :func:`synthetic_id` -- which is stable within a keyspace and meaningless
  across one. **Any comparison must exclude id fields**; they cannot agree, and
  a harness that compares them is measuring the adapter, not the backend.
* **Clusters do not exist yet.** Clustering is staged for a later run (D9), so
  the entity tables are not in the schema. Every entity method raises
  :class:`NotAvailable` rather than returning an empty cluster, which would read
  as "this address has no cluster" -- a wrong answer that looks like data.
* **Tags, pubkey and trace endpoints are out of scope.** They read keyspaces v3
  does not build.

What this is NOT: a migration path. The adapter's job ends when the comparison
does.
"""

from __future__ import annotations

import zlib
from typing import NamedTuple, Optional

from graphsense_v3.codec import encode_address, search_prefix
from graphsense_v3.db.core import Dal


class _Value(NamedTuple):
    """What `services.common.to_values` reads off a value.

    It takes ``.value`` and ``.fiat_values`` as ATTRIBUTES -- v2 hands back a
    driver UDT object, so a plain dict here raises AttributeError inside the
    service rather than at the boundary.
    """

    value: int
    fiat_values: list


class _Io(NamedTuple):
    """One input or output, as `services.common.io_from_rows` reads it.

    Attributes, not keys, and `address` is a LIST -- one output can pay several
    addresses. ``None`` there means a nonstandard I/O, which the service only
    emits when asked for; an empty list would be a standard I/O paying nobody.
    """

    address: Optional[list]
    value: int
    address_type: Optional[int] = None
    script_hex: Optional[bytes] = None
    txinwitness: Optional[list] = None
    sequence: Optional[int] = None


class NotAvailable(NotImplementedError):
    """A v2 method whose data v3 does not (yet) hold.

    Distinct from a bug: the caller asked for something real, and the honest
    answer is that this backend cannot serve it -- not an empty result.
    """


def synthetic_id(address: bytes) -> int:
    """A stable stand-in for v2's ``address_id``.

    v3 has no surrogate id: the address bytes ARE the key. But the service
    layer round-trips ids through paging tokens and neighbour lookups, so it
    needs *something* stable for the duration of a request.

    CRC-32 of the address, which is stable, cheap, and deliberately NOT
    v2's id -- there is no mapping between them, and pretending otherwise
    would make a comparison silently wrong instead of visibly incomparable.
    Collisions are possible and irrelevant here: nothing is looked up by this
    value, it only has to survive a round trip.
    """
    return zlib.crc32(address)


class LegacyAdapter:
    """One adapter over one Dal per currency, matching v2's method signatures.

    Every method takes ``currency`` first, as v2's does, so the service layer
    binds to it unchanged.
    """

    def __init__(self, dals: dict, *, stub_clusters: bool = False) -> None:
        self.dals = dals
        #: TODO(D9): remove once v3 has cluster tables. With this set, the nine
        #: cluster methods stop raising and `get_fresh_cluster_id` reports None
        #: -- which is v2's answer for "no fresh cluster", NOT v3's answer for
        #: "clustering is not built". It exists so the rest of the surface can
        #: be compared before D9 lands, and every report that uses it SAYS SO.
        #: Never default it to True: silence here is a false parity claim.
        self.stub_clusters = bool(stub_clusters)
        #: currency -> {ticker: row}, filled by preload_token_configuration.
        #: Only account networks appear; a UTXO one is answered without a query.
        self._token_config: dict = {}

    def _dal(self, currency: str) -> Dal:
        try:
            return self.dals[currency.lower()]
        except KeyError:
            raise NotAvailable(
                f"no v3 keyspace configured for {currency!r}; have "
                f"{', '.join(sorted(self.dals)) or '(none)'}"
            ) from None

    def _bytes(self, currency: str, address: str) -> bytes:
        """v2 passes addresses as strings; v3 keys on the packed bytes."""
        return encode_address(currency.lower(), address)

    # -- statistics and meta ----------------------------------------------

    def get_supported_currencies(self) -> list:
        return sorted(self.dals)

    async def get_currency_statistics(self, currency: str) -> Optional[dict]:
        """v2's ``summary_statistics`` row, by v2's names.

        The names are the contract, not the numbers: `StatsService` reads
        ``no_blocks``, ``no_transactions``, ``no_addresses``,
        ``no_address_relations``, ``no_clusters`` and ``timestamp`` off this
        dict directly, and v3 renamed two of them. ``no_blocks`` is v2's
        height-called-a-count, so it is the highest block PLUS ONE -- returning
        the height would be off by one everywhere it is used as a bound,
        including the default rate lookup, which asks for ``no_blocks - 1``.
        """
        row = await self._dal(currency).statistics()
        if row is None:
            return None
        highest = row.get("highest_block")
        return {
            "no_blocks": 0 if highest is None else int(highest) + 1,
            "no_transactions": int(row.get("no_transactions") or 0),
            "no_addresses": int(row.get("no_addresses") or 0),
            "no_address_relations": int(row.get("no_address_relations") or 0),
            # v3 has no clusters (D9). Zero is what keeps the model buildable;
            # `compare.IGNORED_FIELDS` records that it is not a measurement.
            "no_clusters": 0,
            "timestamp": int(row.get("timestamp") or 0),
        }

    def get_token_configuration(self, currency: str):
        """**Synchronous**, because the service protocol declares it so.

        Seven service protocols declare this ``def``, not ``async def``, and the
        services call it without awaiting. An ``async`` version here returns a
        coroutine that is then subscripted or iterated, which surfaces as a
        ``TypeError`` far from its cause -- plus a "never awaited" warning.

        The SHAPE is v2's too: ``{ticker: row}`` for an account network, and
        ``None`` for a UTXO one, where v2's loader is ``@eth``-gated and returns
        nothing. A ``{}`` here instead of ``None`` would be a different answer.
        """
        from graphsenselib.utils.rest_utils import is_eth_like

        key = currency.lower()
        if not is_eth_like(key):
            return None
        if key not in self._token_config:
            raise NotAvailable(
                "get_token_configuration is synchronous in the service protocol, "
                "so an account network's token configuration cannot be fetched "
                "on demand -- await preload_token_configuration() first"
            )
        return self._token_config[key]

    async def preload_token_configuration(self) -> None:
        """Load what the synchronous accessor above will hand out.

        Only account networks have any; a UTXO network is answered from
        `is_eth_like` without a query.
        """
        from graphsenselib.utils.rest_utils import is_eth_like

        for currency, dal in self.dals.items():
            key = currency.lower()
            if not is_eth_like(key):
                continue
            rows = await dal.token_configuration()
            self._token_config[key] = {
                row["currency_ticker"]: row
                for row in rows
                if row.get("currency_ticker")
            }

    def _fiat_list(self, currency: str, fiat_values) -> list:
        """v2's ordered ``[{code, value}]`` from v3's ``{code: value}`` map.

        Both backends hold the same numbers; only the representation differs.
        v2 stores a LIST positionally aligned with the keyspace's
        ``fiat_currencies`` and marks it up on read; v3 stores a map and keeps
        the order in ``configuration``. The service layer's `to_values` wants
        v2's form, so a map handed over raw fails model validation.
        """
        order = [
            str(code).lower()
            for code in (self._dal(currency).config.get("fiat_currencies") or [])
        ]
        values = {str(k).lower(): v for k, v in (fiat_values or {}).items()}
        # Sorted, not arbitrary, when the keyspace does not say: the order is
        # positional in v2's response, so an unstable one is a wrong answer.
        return [
            {"code": code, "value": float(values.get(code) or 0.0)}
            for code in (order or sorted(values))
        ]

    # -- blocks ------------------------------------------------------------

    async def get_block(self, currency: str, height: int) -> Optional[dict]:
        return await self._dal(currency).block(height)

    async def get_block_timestamp(self, currency: str, height: int):
        block = await self._dal(currency).block(height)
        return None if block is None else block.get("timestamp")

    async def list_block_txs(self, currency: str, height: int) -> list:
        dal = self._dal(currency)
        found = await dal.block_transactions(height)
        legs = await dal.transaction_io_many([tx["tx_id"] for tx in found])
        return [self._with_io(currency, tx, legs.get(tx["tx_id"], [])) for tx in found]

    def _with_io(self, currency: str, detail: dict, legs: list) -> dict:
        """A v3 transaction row plus the ``inputs``/``outputs`` v2 carries.

        v2 stores the I/Os ON the transaction row; v3 keeps them in
        `transaction_io` under the same partition key, so this is an assembly
        rather than a lookup. `std_tx_from_row` reads ``row["inputs"]`` by
        SUBSCRIPT, so an absent key is a KeyError several layers from its
        cause, not a missing field.
        """
        from graphsense_v3.codec import decode_address

        network = currency.lower()
        inputs: list = []
        outputs: list = []
        for leg in sorted(
            legs, key=lambda r: (bool(r.get("is_output")), r.get("io_index") or 0)
        ):
            decoded = [
                decode_address(network, bytes(a)) for a in (leg.get("address") or [])
            ]
            io = _Io(
                # None, not [] -- the service treats None as a nonstandard I/O
                # and an empty list as a standard one paying nobody.
                address=decoded or None,
                value=int(leg.get("value") or 0),
                address_type=leg.get("address_type"),
                script_hex=leg.get("script_hex"),
                txinwitness=leg.get("txinwitness"),
                sequence=leg.get("sequence"),
            )
            (outputs if leg.get("is_output") else inputs).append(io)
        row = dict(detail)
        row["inputs"] = inputs
        row["outputs"] = outputs
        # v3 names it block_timestamp, being the block's rather than the
        # transaction's; v2's readers ask for `timestamp`.
        row["timestamp"] = detail.get("block_timestamp")
        return row

    async def get_block_below_block_allow_filtering(
        self, currency: str, block_id: int
    ) -> Optional[dict]:
        """The highest block below ``block_id``.

        The name is v2's and so is the contract; the ``allow filtering`` is not.
        v2 scans the whole block table for a ``max()``; v3 reads the partition
        the height already names. Used by the block-by-date binary search.
        """
        return await self._dal(currency).block_below(block_id)

    async def get_block_by_date_allow_filtering(self, currency: str, timestamp: int):
        """v2 scans; v3 has ``block_by_date`` keyed by the day.

        The name is kept because the service layer calls it, but nothing here
        allows filtering -- the day is a partition key.
        """
        from datetime import datetime, timezone

        day = int(datetime.fromtimestamp(timestamp, tz=timezone.utc).strftime("%Y%m%d"))
        blocks = await self._dal(currency).blocks_on_day(day)
        for block in blocks:
            if block["timestamp"] >= timestamp:
                return block["block_id"]
        return blocks[-1]["block_id"] if blocks else None

    # -- rates -------------------------------------------------------------

    async def get_rates(self, currency: str, height: int) -> Optional[dict]:
        dal = self._dal(currency)
        native = (dal.config.get("keyspace_name") or currency).split("_")[0].upper()
        # `Dal.rate` returns the fiat map itself, not a row wrapping it.
        fiat = await dal.rate(native, height)
        if fiat is None:
            return None
        return {"block_id": height, "rates": self._fiat_list(currency, fiat)}

    async def list_rates(self, currency: str, heights) -> list:
        import asyncio

        return list(
            await asyncio.gather(*(self.get_rates(currency, h) for h in heights))
        )

    # -- transactions ------------------------------------------------------

    async def get_tx_by_hash(self, currency: str, tx_hash: bytes) -> Optional[dict]:
        dal = self._dal(currency)
        raw = bytes.fromhex(tx_hash) if isinstance(tx_hash, str) else bytes(tx_hash)
        prefix = raw.hex()[: dal.config["tx_prefix_length"]]
        tx_id = await dal.tx_id_by_hash(raw, prefix)
        if tx_id is None:
            return None
        detail = await dal.transaction(tx_id)
        if detail is None:
            return None
        return self._with_io(currency, detail, await dal.transaction_io(tx_id))

    async def get_tx(self, currency: str, tx_hash) -> Optional[dict]:
        return await self.get_tx_by_hash(currency, tx_hash)

    async def get_spending_txs(self, currency: str, tx_hash, io_index=None) -> list:
        dal = self._dal(currency)
        raw = bytes.fromhex(tx_hash) if isinstance(tx_hash, str) else bytes(tx_hash)
        rows = await dal.spending(raw, raw.hex()[: dal.config["tx_prefix_length"]])
        if io_index is None:
            return rows
        return [r for r in rows if r.get("spending_input_index") == io_index]

    async def get_spent_in_txs(self, currency: str, tx_hash, io_index=None) -> list:
        dal = self._dal(currency)
        raw = bytes.fromhex(tx_hash) if isinstance(tx_hash, str) else bytes(tx_hash)
        rows = await dal.spent_in(raw, raw.hex()[: dal.config["tx_prefix_length"]])
        if io_index is None:
            return rows
        return [r for r in rows if r.get("spent_output_index") == io_index]

    # -- addresses ---------------------------------------------------------

    async def get_address_id(self, currency: str, address: str):
        """v3 has no surrogate id. See :func:`synthetic_id`."""
        return synthetic_id(self._bytes(currency, address))

    async def get_address_id_id_group(self, currency: str, address: str):
        raw = self._bytes(currency, address)
        return synthetic_id(raw), self._dal(currency).entity_bucket(raw)

    async def get_address(self, currency: str, address: str) -> Optional[dict]:
        """v2's ``address`` row, assembled from v3's stats and balance.

        ``cluster_id`` is absent rather than zero: v3 has no clusters yet, and a
        zero would be read as cluster 0.
        """
        dal = self._dal(currency)
        raw = self._bytes(currency, address)
        stats = await dal.stats(raw)
        if stats is None:
            return None
        balances = await dal.balance(raw)
        native = next(iter(balances), None)
        row = {
            "address": address,
            "address_id": synthetic_id(raw),
            "address_id_group": dal.entity_bucket(raw),
            "first_tx_id": stats.first_tx_id,
            "last_tx_id": stats.last_tx_id,
            "balance": balances.get(native, 0) if native else 0,
            "balances": balances,
        }
        row.update(stats.summed)
        row.update(stats.epoch_zero)
        return row

    async def list_address_txs(
        self,
        currency: str,
        address: str,
        direction=None,
        min_height=None,
        max_height=None,
        order=None,
        token_currency=None,
        page=None,
        pagesize=None,
    ) -> tuple:
        """v2's signature; the height filter goes through the page index.

        ``min_height``/``max_height`` become a tx_id range, which is arithmetic
        (:func:`graphsense_v3.codec.tx_id_range`) -- but the ordinal pages are
        not tx_id-aligned, so the starting page still comes from
        ``address_tx_pages``.
        """
        dal = self._dal(currency)
        raw = self._bytes(currency, address)
        # v2 passes the STRING "in" or "out". `bool(direction)` is True for
        # both -- so an incoming listing silently returned outgoing rows, with
        # no error and a plausible-looking answer.
        is_outgoing = None if direction is None else "out" in str(direction).lower()
        before = None
        if page:
            # A resume token from a previous call. The format is OURS -- the
            # service treats it as opaque -- so it is the last tx_id handed
            # out, and `before_tx_id` is exclusive, which makes resuming exact
            # rather than off by one row.
            before = int(page)
        elif max_height is not None:
            from graphsense_v3.codec import tx_id_range

            before = tx_id_range(max_height, max_height)[1] + 1
        start_page = None
        if min_height is not None and is_outgoing is not None:
            from graphsense_v3.codec import tx_id_range

            low = tx_id_range(min_height, min_height)[0]
            start_page = await dal.page_for_tx(raw, is_outgoing, low)
        limit = int(pagesize or 100)
        found = await dal.transactions(
            raw,
            is_outgoing=is_outgoing,
            page=start_page,
            before_tx_id=before,
            limit=limit,
        )
        # A full page MAY have more behind it; a short one cannot. Returning
        # None unconditionally is what made v3 look like every address had
        # exactly one page -- a caller would never see past the first `pagesize`
        # transactions, and nothing would report an error.
        #
        # NOTE: this pages WITHIN one ordinal page, so an address with more
        # than tx_page_size transactions stops at that boundary. Nothing on LTC
        # comes close (tx_page_size is 100_000); crossing it needs the page
        # index, and the direction-merged case needs a cursor per direction.
        token = str(found[-1].tx_id) if found and len(found) == limit else None
        return await self._as_v2_txs(currency, found), token

    async def _as_v2_txs(self, currency: str, found: list) -> list:
        """v3's `AddressTx` rows as the dicts `txs_from_rows` reads.

        v3's address_transactions row is deliberately narrow -- ``tx_id`` and
        ``value``, with the tx_id carrying the height -- while the service
        needs the block's timestamp, the coinbase flag and the hash. Those live
        on the transaction, so they are fetched in ONE concurrent round rather
        than per row.
        """
        dal = self._dal(currency)
        by_id = await dal.transactions_by_ids([tx.tx_id for tx in found])
        rows = []
        for tx in found:
            detail = by_id.get(tx.tx_id)
            if detail is None:
                # A tx the address references but the raw keyspace lacks is a
                # torn keyspace, not a row to quietly drop.
                raise NotAvailable(
                    f"address_transactions references tx_id {tx.tx_id}, which "
                    f"{dal.raw}.transaction does not have"
                )
            rows.append(
                {
                    "height": detail.get("block_id"),
                    # v3 names it block_timestamp, being the block's and not
                    # the transaction's; v2's readers expect `timestamp`.
                    "timestamp": detail.get("block_timestamp"),
                    "coinbase": bool(detail.get("coinbase")),
                    "tx_hash": detail.get("tx_hash"),
                    # v2 signs by direction: money leaving is negative. v3
                    # stores the magnitude and the direction separately.
                    "value": -tx.value if tx.is_outgoing else tx.value,
                    "tx_id": tx.tx_id,
                }
            )
        return rows

    async def list_address_links(
        self,
        currency: str,
        address: str,
        neighbor: str,
        min_height=None,
        max_height=None,
        order=None,
        # Passed BY KEYWORD by addresses_service, so the name is part of the
        # contract. A UTXO keyspace holds one asset, so there is nothing to
        # filter on; it is accepted and ignored rather than rejected.
        token_currency=None,
        page=None,
        pagesize=None,
    ) -> list:
        dal = self._dal(currency)
        return await dal.link_transactions(
            self._bytes(currency, address),
            self._bytes(currency, neighbor),
            limit=int(pagesize or 100),
        )

    async def list_neighbors(
        self,
        currency: str,
        id,
        is_outgoing: bool,
        node_type=None,
        targets=None,
        include_labels=False,
        page=None,
        pagesize=None,
    ) -> tuple:
        """``id`` is an ADDRESS here, not v2's numeric id.

        The service layer passes whatever ``get_address_id`` returned, which
        for this adapter is a synthetic value that cannot be reversed. Callers
        must hand the address itself; anything else raises rather than silently
        returning nothing.
        """
        if not isinstance(id, (bytes, bytearray, str)):
            raise NotAvailable(
                "list_neighbors needs an address; v3 has no surrogate id to "
                "resolve a numeric one back to an address"
            )
        dal = self._dal(currency)
        raw = id if isinstance(id, (bytes, bytearray)) else self._bytes(currency, id)
        if targets:
            found = []
            for target in targets:
                key = (
                    target
                    if isinstance(target, (bytes, bytearray))
                    else self._bytes(currency, target)
                )
                edge = await dal.neighbor(raw, bytes(key), is_outgoing=is_outgoing)
                if edge is not None:
                    found.append(edge)
        else:
            found = await dal.neighbors(bytes(raw), is_outgoing=is_outgoing)
        return self._as_v2_neighbors(currency, found, is_outgoing), None

    def _as_v2_neighbors(self, currency: str, found: list, is_outgoing: bool) -> list:
        """v3's `Neighbor` rows as the dicts the service reads.

        Two conversions. The counterparty is keyed ``dst_address`` or
        ``src_address`` by DIRECTION, which is how the service finds it; and
        ``value`` becomes an object with ``.value`` and ``.fiat_values``,
        because `to_values` reads attributes rather than keys.
        """
        from graphsense_v3.codec import decode_address

        side = "dst_address" if is_outgoing else "src_address"
        rows = []
        for edge in found:
            rows.append(
                {
                    # The DECODED string: the service hands this straight to
                    # `address_to_user_format`, which passes a UTXO address
                    # through unchanged -- raw bytes would reach the response.
                    side: decode_address(currency.lower(), bytes(edge.address)),
                    # The service reads this by SUBSCRIPT before anything else,
                    # then feeds it to get_fresh_cluster_id. Absent, the whole
                    # call dies with a KeyError that names no cause; present,
                    # the call fails honestly on "v3 has no cluster tables".
                    f"{side}_id": synthetic_id(bytes(edge.address)),
                    "no_transactions": edge.no_transactions,
                    "value": _Value(
                        value=int(getattr(edge.value, "value", edge.value) or 0),
                        fiat_values=self._fiat_list(
                            currency, getattr(edge.value, "fiat_values", None)
                        ),
                    ),
                    "token_values": None,
                    "labels": None,
                }
            )
        return rows

    async def list_matching_addresses(
        self, currency: str, expression: str, limit: Optional[int] = 10
    ) -> list:
        """Prefix search.

        v3's index stores the prefix lowercased with the network's dead leading
        run stripped, so the expression goes through the same function -- v2's
        index does neither, and comparing the two raw would find nothing.
        """
        dal = self._dal(currency)
        prefix = search_prefix(
            currency.lower(), expression, dal.config["address_prefix_length"]
        )
        found = await dal.search_addresses(prefix, limit=int(limit or 10))
        from graphsense_v3.codec import decode_address

        decoded = [decode_address(currency.lower(), a) for a in found]
        return [a for a in decoded if a.lower().startswith(expression.lower())]

    # -- not served by v3 --------------------------------------------------

    def _no_clusters(self, method: str):
        raise NotAvailable(
            f"{method}: v3 has no cluster tables yet (clustering is staged for a "
            "later run, D9). Returning an empty cluster would read as 'this "
            "address has no cluster', which is a wrong answer that looks like data."
        )

    async def get_entity(self, *_, **__):
        self._no_clusters("get_entity")

    async def get_entities_by_ids(self, *_, **__):
        self._no_clusters("get_entities_by_ids")

    async def list_entity_txs(self, *_, **__):
        self._no_clusters("list_entity_txs")

    async def list_entity_links(self, *_, **__):
        self._no_clusters("list_entity_links")

    async def list_entity_addresses(self, *_, **__):
        self._no_clusters("list_entity_addresses")

    async def get_address_entity_id(self, *_, **__):
        self._no_clusters("get_address_entity_id")

    async def get_fresh_cluster_id(self, *_, **__):
        """The one cluster method that can be stubbed usefully.

        `addresses_service` calls it for EVERY neighbour of a non-eth address
        and for `get_address`, so while it raises, most of the address surface
        cannot be exercised at all -- including parts that have nothing to do
        with clustering. Returning None under `stub_clusters` is v2's own value
        for "no fresh cluster", so the call completes and the cluster FIELDS are
        excluded from the comparison rather than silently agreeing.
        """
        if self.stub_clusters:
            return None
        self._no_clusters("get_fresh_cluster_id")

    async def new_entity(self, *_, **__):
        self._no_clusters("new_entity")

    async def get_addresses_light(self, *_, **__):
        self._no_clusters("get_addresses_light (returns cluster_id)")

    async def new_address(self, *_, **__):
        raise NotAvailable(
            "new_address: v3 assigns no address ids -- the address bytes are "
            "the key, which is what removed the int32 id ceiling"
        )

    async def get_cross_chain_pubkey_related_addresses(self, *_, **__):
        raise NotAvailable(
            "the pubkey dataset is a separate keyspace that v3 does not build"
        )

    async def fetch_transaction_traces(self, *_, **__):
        raise NotAvailable("traces are a raw account table; not wired up yet")

    async def fetch_transaction_trace(self, *_, **__):
        raise NotAvailable("traces are a raw account table; not wired up yet")

    async def list_token_txs(self, *_, **__):
        raise NotAvailable("token transactions are not wired through the adapter yet")

    async def get_token_rate(self, *_, **__):
        raise NotAvailable(
            "per-token rates live in the merged exchange_rates table; use "
            "Dal.rate(asset, block_id) directly until this is wired up"
        )

    async def list_matching_txs(self, *_, **__):
        raise NotAvailable(
            "transaction prefix search needs a scan of transaction_by_tx_prefix; "
            "not wired up yet"
        )
