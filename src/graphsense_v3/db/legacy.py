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
from typing import Optional

from graphsense_v3.codec import encode_address, search_prefix
from graphsense_v3.db.core import Dal


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

    def __init__(self, dals: dict) -> None:
        self.dals = dals

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
        return await self._dal(currency).statistics()

    async def get_token_configuration(self, currency: str) -> list:
        return await self._dal(currency).token_configuration()

    # -- blocks ------------------------------------------------------------

    async def get_block(self, currency: str, height: int) -> Optional[dict]:
        return await self._dal(currency).block(height)

    async def get_block_timestamp(self, currency: str, height: int):
        block = await self._dal(currency).block(height)
        return None if block is None else block.get("timestamp")

    async def list_block_txs(self, currency: str, height: int) -> list:
        return await self._dal(currency).block_transactions(height)

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
        fiat = await dal.rate(native, height)
        return None if fiat is None else {"block_id": height, "rates": fiat}

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
        return None if tx_id is None else await dal.transaction(tx_id)

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
    ) -> list:
        """v2's signature; the height filter goes through the page index.

        ``min_height``/``max_height`` become a tx_id range, which is arithmetic
        (:func:`graphsense_v3.codec.tx_id_range`) -- but the ordinal pages are
        not tx_id-aligned, so the starting page still comes from
        ``address_tx_pages``.
        """
        dal = self._dal(currency)
        raw = self._bytes(currency, address)
        is_outgoing = None if direction is None else bool(direction)
        before = None
        if max_height is not None:
            from graphsense_v3.codec import tx_id_range

            before = tx_id_range(max_height, max_height)[1] + 1
        start_page = None
        if min_height is not None and is_outgoing is not None:
            from graphsense_v3.codec import tx_id_range

            low = tx_id_range(min_height, min_height)[0]
            start_page = await dal.page_for_tx(raw, is_outgoing, low)
        return await dal.transactions(
            raw,
            is_outgoing=is_outgoing,
            page=start_page,
            before_tx_id=before,
            limit=int(pagesize or 100),
        )

    async def list_address_links(
        self,
        currency: str,
        address: str,
        neighbor: str,
        min_height=None,
        max_height=None,
        order=None,
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
    ) -> list:
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
            return found
        return await dal.neighbors(bytes(raw), is_outgoing=is_outgoing)

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
