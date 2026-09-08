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

from graphsenselib.datatypes.abi import decode_logs_db
from graphsenselib.utils import strip_0x
from graphsenselib.utils.function_call_parser import (
    function_signatures as function_call_signatures,
)
from graphsenselib.utils.function_call_parser import parse_function_call

from graphsense_v3.codec import encode_address, search_prefix
from graphsense_v3.db.core import Dal, NotAvailable


class _Value(NamedTuple):
    """What `services.common.to_values` reads off a value.

    It takes ``.value`` and ``.fiat_values`` as ATTRIBUTES -- v2 hands back a
    driver UDT object, so a plain dict here raises AttributeError inside the
    service rather than at the boundary.
    """

    value: int
    fiat_values: list


class _Rows(list):
    """A list that also answers ``.current_rows``.

    `txs_service.get_spent_in_txs` and `get_spending_txs` iterate
    ``results.current_rows``, because v2 hands back a driver ``ResultSet`` and
    that is how you read one. A plain list raises AttributeError there --
    reported from inside the service, naming neither the method nor the cause.

    A list subclass rather than a wrapper: everything else that touches these
    rows treats them as a list, and the one caller that wants the ResultSet
    shape gets it without the rest having to know.
    """

    @property
    def current_rows(self) -> list:
        return self


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


def decode_page_token(page) -> tuple:
    """``"<tx_id>:<delivered>"`` -> ``(tx_id, delivered)``.

    Tolerates a bare tx_id so a token minted before the count existed still
    resumes -- as zero delivered, which re-reads the boundary transaction
    rather than skipping it. Re-reading is the safe direction to be wrong in.
    """
    text = str(page)
    head, _, tail = text.partition(":")
    return int(head), int(tail or 0)


def encode_page_token(found: list, before_row: Optional[tuple]) -> str:
    """The cursor for the page just handed out.

    ``delivered`` counts the rows of the LAST transaction that the caller has
    now seen -- this page's, plus any it had already been given if the previous
    page ended inside the same transaction.
    """
    last = found[-1].tx_id
    delivered = sum(1 for row in found if row.tx_id == last)
    if before_row is not None and before_row[0] == last:
        delivered += before_row[1]
    return f"{last}:{delivered}"


def block_of_tx_id(tx_id: int) -> int:
    """The block a tx_id names. Arithmetic, per `codec.tx_id_expr`."""
    from graphsense_v3.codec import block_of_tx_id as decode

    return decode(tx_id)


def _fee_of(detail: dict) -> Optional[int]:
    """gas used * the price actually paid, which is what v2 reports.

    `receipt_effective_gas_price` is the post-1559 price and `gas_price` the
    pre-1559 one; a transaction carries whichever its era used.
    """
    used = detail.get("receipt_gas_used")
    price = detail.get("receipt_effective_gas_price") or detail.get("gas_price")
    if used is None or price is None:
        return None
    return int(used) * int(price)


#: ``Transfer(address,address,uint256)``. The one log signature a token
#: transfer is, and the topic v2 restricts on (`cassandra.py:3613`).
TRANSFER_TOPIC = bytes.fromhex(
    "ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
)


def _address_bytes(address: Optional[str]) -> Optional[bytes]:
    """A decoded log parameter's ``0x...`` address as the bytes v3 keys on."""
    if not isinstance(address, str):
        return None
    stripped = strip_0x(address)
    return bytes.fromhex(stripped) if stripped else None


def _native_transfer_row(row: dict, detail: dict, trace: dict) -> None:
    """The columns a native account transfer takes off the TRACE it points at.

    EVERY one of them is the trace's, not the transaction's: v2 reads them off
    the trace (`cassandra.py:5049-5068`) and the two disagree for an internal
    call, which is most of the rows on an account listing. eth's `raw.trace`
    carries all three (`definitions._TRACE_EXTRA`), so none of this is a gap
    in v3 -- it was a reader that never asked.

    Shared by the address listing and by `/links` so the two cannot drift;
    only the transfer's two ends differ between them, and the caller sets
    those.
    """
    row["type"] = "external" if _is_root_trace(trace) else "internal"
    row["contract_creation"] = trace.get("trace_type") == "create" if trace else None
    # No trace at all means the listing references none -- the plain external
    # transfer, whose call data IS the transaction's.
    row["input"] = trace.get("input") if trace else detail.get("input")
    row["input_parsed"] = parse_function_call(row["input"], function_call_signatures)
    _charge_the_fee(row, detail)


def _token_transfer_row(row: dict, detail: dict, log_index: Optional[int]) -> None:
    """The same, for a transfer that came out of a log.

    ``token_tx_id`` is the log index, which is what v2 puts there
    (`cassandra.py:5001`) and what `get_tx_identifier` renders into the
    identifier. A token transfer is not a deployment and carries no call data
    of its own.
    """
    row["type"] = "erc20"
    row["token_tx_id"] = log_index
    row["contract_creation"] = False
    row["input"] = None
    row["input_parsed"] = None
    _charge_the_fee(row, detail)


def _charge_the_fee(row: dict, detail: dict) -> None:
    """THE FEE BELONGS TO THE TRANSACTION, NOT TO THE TRANSFER.

    v2 reports it only where the type is external (`cassandra.py:5090`).
    Putting it on every internal and token row would bill the same gas once
    per transfer, which on a busy transaction multiplies it by a dozen.
    """
    if row["type"] == "external":
        row["fee"] = _fee_of(detail)


def _is_root_trace(trace: dict) -> bool:
    """Whether a trace is the transaction's own call rather than an internal one.

    v2's test, and the one that lives ON the row: ``trace_address`` is the
    path down the call tree, so the root's is empty (`cassandra.py:5053`).
    An absent trace means the listing row references none at all -- the plain
    external transfer -- which is a root by the same reading.
    """
    return not (trace.get("trace_address") or "").strip()


def _topic_address(log: dict, index: int) -> Optional[bytes]:
    """The address in an indexed topic: 32 bytes, left-padded, address last."""
    topics = log.get("topics") or []
    if len(topics) <= index or topics[index] is None:
        return None
    return bytes(topics[index])[-20:]


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

    def _bytes(self, currency: str, address) -> bytes:
        """The address as v3 keys it, from whatever the service hands over.

        NOT always a string. The service canonicalises before every DAL call --
        `addresses_service.list_address_txs`, `common.get_address` and the rest
        all run `cannonicalize_address` first -- and that function's canonical
        form is PER FAMILY:

            elif currency == "eth":
                return hex_str_to_bytes(strip_0x(address))

        because v2's account DAL keys on bytes. UTXO stays a string. So this
        receives bytes for an account network and a string for a UTXO one, and
        assuming a string sent every account call into `strip_0x`, where
        `bytes.startswith("0x")` raises TypeError -- reported from inside
        gslib, naming neither the family nor the adapter.

        Already-bytes passes straight through: it is the same value
        `encode_address` would produce, and re-encoding it is what fails.
        """
        if isinstance(address, (bytes, bytearray, memoryview)):
            return bytes(address)
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
        if isinstance(fiat_values, dict):
            # `exchange_rates` still stores a map -- it is 3 MB and is read
            # directly, so self-describing values are worth it there.
            values = {str(k).lower(): v for k, v in fiat_values.items()}
            return [
                {"code": code, "value": float(values.get(code) or 0.0)}
                for code in (order or sorted(values))
            ]
        # The `currency` UDT is POSITIONAL, ordered by the keyspace's own
        # `configuration.fiat_currencies`. Zipping against a different order
        # would relabel every amount rather than fail, so the order comes from
        # the keyspace that wrote the values, never from a default.
        amounts = list(fiat_values or [])
        return [
            {"code": code, "value": float(amounts[index] or 0.0)}
            for index, code in enumerate(order)
            if index < len(amounts)
        ]

    def _value_of(self, currency: str, total: Optional[dict]) -> _Value:
        """A merged ``currency`` total as the service reads it.

        `services.common.to_values` takes ``.value`` and ``.fiat_values`` as
        ATTRIBUTES -- v2 hands back a driver UDT object -- so a plain dict
        raises AttributeError inside the service rather than at the boundary.
        And its fiat list must be LABELLED: `to_values` does ``r["code"]`` on
        every element, so handing over the UDT's positional list of doubles
        fails with "'float' object is not subscriptable" from inside the
        service, on every address and every neighbour.

        Absent means ZERO, not missing: an address that never received has
        received nothing, and `address_from_row` subscripts the key either way.
        """
        if not total:
            return _Value(0, [])
        return _Value(
            int(total.get("value") or 0),
            self._fiat_list(currency, total.get("fiat_values")),
        )

    # -- blocks ------------------------------------------------------------

    async def get_block(self, currency: str, height: int) -> Optional[dict]:
        """v3's block row, under the names `blocks_service` subscripts.

        v3 renamed the account column: `definitions.py` carries
        ``no_transactions`` with the note "was smallint, and was
        transaction_count", so every raw table spells a count the same way.
        `_block_from_row` reads ``row["transaction_count"]`` by SUBSCRIPT for
        an eth-like currency, so the rename surfaces as a KeyError inside the
        service rather than as a missing field at the boundary -- the same
        shape of trap `get_currency_statistics` already maps around for
        ``no_blocks``.

        Added rather than renamed: the UTXO branch of `_block_from_row` reads
        ``no_transactions``, so both names have to answer.
        """
        row = await self._dal(currency).block(height)
        if row is None:
            return None
        if "transaction_count" not in row and "no_transactions" in row:
            row = {**row, "transaction_count": row["no_transactions"]}
        return row

    async def get_block_timestamp(self, currency: str, height: int) -> Optional[dict]:
        """A ROW, not the timestamp.

        The protocol says ``Optional[Dict[str, Any]]`` and `blocks_service`
        reads ``bts.get("timestamp")`` off it. Returning the bare int raises
        ``AttributeError: 'int' object has no attribute 'get'`` inside the
        block-by-date binary search, nowhere near this method.
        """
        block = await self._dal(currency).block(height)
        if block is None:
            return None
        return {"block_id": height, "timestamp": block.get("timestamp")}

    async def list_block_txs(self, currency: str, height: int) -> list:
        """A block's transactions, in the shape `std_tx_from_row` reads.

        Routed by family, because the two answer different questions: a UTXO
        transaction needs its legs attached, an account transaction needs the
        ``type`` that tells the service which of the two row shapes it is
        holding. Without the branch the account side died on
        ``KeyError: 'type'`` inside the service.
        """
        from graphsenselib.utils.rest_utils import is_eth_like

        dal = self._dal(currency)
        found = await dal.block_transactions(height)
        if is_eth_like(currency.lower()):
            tokens = await self._block_token_transfers(currency, height, found)
            rows = []
            for tx in found:
                rows.append(self._as_v2_block_tx(currency, tx))
                # Interleaved right behind their transaction, as v2 builds
                # them (`cassandra.py:5271-5275`): the listing is ordered by
                # transaction, and a token transfer belongs to one.
                rows.extend(tokens.get(tx["tx_id"], []))
            return rows
        legs = await dal.transaction_io_many([tx["tx_id"] for tx in found])
        return [self._with_io(currency, tx, legs.get(tx["tx_id"], [])) for tx in found]

    async def _block_token_transfers(
        self, currency: str, height: int, found: list
    ) -> dict:
        """``{tx_id: [erc20 row]}`` for one block, decoded from its logs.

        v2 lists a block's transactions with ``include_token_txs=True``, so a
        token transfer is a row of its own next to the transaction that made
        it. The transfers are not stored anywhere as rows -- they are Transfer
        logs -- so this reads the block's logs ONCE and decodes them.

        The decode is `datatypes.abi.decode_logs_db`, the same function v2
        calls: an ABI decoder is not a v2 implementation detail, and writing a
        second one here is how the two would come to disagree about a token.
        Only CONFIGURED tokens count, which is also v2's rule -- an unknown
        contract has no ticker and no decimals to report it with.
        """
        config = self._token_config.get(currency.lower()) or {}
        by_address = {
            bytes(token["token_address"]): ticker
            for ticker, token in config.items()
            if token.get("token_address")
        }
        if not by_address:
            return {}

        logs = await self._dal(currency).logs_in_block(height, topic0=TRANSFER_TOPIC)
        known = [log for log in logs if bytes(log.get("address") or b"") in by_address]
        by_tx = {tx["tx_id"]: tx for tx in found}

        transfers: dict = {}
        for decoded, log in decode_logs_db(known):
            tx = by_tx.get(log.get("tx_id"))
            if tx is None:
                # A log whose transaction is not in the block listing means a
                # torn block, not a row to invent a transaction for.
                continue
            parameters = decoded.get("parameters") or {}
            transfers.setdefault(log["tx_id"], []).append(
                {
                    "tx_hash": tx.get("tx_hash"),
                    "height": tx.get("block_id"),
                    "timestamp": tx.get("block_timestamp"),
                    "currency": by_address[bytes(log["address"])],
                    "value": parameters.get("value"),
                    "from_address": _address_bytes(parameters.get("from")),
                    "to_address": _address_bytes(parameters.get("to")),
                    "type": "erc20",
                    "token_tx_id": log.get("log_index"),
                    "contract_creation": False,
                    "input": None,
                    "input_parsed": None,
                }
            )
        return transfers

    def _as_v2_block_tx(self, currency: str, detail: dict) -> dict:
        """The EXTERNAL row of one account transaction in a block listing.

        Its token transfers are separate rows, built by
        :meth:`_block_token_transfers` and interleaved by the caller.
        """
        to_address = detail.get("to_address")
        created = to_address is None
        row = {
            "tx_hash": detail.get("tx_hash"),
            "height": detail.get("block_id"),
            "timestamp": detail.get("block_timestamp"),
            "value": detail.get("value"),
            "currency": currency.upper(),
            "type": "external",
            "from_address": detail.get("from_address"),
            # A deployment names no recipient; v2 reports the contract it
            # created instead (`cassandra.py:5249-5252`).
            "to_address": (
                detail.get("receipt_contract_address") if created else to_address
            ),
            "contract_creation": True if created else None,
            "input": detail.get("input"),
            "input_parsed": parse_function_call(
                detail.get("input"), function_call_signatures
            ),
            "fee": _fee_of(detail),
        }
        return row

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

        Returns the ROW, not the id: `blocks_service.get_block_by_date` reads
        both ``["block_id"]`` and ``["timestamp"]`` off it. This returned a bare
        int until 2026-09-07, which would have raised "'int' object is not
        subscriptable" from inside the service -- dormant only because
        `block_by_date_use_linear_search` defaults to False, so nothing had ever
        called it.

        STRICTLY after, despite the name. v2's own version of this method is
        ``timestamp >= %s`` (`cassandra.py:1334`) and v3 matched it -- but
        nothing runs that path: with the flag off, v2 answers /blocks/by_date
        by BINARY SEARCH instead (`blocks_service.py:190-220`), and the two
        disagree about an exact timestamp match. `find_insertion_point_async`
        returns the matching height itself, which the service reports as
        ``before_block``, with ``after_block`` the one above. The inclusive
        read makes the match ``after_block`` and shifts BOTH answers down a
        block.

        The REST answer is the contract, not the query, so this reproduces
        what v2 actually serves. Measured on eth block 15060334, whose
        timestamp is exactly 2022-07-02 02:26:36: v2 says
        before=15060334/after=15060335, and the inclusive bound said
        15060333/15060334.

        One case still differs and cannot be reconciled from here: several
        blocks sharing a timestamp. This lands ``before_block`` on the LAST of
        that run; a binary search returns whichever it bisected onto. eth's
        ~12s spacing makes it rare rather than impossible.
        """
        return await self._dal(currency).block_at_or_after(timestamp, inclusive=False)

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
            return _Rows(rows)
        return _Rows(r for r in rows if r.get("spending_input_index") == io_index)

    async def get_spent_in_txs(self, currency: str, tx_hash, io_index=None) -> list:
        dal = self._dal(currency)
        raw = bytes.fromhex(tx_hash) if isinstance(tx_hash, str) else bytes(tx_hash)
        rows = await dal.spent_in(raw, raw.hex()[: dal.config["tx_prefix_length"]])
        if io_index is None:
            return _Rows(rows)
        return _Rows(r for r in rows if r.get("spent_output_index") == io_index)

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
        symbol = currency.upper()
        row = {
            "address": address,
            "address_id": synthetic_id(raw),
            "address_id_group": dal.entity_bucket(raw),
            "first_tx_id": stats.first_tx_id,
            "last_tx_id": stats.last_tx_id,
            # The NATIVE balance, by ticker rather than by whichever asset the
            # driver happened to return first: an account address holds several,
            # and `address_from_row` converts this one with the native rate.
            "balance": balances.get(symbol, balances.get(native, 0) if native else 0),
            "balances": balances,
        }
        row.update(stats.summed)
        row.update(stats.epoch_zero)
        # `address_from_row` SUBSCRIPTS both of these, so an address with no
        # rows for one of them still needs the key -- zero, which is what an
        # address that received nothing received.
        for name in ("total_received", "total_spent"):
            row[name] = self._value_of(currency, stats.totals.get(name))
        for name in ("total_tokens_received", "total_tokens_spent"):
            tokens = stats.token_totals.get(name)
            row[name] = (
                {
                    ticker: self._value_of(currency, amount)
                    for ticker, amount in tokens.items()
                }
                if tokens
                else None
            )
        # Every asset but the native one, which `balance` already carries.
        # `.get`, not a subscript, so None and {} are both fine -- but a UTXO
        # keyspace holds one asset and must not report an empty token map as
        # though it were a finding.
        others = {
            ticker: amount for ticker, amount in balances.items() if ticker != symbol
        }
        row["token_balances"] = others or None
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
        before_row = None
        if page:
            # A resume token from a previous call. The format is OURS -- the
            # service treats it as opaque -- and it carries TWO things: the last
            # tx_id handed out, and how many rows OF THAT TRANSACTION have been
            # delivered so far.
            #
            # The count is what an account listing needs. One transaction there
            # produces several rows -- a trace and a log, or two assets -- so a
            # page can end mid-transaction, and resuming at `tx_id <` would drop
            # the rest of it silently. See `Dal.paging_bounds`.
            before_row = decode_page_token(page)
        elif max_height is not None:
            from graphsense_v3.codec import tx_id_range

            before = tx_id_range(max_height, max_height)[1] + 1
        after = None
        if min_height is not None:
            from graphsense_v3.codec import tx_id_range

            # A BOUND, not a page hint. Choosing a start page and no bound was
            # the bug: rows below the height came back anyway, and 26 of 52
            # sampled addresses returned transactions where v2 correctly
            # returned none. The page index is an optimisation for addresses
            # that span pages; the filter is the correctness mechanism.
            after = tx_id_range(min_height, min_height)[0]
        limit = int(pagesize or 100)
        found = await dal.transactions(
            raw,
            is_outgoing=is_outgoing,
            before_tx_id=before,
            before_row=before_row,
            after_tx_id=after,
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
        token = (
            encode_page_token(found, before_row)
            if found and len(found) == limit
            else None
        )
        return await self._as_v2_txs(currency, found), token

    async def _as_v2_txs(self, currency: str, found: list) -> list:
        from graphsenselib.utils.rest_utils import is_eth_like

        if is_eth_like(currency.lower()):
            return await self._as_v2_account_txs(currency, found)
        return await self._as_v2_utxo_txs(currency, found)

    async def _as_v2_account_txs(self, currency: str, found: list) -> list:
        """v3's account listing rows as the dicts `txs_from_rows` reads.

        The listing carries `tx_reference` -- a trace_index or a log_index --
        and NOT the transfer's two ends, so this fetches the trace or the log
        that names them, exactly as v2 does (`cassandra.py:5045-5060`). The
        outer transaction's from/to would be the wrong counterparty on every
        internal call and every token transfer, which is the failure mode this
        exists to avoid. `/links` needs none of it: the caller named both ends.

        Three concurrent rounds, not three per row: the transactions, then the
        traces and logs the references point at.
        """
        dal = self._dal(currency)
        native = currency.upper()
        by_id = await dal.transactions_by_ids([tx.tx_id for tx in found])

        traces, logs = [], []
        for tx in found:
            reference = getattr(tx, "tx_reference", None)
            block = block_of_tx_id(tx.tx_id)
            log_index = getattr(reference, "log_index", None)
            trace_index = getattr(reference, "trace_index", None)
            if log_index is not None:
                logs.append((block, log_index))
            elif trace_index is not None:
                traces.append((block, trace_index))
        by_trace = await dal.traces_by_ref(traces)
        by_log = await dal.logs_by_ref(logs)

        rows = []
        for tx in found:
            detail = by_id.get(tx.tx_id)
            if detail is None:
                raise NotAvailable(
                    f"address_transactions references tx_id {tx.tx_id}, which "
                    f"{dal.raw}.transaction does not have"
                )
            reference = getattr(tx, "tx_reference", None)
            trace_index = getattr(reference, "trace_index", None)
            log_index = getattr(reference, "log_index", None)
            asset = (getattr(tx, "currency", None) or native).upper()
            block = block_of_tx_id(tx.tx_id)

            row = {
                "tx_hash": detail.get("tx_hash"),
                "height": detail.get("block_id"),
                "timestamp": detail.get("block_timestamp"),
                # v2 signs by direction on this listing -- money leaving is
                # negative (`cassandra.py:4949`). v3 stores the magnitude and
                # the direction separately, as the UTXO branch already knew.
                "value": -tx.value if tx.is_outgoing else tx.value,
                "currency": asset,
            }
            if asset != native:
                event = by_log.get((block, log_index)) or {}
                _token_transfer_row(row, detail, log_index)
                row["from_address"] = _topic_address(event, 1)
                row["to_address"] = _topic_address(event, 2)
            else:
                event = by_trace.get((block, trace_index)) or {}
                _native_transfer_row(row, detail, event)
                if trace_index is not None:
                    row["trace_index"] = trace_index
                # The trace's ends when there is one; the transaction's for a
                # row with no trace reference at all.
                row["from_address"] = event.get("from_address") or detail.get(
                    "from_address"
                )
                row["to_address"] = event.get("to_address") or detail.get("to_address")
            rows.append(row)
        await self._attach_token_rates(currency, rows)
        return rows

    async def _attach_token_rates(self, currency: str, rows: list) -> None:
        """Price the UNPEGGED token rows from the token's own rate.

        Without it `map_rates_for_peged_tokens` has nothing to convert with and
        returns EMPTY fiat for an unpegged token -- not a wrong number, but no
        number, on every row of that token. A pegged token needs none of this:
        its branch returns before `token_rate` is even looked at, and v2 still
        sets the key on every erc20 row, so this does too.

        One lookup per DISTINCT (asset, block), not per row: a page of one
        token in one block is one read.
        """
        import asyncio

        config = self._token_config.get(currency.lower()) or {}

        def unpegged(ticker: Optional[str]) -> bool:
            token = config.get(ticker) or config.get((ticker or "").upper())
            if token is None:
                return False
            peg = token.get("peg_currency")
            return peg is None or (isinstance(peg, str) and not peg.strip())

        # The listing spells the height "height" and a link row "block_id" --
        # `_tx_account_from_row` accepts either (`common.py:528`), so this has
        # to as well or `/links` raises a KeyError where the listing does not.
        def height_of(row: dict) -> Optional[int]:
            return row.get("height", row.get("block_id"))

        # A row with no height cannot be priced at a block; it should not
        # exist, and dropping it here beats looking a rate up at None.
        wanted = {
            (str(row["currency"]), height)
            for row in rows
            if row.get("type") == "erc20" and unpegged(row.get("currency"))
            for height in [height_of(row)]
            if height is not None
        }
        if not wanted:
            return
        pairs = sorted(wanted)
        rates = await asyncio.gather(
            *(self.get_token_rate(currency, asset, block) for asset, block in pairs)
        )
        found = dict(zip(pairs, rates))
        for row in rows:
            if row.get("type") == "erc20":
                row["token_rate"] = found.get((row["currency"], height_of(row)))

    async def _as_v2_utxo_txs(self, currency: str, found: list) -> list:
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
    ) -> tuple:
        from graphsenselib.utils.rest_utils import is_eth_like

        if is_eth_like(currency.lower()):
            return await self._account_links(
                currency, address, neighbor, pagesize=pagesize
            )
        dal = self._dal(currency)
        limit = int(pagesize or 100)
        found = await dal.link_transactions(
            self._bytes(currency, address),
            self._bytes(currency, neighbor),
            limit=limit,
        )
        # `links_response` unpacks two values and reads block_id, timestamp and
        # tx_hash off each row; the link table holds none of those, so they come
        # from the transaction in one concurrent round.
        detail = await dal.transactions_by_ids([row["tx_id"] for row in found])
        rows = []
        for row in found:
            tx = detail.get(row["tx_id"])
            if tx is None:
                raise NotAvailable(
                    f"address_link_transactions references tx_id {row['tx_id']}, "
                    f"which {dal.raw}.transaction does not have"
                )
            rows.append(
                {
                    "tx_hash": tx.get("tx_hash"),
                    "block_id": tx.get("block_id"),
                    "timestamp": tx.get("block_timestamp"),
                    # SIGNED, as v2 signs it. v2 does not read a link table:
                    # it takes each side's `address_transactions.value`, which
                    # is already signed by direction, so the source's input --
                    # an outgoing leg -- arrives negative
                    # (`cassandra.py:2631-2632`). The sign has to go on before
                    # `convert_value` rather than after, or the fiat rounds off
                    # the positive magnitude and lands a cent away from v2's.
                    "input_value": -row["input_value"],
                    # NOT negated, and it does not always agree with v2. v3
                    # stores what each side GROSSLY put in and took out; v2
                    # reports the destination's NET flow for that transaction,
                    # so the two differ exactly when the destination is also an
                    # input to the same transaction (UTXO change). See the
                    # `_link_txs_table` comment: reporting the gross amounts is
                    # the deliberate choice, not an oversight.
                    "output_value": row["output_value"],
                }
            )
        token = str(found[-1]["tx_id"]) if found and len(found) == limit else None
        return rows, token

    async def _account_links(
        self, currency: str, address: str, neighbor: str, *, pagesize=None
    ) -> tuple:
        """One account edge, as the rows `txs_from_rows` reads.

        `links_response` splits on the family: a UTXO link is one row with two
        amounts, an account link is ONE ROW PER TRANSFER routed through
        `txs_from_rows` -> `_tx_account_from_row`. So this returns transaction
        rows, not link rows, and the two branches share nothing but a name.

        The edge saves ONE of the two extra reads, not both: v2 fetches the
        trace or the log to learn a transfer's two ends AND to read the fields
        that only live there (`cassandra.py:5045-5068`), and the caller has
        named only the ends. So the traces are still fetched -- for
        ``trace_type``, ``input`` and ``trace_address`` -- in one concurrent
        round, the same as the address listing does.

        `/links` is v2's address listing under another name: it routes through
        `list_address_txs_ordered` -> `normalize_address_transactions`
        (`cassandra.py:2591-2616`), which is why the row shape is shared here.
        NOT the sign, though: that wrapper does not negate an outgoing value
        the way `list_address_txs_ordered`'s caller does (`4949`), so a link
        row reports the magnitude.
        """
        dal = self._dal(currency)
        limit = int(pagesize or 100)
        src = self._bytes(currency, address)
        dst = self._bytes(currency, neighbor)
        found = await dal.link_transactions(src, dst, limit=limit)
        detail = await dal.transactions_by_ids([row["tx_id"] for row in found])
        native = currency.upper()

        traces = [
            (
                block_of_tx_id(row["tx_id"]),
                getattr(row.get("tx_reference"), "trace_index", None),
            )
            for row in found
            if (row.get("currency") or native).upper() == native
        ]
        by_trace = await dal.traces_by_ref(traces)

        rows = []
        for row in found:
            tx = detail.get(row["tx_id"])
            if tx is None:
                raise NotAvailable(
                    f"address_link_transactions references tx_id {row['tx_id']}, "
                    f"which {dal.raw}.transaction does not have"
                )
            reference = row.get("tx_reference")
            trace_index = getattr(reference, "trace_index", None)
            log_index = getattr(reference, "log_index", None)
            asset = (row.get("currency") or native).upper()
            built = {
                "tx_hash": tx.get("tx_hash"),
                "block_id": tx.get("block_id"),
                "timestamp": tx.get("block_timestamp"),
                # The edge's own ends, not the transaction's. For a token
                # transfer or an internal call those differ from the outer
                # transaction's from/to, and taking the transaction's would
                # report the wrong counterparty on exactly the rows that are
                # not plain external transfers.
                "from_address": src,
                "to_address": dst,
                "value": row.get("value"),
                "currency": asset,
            }
            if asset != native:
                _token_transfer_row(built, tx, log_index)
            else:
                trace = by_trace.get((block_of_tx_id(row["tx_id"]), trace_index)) or {}
                _native_transfer_row(built, tx, trace)
                if trace_index is not None:
                    built["trace_index"] = trace_index
            rows.append(built)
        await self._attach_token_rates(currency, rows)
        # One partition per edge and the walk starts at the newest page, so a
        # full page may have more behind it; a short one cannot.
        token = str(found[-1]["tx_id"]) if found and len(found) == limit else None
        return rows, token

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
        from graphsenselib.utils.rest_utils import is_eth_like

        from graphsense_v3.codec import decode_address

        # v2 stores a UTXO address as TEXT and an account address as a BLOB,
        # and `address_to_user_format` keys off the type it is handed: bytes
        # become "0x..." for eth, while a str is only lowercased. Decoding an
        # eth address here would hand the service "742d..." and it would serve
        # that -- a valid-looking address missing its prefix, on every
        # neighbour of every account chain.
        account = is_eth_like(currency.lower())
        side = "dst_address" if is_outgoing else "src_address"
        rows = []
        for edge in found:
            rows.append(
                {
                    side: (
                        bytes(edge.address)
                        if account
                        # The DECODED string: `address_to_user_format` passes a
                        # UTXO address through unchanged, so raw bytes would
                        # reach the response.
                        else decode_address(currency.lower(), bytes(edge.address))
                    ),
                    # The service reads this by SUBSCRIPT before anything else,
                    # then feeds it to get_fresh_cluster_id. Absent, the whole
                    # call dies with a KeyError that names no cause; present,
                    # the call fails honestly on "v3 has no cluster tables".
                    f"{side}_id": synthetic_id(bytes(edge.address)),
                    "no_transactions": edge.no_transactions,
                    # `edge.value` is the summed AMOUNT and `edge.fiat_values`
                    # its summed fiat, both already folded over the epochs by
                    # the DAL. They were previously read as attributes OF the
                    # amount -- `getattr(edge.value, "fiat_values", None)` on
                    # an int, which resolves to None every time, so no
                    # neighbour edge ever carried a fiat value.
                    "value": _Value(
                        value=int(edge.value or 0),
                        fiat_values=self._fiat_list(currency, edge.fiat_values),
                    ),
                    "token_values": (
                        {
                            ticker: _Value(
                                value=int(amount.get("value") or 0),
                                fiat_values=self._fiat_list(
                                    currency, amount.get("fiat_values")
                                ),
                            )
                            for ticker, amount in edge.token_values.items()
                        }
                        if edge.token_values
                        else None
                    ),
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

    async def get_token_rate(self, currency: str, token: str, block_id: int):
        """A token's own fiat-per-token rate at or before ``block_id``.

        v2 keeps these in a second table, `token_exchange_rates`; v3 merged
        them into `exchange_rates` under the asset's ticker, so this is the
        same read as the native coin's with a different key.

        AT OR BEFORE, not at: a token has a row only where a price was
        fetched. See `Dal.rate_at_or_before`.
        """
        fiat = await self._dal(currency).rate_at_or_before(token.upper(), block_id)
        return None if fiat is None else self._fiat_list(currency, fiat)

    async def list_matching_txs(self, *_, **__):
        raise NotAvailable(
            "transaction prefix search needs a scan of transaction_by_tx_prefix; "
            "not wired up yet"
        )
