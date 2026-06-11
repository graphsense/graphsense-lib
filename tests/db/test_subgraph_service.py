"""Tests for subgraph_service: the pure ``build_summary`` aggregator and the
``summary`` orchestration over a set of transactions."""

from __future__ import annotations

from collections import namedtuple

import pytest

from graphsenselib.db.asynchronous.services.subgraph_service import (
    build_address_summary,
    build_summary,
    summary,
)
from graphsenselib.db.asynchronous.services.models import (
    LabeledItemRef,
    Txs,
)
from graphsenselib.errors import (
    AddressNotFoundException,
    BadUserInputException,
    TransactionNotFoundException,
)
from types import SimpleNamespace
from tests.db.helpers import (
    CURRENCY,
    make_account_tx,
    make_address,
    make_tx,
    make_txvalue,
    make_value,
)


# ---------------------------------------------------------------------------
# build_summary (pure)
# ---------------------------------------------------------------------------


class TestBuildSummary:
    def test_utxo_aggregates_value_fee_inputs_outputs_block_timestamp(self):
        # Summary is derived straight from the txs (no characteristics needed),
        # so it can be built from header data alone.
        txs = [
            make_tx(
                inputs=[make_txvalue("a", 1)] * 2,
                outputs=[make_txvalue("b", 1)] * 3,
                total_input=1_000,
                total_output=900,
                height=100,
                timestamp=1000,
            ),
            make_tx(
                inputs=[make_txvalue("a", 1)] * 1,
                outputs=[make_txvalue("b", 1)] * 1,
                total_input=500,
                total_output=480,
                height=200,
                timestamp=2000,
            ),
        ]
        s = build_summary(CURRENCY, txs)
        assert s.tx_count == 2
        assert s.total_value == 1_380  # sum of total_output
        assert s.total_fee == 120  # (1000-900) + (500-480)
        assert s.total_inputs == 3
        assert s.total_outputs == 4
        assert s.block_min == 100
        assert s.block_max == 200
        assert s.timestamp_min == 1000
        assert s.timestamp_max == 2000

    def test_utxo_coinbase_contributes_no_fee(self):
        # A coinbase tx has no fee; with only coinbase txs total_fee is None.
        txs = [
            make_tx(
                inputs=[make_txvalue("coinbase", 0)],
                outputs=[make_txvalue("b", 1)],
                coinbase=True,
                total_input=0,
                total_output=50_000,
            ),
        ]
        s = build_summary(CURRENCY, txs)
        assert s.total_value == 50_000
        assert s.total_fee is None

    def test_account_aggregates_value_fee_without_io_counts(self):
        # Account txs are flat from->to value transfers; there is no
        # input/output decomposition, so io counts are None.
        txs = [
            make_account_tx(value=1_000, fee=21, height=10, timestamp=100),
            make_account_tx(value=2_000, fee=42, height=20, timestamp=200),
        ]
        s = build_summary("eth", txs)
        assert s.tx_count == 2
        assert s.total_value == 3_000
        assert s.total_fee == 63
        assert s.total_inputs is None
        assert s.total_outputs is None
        assert s.block_min == 10
        assert s.block_max == 20
        assert s.timestamp_min == 100
        assert s.timestamp_max == 200

    def test_fiat_sum_rounded_to_cents(self):
        # 0.1 + 0.2 is 0.30000000000000004 in float; the response value
        # must be rounded to cents.
        txs = [
            make_account_tx(value=1_000, value_usd=0.1),
            make_account_tx(value=2_000, value_usd=0.2),
        ]
        s = build_summary("eth", txs)
        assert s.total_value_fiat == 0.3

    def test_account_fee_none_when_unavailable(self):
        txs = [
            make_account_tx(value=1_000, fee=None),
            make_account_tx(value=2_000, fee=None),
        ]
        s = build_summary("eth", txs)
        assert s.total_value == 3_000
        assert s.total_fee is None

    def test_total_value_fiat_summed_across_native_and_token(self):
        # Native ETH transfer + a USDT token transfer. total_value is the
        # native-unit (wei) sum and excludes the token; total_value_fiat sums
        # USD across both; a note records the excluded token transfer.
        txs = [
            make_account_tx(value=1_000, value_usd=2.5, height=10, timestamp=100),
            make_account_tx(
                value=5_000_000,
                value_usd=5.0,
                token_tx_id=0,
                asset="usdt",
                height=11,
                timestamp=110,
            ),
        ]
        s = build_summary("eth", txs)
        assert s.total_value == 1_000  # native transfer only
        assert s.total_value_fiat == 7.5  # both, in USD
        assert s.fiat_currency == "usd"  # default
        assert s.tx_count == 2
        assert any("token transfer" in n for n in s.notes)

    def test_total_value_fiat_partial_is_summed_and_flagged(self):
        txs = [
            make_account_tx(value=1_000, value_usd=2.5),
            make_account_tx(value=2_000, value_usd=None),  # no rate at height
        ]
        s = build_summary("eth", txs)
        assert s.total_value_fiat == 2.5  # only the tx with a rate
        assert any("partial" in n for n in s.notes)

    def test_total_value_fiat_none_when_no_rates(self):
        txs = [
            make_account_tx(value=1_000, value_usd=None),
            make_account_tx(value=2_000, value_usd=None),
        ]
        s = build_summary("eth", txs)
        assert s.total_value_fiat is None
        assert any("USD" in n for n in s.notes)

    def test_utxo_total_value_fiat_from_outputs(self):
        txs = [
            make_tx(total_input=1_000, total_output=900, total_output_usd=10.0),
            make_tx(total_input=500, total_output=480, total_output_usd=5.0),
        ]
        s = build_summary(CURRENCY, txs)
        assert s.total_value == 1_380
        assert s.total_value_fiat == 15.0
        assert s.fiat_currency == "usd"
        assert s.notes == []  # all rates present, no tokens to exclude

    def test_fiat_currency_eur_selected(self):
        # With fiat_currency="eur" the EUR rates are summed (not USD) and the
        # echoed fiat_currency reflects the choice.
        txs = [
            make_account_tx(value=1_000, value_usd=2.5, value_eur=2.0),
            make_account_tx(value=2_000, value_usd=5.0, value_eur=4.0),
        ]
        s = build_summary("eth", txs, "eur")
        assert s.total_value_fiat == 6.0  # EUR sum, not the 7.5 USD sum
        assert s.fiat_currency == "eur"

    def test_fiat_currency_missing_rate_for_requested_currency(self):
        # Txs carry only USD; requesting EUR yields no fiat total and an
        # EUR-worded note.
        txs = [
            make_account_tx(value=1_000, value_usd=2.5),
            make_account_tx(value=2_000, value_usd=5.0),
        ]
        s = build_summary("eth", txs, "eur")
        assert s.total_value_fiat is None
        assert s.fiat_currency == "eur"
        assert any("EUR" in n for n in s.notes)


# ---------------------------------------------------------------------------
# summary() orchestration
# ---------------------------------------------------------------------------


class FakeTxsService:
    """Minimal fake implementing the surface ``summary`` calls."""

    def __init__(self, tx_map=None, flows_map=None):
        self._tx_map = tx_map or {}
        self._flows = flows_map or {}
        self.get_tx_calls: list[dict] = []
        self.flows_calls: list[str] = []

    async def get_tx(self, currency, tx_hash, *args, **kwargs):
        self.get_tx_calls.append(kwargs)
        if tx_hash not in self._tx_map:
            raise TransactionNotFoundException(currency, tx_hash)
        return self._tx_map[tx_hash]

    async def get_asset_flows_within_tx(self, network, tx_hash, **kwargs):
        self.flows_calls.append(tx_hash)
        # Full asset-flow set: base tx + token-transfer legs; fall back to the
        # base tx when no legs are configured.
        legs = self._flows.get(tx_hash) or [self._tx_map[tx_hash]]
        return Txs(txs=legs)


class TestSubgraphSummary:
    def _utxo_svc(self):
        h0, h1 = "aa" * 32, "bb" * 32
        tx_map = {
            h0: make_tx(
                tx_hash=h0,
                inputs=[make_txvalue("a", 1)] * 2,
                outputs=[make_txvalue("b", 1)] * 1,
                total_input=1_000,
                total_output=900,
                height=100,
                timestamp=1000,
            ),
            h1: make_tx(
                tx_hash=h1,
                inputs=[make_txvalue("a", 1)] * 1,
                outputs=[make_txvalue("b", 1)] * 1,
                total_input=500,
                total_output=480,
                height=200,
                timestamp=2000,
            ),
        }
        return FakeTxsService(tx_map=tx_map), [h0, h1]

    async def test_utxo_header_only_fetch(self):
        svc, hashes = self._utxo_svc()
        s = await summary(svc, None, None, CURRENCY, hashes, [], tagstore_groups=[])
        assert s.currency == CURRENCY
        assert s.addresses is None  # no addresses requested, block omitted
        assert s.txs.tx_count == 2
        assert s.txs.total_value == 1_380
        assert s.txs.total_inputs == 3
        assert s.txs.total_outputs == 2
        # UTXO summary fetches headers only: no IO, no heuristics.
        assert svc.get_tx_calls[0]["include_io"] is False
        assert svc.get_tx_calls[0]["include_heuristics"] == []
        assert svc.flows_calls == []

    @pytest.mark.parametrize("currency", ["bch", "ltc", "zec"])
    async def test_other_utxo_chains_allowed(self, currency):
        svc, hashes = self._utxo_svc()
        s = await summary(svc, None, None, currency, hashes, [], tagstore_groups=[])
        assert s.currency == currency
        assert s.txs.tx_count == 2

    @pytest.mark.parametrize("currency", ["eth", "trx"])
    async def test_account_folds_token_fiat(self, currency):
        # Account summary folds token-transfer fiat into total_value_fiat, not
        # just the base native tx. h1 is a USDT transfer: its base tx moves 0
        # native, the value lives in the token leg.
        h0, h1 = "aa" * 32, "bb" * 32
        base0 = make_account_tx(tx_hash=h0, value=10**18, value_usd=3000.0)
        base1 = make_account_tx(tx_hash=h1, value=0, value_usd=0.0)
        token1 = make_account_tx(
            tx_hash=h1, value=5_000_000, value_usd=1000.0, token_tx_id=0, asset="usdt"
        )
        svc = FakeTxsService(
            tx_map={h0: base0, h1: base1},
            flows_map={h0: [base0], h1: [base1, token1]},
        )
        s = await summary(svc, None, None, currency, [h0, h1], [], tagstore_groups=[])
        assert s.txs.total_value == 10**18  # native only; token leg excluded
        assert s.txs.total_value_fiat == 4000.0  # 3000 native + 1000 token
        assert s.txs.fiat_currency == "usd"  # default
        assert any("token" in n.lower() for n in s.txs.notes)
        # Account path uses asset flows, not get_tx headers.
        assert svc.flows_calls == [h0, h1]
        assert svc.get_tx_calls == []

    async def test_fiat_currency_threaded_through(self):
        # The fiat_currency argument reaches build_summary: EUR rates are
        # summed and echoed instead of USD.
        h0, h1 = "aa" * 32, "bb" * 32
        tx_map = {
            h0: make_account_tx(tx_hash=h0, value=1_000, value_usd=3.0, value_eur=2.0),
            h1: make_account_tx(tx_hash=h1, value=2_000, value_usd=6.0, value_eur=4.0),
        }
        svc = FakeTxsService(tx_map=tx_map)
        s = await summary(
            svc,
            None,
            None,
            "eth",
            [h0, h1],
            [],
            tagstore_groups=[],
            fiat_currency="eur",
        )
        assert s.txs.total_value_fiat == 6.0  # EUR, not the 9.0 USD sum
        assert s.txs.fiat_currency == "eur"

    async def test_needs_at_least_two_nodes(self):
        svc, hashes = self._utxo_svc()
        with pytest.raises(
            BadUserInputException, match="at least 2 distinct tx hashes"
        ):
            await summary(svc, None, None, CURRENCY, hashes[:1], [], tagstore_groups=[])

    async def test_empty_rejected(self):
        svc, _ = self._utxo_svc()
        with pytest.raises(BadUserInputException):
            await summary(svc, None, None, CURRENCY, [], [], tagstore_groups=[])

    async def test_too_many_nodes_rejected(self):
        # Count check happens before any fetch, so an empty tx_map is fine.
        svc = FakeTxsService(tx_map={})
        hashes = [f"{i:064x}" for i in range(101)]
        with pytest.raises(BadUserInputException, match="at most 100"):
            await summary(svc, None, None, CURRENCY, hashes, [], tagstore_groups=[])

    async def test_duplicate_hashes_deduped(self):
        svc, hashes = self._utxo_svc()
        s = await summary(
            svc,
            None,
            None,
            CURRENCY,
            [hashes[0], hashes[1], hashes[0]],
            [],
            tagstore_groups=[],
        )
        assert s.txs.tx_count == 2
        # Each distinct hash fetched once.
        assert len(svc.get_tx_calls) == 2

    async def test_missing_tx_raises_not_found(self):
        svc, hashes = self._utxo_svc()
        with pytest.raises(TransactionNotFoundException):
            await summary(
                svc,
                None,
                None,
                CURRENCY,
                [hashes[0], "ff" * 32],
                [],
                tagstore_groups=[],
            )


# ---------------------------------------------------------------------------
# build_address_summary (pure)
# ---------------------------------------------------------------------------

# Minimal stand-in for tagstore TagPublic: the aggregator only reads
# .identifier (the tagged address) and .actor (an actor id or None).
FakeTag = namedtuple("FakeTag", "identifier actor")


class TestBuildAddressSummary:
    def _two_addresses(self):
        return [
            make_address(
                "a1",
                total_received=make_value(1000, usd=10.0),
                total_spent=make_value(400, usd=4.0),
                balance=make_value(600, usd=6.0),
                first_ts=1000,
                last_ts=5000,
            ),
            make_address(
                "a2",
                total_received=make_value(2000, usd=20.0),
                total_spent=make_value(0, usd=0.0),
                balance=make_value(2000, usd=20.0),
                first_ts=500,
                last_ts=3000,
            ),
        ]

    def test_aggregates_values_and_usage_span(self):
        s = build_address_summary(CURRENCY, self._two_addresses(), [], [])
        assert s.address_count == 2
        assert s.total_received == 3000
        assert s.total_received_fiat == 30.0
        assert s.total_spent == 400
        assert s.total_spent_fiat == 4.0
        assert s.balance == 2600
        assert s.balance_fiat == 26.0
        assert s.fiat_currency == "usd"
        assert s.first_usage == 500
        assert s.last_usage == 5000
        assert s.tagged_address_count == 0
        assert s.actors == []
        assert s.notes == []

    def test_eur_fiat_currency(self):
        addrs = [
            make_address("a1", total_received=make_value(100, usd=1.0, eur=0.9)),
            make_address("a2", total_received=make_value(200, usd=2.0, eur=1.8)),
        ]
        s = build_address_summary(CURRENCY, addrs, [], [], fiat_currency="eur")
        assert s.total_received_fiat == 2.7
        assert s.fiat_currency == "eur"

    def test_fiat_sums_rounded_to_cents(self):
        # 0.1 + 0.2 is 0.30000000000000004 in float; the response value
        # must be rounded to cents.
        addrs = [
            make_address("a1", total_received=make_value(100, usd=0.1)),
            make_address("a2", total_received=make_value(200, usd=0.2)),
        ]
        s = build_address_summary(CURRENCY, addrs, [], [])
        assert s.total_received_fiat == 0.3

    def test_partial_fiat_noted(self):
        addrs = [
            make_address("a1", total_received=make_value(100, usd=1.0)),
            make_address("a2", total_received=make_value(200)),  # no rate
        ]
        s = build_address_summary(CURRENCY, addrs, [], [])
        assert s.total_received == 300
        assert s.total_received_fiat == 1.0
        assert any("partial" in n for n in s.notes)

    def test_all_fiat_missing(self):
        addrs = [make_address("a1"), make_address("a2")]
        s = build_address_summary(CURRENCY, addrs, [], [])
        assert s.total_received_fiat is None
        assert s.total_spent_fiat is None
        assert s.balance_fiat is None
        assert any("unavailable" in n for n in s.notes)

    def test_no_activity_addresses_skipped_for_span(self):
        addrs = [
            make_address("a1", first_ts=None, last_ts=None),
            make_address("a2", first_ts=700, last_ts=900),
        ]
        s = build_address_summary(CURRENCY, addrs, [], [])
        assert s.first_usage == 700
        assert s.last_usage == 900

    def test_no_activity_at_all_omits_span_with_note(self):
        addrs = [
            make_address("a1", first_ts=None, last_ts=None),
            make_address("a2", first_ts=None, last_ts=None),
        ]
        s = build_address_summary(CURRENCY, addrs, [], [])
        assert s.first_usage is None
        assert s.last_usage is None
        assert any("activity" in n for n in s.notes)

    def test_token_holdings_note(self):
        addrs = [
            make_address("a1", token_balances={"usdt": make_value(5)}),
            make_address("a2"),
        ]
        s = build_address_summary("eth", addrs, [], [])
        assert any("token" in n.lower() for n in s.notes)

    def test_tagged_count_dedupes_by_address_and_actors_pass_through(self):
        addrs = [make_address("a1"), make_address("a2"), make_address("a3")]
        tags = [
            FakeTag("a1", "binance"),
            FakeTag("a1", None),  # actor-less tag still counts the address
            FakeTag("a2", "kraken"),
        ]
        actors = [LabeledItemRef(id="binance", label="Binance")]
        s = build_address_summary(CURRENCY, addrs, tags, actors)
        assert s.tagged_address_count == 2
        assert s.actors == actors


# ---------------------------------------------------------------------------
# summary() orchestration -- address block
# ---------------------------------------------------------------------------


class FakeAddressesService:
    """Minimal fake of the surface ``summary`` calls on AddressesService."""

    def __init__(self, addr_map=None):
        self._addrs = addr_map or {}
        self.calls: list[dict] = []

    async def get_address(
        self,
        currency,
        address,
        tagstore_groups,
        include_actors=True,
        new_address_fallback=True,
    ):
        self.calls.append(
            {
                "address": address,
                "include_actors": include_actors,
                "new_address_fallback": new_address_fallback,
            }
        )
        if address not in self._addrs:
            raise AddressNotFoundException(currency, address)
        return self._addrs[address]


class FakeTagsService:
    """Minimal fake of the surface ``summary`` calls on TagsService."""

    def __init__(self, tags=None, actors=None):
        self._tags = tags or []
        self._actors = actors or {}
        self.tags_calls: list[dict] = []
        self.actor_calls: list[str] = []

    async def list_tags_by_addresses_raw(self, addresses, tagstore_groups, **kw):
        self.tags_calls.append(
            {
                "addresses": [a.address for a in addresses],
                "networks": [a.network for a in addresses],
                "groups": list(tagstore_groups),
            }
        )
        return self._tags, True

    async def get_actor(self, actor_id):
        self.actor_calls.append(actor_id)
        return self._actors[actor_id]


class TestSubgraphSummaryAddresses:
    def _addr_setup(self):
        a1, a2 = "addr1", "addr2"
        addr_map = {
            a1: make_address(
                a1,
                total_received=make_value(1000, usd=10.0),
                total_spent=make_value(0, usd=0.0),
                balance=make_value(1000, usd=10.0),
                first_ts=1000,
                last_ts=2000,
            ),
            a2: make_address(
                a2,
                total_received=make_value(500, usd=5.0),
                total_spent=make_value(500, usd=5.0),
                balance=make_value(0, usd=0.0),
                first_ts=1500,
                last_ts=3000,
            ),
        }
        return FakeAddressesService(addr_map), [a1, a2]

    def _utxo_svc_for_mixed(self):
        h0, h1 = "aa" * 32, "bb" * 32
        tx_map = {
            h0: make_tx(
                tx_hash=h0,
                inputs=[make_txvalue("a", 1)],
                outputs=[make_txvalue("b", 1)],
                total_input=1_000,
                total_output=900,
                height=100,
                timestamp=1000,
            ),
            h1: make_tx(
                tx_hash=h1,
                inputs=[make_txvalue("a", 1)],
                outputs=[make_txvalue("b", 1)],
                total_input=500,
                total_output=480,
                height=200,
                timestamp=2000,
            ),
        }
        return FakeTxsService(tx_map=tx_map), [h0, h1]

    async def test_addresses_only(self):
        addr_svc, addrs = self._addr_setup()
        tag_svc = FakeTagsService()
        s = await summary(
            None, addr_svc, tag_svc, CURRENCY, [], addrs, tagstore_groups=["public"]
        )
        assert s.txs is None
        assert s.addresses.address_count == 2
        assert s.addresses.total_received == 1500
        assert s.addresses.total_spent == 500
        assert s.addresses.balance == 1000
        assert s.addresses.total_received_fiat == 15.0
        assert s.addresses.first_usage == 1000
        assert s.addresses.last_usage == 3000
        # Header-level fetch: no per-address actor query, fail-fast lookup.
        assert all(c["include_actors"] is False for c in addr_svc.calls)
        assert all(c["new_address_fallback"] is False for c in addr_svc.calls)
        # One batched tag query over the whole set, groups threaded through.
        assert tag_svc.tags_calls == [
            {
                "addresses": addrs,
                "networks": [CURRENCY, CURRENCY],
                "groups": ["public"],
            }
        ]

    async def test_mixed_txs_and_addresses(self):
        tx_svc, hashes = self._utxo_svc_for_mixed()
        addr_svc, addrs = self._addr_setup()
        s = await summary(
            tx_svc,
            addr_svc,
            FakeTagsService(),
            CURRENCY,
            hashes,
            addrs,
            tagstore_groups=[],
        )
        assert s.txs.tx_count == 2
        assert s.addresses.address_count == 2

    async def test_actors_resolved_once_per_distinct_id(self):
        addr_svc, addrs = self._addr_setup()
        tag_svc = FakeTagsService(
            tags=[
                FakeTag("addr1", "binance"),
                FakeTag("addr2", "binance"),
                FakeTag("addr2", "kraken"),
                FakeTag("addr2", None),
            ],
            actors={
                "binance": SimpleNamespace(id="binance", label="Binance"),
                "kraken": SimpleNamespace(id="kraken", label="Kraken"),
            },
        )
        s = await summary(
            None, addr_svc, tag_svc, CURRENCY, [], addrs, tagstore_groups=[]
        )
        assert tag_svc.actor_calls == ["binance", "kraken"]
        assert [a.label for a in s.addresses.actors] == ["Binance", "Kraken"]
        assert s.addresses.tagged_address_count == 2

    async def test_duplicate_addresses_deduped(self):
        addr_svc, addrs = self._addr_setup()
        s = await summary(
            None,
            addr_svc,
            FakeTagsService(),
            CURRENCY,
            [],
            [addrs[0], addrs[0], addrs[1]],
            tagstore_groups=[],
        )
        assert s.addresses.address_count == 2
        assert len(addr_svc.calls) == 2

    async def test_unknown_address_fails_whole_request(self):
        addr_svc, addrs = self._addr_setup()
        with pytest.raises(AddressNotFoundException):
            await summary(
                None,
                addr_svc,
                FakeTagsService(),
                CURRENCY,
                [],
                [addrs[0], "unknown"],
                tagstore_groups=[],
            )

    async def test_per_list_minimum(self):
        addr_svc, addrs = self._addr_setup()
        tx_svc, hashes = self._utxo_svc_for_mixed()
        # 1 tx + 1 address: both lists below their own minimum.
        with pytest.raises(BadUserInputException, match="at least 2"):
            await summary(
                tx_svc,
                addr_svc,
                FakeTagsService(),
                CURRENCY,
                [hashes[0]],
                [addrs[0]],
                tagstore_groups=[],
            )
        # 1 tx + 2 addresses: the tx list alone is below its minimum.
        with pytest.raises(BadUserInputException, match="tx hashes"):
            await summary(
                tx_svc,
                addr_svc,
                FakeTagsService(),
                CURRENCY,
                [hashes[0]],
                addrs,
                tagstore_groups=[],
            )
        # 1 address only.
        with pytest.raises(BadUserInputException, match="addresses"):
            await summary(
                tx_svc,
                addr_svc,
                FakeTagsService(),
                CURRENCY,
                [],
                [addrs[0]],
                tagstore_groups=[],
            )
        # Nothing at all.
        with pytest.raises(BadUserInputException):
            await summary(
                tx_svc,
                addr_svc,
                FakeTagsService(),
                CURRENCY,
                [],
                [],
                tagstore_groups=[],
            )

    async def test_combined_cap_spans_both_lists(self):
        addr_svc, _ = self._addr_setup()
        tx_svc, _ = self._utxo_svc_for_mixed()
        with pytest.raises(BadUserInputException, match="at most"):
            await summary(
                tx_svc,
                addr_svc,
                FakeTagsService(),
                CURRENCY,
                [f"{i:064x}" for i in range(60)],
                [f"addr{i}" for i in range(41)],
                tagstore_groups=[],
            )
