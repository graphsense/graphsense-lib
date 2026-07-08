"""Tests for graph_service: pure per-network builders, overall merge, and
the summary orchestration over a mixed-network node set."""

from __future__ import annotations

import asyncio
from collections import namedtuple
from types import SimpleNamespace

import pytest

from graphsenselib.db.asynchronous.services.common import canonical_tx_hash
from graphsenselib.db.asynchronous.services.graph_service import (
    build_address_overall,
    build_network_address_summary,
    build_network_tx_summary,
    build_tx_overall,
    summary,
)
from graphsenselib.db.asynchronous.services.models import (
    AddressRefInternal,
    FiatValue,
    LabeledItemRef,
    TxRefInternal,
    Txs,
)
from graphsenselib.errors import (
    AddressNotFoundException,
    BadUserInputException,
    NotFoundException,
    TransactionNotFoundException,
)
from tests.db.helpers import (
    make_account_tx,
    make_address,
    make_tx,
    make_txvalue,
    make_value,
)


# ---------------------------------------------------------------------------
# canonical_tx_hash (pure)
# ---------------------------------------------------------------------------


class TestCanonicalTxHash:
    def test_lowercases_before_stripping_uppercase_0x_prefix(self):
        # removeprefix("0x") only matches a lowercase prefix, so lower()
        # must run first or an uppercase "0X" would survive untouched.
        assert canonical_tx_hash("0X" + "AB" * 32) == "ab" * 32

    def test_plain_lowercase_hash_passes_through(self):
        assert canonical_tx_hash("ab" * 32) == "ab" * 32


# ---------------------------------------------------------------------------
# build_network_tx_summary (pure)
# ---------------------------------------------------------------------------


class TestBuildNetworkTxSummary:
    def test_utxo_aggregates_value_fee_inputs_outputs_block_timestamp(self):
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
        block = build_network_tx_summary("btc", txs)
        assert block.network == "btc"
        assert block.tx_count == 2
        assert block.total_value.value == 1_380  # sum of total_output
        assert block.total_fee == 120  # (1000-900) + (500-480)
        assert block.total_inputs == 3
        assert block.total_outputs == 4
        assert block.block_min == 100
        assert block.block_max == 200
        assert block.timestamp_min == 1000
        assert block.timestamp_max == 2000

    def test_fiat_values_sum_per_code(self):
        # make_tx(...) rows carry both eur and usd rates; the block must
        # sum each code independently instead of picking one.
        txs = [
            make_tx(
                total_input=1_000,
                total_output=900,
                height=100,
                timestamp=1000,
                fiat_eur=10.0,
                fiat_usd=12.0,
            ),
            make_tx(
                total_input=500,
                total_output=480,
                height=200,
                timestamp=2000,
                fiat_eur=5.0,
                fiat_usd=6.5,
            ),
        ]
        block = build_network_tx_summary("btc", txs)
        assert block.network == "btc"
        assert block.total_value.value == 1380
        codes = {fv.code: fv.value for fv in block.total_value.fiat_values}
        assert codes == {"eur": 15.0, "usd": 18.5}

    def test_missing_rates_are_noted(self):
        txs = [
            make_tx(
                total_input=1_000,
                total_output=900,
                height=100,
                timestamp=1000,
                fiat_eur=10.0,
                fiat_usd=12.0,
            ),
            make_tx(
                total_input=500,
                total_output=480,
                height=200,
                timestamp=2000,
                no_rates=True,
            ),
        ]
        block = build_network_tx_summary("btc", txs)
        assert [n.code for n in block.notes] == ["fiat_totals_partial"]
        assert "1 of 2" in block.notes[0].message

    def test_all_rates_missing_noted(self):
        txs = [
            make_tx(total_input=1_000, total_output=900, no_rates=True),
            make_tx(total_input=500, total_output=480, no_rates=True),
        ]
        block = build_network_tx_summary("btc", txs)
        assert block.total_value.fiat_values == []
        assert [n.code for n in block.notes] == ["fiat_totals_missing"]

    def test_coinbase_contributes_no_fee(self):
        # Coinbase txs pay no fee, so an all-coinbase set has a KNOWN total
        # of 0 — not None, which is reserved for "fee data unavailable" on
        # account chains.
        txs = [
            make_tx(
                inputs=[make_txvalue("coinbase", 0)],
                outputs=[make_txvalue("b", 1)],
                coinbase=True,
                total_input=0,
                total_output=50_000,
            ),
        ]
        block = build_network_tx_summary("btc", txs)
        assert block.total_value.value == 50_000
        assert block.total_fee == 0

    def test_fiat_sum_rounded_to_cents(self):
        # 0.1 + 0.2 must come out as 0.3, not 0.30000000000000004.
        txs = [
            make_tx(total_input=100, total_output=90, fiat_eur=0.1),
            make_tx(total_input=100, total_output=90, fiat_eur=0.2),
        ]
        block = build_network_tx_summary("btc", txs)
        codes = {fv.code: fv.value for fv in block.total_value.fiat_values}
        assert codes["eur"] == 0.3

    def test_fiat_codes_merge_case_insensitively(self):
        # Codes from different sources may differ in case; "USD" and "usd"
        # must land in one bucket.
        txs = [
            make_tx(
                total_input=100,
                total_output=90,
                outputs=[],
            ),
            make_tx(total_input=100, total_output=90, fiat_usd=2.0),
        ]
        txs[0].total_output.fiat_values = [FiatValue(code="USD", value=1.0)]
        block = build_network_tx_summary("btc", txs)
        assert [(fv.code, fv.value) for fv in block.total_value.fiat_values] == [
            ("usd", 3.0)
        ]

    def test_account_aggregates_fee_without_double_count(self):
        # Fees sum across base transactions only: the token leg carries
        # fee=None so the tx's fee is counted once.
        txs = [
            make_account_tx(value=1_000, value_usd=1.0, fee=21),
            make_account_tx(value=2_000, value_usd=2.0, fee=42),
            make_account_tx(value=0, value_usd=3.0, token_tx_id=0, asset="usdt"),
        ]
        block = build_network_tx_summary("eth", txs)
        assert block.total_fee == 63

    def test_account_fee_none_when_unavailable(self):
        txs = [
            make_account_tx(value=1, value_usd=1.0),
            make_account_tx(value=2, value_usd=2.0),
        ]
        block = build_network_tx_summary("eth", txs)
        assert block.total_fee is None

    def test_account_fee_none_when_partially_unavailable(self):
        # A partial sum would silently understate the total, so one base tx
        # without fee data makes the whole total "unknown" (None).
        txs = [
            make_account_tx(value=1, value_usd=1.0, fee=21),
            make_account_tx(value=2, value_usd=2.0),
        ]
        block = build_network_tx_summary("eth", txs)
        assert block.total_fee is None

    def test_account_tx_count_excludes_token_legs(self):
        # One submitted eth tx expands to a base leg plus a token leg; the
        # response's tx_count must reflect the submitted transaction, not
        # the number of asset-flow legs.
        txs = [
            make_account_tx(value=1_000, value_usd=2.5),
            make_account_tx(value=0, value_usd=5.0, token_tx_id=0, asset="usdt"),
        ]
        block = build_network_tx_summary("eth", txs)
        assert block.tx_count == 1

    def test_account_missing_rates_counted_per_transfer(self):
        # Fiat sums are per leg on account chains, so the missing-rate note
        # counts transfers, not submitted txs.
        txs = [
            make_account_tx(value=1_000, value_usd=2.5),
            make_account_tx(value=0, token_tx_id=0, asset="usdt"),  # no rate
        ]
        block = build_network_tx_summary("eth", txs)
        codes = {n.code: n for n in block.notes}
        assert "fiat_totals_partial" in codes
        assert "1 of 2 transfers had no rate" in codes["fiat_totals_partial"].message

    def test_account_no_io_counts_and_token_excluded(self):
        # Native ETH transfer + a USDT token transfer. total_value is the
        # native-unit (wei) sum and excludes the token; its fiat_values sum
        # per code across both; a note records the excluded token transfer.
        txs = [
            make_account_tx(
                value=1_000, value_usd=2.5, value_eur=2.0, height=10, timestamp=100
            ),
            make_account_tx(
                value=5_000_000,
                value_usd=5.0,
                value_eur=4.0,
                token_tx_id=0,
                asset="usdt",
                height=11,
                timestamp=110,
            ),
        ]
        block = build_network_tx_summary("eth", txs)
        assert block.total_value.value == 1_000  # native transfer only
        codes = {fv.code: fv.value for fv in block.total_value.fiat_values}
        assert codes == {"eur": 6.0, "usd": 7.5}  # both, per code
        assert block.total_inputs is None
        assert block.total_outputs is None
        assert "token_value_excluded" in [n.code for n in block.notes]

    def test_utxo_assets_is_native_only(self):
        block = build_network_tx_summary("btc", [make_tx(), make_tx()])
        assert block.assets == ["btc"]

    def test_account_assets_native_first_tokens_sorted_lowercased(self):
        # Base eth leg plus two token legs (declared out of order, mixed case)
        txs = [
            make_account_tx(value=1_000, value_usd=2.5),  # currency "eth"
            make_account_tx(value=0, value_usd=5.0, token_tx_id=0, asset="USDT"),
            make_account_tx(value=0, value_usd=1.0, token_tx_id=1, asset="usdc"),
        ]
        block = build_network_tx_summary("eth", txs)
        assert block.assets == ["eth", "usdc", "usdt"]

    def test_account_assets_native_only_when_no_tokens(self):
        txs = [
            make_account_tx(value=1, value_usd=1.0),
            make_account_tx(value=2, value_usd=2.0),
        ]
        block = build_network_tx_summary("eth", txs)
        assert block.assets == ["eth"]


# ---------------------------------------------------------------------------
# build_tx_overall (pure)
# ---------------------------------------------------------------------------


class TestBuildTxOverall:
    def test_merges_fiat_and_timestamps_across_networks(self):
        btc = build_network_tx_summary(
            "btc",
            [
                make_tx(
                    total_input=1_000,
                    total_output=900,
                    height=100,
                    timestamp=1000,
                    fiat_eur=10.0,
                    fiat_usd=12.0,
                ),
                make_tx(
                    total_input=500,
                    total_output=480,
                    height=200,
                    timestamp=2000,
                    fiat_eur=5.0,
                    fiat_usd=6.5,
                ),
            ],
        )
        eth = build_network_tx_summary(
            "eth",
            [
                make_account_tx(
                    value=1_000, value_usd=3.0, value_eur=2.0, height=10, timestamp=500
                ),
                make_account_tx(
                    value=2_000, value_usd=6.0, value_eur=4.0, height=20, timestamp=3000
                ),
            ],
        )
        overall = build_tx_overall([btc, eth])
        assert overall.tx_count == btc.tx_count + eth.tx_count
        assert overall.timestamp_min == min(btc.timestamp_min, eth.timestamp_min)
        assert overall.timestamp_max == max(btc.timestamp_max, eth.timestamp_max)
        codes = {fv.code: fv.value for fv in overall.total_value_fiat}
        assert codes == {"eur": 21.0, "usd": 27.5}
        # overall has no block heights (chain-specific), by model shape.
        assert not hasattr(overall, "block_min")

    def test_network_notes_are_tagged_with_network(self):
        # a per-network note appears in overall tagged with its network.
        btc = build_network_tx_summary(
            "btc",
            [
                make_tx(total_input=1_000, total_output=900, fiat_usd=1.0),
                make_tx(total_input=500, total_output=480, fiat_usd=2.0),
            ],
        )
        eth = build_network_tx_summary(
            "eth",
            [
                make_account_tx(value=1_000, value_usd=None),
                make_account_tx(value=2_000, value_usd=None),
            ],
        )
        overall = build_tx_overall([btc, eth])
        assert {(n.network, n.code) for n in overall.notes} == {
            ("eth", "fiat_totals_missing"),
        }


# ---------------------------------------------------------------------------
# build_network_address_summary / build_address_overall (pure)
# ---------------------------------------------------------------------------

# Minimal stand-in for tagstore TagPublic: the aggregator only reads
# .identifier (the tagged address) and .actor (an actor id or None).
# FakeTagsService additionally matches on .network, mirroring the real
# tagstore query, so tests that route tags through it must set a real one.
FakeTag = namedtuple("FakeTag", "identifier actor network")


class TestBuildNetworkAddressSummary:
    def _two_addresses(self):
        return [
            make_address(
                "a1",
                total_received=make_value(1000, usd=10.0, eur=9.0),
                total_spent=make_value(400, usd=4.0, eur=3.6),
                balance=make_value(600, usd=6.0, eur=5.4),
                first_ts=1000,
                last_ts=5000,
            ),
            make_address(
                "a2",
                total_received=make_value(2000, usd=20.0, eur=18.0),
                total_spent=make_value(0, usd=0.0, eur=0.0),
                balance=make_value(2000, usd=20.0, eur=18.0),
                first_ts=500,
                last_ts=3000,
            ),
        ]

    def test_aggregates_values_and_usage_span(self):
        block = build_network_address_summary("btc", self._two_addresses(), [], [])
        assert block.network == "btc"
        assert block.address_count == 2
        assert block.total_received.value == 3000
        assert {fv.code: fv.value for fv in block.total_received.fiat_values} == {
            "eur": 27.0,
            "usd": 30.0,
        }
        assert block.total_spent.value == 400
        assert block.balance.value == 2600
        assert block.first_usage == 500
        assert block.last_usage == 5000
        assert block.tagged_address_count == 0
        assert block.actors == []
        assert block.notes == []

    def test_address_assets_native_first_tokens_sorted(self):
        addr = make_address(
            token_balances={"USDT": make_value(5), "usdc": make_value(3)}
        )
        block = build_network_address_summary("eth", [addr], [], [])
        assert block.assets == ["eth", "usdc", "usdt"]

    def test_address_assets_native_only_when_no_tokens(self):
        block = build_network_address_summary("btc", [make_address()], [], [])
        assert block.assets == ["btc"]

    def test_partial_fiat_noted(self):
        addrs = [
            make_address("a1", total_received=make_value(100, usd=1.0)),
            make_address("a2", total_received=make_value(200)),  # no rate
        ]
        block = build_network_address_summary("btc", addrs, [], [])
        assert block.total_received.value == 300
        assert [n.code for n in block.notes] == ["fiat_totals_partial"]
        assert "1 of 2 addresses had no rate" in block.notes[0].message

    def test_all_fiat_missing_noted(self):
        addrs = [
            make_address("a1", total_received=make_value(100)),
            make_address("a2", total_received=make_value(200)),
        ]
        block = build_network_address_summary("btc", addrs, [], [])
        assert block.total_received.fiat_values == []
        assert [n.code for n in block.notes] == ["fiat_totals_missing"]

    def test_fiat_sums_rounded_to_cents(self):
        # 0.1 + 0.2 must come out as 0.3, not 0.30000000000000004.
        addrs = [
            make_address("a1", total_received=make_value(1, eur=0.1)),
            make_address("a2", total_received=make_value(2, eur=0.2)),
        ]
        block = build_network_address_summary("btc", addrs, [], [])
        codes = {fv.code: fv.value for fv in block.total_received.fiat_values}
        assert codes["eur"] == 0.3

    def test_inactive_addresses_skipped_for_usage_span(self):
        # Addresses without activity are skipped; the span still derives
        # from the active ones (and no unavailable-note is emitted).
        addrs = [
            make_address("a1", first_ts=None, last_ts=None),
            make_address("a2", first_ts=700, last_ts=900),
        ]
        block = build_network_address_summary("btc", addrs, [], [])
        assert block.first_usage == 700
        assert block.last_usage == 900
        assert "usage_span_unavailable" not in [n.code for n in block.notes]

    def test_no_activity_at_all_omits_span_with_note(self):
        addrs = [
            make_address("a1", first_ts=None, last_ts=None),
            make_address("a2", first_ts=None, last_ts=None),
        ]
        block = build_network_address_summary("btc", addrs, [], [])
        assert block.first_usage is None
        assert block.last_usage is None
        assert "usage_span_unavailable" in [n.code for n in block.notes]

    def test_token_holdings_note(self):
        addrs = [
            make_address("a1", token_balances={"usdt": make_value(5)}),
            make_address("a2"),
        ]
        block = build_network_address_summary("eth", addrs, [], [])
        assert "token_holdings_excluded" in [n.code for n in block.notes]

    def test_tagged_count_dedupes_and_actors_pass_through(self):
        addrs = [make_address("a1"), make_address("a2"), make_address("a3")]
        tags = [
            FakeTag("a1", "binance", "btc"),
            FakeTag("a1", None, "btc"),  # actor-less tag still counts the address
            FakeTag("a2", "kraken", "btc"),
        ]
        actors = [LabeledItemRef(id="binance", label="Binance")]
        block = build_network_address_summary("btc", addrs, tags, actors)
        assert block.tagged_address_count == 2
        assert block.actors == actors


class TestBuildAddressOverall:
    def test_merges_fiat_usage_and_dedups_actors(self):
        btc = build_network_address_summary(
            "btc",
            [
                make_address(
                    "a1",
                    total_received=make_value(1000, usd=10.0, eur=9.0),
                    balance=make_value(1000, usd=10.0, eur=9.0),
                    first_ts=1000,
                    last_ts=2000,
                )
            ]
            * 1
            + [
                make_address(
                    "a2",
                    total_received=make_value(500, usd=5.0, eur=4.5),
                    balance=make_value(500, usd=5.0, eur=4.5),
                    first_ts=1500,
                    last_ts=4000,
                )
            ],
            [],
            [LabeledItemRef(id="binance", label="Binance")],
        )
        eth = build_network_address_summary(
            "eth",
            [
                make_address(
                    "0xa",
                    total_received=make_value(2000, usd=20.0, eur=18.0),
                    balance=make_value(2000, usd=20.0, eur=18.0),
                    first_ts=800,
                    last_ts=900,
                ),
                make_address(
                    "0xb",
                    total_received=make_value(0, usd=0.0, eur=0.0),
                    balance=make_value(0, usd=0.0, eur=0.0),
                    first_ts=None,
                    last_ts=None,
                ),
            ],
            [],
            [LabeledItemRef(id="binance", label="Binance")],  # same id as btc block
        )
        overall = build_address_overall([btc, eth])
        assert overall.address_count == 4
        assert {fv.code: fv.value for fv in overall.total_received_fiat} == {
            "eur": 31.5,
            "usd": 35.0,
        }
        assert overall.first_usage == 800
        assert overall.last_usage == 4000
        # binance appears in both network blocks, deduped to one in overall.
        assert [a.id for a in overall.actors] == ["binance"]


# ---------------------------------------------------------------------------
# summary() orchestration
# ---------------------------------------------------------------------------


class FakeDb:
    def __init__(self, supported):
        self._supported = list(supported)

    def get_supported_currencies(self):
        return self._supported


class FakeTxsService:
    """Minimal fake implementing the surface ``summary`` calls."""

    def __init__(self, tx_map=None, flows_map=None, supported=("btc", "eth")):
        self._tx_map = tx_map or {}
        self._flows = flows_map or {}
        self.get_tx_calls: list[dict] = []
        self.flows_calls: list[dict] = []
        self.db = FakeDb(supported)

    async def get_tx(self, network, tx_hash, *args, **kwargs):
        self.get_tx_calls.append({"network": network, **kwargs})
        if tx_hash not in self._tx_map:
            raise TransactionNotFoundException(network, tx_hash)
        return self._tx_map[tx_hash]

    async def get_asset_flows_within_tx(self, network, tx_hash, **kwargs):
        self.flows_calls.append({"network": network, "tx_hash": tx_hash, **kwargs})
        # Mirror the real service: an unknown hash raises rather than
        # returning an empty flow set.
        if tx_hash not in self._flows and tx_hash not in self._tx_map:
            raise TransactionNotFoundException(network, tx_hash)
        legs = self._flows.get(tx_hash) or [self._tx_map[tx_hash]]
        return Txs(txs=legs)


class FakeAddressesService:
    """Minimal fake of the surface ``summary`` calls on AddressesService."""

    def __init__(self, addr_map=None):
        self._addrs = addr_map or {}
        self.calls: list[dict] = []

    async def get_address(
        self,
        network,
        address,
        tagstore_groups,
        include_actors=True,
        new_address_fallback=True,
    ):
        self.calls.append(
            {
                "network": network,
                "address": address,
                "include_actors": include_actors,
                "new_address_fallback": new_address_fallback,
            }
        )
        if address not in self._addrs:
            raise AddressNotFoundException(network, address)
        return self._addrs[address]


class FakeTagsService:
    """Minimal fake of the surface ``summary`` calls on TagsService."""

    def __init__(self, tags=None, actors=None):
        self._tags = tags or []
        self._actors = actors or {}
        self.tags_calls: list[dict] = []
        self.actor_calls: list[str] = []

    async def list_tags_by_addresses_raw(self, addresses, tagstore_groups, **kw):
        keys = {(a.network, a.address) for a in addresses}
        self.tags_calls.append(
            {
                "addresses": [a.address for a in addresses],
                "networks": [a.network for a in addresses],
                "groups": list(tagstore_groups),
            }
        )
        # Tags match on (network, identifier), mirroring the real tagstore
        # query, so per-network calls only see their own tags.
        return [t for t in self._tags if (t.network, t.identifier) in keys], True

    async def get_actor(self, actor_id):
        self.actor_calls.append(actor_id)
        return self._actors[actor_id]


def _btc_txs():
    h0, h1 = "aa" * 32, "bb" * 32
    tx_map = {
        h0: make_tx(
            tx_hash=h0,
            total_input=1_000,
            total_output=900,
            height=100,
            timestamp=1000,
            fiat_eur=10.0,
            fiat_usd=12.0,
        ),
        h1: make_tx(
            tx_hash=h1,
            total_input=500,
            total_output=480,
            height=200,
            timestamp=2000,
            fiat_eur=5.0,
            fiat_usd=6.5,
        ),
    }
    return tx_map, h0, h1


class TestGraphSummary:
    async def test_utxo_header_only_fetch(self):
        h0, h1 = "aa" * 32, "bb" * 32
        tx_map = {
            h0: make_tx(
                tx_hash=h0,
                inputs=[make_txvalue("addr_a", 600), make_txvalue("addr_b", 400)],
                outputs=[make_txvalue("addr_c", 900)],
                height=100,
                timestamp=1000,
                fiat_eur=10.0,
                fiat_usd=12.0,
            ),
            h1: make_tx(
                tx_hash=h1,
                inputs=[make_txvalue("addr_d", 500)],
                outputs=[make_txvalue("addr_e", 480)],
                height=200,
                timestamp=2000,
                fiat_eur=5.0,
                fiat_usd=6.5,
            ),
        }
        svc = FakeTxsService(tx_map=tx_map)
        refs = [
            TxRefInternal(network="btc", tx_hash=h0),
            TxRefInternal(network="btc", tx_hash=h1),
        ]
        s = await summary(svc, None, None, refs, [], tagstore_groups=[])
        assert s.addresses is None  # no addresses requested, block omitted
        assert [b.network for b in s.txs.networks] == ["btc"]
        block = s.txs.networks[0]
        assert block.tx_count == 2
        assert block.total_value.value == 1_380
        # IO counts come from the row header, independent of include_io.
        assert block.total_inputs == 3
        assert block.total_outputs == 2
        # UTXO summary fetches headers only: no IO, no heuristics.
        assert svc.get_tx_calls[0]["include_io"] is False
        assert svc.get_tx_calls[0]["include_heuristics"] == []
        assert svc.flows_calls == []

    async def test_mixed_networks_two_blocks_and_overall_fiat(self):
        tx_map, h0, h1 = _btc_txs()
        e0, e1 = "cc" * 32, "dd" * 32
        tx_map[e0] = make_account_tx(
            tx_hash=e0, value=1_000, value_usd=3.0, value_eur=2.0, timestamp=500
        )
        tx_map[e1] = make_account_tx(
            tx_hash=e1, value=2_000, value_usd=6.0, value_eur=4.0, timestamp=3000
        )
        svc = FakeTxsService(tx_map=tx_map)
        refs = [
            TxRefInternal(network="btc", tx_hash=h0),
            TxRefInternal(network="btc", tx_hash=h1),
            TxRefInternal(network="eth", tx_hash=e0),
            TxRefInternal(network="eth", tx_hash=e1),
        ]
        s = await summary(svc, None, None, refs, [], tagstore_groups=[])
        # networks entries in request (first-appearance) order.
        assert [b.network for b in s.txs.networks] == ["btc", "eth"]
        assert s.txs.overall.tx_count == 4
        # overall fiat equals per-code sum of both blocks.
        codes = {fv.code: fv.value for fv in s.txs.overall.total_value_fiat}
        assert codes == {"eur": 21.0, "usd": 27.5}
        # Account path uses asset flows, UTXO path uses get_tx headers.
        assert [c["tx_hash"] for c in svc.flows_calls] == [e0, e1]
        assert [c["network"] for c in svc.get_tx_calls] == ["btc", "btc"]
        flow_call = svc.flows_calls[0]
        # Correctness-critical fetch flags: internal traces excluded (they
        # are not submitted txs), token legs and the base leg included.
        assert flow_call["include_internal_txs"] is False
        assert flow_call["include_token_txs"] is True
        assert flow_call["include_base_transaction"] is True

    async def test_dedup_on_network_and_hash(self):
        tx_map, h0, h1 = _btc_txs()
        svc = FakeTxsService(tx_map=tx_map)
        refs = [
            TxRefInternal(network="btc", tx_hash=h0),
            TxRefInternal(network="btc", tx_hash=h1),
            TxRefInternal(network="btc", tx_hash=h0),  # dup
        ]
        s = await summary(svc, None, None, refs, [], tagstore_groups=[])
        assert s.txs.networks[0].tx_count == 2
        assert len(svc.get_tx_calls) == 2

    async def test_case_variant_utxo_tx_refs_deduped(self):
        # Hex hashes compare case-insensitively downstream, so a spelling
        # variant of an already-listed hash is the same node: fetched once,
        # counted once.
        tx_map, h0, h1 = _btc_txs()
        svc = FakeTxsService(tx_map=tx_map)
        refs = [
            TxRefInternal(network="btc", tx_hash=h0),
            TxRefInternal(network="btc", tx_hash=h0.upper()),
            TxRefInternal(network="btc", tx_hash=h1),
        ]
        s = await summary(svc, None, None, refs, [], tagstore_groups=[])
        assert s.txs.networks[0].tx_count == 2
        assert len(svc.get_tx_calls) == 2

    async def test_case_and_prefix_variant_tx_refs_do_not_satisfy_minimum(self):
        # Two spellings of one eth tx (0x prefix, upper hex) are one distinct
        # node and must not satisfy the 2-distinct minimum.
        e0 = "cc" * 32
        eth = make_account_tx(tx_hash=e0, value=1_000, value_usd=3.0)
        svc = FakeTxsService(flows_map={e0: [eth]})
        refs = [
            TxRefInternal(network="eth", tx_hash=e0),
            TxRefInternal(network="eth", tx_hash="0x" + e0.upper()),
        ]
        with pytest.raises(BadUserInputException, match="at least 2"):
            await summary(svc, None, None, refs, [], tagstore_groups=[])

    async def test_same_hash_different_networks_not_deduped(self):
        # A btc and an eth ref sharing the same hash string are distinct nodes.
        h = "aa" * 32
        tx_map = {
            h: make_tx(tx_hash=h, total_input=1_000, total_output=900, fiat_usd=1.0),
        }
        eth = make_account_tx(tx_hash=h, value=1_000, value_usd=2.0)
        svc = FakeTxsService(tx_map={h: tx_map[h]}, flows_map={h: [eth]})
        refs = [
            TxRefInternal(network="btc", tx_hash=h),
            TxRefInternal(network="eth", tx_hash=h),
        ]
        s = await summary(svc, None, None, refs, [], tagstore_groups=[])
        assert {b.network for b in s.txs.networks} == {"btc", "eth"}
        assert s.txs.overall.tx_count == 2

    async def test_uppercase_network_accepted_and_normalized(self):
        tx_map, h0, h1 = _btc_txs()
        svc = FakeTxsService(tx_map=tx_map)
        refs = [
            TxRefInternal(network="BTC", tx_hash=h0),
            TxRefInternal(network="btc", tx_hash=h1),
        ]
        s = await summary(svc, None, None, refs, [], tagstore_groups=[])
        assert [b.network for b in s.txs.networks] == ["btc"]
        assert s.txs.networks[0].tx_count == 2

    async def test_account_summary_counts_submitted_txs_not_legs(self):
        # A submitted eth tx with a token transfer expands to 2 legs; the
        # per-network and overall counts must stay at the submitted 2 txs.
        e0, e1 = "cc" * 32, "dd" * 32
        flows = {
            e0: [
                make_account_tx(tx_hash=e0, value=1_000, value_usd=3.0),
                make_account_tx(
                    tx_hash=e0, value=0, value_usd=5.0, token_tx_id=0, asset="usdt"
                ),
            ],
            e1: [make_account_tx(tx_hash=e1, value=2_000, value_usd=6.0)],
        }
        svc = FakeTxsService(flows_map=flows)
        refs = [
            TxRefInternal(network="eth", tx_hash=e0),
            TxRefInternal(network="eth", tx_hash=e1),
        ]
        s = await summary(svc, None, None, refs, [], tagstore_groups=[])
        assert s.txs.networks[0].tx_count == 2
        assert s.txs.overall.tx_count == 2
        # Fiat still sums across all legs, token transfer included.
        codes = {fv.code: fv.value for fv in s.txs.overall.total_value_fiat}
        assert codes == {"usd": 14.0}

    async def test_needs_at_least_two_tx_refs(self):
        tx_map, h0, h1 = _btc_txs()
        svc = FakeTxsService(tx_map=tx_map)
        with pytest.raises(BadUserInputException, match="at least 2"):
            await summary(
                svc,
                None,
                None,
                [TxRefInternal(network="btc", tx_hash=h0)],
                [],
                tagstore_groups=[],
            )

    async def test_empty_rejected(self):
        svc = FakeTxsService(tx_map={})
        with pytest.raises(BadUserInputException):
            await summary(svc, None, None, [], [], tagstore_groups=[])

    async def test_too_many_nodes_rejected(self):
        svc = FakeTxsService(tx_map={})
        refs = [TxRefInternal(network="btc", tx_hash=f"{i:064x}") for i in range(101)]
        with pytest.raises(BadUserInputException, match="at most 100"):
            await summary(svc, None, None, refs, [], tagstore_groups=[])

    async def test_unsupported_network_rejected(self):
        tx_map, h0, h1 = _btc_txs()
        svc = FakeTxsService(tx_map=tx_map, supported=["btc", "eth"])
        refs = [
            TxRefInternal(network="doge", tx_hash=h0),
            TxRefInternal(network="doge", tx_hash=h1),
        ]
        with pytest.raises(BadUserInputException, match="unsupported network"):
            await summary(svc, None, None, refs, [], tagstore_groups=[])

    async def test_missing_tx_below_minimum_raises_not_found(self):
        # 2 refs, 1 unknown: only 1 known survivor is left, which is below
        # the 2-node minimum, so the whole request 404s naming the missing.
        tx_map, h0, h1 = _btc_txs()
        svc = FakeTxsService(tx_map=tx_map)
        missing = "ff" * 32
        refs = [
            TxRefInternal(network="btc", tx_hash=h0),
            TxRefInternal(network="btc", tx_hash=missing),
        ]
        with pytest.raises(NotFoundException, match=f"fewer than 2.*btc:{missing}"):
            await summary(svc, None, None, refs, [], tagstore_groups=[])

    async def test_missing_tx_dropped_with_note(self):
        # 3 refs, 1 unknown: the summary proceeds over the 2 known txs and
        # reports the dropped ref in a machine-readable nodes_not_found
        # note on the overall rollup.
        tx_map, h0, h1 = _btc_txs()
        svc = FakeTxsService(tx_map=tx_map)
        missing = "ff" * 32
        refs = [
            TxRefInternal(network="btc", tx_hash=h0),
            TxRefInternal(network="btc", tx_hash=h1),
            TxRefInternal(network="btc", tx_hash=missing),
        ]
        result = await summary(svc, None, None, refs, [], tagstore_groups=[])
        assert result.txs.overall.tx_count == 2
        assert result.txs.networks[0].tx_count == 2
        notes = [n for n in result.txs.overall.notes if n.code == "nodes_not_found"]
        assert len(notes) == 1
        assert notes[0].network == "btc"
        assert notes[0].items == [missing]

    async def test_network_with_only_missing_txs_gets_no_block(self):
        # Both eth refs are unknown; the btc pair survives. The eth
        # per-network block is absent and the drop is attributed to eth in
        # the overall note.
        tx_map, h0, h1 = _btc_txs()
        svc = FakeTxsService(tx_map=tx_map)
        missing = "ee" * 32
        refs = [
            TxRefInternal(network="btc", tx_hash=h0),
            TxRefInternal(network="btc", tx_hash=h1),
            TxRefInternal(network="eth", tx_hash=missing),
        ]
        result = await summary(svc, None, None, refs, [], tagstore_groups=[])
        assert [b.network for b in result.txs.networks] == ["btc"]
        notes = [n for n in result.txs.overall.notes if n.code == "nodes_not_found"]
        assert len(notes) == 1
        assert notes[0].network == "eth"
        assert notes[0].items == [missing]

    async def test_duplicate_tx_refs_noted(self):
        # 2 distinct txs, one submitted twice (once with an uppercase
        # spelling): the collapse must be observable via a
        # duplicates_collapsed note, not just a smaller tx_count.
        tx_map, h0, h1 = _btc_txs()
        svc = FakeTxsService(tx_map=tx_map)
        refs = [
            TxRefInternal(network="btc", tx_hash=h0),
            TxRefInternal(network="btc", tx_hash=h1),
            TxRefInternal(network="btc", tx_hash=h0.upper()),
        ]
        result = await summary(svc, None, None, refs, [], tagstore_groups=[])
        assert result.txs.overall.tx_count == 2
        notes = [
            n for n in result.txs.overall.notes if n.code == "duplicates_collapsed"
        ]
        assert len(notes) == 1
        assert notes[0].network == "btc"
        assert notes[0].items == [h0]

    async def test_no_duplicates_no_note(self):
        tx_map, h0, h1 = _btc_txs()
        svc = FakeTxsService(tx_map=tx_map)
        refs = [
            TxRefInternal(network="btc", tx_hash=h0),
            TxRefInternal(network="btc", tx_hash=h1),
        ]
        result = await summary(svc, None, None, refs, [], tagstore_groups=[])
        assert not any(
            n.code == "duplicates_collapsed" for n in result.txs.overall.notes
        )

    async def test_sub_tx_identifier_rejected(self):
        # <hash>_T1-style refs would double count against their base hash
        # (dedup cannot unify them) and are rejected with a clear 400.
        tx_map, h0, h1 = _btc_txs()
        svc = FakeTxsService(tx_map=tx_map)
        refs = [
            TxRefInternal(network="eth", tx_hash="0x" + "cc" * 32),
            TxRefInternal(network="eth", tx_hash="0x" + "cc" * 32 + "_T1"),
        ]
        with pytest.raises(BadUserInputException, match="sub-transaction"):
            await summary(svc, None, None, refs, [], tagstore_groups=[])

    async def test_all_txs_missing_raises_not_found(self):
        svc = FakeTxsService(tx_map={})
        refs = [
            TxRefInternal(network="btc", tx_hash="ee" * 32),
            TxRefInternal(network="btc", tx_hash="ff" * 32),
        ]
        with pytest.raises(NotFoundException, match="fewer than 2"):
            await summary(svc, None, None, refs, [], tagstore_groups=[])

    async def test_failed_tx_phase_cancels_address_phase(self):
        # A 404 from the tx phase must not leave the address phase's db
        # queries running in the background after summary() has raised.
        tx_map, h0, _ = _btc_txs()
        svc = FakeTxsService(tx_map=tx_map)

        started = asyncio.Event()
        cancelled = asyncio.Event()

        class HangingAddressesService(FakeAddressesService):
            async def get_address(self, *args, **kwargs):
                started.set()
                try:
                    await asyncio.Event().wait()
                except asyncio.CancelledError:
                    cancelled.set()
                    raise

        tx_refs = [
            TxRefInternal(network="btc", tx_hash=h0),
            TxRefInternal(network="btc", tx_hash="ff" * 32),
        ]
        addr_refs = [
            AddressRefInternal(network="btc", address="a1"),
            AddressRefInternal(network="btc", address="a2"),
        ]
        with pytest.raises(NotFoundException):
            await summary(
                svc,
                HangingAddressesService(),
                FakeTagsService(),
                tx_refs,
                addr_refs,
                tagstore_groups=[],
            )
        assert started.is_set()
        assert cancelled.is_set()

    async def test_missing_account_tx_below_minimum_raises_not_found(self):
        e0, e1 = "cc" * 32, "dd" * 32
        svc = FakeTxsService(tx_map={e0: make_account_tx(tx_hash=e0)})
        refs = [
            TxRefInternal(network="eth", tx_hash=e0),
            TxRefInternal(network="eth", tx_hash=e1),
        ]
        with pytest.raises(NotFoundException, match="fewer than 2"):
            await summary(svc, None, None, refs, [], tagstore_groups=[])

    async def test_malformed_bch_address_raises_bad_user_input(self):
        # cannonicalize_address must map structurally broken cashaddrs to a
        # 400, not let IndexError/InvalidAddress escape as a 500.
        svc = FakeTxsService(supported=("btc", "bch"))
        refs = [
            AddressRefInternal(network="bch", address="bitcoincash:qq"),
            AddressRefInternal(network="bch", address="bitcoincash:zz"),
        ]
        with pytest.raises(BadUserInputException):
            await summary(
                svc,
                FakeAddressesService(),
                FakeTagsService(),
                [],
                refs,
                tagstore_groups=[],
            )


class TestGraphSummaryAddresses:
    def _addr_setup(self):
        addr_map = {
            "b1": make_address(
                "b1",
                total_received=make_value(1000, usd=10.0),
                balance=make_value(1000, usd=10.0),
                first_ts=1000,
                last_ts=2000,
            ),
            "b2": make_address(
                "b2",
                total_received=make_value(500, usd=5.0),
                balance=make_value(0, usd=0.0),
                first_ts=1500,
                last_ts=3000,
            ),
            "e1": make_address(
                "e1",
                total_received=make_value(2000, usd=20.0),
                balance=make_value(2000, usd=20.0),
                first_ts=800,
                last_ts=900,
            ),
            "e2": make_address(
                "e2",
                total_received=make_value(300, usd=3.0),
                balance=make_value(300, usd=3.0),
                first_ts=700,
                last_ts=1200,
            ),
        }
        return FakeAddressesService(addr_map)

    async def test_addresses_only_single_network(self):
        addr_svc = self._addr_setup()
        tag_svc = FakeTagsService()
        tx_svc = FakeTxsService(supported=["btc", "eth"])
        refs = [
            AddressRefInternal(network="btc", address="b1"),
            AddressRefInternal(network="btc", address="b2"),
        ]
        s = await summary(
            tx_svc, addr_svc, tag_svc, [], refs, tagstore_groups=["public"]
        )
        assert s.txs is None
        assert [b.network for b in s.addresses.networks] == ["btc"]
        block = s.addresses.networks[0]
        assert block.address_count == 2
        assert block.total_received.value == 1500
        assert s.addresses.overall.first_usage == 1000
        assert s.addresses.overall.last_usage == 3000
        # Header-level fetch: no per-address actor query, fail-fast lookup.
        assert all(c["include_actors"] is False for c in addr_svc.calls)
        assert all(c["new_address_fallback"] is False for c in addr_svc.calls)

    async def test_mixed_networks_two_address_blocks(self):
        addr_svc = self._addr_setup()
        tx_svc = FakeTxsService(supported=["btc", "eth"])
        refs = [
            AddressRefInternal(network="btc", address="b1"),
            AddressRefInternal(network="btc", address="b2"),
            AddressRefInternal(network="eth", address="e1"),
            AddressRefInternal(network="eth", address="e2"),
        ]
        s = await summary(
            tx_svc, addr_svc, FakeTagsService(), [], refs, tagstore_groups=[]
        )
        assert [b.network for b in s.addresses.networks] == ["btc", "eth"]
        assert s.addresses.overall.address_count == 4
        assert s.addresses.overall.first_usage == 700

    async def test_actors_deduped_by_id_across_networks(self):
        addr_svc = self._addr_setup()
        tag_svc = FakeTagsService(
            tags=[
                FakeTag("b1", "binance", "btc"),
                FakeTag("e1", "binance", "eth"),  # same actor, other network
                FakeTag("e2", "kraken", "eth"),
            ],
            actors={
                "binance": SimpleNamespace(id="binance", label="Binance"),
                "kraken": SimpleNamespace(id="kraken", label="Kraken"),
            },
        )
        tx_svc = FakeTxsService(supported=["btc", "eth"])
        refs = [
            AddressRefInternal(network="btc", address="b1"),
            AddressRefInternal(network="btc", address="b2"),
            AddressRefInternal(network="eth", address="e1"),
            AddressRefInternal(network="eth", address="e2"),
        ]
        s = await summary(tx_svc, addr_svc, tag_svc, [], refs, tagstore_groups=[])
        # Each distinct actor id resolved once across all networks.
        assert sorted(tag_svc.actor_calls) == ["binance", "kraken"]
        # overall dedupes binance across the two network blocks.
        assert sorted(a.id for a in s.addresses.overall.actors) == ["binance", "kraken"]
        assert s.addresses.overall.tagged_address_count == 3

    async def test_tags_matched_per_network(self):
        # A tag on the btc spelling of an address must not leak into an
        # eth block that queries the same identifier string. The identifier
        # is hex-shaped so it canonicalizes cleanly on both networks.
        shared = "0x" + "ab" * 20
        other = "0x" + "cd" * 20
        addr_svc = FakeAddressesService(
            {
                shared: make_address(
                    shared,
                    total_received=make_value(100, usd=1.0),
                    balance=make_value(100, usd=1.0),
                ),
                other: make_address(
                    other,
                    total_received=make_value(200, usd=2.0),
                    balance=make_value(200, usd=2.0),
                ),
            }
        )
        tag_svc = FakeTagsService(
            tags=[FakeTag(shared, "binance", "btc")],
            actors={"binance": SimpleNamespace(id="binance", label="Binance")},
        )
        tx_svc = FakeTxsService(supported=["btc", "eth"])
        refs = [
            AddressRefInternal(network="btc", address=shared),
            AddressRefInternal(network="btc", address=other),
            AddressRefInternal(network="eth", address=shared),
            AddressRefInternal(network="eth", address=other),
        ]
        s = await summary(tx_svc, addr_svc, tag_svc, [], refs, tagstore_groups=[])
        blocks = {b.network: b for b in s.addresses.networks}
        assert blocks["btc"].tagged_address_count == 1
        assert blocks["eth"].tagged_address_count == 0

    async def test_missing_actor_is_skipped_not_fatal(self):
        # One tag references a resolvable actor, another references an actor
        # id whose pack is not loaded (get_actor raises NotFoundException).
        # The summary must still succeed: the resolved actor appears, the
        # missing one is silently omitted (per-network and overall).
        addr_svc = self._addr_setup()
        tag_svc = FakeTagsService(
            tags=[
                FakeTag("b1", "binance", "btc"),
                FakeTag("b2", "ghost", "btc"),  # actor id with no loaded row
            ],
            actors={"binance": SimpleNamespace(id="binance", label="Binance")},
        )

        async def _raising_get_actor(actor_id):
            tag_svc.actor_calls.append(actor_id)
            if actor_id not in tag_svc._actors:
                raise NotFoundException(f"actor {actor_id} not found")
            return tag_svc._actors[actor_id]

        tag_svc.get_actor = _raising_get_actor
        tx_svc = FakeTxsService(supported=["btc"])
        refs = [
            AddressRefInternal(network="btc", address="b1"),
            AddressRefInternal(network="btc", address="b2"),
        ]
        s = await summary(tx_svc, addr_svc, tag_svc, [], refs, tagstore_groups=[])
        assert sorted(tag_svc.actor_calls) == ["binance", "ghost"]
        # resolved actor present, missing one omitted (per-network).
        assert [a.id for a in s.addresses.networks[0].actors] == ["binance"]
        # and in the overall rollup.
        assert [a.id for a in s.addresses.overall.actors] == ["binance"]
        # the tagged-address count still reflects both tagged addresses.
        assert s.addresses.networks[0].tagged_address_count == 2

    async def test_mixed_txs_and_addresses(self):
        tx_map, h0, h1 = _btc_txs()
        svc = FakeTxsService(tx_map=tx_map)
        addr_svc = self._addr_setup()
        tx_refs = [
            TxRefInternal(network="btc", tx_hash=h0),
            TxRefInternal(network="btc", tx_hash=h1),
        ]
        addr_refs = [
            AddressRefInternal(network="btc", address="b1"),
            AddressRefInternal(network="btc", address="b2"),
        ]
        s = await summary(
            svc, addr_svc, FakeTagsService(), tx_refs, addr_refs, tagstore_groups=[]
        )
        assert s.txs.overall.tx_count == 2
        assert s.addresses.overall.address_count == 2

    async def test_duplicate_addresses_deduped(self):
        addr_svc = self._addr_setup()
        tx_svc = FakeTxsService(supported=["btc"])
        refs = [
            AddressRefInternal(network="btc", address="b1"),
            AddressRefInternal(network="btc", address="b1"),
            AddressRefInternal(network="btc", address="b2"),
        ]
        s = await summary(
            tx_svc, addr_svc, FakeTagsService(), [], refs, tagstore_groups=[]
        )
        assert s.addresses.networks[0].address_count == 2
        assert len(addr_svc.calls) == 2

    async def test_case_variant_addresses_do_not_satisfy_minimum(self):
        # eth addresses are hex: a checksummed and a lowercase spelling of
        # the same address are one node, not two distinct ones.
        a = "0x" + "ab" * 20
        addr_svc = FakeAddressesService(
            {a: make_address(a, total_received=make_value(10, usd=1.0))}
        )
        tx_svc = FakeTxsService(supported=["eth"])
        refs = [
            AddressRefInternal(network="eth", address=a),
            AddressRefInternal(network="eth", address="0x" + "AB" * 20),
        ]
        with pytest.raises(BadUserInputException, match="at least 2"):
            await summary(
                tx_svc, addr_svc, FakeTagsService(), [], refs, tagstore_groups=[]
            )

    async def test_unknown_address_below_minimum_raises_not_found(self):
        # 2 refs, 1 unknown: only 1 known survivor, below the 2-node
        # minimum, so the whole request 404s naming the missing address.
        addr_svc = self._addr_setup()
        tx_svc = FakeTxsService(supported=["btc"])
        refs = [
            AddressRefInternal(network="btc", address="b1"),
            AddressRefInternal(network="btc", address="unknown"),
        ]
        with pytest.raises(NotFoundException, match="fewer than 2.*btc:unknown"):
            await summary(
                tx_svc, addr_svc, FakeTagsService(), [], refs, tagstore_groups=[]
            )

    async def test_unknown_address_dropped_with_note(self):
        # 3 refs, 1 unknown: the summary proceeds over the 2 known
        # addresses and reports the dropped ref in a nodes_not_found note.
        addr_svc = self._addr_setup()
        tx_svc = FakeTxsService(supported=["btc"])
        refs = [
            AddressRefInternal(network="btc", address="b1"),
            AddressRefInternal(network="btc", address="b2"),
            AddressRefInternal(network="btc", address="unknown"),
        ]
        result = await summary(
            tx_svc, addr_svc, FakeTagsService(), [], refs, tagstore_groups=[]
        )
        assert result.addresses.overall.address_count == 2
        assert result.addresses.networks[0].address_count == 2
        notes = [
            n for n in result.addresses.overall.notes if n.code == "nodes_not_found"
        ]
        assert len(notes) == 1
        assert notes[0].network == "btc"
        assert notes[0].items == ["unknown"]

    async def test_per_list_minimum_across_types(self):
        tx_map, h0, h1 = _btc_txs()
        svc = FakeTxsService(tx_map=tx_map)
        addr_svc = self._addr_setup()
        # 1 tx + 1 address: both lists below their own minimum.
        with pytest.raises(BadUserInputException, match="at least 2"):
            await summary(
                svc,
                addr_svc,
                FakeTagsService(),
                [TxRefInternal(network="btc", tx_hash=h0)],
                [AddressRefInternal(network="btc", address="b1")],
                tagstore_groups=[],
            )

    async def test_per_list_minimum_short_tx_list_names_tx_refs(self):
        # A single tx ref is below its own minimum even when the address
        # list is fine; the error names the offending list.
        tx_map, h0, h1 = _btc_txs()
        svc = FakeTxsService(tx_map=tx_map)
        addr_svc = self._addr_setup()
        with pytest.raises(BadUserInputException, match="tx refs"):
            await summary(
                svc,
                addr_svc,
                FakeTagsService(),
                [TxRefInternal(network="btc", tx_hash=h0)],
                [
                    AddressRefInternal(network="btc", address="b1"),
                    AddressRefInternal(network="btc", address="b2"),
                ],
                tagstore_groups=[],
            )

    async def test_per_list_minimum_short_address_list_names_addresses(self):
        tx_map, h0, h1 = _btc_txs()
        svc = FakeTxsService(tx_map=tx_map)
        addr_svc = self._addr_setup()
        with pytest.raises(BadUserInputException, match="addresses"):
            await summary(
                svc,
                addr_svc,
                FakeTagsService(),
                [
                    TxRefInternal(network="btc", tx_hash=h0),
                    TxRefInternal(network="btc", tx_hash=h1),
                ],
                [AddressRefInternal(network="btc", address="b1")],
                tagstore_groups=[],
            )

    async def test_combined_cap_spans_both_lists(self):
        svc = FakeTxsService(tx_map={})
        addr_svc = self._addr_setup()
        tx_refs = [TxRefInternal(network="btc", tx_hash=f"{i:064x}") for i in range(60)]
        addr_refs = [
            AddressRefInternal(network="btc", address=f"addr{i}") for i in range(41)
        ]
        with pytest.raises(BadUserInputException, match="at most"):
            await summary(
                svc, addr_svc, FakeTagsService(), tx_refs, addr_refs, tagstore_groups=[]
            )
