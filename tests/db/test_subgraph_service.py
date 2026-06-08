"""Tests for subgraph_service: the pure ``build_summary`` aggregator and the
``summary`` orchestration over a set of transactions."""

from __future__ import annotations

import pytest

from graphsenselib.db.asynchronous.services.subgraph_service import (
    build_summary,
    summary,
)
from graphsenselib.db.asynchronous.services.models import Txs
from graphsenselib.errors import (
    BadUserInputException,
    TransactionNotFoundException,
)
from tests.db.helpers import (
    CURRENCY,
    make_account_tx,
    make_tx,
    make_txvalue,
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
        assert s.currency == CURRENCY
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
        assert s.currency == "eth"
        assert s.total_value == 3_000
        assert s.total_fee == 63
        assert s.total_inputs is None
        assert s.total_outputs is None
        assert s.block_min == 10
        assert s.block_max == 20
        assert s.timestamp_min == 100
        assert s.timestamp_max == 200

    def test_account_fee_none_when_unavailable(self):
        txs = [
            make_account_tx(value=1_000, fee=None),
            make_account_tx(value=2_000, fee=None),
        ]
        s = build_summary("eth", txs)
        assert s.total_value == 3_000
        assert s.total_fee is None

    def test_total_value_usd_summed_across_native_and_token(self):
        # Native ETH transfer + a USDT token transfer. total_value is the
        # native-unit (wei) sum and excludes the token; total_value_usd sums
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
        assert s.total_value_usd == 7.5  # both, in USD
        assert s.tx_count == 2
        assert any("token transfer" in n for n in s.notes)

    def test_total_value_usd_partial_is_summed_and_flagged(self):
        txs = [
            make_account_tx(value=1_000, value_usd=2.5),
            make_account_tx(value=2_000, value_usd=None),  # no rate at height
        ]
        s = build_summary("eth", txs)
        assert s.total_value_usd == 2.5  # only the tx with a rate
        assert any("partial" in n for n in s.notes)

    def test_total_value_usd_none_when_no_rates(self):
        txs = [
            make_account_tx(value=1_000, value_usd=None),
            make_account_tx(value=2_000, value_usd=None),
        ]
        s = build_summary("eth", txs)
        assert s.total_value_usd is None
        assert any("USD" in n for n in s.notes)

    def test_utxo_total_value_usd_from_outputs(self):
        txs = [
            make_tx(total_input=1_000, total_output=900, total_output_usd=10.0),
            make_tx(total_input=500, total_output=480, total_output_usd=5.0),
        ]
        s = build_summary(CURRENCY, txs)
        assert s.total_value == 1_380
        assert s.total_value_usd == 15.0
        assert s.notes == []  # all rates present, no tokens to exclude


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
        s = await summary(svc, CURRENCY, hashes, [], tagstore_groups=[])
        assert s.tx_count == 2
        assert s.currency == CURRENCY
        assert s.total_value == 1_380
        assert s.total_inputs == 3
        assert s.total_outputs == 2
        # UTXO summary fetches headers only: no IO, no heuristics.
        assert svc.get_tx_calls[0]["include_io"] is False
        assert svc.get_tx_calls[0]["include_heuristics"] == []
        assert svc.flows_calls == []

    @pytest.mark.parametrize("currency", ["bch", "ltc", "zec"])
    async def test_other_utxo_chains_allowed(self, currency):
        svc, hashes = self._utxo_svc()
        s = await summary(svc, currency, hashes, [], tagstore_groups=[])
        assert s.tx_count == 2
        assert s.currency == currency

    @pytest.mark.parametrize("currency", ["eth", "trx"])
    async def test_account_folds_token_usd(self, currency):
        # Account summary folds token-transfer USD into total_value_usd, not
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
        s = await summary(svc, currency, [h0, h1], [], tagstore_groups=[])
        assert s.total_value == 10**18  # native only; token leg excluded
        assert s.total_value_usd == 4000.0  # 3000 native + 1000 token
        assert any("token" in n.lower() for n in s.notes)
        # Account path uses asset flows, not get_tx headers.
        assert svc.flows_calls == [h0, h1]
        assert svc.get_tx_calls == []

    async def test_addresses_rejected_for_now(self):
        svc, hashes = self._utxo_svc()
        with pytest.raises(BadUserInputException, match="addresses"):
            await summary(svc, CURRENCY, hashes, ["addr1"], tagstore_groups=[])

    async def test_needs_at_least_two_nodes(self):
        svc, hashes = self._utxo_svc()
        with pytest.raises(BadUserInputException, match="at least 2"):
            await summary(svc, CURRENCY, hashes[:1], [], tagstore_groups=[])

    async def test_empty_rejected(self):
        svc, _ = self._utxo_svc()
        with pytest.raises(BadUserInputException, match="at least 2"):
            await summary(svc, CURRENCY, [], [], tagstore_groups=[])

    async def test_too_many_nodes_rejected(self):
        # Count check happens before any fetch, so an empty tx_map is fine.
        svc = FakeTxsService(tx_map={})
        hashes = [f"{i:064x}" for i in range(101)]
        with pytest.raises(BadUserInputException, match="at most 100"):
            await summary(svc, CURRENCY, hashes, [], tagstore_groups=[])

    async def test_duplicate_hashes_deduped(self):
        svc, hashes = self._utxo_svc()
        s = await summary(
            svc, CURRENCY, [hashes[0], hashes[1], hashes[0]], [], tagstore_groups=[]
        )
        assert s.tx_count == 2
        # Each distinct hash fetched once.
        assert len(svc.get_tx_calls) == 2

    async def test_missing_tx_raises_not_found(self):
        svc, hashes = self._utxo_svc()
        with pytest.raises(TransactionNotFoundException):
            await summary(svc, CURRENCY, [hashes[0], "ff" * 32], [], tagstore_groups=[])
