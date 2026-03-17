from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from graphsenselib.db.asynchronous.services.heuristics import (
    AddressOutput,
    DirectChangeHeuristic,
    MultiInputChangeDetails,
    MultiInputChangeHeuristic,
    OneTimeChangeDetails,
    OneTimeChangeHeuristic,
)
from graphsenselib.db.asynchronous.services.heuristics_service import (
    _build_change_consensus_map,
    _direct_change_heuristic,
    _multi_input_change_heuristic,
    _one_time_change_heuristic,
    calculate_heuristics,
)
from graphsenselib.db.asynchronous.services.txs_service import TxsService


def summary_get(summary: dict, address: str) -> bool:
    """Look up summary value by address string."""
    for k, v in summary.items():
        if k.address == address:
            return v
    raise KeyError(address)


def addr_in(lst: list, address: str) -> bool:
    """Check if address string appears in a list of AddressOutput."""
    return any(item.address == address for item in lst)


CURRENCY = "btc"
TX_HASH = b"\xab\x12"
BLOCK_ID = 100


def make_input(value, address_type="p2pkh", address=None):
    inp = MagicMock()
    inp.value = value
    inp.address_type = address_type
    inp.address = [address] if address is not None else []
    return inp


def make_output(address, value, address_type="p2pkh"):
    out = MagicMock()
    out.address = [address] if address is not None else []
    out.value = value
    out.address_type = address_type
    out.script_hex = None
    return out


def make_address_record(no_incoming=1, no_outgoing=0, first_tx_height=BLOCK_ID):
    first_tx = MagicMock()
    first_tx.height = first_tx_height
    return {
        "no_incoming_txs": no_incoming,
        "no_outgoing_txs": no_outgoing,
        "first_tx": first_tx,
    }


def make_get_address(address_data: dict):
    """Returns an async callable that looks up address records by address string.

    address_data: {address: {"no_incoming_txs": int, "no_outgoing_txs": int, "first_tx": obj}}
    Use make_address_record() to build entries.
    """

    async def get_address(currency, address):
        return address_data.get(address)

    return get_address


def make_get_address_with_cluster(cluster_map: dict):
    """Returns an async callable that returns dicts with cluster_id key.

    cluster_map: {address: cluster_id} — missing key means no cluster (returns -1 default).
    """

    async def get_address(currency, address):
        cluster_id = cluster_map.get(address, -1)
        return {"cluster_id": cluster_id}

    return get_address


def make_tx(inputs, outputs, coinbase=False, block_id=BLOCK_ID):
    return {
        "coinbase": coinbase,
        "tx_hash": TX_HASH,
        "block_id": block_id,
        "inputs": inputs,
        "outputs": outputs,
    }


# patch cannonicalize_address to identity so tests don't need valid BTC addresses
patcher = patch(
    "graphsenselib.db.asynchronous.services.heuristics_service.cannonicalize_address",
    side_effect=lambda currency, addr: addr,
)


@pytest.fixture(autouse=True)
def patch_canonicalize():
    with patcher:
        yield


class TestOneTimeChangeHeuristic:
    async def test_coinbase_returns_all_false(self):
        """Coinbase transactions have no real inputs, heuristic does not apply."""
        tx = make_tx(inputs=[], outputs=[make_output("addr_A", 5000000)], coinbase=True)
        result = await _one_time_change_heuristic(tx, CURRENCY, make_get_address({}))
        assert result.summary == []
        assert all(
            v == []
            for v in [
                result.details.same_script_type,
                result.details.not_nicely_divisible,
                result.details.output_less_than_input,
                result.details.not_reused,
            ]
        )

    async def test_too_few_outputs_returns_all_false(self):
        tx = make_tx(
            inputs=[make_input(50000)],
            outputs=[make_output("addr_A", 49000)],
        )
        result = await _one_time_change_heuristic(tx, CURRENCY, make_get_address({}))
        assert result.summary == []
        assert all(
            v == []
            for v in [
                result.details.same_script_type,
                result.details.not_nicely_divisible,
                result.details.output_less_than_input,
                result.details.not_reused,
            ]
        )

    async def test_too_many_outputs_returns_all_false(self):
        tx = make_tx(
            inputs=[make_input(50000)],
            outputs=[make_output(f"addr_{i}", 1000) for i in range(11)],
        )
        result = await _one_time_change_heuristic(tx, CURRENCY, make_get_address({}))
        assert result.summary == []
        assert all(
            v == []
            for v in [
                result.details.same_script_type,
                result.details.not_nicely_divisible,
                result.details.output_less_than_input,
                result.details.not_reused,
            ]
        )

    async def test_exactly_two_outputs_runs_heuristic(self):
        tx = make_tx(
            inputs=[make_input(50000)],
            outputs=[make_output("addr_A", 49000), make_output("addr_B", 999)],
        )
        result = await _one_time_change_heuristic(tx, CURRENCY, make_get_address({}))
        assert result.summary is not None

    async def test_exactly_ten_outputs_runs_heuristic(self):
        tx = make_tx(
            inputs=[make_input(50000)],
            outputs=[make_output(f"addr_{i}", 1000) for i in range(10)],
        )
        result = await _one_time_change_heuristic(tx, CURRENCY, make_get_address({}))
        assert result.summary is not None

    async def test_clear_change_address(self):
        """addr_change meets all 3 conditions and has not been reused — True.
        addr_payment fails all 3 conditions — never a candidate — False."""
        tx = make_tx(
            inputs=[make_input(50000, address_type="p2pkh")],
            outputs=[
                make_output("addr_payment", 49000, address_type="p2sh"),
                make_output("addr_change", 999, address_type="p2pkh"),
            ],
        )
        address_data = {
            "addr_change": make_address_record(
                no_incoming=1, no_outgoing=0, first_tx_height=BLOCK_ID
            )
        }
        result = await _one_time_change_heuristic(
            tx, CURRENCY, make_get_address(address_data)
        )
        assert addr_in(result.summary, "addr_change")
        assert not addr_in(result.summary, "addr_payment")

    async def test_meets_only_some_conditions_not_candidate(self):
        """addr_partial meets same_script and out_less_than_in but value is divisible by 1000
        → not in intersection → False, get_address is never called."""
        tx = make_tx(
            inputs=[make_input(50000, address_type="p2pkh")],
            outputs=[
                make_output("addr_partial", 49000, address_type="p2pkh"),
                make_output("addr_other", 49001, address_type="p2sh"),
            ],
        )
        result = await _one_time_change_heuristic(tx, CURRENCY, make_get_address({}))
        assert not addr_in(result.summary, "addr_partial")

    async def test_past_use_disqualified(self):
        """Address that meets all 3 conditions but first_tx is before the current block."""
        tx = make_tx(
            inputs=[make_input(50000, address_type="p2pkh")],
            outputs=[
                make_output("addr_payment", 49000, address_type="p2pkh"),
                make_output("addr_change", 999, address_type="p2pkh"),
            ],
        )
        address_data = {
            "addr_change": make_address_record(first_tx_height=BLOCK_ID - 1)
        }
        result = await _one_time_change_heuristic(
            tx, CURRENCY, make_get_address(address_data)
        )
        assert not addr_in(result.details.not_reused, "addr_change")
        assert not addr_in(result.summary, "addr_change")

    async def test_one_future_outgoing_still_valid(self):
        """One outgoing tx is allowed — the change being spent once after receiving."""
        tx = make_tx(
            inputs=[make_input(50000, address_type="p2pkh")],
            outputs=[
                make_output("addr_payment", 49000, address_type="p2sh"),
                make_output("addr_change", 999, address_type="p2pkh"),
            ],
        )
        address_data = {
            "addr_change": make_address_record(
                no_incoming=1, no_outgoing=1, first_tx_height=BLOCK_ID
            )
        }
        result = await _one_time_change_heuristic(
            tx, CURRENCY, make_get_address(address_data)
        )
        assert addr_in(result.summary, "addr_change")

    async def test_two_outgoing_disqualified(self):
        """Two outgoing txs disqualify — change was spent more than once."""
        tx = make_tx(
            inputs=[make_input(50000, address_type="p2pkh")],
            outputs=[
                make_output("addr_payment", 49000, address_type="p2sh"),
                make_output("addr_change", 999, address_type="p2pkh"),
            ],
        )
        address_data = {
            "addr_change": make_address_record(
                no_incoming=1, no_outgoing=2, first_tx_height=BLOCK_ID
            )
        }
        result = await _one_time_change_heuristic(
            tx, CURRENCY, make_get_address(address_data)
        )
        assert not addr_in(result.details.not_reused, "addr_change")
        assert not addr_in(result.summary, "addr_change")

    async def test_two_incoming_disqualified(self):
        """Two incoming txs disqualify — address received funds more than once."""
        tx = make_tx(
            inputs=[make_input(50000, address_type="p2pkh")],
            outputs=[
                make_output("addr_payment", 49000, address_type="p2sh"),
                make_output("addr_change", 999, address_type="p2pkh"),
            ],
        )
        address_data = {
            "addr_change": make_address_record(
                no_incoming=2, no_outgoing=0, first_tx_height=BLOCK_ID
            )
        }
        result = await _one_time_change_heuristic(
            tx, CURRENCY, make_get_address(address_data)
        )
        assert not addr_in(result.details.not_reused, "addr_change")
        assert not addr_in(result.summary, "addr_change")

    async def test_mixed_input_script_types_no_candidates(self):
        """Mixed input script types → same_script condition is empty → summary all False."""
        tx = make_tx(
            inputs=[
                make_input(30000, address_type="p2pkh"),
                make_input(20000, address_type="p2wpkh"),
            ],
            outputs=[
                make_output("addr_A", 49000, address_type="p2pkh"),
                make_output("addr_B", 999, address_type="p2wpkh"),
            ],
        )
        result = await _one_time_change_heuristic(tx, CURRENCY, make_get_address({}))
        assert result.details.same_script_type == []
        assert result.summary == []

    async def test_duplicate_output_address_excluded(self):
        """An address appearing multiple times in outputs is never flagged as change."""
        tx = make_tx(
            inputs=[make_input(50000, address_type="p2pkh")],
            outputs=[
                make_output("addr_dup", 999, address_type="p2pkh"),
                make_output("addr_dup", 999, address_type="p2pkh"),
                make_output("addr_payment", 48000, address_type="p2pkh"),
            ],
        )
        result = await _one_time_change_heuristic(tx, CURRENCY, make_get_address({}))
        assert not addr_in(result.summary, "addr_dup")

    async def test_nonstandard_output_no_address_skipped(self):
        """OP_RETURN outputs (empty address list) must not crash and are excluded from summary."""
        tx = make_tx(
            inputs=[make_input(50000)],
            outputs=[
                make_output(None, 0),
                make_output("addr_A", 49000),
                make_output("addr_B", 999),
            ],
        )
        result = await _one_time_change_heuristic(tx, CURRENCY, make_get_address({}))
        assert result.summary is not None
        assert all(k.address is not None for k in result.summary)

    async def test_all_outputs_divisible_no_candidates(self):
        """When all output values are multiples of 1000, not_nicely_divisible is empty."""
        tx = make_tx(
            inputs=[make_input(50000)],
            outputs=[
                make_output("addr_A", 49000),
                make_output("addr_B", 1000),
            ],
        )
        result = await _one_time_change_heuristic(tx, CURRENCY, make_get_address({}))
        assert result.details.not_nicely_divisible == []
        assert result.summary == []

    async def test_two_candidates_both_false(self):
        """Two outputs both meeting all 3 conditions → uniqueness check fires → both False."""
        tx = make_tx(
            inputs=[make_input(50000, address_type="p2pkh")],
            outputs=[
                make_output("addr_A", 999, address_type="p2pkh"),
                make_output("addr_B", 997, address_type="p2pkh"),
            ],
        )
        address_data = {
            "addr_A": make_address_record(),
            "addr_B": make_address_record(),
        }
        result = await _one_time_change_heuristic(
            tx, CURRENCY, make_get_address(address_data)
        )
        assert result.summary == []


class TestDirectChangeHeuristic:
    async def test_coinbase_returns_empty(self):
        tx = make_tx(inputs=[], outputs=[make_output("addr_A", 5000)], coinbase=True)
        result = await _direct_change_heuristic(tx)
        assert result.summary == []

    async def test_no_overlap_returns_empty(self):
        tx = make_tx(
            inputs=[make_input(50000, address="addr_in")],
            outputs=[make_output("addr_out", 49000)],
        )
        result = await _direct_change_heuristic(tx)
        assert result.summary == []

    async def test_address_in_both_input_and_output_flagged(self):
        """addr_A appears in both inputs and outputs → marked as change."""
        tx = make_tx(
            inputs=[make_input(50000, address="addr_A")],
            outputs=[
                make_output("addr_A", 1000),
                make_output("addr_B", 49000),
            ],
        )
        result = await _direct_change_heuristic(tx)
        assert addr_in(result.summary, "addr_A")
        assert not addr_in(result.summary, "addr_B")

    async def test_multiple_overlapping_addresses_all_flagged(self):
        tx = make_tx(
            inputs=[
                make_input(30000, address="addr_A"),
                make_input(20000, address="addr_B"),
            ],
            outputs=[
                make_output("addr_A", 1000),
                make_output("addr_B", 1000),
                make_output("addr_C", 48000),
            ],
        )
        result = await _direct_change_heuristic(tx)
        assert addr_in(result.summary, "addr_A")
        assert addr_in(result.summary, "addr_B")
        assert not addr_in(result.summary, "addr_C")

    async def test_correct_output_index_recorded(self):
        """addr_A is at output index 1 — the AddressOutput must reflect that."""
        tx = make_tx(
            inputs=[make_input(50000, address="addr_A")],
            outputs=[
                make_output("addr_B", 49000),
                make_output("addr_A", 1000),
            ],
        )
        result = await _direct_change_heuristic(tx)
        matching = [k for k in result.summary if k.address == "addr_A"]
        assert len(matching) == 1
        assert matching[0].index == 1


class TestMultiInputChangeHeuristic:
    async def test_coinbase_returns_empty(self):
        tx = make_tx(inputs=[], outputs=[make_output("addr_A", 5000)], coinbase=True)
        result = await _multi_input_change_heuristic(
            tx, CURRENCY, make_get_address_with_cluster({})
        )
        assert result.summary == []

    async def test_output_without_cluster_not_change(self):
        """Output address has no cluster → not in summary."""
        tx = make_tx(
            inputs=[make_input(50000, address="addr_in")],
            outputs=[make_output("addr_out", 49000)],
        )
        result = await _multi_input_change_heuristic(
            tx,
            CURRENCY,
            make_get_address_with_cluster({"addr_in": 42}),
        )
        assert not addr_in(result.summary, "addr_out")

    async def test_output_matching_input_cluster_flagged(self):
        """Output and an input share cluster_id=42 → output is change."""
        tx = make_tx(
            inputs=[make_input(50000, address="addr_in")],
            outputs=[make_output("addr_out", 49000)],
        )
        result = await _multi_input_change_heuristic(
            tx,
            CURRENCY,
            make_get_address_with_cluster({"addr_in": 42, "addr_out": 42}),
        )
        assert addr_in(result.summary, "addr_out")

    async def test_output_different_cluster_not_flagged(self):
        """Output cluster does not match any input cluster → not in summary."""
        tx = make_tx(
            inputs=[make_input(50000, address="addr_in")],
            outputs=[make_output("addr_out", 49000)],
        )
        result = await _multi_input_change_heuristic(
            tx,
            CURRENCY,
            make_get_address_with_cluster({"addr_in": 42, "addr_out": 99}),
        )
        assert not addr_in(result.summary, "addr_out")

    async def test_evidence_recorded_in_details(self):
        """Matching cluster → details contains evidence with correct input address."""
        tx = make_tx(
            inputs=[make_input(50000, address="addr_in")],
            outputs=[make_output("addr_out", 49000)],
        )
        result = await _multi_input_change_heuristic(
            tx,
            CURRENCY,
            make_get_address_with_cluster({"addr_in": 42, "addr_out": 42}),
        )
        assert 42 in result.details.cluster
        evidence_list = result.details.cluster[42]
        assert any(e.matching_input_address == "addr_in" for e in evidence_list)


class TestBuildConsensusMap:
    def test_empty_map_returns_empty(self):
        assert _build_change_consensus_map({}) == {}

    def test_same_address_merges_sources_and_max_confidence(self):
        addr_low_conf = AddressOutput(address="addr_A", index=1)
        addr_high_conf = AddressOutput(address="addr_A", index=3)
        one_time_details = OneTimeChangeDetails(
            same_script_type=[],
            not_nicely_divisible=[],
            output_less_than_input=[],
            not_reused=[],
        )
        heuristic_map = {
            "one_time_change": OneTimeChangeHeuristic(
                summary=[addr_low_conf],
                details=one_time_details,
                confidence=50,
            ),
            "direct_change": DirectChangeHeuristic(
                summary=[addr_high_conf],
                confidence=100,
            ),
        }

        consensus_map = _build_change_consensus_map(heuristic_map)

        assert set(consensus_map.keys()) == {"addr_A"}
        entry = consensus_map["addr_A"]
        assert entry.output.index == 3
        assert entry.confidence == 100
        assert entry.sources == ["direct_change", "one_time_change"]

    def test_distinct_addresses_create_independent_entries(self):
        heuristic_map = {
            "direct_change": DirectChangeHeuristic(
                summary=[AddressOutput(address="addr_A", index=0)],
                confidence=100,
            ),
            "multi_input_change": MultiInputChangeHeuristic(
                summary=[AddressOutput(address="addr_B", index=2)],
                details=MultiInputChangeDetails(cluster={}),
                confidence=50,
            ),
        }

        consensus_map = _build_change_consensus_map(heuristic_map)

        assert set(consensus_map.keys()) == {"addr_A", "addr_B"}
        assert consensus_map["addr_A"].sources == ["direct_change"]
        assert consensus_map["addr_B"].sources == ["multi_input_change"]


class TestCalculateHeuristics:
    async def test_direct_change_in_consensus(self):
        """Address in both inputs and outputs is flagged as change in consensus."""
        tx = make_tx(
            inputs=[make_input(50000, address="addr_A")],
            outputs=[
                make_output("addr_A", 1000),
                make_output("addr_B", 49000),
            ],
        )
        result = await calculate_heuristics(
            tx, CURRENCY, make_get_address({}), ["direct_change"]
        )
        consensus = result.change_heuristics.consensus
        assert any(e.output.address == "addr_A" for e in consensus)

    async def test_consensus_sources_tracked(self):
        tx = make_tx(
            inputs=[make_input(50000, address="addr_A")],
            outputs=[
                make_output("addr_A", 1000),
                make_output("addr_B", 49000),
            ],
        )
        result = await calculate_heuristics(
            tx, CURRENCY, make_get_address({}), ["direct_change"]
        )
        entry = next(
            e
            for e in result.change_heuristics.consensus
            if e.output.address == "addr_A"
        )
        assert "direct_change" in entry.sources

    async def test_consensus_any_wins(self):
        """direct_change flags addr_A; one_time_change does not — still in consensus."""
        tx = make_tx(
            inputs=[make_input(50000, address="addr_A")],
            outputs=[
                make_output("addr_A", 49000),
                make_output("addr_B", 1000),
            ],
        )
        result = await calculate_heuristics(
            tx, CURRENCY, make_get_address({}), ["direct_change", "one_time_change"]
        )
        assert any(
            e.output.address == "addr_A" for e in result.change_heuristics.consensus
        )

    async def test_consensus_not_change_when_none_flag(self):
        """Address not flagged by any heuristic is absent from consensus."""
        tx = make_tx(
            inputs=[make_input(50000, address="addr_in")],
            outputs=[make_output("addr_out", 49000)],
        )
        result = await calculate_heuristics(
            tx, CURRENCY, make_get_address({}), ["direct_change"]
        )
        assert not any(
            e.output.address == "addr_out" for e in result.change_heuristics.consensus
        )

    async def test_consensus_confidence_is_max_of_sources(self):
        """When multiple heuristics flag the same address, confidence = max of their confidences."""
        tx = make_tx(
            inputs=[make_input(50000, address="addr_A")],
            outputs=[
                make_output("addr_A", 1000),
                make_output("addr_B", 49000),
            ],
        )
        result = await calculate_heuristics(
            tx, CURRENCY, make_get_address({}), ["direct_change"]
        )
        entry = next(
            e
            for e in result.change_heuristics.consensus
            if e.output.address == "addr_A"
        )
        # direct_change has confidence=100
        assert entry.confidence == 100

    async def test_all_heuristics_populated_when_requested(self):
        tx = make_tx(
            inputs=[make_input(50000, address="addr_in")],
            outputs=[make_output("addr_out", 49000)],
        )
        result = await calculate_heuristics(
            tx, CURRENCY, make_get_address({}), ["direct_change", "one_time_change"]
        )
        ch = result.change_heuristics
        assert ch.direct_change is not None
        assert ch.one_time_change is not None
        assert ch.multi_input_change is None


def make_raw_utxo_tx(block_id=BLOCK_ID):
    inp = MagicMock()
    inp.value = 50000
    inp.address_type = "p2pkh"
    inp.address = []
    out_change = MagicMock()
    out_change.address = ["addr_change"]
    out_change.value = 999
    out_change.address_type = "p2pkh"
    out_payment = MagicMock()
    out_payment.address = ["addr_payment"]
    out_payment.value = 49000
    out_payment.address_type = "p2sh"
    return {
        "coinbase": False,
        "tx_hash": TX_HASH,
        "block_id": block_id,
        "timestamp": 123456789,
        "total_input": 50000,
        "total_output": 49999,
        "inputs": [inp],
        "outputs": [out_change, out_payment],
        "type": "external",
    }


def make_rates_service(block_id=BLOCK_ID):
    rates = MagicMock()
    rates.rates = [{"code": "eur", "value": 0.0}, {"code": "usd", "value": 0.0}]
    rs = MagicMock()
    rs.get_rates = AsyncMock(return_value=rates)
    return rs


class TestTxsServiceHeuristicsRouting:
    """Tests that heuristics are computed for UTXO currencies and skipped for ETH-like ones."""

    def make_service(self, currency, raw_tx):
        db = MagicMock()
        db.get_tx = AsyncMock(return_value=raw_tx)
        db.get_token_configuration = MagicMock(return_value=None)
        db.get_address = AsyncMock(return_value=None)
        if currency == "eth":
            trace = dict(raw_tx)
            trace["tx_hash"] = TX_HASH
            trace["block_timestamp"] = 123456789
            trace["from_address"] = b"\x00" * 20
            trace["to_address"] = b"\x00" * 20
            trace["value"] = 0
            trace["trace_type"] = "call"
            trace["trace_address"] = ""
            trace["trace_index"] = None
            db.fetch_transaction_trace = AsyncMock(return_value=trace)
        raw_tx["block_timestamp"] = 123456789
        return TxsService(db=db, rates_service=make_rates_service(), logger=MagicMock())

    @patch(
        "graphsenselib.db.asynchronous.services.heuristics_service.cannonicalize_address",
        side_effect=lambda currency, addr: addr,
    )
    async def test_utxo_heuristics_present_when_requested(self, _patch):
        raw_tx = make_raw_utxo_tx()
        svc = self.make_service("btc", raw_tx)
        result = await svc.get_tx(
            "btc",
            TX_HASH.hex(),
            include_io=True,
            include_heuristics=["one_time_change"],
        )
        assert result.heuristics is not None
        assert result.heuristics.change_heuristics.one_time_change is not None

    @patch(
        "graphsenselib.db.asynchronous.services.heuristics_service.cannonicalize_address",
        side_effect=lambda currency, addr: addr,
    )
    async def test_utxo_heuristics_absent_when_not_requested(self, _patch):
        raw_tx = make_raw_utxo_tx()
        svc = self.make_service("btc", raw_tx)
        result = await svc.get_tx(
            "btc", TX_HASH.hex(), include_io=True, include_heuristics=[]
        )
        assert result.heuristics is None

    async def test_eth_heuristics_never_computed(self):
        """ETH returns early via the trace path — _calculate_heuristics must never be called."""
        raw_tx = make_raw_utxo_tx()
        svc = self.make_service("eth", raw_tx)
        with patch(
            "graphsenselib.db.asynchronous.services.txs_service.calculate_heuristics",
            new=AsyncMock(),
        ) as mock_heuristics:
            result = await svc.get_tx(
                "eth", TX_HASH.hex(), include_heuristics=["one_time_change"]
            )
            mock_heuristics.assert_not_called()
        assert not hasattr(result, "heuristics")
