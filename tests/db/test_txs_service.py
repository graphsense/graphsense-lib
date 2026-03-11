from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from graphsenselib.db.asynchronous.services.txs_service import (
    TxsService,
    _one_time_change_heuristic,
)

CURRENCY = "btc"
TX_HASH = b"\xab\x12"
BLOCK_ID = 100


def make_input(value, address_type="p2pkh"):
    inp = MagicMock()
    inp.value = value
    inp.address_type = address_type
    return inp


def make_output(address, value, address_type="p2pkh"):
    out = MagicMock()
    out.address = [address] if address is not None else []
    out.value = value
    out.address_type = address_type
    return out


def make_list_address_txs(tx_history: dict):
    """Returns an async callable that looks up address tx history from a dict.

    tx_history: {address: [{"tx_hash": ..., "is_outgoing": ..., "block_id": ...}, ...]}
    """

    async def list_address_txs(currency, address, direction=None, **kwargs):
        return (tx_history.get(address, []), None)

    return list_address_txs


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
    "graphsenselib.db.asynchronous.services.txs_service.cannonicalize_address",
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
        result = await _one_time_change_heuristic(tx, CURRENCY, make_list_address_txs({}))
        assert result["summary"] == {"addr_A": False}
        assert all(v == [] for v in result["details"].values())

    async def test_too_few_outputs_returns_all_false(self):
        tx = make_tx(
            inputs=[make_input(50000)],
            outputs=[make_output("addr_A", 49000)],
        )
        result = await _one_time_change_heuristic(tx, CURRENCY, make_list_address_txs({}))
        assert result["summary"] == {"addr_A": False}
        assert all(v == [] for v in result["details"].values())

    async def test_too_many_outputs_returns_all_false(self):
        tx = make_tx(
            inputs=[make_input(50000)],
            outputs=[make_output(f"addr_{i}", 1000) for i in range(11)],
        )
        result = await _one_time_change_heuristic(tx, CURRENCY, make_list_address_txs({}))
        assert all(v is False for v in result["summary"].values())
        assert all(v == [] for v in result["details"].values())

    async def test_exactly_two_outputs_runs_heuristic(self):
        tx = make_tx(
            inputs=[make_input(50000)],
            outputs=[make_output("addr_A", 49000), make_output("addr_B", 999)],
        )
        result = await _one_time_change_heuristic(tx, CURRENCY, make_list_address_txs({}))
        assert "summary" in result

    async def test_exactly_ten_outputs_runs_heuristic(self):
        tx = make_tx(
            inputs=[make_input(50000)],
            outputs=[make_output(f"addr_{i}", 1000) for i in range(10)],
        )
        result = await _one_time_change_heuristic(tx, CURRENCY, make_list_address_txs({}))
        assert "summary" in result

    async def test_clear_change_address(self):
        """addr_change meets all 3 conditions (intersection) and is not reused — True.
        addr_payment fails all 3 conditions — never a candidate — False."""
        tx = make_tx(
            inputs=[make_input(50000, address_type="p2pkh")],
            outputs=[
                # different script type, divisible by 1000, value >= min input
                # → fails all 3 conditions, not in intersection
                make_output("addr_payment", 49000, address_type="p2sh"),
                # same script type, not divisible by 1000, value < min input
                # → meets all 3, no prior txs → True
                make_output("addr_change", 999, address_type="p2pkh"),
            ],
        )
        result = await _one_time_change_heuristic(tx, CURRENCY, make_list_address_txs({}))
        assert result["summary"]["addr_change"] is True
        assert result["summary"]["addr_payment"] is False

    async def test_meets_only_some_conditions_not_candidate(self):
        """With intersection, an address must meet ALL 3 conditions to be a candidate.
        addr_partial meets same_script and out_less_than_in but value is divisible by 1000
        → not in intersection → False."""
        tx = make_tx(
            inputs=[make_input(50000, address_type="p2pkh")],
            outputs=[
                # same script, divisible by 1000 (fails not_nicely_divisible), < min input
                # → only 2 of 3 conditions → not a candidate with intersection
                make_output("addr_partial", 49000, address_type="p2pkh"),
                make_output("addr_other", 49001, address_type="p2sh"),
            ],
        )
        result = await _one_time_change_heuristic(tx, CURRENCY, make_list_address_txs({}))
        assert result["summary"]["addr_partial"] is False

    async def test_past_incoming_disqualified(self):
        """Address that meets all 3 conditions but received funds in a past block is excluded."""
        tx = make_tx(
            inputs=[make_input(50000, address_type="p2pkh")],
            outputs=[
                make_output("addr_payment", 49000, address_type="p2pkh"),
                make_output("addr_change", 999, address_type="p2pkh"),
            ],
        )
        tx_history = {
            "addr_change": [{"tx_hash": b"\xcd\xef", "is_outgoing": False, "height": BLOCK_ID - 1}]
        }
        result = await _one_time_change_heuristic(tx, CURRENCY, make_list_address_txs(tx_history))
        assert "addr_change" not in result["details"]["not_reused"]
        assert result["summary"]["addr_change"] is False

    async def test_past_outgoing_disqualified(self):
        """Address that meets all 3 conditions but spent funds in a past block is excluded."""
        tx = make_tx(
            inputs=[make_input(50000, address_type="p2pkh")],
            outputs=[
                make_output("addr_payment", 49000, address_type="p2pkh"),
                make_output("addr_change", 999, address_type="p2pkh"),
            ],
        )
        tx_history = {
            "addr_change": [{"tx_hash": b"\xcd\xef", "is_outgoing": True, "height": BLOCK_ID - 1}]
        }
        result = await _one_time_change_heuristic(tx, CURRENCY, make_list_address_txs(tx_history))
        assert "addr_change" not in result["details"]["not_reused"]
        assert result["summary"]["addr_change"] is False

    async def test_one_future_outgoing_still_valid(self):
        """One outgoing tx at or after the current block is allowed — the change being spent later."""
        tx = make_tx(
            inputs=[make_input(50000, address_type="p2pkh")],
            outputs=[
                make_output("addr_payment", 49000, address_type="p2sh"),
                make_output("addr_change", 999, address_type="p2pkh"),
            ],
        )
        tx_history = {
            "addr_change": [{"tx_hash": b"\xcd\xef", "is_outgoing": True, "height": BLOCK_ID + 1}]
        }
        result = await _one_time_change_heuristic(tx, CURRENCY, make_list_address_txs(tx_history))
        assert result["summary"]["addr_change"] is True

    async def test_two_future_outgoing_disqualified(self):
        """Two outgoing txs at or after current block disqualify — change was re-spent more than once."""
        tx = make_tx(
            inputs=[make_input(50000, address_type="p2pkh")],
            outputs=[
                make_output("addr_payment", 49000, address_type="p2sh"),
                make_output("addr_change", 999, address_type="p2pkh"),
            ],
        )
        tx_history = {
            "addr_change": [
                {"tx_hash": b"\xcd\xef", "is_outgoing": True, "height": BLOCK_ID + 1},
                {"tx_hash": b"\xde\xf0", "is_outgoing": True, "height": BLOCK_ID + 2},
            ]
        }
        result = await _one_time_change_heuristic(tx, CURRENCY, make_list_address_txs(tx_history))
        assert "addr_change" not in result["details"]["not_reused"]
        assert result["summary"]["addr_change"] is False

    async def test_two_future_incoming_disqualified(self):
        """Two incoming txs at or after current block disqualify — address received funds twice."""
        tx = make_tx(
            inputs=[make_input(50000, address_type="p2pkh")],
            outputs=[
                make_output("addr_payment", 49000, address_type="p2sh"),
                make_output("addr_change", 999, address_type="p2pkh"),
            ],
        )
        tx_history = {
            "addr_change": [
                {"tx_hash": b"\xcd\xef", "is_outgoing": False, "height": BLOCK_ID},
                {"tx_hash": b"\xde\xf0", "is_outgoing": False, "height": BLOCK_ID + 1},
            ]
        }
        result = await _one_time_change_heuristic(tx, CURRENCY, make_list_address_txs(tx_history))
        assert "addr_change" not in result["details"]["not_reused"]
        assert result["summary"]["addr_change"] is False

    async def test_mixed_input_script_types_no_candidates(self):
        """Mixed input script types → same_script condition is empty.
        With intersection, no address can meet all 3 → summary is all False."""
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
        result = await _one_time_change_heuristic(tx, CURRENCY, make_list_address_txs({}))
        assert result["details"]["same_script_type"] == []
        assert all(v is False for v in result["summary"].values())

    async def test_duplicate_output_address_excluded(self):
        """An address appearing multiple times in outputs is never flagged as change,
        even if it meets all 3 conditions."""
        tx = make_tx(
            inputs=[make_input(50000, address_type="p2pkh")],
            outputs=[
                # meets all 3 conditions but appears twice → excluded
                make_output("addr_dup", 999, address_type="p2pkh"),
                make_output("addr_dup", 999, address_type="p2pkh"),
                # fails not_nicely_divisible → not a candidate anyway
                make_output("addr_payment", 48000, address_type="p2pkh"),
            ],
        )
        result = await _one_time_change_heuristic(tx, CURRENCY, make_list_address_txs({}))
        assert result["summary"].get("addr_dup") is False

    async def test_nonstandard_output_no_address_skipped(self):
        """OP_RETURN outputs (empty address list) must not crash and are excluded from summary."""
        tx = make_tx(
            inputs=[make_input(50000)],
            outputs=[
                make_output(None, 0),           # OP_RETURN — no address
                make_output("addr_A", 49000),
                make_output("addr_B", 999),
            ],
        )
        result = await _one_time_change_heuristic(tx, CURRENCY, make_list_address_txs({}))
        assert "summary" in result
        assert None not in result["summary"]

    async def test_all_outputs_divisible_no_candidates(self):
        """When all output values are multiples of 1000, not_nicely_divisible is empty.
        With intersection, no address meets all 3 conditions → summary is all False."""
        tx = make_tx(
            inputs=[make_input(50000)],
            outputs=[
                make_output("addr_A", 49000),
                make_output("addr_B", 1000),
            ],
        )
        result = await _one_time_change_heuristic(tx, CURRENCY, make_list_address_txs({}))
        assert result["details"]["not_nicely_divisible"] == []
        assert all(v is False for v in result["summary"].values())

    async def test_two_candidates_both_false(self):
        """Two outputs both meeting all 3 conditions → uniqueness check fires → both False."""
        tx = make_tx(
            inputs=[make_input(50000, address_type="p2pkh")],
            outputs=[
                make_output("addr_A", 999, address_type="p2pkh"),  # meets all 3
                make_output("addr_B", 997, address_type="p2pkh"),  # meets all 3
            ],
        )
        result = await _one_time_change_heuristic(tx, CURRENCY, make_list_address_txs({}))
        assert result["summary"]["addr_A"] is False
        assert result["summary"]["addr_B"] is False


def make_raw_utxo_tx(block_id=BLOCK_ID):
    inp = MagicMock()
    inp.value = 50000
    inp.address_type = "p2pkh"
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
        db.list_address_txs = AsyncMock(return_value=([], None))
        if currency == "eth":
            # ETH path calls fetch_transaction_trace
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
        "graphsenselib.db.asynchronous.services.txs_service.cannonicalize_address",
        side_effect=lambda currency, addr: addr,
    )
    async def test_utxo_heuristics_present_when_requested(self, _patch):
        raw_tx = make_raw_utxo_tx()
        svc = self.make_service("btc", raw_tx)
        result = await svc.get_tx("btc", TX_HASH.hex(), include_io=True, include_heuristics=True)
        assert result.heuristics is not None
        assert "change" in result.heuristics

    @patch(
        "graphsenselib.db.asynchronous.services.txs_service.cannonicalize_address",
        side_effect=lambda currency, addr: addr,
    )
    async def test_utxo_heuristics_absent_when_not_requested(self, _patch):
        raw_tx = make_raw_utxo_tx()
        svc = self.make_service("btc", raw_tx)
        result = await svc.get_tx("btc", TX_HASH.hex(), include_io=True, include_heuristics=False)
        assert result.heuristics is None

    async def test_eth_heuristics_never_computed(self):
        """ETH returns early via the trace path — _calculate_heuristics must never be called."""
        raw_tx = make_raw_utxo_tx()
        svc = self.make_service("eth", raw_tx)
        with patch.object(svc, "_calculate_heuristics", new=AsyncMock()) as mock_heuristics:
            result = await svc.get_tx("eth", TX_HASH.hex(), include_heuristics=True)
            mock_heuristics.assert_not_called()
        # ETH returns a TxAccount which has no heuristics field
        assert not hasattr(result, "heuristics")
