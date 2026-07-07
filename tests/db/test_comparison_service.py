"""Tests for the pure functions in comparison_service.py plus an
orchestration test for ``compare_txs``.

NOTE: The signal ``weight`` ints, the verdict ``confidence`` int, and the
``score_total`` float are tentative and not yet calibrated, so these tests
deliberately avoid asserting on specific numeric values for them. We only
check categorical labels, list membership, and structural shape.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from graphsenselib.db.asynchronous.services.comparison_service import (
    _MAX_TOTAL_IOS,
    _aggregate_inputs_have_witness,
    _bip69_outputs_sorted,
    _canonical_input_addresses,
    _classify_locktime,
    _connected_components,
    _consensus_change_addresses,
    _fetch_input_address_exchange_flags,
    _has_witness_for_type,
    _input_cluster_ids_for_tx,
    _inputs_have_exchange_for_tx,
    _lineage_edges_from_refs,
    _parent_hashes_from_refs,
    _unique_script_types,
    _utxo_parent_indexes_from_hashes,
    aggregate_verdict,
    compare_txs,
    compute_cluster_verdict,
    extract_characteristics,
    script_type_from_address,
    signal_bip69_outputs_sorted,
    signal_change_chain,
    signal_common_ancestor,
    signal_direct_input_overlap,
    signal_exchange_input_overlap,
    signal_locktime_pattern,
    signal_output_count_shape,
    signal_rbf,
    signal_script_type,
    signal_shared_cluster,
    signal_tx_version,
    signal_utxo_linkage,
    signal_witness_present,
)
from graphsenselib.db.asynchronous.services.heuristics import (
    AddressOutput,
    ChangeHeuristics,
    CoinJoinConsensus,
    CoinJoinHeuristics,
    ConsensusEntry,
)
from graphsenselib.db.asynchronous.services.models import (
    ComparisonSignalInternal,
    LineageEdgeInternal,
    TxCharacteristicsInternal,
    TxRef,
    Txs,
    TxUtxo,
    TxValue,
    UtxoHeuristics,
    Values,
)
from graphsenselib.errors import (
    BadUserInputException,
    NotFoundException,
    TransactionNotFoundException,
)
from tests.db.helpers import (
    CURRENCY,
    make_tx,
    make_txvalue,
    make_value,
)


def make_chars(
    *,
    inputs_script_types: list[str] | None = None,
    outputs_script_types: list[str] | None = None,
    inputs_have_witness: bool | None = None,
    n_inputs: int = 1,
    n_outputs: int = 1,
    total_input_sat: int = 0,
    total_output_sat: int = 0,
    fee_sat: int | None = None,
    coinjoin_detected: bool = False,
    coinjoin_protocol: str | None = None,
    input_cluster_ids: list[int] | None = None,
    utxo_parent_indexes: list[int] | None = None,
    tx_version: int | None = None,
    locktime: int | None = None,
    inputs_signal_rbf: bool | None = None,
    block_height: int | None = None,
    bip69_outputs_sorted: bool | None = None,
    inputs_have_exchange: bool | None = None,
    input_addresses_canon: list[str] | None = None,
    change_addresses_canon: list[str] | None = None,
    parent_tx_hashes: list[str] | None = None,
) -> TxCharacteristicsInternal:
    return TxCharacteristicsInternal(
        inputs_script_types=inputs_script_types or [],
        outputs_script_types=outputs_script_types or [],
        inputs_have_witness=inputs_have_witness,
        n_inputs=n_inputs,
        n_outputs=n_outputs,
        total_input_sat=total_input_sat,
        total_output_sat=total_output_sat,
        fee_sat=fee_sat,
        coinjoin_detected=coinjoin_detected,
        coinjoin_protocol=coinjoin_protocol,
        input_cluster_ids=input_cluster_ids or [],
        utxo_parent_indexes=utxo_parent_indexes or [],
        tx_version=tx_version,
        locktime=locktime,
        inputs_signal_rbf=inputs_signal_rbf,
        block_height=block_height,
        bip69_outputs_sorted=bip69_outputs_sorted,
        inputs_have_exchange=inputs_have_exchange,
        input_addresses_canon=input_addresses_canon or [],
        change_addresses_canon=change_addresses_canon or [],
        parent_tx_hashes=parent_tx_hashes or [],
    )


# ---------------------------------------------------------------------------
# script_type_from_address
# ---------------------------------------------------------------------------


class TestScriptTypeFromAddress:
    @pytest.mark.parametrize(
        "addr,expected",
        [
            # P2WPKH (bech32, short)
            ("bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3t4", "P2WPKH"),
            ("tb1qw508d6qejxtdg4y5r3zarvary0c5xw7kxpjzsx", "P2WPKH"),
            # P2WSH (bech32, exactly 62 chars)
            (
                "bc1qrp33g0q5c5txsp9arysrx4k6zdkfs4nce4xj0gdcccefvpysxf3qccfmv3",
                "P2WSH",
            ),
            # bech32 v0 with a length that is neither P2WPKH (42) nor
            # P2WSH (62): classify as unknown instead of guessing.
            ("bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3t4xxxx", "UNKNOWN"),
            # P2TR (bech32m)
            (
                "bc1pmzfrwwndsqmk5yh69yjr5lfgfg4ev8c0tsc06e",
                "P2TR",
            ),
            # P2PKH
            ("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", "P2PKH"),
            ("mxosQ4CvQR8ipfWdRktyB3u16tauEdamGc", "P2PKH"),
            ("n2eMqTT929pb1RDNuqEnxdaLau1rxy3efi", "P2PKH"),
            # P2SH
            ("3J98t1WpEZ73CNmQviecrnyiWrnqRhWNLy", "P2SH"),
            ("2N2JD6wb56AfK4tfmM6PwdVmoYk2dCKf4Br", "P2SH"),
            # Coinbase
            ("coinbase", "COINBASE"),
            # Unknown / empty
            ("", "UNKNOWN"),
            ("zzzzz", "UNKNOWN"),
        ],
    )
    def test_prefix_table(self, addr, expected):
        assert script_type_from_address(addr) == expected


# ---------------------------------------------------------------------------
# _has_witness_for_type
# ---------------------------------------------------------------------------


class TestHasWitnessForType:
    @pytest.mark.parametrize(
        "script_type,expected",
        [
            ("P2WPKH", True),
            ("P2WSH", True),
            ("P2TR", True),
            ("WITNESS_UNKNOWN", True),
            ("P2PKH", False),
            ("P2PK", False),
            ("MULTISIG", False),
            ("MULTISIG_PUBKEY", False),
            ("P2SH", None),  # ambiguous: could be wrapped SegWit
            ("COINBASE", None),
            ("UNKNOWN", None),
            ("NONSTANDARD", None),
            ("anything-else", None),
        ],
    )
    def test_table(self, script_type, expected):
        assert _has_witness_for_type(script_type) == expected


# ---------------------------------------------------------------------------
# _aggregate_inputs_have_witness
# ---------------------------------------------------------------------------


class TestAggregateInputsHaveWitness:
    def test_all_true_ground_truth_wins(self):
        ins = [make_txvalue("a", 1, True), make_txvalue("b", 1, True)]
        # Pass conflicting inferred types, ground truth must win.
        assert _aggregate_inputs_have_witness(ins, ["P2PKH"]) is True

    def test_all_false_ground_truth_wins(self):
        ins = [make_txvalue("a", 1, False), make_txvalue("b", 1, False)]
        assert _aggregate_inputs_have_witness(ins, ["P2WPKH"]) is False

    def test_mixed_ground_truth_returns_none(self):
        ins = [make_txvalue("a", 1, True), make_txvalue("b", 1, False)]
        assert _aggregate_inputs_have_witness(ins, ["P2WPKH"]) is None

    def test_all_none_falls_back_to_unambiguous_inference(self):
        ins = [make_txvalue("a", 1, None), make_txvalue("b", 1, None)]
        assert _aggregate_inputs_have_witness(ins, ["P2WPKH"]) is True

    def test_all_none_falls_back_to_unambiguous_p2pkh(self):
        ins = [make_txvalue("a", 1, None)]
        assert _aggregate_inputs_have_witness(ins, ["P2PKH"]) is False

    def test_all_none_p2sh_is_ambiguous(self):
        ins = [make_txvalue("a", 1, None)]
        assert _aggregate_inputs_have_witness(ins, ["P2SH"]) is None

    def test_empty_inputs_returns_none(self):
        assert _aggregate_inputs_have_witness([], []) is None


# ---------------------------------------------------------------------------
# _unique_script_types
# ---------------------------------------------------------------------------


class TestUniqueScriptTypes:
    def test_dedupes_in_order(self):
        ios = [
            make_txvalue("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1),  # P2PKH
            make_txvalue("3J98t1WpEZ73CNmQviecrnyiWrnqRhWNLy", 1),  # P2SH
            make_txvalue("1Q2bL5sHGc8j1xC9JkbY9G7gx7VfL3FqYZ", 1),  # P2PKH again
        ]
        assert _unique_script_types(ios) == ["P2PKH", "P2SH"]

    def test_empty_returns_empty(self):
        assert _unique_script_types([]) == []

    def test_unknown_addresses_kept_once(self):
        ios = [make_txvalue("", 1), make_txvalue("", 1), make_txvalue("zzz", 1)]
        assert _unique_script_types(ios) == ["UNKNOWN"]

    def test_row_level_script_type_wins_over_prefix_inference(self):
        # The ingest-time classification must beat the address heuristic:
        # a "3..." address whose row says P2SH-embedded multisig-ish type
        # is reported as the row type, not as prefix-derived P2SH.
        ios = [
            make_txvalue(
                "3J98t1WpEZ73CNmQviecrnyiWrnqRhWNLy", 1, script_type="MULTISIG"
            ),
            # No row type: falls back to prefix inference.
            make_txvalue("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1),
        ]
        assert _unique_script_types(ios) == ["MULTISIG", "P2PKH"]

    def test_row_level_type_classifies_addressless_io(self):
        # OP_RETURN outputs carry no address; the prefix fallback alone
        # could never classify them, the row type can.
        ios = [make_txvalue("", 0, script_type="OP_RETURN")]
        assert _unique_script_types(ios) == ["OP_RETURN"]


# ---------------------------------------------------------------------------
# extract_characteristics
# ---------------------------------------------------------------------------


class TestExtractCharacteristics:
    def test_regular_tx_fee_computed_and_witness_from_inputs(self):
        ins = [
            make_txvalue(
                "bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3t4", 100_000, has_witness=True
            )
        ]
        outs = [
            make_txvalue("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 90_000),
        ]
        tx = make_tx(inputs=ins, outputs=outs)
        c = extract_characteristics(tx)
        assert c.inputs_script_types == ["P2WPKH"]
        assert c.outputs_script_types == ["P2PKH"]
        assert c.inputs_have_witness is True
        assert c.fee_sat == 10_000
        assert c.n_inputs == 1
        assert c.n_outputs == 1
        assert c.total_input_sat == 100_000
        assert c.total_output_sat == 90_000
        assert c.coinjoin_detected is False
        assert c.coinjoin_protocol is None

    def test_coinbase_tx_has_no_fee(self):
        ins = [make_txvalue("coinbase", 0)]
        outs = [make_txvalue("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 50_000_000)]
        tx = make_tx(
            inputs=ins,
            outputs=outs,
            coinbase=True,
            total_input=0,
            total_output=50_000_000,
        )
        c = extract_characteristics(tx)
        assert c.fee_sat is None
        assert c.inputs_script_types == ["COINBASE"]

    def test_coinjoin_flag_propagated(self):
        heur = UtxoHeuristics(
            change_heuristics=None,
            coinjoin_heuristics=CoinJoinHeuristics(
                consensus=CoinJoinConsensus(
                    detected=True,
                    confidence=80,
                    sources=["wasabi_coinjoin"],
                ),
            ),
        )
        ins = [make_txvalue("bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3t4", 1, True)]
        outs = [make_txvalue("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1)]
        tx = make_tx(inputs=ins, outputs=outs, heuristics=heur)
        c = extract_characteristics(tx)
        assert c.coinjoin_detected is True
        assert c.coinjoin_protocol == "wasabi_coinjoin"

    def test_inputs_have_witness_uses_ground_truth_over_inference(self):
        # P2SH is ambiguous via inference, but ground truth says True.
        ins = [
            make_txvalue("3J98t1WpEZ73CNmQviecrnyiWrnqRhWNLy", 1, has_witness=True),
        ]
        outs = [make_txvalue("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1)]
        tx = make_tx(inputs=ins, outputs=outs)
        c = extract_characteristics(tx)
        assert c.inputs_script_types == ["P2SH"]
        assert c.inputs_have_witness is True

    def test_version_locktime_block_height_propagated(self):
        ins = [make_txvalue("bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3t4", 1)]
        outs = [make_txvalue("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1)]
        tx = make_tx(
            inputs=ins, outputs=outs, height=812345, version=2, lock_time=812340
        )
        c = extract_characteristics(tx)
        assert c.tx_version == 2
        assert c.locktime == 812340
        assert c.block_height == 812345

    def test_inputs_signal_rbf_true_when_any_input_signals(self):
        ins = [
            make_txvalue("bc1q...", 1, sequence=0xFFFFFFFF),
            make_txvalue("bc1q...", 1, sequence=0xFFFFFFFD),
        ]
        outs = [make_txvalue("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1)]
        tx = make_tx(inputs=ins, outputs=outs)
        c = extract_characteristics(tx)
        assert c.inputs_signal_rbf is True

    def test_inputs_signal_rbf_false_when_all_final(self):
        ins = [
            make_txvalue("bc1q...", 1, sequence=0xFFFFFFFF),
            make_txvalue("bc1q...", 1, sequence=0xFFFFFFFE),
        ]
        outs = [make_txvalue("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1)]
        tx = make_tx(inputs=ins, outputs=outs)
        c = extract_characteristics(tx)
        assert c.inputs_signal_rbf is False

    def test_inputs_signal_rbf_none_when_sequences_missing(self):
        ins = [make_txvalue("bc1q...", 1, sequence=None)]
        outs = [make_txvalue("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1)]
        tx = make_tx(inputs=ins, outputs=outs)
        c = extract_characteristics(tx)
        assert c.inputs_signal_rbf is None


# ---------------------------------------------------------------------------
# signal_script_type
# ---------------------------------------------------------------------------


class TestSignalScriptType:
    def test_match_when_all_same(self):
        chars = [
            make_chars(inputs_script_types=["P2WPKH"]),
            make_chars(inputs_script_types=["P2WPKH"]),
        ]
        sig = signal_script_type(chars)
        assert sig.kind == "discriminator"
        assert sig.verdict == "match"

    def test_mismatch_when_distinct(self):
        chars = [
            make_chars(inputs_script_types=["P2WPKH"]),
            make_chars(inputs_script_types=["P2PKH"]),
        ]
        sig = signal_script_type(chars)
        assert sig.verdict == "mismatch"

    def test_inconclusive_when_any_missing(self):
        chars = [
            make_chars(inputs_script_types=["P2WPKH"]),
            make_chars(inputs_script_types=[]),
        ]
        sig = signal_script_type(chars)
        assert sig.verdict == "inconclusive"


# ---------------------------------------------------------------------------
# signal_witness_present
# ---------------------------------------------------------------------------


class TestSignalWitnessPresent:
    def test_match_all_true(self):
        chars = [
            make_chars(inputs_have_witness=True),
            make_chars(inputs_have_witness=True),
        ]
        sig = signal_witness_present(chars)
        assert sig.kind == "score"
        assert sig.verdict == "match"
        assert sig.per_tx == [True, True]

    def test_match_all_false(self):
        chars = [
            make_chars(inputs_have_witness=False),
            make_chars(inputs_have_witness=False),
        ]
        sig = signal_witness_present(chars)
        assert sig.verdict == "match"
        assert sig.per_tx == [False, False]

    def test_mismatch_when_distinct(self):
        chars = [
            make_chars(inputs_have_witness=True),
            make_chars(inputs_have_witness=False),
        ]
        sig = signal_witness_present(chars)
        assert sig.verdict == "mismatch"

    def test_inconclusive_when_any_none(self):
        chars = [
            make_chars(inputs_have_witness=True),
            make_chars(inputs_have_witness=None),
        ]
        sig = signal_witness_present(chars)
        assert sig.verdict == "inconclusive"


# ---------------------------------------------------------------------------
# signal_tx_version
# ---------------------------------------------------------------------------


class TestSignalTxVersion:
    def test_match_when_all_same(self):
        chars = [make_chars(tx_version=2), make_chars(tx_version=2)]
        sig = signal_tx_version(chars)
        assert sig.kind == "discriminator"
        assert sig.verdict == "match"
        assert sig.per_tx == [2, 2]

    def test_mismatch_when_distinct(self):
        chars = [make_chars(tx_version=1), make_chars(tx_version=2)]
        sig = signal_tx_version(chars)
        assert sig.verdict == "mismatch"
        assert sig.per_tx == [1, 2]

    def test_inconclusive_when_any_missing(self):
        chars = [make_chars(tx_version=2), make_chars(tx_version=None)]
        sig = signal_tx_version(chars)
        assert sig.verdict == "inconclusive"


# ---------------------------------------------------------------------------
# signal_rbf
# ---------------------------------------------------------------------------


class TestSignalRbf:
    def test_match_all_rbf(self):
        chars = [
            make_chars(inputs_signal_rbf=True),
            make_chars(inputs_signal_rbf=True),
        ]
        sig = signal_rbf(chars)
        assert sig.kind == "discriminator"
        assert sig.verdict == "match"
        assert sig.per_tx == [True, True]

    def test_match_all_final(self):
        chars = [
            make_chars(inputs_signal_rbf=False),
            make_chars(inputs_signal_rbf=False),
        ]
        sig = signal_rbf(chars)
        assert sig.verdict == "match"
        assert sig.per_tx == [False, False]

    def test_mismatch_when_distinct(self):
        chars = [
            make_chars(inputs_signal_rbf=True),
            make_chars(inputs_signal_rbf=False),
        ]
        sig = signal_rbf(chars)
        assert sig.verdict == "mismatch"

    def test_inconclusive_when_any_missing(self):
        chars = [
            make_chars(inputs_signal_rbf=True),
            make_chars(inputs_signal_rbf=None),
        ]
        sig = signal_rbf(chars)
        assert sig.verdict == "inconclusive"


# ---------------------------------------------------------------------------
# _classify_locktime + signal_locktime_pattern
# ---------------------------------------------------------------------------


class TestClassifyLocktime:
    @pytest.mark.parametrize(
        "locktime,height,expected",
        [
            (0, 812345, "zero"),
            (812340, 812345, "anti_sniping"),  # 5 blocks below tip
            (812245, 812345, "anti_sniping"),  # exactly 100 blocks below
            (812244, 812345, "other"),  # 101 blocks, outside window
            (812346, 812345, "other"),  # locktime above tip
            (1_700_000_000, 812345, "other"),  # unix-time form
            (None, 812345, None),
            (812340, None, "other"),  # locktime>0 but no height to anchor
        ],
    )
    def test_table(self, locktime, height, expected):
        assert _classify_locktime(locktime, height) == expected


class TestSignalLocktimePattern:
    def test_match_when_all_anti_sniping(self):
        chars = [
            make_chars(locktime=812340, block_height=812345),
            make_chars(locktime=812341, block_height=812346),
        ]
        sig = signal_locktime_pattern(chars)
        assert sig.kind == "discriminator"
        assert sig.verdict == "match"
        assert sig.per_tx == ["anti_sniping", "anti_sniping"]

    def test_match_when_all_zero(self):
        chars = [
            make_chars(locktime=0, block_height=100),
            make_chars(locktime=0, block_height=200),
        ]
        sig = signal_locktime_pattern(chars)
        assert sig.verdict == "match"
        assert sig.per_tx == ["zero", "zero"]

    def test_mismatch_zero_vs_anti_sniping(self):
        chars = [
            make_chars(locktime=0, block_height=812345),
            make_chars(locktime=812340, block_height=812345),
        ]
        sig = signal_locktime_pattern(chars)
        assert sig.verdict == "mismatch"

    def test_inconclusive_when_any_missing(self):
        chars = [
            make_chars(locktime=0, block_height=100),
            make_chars(locktime=None, block_height=100),
        ]
        sig = signal_locktime_pattern(chars)
        assert sig.verdict == "inconclusive"


# ---------------------------------------------------------------------------
# _bip69_outputs_sorted + signal_bip69_outputs_sorted
# ---------------------------------------------------------------------------


def _outs(amounts: list[int]) -> list[TxValue]:
    return [
        TxValue(address=[f"out{i}"], value=Values(value=a, fiat_values=[]), index=i)
        for i, a in enumerate(amounts)
    ]


class TestBip69OutputsSorted:
    def test_strictly_ascending_returns_true(self):
        assert _bip69_outputs_sorted(_outs([100, 200, 300])) is True

    def test_strictly_descending_returns_false(self):
        assert _bip69_outputs_sorted(_outs([300, 200, 100])) is False

    def test_unsorted_returns_false(self):
        assert _bip69_outputs_sorted(_outs([100, 300, 200])) is False

    def test_tied_amounts_inconclusive(self):
        # Tie tiebreaker requires script_hex (not stored for non-OP_RETURNs).
        assert _bip69_outputs_sorted(_outs([100, 100, 200])) is None

    def test_single_output_inconclusive(self):
        assert _bip69_outputs_sorted(_outs([100])) is None

    def test_empty_outputs_inconclusive(self):
        assert _bip69_outputs_sorted([]) is None


class TestSignalBip69OutputsSorted:
    def test_match_when_all_sorted(self):
        chars = [
            make_chars(bip69_outputs_sorted=True),
            make_chars(bip69_outputs_sorted=True),
        ]
        sig = signal_bip69_outputs_sorted(chars)
        # Soft signal, kind="score" so mismatch doesn't flip strong verdicts.
        assert sig.kind == "score"
        assert sig.verdict == "match"
        assert sig.per_tx == [True, True]

    def test_match_when_all_unsorted(self):
        chars = [
            make_chars(bip69_outputs_sorted=False),
            make_chars(bip69_outputs_sorted=False),
        ]
        sig = signal_bip69_outputs_sorted(chars)
        assert sig.verdict == "match"
        assert sig.per_tx == [False, False]

    def test_mismatch_when_distinct(self):
        chars = [
            make_chars(bip69_outputs_sorted=True),
            make_chars(bip69_outputs_sorted=False),
        ]
        sig = signal_bip69_outputs_sorted(chars)
        assert sig.verdict == "mismatch"

    def test_inconclusive_when_any_missing(self):
        chars = [
            make_chars(bip69_outputs_sorted=True),
            make_chars(bip69_outputs_sorted=None),
        ]
        sig = signal_bip69_outputs_sorted(chars)
        assert sig.verdict == "inconclusive"


# ---------------------------------------------------------------------------
# signal_output_count_shape
# ---------------------------------------------------------------------------


class TestSignalOutputCountShape:
    def test_single_output_match(self):
        chars = [make_chars(n_outputs=1), make_chars(n_outputs=1)]
        sig = signal_output_count_shape(chars)
        assert sig.kind == "score"
        assert sig.verdict == "match"
        assert sig.per_tx == ["single", "single"]

    def test_pay_plus_change_match(self):
        chars = [make_chars(n_outputs=2), make_chars(n_outputs=2)]
        sig = signal_output_count_shape(chars)
        assert sig.verdict == "match"
        assert sig.per_tx == ["pay_plus_change", "pay_plus_change"]

    def test_many_match(self):
        chars = [make_chars(n_outputs=5), make_chars(n_outputs=10)]
        sig = signal_output_count_shape(chars)
        assert sig.verdict == "match"
        assert sig.per_tx == ["many", "many"]

    def test_mismatch_across_buckets(self):
        chars = [make_chars(n_outputs=2), make_chars(n_outputs=10)]
        sig = signal_output_count_shape(chars)
        assert sig.verdict == "mismatch"


# ---------------------------------------------------------------------------
# signal_direct_input_overlap
# ---------------------------------------------------------------------------


class TestSignalDirectInputOverlap:
    def test_match_when_all_share_an_input(self):
        chars = [
            make_chars(input_addresses_canon=["A", "B"]),
            make_chars(input_addresses_canon=["B", "C"]),
        ]
        sig = signal_direct_input_overlap(chars)
        assert sig.kind == "linkage"
        assert sig.verdict == "match"
        assert sig.weight == 0
        # Each tx's per_tx column is the set of its inputs that show up in others.
        assert sig.per_tx == [["B"], ["B"]]

    def test_match_when_connected_via_chain(self):
        # 0-1 share A, 1-2 share C → connected via tx 1.
        chars = [
            make_chars(input_addresses_canon=["A"]),
            make_chars(input_addresses_canon=["A", "C"]),
            make_chars(input_addresses_canon=["C"]),
        ]
        sig = signal_direct_input_overlap(chars)
        assert sig.verdict == "match"

    def test_mismatch_when_disjoint(self):
        chars = [
            make_chars(input_addresses_canon=["A"]),
            make_chars(input_addresses_canon=["B"]),
        ]
        sig = signal_direct_input_overlap(chars)
        assert sig.verdict == "mismatch"

    def test_inconclusive_when_any_tx_has_no_inputs(self):
        chars = [
            make_chars(input_addresses_canon=["A"]),
            make_chars(input_addresses_canon=[]),
        ]
        sig = signal_direct_input_overlap(chars)
        assert sig.verdict == "inconclusive"


# ---------------------------------------------------------------------------
# signal_change_chain
# ---------------------------------------------------------------------------


class TestSignalChangeChain:
    def test_match_when_change_becomes_input_of_another(self):
        # Tx 0's change "X" is an input to tx 1.
        chars = [
            make_chars(input_addresses_canon=["A"], change_addresses_canon=["X"]),
            make_chars(input_addresses_canon=["X", "Y"]),
        ]
        sig = signal_change_chain(chars)
        assert sig.kind == "linkage"
        assert sig.verdict == "match"
        assert sig.weight == 0
        # Tx 0 has the consumed change; tx 1 has none of its own change consumed.
        assert sig.per_tx == [["X"], []]

    def test_mismatch_when_no_change_consumed(self):
        chars = [
            make_chars(input_addresses_canon=["A"], change_addresses_canon=["X"]),
            make_chars(input_addresses_canon=["B"]),
        ]
        sig = signal_change_chain(chars)
        assert sig.verdict == "mismatch"

    def test_inconclusive_when_no_change_addresses_at_all(self):
        chars = [
            make_chars(input_addresses_canon=["A"]),
            make_chars(input_addresses_canon=["B"]),
        ]
        sig = signal_change_chain(chars)
        assert sig.verdict == "inconclusive"


# ---------------------------------------------------------------------------
# signal_common_ancestor
# ---------------------------------------------------------------------------


class TestSignalCommonAncestor:
    def test_match_when_pair_shares_parent(self):
        chars = [
            make_chars(parent_tx_hashes=["P1", "P2"]),
            make_chars(parent_tx_hashes=["P2", "P3"]),
        ]
        sig = signal_common_ancestor(chars)
        assert sig.kind == "linkage"
        assert sig.verdict == "match"
        assert sig.weight == 0
        assert sig.per_tx == [["P2"], ["P2"]]

    def test_mismatch_when_disjoint_parents(self):
        chars = [
            make_chars(parent_tx_hashes=["P1"]),
            make_chars(parent_tx_hashes=["P2"]),
        ]
        sig = signal_common_ancestor(chars)
        assert sig.verdict == "mismatch"

    def test_inconclusive_when_any_tx_has_no_parents(self):
        # Coinbase has no parents → inconclusive.
        chars = [
            make_chars(parent_tx_hashes=["P1"]),
            make_chars(parent_tx_hashes=[]),
        ]
        sig = signal_common_ancestor(chars)
        assert sig.verdict == "inconclusive"


# ---------------------------------------------------------------------------
# _inputs_have_exchange_for_tx + signal_exchange_input_overlap
# ---------------------------------------------------------------------------


class TestInputsHaveExchangeForTx:
    def test_returns_none_when_map_empty(self):
        tx = make_tx(inputs=[make_txvalue("1Aa", 1)])
        assert _inputs_have_exchange_for_tx("btc", tx, {}) is None

    def test_returns_true_when_any_input_is_exchange(self):
        tx = make_tx(inputs=[make_txvalue("1Aa", 1), make_txvalue("1Bb", 1)])
        flags = {"1Aa": False, "1Bb": True}
        assert _inputs_have_exchange_for_tx("btc", tx, flags) is True

    def test_returns_false_when_addresses_resolved_but_none_exchange(self):
        tx = make_tx(inputs=[make_txvalue("1Aa", 1), make_txvalue("1Bb", 1)])
        flags = {"1Aa": False, "1Bb": False}
        assert _inputs_have_exchange_for_tx("btc", tx, flags) is False

    def test_returns_none_when_no_input_address_resolved(self):
        tx = make_tx(inputs=[make_txvalue("1Aa", 1)])
        # Map carries other addresses but not this tx's input → no info → None.
        flags = {"1Cc": True}
        assert _inputs_have_exchange_for_tx("btc", tx, flags) is None


class TestFetchInputAddressExchangeFlags:
    """The fast-path replacement for the old best_cluster_tag digest call:
    a cluster-level existence check via ``which_clusters_have_concept``."""

    def _svc(self, exchange_cluster_ids: set[int]):
        svc = MagicMock()
        tags = MagicMock()
        tags.which_clusters_have_concept = AsyncMock(
            return_value=set(exchange_cluster_ids)
        )
        svc.tags_service = tags
        return svc, tags

    async def test_returns_empty_when_no_tags_service(self):
        svc = MagicMock(spec=[])  # no tags_service attribute
        out = await _fetch_input_address_exchange_flags(svc, "btc", {"1Aa": 5}, [])
        assert out == {}

    async def test_returns_empty_when_no_addresses(self):
        svc, _ = self._svc(set())
        out = await _fetch_input_address_exchange_flags(svc, "btc", {}, [])
        assert out == {}

    async def test_address_in_exchange_cluster_flagged(self):
        svc, tags = self._svc({7})
        out = await _fetch_input_address_exchange_flags(
            svc, "btc", {"1Aa": 7, "1Bb": 8}, ["public"]
        )
        assert out == {"1Aa": True, "1Bb": False}
        # Should call the cheap path with the deduped cluster set, not the
        # heavy ``get_tag_summaries_by_subject_ids`` digest path.
        tags.which_clusters_have_concept.assert_awaited_once()
        call_args = tags.which_clusters_have_concept.await_args
        assert call_args.args[0] == "btc"
        assert sorted(call_args.args[1]) == [7, 8]
        assert call_args.args[2] == ["public"]
        assert call_args.args[3] == "exchange"

    async def test_unresolved_clusters_filtered_and_all_false(self):
        # -1 marks unresolved in _fetch_input_address_clusters; nothing should
        # be queried and everything reads as False.
        svc, tags = self._svc(set())
        out = await _fetch_input_address_exchange_flags(
            svc, "btc", {"1Aa": -1, "1Bb": -1}, []
        )
        assert out == {"1Aa": False, "1Bb": False}
        tags.which_clusters_have_concept.assert_not_called()

    async def test_cluster_ids_deduped_in_query(self):
        # Two addresses in the same cluster → one cluster id sent to tagstore.
        svc, tags = self._svc({42})
        out = await _fetch_input_address_exchange_flags(
            svc, "btc", {"1Aa": 42, "1Bb": 42, "1Cc": 99}, []
        )
        assert out == {"1Aa": True, "1Bb": True, "1Cc": False}
        sent_ids = tags.which_clusters_have_concept.await_args.args[1]
        assert sorted(sent_ids) == [42, 99]


class TestSignalExchangeInputOverlap:
    def test_match_when_all_exchange(self):
        chars = [
            make_chars(inputs_have_exchange=True),
            make_chars(inputs_have_exchange=True),
        ]
        sig = signal_exchange_input_overlap(chars)
        assert sig.kind == "linkage"
        assert sig.verdict == "match"
        assert sig.weight == 0
        assert sig.per_tx == [True, True]

    def test_mismatch_when_some_not_exchange(self):
        chars = [
            make_chars(inputs_have_exchange=True),
            make_chars(inputs_have_exchange=False),
        ]
        sig = signal_exchange_input_overlap(chars)
        assert sig.verdict == "mismatch"

    def test_inconclusive_when_any_unknown(self):
        chars = [
            make_chars(inputs_have_exchange=True),
            make_chars(inputs_have_exchange=None),
        ]
        sig = signal_exchange_input_overlap(chars)
        assert sig.verdict == "inconclusive"


# ---------------------------------------------------------------------------
# aggregate_verdict, exchange-overlap demotion
# ---------------------------------------------------------------------------


def _exchange_match() -> ComparisonSignalInternal:
    return ComparisonSignalInternal(
        name="exchange_input_overlap",
        kind="linkage",
        per_tx=[True, True],
        verdict="match",
        weight=0,
    )


class TestExchangeOverlapDemotion:
    def test_same_cluster_with_exchange_demotes_to_likely_linked(self):
        chars = [make_chars(), make_chars()]
        baseline = aggregate_verdict([_disc("match"), _score(0)], chars, "same")
        demoted = aggregate_verdict(
            [_disc("match"), _score(0), _exchange_match()], chars, "same"
        )
        assert baseline.relation == "linked"
        assert demoted.relation == "likely_linked"
        assert demoted.confidence < baseline.confidence
        assert any(n.code == "exchange_overlap_demotion" for n in demoted.notes)

    def test_same_cluster_with_disc_and_exchange_compounds(self):
        chars = [make_chars(), make_chars()]
        only_disc = aggregate_verdict([_disc("mismatch")], chars, "same")
        with_both = aggregate_verdict(
            [_disc("mismatch"), _exchange_match()], chars, "same"
        )
        assert only_disc.relation == with_both.relation == "likely_linked"
        assert with_both.confidence < only_disc.confidence


# ---------------------------------------------------------------------------
# signal_shared_cluster
# ---------------------------------------------------------------------------


class TestSignalSharedCluster:
    def test_same_cluster_match(self):
        chars = [make_chars(input_cluster_ids=[1]), make_chars(input_cluster_ids=[1])]
        sig = signal_shared_cluster(chars, "same")
        assert sig.kind == "linkage"
        assert sig.verdict == "match"

    def test_different_cluster_mismatch(self):
        chars = [make_chars(input_cluster_ids=[1]), make_chars(input_cluster_ids=[2])]
        sig = signal_shared_cluster(chars, "different")
        assert sig.verdict == "mismatch"

    def test_unknown_cluster_inconclusive(self):
        chars = [make_chars(input_cluster_ids=[]), make_chars(input_cluster_ids=[1])]
        sig = signal_shared_cluster(chars, "unknown")
        assert sig.verdict == "inconclusive"


# ---------------------------------------------------------------------------
# signal_utxo_linkage
# ---------------------------------------------------------------------------


class TestSignalUtxoLinkage:
    def test_two_txs_with_edge_match(self):
        chars = [
            make_chars(utxo_parent_indexes=[1]),
            make_chars(utxo_parent_indexes=[]),
        ]
        sig = signal_utxo_linkage(chars)
        assert sig.kind == "linkage"
        assert sig.verdict == "match"

    def test_three_txs_two_components_mismatch(self):
        # 0-1 connected; 2 alone → 2 components → mismatch
        chars = [
            make_chars(utxo_parent_indexes=[1]),
            make_chars(utxo_parent_indexes=[]),
            make_chars(utxo_parent_indexes=[]),
        ]
        sig = signal_utxo_linkage(chars)
        assert sig.verdict == "mismatch"

    def test_single_tx_alone_mismatch(self):
        chars = [make_chars()]
        sig = signal_utxo_linkage(chars)
        assert sig.verdict == "mismatch"

    def test_three_txs_single_component_match(self):
        # chain: 0 - 1 - 2 → single component → match
        chars = [
            make_chars(utxo_parent_indexes=[1]),
            make_chars(utxo_parent_indexes=[2]),
            make_chars(utxo_parent_indexes=[]),
        ]
        sig = signal_utxo_linkage(chars)
        assert sig.verdict == "match"


# ---------------------------------------------------------------------------
# compute_cluster_verdict
# ---------------------------------------------------------------------------


class TestComputeClusterVerdict:
    def test_same_when_all_share_one(self):
        chars = [
            make_chars(input_cluster_ids=[1, 2]),
            make_chars(input_cluster_ids=[2, 3]),
        ]
        assert compute_cluster_verdict(chars) == "same"

    def test_different_when_disjoint(self):
        chars = [
            make_chars(input_cluster_ids=[1]),
            make_chars(input_cluster_ids=[2]),
        ]
        assert compute_cluster_verdict(chars) == "different"

    def test_unknown_when_any_empty(self):
        chars = [
            make_chars(input_cluster_ids=[1]),
            make_chars(input_cluster_ids=[]),
        ]
        assert compute_cluster_verdict(chars) == "unknown"


# ---------------------------------------------------------------------------
# aggregate_verdict
# ---------------------------------------------------------------------------


def _disc(
    verdict: str = "mismatch",
    name: str = "script_type",
    weight: int | None = None,
) -> ComparisonSignalInternal:
    """Discriminator mock. Default weight reflects spec magnitudes so that
    ``mis_w`` / ``match_w`` accumulate as the aggregator expects: one
    mismatch contributes -35 (lands in ``potential_unlink`` alone, two land
    in ``likely_unlinked`` at the -60 threshold); one match contributes +5."""
    if weight is None:
        weight = -35 if verdict == "mismatch" else (5 if verdict == "match" else 0)
    return ComparisonSignalInternal(
        name=name,
        kind="discriminator",
        per_tx=[None, None],
        verdict=verdict,
        weight=weight,
    )


def _score(weight: int, verdict: str = "match") -> ComparisonSignalInternal:
    return ComparisonSignalInternal(
        name="witness_present",
        kind="score",
        per_tx=[None, None],
        verdict=verdict,
        weight=weight,
    )


def _linkage(
    name: str = "direct_input_overlap", verdict: str = "match"
) -> ComparisonSignalInternal:
    return ComparisonSignalInternal(
        name=name,
        kind="linkage",
        per_tx=[None, None],
        verdict=verdict,
        weight=0,
    )


class TestAggregateVerdict:
    def test_same_cluster_no_disc_hit_linked(self):
        chars = [make_chars(), make_chars()]
        verdict = aggregate_verdict([_disc("match"), _score(0)], chars, "same")
        assert verdict.relation == "linked"
        assert verdict.cluster_verdict == "same"
        assert any(n.code == "shared_cluster_support" for n in verdict.notes)
        # The fired linkage gates are exposed machine-readably, mirroring
        # discriminator_hits on the negative side.
        assert verdict.linkage_hits == ["shared_cluster"]

    def test_same_cluster_with_disc_hit_likely_linked_with_merge_note(self):
        chars = [make_chars(), make_chars()]
        verdict = aggregate_verdict([_disc("mismatch")], chars, "same")
        assert verdict.relation == "likely_linked"
        assert "script_type" in verdict.discriminator_hits
        assert any(n.code == "cluster_merge_or_wallet_upgrade" for n in verdict.notes)

    def test_same_cluster_disc_hit_score_disagrees_lowers_confidence(self):
        chars = [make_chars(), make_chars()]
        baseline = aggregate_verdict([_disc("mismatch"), _score(0)], chars, "same")
        with_neg_score = aggregate_verdict(
            [_disc("mismatch"), _score(-50, verdict="mismatch")], chars, "same"
        )
        # Relation is fixed by cluster_verdict; only confidence moves.
        assert with_neg_score.relation == baseline.relation == "likely_linked"
        assert with_neg_score.confidence < baseline.confidence

    def test_same_cluster_no_disc_score_agrees_raises_confidence(self):
        chars = [make_chars(), make_chars()]
        baseline = aggregate_verdict([_disc("match"), _score(0)], chars, "same")
        with_pos_score = aggregate_verdict(
            [_disc("match"), _score(50, verdict="match")], chars, "same"
        )
        assert with_pos_score.relation == baseline.relation == "linked"
        assert with_pos_score.confidence > baseline.confidence
        assert with_pos_score.confidence <= 100

    def test_different_cluster_with_disc_hit_unlinked(self):
        chars = [make_chars(), make_chars()]
        verdict = aggregate_verdict([_disc("mismatch")], chars, "different")
        assert verdict.relation == "unlinked"
        assert "script_type" in verdict.discriminator_hits

    def test_different_cluster_no_disc_likely_unlinked(self):
        chars = [make_chars(), make_chars()]
        verdict = aggregate_verdict([_disc("match"), _score(0)], chars, "different")
        assert verdict.relation == "likely_unlinked"

    def test_unknown_cluster_single_disc_hit_potential_unlink(self):
        chars = [make_chars(), make_chars()]
        verdict = aggregate_verdict([_disc("mismatch")], chars, "unknown")
        assert verdict.relation == "potential_unlink"

    def test_unknown_cluster_multiple_disc_hits_likely_unlinked(self):
        chars = [make_chars(), make_chars()]
        verdict = aggregate_verdict(
            [_disc("mismatch", name="a"), _disc("mismatch", name="b")],
            chars,
            "unknown",
        )
        assert verdict.relation == "likely_unlinked"

    def test_different_cluster_all_disc_match_likely_unlinked_no_linkage(self):
        # Per spec: cluster=different always → likely_unlinked when no
        # linkage gate fires, regardless of fingerprint agreement.
        # Discriminator agreement alone never overrides cluster=different.
        chars = [make_chars(), make_chars()]
        verdict = aggregate_verdict(
            [_disc("match", name=f"d{i}") for i in range(5)],
            chars,
            "different",
        )
        assert verdict.relation == "likely_unlinked"

    def test_different_cluster_partial_fingerprint_likely_unlinked(self):
        chars = [make_chars(), make_chars()]
        verdict = aggregate_verdict(
            [_disc("match", name=f"d{i}") for i in range(3)],
            chars,
            "different",
        )
        assert verdict.relation == "likely_unlinked"

    def test_different_cluster_strong_linkage_promotes_to_likely_linked(self):
        # Cluster says different, but a strong on-chain linkage signal fires.
        # Direct facts override the heuristic.
        chars = [make_chars(), make_chars()]
        verdict = aggregate_verdict(
            [_disc("match", name=f"d{i}") for i in range(5)]
            + [_linkage("direct_input_overlap", "match")],
            chars,
            "different",
        )
        assert verdict.relation == "likely_linked"
        assert verdict.confidence == 65
        assert any(n.code == "onchain_linkage_support" for n in verdict.notes)

    def test_different_cluster_one_match_likely_unlinked(self):
        chars = [make_chars(), make_chars()]
        verdict = aggregate_verdict([_disc("match")], chars, "different")
        assert verdict.relation == "likely_unlinked"

    def test_unknown_cluster_strong_linkage_likely_linked(self):
        chars = [make_chars(), make_chars()]
        verdict = aggregate_verdict(
            [_linkage("change_chain", "match")],
            chars,
            "unknown",
        )
        assert verdict.relation == "likely_linked"
        assert verdict.confidence == 60

    def test_unknown_cluster_no_disc_negative_score_likely_unlinked(self):
        # mis_w must be ≤ -60 to land in likely_unlinked under the new spec
        # threshold; -65 from a single strong score mismatch suffices.
        chars = [make_chars(), make_chars()]
        verdict = aggregate_verdict(
            [_disc("match"), _score(-65, verdict="mismatch")], chars, "unknown"
        )
        assert verdict.relation == "likely_unlinked"

    def test_unknown_cluster_no_disc_positive_score_potential_link(self):
        # Score signals are softer than discriminators and never promote past
        # potential_link without actual on-chain linkage evidence.
        chars = [make_chars(), make_chars()]
        verdict = aggregate_verdict(
            [_disc("match"), _score(50, verdict="match")], chars, "unknown"
        )
        assert verdict.relation == "potential_link"

    def test_unknown_cluster_no_disc_zero_score_inconclusive(self):
        # Truly nothing fired: inconclusive (any positive match_w would
        # promote to potential_link under the new architecture).
        chars = [make_chars(), make_chars()]
        verdict = aggregate_verdict(
            [_disc("match", weight=0), _score(0)], chars, "unknown"
        )
        assert verdict.relation == "inconclusive"

    def test_weak_negative_with_match_falls_to_inconclusive(self):
        # Spec (fingerprint_verdict.tex): potential_link requires mis_w == 0.
        # A weak negative in (-30, 0), e.g. a single rbf mismatch at -25,
        # plus a positive match must not read as potential_link.
        chars = [make_chars(), make_chars()]
        verdict = aggregate_verdict(
            [_disc("mismatch", name="rbf", weight=-25), _disc("match")],
            chars,
            "unknown",
        )
        assert verdict.relation == "inconclusive"

    def test_coinjoin_note_appended(self):
        chars = [make_chars(coinjoin_detected=True), make_chars()]
        verdict = aggregate_verdict([_disc("match"), _score(0)], chars, "unknown")
        assert any(n.code == "coinjoin_detected" for n in verdict.notes)

    def test_relation_label_in_known_set(self):
        # Sanity: every branch returns one of the documented labels.
        chars = [make_chars(), make_chars()]
        for cv in ("same", "different", "unknown"):
            for disc in ("match", "mismatch"):
                v = aggregate_verdict([_disc(disc), _score(0)], chars, cv)
                assert v.relation in {
                    "linked",
                    "likely_linked",
                    "potential_link",
                    "inconclusive",
                    "potential_unlink",
                    "likely_unlinked",
                    "unlinked",
                }


# ---------------------------------------------------------------------------
# Private helpers used during orchestration
# ---------------------------------------------------------------------------


class TestCanonicalInputAddresses:
    def test_dedupes_in_order_of_first_appearance(self):
        ins = [
            make_txvalue("A", 1),
            make_txvalue("B", 1),
            make_txvalue("A", 1),  # duplicate
            make_txvalue("C", 1),
        ]
        tx = make_tx(inputs=ins)
        assert _canonical_input_addresses("btc", tx) == ["A", "B", "C"]

    def test_skips_inputs_without_address(self):
        # Coinbase-style input: empty address list, should be skipped.
        ins = [
            TxValue(address=[], value=make_value(0), has_witness=None, sequence=None),
            make_txvalue("A", 1),
        ]
        tx = make_tx(inputs=ins)
        assert _canonical_input_addresses("btc", tx) == ["A"]

    def test_empty_inputs_returns_empty(self):
        tx = make_tx(inputs=[])
        assert _canonical_input_addresses("btc", tx) == []


class TestConsensusChangeAddresses:
    def _entry(self, addr: str, index: int = 0) -> ConsensusEntry:
        return ConsensusEntry(
            output=AddressOutput(address=addr, index=index),
            confidence=80,
            sources=["multi_input"],
        )

    def test_empty_when_no_heuristics(self):
        tx = make_tx(heuristics=None)
        assert _consensus_change_addresses(tx) == []

    def test_empty_when_change_heuristics_missing(self):
        heur = UtxoHeuristics(change_heuristics=None, coinjoin_heuristics=None)
        tx = make_tx(heuristics=heur)
        assert _consensus_change_addresses(tx) == []

    def test_empty_when_consensus_empty(self):
        heur = UtxoHeuristics(
            change_heuristics=ChangeHeuristics(consensus=[]),
            coinjoin_heuristics=None,
        )
        tx = make_tx(heuristics=heur)
        assert _consensus_change_addresses(tx) == []

    def test_returns_addresses_in_order(self):
        heur = UtxoHeuristics(
            change_heuristics=ChangeHeuristics(
                consensus=[self._entry("X", 0), self._entry("Y", 1)],
            ),
            coinjoin_heuristics=None,
        )
        tx = make_tx(heuristics=heur)
        assert _consensus_change_addresses(tx) == ["X", "Y"]


class TestUtxoParentIndexesFromHashes:
    def test_simple_two_tx_projection(self):
        # tx 0 has tx 1 as a parent.
        result = _utxo_parent_indexes_from_hashes(
            parent_hashes=[["h1"], []],
            tx_hashes=["h0", "h1"],
        )
        assert result == [[1], []]

    def test_self_reference_filtered(self):
        # tx 0's parents list contains its own hash, must be dropped.
        result = _utxo_parent_indexes_from_hashes(
            parent_hashes=[["h0", "h1"], []],
            tx_hashes=["h0", "h1"],
        )
        assert result == [[1], []]

    def test_unknown_hashes_dropped(self):
        # External parents (not in compared set) must be filtered.
        result = _utxo_parent_indexes_from_hashes(
            parent_hashes=[["external_parent", "h1"], []],
            tx_hashes=["h0", "h1"],
        )
        assert result == [[1], []]

    def test_duplicates_deduped_preserving_first_order(self):
        result = _utxo_parent_indexes_from_hashes(
            parent_hashes=[["h2", "h1", "h2"], [], []],
            tx_hashes=["h0", "h1", "h2"],
        )
        assert result == [[2, 1], [], []]

    def test_empty_parents_yields_empty_per_tx(self):
        result = _utxo_parent_indexes_from_hashes(
            parent_hashes=[[], [], []],
            tx_hashes=["h0", "h1", "h2"],
        )
        assert result == [[], [], []]


class TestParentHashesFromRefs:
    def test_dedupes_preserving_first_order(self):
        refs = [
            [
                TxRef(input_index=0, output_index=0, tx_hash="h2"),
                TxRef(input_index=1, output_index=0, tx_hash="h1"),
                TxRef(input_index=2, output_index=1, tx_hash="h2"),  # dup hash
            ],
            [],
        ]
        assert _parent_hashes_from_refs(refs) == [["h2", "h1"], []]

    def test_external_parents_kept(self):
        # Parents outside the compared set still belong in parent_tx_hashes;
        # signal_common_ancestor matches on external ancestors too.
        refs = [[TxRef(input_index=0, output_index=0, tx_hash="external")]]
        assert _parent_hashes_from_refs(refs) == [["external"]]

    def test_empty_refs_yields_empty_lists(self):
        assert _parent_hashes_from_refs([[], []]) == [[], []]


class TestLineageEdgesFromRefs:
    def test_simple_spend_edge(self):
        # tx 0 (spender) spends output 3 of tx 1 via its input 0.
        refs = [
            [TxRef(input_index=0, output_index=3, tx_hash="h1")],
            [],
        ]
        edges = _lineage_edges_from_refs(refs, ["h0", "h1"])
        assert edges == [
            LineageEdgeInternal(
                from_idx=1,
                to_idx=0,
                kind="output_spent_by_input",
                out_index=3,
                in_index=0,
            )
        ]

    def test_external_parent_dropped(self):
        # Parent not in the compared set produces no lineage edge.
        refs = [[TxRef(input_index=0, output_index=0, tx_hash="external")], []]
        assert _lineage_edges_from_refs(refs, ["h0", "h1"]) == []

    def test_self_reference_dropped(self):
        refs = [[TxRef(input_index=0, output_index=0, tx_hash="h0")]]
        assert _lineage_edges_from_refs(refs, ["h0"]) == []

    def test_multiple_outputs_of_same_parent_are_distinct_edges(self):
        # tx 0 spends two different outputs of tx 1 → two distinct edges.
        refs = [
            [
                TxRef(input_index=0, output_index=1, tx_hash="h1"),
                TxRef(input_index=1, output_index=4, tx_hash="h1"),
            ],
            [],
        ]
        edges = _lineage_edges_from_refs(refs, ["h0", "h1"])
        assert [(e.out_index, e.in_index) for e in edges] == [(1, 0), (4, 1)]
        assert all(e.from_idx == 1 and e.to_idx == 0 for e in edges)

    def test_empty_refs_yields_no_edges(self):
        assert _lineage_edges_from_refs([[], []], ["h0", "h1"]) == []


class TestInputClusterIdsForTx:
    def test_dedupes_in_order(self):
        ins = [
            make_txvalue("A", 1),
            make_txvalue("B", 1),
            make_txvalue("A", 1),  # same address again → same cluster, deduped
        ]
        tx = make_tx(inputs=ins)
        mapping = {"A": 7, "B": 9}
        assert _input_cluster_ids_for_tx("btc", tx, mapping) == [7, 9]

    def test_unresolved_minus_one_filtered(self):
        ins = [make_txvalue("A", 1), make_txvalue("B", 1)]
        tx = make_tx(inputs=ins)
        mapping = {"A": 7, "B": -1}
        assert _input_cluster_ids_for_tx("btc", tx, mapping) == [7]

    def test_address_missing_from_map_treated_as_unresolved(self):
        ins = [make_txvalue("A", 1), make_txvalue("B", 1)]
        tx = make_tx(inputs=ins)
        mapping = {"A": 7}  # B absent → defaults to -1 → filtered
        assert _input_cluster_ids_for_tx("btc", tx, mapping) == [7]

    def test_coinbase_input_skipped(self):
        ins = [
            TxValue(address=[], value=make_value(0), has_witness=None, sequence=None),
            make_txvalue("A", 1),
        ]
        tx = make_tx(inputs=ins)
        assert _input_cluster_ids_for_tx("btc", tx, {"A": 5}) == [5]

    def test_empty_inputs_returns_empty(self):
        tx = make_tx(inputs=[])
        assert _input_cluster_ids_for_tx("btc", tx, {"A": 1}) == []


class TestConnectedComponents:
    def test_empty_graph_zero_components(self):
        assert _connected_components([]) == 0

    def test_single_isolated_node(self):
        assert _connected_components([set()]) == 1

    def test_two_isolated_nodes(self):
        assert _connected_components([set(), set()]) == 2

    def test_two_connected_nodes_one_component(self):
        assert _connected_components([{1}, {0}]) == 1

    def test_three_node_chain_one_component(self):
        # 0 -- 1 -- 2
        assert _connected_components([{1}, {0, 2}, {1}]) == 1

    def test_pair_plus_isolated_singleton(self):
        # 0 -- 1, 2 alone
        assert _connected_components([{1}, {0}, set()]) == 2

    def test_disconnected_pairs(self):
        # 0 -- 1, 2 -- 3
        assert _connected_components([{1}, {0}, {3}, {2}]) == 2


# ---------------------------------------------------------------------------
# Orchestration: compare_txs
# ---------------------------------------------------------------------------


class FakeTxsService:
    """Minimal fake implementing the surface ``compare_txs`` calls."""

    def __init__(
        self,
        tx_map: dict[str, TxUtxo],
        spending_map: dict[str, list[TxRef]] | None = None,
        addresses_light: dict[str, dict] | None = None,
        flows_map: dict[str, list] | None = None,
    ):
        self._tx_map = tx_map
        self._spending = spending_map or {}
        self._flows = flows_map or {}
        self.db = MagicMock()
        self.db.get_addresses_light = AsyncMock(return_value=addresses_light or {})
        self.get_tx_calls: list[dict] = []
        self.spending_calls = 0

    async def get_tx(self, currency, tx_hash, *args, **kwargs):
        self.get_tx_calls.append(kwargs)
        # Mirror the real service: an unknown hash (including a hash from
        # another chain, which won't exist in this currency's keyspace)
        # raises TransactionNotFoundException rather than returning None.
        if tx_hash not in self._tx_map:
            raise TransactionNotFoundException(currency, tx_hash)
        return self._tx_map[tx_hash]

    async def get_spending_txs(self, currency, tx_hash, io_index):
        self.spending_calls += 1
        return self._spending.get(tx_hash, [])

    async def get_asset_flows_within_tx(self, network, tx_hash, **kwargs):
        # Full asset-flow set for a hash: base tx + token-transfer legs. Falls
        # back to the single base tx when no flows are configured.
        legs = self._flows.get(tx_hash)
        if legs is None:
            legs = [self._tx_map[tx_hash]]
        return Txs(txs=legs)


class TestCompareTxsOrchestration:
    def _make_two_linked_txs(self):
        # tx0 spends tx1's output (tx0's input references tx1).
        h0, h1 = "aa" * 32, "bb" * 32
        ins0 = [
            make_txvalue(
                "bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3t4", 100_000, has_witness=True
            )
        ]
        outs0 = [make_txvalue("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 90_000)]
        ins1 = [
            make_txvalue(
                "bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3t4", 50_000, has_witness=True
            )
        ]
        outs1 = [make_txvalue("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 49_000)]
        tx0 = make_tx(tx_hash=h0, inputs=ins0, outputs=outs0, height=100)
        tx1 = make_tx(tx_hash=h1, inputs=ins1, outputs=outs1, height=99)
        # tx0 has a parent edge to tx1, get_spending_txs(h0) returns ref(h1).
        spending = {
            h0: [TxRef(input_index=0, output_index=0, tx_hash=h1)],
            h1: [],
        }
        addrs_light = {
            "bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3t4": {"cluster_id": 7},
            "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa": {"cluster_id": 8},
        }
        svc = FakeTxsService(
            tx_map={h0: tx0, h1: tx1},
            spending_map=spending,
            addresses_light=addrs_light,
        )
        return svc, [h0, h1]

    async def test_basic_two_tx_comparison(self):
        svc, hashes = self._make_two_linked_txs()
        result = await compare_txs(
            svc,
            CURRENCY,
            hashes,
            include_details=False,
            include_characteristics=True,
            include_signals=True,
            tagstore_groups=[],
        )
        assert len(result.txs) == 2
        # Signals always computed.
        names = {s.name for s in result.signals}
        assert names == {
            "script_type",
            "witness_present",
            "tx_version",
            "rbf",
            "locktime_pattern",
            "bip69_outputs_sorted",
            "output_count_shape",
            "shared_cluster",
            "exchange_input_overlap",
            "direct_input_overlap",
            "change_chain",
            "common_ancestor",
            "utxo_linkage",
        }
        # Verdict relation must be one of the documented labels.
        assert result.verdict.relation in {
            "linked",
            "likely_linked",
            "inconclusive",
            "likely_unlinked",
            "unlinked",
        }
        assert result.verdict.cluster_verdict in {"same", "different", "unknown"}
        # Characteristics included.
        for item in result.txs:
            assert item.characteristics is not None
            assert item.details is None

    async def test_lineage_populated_from_spending_edges(self):
        svc, hashes = self._make_two_linked_txs()
        result = await compare_txs(
            svc,
            CURRENCY,
            hashes,
            include_details=False,
            include_characteristics=False,
            include_signals=True,
            tagstore_groups=[],
        )
        # tx0 (index 0) spends tx1's (index 1) output 0 via its input 0.
        assert len(result.lineage) == 1
        edge = result.lineage[0]
        assert edge.from_idx == 1
        assert edge.to_idx == 0
        assert edge.kind == "output_spent_by_input"
        assert edge.out_index == 0
        assert edge.in_index == 0

    async def test_case_variant_hash_still_yields_lineage(self):
        # Parent refs come back lowercase from the db. A case-variant
        # compared hash must be canonicalized up front, or the h_to_idx
        # lookup misses and the lineage edge is silently dropped.
        svc, hashes = self._make_two_linked_txs()
        variant = [hashes[0], hashes[1].upper()]
        result = await compare_txs(
            svc,
            CURRENCY,
            variant,
            include_details=False,
            include_characteristics=True,
            include_signals=True,
            tagstore_groups=[],
        )
        assert len(result.lineage) == 1
        assert result.txs[1].tx_hash == hashes[1]  # canonical spelling echoed

    async def test_include_signals_false_suppresses(self):
        svc, hashes = self._make_two_linked_txs()
        result = await compare_txs(
            svc,
            CURRENCY,
            hashes,
            include_details=False,
            include_characteristics=True,
            include_signals=False,
            tagstore_groups=[],
        )
        assert result.signals is None
        # Verdict still computed.
        assert result.verdict is not None

    async def test_include_characteristics_false_suppresses(self):
        svc, hashes = self._make_two_linked_txs()
        result = await compare_txs(
            svc,
            CURRENCY,
            hashes,
            include_details=False,
            include_characteristics=False,
            include_signals=True,
            tagstore_groups=[],
        )
        for item in result.txs:
            assert item.characteristics is None

    async def test_include_details_true_populates(self):
        svc, hashes = self._make_two_linked_txs()
        result = await compare_txs(
            svc,
            CURRENCY,
            hashes,
            include_details=True,
            include_characteristics=False,
            include_signals=False,
            tagstore_groups=[],
        )
        for item in result.txs:
            assert item.details is not None

    async def test_include_lineage_false_suppresses(self):
        # Lineage is still computed (the verdict can use it) but not returned.
        svc, hashes = self._make_two_linked_txs()
        result = await compare_txs(
            svc,
            CURRENCY,
            hashes,
            include_details=False,
            include_characteristics=False,
            include_signals=True,
            include_lineage=False,
            tagstore_groups=[],
        )
        assert result.lineage is None
        # Verdict still computed.
        assert result.verdict is not None

    async def test_include_verdict_false_suppresses(self):
        svc, hashes = self._make_two_linked_txs()
        result = await compare_txs(
            svc,
            CURRENCY,
            hashes,
            include_details=False,
            include_characteristics=False,
            include_signals=True,
            include_verdict=False,
            tagstore_groups=[],
        )
        assert result.verdict is None
        # Signals still returned.
        assert len(result.signals) > 0

    async def test_three_tx_orchestration(self):
        h0, h1, h2 = "aa" * 32, "bb" * 32, "cc" * 32
        ins = [
            make_txvalue(
                "bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3t4", 1_000, has_witness=True
            )
        ]
        outs = [make_txvalue("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 900)]
        tx_map = {
            h: make_tx(tx_hash=h, inputs=ins, outputs=outs, height=100 + i)
            for i, h in enumerate([h0, h1, h2])
        }
        # No spending edges between any of them.
        spending: dict[str, list[TxRef]] = {h0: [], h1: [], h2: []}
        svc = FakeTxsService(
            tx_map=tx_map,
            spending_map=spending,
            addresses_light={
                "bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3t4": {"cluster_id": 1},
                "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa": {"cluster_id": 2},
            },
        )
        result = await compare_txs(
            svc,
            CURRENCY,
            [h0, h1, h2],
            include_details=False,
            include_characteristics=True,
            include_signals=True,
            tagstore_groups=[],
        )
        assert len(result.txs) == 3
        # No edges → utxo_linkage signal must say mismatch.
        link_sig = next(s for s in result.signals if s.name == "utxo_linkage")
        assert link_sig.verdict == "mismatch"
        # No spending edges between compared txs → empty lineage.
        assert result.lineage == []

    async def test_missing_tx_hash_rejected_with_not_found(self):
        # A hash that does not exist (e.g. typo, or a hash that belongs to a
        # different chain and so is absent from this currency's keyspace) must
        # surface as not-found, not a partial summary over the hashes found.
        svc, hashes = self._make_two_linked_txs()
        missing = "ff" * 32
        with pytest.raises(NotFoundException, match=missing):
            await compare_txs(
                svc,
                CURRENCY,
                [hashes[0], missing],  # second hash is unknown
                include_details=False,
                include_characteristics=False,
                include_signals=True,
                tagstore_groups=[],
            )

    async def test_missing_tx_hash_rejected_in_full_analysis(self):
        # Same guarantee on the full-analysis path.
        svc, hashes = self._make_two_linked_txs()
        with pytest.raises(NotFoundException, match="ff" * 32):
            await compare_txs(
                svc,
                CURRENCY,
                ["ff" * 32, hashes[1]],  # first hash is unknown
                include_details=False,
                include_characteristics=True,
                include_signals=True,
                tagstore_groups=[],
            )

    async def test_all_missing_tx_hashes_enumerated_in_not_found(self):
        # The 404 must name EVERY missing hash so the client fixes its
        # request in one round instead of bisecting, and it must fire
        # before any expensive per-io work runs.
        svc, hashes = self._make_two_linked_txs()
        m1, m2 = "ee" * 32, "ff" * 32
        with pytest.raises(NotFoundException, match=f"{m1}, {m2}"):
            await compare_txs(
                svc,
                CURRENCY,
                [hashes[0], m1, m2],
                include_details=False,
                include_characteristics=True,
                include_signals=True,
                tagstore_groups=[],
            )
        assert not any(kw.get("include_io") for kw in svc.get_tx_calls)
        assert svc.spending_calls == 0

    async def test_sub_tx_identifier_rejected(self):
        # Account-model sub-tx identifiers (<hash>_T1) are meaningless on
        # the BTC-only compare and must 400 with a clear message, not 404.
        svc, hashes = self._make_two_linked_txs()
        with pytest.raises(BadUserInputException, match="sub-transaction"):
            await compare_txs(
                svc,
                CURRENCY,
                [hashes[0], hashes[1] + "_T1"],
                include_details=False,
                include_characteristics=False,
                include_signals=True,
                tagstore_groups=[],
            )

    async def test_duplicate_hashes_collapse_to_one_rejected(self):
        # The same hash repeated is a single distinct tx; with nothing to
        # compare it against, the request must be rejected (not a self-link).
        svc, hashes = self._make_two_linked_txs()
        with pytest.raises(BadUserInputException):
            await compare_txs(
                svc,
                CURRENCY,
                [hashes[0], hashes[0]],
                include_details=False,
                include_characteristics=True,
                include_signals=True,
                tagstore_groups=[],
            )

    async def test_case_variant_duplicate_collapses_to_one_rejected(self):
        # Hex hashes compare case-insensitively downstream, so an uppercase
        # spelling of the same hash is the same tx: nothing to compare.
        svc, hashes = self._make_two_linked_txs()
        with pytest.raises(BadUserInputException):
            await compare_txs(
                svc,
                CURRENCY,
                [hashes[0], hashes[0].upper()],
                include_details=False,
                include_characteristics=True,
                include_signals=True,
                tagstore_groups=[],
            )

    async def test_duplicate_hashes_deduped_before_compare(self):
        # An over-specified list (a hash repeated alongside distinct ones)
        # dedups order-preserving: the summary counts each tx once and each tx
        # is fetched once, so values are not double-counted.
        svc, hashes = self._make_two_linked_txs()
        result = await compare_txs(
            svc,
            CURRENCY,
            [hashes[0], hashes[1], hashes[0]],  # hashes[0] repeated
            include_details=False,
            include_characteristics=False,
            include_signals=True,
            tagstore_groups=[],
        )
        assert len(result.txs) == 2
        # Each distinct hash fetched once per phase (header + full IO); the
        # duplicate ref adds no DB work.
        assert len(svc.get_tx_calls) == 4
        assert sum(1 for kw in svc.get_tx_calls if kw.get("include_io")) == 2
        # The full fetch must include nonstandard I/Os (OP_RETURN outputs
        # are part of the fingerprint).
        assert all(
            kw.get("include_nonstandard_io")
            for kw in svc.get_tx_calls
            if kw.get("include_io")
        )

    @pytest.mark.parametrize(
        "currency", ["eth", "trx", "ETH", "TRX", "bch", "ltc", "zec"]
    )
    async def test_non_btc_rejected(self, currency):
        # /graph/compare is BTC-only; other chains use /graph/summary.
        svc = FakeTxsService(tx_map={})
        with pytest.raises(BadUserInputException):
            await compare_txs(
                svc,
                currency,
                ["aa" * 32, "bb" * 32],
                include_details=False,
                include_characteristics=True,
                include_signals=True,
                tagstore_groups=[],
            )

    async def test_compare_accepts_io_set_at_exact_limit(self):
        # The ref-count cap bounds list length, not work: the address and
        # cluster prefetches scale with the IO count of the fetched txs.
        # Exactly _MAX_TOTAL_IOS combined inputs/outputs must still pass.
        svc, hashes = self._make_two_linked_txs()
        # Base fixture already contributes 1 output (tx0) + 1 input + 1
        # output (tx1) = 3 ios outside the field under test.
        svc._tx_map[hashes[0]].no_inputs = _MAX_TOTAL_IOS - 3
        result = await compare_txs(
            svc,
            CURRENCY,
            hashes,
            include_details=False,
            include_characteristics=True,
            include_signals=True,
            tagstore_groups=[],
        )
        assert len(result.txs) == 2

    async def test_compare_rejects_io_set_over_limit(self):
        svc, hashes = self._make_two_linked_txs()
        svc._tx_map[hashes[0]].no_inputs = _MAX_TOTAL_IOS - 2
        with pytest.raises(BadUserInputException):
            await compare_txs(
                svc,
                CURRENCY,
                hashes,
                include_details=False,
                include_characteristics=True,
                include_signals=True,
                tagstore_groups=[],
            )
        # Rejection happens on the header point reads, before the full
        # IO/heuristics fetch and any of the expensive prefetches run.
        assert not any(kw.get("include_io") for kw in svc.get_tx_calls)
        assert svc.spending_calls == 0
        svc.db.get_addresses_light.assert_not_awaited()
