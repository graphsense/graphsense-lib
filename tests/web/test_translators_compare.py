"""Tests for `to_api_transaction_comparison` translator.

The uncalibrated numerics (`weight`, `confidence`, `score_total`) are
backend-only: the API must not carry them at all. We only assert
categorical labels, structural shape, and field-level round-trip
identity of the exposed fields.
"""

from typing import Optional

from graphsenselib.db.asynchronous.services.models import (
    ComparisonSignalInternal,
    ComparisonVerdictInternal,
    LineageEdgeInternal,
    TransactionComparisonInternal,
    TxCharacteristicsInternal,
    TxComparedItemInternal,
    TxUtxo,
    TxValue,
    Values,
)
from graphsenselib.web.translators import to_api_transaction_comparison


SIGNAL_VERDICTS = {"match", "mismatch", "inconclusive"}
SIGNAL_KINDS = {"discriminator", "score", "linkage"}
RELATIONS = {
    "linked",
    "likely_linked",
    "inconclusive",
    "likely_unlinked",
    "unlinked",
}
CLUSTER_VERDICTS = {"same", "different", "unknown"}


def _make_values(value: int = 1000) -> Values:
    return Values(value=value, fiat_values=[])


def _make_tx_utxo(tx_hash: str = "aa11", height: int = 100) -> TxUtxo:
    return TxUtxo(
        currency="btc",
        tx_hash=tx_hash,
        coinbase=False,
        height=height,
        no_inputs=1,
        no_outputs=1,
        inputs=[TxValue(address=["addrA"], value=_make_values(1500))],
        outputs=[TxValue(address=["addrB"], value=_make_values(1000))],
        timestamp=1_700_000_000,
        total_input=_make_values(1500),
        total_output=_make_values(1000),
    )


def _make_characteristics(
    inputs_script_types: Optional[list] = None,
    outputs_script_types: Optional[list] = None,
) -> TxCharacteristicsInternal:
    return TxCharacteristicsInternal(
        inputs_script_types=inputs_script_types
        if inputs_script_types is not None
        else ["P2WPKH"],
        outputs_script_types=outputs_script_types
        if outputs_script_types is not None
        else ["P2PKH"],
        inputs_have_witness=True,
        n_inputs=1,
        n_outputs=1,
        total_input_sat=1500,
        total_output_sat=1000,
        fee_sat=500,
        tx_version=2,
        locktime=0,
        input_cluster_ids=[42],
        coinjoin_detected=False,
        coinjoin_protocol=None,
    )


def _make_verdict(
    relation: str = "likely_linked",
    confidence: int = 73,
    cluster_verdict: str = "same",
    score_total: float = 1.25,
    discriminator_hits: Optional[list] = None,
    linkage_hits: Optional[list] = None,
    notes: Optional[list] = None,
) -> ComparisonVerdictInternal:
    return ComparisonVerdictInternal(
        relation=relation,
        confidence=confidence,
        cluster_verdict=cluster_verdict,
        discriminator_hits=discriminator_hits or ["shared_cluster"],
        linkage_hits=linkage_hits or ["shared_cluster"],
        score_total=score_total,
        notes=notes
        or [
            {
                "code": "shared_cluster_support",
                "message": "All compared txs share at least one input cluster.",
            }
        ],
    )


def _make_full_internal(
    item_overrides: Optional[list] = None,
    signals: Optional[list] = None,
    lineage: Optional[list] = None,
    verdict: Optional[ComparisonVerdictInternal] = None,
) -> TransactionComparisonInternal:
    if item_overrides is None:
        items = [
            TxComparedItemInternal(
                tx_hash="aa11",
                characteristics=_make_characteristics(),
                details=_make_tx_utxo("aa11", height=100),
            ),
            TxComparedItemInternal(
                tx_hash="bb22",
                characteristics=_make_characteristics(
                    inputs_script_types=["P2PKH", "P2WPKH"],
                    outputs_script_types=[],
                ),
                details=_make_tx_utxo("bb22", height=101),
            ),
        ]
    else:
        items = item_overrides

    if signals is None:
        signals = [
            ComparisonSignalInternal(
                name="script_type",
                kind="discriminator",
                per_tx=[["P2WPKH"], ["P2PKH", "P2WPKH"]],
                verdict="mismatch",
                weight=2,
            ),
            ComparisonSignalInternal(
                name="shared_cluster",
                kind="linkage",
                per_tx=[[42], [42]],
                verdict="match",
                weight=5,
            ),
        ]

    if lineage is None:
        lineage = [
            LineageEdgeInternal(
                from_idx=0,
                to_idx=1,
                kind="output_spent_by_input",
                out_index=0,
                in_index=0,
            )
        ]

    return TransactionComparisonInternal(
        txs=items,
        signals=signals,
        lineage=lineage,
        verdict=verdict or _make_verdict(),
    )


def test_to_api_transaction_comparison_full_payload():
    internal = _make_full_internal()

    api = to_api_transaction_comparison(internal)

    assert api is not None
    assert len(api.txs) == 2
    # tx 0: passes the script-type lists through, sorted
    assert api.txs[0].tx_hash == "aa11"
    assert api.txs[0].characteristics is not None
    assert api.txs[0].characteristics.input_script_types == ["P2WPKH"]
    assert api.txs[0].characteristics.output_script_types == ["P2PKH"]
    assert api.txs[0].details is not None
    assert api.txs[0].details.tx_hash == "aa11"

    # tx 1: heterogeneous list -> sorted; empty -> []
    assert api.txs[1].characteristics is not None
    assert api.txs[1].characteristics.input_script_types == ["P2PKH", "P2WPKH"]
    assert api.txs[1].characteristics.output_script_types == []

    # categorical labels
    for s in api.signals:
        assert s.kind in SIGNAL_KINDS
        assert s.verdict in SIGNAL_VERDICTS
    assert api.verdict.relation in RELATIONS
    assert api.verdict.cluster_verdict in CLUSTER_VERDICTS

    # length preservation
    assert len(api.signals) == len(internal.signals)
    assert len(api.lineage) == len(internal.lineage)


def test_to_api_transaction_comparison_script_types_sorted():
    # Out-of-order list should be sorted alphabetically.
    internal = _make_full_internal(
        item_overrides=[
            TxComparedItemInternal(
                tx_hash="cc33",
                characteristics=_make_characteristics(
                    inputs_script_types=["P2WPKH", "P2PKH", "P2SH"],
                    outputs_script_types=["P2PKH"],
                ),
                details=None,
            ),
            TxComparedItemInternal(
                tx_hash="dd44",
                characteristics=_make_characteristics(
                    inputs_script_types=[],
                    outputs_script_types=["P2WPKH", "P2PKH"],
                ),
                details=None,
            ),
        ]
    )

    api = to_api_transaction_comparison(internal)

    # sorted alphabetically
    assert api.txs[0].characteristics.input_script_types == ["P2PKH", "P2SH", "P2WPKH"]
    # single-element still works
    assert api.txs[0].characteristics.output_script_types == ["P2PKH"]
    # empty stays empty
    assert api.txs[1].characteristics.input_script_types == []
    # two distinct -> sorted
    assert api.txs[1].characteristics.output_script_types == ["P2PKH", "P2WPKH"]


def test_to_api_transaction_comparison_include_characteristics_false():
    # When include_characteristics=False the service zeroes out the field.
    internal = _make_full_internal(
        item_overrides=[
            TxComparedItemInternal(
                tx_hash="aa11",
                characteristics=None,
                details=_make_tx_utxo("aa11"),
            ),
            TxComparedItemInternal(
                tx_hash="bb22",
                characteristics=None,
                details=_make_tx_utxo("bb22"),
            ),
        ]
    )

    api = to_api_transaction_comparison(internal)

    for item in api.txs:
        assert item.characteristics is None
        assert item.details is not None


def test_to_api_transaction_comparison_include_details_false():
    internal = _make_full_internal(
        item_overrides=[
            TxComparedItemInternal(
                tx_hash="aa11",
                characteristics=_make_characteristics(),
                details=None,
            ),
            TxComparedItemInternal(
                tx_hash="bb22",
                characteristics=_make_characteristics(),
                details=None,
            ),
        ]
    )

    api = to_api_transaction_comparison(internal)

    for item in api.txs:
        assert item.details is None
        assert item.characteristics is not None


def test_to_api_transaction_comparison_verdict_round_trip():
    # Round-trip identity for the exposed fields: whatever value is on the
    # internal model must appear on the API model unchanged. We don't
    # assert specific values, just that they are preserved — and that the
    # backend-only numerics are dropped.
    internal_verdict = _make_verdict(
        relation="inconclusive",
        confidence=42,
        cluster_verdict="unknown",
        score_total=0.875,
        discriminator_hits=["script_type", "shared_cluster"],
        linkage_hits=["common_ancestor", "shared_cluster"],
        notes=[
            {"code": "coinjoin_detected", "message": "a"},
            {"code": "exchange_overlap_demotion", "message": "b"},
        ],
    )
    internal = _make_full_internal(verdict=internal_verdict)

    api = to_api_transaction_comparison(internal)

    assert api.verdict.relation == internal_verdict.relation
    assert api.verdict.cluster_verdict == internal_verdict.cluster_verdict
    assert api.verdict.discriminator_hits == internal_verdict.discriminator_hits
    assert api.verdict.linkage_hits == internal_verdict.linkage_hits
    assert [(n.code, n.message) for n in api.verdict.notes] == [
        (n.code, n.message) for n in internal_verdict.notes
    ]
    # confidence / score_total are backend-only until calibrated: the API
    # model must not carry them — and neither is the per-signal weight.
    assert not hasattr(api.verdict, "confidence")
    assert not hasattr(api.verdict, "score_total")
    assert all(not hasattr(sig, "weight") for sig in api.signals)

    # categorical sanity
    assert api.verdict.relation in RELATIONS
    assert api.verdict.cluster_verdict in CLUSTER_VERDICTS


def test_to_api_transaction_comparison_signals_and_lineage_length_preserved():
    signals = [
        ComparisonSignalInternal(
            name=f"sig_{i}",
            kind="score",
            per_tx=["x", "y"],
            verdict="match",
            weight=i,
        )
        for i in range(5)
    ]
    lineage = [
        LineageEdgeInternal(
            from_idx=0,
            to_idx=1,
            kind="output_spent_by_input",
            out_index=i,
            in_index=i,
        )
        for i in range(3)
    ]

    internal = _make_full_internal(signals=signals, lineage=lineage)

    api = to_api_transaction_comparison(internal)

    assert len(api.signals) == 5
    assert len(api.lineage) == 3
    # field-level round-trip on signals (names + kinds preserved, no number assertions)
    for src, dst in zip(internal.signals, api.signals):
        assert src.name == dst.name
        assert src.kind == dst.kind
        assert src.verdict == dst.verdict
        assert src.per_tx == dst.per_tx
    for src, dst in zip(internal.lineage, api.lineage):
        assert src.from_idx == dst.from_idx
        assert src.to_idx == dst.to_idx
        assert src.kind == dst.kind
        assert src.out_index == dst.out_index
        assert src.in_index == dst.in_index


def test_to_api_transaction_comparison_verdict_none_omitted():
    # Excluding the verdict from the include list produces verdict=None; the
    # translator must pass that through as None so the API drops it.
    internal = _make_full_internal().model_copy(update={"verdict": None})

    api = to_api_transaction_comparison(internal)

    assert api.verdict is None
