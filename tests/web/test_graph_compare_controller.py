"""Tests for `POST /graph/compare`.

The controller is exercised end-to-end through the FastAPI test client.
We monkeypatch the DB-layer function
``graphsenselib.web.service.graph_service._db_compare_txs`` so the test
does not depend on Cassandra fixtures or DB-layer wiring. The real web
service ``compare`` (network validation, include expansion, translation)
runs unpatched.

CRITICAL: tests do NOT pin specific values for `weight`, `confidence`,
or `score_total`. Those are tentative and not yet calibrated.
"""

import pytest

from graphsenselib.db.asynchronous.services.models import (
    ComparisonSignalInternal,
    ComparisonVerdictInternal,
    TransactionComparisonInternal,
    TxCharacteristicsInternal,
    TxComparedItemInternal,
)
from tests.web.helpers import request_with_status


SIGNAL_KINDS = {"discriminator", "score", "linkage"}
SIGNAL_VERDICTS = {"match", "mismatch", "inconclusive"}
RELATIONS = {
    "linked",
    "likely_linked",
    "inconclusive",
    "likely_unlinked",
    "unlinked",
}
CLUSTER_VERDICTS = {"same", "different", "unknown"}


HASH_A = "ab1880"
HASH_B = "ab188013"


def _build_internal_response(
    include_characteristics: bool,
    include_details: bool,
    include_signals: bool,
    include_lineage: bool,
    include_verdict: bool,
) -> TransactionComparisonInternal:
    """Return a hand-built internal response that the real translator maps
    to the API model. Numeric values are arbitrary placeholders, tests must
    not pin them. Every compared item carries ``network="btc"``."""
    chars_a = (
        TxCharacteristicsInternal(
            inputs_script_types=["P2WPKH"],
            outputs_script_types=["P2PKH"],
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
        if include_characteristics
        else None
    )
    chars_b = (
        TxCharacteristicsInternal(
            inputs_script_types=["P2PKH", "P2WPKH"],
            outputs_script_types=["P2PKH"],
            n_inputs=2,
            n_outputs=1,
            total_input_sat=3000,
            total_output_sat=2500,
            fee_sat=500,
            tx_version=2,
            locktime=0,
            input_cluster_ids=[42],
            coinjoin_detected=False,
            coinjoin_protocol=None,
        )
        if include_characteristics
        else None
    )

    items = [
        TxComparedItemInternal(
            tx_hash=HASH_A, network="btc", characteristics=chars_a, details=None
        ),
        TxComparedItemInternal(
            tx_hash=HASH_B, network="btc", characteristics=chars_b, details=None
        ),
    ]

    signals = []
    if include_signals:
        signals = [
            ComparisonSignalInternal(
                name="script_type",
                kind="discriminator",
                per_tx=["P2WPKH", "P2PKH,P2WPKH"],
                verdict="mismatch",
                weight=2,
            ),
            ComparisonSignalInternal(
                name="shared_cluster",
                kind="linkage",
                per_tx=["42", "42"],
                verdict="match",
                weight=5,
            ),
        ]

    verdict = (
        ComparisonVerdictInternal(
            relation="likely_linked",
            confidence=73,
            cluster_verdict="same",
            discriminator_hits=["shared_cluster"],
            score_total=1.25,
            notes=[],
        )
        if include_verdict
        else None
    )

    return TransactionComparisonInternal(
        txs=items,
        signals=signals,
        lineage=[],
        verdict=verdict,
    )


@pytest.fixture
def patch_compare(monkeypatch):
    """Monkeypatch the DB-layer ``_db_compare_txs`` to avoid DB calls.

    The web service ``compare`` calls ``_db_compare_txs(...)`` imported into
    ``graphsenselib.web.service.graph_service``. Patching the attribute on
    that module records the include_* kwargs the real service computed from
    the request's ``include`` list, and returns a canned internal response
    that the real translator maps to the API model."""
    state = {"calls": []}

    async def _fake_compare_txs(
        txs_service,
        currency,
        tx_hashes,
        include_details,
        include_characteristics,
        include_signals,
        tagstore_groups,
        include_lineage=True,
        include_verdict=True,
    ):
        state["calls"].append(
            {
                "currency": currency,
                "tx_hashes": list(tx_hashes),
                "include_characteristics": include_characteristics,
                "include_details": include_details,
                "include_signals": include_signals,
                "include_lineage": include_lineage,
                "include_verdict": include_verdict,
            }
        )
        return _build_internal_response(
            include_characteristics=include_characteristics,
            include_details=include_details,
            include_signals=include_signals,
            include_lineage=include_lineage,
            include_verdict=include_verdict,
        )

    monkeypatch.setattr(
        "graphsenselib.web.service.graph_service._db_compare_txs",
        _fake_compare_txs,
    )
    return state


def _body(*hashes, network="btc", include=None):
    body = {"txs": [{"tx_hash": h, "network": network} for h in hashes]}
    if include is not None:
        body["include"] = include
    return body


def test_compare_happy_path(client, patch_compare):
    body = _body(HASH_A, HASH_B)
    result = request_with_status(client, "/graph/compare", 200, body=body)

    assert "txs" in result
    assert "signals" in result
    assert "verdict" in result
    # summary lives at POST /graph/summary
    assert "summary" not in result

    assert len(result["txs"]) == 2
    assert result["txs"][0]["tx_hash"] == HASH_A
    assert result["txs"][1]["tx_hash"] == HASH_B

    for s in result["signals"]:
        assert s["kind"] in SIGNAL_KINDS
        assert s["verdict"] in SIGNAL_VERDICTS
    assert result["verdict"]["relation"] in RELATIONS
    assert result["verdict"]["cluster_verdict"] in CLUSTER_VERDICTS

    assert len(patch_compare["calls"]) == 1
    call = patch_compare["calls"][0]
    assert call["currency"] == "btc"
    assert call["tx_hashes"] == [HASH_A, HASH_B]


def test_compare_non_btc_network_is_400(client, monkeypatch):
    # The web service rejects mixed/non-btc networks before calling the DB
    # layer, so no patching of the DB compare is needed here.
    body = {
        "txs": [
            {"tx_hash": "aa11", "network": "btc"},
            {"tx_hash": "bb22", "network": "eth"},
        ]
    }
    request_with_status(client, "/graph/compare", 400, body=body)


def test_compare_needs_two_refs_is_422(client):
    body = {"txs": [{"tx_hash": "aa11", "network": "btc"}]}
    request_with_status(client, "/graph/compare", 422, body=body)


def test_compare_items_echo_network(client, patch_compare):
    body = {
        "txs": [
            {"tx_hash": "aa11", "network": "btc"},
            {"tx_hash": "bb22", "network": "btc"},
        ]
    }
    result = request_with_status(client, "/graph/compare", 200, body=body)
    assert all(item["network"] == "btc" for item in result["txs"])


def test_compare_include_defaults_exclude_details(client, patch_compare):
    body = {
        "txs": [
            {"tx_hash": "aa11", "network": "btc"},
            {"tx_hash": "bb22", "network": "btc"},
        ]
    }
    request_with_status(client, "/graph/compare", 200, body=body)
    call = patch_compare["calls"][0]
    assert call["include_details"] is False
    assert call["include_signals"] is True
    assert call["include_characteristics"] is True
    assert call["include_lineage"] is True
    assert call["include_verdict"] is True


def test_compare_include_all_adds_details(client, patch_compare):
    body = _body(HASH_A, HASH_B, include=["all"])
    request_with_status(client, "/graph/compare", 200, body=body)
    call = patch_compare["calls"][0]
    assert call["include_characteristics"] is True
    assert call["include_details"] is True
    assert call["include_signals"] is True
    assert call["include_lineage"] is True
    assert call["include_verdict"] is True


def test_compare_include_subset_only(client, patch_compare):
    """An explicit subset turns everything else off."""
    body = _body(HASH_A, HASH_B, include=["signals", "verdict"])
    result = request_with_status(client, "/graph/compare", 200, body=body)
    call = patch_compare["calls"][0]
    assert call["include_signals"] is True
    assert call["include_verdict"] is True
    assert call["include_characteristics"] is False
    assert call["include_details"] is False
    assert call["include_lineage"] is False

    for item in result["txs"]:
        assert item.get("characteristics") is None
        assert item.get("details") is None
    assert "verdict" in result
    assert len(result["signals"]) > 0


def test_compare_include_verdict_only_omits_signals(client, patch_compare):
    body = _body(HASH_A, HASH_B, include=["verdict"])
    result = request_with_status(client, "/graph/compare", 200, body=body)
    assert result["signals"] == []
    assert "verdict" in result


def test_compare_rejects_unknown_include_value(client, patch_compare):
    body = _body(HASH_A, HASH_B, include=["bogus"])
    request_with_status(client, "/graph/compare", 422, body=body)


def test_compare_too_many_refs_is_422(client, patch_compare):
    body = {"txs": [{"tx_hash": f"h{i}", "network": "btc"} for i in range(101)]}
    request_with_status(client, "/graph/compare", 422, body=body)


def test_compare_include_signals_does_not_change_verdict(client, patch_compare):
    """Dropping signals from the include list only controls serialization, the
    verdict must be identical regardless of whether signals are returned."""
    with_signals = request_with_status(
        client,
        "/graph/compare",
        200,
        body=_body(HASH_A, HASH_B, include=["signals", "verdict"]),
    )
    without_signals = request_with_status(
        client,
        "/graph/compare",
        200,
        body=_body(HASH_A, HASH_B, include=["verdict"]),
    )

    assert len(with_signals["signals"]) > 0
    assert without_signals["signals"] == []

    assert with_signals["verdict"]["relation"] == without_signals["verdict"]["relation"]
    assert (
        with_signals["verdict"]["cluster_verdict"]
        == without_signals["verdict"]["cluster_verdict"]
    )
    assert (
        with_signals["verdict"]["discriminator_hits"]
        == without_signals["verdict"]["discriminator_hits"]
    )
    assert with_signals["verdict"]["notes"] == without_signals["verdict"]["notes"]
