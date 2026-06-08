"""Tests for `GET /txs/compare`.

The controller is exercised end-to-end through the FastAPI test client.
We monkeypatch the **service-layer** function
``graphsenselib.web.service.txs_service.compare_txs`` so the test does
not depend on Cassandra fixtures or DB-layer wiring.

CRITICAL: tests do NOT pin specific values for `weight`, `confidence`,
or `score_total`. Those are tentative and not yet calibrated.
"""

import pytest

from graphsenselib.errors import BadUserInputException
from graphsenselib.web.models import (
    ComparisonSignal,
    ComparisonVerdict,
    TransactionComparison,
    TxCharacteristics,
    TxComparedItem,
)
from tests.web.helpers import get_json, raw_request


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


def _build_api_response(
    include_characteristics: bool,
    include_details: bool,
    include_signals: bool,
    include_lineage: bool,
    include_verdict: bool,
) -> TransactionComparison:
    """Return a hand-built API response that mirrors what the translator
    would emit. Numeric values are arbitrary placeholders, tests must
    not pin them."""
    chars_a = (
        TxCharacteristics(
            input_script_types=["P2WPKH"],
            output_script_types=["P2PKH"],
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
        TxCharacteristics(
            input_script_types=["P2PKH", "P2WPKH"],
            output_script_types=["P2PKH"],
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
        TxComparedItem(tx_hash=HASH_A, characteristics=chars_a, details=None),
        TxComparedItem(tx_hash=HASH_B, characteristics=chars_b, details=None),
    ]

    signals = []
    if include_signals:
        signals = [
            ComparisonSignal(
                name="script_type",
                kind="discriminator",
                per_tx=["P2WPKH", "P2PKH,P2WPKH"],
                verdict="mismatch",
                weight=2,
            ),
            ComparisonSignal(
                name="shared_cluster",
                kind="linkage",
                per_tx=["42", "42"],
                verdict="match",
                weight=5,
            ),
        ]

    verdict = (
        ComparisonVerdict(
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

    return TransactionComparison(
        txs=items,
        signals=signals,
        lineage=[],
        verdict=verdict,
    )


@pytest.fixture
def patch_compare(monkeypatch):
    """Monkeypatch the service-layer compare_txs to avoid DB calls.

    The route calls ``service.compare_txs(...)`` where ``service`` is
    the module ``graphsenselib.web.service.txs_service`` imported via
    ``import ... as service``. Patching the attribute on the source
    module is picked up because the route resolves the attribute at
    call time."""
    state = {"calls": []}

    async def _fake_compare_txs(
        ctx,
        currency,
        tx_hashes,
        include_characteristics,
        include_details,
        include_signals,
        include_lineage,
        include_verdict,
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
        if currency != "btc":
            raise BadUserInputException(
                f"/txs/compare is BTC-only; '{currency}' is not supported."
            )
        return _build_api_response(
            include_characteristics=include_characteristics,
            include_details=include_details,
            include_signals=include_signals,
            include_lineage=include_lineage,
            include_verdict=include_verdict,
        )

    monkeypatch.setattr(
        "graphsenselib.web.service.txs_service.compare_txs",
        _fake_compare_txs,
    )
    return state


def test_compare_txs_happy_path(client, patch_compare):
    path = f"/btc/txs/compare?tx_hash={HASH_A}&tx_hash={HASH_B}"
    result = get_json(client, path)

    assert "txs" in result
    assert "signals" in result
    assert "verdict" in result
    # summary moved to /{currency}/subgraph/summary
    assert "summary" not in result

    assert len(result["txs"]) == 2
    assert result["txs"][0]["tx_hash"] == HASH_A
    assert result["txs"][1]["tx_hash"] == HASH_B

    # categorical-only assertions
    for s in result["signals"]:
        assert s["kind"] in SIGNAL_KINDS
        assert s["verdict"] in SIGNAL_VERDICTS
    assert result["verdict"]["relation"] in RELATIONS
    assert result["verdict"]["cluster_verdict"] in CLUSTER_VERDICTS

    # service was called once with the right arguments
    assert len(patch_compare["calls"]) == 1
    call = patch_compare["calls"][0]
    assert call["currency"] == "btc"
    assert call["tx_hashes"] == [HASH_A, HASH_B]


def test_compare_txs_default_include_set(client, patch_compare):
    """Omitting ``include`` yields the default set: characteristics, signals,
    lineage, verdict; details excluded."""
    get_json(client, f"/btc/txs/compare?tx_hash={HASH_A}&tx_hash={HASH_B}")
    call = patch_compare["calls"][0]
    assert call["include_characteristics"] is True
    assert call["include_signals"] is True
    assert call["include_lineage"] is True
    assert call["include_verdict"] is True
    assert call["include_details"] is False


def test_compare_txs_include_all_adds_details(client, patch_compare):
    get_json(client, f"/btc/txs/compare?tx_hash={HASH_A}&tx_hash={HASH_B}&include=all")
    call = patch_compare["calls"][0]
    assert call["include_characteristics"] is True
    assert call["include_details"] is True
    assert call["include_signals"] is True
    assert call["include_lineage"] is True
    assert call["include_verdict"] is True


def test_compare_txs_include_subset_only(client, patch_compare):
    """An explicit subset turns everything else off."""
    path = (
        f"/btc/txs/compare?tx_hash={HASH_A}&tx_hash={HASH_B}"
        "&include=signals&include=verdict"
    )
    result = get_json(client, path)
    call = patch_compare["calls"][0]
    assert call["include_signals"] is True
    assert call["include_verdict"] is True
    assert call["include_characteristics"] is False
    assert call["include_details"] is False
    assert call["include_lineage"] is False

    # per-tx characteristics absent (response_model_exclude_none); verdict present
    for item in result["txs"]:
        assert item.get("characteristics") is None
        assert item.get("details") is None
    assert "verdict" in result
    assert len(result["signals"]) > 0


def test_compare_txs_include_verdict_only_omits_signals(client, patch_compare):
    path = f"/btc/txs/compare?tx_hash={HASH_A}&tx_hash={HASH_B}&include=verdict"
    result = get_json(client, path)
    assert result["signals"] == []
    assert "verdict" in result


def test_compare_txs_rejects_unknown_include_value(client, patch_compare):
    path = f"/btc/txs/compare?tx_hash={HASH_A}&tx_hash={HASH_B}&include=bogus"
    status, _ = raw_request(client, path)
    assert status == 422


def test_compare_txs_zero_hashes_returns_422(client, patch_compare):
    status, _ = raw_request(client, "/btc/txs/compare")
    assert status == 422


def test_compare_txs_one_hash_returns_422(client, patch_compare):
    status, _ = raw_request(client, f"/btc/txs/compare?tx_hash={HASH_A}")
    assert status == 422


def test_compare_txs_too_many_hashes_returns_422(client, patch_compare):
    qs = "&".join(f"tx_hash=h{i}" for i in range(101))
    status, _ = raw_request(client, f"/btc/txs/compare?{qs}")
    assert status == 422


@pytest.mark.parametrize("currency", ["eth", "trx", "bch", "ltc", "zec"])
def test_compare_txs_non_btc_returns_400(client, patch_compare, currency):
    path = f"/{currency}/txs/compare?tx_hash={HASH_A}&tx_hash={HASH_B}"
    status, body = raw_request(client, path)
    assert status == 400
    assert "BTC-only" in body


def test_compare_txs_include_signals_does_not_change_verdict(client, patch_compare):
    """Dropping signals from the include list only controls serialization, the
    verdict must be identical regardless of whether signals are returned."""
    base = f"/btc/txs/compare?tx_hash={HASH_A}&tx_hash={HASH_B}"

    with_signals = get_json(
        client, base + "&include=signals&include=verdict"
    )
    without_signals = get_json(client, base + "&include=verdict")

    # Non-empty vs. empty signal list
    assert len(with_signals["signals"]) > 0
    assert without_signals["signals"] == []

    # Verdict is structurally identical (categorical labels + lists)
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
