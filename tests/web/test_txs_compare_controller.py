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
    ComparisonSummary,
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
    include_signals: bool,
    include_characteristics: bool,
    include_details: bool,
    include_analysis: bool = True,
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
    if include_signals and include_analysis:
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

    summary = ComparisonSummary(
        tx_count=2,
        currency="btc",
        total_value=3500,
        total_fee=200,
        total_inputs=3,
        total_outputs=2,
        block_min=100,
        block_max=101,
        timestamp_min=1_700_000_000,
        timestamp_max=1_700_000_500,
    )
    verdict = (
        ComparisonVerdict(
            relation="likely_linked",
            confidence=73,
            cluster_verdict="same",
            discriminator_hits=["shared_cluster"],
            score_total=1.25,
            notes=[],
        )
        if include_analysis
        else None
    )

    # Suppress details if not requested (they would be None anyway,
    # but keep parity with translator output).
    if not include_details:
        for it in items:
            it.details = None

    return TransactionComparison(
        txs=items,
        signals=signals,
        lineage=[],
        summary=summary,
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
        include_details,
        include_characteristics,
        include_signals,
        include_analysis,
    ):
        state["calls"].append(
            {
                "currency": currency,
                "tx_hashes": list(tx_hashes),
                "include_details": include_details,
                "include_characteristics": include_characteristics,
                "include_signals": include_signals,
                "include_analysis": include_analysis,
            }
        )
        if currency in ("eth", "trx"):
            raise BadUserInputException(
                f"/txs/compare is UTXO-only; '{currency}' is account-based."
            )
        return _build_api_response(
            include_signals=include_signals,
            include_characteristics=include_characteristics,
            include_details=include_details,
            include_analysis=include_analysis,
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
    assert "summary" in result
    assert "verdict" in result

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


def test_compare_txs_eth_returns_400(client, patch_compare):
    path = f"/eth/txs/compare?tx_hash={HASH_A}&tx_hash={HASH_B}"
    status, body = raw_request(client, path)
    assert status == 400
    assert "UTXO-only" in body or "account-based" in body


def test_compare_txs_minimal_payload_excludes_optionals(client, patch_compare):
    path = (
        f"/btc/txs/compare?tx_hash={HASH_A}&tx_hash={HASH_B}"
        "&include_details=false"
        "&include_characteristics=false"
        "&include_signals=false"
    )
    result = get_json(client, path)

    # signals empty
    assert result["signals"] == []

    # per-tx characteristics and details are absent (response_model_exclude_none)
    for item in result["txs"]:
        assert item.get("characteristics") is None
        assert item.get("details") is None

    # summary + verdict still present
    assert "summary" in result
    assert "verdict" in result
    assert result["verdict"]["relation"] in RELATIONS
    assert result["verdict"]["cluster_verdict"] in CLUSTER_VERDICTS


def test_compare_txs_summary_only_omits_verdict(client, patch_compare):
    path = (
        f"/btc/txs/compare?tx_hash={HASH_A}&tx_hash={HASH_B}"
        "&include_analysis=false"
    )
    result = get_json(client, path)

    # verdict and signals are absent in summary-only mode
    assert "verdict" not in result
    assert result["signals"] == []
    # summary still present
    assert "summary" in result
    assert result["summary"]["tx_count"] == 2

    # service received include_analysis=False; default elsewhere is True
    call = patch_compare["calls"][0]
    assert call["include_analysis"] is False


def test_compare_txs_include_analysis_defaults_true(client, patch_compare):
    path = f"/btc/txs/compare?tx_hash={HASH_A}&tx_hash={HASH_B}"
    get_json(client, path)
    assert patch_compare["calls"][0]["include_analysis"] is True


def test_compare_txs_include_signals_does_not_change_verdict(client, patch_compare):
    """include_signals only controls serialization, the verdict must
    be identical regardless of whether signals are returned."""
    base = f"/btc/txs/compare?tx_hash={HASH_A}&tx_hash={HASH_B}"

    with_signals = get_json(client, base + "&include_signals=true")
    without_signals = get_json(client, base + "&include_signals=false")

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
    # Identity of the numeric values across the two flag settings (not pinned to specific values)
    assert (
        with_signals["verdict"]["confidence"]
        == without_signals["verdict"]["confidence"]
    )
    assert (
        with_signals["verdict"]["score_total"]
        == without_signals["verdict"]["score_total"]
    )
