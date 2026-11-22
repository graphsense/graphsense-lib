"""An eth Relay fill through GET /eth/txs/{tx}/conversions.

A fill logs nothing of Relay's, so graphsense-lib has no half bridge for it.
A tx whose calldata has a fill's shape (defi/bridging/halves/relay.py) is a
possible fill: the conversions route asks the external backend, which knows
Relay's solver wallets and Relay's record of the order, and takes its row. A
refund has the same shape and answers no bridge row.

The txs and the backend's rows are the recorded ones of
tests/defi/data/bridges/relay/eth_fills.json: the adapter's own answers for
these eth txs, equal to its live answers from each deposit's chain.
"""

import httpx
import pytest

from tests.defi.test_half_relay import ETH_TXS, tx_row
from tests.web.test_delegated_conversions import (
    BACKEND_URL,
    DECLINED_HEADER,
    GATED,
    ROLES,
    make_client,
)

FILLS = ETH_TXS["fills"]
PAIRED = sorted(name for name, tx in FILLS.items() if tx["adapter_rows"])
NO_BRIDGE = sorted(name for name, tx in FILLS.items() if not tx["adapter_rows"])


def ask(recorded, answer=None, suffix="", **client):
    """(response, the backend's requests) of one conversions request for a
    recorded eth tx, the backend answering `answer`. The tx has no logs here
    (the gate reads its row alone), so nothing is detected locally."""
    tx = recorded["tx_hash"]
    client.setdefault("backend_answer", httpx.Response(200, json=answer or []))
    client.setdefault("included_bridges", ("relay",))
    http, seen, _ = make_client(
        log_rows=[], indexed=(tx,), tx_row=tx_row(recorded), **client
    )
    return http.get(f"/eth/txs/{tx}{suffix}/conversions"), seen


def test_the_recorded_fills_are_the_six_gaps_and_two_that_are_no_bridge():
    assert len(PAIRED) == 6
    assert NO_BRIDGE == ["refund", "third_address_fill"]


@pytest.mark.parametrize("name", PAIRED)
def test_an_eth_fill_answers_the_row_its_deposit_answers(name):
    """The backend's pair, as it is: the same row the deposit's chain answers."""
    recorded = FILLS[name]
    (row,) = recorded["adapter_rows"]
    assert row["to_network"] == "eth" and row["from_network"] != "eth"

    response, seen = ask(recorded, [row])

    assert response.status_code == 200
    assert response.json() == [row]
    assert "Cache-Control" not in response.headers
    (call,) = seen
    assert str(call.url) == (
        f"{BACKEND_URL}/eth/txs/{recorded['tx_hash']}/conversions?only_bridges=true"
    )


@pytest.mark.parametrize("name", NO_BRIDGE)
def test_a_fill_shaped_tx_the_backend_names_no_bridge_for_has_no_bridge_row(name):
    """A refund (a solver's payment on the deposit's own chain) and a fill
    whose payout the backend does not read: asked, and no row, not a half."""
    response, seen = ask(FILLS[name], [])
    assert response.json() == []
    assert len(seen) == 1 and "Cache-Control" not in response.headers


def test_a_tx_without_a_fills_shape_asks_nothing():
    response, seen = ask(ETH_TXS["other"]["usdc_deposit"])
    assert response.json() == []
    assert seen == []


def test_without_relay_in_included_bridges_a_fill_asks_nothing():
    recorded = FILLS["native_fill"]
    response, seen = ask(
        recorded, recorded["adapter_rows"], included_bridges=("cctp", "across")
    )
    assert response.json() == []
    assert seen == []


def test_a_fill_whose_order_relay_does_not_list_yet_says_it_is_not_final():
    """The backend's half for a solver's payment Relay does not list yet: for
    that minute a fill and a refund look alike. The row is passed on with its
    reason, and the answer is not to be stored."""
    recorded = FILLS["native_fill"]
    (half,) = recorded["adapter_rows_unlisted"]
    assert half["partial_reason"] == "counter_lookup_failed"

    response, _ = ask(recorded, [half])

    assert response.json() == [half]
    assert response.headers["Cache-Control"] == "no-store"


@pytest.mark.parametrize(
    "failure",
    [
        {"fail_with": httpx.ConnectError("refused")},
        {"backend_answer": httpx.Response(503)},
    ],
    ids=["refused", "503"],
)
def test_a_possible_fill_the_backend_does_not_answer_for_has_no_bridge_row(failure):
    """Only the backend can tell a fill from any other payment of this shape:
    without its answer there is no row to show, and nothing may keep the
    answer as final."""
    response, seen = ask(FILLS["usdc_fill"], **failure)
    assert response.status_code == 200
    assert response.json() == []
    assert response.headers["Cache-Control"] == "no-store"
    assert response.headers[DECLINED_HEADER] == BACKEND_URL
    assert len(seen) == 1


@pytest.mark.parametrize(
    "roles, shown", [("currency-arb", True), ("currency-bnb", False)]
)
def test_a_fill_is_shown_only_to_a_caller_who_may_see_its_deposits_chain(
    roles, shown
):
    """As for a pair of a half bridge: a caller without the gated currency a
    row names gets no row of the backend. For a fill that is no bridge row."""
    recorded = FILLS["usdc_fill"]
    (row,) = recorded["adapter_rows"]
    assert row["from_network"] == "arb"
    tx = recorded["tx_hash"]
    client, _, _ = make_client(
        log_rows=[],
        indexed=(tx,),
        included_bridges=("relay",),
        tx_row=tx_row(recorded),
        backend_answer=httpx.Response(200, json=[row]),
        roles_config=ROLES,
        gated=GATED,
    )
    response = client.get(
        f"/eth/txs/{tx}/conversions", headers={"X-User-Roles": roles}
    )
    assert response.json() == ([row] if shown else [])


def test_a_fills_token_leg_answers_the_row_and_another_leg_does_not():
    """A sub-tx request: the row only for the leg it names."""
    recorded = FILLS["usdc_fill"]
    (row,) = recorded["adapter_rows"]
    leg = row["to_asset_transfer"].partition("_")[2]

    response, seen = ask(recorded, [row], suffix=f"_{leg}")
    assert response.json() == [row]
    assert str(seen[0].url).endswith(f"_{leg}/conversions?only_bridges=true")

    other, _ = ask(recorded, [row], suffix="_T1")
    assert other.json() == []


def test_a_row_of_the_backend_that_is_no_fill_on_this_tx_is_not_taken():
    """The backend's rows are taken for the fill they are: its Relay row
    paying on eth in this tx. The same row as another protocol's, or with
    this tx as its source, was not asked for."""
    recorded = FILLS["native_fill"]
    (row,) = recorded["adapter_rows"]
    swapped = {
        **row,
        "from_network": "eth",
        "from_asset_transfer": row["to_asset_transfer"],
        "to_network": "arb",
        "to_asset_transfer": row["from_asset_transfer"],
    }
    response, _ = ask(recorded, [{**row, "protocol": "across"}, swapped])
    assert response.json() == []
