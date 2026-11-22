"""Conversions of an eth tx with a bridge graphsense-lib finds only half of
(CCTP): the half bridge, or the pairing an external backend answers in its
place (middleware/external_backends.py, rule 6); and proxied conversions
answers that name an eth leg, which pass through.

These run without containers: the real txs route and TxsService over a fake
database, the middleware in front, and an httpx MockTransport for the backend.
The txs are real mainnet Circle CCTP transfers (tests/defi/cctp_eth_logs.json).
"""

import asyncio
import dataclasses
import logging
import time
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
from fastapi import FastAPI, Response
from starlette.testclient import TestClient

from graphsenselib.db.asynchronous.services.txs_service import TxsService
from graphsenselib.defi import ExternalSwap
from graphsenselib.errors import TransactionNotFoundException
from graphsenselib.web.app import _register_exception_handlers, create_app_from_dict
from graphsenselib.web.config import CurrencyRolesConfig, GSRestConfig
from graphsenselib.web.middleware import external_backends
from graphsenselib.web.middleware.external_backends import (
    DECLINED_HEADER,
    SERVED_BY_HEADER,
    SERVED_BY_VALUE,
    ExternalBackendMiddleware,
)
from graphsenselib.web.routes import txs

from tests.defi.test_delegated_bridges import (
    FIXTURE,
    HALF_BRIDGES,
    db_rows,
    without_cctp_events,
)
from tests.defi.test_half_across import FIXTURE as ACROSS
from tests.defi.test_half_across import db_rows as across_rows

BACKEND_URL = "https://backend.test"
USDC = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"

# eth V2 burn -> arb (0xe8c9cfb2…), its USDC funding Transfer is log 694; the
# arb mint 0xaf516e4c… pays out in log 8
CCTP_BURN = FIXTURE["v2_burn"]["tx_hash"]
ARB_MINT = "0xaf516e4ccec230311bb7dc7bea67bc841b33808c1eb5e8c90968830473d4c804"
OTHER_ETH_TX = "0x" + "11" * 32
CONVERSIONS = f"/eth/txs/{CCTP_BURN}/conversions"

# the backend's pairing of the burn
BRIDGE_ROW = {
    "conversion_type": "bridge_tx",
    "from_address": "0x9e9852ba17ab249fd668cdc8f5f11cbcdff306d4",
    "to_address": "0x7ae6b9a6d79627099b0c46bf3363b8ac400277cc",
    "from_asset": USDC,
    "to_asset": "0xaf88d065e77c8cc2239327c5edb3a432268e5831",
    "from_amount": "0x52201b3",
    "to_amount": "0x521e010",
    "from_asset_transfer": f"{CCTP_BURN[2:]}_T694",
    "to_asset_transfer": f"{ARB_MINT[2:]}_T8",
    "from_network": "eth",
    "to_network": "arb",
    "from_is_supported_asset": True,
    "to_is_supported_asset": True,
}
# a swap into USDC before the burn, as the backend may answer one
SWAP_ROW = {
    **BRIDGE_ROW,
    "conversion_type": "dex_swap",
    "from_asset": "native",
    "from_amount": "0xde0b6b3a7640000",
    "from_asset_transfer": f"{CCTP_BURN}_I0",
    "to_asset_transfer": f"{CCTP_BURN}_T690",
    "to_asset": USDC,
    "to_network": "eth",
}
# the backend's fiat extension: not part of the eth answer's contract
FIAT = {"to_amount_fiat_values": [{"code": "usd", "value": 86.0}]}

# No captured CCTP receipt has a swap the local detection finds, so the local
# answer would be []. The local swap detection reports this one instead, to
# tell the local rows from the added ones.
LOCAL_SWAP = ExternalSwap(
    fromAddress="0x" + "11" * 20,
    toAddress="0x" + "11" * 20,
    fromAsset="native",
    toAsset=USDC,
    fromAmount=10**18,
    toAmount=2_000_000_000,
    fromPayment=f"{CCTP_BURN}_I0",
    toPayment=f"{CCTP_BURN}_T690",
)
LOCAL_ROW = {
    "conversion_type": "dex_swap",
    "from_address": "0x" + "11" * 20,
    "to_address": "0x" + "11" * 20,
    "from_asset": "native",
    "to_asset": USDC,
    "from_amount": "0xde0b6b3a7640000",
    "to_amount": "0x77359400",
    "from_asset_transfer": f"{CCTP_BURN}_I0",
    "to_asset_transfer": f"{CCTP_BURN}_T690",
    "from_network": "eth",
    "to_network": "eth",
    "from_is_supported_asset": True,
    "to_is_supported_asset": False,
}


def half_row(part):
    """A fixture tx's half bridge as the route answers it: no token is
    configured, so no asset is supported. It names its protocol, and the
    transfer id where the eth logs hold one."""
    bridge = HALF_BRIDGES[part]
    names = {"protocol": bridge.protocol, "transfer_id": bridge.transferId}
    return {
        "conversion_type": "bridge_tx",
        "from_address": bridge.fromAddress,
        "to_address": bridge.toAddress,
        "from_asset": bridge.fromAsset,
        "to_asset": bridge.toAsset,
        "from_amount": hex(bridge.fromAmount),
        "to_amount": hex(bridge.toAmount),
        "from_asset_transfer": bridge.fromPayment,
        "to_asset_transfer": bridge.toPayment,
        "from_network": bridge.fromNetwork,
        "to_network": bridge.toNetwork,
        "from_is_supported_asset": False,
        "to_is_supported_asset": False,
        **{name: value for name, value in names.items() if value is not None},
    }


# the burn's half bridge: its eth leg is the one BRIDGE_ROW names
HALF_ROW = half_row("v2_burn")
assert HALF_ROW["from_asset_transfer"] == BRIDGE_ROW["from_asset_transfer"]


def unanswered(error=None):
    """The burn's half when the backend did not answer: not final, and its
    row says why (out of time for a timeout, failed for anything else)."""
    timed_out = isinstance(error, (httpx.TimeoutException, TimeoutError))
    reason = "counter_lookup_timeout" if timed_out else "counter_lookup_failed"
    return {**HALF_ROW, "partial_reason": reason}


@pytest.fixture(autouse=True)
def local_swap():
    with patch(
        "graphsenselib.defi.swaps.get_swap_from_decoded_logs",
        return_value=[LOCAL_SWAP],
    ):
        yield


@contextmanager
def records_of_the_middleware():
    records = []

    class Sink(logging.Handler):
        def emit(self, record):
            records.append(record)

    logger = logging.getLogger("graphsenselib.web.middleware.external_backends")
    handler = Sink(level=logging.INFO)
    logger.addHandler(handler)
    # setLevel, not an assignment: it clears the logger's isEnabledFor cache,
    # which a test that logged at INFO while the level was unset has filled
    # with "INFO disabled" (the pairing and merge lines are logged in tests
    # that capture nothing)
    previous = logger.level
    logger.setLevel(logging.INFO)
    try:
        yield records
    finally:
        logger.removeHandler(handler)
        logger.setLevel(previous)


def fake_db(log_rows, indexed):
    """An eth database whose txs `indexed` all have `log_rows` as logs."""
    known = {bytes.fromhex(h.removeprefix("0x")) for h in indexed}

    async def get_tx_by_hash(currency, tx_hash):
        if currency != "eth" or bytes(tx_hash) not in known:
            raise TransactionNotFoundException(currency, tx_hash)
        return {
            "tx_hash": bytes(tx_hash),
            "block_id": FIXTURE["v2_burn"]["block_id"],
            "from_address": bytes.fromhex("11" * 20),  # every tx row names its sender
            "input": None,
        }

    db = MagicMock()
    db.get_tx_by_hash = AsyncMock(side_effect=get_tx_by_hash)
    db.fetch_transaction_logs = AsyncMock(return_value=log_rows)
    db.fetch_transaction_traces = AsyncMock(return_value=[])
    db.get_token_configuration = MagicMock(return_value={})
    return db


def make_client(
    log_rows=None,
    indexed=(CCTP_BURN,),
    delegated_conversions=None,
    networks=None,
    backend_answer=None,
    fail_with=None,
    roles_config=None,
    gated=None,
    timeout_s=None,
    with_middleware=True,
    included_bridges=("cctp",),
):
    """The txs routes over a fake database behind the middleware, and a
    recording mock backend. `backend_answer` is an httpx.Response or a function
    of the request returning one; by default the backend answers the bridge
    row (with its fiat extension) and a swap row."""
    app = FastAPI()
    app.include_router(txs.router, prefix="/{currency}")
    _register_exception_handlers(app)
    if delegated_conversions is None:
        delegated_conversions = {"eth": {"url": BACKEND_URL, "api_key": "backend-key"}}
    backends = {
        "enabled": True,
        "networks": networks or {},
        "delegated_conversions": delegated_conversions,
    }
    if timeout_s is not None:
        backends["merge_timeout_s"] = timeout_s
    config = GSRestConfig.from_dict(
        {
            "database": {"nodes": ["localhost"]},
            "external_backends": backends,
            "included_bridges": list(included_bridges),
        }
    )
    db = fake_db(db_rows("v2_burn") if log_rows is None else log_rows, indexed)
    app.state.config = config
    app.state.db = db
    app.state.services = SimpleNamespace(
        txs_service=TxsService(db=db, rates_service=MagicMock(), logger=MagicMock())
    )

    seen: list[httpx.Request] = []

    def backend(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        if fail_with is not None:
            raise fail_with
        if callable(backend_answer):
            return backend_answer(request)
        if backend_answer is not None:
            return backend_answer
        return httpx.Response(200, json=[{**BRIDGE_ROW, **FIAT}, SWAP_ROW])

    if with_middleware:
        app.add_middleware(
            ExternalBackendMiddleware,
            config=config.external_backends,
            client=httpx.AsyncClient(transport=httpx.MockTransport(backend)),
            roles_config=roles_config,
            gated=gated,
        )
    return TestClient(app), seen, db


# ---------------------------------------------------------------------------
# without a backend for the network the half bridge is the answer
# ---------------------------------------------------------------------------


def test_without_the_middleware_a_cctp_tx_gets_its_half_bridge():
    """graphsense-lib alone."""
    client, _, _ = make_client(with_middleware=False)
    response = client.get(CONVERSIONS)
    assert response.status_code == 200
    assert response.json() == [LOCAL_ROW, HALF_ROW]


@pytest.mark.parametrize("delegated_conversions", [{}, {"trx": {"url": BACKEND_URL}}])
def test_without_a_backend_for_eth_nothing_is_asked(delegated_conversions):
    client, seen, _ = make_client(
        delegated_conversions=delegated_conversions,
        networks={"arb": {"url": BACKEND_URL}},
    )
    assert client.get(CONVERSIONS).json() == [LOCAL_ROW, HALF_ROW]
    assert seen == []


def test_without_cctp_in_included_bridges_nothing_is_reported_or_asked():
    client, seen, _ = make_client(included_bridges=("thorchain",))
    assert client.get(CONVERSIONS).json() == [LOCAL_ROW]
    assert seen == []


def test_any_other_eth_tx_is_passed_through_and_reads_what_it_read_before():
    rows = db_rows("v2_burn", without_cctp_events("v2_burn"))
    standalone, _, before = make_client(log_rows=rows, with_middleware=False)
    expected = standalone.get(CONVERSIONS)

    client, seen, db = make_client(log_rows=rows)
    response = client.get(CONVERSIONS)

    assert seen == []
    assert response.content == expected.content
    assert db.mock_calls == before.mock_calls


# ---------------------------------------------------------------------------
# a CCTP tx gains the bridge the backend pairs
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "reason", ["counter_lookup_failed", "counter_lookup_timeout"]
)
def test_a_half_the_backend_could_not_pair_for_now_says_why_in_its_row(reason):
    """The backend answers 200 with the burn as a half whose counter-chain
    lookup failed or was cut off (the row's partial_reason). The local half
    stays, with that reason in the body: no response header names it. The
    answer must not be stored, so the next request asks again."""
    unpaired = {**BRIDGE_ROW, "to_asset_transfer": "pending", "partial_reason": reason}
    client, _, _ = make_client(backend_answer=httpx.Response(200, json=[unpaired]))
    with records_of_the_middleware() as records:
        response = client.get(CONVERSIONS)
    assert response.status_code == 200
    assert response.json() == [LOCAL_ROW, {**HALF_ROW, "partial_reason": reason}]
    assert "X-Ikn-Partial" not in response.headers
    assert response.headers["Cache-Control"] == "no-store"
    assert DECLINED_HEADER not in response.headers
    (record,) = [r for r in records if "event=bridge_pairing" in r.getMessage()]
    assert f"partial={reason}" in record.getMessage()


def test_a_paired_row_that_is_not_final_keeps_its_reason():
    """The pair is proven, the name of its native payout could not be read:
    the row replaces the half and still says it is not final."""
    row = {**BRIDGE_ROW, "partial_reason": "counter_lookup_failed"}
    client, _, _ = make_client(
        backend_answer=httpx.Response(200, json=[{**row, **FIAT}, SWAP_ROW])
    )
    response = client.get(CONVERSIONS)
    assert response.json() == [LOCAL_ROW, row]
    assert response.headers["Cache-Control"] == "no-store"


@pytest.mark.parametrize(
    "reason", [None, "counter_lookup_failed"], ids=["final", "not final"]
)
def test_the_local_half_takes_the_transfer_id_only_the_backend_knows(reason):
    """Circle assigns a CCTP V2 burn's nonce off chain, so the eth logs hold
    none. The backend's half names it once Circle attested the burn (and the
    mint is not found): the local half stays, with that id."""
    nonce = "0x" + "3b" * 32
    said = {"protocol": "cctp", "transfer_id": nonce}
    if reason:
        said["partial_reason"] = reason
    half = {**BRIDGE_ROW, "to_asset_transfer": "pending", **said}
    client, _, _ = make_client(backend_answer=httpx.Response(200, json=[half]))
    response = client.get(CONVERSIONS)
    assert response.json() == [LOCAL_ROW, {**HALF_ROW, **said}]
    assert ("Cache-Control" in response.headers) == bool(reason)


def test_a_half_of_the_backend_is_never_a_row_of_the_answer():
    """Every half the backend answers reaches the merge (it may carry a reason,
    a transfer id or a chain name for ours). It never becomes a row: the half
    of our own leg leaves ours as it is, and a half of a leg we do not have is
    dropped, so no local half is replaced or doubled."""
    ours = {**BRIDGE_ROW, "to_asset_transfer": "pending"}
    another = {**ours, "from_asset_transfer": f"{CCTP_BURN[2:]}_T1"}
    client, _, _ = make_client(
        backend_answer=httpx.Response(200, json=[ours, another, ours])
    )
    response = client.get(CONVERSIONS)
    assert response.json() == [LOCAL_ROW, HALF_ROW]
    assert "Cache-Control" not in response.headers


def _burn_to(domain):
    """The V2 burn's logs with another destination domain."""
    logs = [dict(log) for log in FIXTURE["v2_burn"]["logs"]]
    burn = next(log for log in logs if log["log_index"] == 698)
    data = burn["data"].removeprefix("0x")
    burn["data"] = "0x" + data[:128] + f"{domain:064x}" + data[192:]
    return logs


def test_the_local_half_takes_the_chain_the_backends_half_names():
    """A burn to Solana (domain 5): the eth logs name no network for it, the
    backend's half does. The local half takes that name and nothing else of
    the side: the answer is final, and the asset stays unsupported."""
    ours = {**HALF_ROW, "to_network": "unknown", "to_address": "unknown"}
    theirs = {**ours, "to_network": "solana"}
    client, _, _ = make_client(
        log_rows=db_rows("v2_burn", _burn_to(5)),
        backend_answer=httpx.Response(200, json=[theirs]),
    )
    response = client.get(CONVERSIONS)
    assert response.json() == [LOCAL_ROW, theirs]
    assert theirs["to_is_supported_asset"] is False
    assert "Cache-Control" not in response.headers


def test_a_mint_takes_the_source_chain_the_backends_half_names():
    """A mint whose MessageReceived is not in the logs comes from an unknown
    network; the backend's half names it."""
    tx = FIXTURE["v2_mint"]["tx_hash"]
    logs = [log for log in FIXTURE["v2_mint"]["logs"] if log["log_index"] != 567]
    ours = {**half_row("v2_mint"), "from_network": "unknown"}
    del ours["transfer_id"]  # the nonce was in the missing log
    theirs = {**ours, "from_network": "solana"}
    client, _, _ = make_client(
        log_rows=db_rows("v2_mint", logs),
        indexed=(tx,),
        backend_answer=httpx.Response(200, json=[theirs]),
    )
    assert client.get(f"/eth/txs/{tx}/conversions").json() == [LOCAL_ROW, theirs]


def test_a_network_the_local_half_names_is_not_replaced():
    """The eth logs name arb: the backend's half of the leg changes nothing."""
    theirs = {**HALF_ROW, "to_network": "solana"}
    client, _, _ = make_client(backend_answer=httpx.Response(200, json=[theirs]))
    assert client.get(CONVERSIONS).json() == [LOCAL_ROW, HALF_ROW]


def test_a_final_half_of_the_backend_leaves_the_local_half_as_it_is():
    """The backend's own half without a reason (the other chain is not
    served, nothing in the window) is no better than the local one."""
    final = {**BRIDGE_ROW, "to_asset_transfer": "pending"}
    client, _, _ = make_client(backend_answer=httpx.Response(200, json=[final]))
    response = client.get(CONVERSIONS)
    assert response.json() == [LOCAL_ROW, HALF_ROW]
    assert "Cache-Control" not in response.headers


def test_a_complete_backend_answer_is_not_partial():
    client, _, _ = make_client()
    response = client.get(CONVERSIONS)
    assert response.json() == [LOCAL_ROW, BRIDGE_ROW]
    assert "Cache-Control" not in response.headers


@pytest.mark.parametrize(
    "names",
    [
        {},
        {"protocol": "across", "transfer_id": "1108583"},
        {"protocol": "cctp", "transfer_id": "0x" + "3b" * 32},
    ],
    ids=["an older backend: none", "across", "cctp"],
)
def test_a_paired_row_keeps_the_protocol_and_transfer_id_it_names(names):
    """The backend's protocol and transfer_id reach the caller as it sent
    them; a row without them (an older backend) still pairs."""
    bridge = {**BRIDGE_ROW, **names}
    answer = httpx.Response(200, json=[{**bridge, **FIAT}, SWAP_ROW])
    client, seen, _ = make_client(backend_answer=answer)
    response = client.get(CONVERSIONS)
    assert response.status_code == 200
    assert response.json() == [LOCAL_ROW, bridge]
    assert len(seen) == 1


def test_a_protocol_this_version_does_not_list_costs_the_names_not_the_pair():
    """A protocol newer than this version's list is dropped with its app and
    transfer id, which describe it; the pair is served."""
    names = {"protocol": "teleport", "protocol_app": "x", "transfer_id": "7"}
    answer = httpx.Response(200, json=[{**BRIDGE_ROW, **names, **FIAT}, SWAP_ROW])
    client, _, _ = make_client(backend_answer=answer)
    assert client.get(CONVERSIONS).json() == [LOCAL_ROW, BRIDGE_ROW]


@pytest.mark.parametrize(
    "part, side",
    [("v1_burn", "from"), ("v2_burn", "from"), ("v1_mint", "to"), ("v2_mint", "to")],
)
def test_the_bridge_the_backend_pairs_takes_the_half_bridges_place(part, side):
    tx = FIXTURE[part]["tx_hash"]
    leg = half_row(part)[f"{side}_asset_transfer"]
    other = "to" if side == "from" else "from"
    # the leg on eth, the other chain's on arb, as the backend pairs it
    bridge = {
        **BRIDGE_ROW,
        f"{side}_asset_transfer": leg,
        f"{side}_network": "eth",
        f"{other}_network": "arb",
    }
    answer = httpx.Response(200, json=[{**bridge, **FIAT}, SWAP_ROW])
    client, seen, _ = make_client(
        log_rows=db_rows(part), indexed=(tx,), backend_answer=answer
    )

    response = client.get(f"/eth/txs/{tx}/conversions", headers={"X-Username": "alice"})

    assert response.status_code == 200
    # the bridge without the fiat extension; swaps are the local detection's
    assert response.json() == [LOCAL_ROW, bridge]
    assert DECLINED_HEADER not in response.headers
    (call,) = seen
    assert str(call.url) == f"{BACKEND_URL}/eth/txs/{tx}/conversions?only_bridges=true"
    assert call.headers["authorization"] == "backend-key"
    assert call.headers["x-consumer-username"] == "alice"


@pytest.mark.parametrize(
    "row",
    [
        SWAP_ROW,
        {
            **BRIDGE_ROW,
            "from_asset_transfer": f"{OTHER_ETH_TX[2:]}_T1",
            "to_asset_transfer": f"{ARB_MINT[2:]}_T8",
        },
    ],
    ids=["swap", "bridge of another tx"],
)
def test_no_row_but_a_bridge_of_the_tx_is_taken(row):
    client, seen, _ = make_client(backend_answer=httpx.Response(200, json=[row]))
    assert client.get(CONVERSIONS).json() == [LOCAL_ROW, HALF_ROW]
    assert len(seen) == 1


@pytest.mark.parametrize(
    "transfer, bare_half, paired",
    [
        (f"0X{CCTP_BURN[2:].upper()}_T694", False, True),
        (f"{CCTP_BURN[2:]}_T1", False, False),
        (CCTP_BURN[2:], False, False),
        (f"{CCTP_BURN[2:]}_T694", True, True),
    ],
    ids=["its leg", "another transfer", "the tx", "bare half"],
)
def test_a_paired_bridge_takes_the_place_of_the_half_bridge_it_names(
    transfer, bare_half, paired
):
    """A bare half can match a transfer, but a precise half needs that transfer."""
    bridge = {**BRIDGE_ROW, "from_asset_transfer": transfer}
    logs = FIXTURE["v2_burn"]["logs"]
    if bare_half:
        funding = next(log for log in logs if log["log_index"] == 694)
        logs = [{**funding, "log_index": 693}, *logs]
    client, _, _ = make_client(
        log_rows=db_rows("v2_burn", logs),
        backend_answer=httpx.Response(200, json=[bridge]),
    )
    expected = [LOCAL_ROW, bridge] if paired else [LOCAL_ROW, HALF_ROW]
    assert client.get(CONVERSIONS).json() == expected


def test_a_pair_takes_only_its_own_half_among_the_native_fills_of_one_tx():
    """eth 0x7d7e5024…6353: a relayer tx filling four Across deposits in
    native ETH, so each local half names the same bare tx hash as its leg.
    The pair the backend answers for opt deposit 58732 replaces that half
    alone, and the reason it gives for the sibling it could not look up
    (arb deposit 81435) lands on that sibling alone."""
    tx = ACROSS["opt_eth_v20_native_fill"]["tx_hash"]
    path = f"/eth/txs/{tx}/conversions"

    def ask(answer):
        client, _, _ = make_client(
            log_rows=across_rows("opt_eth_v20_native_fill"),
            indexed=(tx,),
            included_bridges=("across",),
            backend_answer=httpx.Response(200, json=answer),
        )
        return client.get(path)

    local, *halves = ask([]).json()
    assert [half["transfer_id"] for half in halves] == [
        "58732",
        "81435",
        "58731",
        "81434",
    ]
    assert {half["to_asset_transfer"] for half in halves} == {tx[2:]}
    own, failed, third, fourth = halves
    pair = {
        **own,
        "from_address": "0x517c07288216fd840a49e49d49e2644c5ee5a29d",
        "from_asset": "native",
        "from_asset_transfer": "b6fb15e4bcd8f145ab89fbc709e4bdb19e4fb6902acca46c429888edaddddf94",
        "from_is_supported_asset": True,
        "to_is_supported_asset": True,
    }
    reason = {"partial_reason": "counter_lookup_failed"}

    response = ask([pair, {**failed, **reason}, third, fourth])

    assert response.json() == [local, {**failed, **reason}, third, fourth, pair]
    assert response.headers["Cache-Control"] == "no-store"


def test_a_paired_bridge_replaces_only_the_half_bridge_it_names():
    """Two burns in one tx, one paired: the other stays a half bridge."""
    logs = FIXTURE["v2_burn"]["logs"]
    again = [{**log, "log_index": log["log_index"] + 100} for log in logs]
    client, _, _ = make_client(
        log_rows=db_rows("v2_burn", logs + again),
        backend_answer=httpx.Response(200, json=[BRIDGE_ROW]),
    )
    rows = client.get(CONVERSIONS).json()
    assert rows[0] == LOCAL_ROW and rows[-1] == BRIDGE_ROW
    (half,) = rows[1:-1]
    assert half["from_asset_transfer"] == f"{CCTP_BURN[2:]}_T794"
    assert half["to_asset_transfer"] == "pending"


def test_a_local_row_naming_a_leg_of_a_paired_bridge_is_dropped():
    """It is that bridge too; transfers are compared in lower case and
    without 0x."""
    swap = dataclasses.replace(LOCAL_SWAP, toPayment=f"0X{CCTP_BURN[2:].upper()}_T694")
    with patch(
        "graphsenselib.defi.swaps.get_swap_from_decoded_logs", return_value=[swap]
    ):
        client, _, _ = make_client(
            backend_answer=httpx.Response(200, json=[BRIDGE_ROW])
        )
        assert client.get(CONVERSIONS).json() == [BRIDGE_ROW]


@pytest.mark.parametrize("placeholder", ["pending", "Unknown"])
def test_a_placeholder_leg_names_no_bridge(placeholder):
    """The placeholders "pending" and "unknown" name no transfer: a local row
    carrying one, such as a THORChain partial bridge, is not the paired
    bridge. A backend row carrying one is a half row, which pairs nothing
    (tests/web/test_delegated_merge.py): the local half stays."""
    local = dataclasses.replace(LOCAL_SWAP, toPayment=placeholder)
    half = {**BRIDGE_ROW, "to_asset_transfer": placeholder.lower()}
    with patch(
        "graphsenselib.defi.swaps.get_swap_from_decoded_logs", return_value=[local]
    ):
        for answer, paired in ((BRIDGE_ROW, BRIDGE_ROW), (half, HALF_ROW)):
            client, _, _ = make_client(
                backend_answer=httpx.Response(200, json=[answer])
            )
            assert client.get(CONVERSIONS).json() == [
                {**LOCAL_ROW, "to_asset_transfer": placeholder},
                paired,
            ]


@pytest.mark.parametrize(
    "network, identifier",
    [
        ("eth", f"{CCTP_BURN}_T694"),
        ("ETH", CCTP_BURN),
        # the local lookup ignores whitespace in a hash (bytes.fromhex), so a
        # control character gets this far; httpx refuses it unencoded
        ("eth", f"{CCTP_BURN}%0B"),
    ],
)
def test_the_identifier_reaches_the_backend_as_it_was_asked(network, identifier):
    """The sub-tx suffix included (the backend scopes its answer to it), the
    network in lower case; the swaps are the local detection's, so only the
    bridges are asked for."""
    client, seen, _ = make_client()
    response = client.get(f"/{network}/txs/{identifier}/conversions")
    assert BRIDGE_ROW in response.json()
    assert (
        seen[0].url.raw_path
        == f"/eth/txs/{identifier}/conversions?only_bridges=true".encode()
    )


def test_a_sub_tx_of_no_half_bridge_asks_nothing():
    client, seen, _ = make_client()
    assert client.get(f"/eth/txs/{CCTP_BURN}_T5/conversions").json() == []
    assert seen == []


@pytest.mark.parametrize("timeout_s, expected", [(None, 5.0), (4.0, 4.0)])
def test_the_call_waits_merge_timeout_s(timeout_s, expected):
    client, seen, _ = make_client(timeout_s=timeout_s)
    client.get(CONVERSIONS)
    assert seen[0].extensions["timeout"]["read"] == expected


# ---------------------------------------------------------------------------
# the client opt-out and the currency roles: behind the gateway the opt-out
# arrives as the caller's lite-currency roles stripped, not as the header
# ---------------------------------------------------------------------------

ROLES = CurrencyRolesConfig()
GATED = {"arb", "pol", "bnb"}


@pytest.mark.parametrize(
    "headers, roles_config, asked, answer",
    [
        ({"X-Ikn-Currency-Opt-Out": "all-light"}, None, False, [LOCAL_ROW, HALF_ROW]),
        ({"X-User-Roles": "currency-eth"}, ROLES, False, [LOCAL_ROW, HALF_ROW]),
        ({"X-User-Roles": "currency-bnb"}, ROLES, True, [LOCAL_ROW, HALF_ROW]),
        ({"X-User-Roles": "currency-arb"}, ROLES, True, [LOCAL_ROW, BRIDGE_ROW]),
        (
            {},
            CurrencyRolesConfig(enforce_currency_roles=False),
            True,
            [LOCAL_ROW, BRIDGE_ROW],
        ),
    ],
    ids=["opt-out", "no lite role", "not the bridged one", "role", "gate off"],
)
def test_the_opt_out_and_the_currency_roles(headers, roles_config, asked, answer):
    client, seen, _ = make_client(roles_config=roles_config, gated=GATED)
    response = client.get(CONVERSIONS, headers=headers)
    assert response.json() == answer
    assert len(seen) == int(asked)


def test_a_pairing_the_caller_may_see_only_in_part_leaves_the_half_bridges():
    """The paired rows replace every half bridge of the tx (a backend pairs
    all or none). A caller entitled to arb but not to pol, of a tx bridging
    to both, would otherwise get the arb bridge and lose the pol half."""
    to_pol = {
        **BRIDGE_ROW,
        "from_asset_transfer": f"{CCTP_BURN[2:]}_T700",
        "to_asset_transfer": f"{'22' * 32}_T1",
        "to_network": "pol",
    }
    client, seen, _ = make_client(
        roles_config=ROLES,
        gated=GATED,
        backend_answer=httpx.Response(200, json=[BRIDGE_ROW, to_pol]),
    )
    response = client.get(CONVERSIONS, headers={"X-User-Roles": "currency-arb"})
    assert response.json() == [LOCAL_ROW, HALF_ROW]
    assert len(seen) == 1


# ---------------------------------------------------------------------------
# without the backend's pairing the half bridge is the answer
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("status", [404, 501])
def test_a_backend_that_cannot_pair_the_tx_leaves_the_half_bridge(status):
    """It cannot pair the tx (501, e.g. not minted yet) or does not know it
    (404): nothing is declined, nothing alerts, and the log says so."""
    answer = httpx.Response(status, json={"detail": "cannot pair it"})
    client, _, _ = make_client(backend_answer=answer)
    with records_of_the_middleware() as records:
        response = client.get(CONVERSIONS)
    assert response.json() == [LOCAL_ROW, HALF_ROW]
    assert DECLINED_HEADER not in response.headers
    # the pairing's line and the merge's, nothing alerts
    assert [r.levelno for r in records] == [logging.INFO, logging.INFO]
    (record,) = [r for r in records if "event=bridge_pairing" in r.getMessage()]
    assert f"{BACKEND_URL} cannot pair /eth/txs/{CCTP_BURN}/conversions" in (
        record.getMessage()
    )


@pytest.mark.parametrize(
    "answer, level",
    [
        (httpx.Response(500, json={"detail": "boom"}), logging.ERROR),
        (httpx.Response(400, json={"detail": "bad hash"}), logging.ERROR),
        (httpx.Response(429, headers={"Retry-After": "7"}), logging.WARNING),
        (httpx.Response(200, content=b"<html>busy</html>"), logging.ERROR),
        (httpx.Response(200, json=[{"conversion_type": "bridge_tx"}]), logging.WARNING),
        (httpx.Response(200, json={"detail": "not a list"}), logging.WARNING),
    ],
    ids=["error", "400", "429", "not json", "invalid row", "not a list"],
)
def test_a_failed_backend_leaves_the_half_bridge_and_is_named(answer, level):
    client, _, _ = make_client(backend_answer=answer)
    with records_of_the_middleware() as records:
        response = client.get(CONVERSIONS)
    assert response.status_code == 200
    assert response.json() == [LOCAL_ROW, unanswered()]
    assert response.headers[DECLINED_HEADER] == BACKEND_URL
    # the alert, then the pairing's INFO line and the merge's
    assert [r.levelno for r in records] == [level, logging.INFO, logging.INFO]


async def _slow(request):
    await asyncio.sleep(3)
    return httpx.Response(200, json=[BRIDGE_ROW])


@pytest.mark.parametrize(
    "backend, reason",
    [
        ({"timeout_s": 0.2, "backend_answer": _slow}, "counter_lookup_timeout"),
        ({"fail_with": httpx.ReadTimeout("hung")}, "counter_lookup_timeout"),
        ({"fail_with": httpx.ConnectError("refused")}, "counter_lookup_failed"),
        ({"backend_answer": httpx.Response(503)}, "counter_lookup_failed"),
        ({"backend_answer": httpx.Response(200, json=[])}, None),
        ({"backend_answer": httpx.Response(404)}, None),
    ],
    ids=["timeout", "hung", "refused", "503", "empty answer", "404"],
)
def test_the_local_half_says_why_when_the_backend_did_not_answer(backend, reason):
    """A backend that did not answer leaves the local half in place, and the
    half is not final: its row says why, and the answer must not be stored.
    A backend that answered "nothing to pair" leaves a final half."""
    client, _, _ = make_client(**backend)
    response = client.get(CONVERSIONS)
    assert response.status_code == 200
    half = {**HALF_ROW, "partial_reason": reason} if reason else HALF_ROW
    assert response.json() == [LOCAL_ROW, half]
    assert response.headers.get("Cache-Control") == ("no-store" if reason else None)
    assert (DECLINED_HEADER in response.headers) == bool(reason)


@pytest.mark.parametrize(
    "error", [httpx.ConnectError("refused"), httpx.ReadTimeout("hung")]
)
def test_an_unreachable_or_hung_backend_is_asked_again_by_the_next_pairing(
    error, monkeypatch
):
    """A pairing only adds to the answer, so its failure starts no skip
    window: the next pairing asks again. The outage is alerted once."""
    now = [1000.0]
    monkeypatch.setattr(external_backends, "_now", lambda: now[0])
    client, seen, _ = make_client(fail_with=error)
    with records_of_the_middleware() as records:
        answers = [client.get(CONVERSIONS), client.get(CONVERSIONS)]
    for response in answers:
        assert response.json() == [LOCAL_ROW, unanswered(error)]
        assert response.headers[DECLINED_HEADER] == BACKEND_URL
    assert len(seen) == 2
    (alert,) = [r.getMessage() for r in records if r.levelno == logging.ERROR]
    assert "eth conversions lack the bridges it pairs" in alert


def test_merge_timeout_s_bounds_the_whole_call():
    """httpx applies the timeout per phase: a backend that answers in slow
    pieces is not timed out by it. The pairing gets merge_timeout_s in all;
    running over declines and alerts, and the next pairing asks again."""

    async def slow(request):
        await asyncio.sleep(3)
        return httpx.Response(200, json=[BRIDGE_ROW])

    client, seen, _ = make_client(timeout_s=0.2, backend_answer=slow)
    with records_of_the_middleware() as records:
        started = time.monotonic()
        response = client.get(CONVERSIONS)
        elapsed = time.monotonic() - started
        again = client.get(CONVERSIONS)
    assert elapsed < 2
    for answer in (response, again):
        assert answer.json() == [LOCAL_ROW, unanswered(TimeoutError())]
        assert answer.headers[DECLINED_HEADER] == BACKEND_URL
    assert len(seen) == 2
    assert any(r.levelno == logging.ERROR for r in records)


# ---------------------------------------------------------------------------
# a proxied conversions answer passes through, eth legs or not
# ---------------------------------------------------------------------------

ARB_CONVERSIONS = f"/arb/txs/{ARB_MINT}/conversions"


def arb_client(rows, status=200, content=None):
    """arb served by the backend, which answers `rows` for a conversions
    request; Cassandra has the eth tx CCTP_BURN."""

    def answer(request):
        if not request.url.path.endswith("/conversions"):
            return httpx.Response(200, json={"backend_path": request.url.path})
        if content is not None:
            return httpx.Response(status, content=content)
        return httpx.Response(status, json=rows)

    return make_client(
        networks={"arb": {"url": BACKEND_URL}},
        delegated_conversions={},
        backend_answer=answer,
    )


@pytest.mark.parametrize(
    "rows",
    [
        [BRIDGE_ROW],
        [
            {
                **BRIDGE_ROW,
                "from_network": "arb",
                "from_asset_transfer": f"{ARB_MINT[2:]}_T8",
                "to_network": "eth",
                "to_asset_transfer": f"{OTHER_ETH_TX[2:]}_T3",
            },
            {**SWAP_ROW, "from_network": "arb", "to_network": "arb"},
        ],
        [{**BRIDGE_ROW, "from_asset_transfer": "pending"}],
        [{**BRIDGE_ROW, "from_asset_transfer": "unknown"}],
        [{**BRIDGE_ROW, "from_asset_transfer": "not-a-hash_T1"}],
        [{**BRIDGE_ROW, **FIAT}],
        ["junk", 1, None],
    ],
    ids=[
        "indexed eth leg",
        "eth leg not indexed yet",
        "pending",
        "unknown",
        "no tx hash",
        "extension fields",
        "no rows",
    ],
)
def test_a_proxied_conversions_answer_passes_through_unread(rows):
    """Byte for byte, whether or not Cassandra has the eth txs it names. A
    leg it lacks yet answers 404, which ends the dashboard's walk of that row
    alone; refusing the whole answer would hide the tx's other conversions
    too, and the bridge would be missing all the same (review of #213)."""
    client, seen, db = arb_client(rows)
    response = client.get(ARB_CONVERSIONS)
    assert response.status_code == 200
    assert response.content == httpx.Response(200, json=rows).content
    assert response.headers[SERVED_BY_HEADER] == SERVED_BY_VALUE
    db.get_tx_by_hash.assert_not_awaited()
    assert len(seen) == 1


@pytest.mark.parametrize(
    "path, status, content",
    [
        (ARB_CONVERSIONS, 501, b'{"detail": "cannot pair it"}'),
        (ARB_CONVERSIONS, 200, b"not json"),
        (f"/arb/txs/{ARB_MINT}", 200, None),
    ],
    ids=["error answer", "not json", "other route"],
)
def test_any_other_proxied_answer_reads_nothing(path, status, content):
    client, _, db = arb_client([BRIDGE_ROW], status=status, content=content)
    response = client.get(path)
    assert response.status_code == status
    if content is not None:
        assert response.content == content
    db.get_tx_by_hash.assert_not_awaited()


# ---------------------------------------------------------------------------
# setup
# ---------------------------------------------------------------------------


PAIRS_ON_ETH = "External backends pair the bridges in conversions on: eth"
PAIRS_NOTHING = (
    "delegated_conversions pairs nothing: included_bridges names none of across, cctp, "
    "debridge, layerzero, relay, wormhole, so no half bridge is found to pair"
)


@pytest.mark.parametrize(
    "enabled, included_bridges, logged",
    [
        (True, ["cctp"], [PAIRS_ON_ETH]),
        (True, ["thorchain"], [PAIRS_ON_ETH, PAIRS_NOTHING]),
        (False, ["cctp"], []),
    ],
    ids=["cctp", "no cctp", "disabled"],
)
def test_delegated_conversions_alone_install_the_middleware(
    enabled, included_bridges, logged, caplog
):
    """A deployment may pair eth conversions without serving a network
    externally; the startup log names the networks, and warns when no half
    bridge is reported to pair."""
    with caplog.at_level(logging.INFO, logger="graphsenselib.web.app"):
        app = create_app_from_dict(
            {
                "database": {"nodes": ["localhost"]},
                "included_bridges": included_bridges,
                "external_backends": {
                    "enabled": enabled,
                    "delegated_conversions": {"eth": {"url": BACKEND_URL}},
                },
            }
        )
    installed = any(m.cls is ExternalBackendMiddleware for m in app.user_middleware)
    assert installed == enabled
    assert [r.getMessage() for r in caplog.records if "pair" in r.getMessage()] == (
        logged
    )


@pytest.mark.parametrize(
    "path",
    [
        "/stats",
        "/capabilities",
        "/search?q=0xab",
        "/eth/addresses/0xab/related_addresses",
    ],
)
def test_delegated_conversions_alone_merge_nothing(path):
    """Without an externally served network there is nothing to merge: the
    local answers pass through as they are, not re-serialized."""
    app = FastAPI()
    body = b'{"currencies": [], "networks": [], "related_addresses": []}'

    @app.get("/{rest:path}")
    async def local(rest: str):
        return Response(content=body, media_type="application/json")

    config = GSRestConfig.from_dict(
        {
            "database": {"nodes": ["localhost"]},
            "external_backends": {
                "enabled": True,
                "delegated_conversions": {"eth": {"url": BACKEND_URL}},
            },
        }
    )
    seen = []
    app.add_middleware(
        ExternalBackendMiddleware,
        config=config.external_backends,
        client=httpx.AsyncClient(
            transport=httpx.MockTransport(
                lambda r: seen.append(r) or httpx.Response(500)
            )
        ),
    )
    response = TestClient(app).get(path)
    assert response.content == body
    assert seen == []
