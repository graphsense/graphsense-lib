"""Conversions of an eth tx with a bridge graphsense-lib cannot pair (CCTP),
merged with the pairing an external backend answers
(middleware/external_backends.py, rule 6), and proxied conversions answers
that name an eth leg (rule 1).

These run without containers: the real txs route and TxsService over a fake
database, the middleware in front, and an httpx MockTransport for the backend.
The txs are real mainnet Circle CCTP transfers (tests/defi/cctp_eth_logs.json).
"""

import logging
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

from tests.defi.test_delegated_bridges import FIXTURE, db_rows, without_cctp_events

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
    previous, logger.level = logger.level, logging.INFO
    try:
        yield records
    finally:
        logger.removeHandler(handler)
        logger.level = previous


def fake_db(log_rows, indexed):
    """An eth database whose txs `indexed` all have `log_rows` as logs."""
    known = {bytes.fromhex(h.removeprefix("0x")) for h in indexed}

    async def get_tx_by_hash(currency, tx_hash):
        if currency != "eth" or bytes(tx_hash) not in known:
            raise TransactionNotFoundException(currency, tx_hash)
        return {
            "tx_hash": bytes(tx_hash),
            "block_id": FIXTURE["v2_burn"]["block_id"],
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
        backends["delegated_conversions_timeout_s"] = timeout_s
    config = GSRestConfig.from_dict(
        {"database": {"nodes": ["localhost"]}, "external_backends": backends}
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
# without a backend for the network the answer is the local one
# ---------------------------------------------------------------------------


def test_without_the_middleware_a_cctp_tx_gets_the_local_answer():
    """graphsense-lib alone: the route marks the tx, and nothing reads it."""
    client, _, _ = make_client(with_middleware=False)
    response = client.get(CONVERSIONS)
    assert response.status_code == 200
    assert response.json() == [LOCAL_ROW]


@pytest.mark.parametrize("delegated_conversions", [{}, {"trx": {"url": BACKEND_URL}}])
def test_without_a_backend_for_eth_nothing_is_asked(delegated_conversions):
    client, seen, _ = make_client(
        delegated_conversions=delegated_conversions,
        networks={"arb": {"url": BACKEND_URL}},
    )
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
    "part, side",
    [("v1_burn", "from"), ("v2_burn", "from"), ("v1_mint", "to"), ("v2_mint", "to")],
)
def test_a_cctp_tx_gains_the_bridge_the_backend_pairs(part, side):
    tx = FIXTURE[part]["tx_hash"]
    bridge = {**BRIDGE_ROW, f"{side}_asset_transfer": f"{tx[2:]}_T1"}
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
def test_no_row_but_a_bridge_of_the_tx_is_added(row):
    client, seen, _ = make_client(backend_answer=httpx.Response(200, json=[row]))
    assert client.get(CONVERSIONS).json() == [LOCAL_ROW]
    assert len(seen) == 1


def test_a_local_row_naming_a_transfer_of_an_added_bridge_is_dropped():
    """The backend's bridge takes its place; transfers are compared in lower
    case and without 0x."""
    bridge = {**BRIDGE_ROW, "from_asset_transfer": f"{CCTP_BURN[2:].upper()}_T690"}
    client, _, _ = make_client(backend_answer=httpx.Response(200, json=[bridge]))
    assert client.get(CONVERSIONS).json() == [bridge]


@pytest.mark.parametrize(
    "network, identifier",
    [
        ("eth", f"{CCTP_BURN}_T694"),
        ("eth", f"{CCTP_BURN}_I0"),
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


@pytest.mark.parametrize("timeout_s, expected", [(None, 15.0), (4.0, 4.0)])
def test_the_call_waits_delegated_conversions_timeout_s(timeout_s, expected):
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
        ({"X-Ikn-Currency-Opt-Out": "all-light"}, None, False, [LOCAL_ROW]),
        ({"X-User-Roles": "currency-eth"}, ROLES, False, [LOCAL_ROW]),
        ({"X-User-Roles": "currency-bnb"}, ROLES, True, [LOCAL_ROW]),
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


# ---------------------------------------------------------------------------
# without the backend's pairing the local answer stands
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("status", [400, 404, 422, 501])
def test_a_backend_that_cannot_answer_the_tx_leaves_the_local_answer(status):
    """An answer about this tx (e.g. 501, not minted yet) from an up backend:
    nothing is declined, nothing alerts, and the log says why."""
    answer = httpx.Response(status, json={"detail": "cannot pair it"})
    client, _, _ = make_client(backend_answer=answer)
    with records_of_the_middleware() as records:
        response = client.get(CONVERSIONS)
    assert response.json() == [LOCAL_ROW]
    assert DECLINED_HEADER not in response.headers
    (record,) = records
    assert record.levelno == logging.INFO
    assert f"HTTP {status}: cannot pair it" in record.getMessage()


@pytest.mark.parametrize(
    "answer, level",
    [
        (httpx.Response(500, json={"detail": "boom"}), logging.ERROR),
        (httpx.Response(429, headers={"Retry-After": "7"}), logging.WARNING),
        (httpx.Response(200, content=b"<html>busy</html>"), logging.ERROR),
        (httpx.Response(200, json=[{"conversion_type": "bridge_tx"}]), logging.WARNING),
        (httpx.Response(200, json={"detail": "not a list"}), logging.WARNING),
    ],
    ids=["error", "429", "not json", "invalid row", "not a list"],
)
def test_a_failed_backend_leaves_the_local_answer_and_is_named(answer, level):
    client, _, _ = make_client(backend_answer=answer)
    with records_of_the_middleware() as records:
        response = client.get(CONVERSIONS)
    assert response.status_code == 200
    assert response.json() == [LOCAL_ROW]
    assert response.headers[DECLINED_HEADER] == BACKEND_URL
    assert [r.levelno for r in records] == [level]


@pytest.mark.parametrize(
    "error", [httpx.ConnectError("refused"), httpx.ReadTimeout("hung")]
)
def test_an_unreachable_or_hung_backend_is_left_out_for_a_while(error, monkeypatch):
    now = [1000.0]
    monkeypatch.setattr(external_backends, "_now", lambda: now[0])
    client, seen, _ = make_client(fail_with=error)
    with records_of_the_middleware() as records:
        answers = [client.get(CONVERSIONS), client.get(CONVERSIONS)]
    for response in answers:
        assert response.json() == [LOCAL_ROW]
        assert response.headers[DECLINED_HEADER] == BACKEND_URL
    # asked once in its skip window, and one alert saying what it takes out
    assert len(seen) == 1
    (alert,) = [r.getMessage() for r in records if r.levelno == logging.ERROR]
    assert "eth conversions lack the bridges it pairs" in alert
    now[0] += 31
    client.get(CONVERSIONS)
    assert len(seen) == 2


# ---------------------------------------------------------------------------
# rule 1: a proxied conversions answer naming an eth leg Cassandra lacks
# ---------------------------------------------------------------------------

ARB_CONVERSIONS = f"/arb/txs/{ARB_MINT}/conversions"


def arb_client(rows, indexed=(CCTP_BURN,), status=200, content=None):
    """arb served by the backend, which answers `rows` for a conversions
    request; Cassandra has the eth txs `indexed`."""

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
        indexed=indexed,
    )


def eth_tx(tx_hash):
    return bytes.fromhex(tx_hash.removeprefix("0x"))


@pytest.mark.parametrize(
    "rows, indexed, looked_up",
    [
        ([BRIDGE_ROW], (CCTP_BURN,), [CCTP_BURN]),
        # one lookup per eth tx, however its legs write it
        (
            [
                BRIDGE_ROW,
                {**BRIDGE_ROW, "from_asset_transfer": f"0X{CCTP_BURN[2:].upper()}_T1"},
            ],
            (CCTP_BURN,),
            [CCTP_BURN],
        ),
        (
            [BRIDGE_ROW, {**BRIDGE_ROW, "from_asset_transfer": f"{OTHER_ETH_TX}_T1"}],
            (CCTP_BURN, OTHER_ETH_TX),
            [CCTP_BURN, OTHER_ETH_TX],
        ),
        ([{**SWAP_ROW, "from_network": "arb", "to_network": "arb"}], (), []),
        ([{**BRIDGE_ROW, "from_asset_transfer": ""}], (), []),
        (["junk", 1, None], (), []),
    ],
    ids=["indexed", "written twice", "two txs", "no eth leg", "empty", "no rows"],
)
def test_an_answer_whose_eth_legs_are_indexed_passes_through(rows, indexed, looked_up):
    client, seen, db = arb_client(rows, indexed=indexed)
    response = client.get(ARB_CONVERSIONS)
    assert response.status_code == 200
    # byte for byte what the backend sent
    assert response.content == httpx.Response(200, json=rows).content
    assert response.headers[SERVED_BY_HEADER] == SERVED_BY_VALUE
    assert [c.args for c in db.get_tx_by_hash.await_args_list] == [
        ("eth", eth_tx(tx)) for tx in looked_up
    ]
    assert len(seen) == 1


@pytest.mark.parametrize(
    "row",
    [
        BRIDGE_ROW,
        {
            **BRIDGE_ROW,
            "from_network": "arb",
            "from_asset_transfer": f"{ARB_MINT[2:]}_T8",
            "to_network": "eth",
            "to_asset_transfer": f"{OTHER_ETH_TX[2:]}_T3",
        },
        {**BRIDGE_ROW, "from_asset_transfer": "not-a-hash_T1"},
    ],
    ids=["from side", "to side", "no tx hash"],
)
def test_an_answer_naming_an_eth_leg_cassandra_lacks_is_a_501(row):
    """Cassandra lags the chain tip: the dashboard could not load that leg,
    and the bridge would go missing from the graph. The whole answer is
    refused rather than served in part."""
    client, _, _ = arb_client([row], indexed=())
    response = client.get(ARB_CONVERSIONS)
    assert response.status_code == 501
    assert response.json()["code"] == "NOT_IMPLEMENTED"
    assert response.headers[SERVED_BY_HEADER] == SERVED_BY_VALUE


def test_a_failed_lookup_is_a_501_too():
    client, _, db = arb_client([BRIDGE_ROW])
    db.get_tx_by_hash.side_effect = RuntimeError("cassandra down")
    with records_of_the_middleware() as records:
        response = client.get(ARB_CONVERSIONS)
    assert response.status_code == 501
    assert any(
        r.levelno == logging.WARNING and "cassandra down" in r.getMessage()
        for r in records
    )


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


@pytest.mark.parametrize("enabled", [True, False])
def test_delegated_conversions_alone_install_the_middleware(enabled, caplog):
    """A deployment may merge eth conversions without serving a network
    externally; the startup log names the networks."""
    with caplog.at_level(logging.INFO, logger="graphsenselib.web.app"):
        app = create_app_from_dict(
            {
                "database": {"nodes": ["localhost"]},
                "external_backends": {
                    "enabled": enabled,
                    "delegated_conversions": {"eth": {"url": BACKEND_URL}},
                },
            }
        )
    installed = any(m.cls is ExternalBackendMiddleware for m in app.user_middleware)
    assert installed == enabled
    logged = [r.getMessage() for r in caplog.records if "pair" in r.getMessage()]
    assert logged == (
        ["External backends pair the bridges in conversions on: eth"] if enabled else []
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
