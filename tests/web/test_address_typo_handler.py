"""The REST 404 handler answers 400 for an unknown address with a wrong
checksum (a typo), and leaves every other "not found" alone."""

from fastapi import FastAPI
from fastapi.testclient import TestClient

from graphsenselib.errors import AddressNotFoundException, TransactionNotFoundException
from graphsenselib.web.app import _register_exception_handlers

GENESIS = "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa"
TYPO = "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNb"


def client() -> TestClient:
    app = FastAPI()
    _register_exception_handlers(app)

    @app.get("/{currency}/addresses/{address}")
    async def lookup(currency: str, address: str, no_external_txs: bool = False):
        raise AddressNotFoundException(currency, address, no_external_txs)

    @app.get("/{currency}/txs/{tx_hash}")
    async def tx(currency: str, tx_hash: str):
        raise TransactionNotFoundException(currency, tx_hash)

    return TestClient(app)


def test_unknown_address_with_wrong_checksum_is_400():
    response = client().get(f"/btc/addresses/{TYPO}")
    assert response.status_code == 400
    detail = response.json()["detail"]
    assert TYPO in detail
    assert "checksum" in detail


def test_unknown_address_with_valid_checksum_stays_404():
    response = client().get(f"/btc/addresses/{GENESIS}")
    assert response.status_code == 404
    assert response.json()["detail"] == f"Address {GENESIS} not found in network btc"


def test_known_address_without_external_txs_stays_404():
    response = client().get(f"/btc/addresses/{TYPO}?no_external_txs=true")
    assert response.status_code == 404


def test_other_networks_and_not_found_kinds_are_untouched():
    c = client()
    assert c.get("/eth/addresses/0xnotanaddress").status_code == 404
    assert c.get(f"/btc/addresses/nonstandard{'0' * 40}").status_code == 404
    assert c.get(f"/btc/txs/{'0' * 64}").status_code == 404
