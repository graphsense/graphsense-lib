from tests.web.helpers import get_json, raw_request
from tests.web.testdata.txs import (
    token_tx1_eth,
    token_tx2_eth,
    tx1,
    tx1_eth_with_identifier,
)


async def test_get_tx(client):
    path = "/{currency}/txs/{tx_hash}?include_io={include_io}"
    result = await get_json(
        client, path, currency="btc", tx_hash="ab1880", include_io=True
    )
    assert tx1.to_dict() == result
    result = await get_json(
        client, path, currency="btc", tx_hash="ab1880", include_io=False
    )
    tx = tx1.to_dict()
    tx.pop("inputs")
    tx.pop("outputs")
    assert tx == result

    result = await get_json(
        client, path, currency="eth", tx_hash="af6e0000", include_io=True
    )
    assert tx1_eth_with_identifier.to_dict() == result

    result = await get_json(
        client, path, currency="eth", tx_hash="af6e0000_I0", include_io=True
    )
    assert tx1_eth_with_identifier.to_dict() == result

    path = "/{currency}/txs/{tx_hash}?token_tx_id=1"
    result = await get_json(client, path, currency="eth", tx_hash="0xaf6e0003")
    assert token_tx1_eth.to_dict() == result

    path = "/{currency}/txs/{tx_hash}?token_tx_id=2"
    result = await get_json(client, path, currency="eth", tx_hash="0xaf6e0003")
    assert token_tx2_eth.to_dict() == result

    invalid_hash = "abcdefg"
    path = "/{currency}/txs/{tx_hash}?include_io={include_io}"
    status, body = await raw_request(
        client, path, currency="eth", tx_hash=invalid_hash, include_io=False
    )
    assert status == 400
    assert (f"{invalid_hash} does not look like a valid transaction hash.") in body

    status, body = await raw_request(
        client, path, currency="btc", tx_hash=invalid_hash, include_io=False
    )
    assert status == 400
    assert (f"{invalid_hash} does not look like a valid transaction hash.") in body

    invalid_hash = "L"
    path = "/{currency}/txs/{tx_hash}?include_io={include_io}"
    status, body = await raw_request(
        client, path, currency="eth", tx_hash=invalid_hash, include_io=False
    )
    assert status == 400
    assert (f"{invalid_hash} does not look like a valid transaction hash.") in body

    status, body = await raw_request(
        client, path, currency="btc", tx_hash=invalid_hash, include_io=False
    )
    assert status == 400
    assert (f"{invalid_hash} does not look like a valid transaction hash.") in body


async def test_list_token_txs(client):
    path = "/{currency}/token_txs/{tx_hash}"
    results = await get_json(client, path, currency="eth", tx_hash="0xaf6e0003")

    assert len(results) == 2
    assert [token_tx1_eth.to_dict(), token_tx2_eth.to_dict()] == results


async def test_get_tx_io(client):
    path = "/{currency}/txs/{tx_hash}/{io}"
    result = await get_json(
        client, path, currency="btc", tx_hash="ab1880", io="inputs"
    )
    assert tx1.to_dict()["inputs"] == result

    result = await get_json(
        client, path, currency="btc", tx_hash="ab1880", io="outputs"
    )
    assert tx1.to_dict()["outputs"] == result


async def test_get_spending_txs(client):
    path = "/{currency}/txs/{tx_hash}/spending"
    result = await get_json(client, path, currency="btc", tx_hash="ab1880")
    assert [{"input_index": 0, "output_index": 0, "tx_hash": "ab"}] == result

    result = await get_json(client, path, currency="btc", tx_hash="ab188013")
    assert [{"input_index": 0, "output_index": 0, "tx_hash": "ab1880"}] == result

    result = await get_json(client, path, currency="btc", tx_hash="00ab188013")
    assert [{"input_index": 0, "output_index": 0, "tx_hash": "ab188013"}] == result

    status, body = await raw_request(client, path, currency="eth", tx_hash="ab")
    assert status == 400
    assert "does not support transaction level linking" in body


async def test_get_spent_in_txs(client):
    path = "/{currency}/txs/{tx_hash}/spent_in"

    result = await get_json(client, path, currency="btc", tx_hash="ab1880")
    assert [{"input_index": 0, "output_index": 0, "tx_hash": "ab188013"}] == result

    result = await get_json(client, path, currency="btc", tx_hash="ab188013")
    assert [{"input_index": 0, "output_index": 0, "tx_hash": "00ab188013"}] == result

    result = await get_json(client, path, currency="btc", tx_hash="00ab188013")
    assert [{"input_index": 0, "output_index": 0, "tx_hash": "000000"}] == result

    status, body = await raw_request(client, path, currency="eth", tx_hash="ab")
    assert status == 400
    assert "does not support transaction level linking" in body
