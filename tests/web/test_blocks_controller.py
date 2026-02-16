from graphsenselib.web.models import Block, TxAccount
from tests.web.helpers import get_json, request_with_status
from tests.web.testdata.blocks import block, block2, eth_block, eth_block2
from tests.web.testdata.txs import (
    token_tx1_eth,
    token_tx2_eth,
    tx1,
    tx1_eth,
    tx2_eth,
)


def test_get_block(client):
    path = "/{currency}/blocks/{height}"
    result = get_json(client, path, currency="btc", height=1)
    assert block == Block.from_dict(result)
    result = get_json(client, path, currency="btc", height=2)
    assert block2 == Block.from_dict(result)

    result = get_json(client, path, currency="eth", height=1)
    assert eth_block == Block.from_dict(result)
    result = get_json(client, path, currency="eth", height=2300001)
    assert eth_block2 == Block.from_dict(result)

    request_with_status(client, path, 404, currency="btc", height="0")
    request_with_status(client, path, 404, currency="eth", height="0")


def test_list_block_txs(client):
    path = "/{currency}/blocks/{height}/txs"
    block_txs = [tx1.to_dict()]
    result = get_json(client, path, currency="btc", height=1)
    assert block_txs == result

    result = get_json(client, path, currency="eth", height=2)

    tx22_eth = TxAccount(**tx1_eth.to_dict())
    tx22_eth.tx_hash = "af6e0001"
    tx22_eth.identifier = "af6e0001"
    tx22_eth.height = 2
    eth_txs = [
        tx22_eth.to_dict(),
        tx2_eth.to_dict(),
        token_tx1_eth.to_dict(),
        token_tx2_eth.to_dict(),
    ]

    result = get_json(client, path, currency="eth", height=2300001)
    eth_txs = [tx1_eth.to_dict(), tx22_eth.to_dict()]

    assert eth_txs == result
