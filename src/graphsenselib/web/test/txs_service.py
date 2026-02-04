from graphsenselib.web.models import TxAccount, TxUtxo, TxValue
from graphsenselib.web.util.values_legacy import make_values

tx1 = TxUtxo(
    height=1,
    currency="btc",
    total_output=make_values(eur=0.0, usd=0.0, value=75137389),
    coinbase=False,
    outputs=[
        TxValue(
            address=["addressE"],
            value=make_values(eur=0.0, usd=0.0, value=75000000),
        ),
        TxValue(
            address=["addressA"],
            value=make_values(eur=0.0, usd=0.0, value=137389),
        ),
    ],
    inputs=[
        TxValue(
            address=["addressA"],
            value=make_values(eur=0.0, usd=0.0, value=75110000),
        ),
        TxValue(
            address=["addressA"],
            value=make_values(eur=0.0, usd=0.0, value=37389),
        ),
    ],
    no_inputs=2,
    no_outputs=2,
    timestamp=1434554207,
    total_input=make_values(eur=0.0, usd=0.0, value=75147389),
    tx_hash="ab1880",
)

tx2 = TxUtxo(
    timestamp=1373266967,
    currency="btc",
    total_output=make_values(eur=0.1, usd=0.2, value=9950000),
    coinbase=False,
    tx_hash="ab188013",
    total_input=make_values(eur=0.1, usd=0.2, value=10000000),
    outputs=[
        TxValue(
            address=["138cWsiAGpW9yqfjMVCCsFcnaiSHyoWMnJ"],
            value=make_values(eur=0.0, usd=0.0, value=1),
        ),
        TxValue(
            address=["1CRqaDkksF1zp6wLunY7ygSafXsiftH9FN"],
            value=make_values(eur=0.1, usd=0.2, value=9949999),
        ),
    ],
    inputs=[
        TxValue(
            address=["1H8omroLCN2578Mj6sDrWa8YueqWEckNKY"],
            value=make_values(eur=0.1, usd=0.2, value=10000000),
        )
    ],
    no_inputs=1,
    no_outputs=2,
    height=2,
)

tx3 = TxUtxo(
    timestamp=1373266967,
    currency="btc",
    total_output=make_values(eur=0.1, usd=0.2, value=9950000),
    coinbase=False,
    tx_hash="00ab188013",
    total_input=make_values(eur=0.1, usd=0.2, value=10000000),
    outputs=[
        TxValue(
            address=["138cWsiAGpW9yqfjMVCCsFcnaiSHyoWMnJ"],
            value=make_values(eur=0.0, usd=0.0, value=1),
        ),
        TxValue(
            address=["1CRqaDkksF1zp6wLunY7ygSafXsiftH9FN"],
            value=make_values(eur=0.1, usd=0.2, value=9949999),
        ),
    ],
    inputs=[
        TxValue(
            address=["1H8omroLCN2578Mj6sDrWa8YueqWEckNKY"],
            value=make_values(eur=0.1, usd=0.2, value=10000000),
        )
    ],
    no_inputs=1,
    no_outputs=2,
    height=2,
)

tx1_eth = TxAccount(
    tx_hash="af6e0000",
    currency="eth",
    network="eth",
    height=1,
    timestamp=15,
    from_address="0xbe370541310b13922e209515ebffe8d459e050",
    to_address="0xd6cc43ac66902302074a19e52bacc68d15421551",
    identifier="af6e0000",
    value=make_values(eur=123.0, usd=246.0, value=123000000000000000000),
    is_external=True,
)


tx2_eth = TxAccount(
    tx_hash="af6e0003",
    currency="eth",
    network="eth",
    height=2,
    timestamp=16,
    from_address="0xabcdef",
    to_address="0x123456",
    identifier="af6e0003",
    value=make_values(eur=123.0, usd=246.0, value=123000000000000000000),
    is_external=True,
)

tx22_eth = TxAccount(
    tx_hash="af6e0004",
    identifier="af6e0004",
    currency="eth",
    network="eth",
    height=3,
    timestamp=17,
    from_address="0xabcdef",
    to_address="0x123456",
    value=make_values(eur=124.0, usd=248.0, value=124000000000000000000),
    is_external=True,
)

tx3_eth = TxAccount(
    tx_hash="ab188013",
    identifier="ab188013",
    currency="eth",
    network="eth",
    height=1,
    timestamp=17,
    from_address="0xabcdef",
    to_address="0x123456",
    value=make_values(eur=234.0, usd=468.0, value=234000000000000000000),
    is_external=True,
)

tx4_eth = TxAccount(
    tx_hash="123456",
    identifier="123456",
    currency="eth",
    network="eth",
    height=4,
    timestamp=17,
    from_address="0xabcdef",
    to_address="0x123456",
    value=make_values(eur=234.0, usd=468.0, value=234000000000000000000),
    is_external=True,
)


tx1_eth_with_identifier = TxAccount(**tx1_eth.to_dict())
tx1_eth_with_identifier.identifier = "af6e0000_I0"
tx1_eth_with_identifier.contract_creation = False

tx2_eth_with_identifier = TxAccount(**tx2_eth.to_dict())
tx2_eth_with_identifier.identifier = "af6e0003_I1"
tx2_eth_with_identifier.contract_creation = False

tx22_eth_with_identifier = TxAccount(**tx22_eth.to_dict())
tx22_eth_with_identifier.identifier = "af6e0004_I0"
tx22_eth_with_identifier.contract_creation = False

tx3_eth_with_identifier = TxAccount(**tx3_eth.to_dict())
tx3_eth_with_identifier.identifier = "ab188013_I1"
tx3_eth_with_identifier.contract_creation = False

tx4_eth_with_identifier = TxAccount(**tx4_eth.to_dict())
tx4_eth_with_identifier.identifier = "123456_I0"
tx4_eth_with_identifier.contract_creation = False

# {'tx_type': 'account',
# 'token_tx_id': 1,
# 'currency': 'weth',
# 'tx_hash': 'af6e0000',
# 'height': 1,
# 'timestamp': 15,
# 'value': {'fiat_values': [{'code': 'eur', 'value': 6.82},
# {'code': 'usd', 'value': 13.64}], 'value': 6818627949560085517},
# 'from_address': '0x06729eb2424da47898f935267bd4a62940de5105',
# 'to_address': '0xbeefbabeea323f07c59926295205d3b7a17e8638'}
token_tx1_eth = TxAccount(
    tx_hash="af6e0003",
    identifier="af6e0003_T1",
    tx_type="account",
    currency="weth",
    network="eth",
    height=2,
    timestamp=16,
    token_tx_id=1,
    from_address="0x06729eb2424da47898f935267bd4a62940de5105",
    to_address="0xbeefbabeea323f07c59926295205d3b7a17e8638",
    value=make_values(eur=6.82, usd=13.64, value=6818627949560085517),
)

# {'tx_type': 'account',
# 'token_tx_id': 2,
# 'currency': 'usdt',
# 'tx_hash': 'af6e0000',
# 'height': 1,
# 'timestamp': 15,
# 'value': {'fiat_values': [{'code': 'usd', 'value': 3360.49},
# {'code': 'eur', 'value': 1680.24}], 'value': 3360488227},
# 'from_address': '0x45225d3536ac02928f16071ab05066bce95c2cd5',
# 'to_address': '0xcaf7ce56598e8588c9bf471e08b53e8a8d9541b3'}
token_tx2_eth = TxAccount(
    tx_hash="af6e0003",
    identifier="af6e0003_T2",
    currency="usdt",
    network="eth",
    tx_type="account",
    height=2,
    timestamp=16,
    token_tx_id=2,
    from_address="0x45225d3536ac02928f16071ab05066bce95c2cd5",
    to_address="0xcaf7ce56598e8588c9bf471e08b53e8a8d9541b3",
    value=make_values(eur=1680.24, usd=3360.49, value=3360488227),
)


async def get_tx(test_case):
    path = "/{currency}/txs/{tx_hash}?include_io={include_io}"
    result = await test_case.request(
        path, currency="btc", tx_hash="ab1880", include_io=True
    )
    test_case.assertEqual(tx1.to_dict(), result)
    result = await test_case.request(
        path, currency="btc", tx_hash="ab1880", include_io=False
    )
    tx = tx1.to_dict()
    tx.pop("inputs")
    tx.pop("outputs")

    test_case.assertEqual(tx, result)

    result = await test_case.request(
        path, currency="eth", tx_hash="af6e0000", include_io=True
    )
    test_case.assertEqual(tx1_eth_with_identifier.to_dict(), result)

    result = await test_case.request(
        path, currency="eth", tx_hash="af6e0000_I0", include_io=True
    )
    test_case.assertEqual(tx1_eth_with_identifier.to_dict(), result)

    path = "/{currency}/txs/{tx_hash}?token_tx_id=1"
    result = await test_case.request(path, currency="eth", tx_hash="0xaf6e0003")

    test_case.assertEqual(token_tx1_eth.to_dict(), result)

    path = "/{currency}/txs/{tx_hash}?token_tx_id=2"
    result = await test_case.request(path, currency="eth", tx_hash="0xaf6e0003")

    test_case.assertEqual(token_tx2_eth.to_dict(), result)

    invalid_hash = "abcdefg"
    path = "/{currency}/txs/{tx_hash}?include_io={include_io}"
    result, body = await test_case.requestOnly(
        path, None, currency="eth", tx_hash=invalid_hash, include_io=False
    )

    assert result.status == 400
    assert (f"{invalid_hash} does not look like a valid transaction hash.") in body

    result, body = await test_case.requestOnly(
        path, None, currency="btc", tx_hash=invalid_hash, include_io=False
    )
    assert result.status == 400
    assert (f"{invalid_hash} does not look like a valid transaction hash.") in body

    invalid_hash = "L"
    path = "/{currency}/txs/{tx_hash}?include_io={include_io}"
    result, body = await test_case.requestOnly(
        path, None, currency="eth", tx_hash=invalid_hash, include_io=False
    )

    assert result.status == 400
    assert (f"{invalid_hash} does not look like a valid transaction hash.") in body

    result, body = await test_case.requestOnly(
        path, None, currency="btc", tx_hash=invalid_hash, include_io=False
    )
    assert result.status == 400
    assert (f"{invalid_hash} does not look like a valid transaction hash.") in body


async def list_token_txs(test_case):
    path = "/{currency}/token_txs/{tx_hash}"
    results = await test_case.request(path, currency="eth", tx_hash="0xaf6e0003")

    assert len(results) == 2
    test_case.assertEqual([token_tx1_eth.to_dict(), token_tx2_eth.to_dict()], results)


async def get_tx_io(test_case):
    path = "/{currency}/txs/{tx_hash}/{io}"
    result = await test_case.request(
        path, currency="btc", tx_hash="ab1880", io="inputs"
    )

    test_case.assertEqual(tx1.to_dict()["inputs"], result)
    result = await test_case.request(
        path, currency="btc", tx_hash="ab1880", io="outputs"
    )
    test_case.assertEqual(tx1.to_dict()["outputs"], result)


async def get_spending_txs(test_case):
    path = "/{currency}/txs/{tx_hash}/spending"
    result = await test_case.request(path, currency="btc", tx_hash="ab1880")

    test_case.assertEqual(
        [{"input_index": 0, "output_index": 0, "tx_hash": "ab"}], result
    )

    result = await test_case.request(path, currency="btc", tx_hash="ab188013")

    test_case.assertEqual(
        [{"input_index": 0, "output_index": 0, "tx_hash": "ab1880"}], result
    )

    result = await test_case.request(path, currency="btc", tx_hash="00ab188013")

    test_case.assertEqual(
        [{"input_index": 0, "output_index": 0, "tx_hash": "ab188013"}], result
    )

    result, body = await test_case.requestOnly(path, None, currency="eth", tx_hash="ab")
    assert result.status == 400
    assert "does not support transaction level linking" in body


async def get_spent_in_txs(test_case):
    path = "/{currency}/txs/{tx_hash}/spent_in"

    result = await test_case.request(path, currency="btc", tx_hash="ab1880")

    test_case.assertEqual(
        [{"input_index": 0, "output_index": 0, "tx_hash": "ab188013"}], result
    )

    result = await test_case.request(path, currency="btc", tx_hash="ab188013")

    test_case.assertEqual(
        [{"input_index": 0, "output_index": 0, "tx_hash": "00ab188013"}], result
    )

    result = await test_case.request(path, currency="btc", tx_hash="00ab188013")

    test_case.assertEqual(
        [{"input_index": 0, "output_index": 0, "tx_hash": "000000"}], result
    )

    result, body = await test_case.requestOnly(path, None, currency="eth", tx_hash="ab")
    assert result.status == 400
    assert "does not support transaction level linking" in body
