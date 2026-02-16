from tests.web.helpers import get_json
from tests.web.testdata.rates import rate, rate_eth, rate_usdstable_coin


def test_get_exchange_rates(client):
    result = get_json(client, "/btc/rates/1")
    assert rate.to_dict() == result

    result = get_json(client, "/eth:USDT/rates/1")
    assert rate_usdstable_coin.to_dict() == result

    result = get_json(client, "/eth:weth/rates/1")
    assert rate_eth.to_dict() == result

    result = get_json(client, "/eth/rates/1")
    assert rate_eth.to_dict() == result
