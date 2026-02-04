from graphsenselib.web.models import Rates

rate = Rates(
    height=1, rates=[{"code": "eur", "value": 0.0}, {"code": "usd", "value": 0.0}]
)

rate_eth = Rates(
    height=1, rates=[{"code": "eur", "value": 1.0}, {"code": "usd", "value": 2.0}]
)

rate_usdstable_coin = Rates(
    height=1, rates=[{"code": "eur", "value": 0.5}, {"code": "usd", "value": 1.0}]
)


async def get_exchange_rates(test_case):
    result = await test_case.request("/btc/rates/1")
    test_case.assertEqual(rate.to_dict(), result)

    result = await test_case.request("/eth:USDT/rates/1")
    test_case.assertEqual(rate_usdstable_coin.to_dict(), result)

    result = await test_case.request("/eth:weth/rates/1")
    test_case.assertEqual(rate_eth.to_dict(), result)

    result = await test_case.request("/eth/rates/1")
    test_case.assertEqual(rate_eth.to_dict(), result)
