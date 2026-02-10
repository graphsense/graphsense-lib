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
