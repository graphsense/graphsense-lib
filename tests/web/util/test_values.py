from graphsenselib.web.util.values_legacy import convert_value


def test_converted_value():
    rates = [{"code": "eur", "value": 0.4}, {"code": "usd", "value": 0.7}]

    value = convert_value("btc", 175000000000, rates)

    assert value.value == 175000000000
    assert value.fiat_values[0].value == 700
    assert value.fiat_values[1].value == 1225

    value = convert_value("eth", 1750000000000000000000, rates)

    assert value.value == 1750000000000000000000
    assert value.fiat_values[0].value == 700
    assert value.fiat_values[1].value == 1225
