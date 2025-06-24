from tagpack.constants import suggest_networks_from_currency


def test_chain_suggestiong():
    assert len(suggest_networks_from_currency("USDT")) > 0

    assert len(suggest_networks_from_currency("ETH")) == 0

    assert len(suggest_networks_from_currency("ETH", only_unknown=False)) == 1
