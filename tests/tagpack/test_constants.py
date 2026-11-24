from graphsenselib.tagpack.constants import suggest_networks_from_currency


def test_chain_suggestiong():
    assert len(suggest_networks_from_currency("USDT")) > 0

    assert len(suggest_networks_from_currency("ETH")) == 0

    assert len(suggest_networks_from_currency("ETH", only_unknown=False)) == 1


def test_network_aliases_point_to_codes_and_never_shadow_one():
    from graphsenselib.tagpack.constants import (
        KNOWN_NETWORKS,
        NETWORK_ALIASES,
        network_alias_key,
    )

    for alias, code in NETWORK_ALIASES.items():
        assert alias == network_alias_key(alias), alias
        assert alias not in KNOWN_NETWORKS, alias
        assert code == code.upper() and code not in NETWORK_ALIASES, code


def test_suggest_network_code():
    from graphsenselib.tagpack.constants import suggest_network_code

    assert suggest_network_code("bitcoin") == "BTC"
    assert suggest_network_code("Bitcoin Cash") == "BCH"
    assert suggest_network_code("bnb-smart-chain") == "BSC"
    assert suggest_network_code("btc") is None
    assert suggest_network_code("MATIC") is None  # a code, not an alias
    assert suggest_network_code("erc20") is None  # token standard
    assert suggest_network_code("") is None
