"""Unit tests for the _slim() response-shape helper applied to every
consolidated tool response.
"""

from graphsenselib.mcp.tools.consolidated import _slim


def test_basic_money():
    obj = {
        "fiat_values": [
            {"code": "eur", "value": 1.0},
            {"code": "usd", "value": 1.1},
        ],
        "value": 100,
    }
    assert _slim(obj) == {"native": 100, "eur": 1.0, "usd": 1.1}


def test_empty_fiat_values_still_flattens():
    obj = {"fiat_values": [], "value": 100}
    assert _slim(obj) == {"native": 100}


def test_extra_keys_preserved_alongside_money():
    obj = {
        "fiat_values": [{"code": "eur", "value": 1.0}],
        "value": 100,
        "timestamp": 1234,
    }
    assert _slim(obj) == {"native": 100, "eur": 1.0, "timestamp": 1234}


def test_nested_money_objects():
    obj = {
        "balance": {
            "fiat_values": [{"code": "eur", "value": 1.0}],
            "value": 100,
        },
        "fees": {
            "fiat_values": [{"code": "usd", "value": 0.5}],
            "value": 1000,
        },
    }
    assert _slim(obj) == {
        "balance": {"native": 100, "eur": 1.0},
        "fees": {"native": 1000, "usd": 0.5},
    }


def test_non_money_dict_with_value_key_untouched():
    # A dict that has `value` but no `fiat_values` is NOT a money object
    # and must be left alone.
    obj = {"value": 100, "other": "x"}
    assert _slim(obj) == {"value": 100, "other": "x"}


def test_malformed_fiat_values_passthrough():
    # `fiat_values` exists but entries aren't `{code, value}` dicts —
    # detection must refuse, return the dict unchanged (keys deep-slimmed).
    obj = {"fiat_values": [1, 2, 3], "value": 100}
    assert _slim(obj) == {"fiat_values": [1, 2, 3], "value": 100}


def test_list_of_money_objects():
    obj = [
        {"fiat_values": [{"code": "eur", "value": 1.0}], "value": 100},
        {"fiat_values": [{"code": "eur", "value": 2.0}], "value": 200},
    ]
    assert _slim(obj) == [
        {"native": 100, "eur": 1.0},
        {"native": 200, "eur": 2.0},
    ]


def test_money_nested_inside_list_inside_dict():
    obj = {
        "neighbors": [
            {
                "address": "abc",
                "value": {
                    "fiat_values": [{"code": "usd", "value": 3.0}],
                    "value": 42,
                },
            },
        ],
    }
    assert _slim(obj) == {
        "neighbors": [{"address": "abc", "value": {"native": 42, "usd": 3.0}}]
    }


def test_primitives_pass_through():
    assert _slim(42) == 42
    assert _slim(0) == 0
    assert _slim("hello") == "hello"
    assert _slim(None) is None
    assert _slim(True) is True
    assert _slim(3.14) == 3.14


def test_empty_containers():
    assert _slim({}) == {}
    assert _slim([]) == []


def test_real_shaped_address_response():
    # Representative slice of a real graphsense /addresses/{addr} response.
    obj = {
        "address": "1A1zP...",
        "balance": {
            "fiat_values": [
                {"code": "eur", "value": 6731775.5},
                {"code": "usd", "value": 7916567.79},
            ],
            "value": 10718923032,
        },
        "total_received": {
            "fiat_values": [
                {"code": "eur", "value": 1708466.12},
                {"code": "usd", "value": 1887294.5},
            ],
            "value": 10718923032,
        },
        "no_incoming_txs": 62658,
    }
    slim = _slim(obj)
    assert slim == {
        "address": "1A1zP...",
        "balance": {"native": 10718923032, "eur": 6731775.5, "usd": 7916567.79},
        "total_received": {
            "native": 10718923032,
            "eur": 1708466.12,
            "usd": 1887294.5,
        },
        "no_incoming_txs": 62658,
    }
