import pandas as pd

from graphsenselib.rates.token_rates import (
    _is_unpegged,
    _to_rows,
    select_unpegged_tokens,
)


def test_is_unpegged():
    assert _is_unpegged(None)
    assert _is_unpegged("")
    assert _is_unpegged("   ")
    assert _is_unpegged(float("nan"))
    assert not _is_unpegged("USD")
    assert not _is_unpegged("ETH")


def test_select_unpegged_tokens_filters_and_keeps_contract():
    df = pd.DataFrame(
        [
            {"currency_ticker": "USDT", "token_address": "0xaa", "peg_currency": "USD"},
            {"currency_ticker": "WETH", "token_address": "0xbb", "peg_currency": "ETH"},
            {"currency_ticker": "UNI", "token_address": "0xcc", "peg_currency": None},
            {"currency_ticker": "FOO", "token_address": "0xdd", "peg_currency": ""},
        ]
    )
    selected = select_unpegged_tokens(df)
    assert [t["ticker"] for t in selected] == ["UNI", "FOO"]
    assert [t["contract"] for t in selected] == ["0xcc", "0xdd"]


def test_select_unpegged_tokens_empty():
    assert select_unpegged_tokens(None) == []
    assert select_unpegged_tokens(pd.DataFrame()) == []


def test_to_rows_collapses_to_fiat_values_map():
    df = pd.DataFrame(
        [
            {"date": "2024-01-01", "USD": 2.0, "EUR": 1.8},
            {"date": "2024-01-02", "USD": 2.5, "EUR": 2.25},
        ]
    )
    rows = _to_rows("UNI", df, ["USD", "EUR"])
    assert rows == [
        {"asset": "UNI", "date": "2024-01-01", "fiat_values": {"USD": 2.0, "EUR": 1.8}},
        {
            "asset": "UNI",
            "date": "2024-01-02",
            "fiat_values": {"USD": 2.5, "EUR": 2.25},
        },
    ]


def test_to_rows_drops_usd_when_not_requested():
    df = pd.DataFrame([{"date": "2024-01-01", "USD": 2.0, "EUR": 1.8}])
    rows = _to_rows("UNI", df, ["EUR"])
    assert rows == [{"asset": "UNI", "date": "2024-01-01", "fiat_values": {"EUR": 1.8}}]
