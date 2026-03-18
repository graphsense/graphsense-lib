from datetime import datetime

import pandas as pd
import pytest

from graphsenselib.rates import coindesk, coingecko, coinmarketcap, cryptocompare
from graphsenselib.rates.cryptocompare import fetch_impl

from ..helpers import vcr_default_params


class DummyRaw:
    def __init__(self, last_date: datetime):
        self.last_date = last_date

    @staticmethod
    def get_keyspace():
        return "btc_raw_dev"

    def get_last_exchange_rate_date(self, table=None):
        assert table == "exchange_rates"
        return self.last_date


class DummyDb:
    def __init__(self, last_date: datetime):
        self.raw = DummyRaw(last_date)


@pytest.mark.vcr(**vcr_default_params)
def test_rates_fetching():
    df = fetch_impl(
        None,
        "BTC",
        ["USD", "EUR"],
        "2024-01-01T00:00:00.000000+00:00",
        "2025-01-01T00:00:00.000000+00:00",
        None,
        False,
        False,
        False,
    )

    assert len(df.dropna()) == 303


def test_rates_fetching_normalizes_naive_db_dates(monkeypatch):
    seen = {}

    def fake_fetch_cryptocompare_rates(start, end, symbol, fiat):
        seen["start"] = start
        seen["end"] = end
        seen["symbol"] = symbol
        seen["fiat"] = fiat
        return pd.DataFrame({"date": ["2026-03-17"], "USD": [100.0]})

    def fake_fetch_ecb_rates(symbol_list):
        assert symbol_list == ["USD", "EUR"]
        return pd.DataFrame({"date": ["2026-03-17"], "USD": [1.0], "EUR": [0.9]})

    monkeypatch.setattr(
        cryptocompare, "fetch_cryptocompare_rates", fake_fetch_cryptocompare_rates
    )
    monkeypatch.setattr(cryptocompare, "fetch_ecb_rates", fake_fetch_ecb_rates)

    df = fetch_impl(
        DummyDb(datetime(2026, 3, 17)),
        "BTC",
        ["USD", "EUR"],
        "2026-03-01T00:00:00.000000+00:00",
        "2026-03-17T00:00:00.000000+00:00",
        "exchange_rates",
        False,
        False,
        True,
    )

    assert seen == {
        "start": "2026-03-17T00:00:00+00:00",
        "end": "2026-03-17T00:00:00+00:00",
        "symbol": "BTC",
        "fiat": "USD",
    }
    assert df.to_dict("records") == [{"date": "2026-03-17", "USD": 100.0, "EUR": 90.0}]


def test_coindesk_normalizes_naive_db_dates(monkeypatch):
    seen = {}

    def fake_fetch_exchange_rates(start_date, end_date, symbol_list):
        seen["start"] = start_date
        seen["end"] = end_date
        seen["symbols"] = symbol_list
        return pd.DataFrame(
            [{"date": "2026-03-17", "fiat_values": {"USD": 100.0, "EUR": 90.0}}]
        )

    monkeypatch.setattr(coindesk, "fetch_exchange_rates", fake_fetch_exchange_rates)

    df = coindesk.fetch_impl(
        DummyDb(datetime(2026, 3, 17)),
        "dev",
        "BTC",
        ["USD", "EUR"],
        "2026-03-01T00:00:00.000000+00:00",
        "2026-03-17T00:00:00.000000+00:00",
        "exchange_rates",
        False,
        False,
        False,
    )

    assert seen == {
        "start": "2026-03-17",
        "end": "2026-03-17",
        "symbols": ["USD", "EUR"],
    }
    assert df.to_dict("records") == [
        {"date": "2026-03-17", "fiat_values": {"USD": 100.0, "EUR": 90.0}}
    ]


def test_coingecko_normalizes_naive_db_dates(monkeypatch):
    seen = {}

    def fake_fetch_coingecko_rates(start, end, crypto_currency, api_key):
        seen["start"] = start
        seen["end"] = end
        seen["currency"] = crypto_currency
        seen["api_key"] = api_key
        return pd.DataFrame({"date": ["2026-03-17"], "USD": [100.0]})

    def fake_fetch_ecb_rates(symbol_list):
        assert symbol_list == ["USD", "EUR"]
        return pd.DataFrame({"date": ["2026-03-17"], "USD": [1.0], "EUR": [0.9]})

    monkeypatch.setattr(coingecko, "fetch_coingecko_rates", fake_fetch_coingecko_rates)
    monkeypatch.setattr(coingecko, "fetch_ecb_rates", fake_fetch_ecb_rates)

    df = coingecko.fetch_impl(
        DummyDb(datetime(2026, 3, 17)),
        "dev",
        "BTC",
        ["USD", "EUR"],
        "2026-03-01T00:00:00.000000+00:00",
        "2026-03-17T00:00:00.000000+00:00",
        "exchange_rates",
        False,
        False,
        True,
        "api-key",
    )

    assert seen == {
        "start": "2026-03-17",
        "end": "2026-03-17",
        "currency": "BTC",
        "api_key": "api-key",
    }
    assert df.to_dict("records") == [{"date": "2026-03-17", "USD": 100.0, "EUR": 90.0}]


def test_coinmarketcap_normalizes_naive_db_dates(monkeypatch):
    seen = {}

    def fake_fetch_cmc_rates(start, end, crypto_currency, api_key):
        seen["start"] = start
        seen["end"] = end
        seen["currency"] = crypto_currency
        seen["api_key"] = api_key
        return pd.DataFrame({"date": ["2026-03-17"], "USD": [100.0]})

    def fake_fetch_ecb_rates(symbol_list):
        assert symbol_list == ["USD", "EUR"]
        return pd.DataFrame({"date": ["2026-03-17"], "USD": [1.0], "EUR": [0.9]})

    monkeypatch.setattr(coinmarketcap, "fetch_cmc_rates", fake_fetch_cmc_rates)
    monkeypatch.setattr(coinmarketcap, "fetch_ecb_rates", fake_fetch_ecb_rates)

    df = coinmarketcap.fetch_impl(
        DummyDb(datetime(2026, 3, 17)),
        "dev",
        "BTC",
        ["USD", "EUR"],
        "2026-03-01T00:00:00.000000+00:00",
        "2026-03-17T00:00:00.000000+00:00",
        "exchange_rates",
        False,
        False,
        True,
        "api-key",
    )

    assert seen == {
        "start": "2026-03-17",
        "end": "2026-03-17",
        "currency": "BTC",
        "api_key": "api-key",
    }
    assert df.to_dict("records") == [{"date": "2026-03-17", "USD": 100.0, "EUR": 90.0}]
