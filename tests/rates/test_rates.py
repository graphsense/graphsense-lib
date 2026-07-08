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
        "",  # no api_key so the request matches the recorded cassette
    )

    assert len(df.dropna()) == 303


def test_rates_fetching_normalizes_naive_db_dates(monkeypatch):
    seen = {}

    def fake_fetch_cryptocompare_rates(start, end, symbol, fiat, api_key):
        seen["start"] = start
        seen["end"] = end
        seen["symbol"] = symbol
        seen["fiat"] = fiat
        seen["api_key"] = api_key
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
        "api-key",
    )

    assert seen == {
        "start": "2026-03-17T00:00:00+00:00",
        "end": "2026-03-17T00:00:00+00:00",
        "symbol": "BTC",
        "fiat": "USD",
        "api_key": "api-key",
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


def test_coingecko_forward_fills_weekend_fx_gap(monkeypatch):
    """Monday update whose window is only Sat+Sun: the ECB has not published
    weekend FX, so the anchoring Friday rate lies *before* the window. The
    rate must be forward-filled (not aborted) so the import continues."""

    def fake_fetch_coingecko_rates(start, end, crypto_currency, api_key):
        # coingecko provides crypto USD prices for the weekend days
        return pd.DataFrame(
            {"date": ["2026-06-20", "2026-06-21"], "USD": [475.79, 471.43]}
        )

    def fake_fetch_ecb_rates(symbol_list):
        # ECB history ends on Friday 2026-06-19 (no weekend rates)
        return pd.DataFrame(
            {
                "date": ["2026-06-18", "2026-06-19"],
                "USD": [1.0, 1.0],
                "EUR": [0.89, 0.90],
            }
        )

    monkeypatch.setattr(coingecko, "fetch_coingecko_rates", fake_fetch_coingecko_rates)
    monkeypatch.setattr(coingecko, "fetch_ecb_rates", fake_fetch_ecb_rates)

    # last imported rate is Saturday -> window normalizes to Sat..Sun
    df = coingecko.fetch_impl(
        DummyDb(datetime(2026, 6, 20)),
        "dev",
        "ZEC",
        ["USD", "EUR"],
        "2026-06-20T00:00:00.000000+00:00",
        "2026-06-21T00:00:00.000000+00:00",
        "exchange_rates",
        False,  # force
        False,  # dry_run
        True,  # abort_on_gaps
        "api-key",
    )

    # Friday's EUR fx (0.90) is carried forward onto both weekend days
    assert df.to_dict("records") == [
        {"date": "2026-06-20", "USD": 475.79, "EUR": 475.79 * 0.90},
        {"date": "2026-06-21", "USD": 471.43, "EUR": 471.43 * 0.90},
    ]


def _raise_no_fetch(*args, **kwargs):
    raise AssertionError("no rates should be fetched when the DB is ahead")


def test_cryptocompare_resume_past_end_is_noop(monkeypatch):
    # Manual forward-fill can leave a rate for *today* in the DB while the
    # fetch window ends *yesterday*; that is nothing-to-fetch, not an error.
    monkeypatch.setattr(cryptocompare, "fetch_cryptocompare_rates", _raise_no_fetch)

    df = fetch_impl(
        DummyDb(datetime(2026, 7, 8)),
        "BTC",
        ["USD", "EUR"],
        "2026-06-01T00:00:00.000000+00:00",
        "2026-07-07T00:00:00.000000+00:00",
        "exchange_rates",
        False,
        False,
        True,
        "api-key",
    )

    assert len(df) == 0
    # the empty frame must survive the ingest() post-processing steps
    df["fiat_values"] = df.drop("date", axis=1).to_dict(orient="records")
    df.drop(["USD", "EUR"], axis=1, inplace=True)
    assert df.to_dict("records") == []


def test_coindesk_resume_past_end_is_noop(monkeypatch):
    monkeypatch.setattr(coindesk, "fetch_exchange_rates", _raise_no_fetch)

    df = coindesk.fetch_impl(
        DummyDb(datetime(2026, 7, 8)),
        "dev",
        "BTC",
        ["USD", "EUR"],
        "2026-06-01T00:00:00.000000+00:00",
        "2026-07-07T00:00:00.000000+00:00",
        "exchange_rates",
        False,
        False,
        True,
    )

    assert len(df) == 0


def test_coingecko_resume_past_end_is_noop(monkeypatch):
    monkeypatch.setattr(coingecko, "fetch_coingecko_rates", _raise_no_fetch)

    df = coingecko.fetch_impl(
        DummyDb(datetime(2026, 7, 8)),
        "dev",
        "ZEC",
        ["USD", "EUR"],
        "2026-06-01T00:00:00.000000+00:00",
        "2026-07-07T00:00:00.000000+00:00",
        "exchange_rates",
        False,
        False,
        True,
        "api-key",
    )

    assert len(df) == 0


def test_coinmarketcap_resume_past_end_is_noop(monkeypatch):
    monkeypatch.setattr(coinmarketcap, "fetch_cmc_rates", _raise_no_fetch)

    df = coinmarketcap.fetch_impl(
        DummyDb(datetime(2026, 7, 8)),
        "dev",
        "BTC",
        ["USD", "EUR"],
        "2026-06-01T00:00:00.000000+00:00",
        "2026-07-07T00:00:00.000000+00:00",
        "exchange_rates",
        False,
        False,
        True,
        "api-key",
    )

    assert len(df) == 0


def test_explicit_start_after_end_still_errors():
    # force=True skips the DB resume lookup: a user-supplied start date
    # beyond the end date remains a hard error.
    with pytest.raises(SystemExit):
        fetch_impl(
            None,
            "BTC",
            ["USD", "EUR"],
            "2026-07-09T00:00:00.000000+00:00",
            "2026-07-07T00:00:00.000000+00:00",
            "exchange_rates",
            True,
            False,
            True,
            "api-key",
        )
