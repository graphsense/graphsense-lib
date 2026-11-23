from datetime import datetime

import pandas as pd
import pytest

from graphsenselib.rates import coingecko, coinmarketcap, cryptocompare
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
    df = df.drop(["USD", "EUR"], axis=1)
    assert df.to_dict("records") == []


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


def _utc_ms(day: str, hour: int = 0) -> int:
    from datetime import timezone

    dt = datetime.fromisoformat(day).replace(hour=hour, tzinfo=timezone.utc)
    return int(dt.timestamp()) * 1000


class FakeCoingeckoResponse:
    def __init__(self, payload, status_code=200):
        import json

        self.content = json.dumps(payload).encode()
        self.status_code = status_code

    def raise_for_status(self):
        import requests

        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code} Client Error")


def _candles(days):
    return [[_utc_ms(d), 1.0, 2.0, 0.5, float(i + 1)] for i, d in enumerate(days)]


def _install_fake_coingecko(monkeypatch, responder):
    urls = []

    class FakeSession:
        def get(self, url, headers, timeout):
            assert headers["accept"] == "application/json"
            urls.append(url)
            return responder(url)

    monkeypatch.setattr(coingecko, "rates_session", lambda: FakeSession())
    return urls


def _url_bounds(url):
    from urllib.parse import parse_qs, urlparse

    q = parse_qs(urlparse(url).query)
    return int(q["from"][0]), int(q["to"][0])


def test_coingecko_url_covers_whole_utc_days(monkeypatch):
    # the host time zone must not move the request window
    import time as _time
    from datetime import date

    monkeypatch.setenv("TZ", "America/New_York")
    if hasattr(_time, "tzset"):
        _time.tzset()
    try:
        url = coingecko.coingecko_historical_url(
            "btc", date(2026, 1, 1), date(2026, 1, 2)
        )
    finally:
        monkeypatch.delenv("TZ")
        if hasattr(_time, "tzset"):
            _time.tzset()
    fromd, tod = _url_bounds(url)
    assert fromd == _utc_ms("2026-01-01") // 1000
    assert tod == _utc_ms("2026-01-03") // 1000 - 1


def test_coingecko_parse_uses_utc_dates(monkeypatch):
    import time as _time

    # 00:00 UTC is still the previous day in New York
    monkeypatch.setenv("TZ", "America/New_York")
    if hasattr(_time, "tzset"):
        _time.tzset()
    try:
        df = coingecko.parse_coingecko_historical_response(
            FakeCoingeckoResponse(_candles(["2026-01-01"]))
        )
    finally:
        monkeypatch.delenv("TZ")
        if hasattr(_time, "tzset"):
            _time.tzset()
    assert df.to_dict("records") == [{"date": "2026-01-01", "USD": 1.0}]


def test_coingecko_http_error_surfaces_status():
    import requests

    with pytest.raises(requests.HTTPError, match="401"):
        coingecko.parse_coingecko_historical_response(
            FakeCoingeckoResponse({"status": {"error_code": 10002}}, 401)
        )


def test_coingecko_non_list_payload_is_a_clear_error():
    with pytest.raises(ValueError, match="Unexpected CoinGecko OHLC response"):
        coingecko.parse_coingecko_historical_response(
            FakeCoingeckoResponse({"status": {"error_code": 10002}})
        )


def test_coingecko_batches_have_at_most_180_days(monkeypatch):
    def responder(url):
        fromd, tod = _url_bounds(url)
        days = pd.date_range(
            pd.Timestamp(fromd, unit="s"), pd.Timestamp(tod, unit="s"), freq="D"
        ).strftime("%Y-%m-%d")
        return FakeCoingeckoResponse(_candles(list(days)))

    urls = _install_fake_coingecko(monkeypatch, responder)
    df = coingecko.fetch_coingecko_rates("2026-01-01", "2026-12-31", "btc", "k")

    assert len(urls) == 3
    for url in urls:
        fromd, tod = _url_bounds(url)
        assert (tod + 1 - fromd) // 86400 <= 180
    # every day once, the first day of each batch included
    assert list(df.date) == list(
        pd.date_range("2026-01-01", "2026-12-31").strftime("%Y-%m-%d")
    )


def test_coingecko_drops_out_of_range_candles_and_reports_gaps(monkeypatch, caplog):
    def responder(url):
        # a candle before the window, and no rate for 2026-01-02
        return FakeCoingeckoResponse(
            _candles(["2025-12-31", "2026-01-01", "2026-01-03"])
        )

    _install_fake_coingecko(monkeypatch, responder)
    with caplog.at_level("WARNING"):
        df = coingecko.fetch_coingecko_rates("2026-01-01", "2026-01-03", "btc", "k")

    assert list(df.date) == ["2026-01-01", "2026-01-03"]
    assert "2026-01-02" in caplog.text


def test_coingecko_duplicate_days_raise(monkeypatch):
    _install_fake_coingecko(
        monkeypatch,
        lambda url: FakeCoingeckoResponse(_candles(["2026-01-01", "2026-01-01"])),
    )
    with pytest.raises(ValueError, match="duplicate"):
        coingecko.fetch_coingecko_rates("2026-01-01", "2026-01-01", "btc", "k")


def _fetch_impl_with_usd_days(monkeypatch, usd_days, abort_on_gaps=True):
    monkeypatch.setattr(
        coingecko,
        "fetch_coingecko_rates",
        lambda start, end, c, k: pd.DataFrame(
            {"date": usd_days, "USD": [100.0] * len(usd_days)}
        ),
    )
    monkeypatch.setattr(
        coingecko,
        "fetch_ecb_rates",
        lambda symbols: pd.DataFrame(
            {"date": ["2026-03-16"], "USD": [1.0], "EUR": [0.9]}
        ),
    )
    return coingecko.fetch_impl(
        None,
        "dev",
        "BTC",
        ["USD", "EUR"],
        "2026-03-16",
        "2026-03-19",
        "exchange_rates",
        True,  # force
        False,  # dry_run
        abort_on_gaps,
        "api-key",
    )


def test_coingecko_interior_usd_gap_is_critical(monkeypatch):
    # a hole inside the window never heals on resume -> exit 2
    with pytest.raises(SystemExit) as e:
        _fetch_impl_with_usd_days(
            monkeypatch, ["2026-03-16", "2026-03-18", "2026-03-19"]
        )
    assert e.value.code == 2


def test_coingecko_trailing_usd_gap_is_recoverable(monkeypatch):
    # not yet published at the end of the window -> exit 15, retried next run
    with pytest.raises(SystemExit) as e:
        _fetch_impl_with_usd_days(monkeypatch, ["2026-03-16", "2026-03-17"])
    assert e.value.code == 15


def test_coingecko_usd_gap_without_abort_writes_around_it(monkeypatch):
    df = _fetch_impl_with_usd_days(
        monkeypatch, ["2026-03-16", "2026-03-18", "2026-03-19"], abort_on_gaps=False
    )
    assert list(df.date) == ["2026-03-16", "2026-03-18", "2026-03-19"]
