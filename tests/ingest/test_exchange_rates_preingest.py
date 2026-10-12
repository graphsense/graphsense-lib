from types import SimpleNamespace

import pytest

from graphsenselib.ingest import cli as ingest_cli


@pytest.fixture
def calls(monkeypatch):
    calls = {"rates": [], "token_rates": []}
    monkeypatch.setattr(
        "graphsenselib.rates.cli.get_api_key", lambda provider: "test-key"
    )
    monkeypatch.setattr(
        "graphsenselib.rates.coingecko.ingest",
        lambda env, currency, *args: calls["rates"].append(currency),
    )
    monkeypatch.setattr(
        "graphsenselib.rates.token_rates.ingest_token_rates",
        lambda env, currency, *args: calls["token_rates"].append(currency),
    )
    return calls


def _run(currency):
    ingest_cli._run_exchange_rates_ingest(
        "test",
        currency,
        SimpleNamespace(exchange_rates_provider="coingecko"),
        use_cassandra=True,
        provider=None,
    )


@pytest.mark.parametrize("currency", ["btc", "ltc", "bch", "zec"])
def test_utxo_currencies_skip_token_rates(calls, currency):
    # UTXO raw keyspaces have no token_configuration table; querying it
    # failed (and logged a warning) on every hourly ingest.
    _run(currency)
    assert calls == {"rates": [currency], "token_rates": []}


@pytest.mark.parametrize("currency", ["eth", "trx"])
def test_account_currencies_ingest_token_rates(calls, currency):
    _run(currency)
    assert calls == {"rates": [currency], "token_rates": [currency]}
