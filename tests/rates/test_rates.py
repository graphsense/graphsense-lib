import pytest

from graphsenselib.rates.cryptocompare import fetch_impl

from ..helpers import vcr_default_params


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
