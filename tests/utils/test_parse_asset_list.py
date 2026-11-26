import pytest

from graphsenselib.utils.rest_utils import parse_asset_list


@pytest.mark.parametrize(
    "value, expected",
    [
        (None, None),
        ("", None),
        (" , ,", None),
        ("weth", ["WETH"]),
        ("eth,USDT", ["ETH", "USDT"]),
        (" usdt , eth ", ["USDT", "ETH"]),
        ("weth,WETH,eth", ["WETH", "ETH"]),
    ],
)
def test_parse_asset_list(value, expected):
    assert parse_asset_list(value) == expected
