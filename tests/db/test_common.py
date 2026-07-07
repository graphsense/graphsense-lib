import pytest

from graphsenselib.db.asynchronous.services.common import function_call_from_row
from graphsenselib.db.asynchronous.services.common import FunctionCall
from graphsenselib.db.asynchronous.services.common import (
    convert_token_value,
    get_address as get_address_common,
    map_rates_for_peged_tokens,
)
from graphsenselib.errors import AddressNotFoundException

RATES = [{"code": "eur", "value": 0.9}, {"code": "usd", "value": 1.0}]


def test_map_rates_for_unpegged_token_returns_empty():
    # Unpegged / unknown / missing peg -> no fiat conversion (no exception).
    assert map_rates_for_peged_tokens(RATES, {"peg_currency": None}) == []
    assert map_rates_for_peged_tokens(RATES, {"peg_currency": ""}) == []
    assert map_rates_for_peged_tokens(RATES, {"peg_currency": "SOL"}) == []


def test_map_rates_for_usd_pegged_token():
    r = map_rates_for_peged_tokens(RATES, {"peg_currency": "USD"})
    assert {i["code"]: i["value"] for i in r} == {"eur": 0.9, "usd": 1}


def test_convert_token_value_unpegged_has_no_fiat_values():
    token_config = {"peg_currency": None, "decimal_divisor": 10**6}
    result = convert_token_value(2_500_000, RATES, token_config)
    # raw amount preserved, but no fiat conversion
    assert result.value == 2_500_000
    assert result.fiat_values == []


def test_convert_token_value_usd_pegged_has_fiat_values():
    token_config = {"peg_currency": "USD", "decimal_divisor": 10**6}
    result = convert_token_value(2_500_000, RATES, token_config)
    fiat = {f.code: f.value for f in result.fiat_values}
    assert result.value == 2_500_000
    assert fiat == {"usd": 2.5, "eur": 2.25}


def test_convert_token_value_unpegged_with_rate_uses_token_rate():
    # A fetched per-token rate (fiat-per-whole-token) prices an unpegged token.
    token_config = {"peg_currency": None, "decimal_divisor": 10**6}
    token_rate = [{"code": "eur", "value": 2.0}, {"code": "usd", "value": 2.5}]
    result = convert_token_value(2_500_000, RATES, token_config, token_rate=token_rate)
    fiat = {f.code: f.value for f in result.fiat_values}
    assert result.value == 2_500_000
    assert fiat == {"eur": 5.0, "usd": 6.25}


def test_map_rates_for_unpegged_token_uses_supplied_rate():
    token_rate = [{"code": "eur", "value": 2.0}, {"code": "usd", "value": 2.5}]
    assert (
        map_rates_for_peged_tokens(RATES, {"peg_currency": None}, token_rate=token_rate)
        == token_rate
    )
    # no rate -> empty (zero fiat fallback)
    assert map_rates_for_peged_tokens(RATES, {"peg_currency": None}) == []


def test_function_call_from_row_none():
    assert function_call_from_row(None) is None


def test_function_call_from_row_with_function_def_and_parameters():
    parsed_input = {
        "inputs": [
            {"name": "foo", "type": "int", "value": "1"},
            {"name": "bar", "type": "int", "value": "2"},
        ],
        "parameters": {"foo": 1, "bar": 2},
        "function_def": {
            "name": "transfer",
            "inputs": [
                {"name": "to", "type": "address"},
                {"name": "value", "type": "uint256"},
            ],
            "tags": ["token", "transfer"],
        },
        "selector": "0xa9059cbb",
    }
    result = function_call_from_row(parsed_input)
    assert isinstance(result, FunctionCall)
    assert any(
        pd.name == "foo" and pd.type == "int" and pd.value == "1"
        for pd in result.parameter_details
    )
    assert result.parameter_values == {"foo": 1, "bar": 2}
    assert result.function_definition.name == "transfer"
    assert result.function_definition.selector == "0xa9059cbb"
    assert len(result.function_definition.arguments) == 2
    assert result.function_definition.arguments[0].name == "to"
    assert result.function_definition.arguments[0].type == "address"
    assert result.function_definition.arguments[1].name == "value"
    assert result.function_definition.arguments[1].type == "uint256"


def test_function_call_from_row_with_empty_inputs():
    parsed_input = {
        "inputs": [],
        "parameters": {},
        "function_def": {"name": "approve", "inputs": []},
        "selector": "0x095ea7b3",
    }
    result = function_call_from_row(parsed_input)
    assert isinstance(result, FunctionCall)
    assert result.function_definition.name == "approve"
    assert result.function_definition.selector == "0x095ea7b3"
    assert result.function_definition.arguments == []


def test_real_function_call_from_row():
    """
    This function is a placeholder for the actual implementation of function_call_from_row.
    It should be replaced with the real function when running tests.
    """

    parsed_input = {
        "name": "swapEthForTokens",
        "selector": "0xff190b9f",
        "inputs": [
            {
                "name": "token",
                "type": "address",
                "value": "0x9acb099a6460dead936fe7e591d2c875ae4d84b8",
            },
            {"name": "amountOutMin", "type": "uint256", "value": 3704579385785495},
            {"name": "deadline", "type": "uint256", "value": 1755001877},
        ],
        "parameters": {
            "token": "0x9acb099a6460dead936fe7e591d2c875ae4d84b8",
            "amountOutMin": 3704579385785495,
            "deadline": 1755001877,
        },
        "function_def": {
            "name": "swapEthForTokens",
            "inputs": [
                {"name": "token", "type": "address"},
                {"name": "amountOutMin", "type": "uint256"},
                {"name": "deadline", "type": "uint256"},
            ],
            "tags": ["swap", "weth"],
        },
    }

    result = function_call_from_row(parsed_input)

    assert result.function_definition.name == "swapEthForTokens"
    assert result.function_definition.selector == "0xff190b9f"
    assert result.function_definition.tags == ["swap", "weth"]


async def test_get_address_without_fallback_propagates_not_found():
    class FakeDb:
        async def get_address(self, currency, address):
            raise AddressNotFoundException(currency, address)

        async def new_address(self, currency, address):
            raise AssertionError("new_address fallback must not be used")

    with pytest.raises(AddressNotFoundException):
        await get_address_common(
            FakeDb(),
            None,  # tagstore unused: include_actors=False
            None,  # rates_service unused: raises before conversion
            "btc",
            "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa",
            [],
            include_actors=False,
            new_address_fallback=False,
        )
