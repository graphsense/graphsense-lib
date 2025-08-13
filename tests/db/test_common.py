from graphsenselib.db.asynchronous.services.common import function_call_from_row
from graphsenselib.db.asynchronous.services.common import FunctionCall


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
