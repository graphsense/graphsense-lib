from graphsenselib.defi.swapping.models import (
    SwapStrategy,
    get_swap_strategy_from_decoded_logs,
)


def test_1inch_swap_call_marks_logs_as_swap():
    dlogs = [
        {
            "name": "Deposit",
            "log_def": {"tags": ["weth", "wrap", "token"]},
        }
    ]
    parsed_input = {
        "function_def": {"tags": ["swap", "1inch"]},
    }

    assert get_swap_strategy_from_decoded_logs(dlogs, parsed_input) is SwapStrategy.SWAP


def test_unknown_function_call_does_not_change_log_strategy():
    dlogs = [
        {
            "name": "Deposit",
            "log_def": {"tags": ["weth", "wrap", "token"]},
        }
    ]

    assert get_swap_strategy_from_decoded_logs(dlogs) is SwapStrategy.UNKNOWN
