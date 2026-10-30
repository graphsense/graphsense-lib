from eth_abi import encode as abi_encode

from graphsenselib.defi.models import Trace
from graphsenselib.defi.swaps import get_swap_from_decoded_logs


def test_1inch_eth_to_token_swap_is_detected_from_call_and_flows():
    sender = "0xedcea136f0f7e5d51e1834bd96937847089fcdd4"
    router = "0x111111125421ca6dc452d289314280a0f8842a65"
    executor = "0x111116053f09d34a7eae8102887004445176ca11"
    weth = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
    token = "0xe92f673ca36c5e2efd2de7628f815f84807e803f"
    amount = 2000000000000000
    weth_amount = 1950000000000000
    token_amount = 13848866016157488

    dlogs = [
        {
            "name": "Deposit",
            "address": weth,
            "parameters": {"dst": executor, "wad": weth_amount},
            "log_def": {"tags": ["weth", "wrap", "token"]},
        },
        {
            "name": "Approval",
            "address": weth,
            "parameters": {"owner": executor, "spender": router, "value": weth_amount},
            "log_def": {"tags": ["token", "erc20"]},
        },
        {
            "name": "Transfer",
            "address": token,
            "parameters": {
                "from": "0x67336cec42645f55059eff241cb02ea5cc52ff86",
                "to": router,
                "value": token_amount,
            },
            "log_def": {"tags": ["token", "erc20"]},
        },
        {
            "name": "Transfer",
            "address": weth,
            "parameters": {
                "from": executor,
                "to": "0x67336cec42645f55059eff241cb02ea5cc52ff86",
                "value": weth_amount,
            },
            "log_def": {"tags": ["token", "erc20"]},
        },
        {
            "name": "Transfer",
            "address": token,
            "parameters": {"from": router, "to": sender, "value": token_amount},
            "log_def": {"tags": ["token", "erc20"]},
        },
    ]
    tx_hash = bytes.fromhex(
        "3e6538270a0b7dbb9bc00a80ee9c4ac5487b9a4dd94c73e753e45538219f0cf2"
    )
    logs_raw = [
        {"tx_hash": tx_hash, "log_index": log_index}
        for log_index in (1858, 1859, 1860, 1862, 1864)
    ]
    traces = [
        Trace(
            from_address=sender,
            to_address=router,
            value=amount,
            is_call=True,
            trace_index=0,
            trace_address="",
        ),
        Trace(
            from_address=router,
            to_address=executor,
            value=amount,
            is_call=True,
            trace_index=1,
            trace_address="1",
        ),
        Trace(
            from_address=executor,
            to_address=weth,
            value=weth_amount,
            is_call=True,
            trace_index=2,
            trace_address="9",
        ),
    ]
    transaction_input = bytes.fromhex("07ed2379") + abi_encode(
        [
            "address",
            "(address,address,address,address,uint256,uint256,uint256)",
            "bytes",
        ],
        [
            executor,
            (
                "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
                token,
                executor,
                sender,
                amount,
                13431294583590000,
                0,
            ),
            b"\x01",
        ],
    )

    result = get_swap_from_decoded_logs(
        dlogs, logs_raw, traces, transaction_input=transaction_input
    )

    assert len(result) == 1
    assert result[0].fromAddress == sender
    assert result[0].toAddress == sender
    assert result[0].fromAsset == "native"
    assert result[0].toAsset == token
    assert result[0].fromAmount == amount
    assert result[0].toAmount == token_amount
    assert result[0].fromPayment.endswith("_I0")
    assert result[0].toPayment.endswith("_T1864")
