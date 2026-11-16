"""Across half bridges (defi/bridging/halves/across.py).

The fixture holds the eth receipt logs of five real mainnet Across txs on eth's
SpokePool, copied from the external backend adapter's recordings
(tests/data/bridges/across/gslib/eth_logs.json): two deposits (one in each ABI
era) and three fills, native and ERC20.
"""

import json
from pathlib import Path

from graphsenselib.defi.bridging.delegated import half_bridges
from graphsenselib.defi.bridging.halves import across
from graphsenselib.defi.bridging.models import Bridge

FIXTURE = json.loads(
    (
        Path(__file__).parent / "data" / "bridges" / "across" / "eth_logs.json"
    ).read_text()
)
SPOKE_POOL = "0x5c7bcd6e7de5423a257d81b442095a1a6ced35c5"
USDC = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
HANDLER = "0x0f7ae28de1c8532170ad4ee566b5801485c13a0e"  # Across multicall handler


def unhex(s):
    return bytes.fromhex(s.removeprefix("0x"))


def db_rows(case, logs=None):
    """Logs of one fixture tx as rows of the raw log table."""
    tx = FIXTURE[case]
    return [
        {
            "tx_hash": unhex(tx["tx_hash"]),
            "block_id": tx["block_id"],
            "log_index": log["log_index"],
            "address": unhex(log["address"]),
            "topics": [unhex(t) for t in log["topics"]],
            "topic0": unhex(log["topics"][0]),
            "data": unhex(log["data"]),
        }
        for log in (tx["logs"] if logs is None else logs)
    ]


def bare(case):
    return FIXTURE[case]["tx_hash"].removeprefix("0x")


def test_across_eth_source_half():
    # native ETH: the SpokePool wraps msg.value (WETH Deposit), no Transfer, so
    # the leg is the tx; base is the destination the deposit names
    assert across.read("eth", db_rows("eth_base_native")) == [
        Bridge(
            fromAddress="0x42fda7e1f87cc910fcdaaa18ee0f7a8ecb0c8f64",
            fromAsset="native",
            fromAmount=12628350647202970,
            fromNetwork="eth",
            fromPayment=bare("eth_base_native"),
            toAddress="0x42fda7e1f87cc910fcdaaa18ee0f7a8ecb0c8f64",
            toAsset="0x4200000000000000000000000000000000000006",
            toAmount=12623766337738166,
            toNetwork="base",
            toPayment="pending",
        )
    ]


def test_across_eth_legacy_source_half():
    # V3FundsDeposited: address-typed fields, uint32 depositId
    [half] = across.read("eth", db_rows("eth_pol_v3_legacy"))
    assert (half.fromAsset, half.fromPayment, half.fromAmount) == (
        "native",
        bare("eth_pol_v3_legacy"),
        0x4536EE82B69AD8AC,
    )
    assert (half.toNetwork, half.toAddress, half.toAsset, half.toPayment) == (
        "pol",
        "0x89f423567c2648bb828c3997f60c47b54f57fa6e",
        "0x7ceb23fd6bc0add59e62ac25578270cff1b9f619",
        "pending",
    )


def test_across_eth_autotoken_deposit_names_no_asset():
    # an old-era deposit may name outputToken 0x0 ("any"): no asset declared
    [half] = across.read("eth", db_rows("eth_opt_v3_legacy_autotoken"))
    assert (half.toNetwork, half.toAsset) == ("opt", "unknown")


def test_across_eth_destination_half():
    # USDC paid by the relayer to the recipient, the multicall handler, after
    # the fill's event; pol is the origin the fill names
    assert across.read("eth", db_rows("pol_eth_usdc_fill")) == [
        Bridge(
            fromAddress="0x871291282f281563e5179d5d89388fce837dd462",
            fromAsset="0x3c499c542cef5e3811e1192ce70d8cc03d5c3359",
            fromAmount=4995039812,
            fromNetwork="pol",
            fromPayment="unknown",
            toAddress=HANDLER,
            toAsset=USDC,
            toAmount=4993359522,
            toNetwork="eth",
            toPayment=f"{bare('pol_eth_usdc_fill')}_T867",
        )
    ]


def test_across_eth_native_destination_half():
    # the SpokePool unwraps the relayer's WETH (Withdrawal) and pays ETH: the tx
    [half] = across.read("eth", db_rows("base_eth_weth_fill"))
    assert (half.toAsset, half.toPayment, half.toAmount, half.toAddress) == (
        "native",
        bare("base_eth_weth_fill"),
        17736178764405541,
        "0xa9c9259ca0efe93e2af7bda7d8f438683d4b1fd6",
    )
    assert (half.fromNetwork, half.fromPayment) == ("base", "unknown")


def test_across_spoofed_emitter_is_ignored():
    for case in FIXTURE:
        logs = [
            dict(log, address="0x" + "66" * 20) if log["address"] == SPOKE_POOL else log
            for log in FIXTURE[case]["logs"]
        ]
        assert across.read("eth", db_rows(case, logs)) == []


def test_unserved_destination_is_unknown():
    logs = [dict(log) for log in FIXTURE["eth_base_native"]["logs"]]
    deposit = next(log for log in logs if log["address"] == SPOKE_POOL)
    # Across' chain id for Solana
    deposit["topics"] = [
        deposit["topics"][0],
        "0x" + f"{34268394551451:064x}",
        *deposit["topics"][2:],
    ]
    [half] = across.read("eth", db_rows("eth_base_native", logs))
    assert (half.toNetwork, half.toAddress, half.toAsset, half.toPayment) == (
        "unknown",
        "unknown",
        "unknown",
        "pending",
    )


def test_registered_for_eth():
    halves = half_bridges("eth", db_rows("pol_eth_usdc_fill"), ("across",))
    assert [h.toPayment for h in halves] == [f"{bare('pol_eth_usdc_fill')}_T867"]
    assert half_bridges("eth", db_rows("pol_eth_usdc_fill"), ("cctp",)) == ()
