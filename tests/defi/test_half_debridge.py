"""deBridge DLN half bridges (defi/bridging/halves/debridge.py).

The fixture holds the eth receipt logs of three real mainnet deBridge txs,
copied from the external backend adapter's recordings
(tests/data/bridges/debridge/gslib/eth_logs.json): a native CreatedOrder, an
old-signature fill of an avax order, and one tx filling a bnb order and a
Solana order.
"""

import json
from pathlib import Path

import pytest

from graphsenselib.defi.bridging.delegated import half_bridges
from graphsenselib.defi.bridging.halves import debridge
from graphsenselib.defi.bridging.models import Bridge

FIXTURE = json.loads(
    (
        Path(__file__).parent / "data" / "bridges" / "debridge" / "eth_logs.json"
    ).read_text()
)
DLN_SOURCE = "0xef4fb24ad0916217251f553c0596f8edc630eb66"
DLN_DESTINATION = "0xe7351fd770a37282b91d153ee690b63579d6dd7f"
ETH_USDC = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
ETH_USDT = "0xdac17f958d2ee523a2206206994597c13d831ec7"
BNB_USDC = "0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d"
AVAX_USDC = "0xb97ef9ef8734c71904d8002f8b6bc66dd9c48a6e"
BNB_MAKER = "0x98fc13632ff112e4667fc4f21ae980571f122b5a"
AVAX_MAKER = "0x6a699b852e381bfd36bee888546d5478a324440a"
ETH_SENDER = "0x8706d76ac9de89cd392ad4f3c966ab46312de8ce"


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


def test_debridge_eth_source_half():
    # a native order: the leg is the tx, paying giveAmount + percentFee +
    # nativeFixFee; the order names what arrives on arb; the payer is the tx's
    # sender (half_bridges)
    [half] = half_bridges(
        "eth", db_rows("eth_arb_native"), ("debridge",), tx_sender=ETH_SENDER
    )
    assert half == Bridge(
        fromAddress=ETH_SENDER,
        fromAsset="native",
        fromAmount=24068394746666929 + 9631210382819 + 10**15,
        fromNetwork="eth",
        fromPayment=bare("eth_arb_native"),
        toAddress=ETH_SENDER,
        toAsset="native",
        toAmount=23920851830400002,
        toNetwork="arb",
        toPayment="pending",
        protocol="debridge",
        transferId="0xa73bbde6860120ce7dfa6a8767b8314f0dda1e4a903c48ee437d057c22546f5a",
    )


def test_debridge_eth_destination_half():
    # an old-signature fill of an avax order: the payout is its Transfer, the
    # source side what the order declares
    assert debridge.read("eth", db_rows("avax_eth_old_fill")) == [
        Bridge(
            fromAddress="unknown",  # the maker is not the payer
            fromAsset=AVAX_USDC,
            fromAmount=8157964454,
            fromNetwork="avax",
            fromPayment="unknown",
            toAddress=AVAX_MAKER,
            toAsset=ETH_USDC,
            toAmount=8153473306,
            toNetwork="eth",
            toPayment=f"{bare('avax_eth_old_fill')}_T680",
            protocol="debridge",
            transferId="0x45e24834b5f73a7e50cca0c37358c68948251994522756bc9abd9c108f13b9a8",
        )
    ]


def test_a_batched_fill_is_one_half_per_order():
    # log 222 fills a bnb order (payout log 221), log 234 a Solana one (payout
    # log 233), whose maker and token are not EVM addresses
    first, second = debridge.read("eth", db_rows("bnb_eth_batched_fill"))
    # the bnb order was paid by a forwarder, not by its maker
    assert (first.fromNetwork, first.fromAsset, first.fromAddress) == (
        "bnb",
        BNB_USDC,
        "unknown",
    )
    assert (first.toPayment, first.toAsset, first.toAmount) == (
        f"{bare('bnb_eth_batched_fill')}_T221",
        ETH_USDC,
        299399173274,
    )
    assert (second.fromNetwork, second.fromAsset, second.fromAddress) == (
        "unknown",
        "unknown",
        "unknown",
    )
    assert (second.toPayment, second.toAsset, second.toAmount) == (
        f"{bare('bnb_eth_batched_fill')}_T233",
        ETH_USDT,
        418082679,
    )


def test_debridge_spoofed_emitter_is_ignored():
    for case in FIXTURE:
        logs = [
            dict(log, address="0x" + "66" * 20)
            if log["address"] in (DLN_SOURCE, DLN_DESTINATION)
            else log
            for log in FIXTURE[case]["logs"]
        ]
        assert debridge.read("eth", db_rows(case, logs)) == []


def test_registered_for_eth():
    halves = half_bridges("eth", db_rows("avax_eth_old_fill"), ("debridge",))
    assert [h.toPayment for h in halves] == [f"{bare('avax_eth_old_fill')}_T680"]
    assert half_bridges("eth", db_rows("avax_eth_old_fill"), ("across",)) == ()


# The adapter's own reading of the same receipts (its services/bridges/debridge.py
# over tests/data/bridges/debridge/, the eth legs, 2026-11-19): leg, address,
# asset, amount and transfer id. A pair the adapter answers for a delegated
# half names exactly these legs, so the two readers must agree.
# (case, position, side, leg, address, asset, amount, transfer id)
ADAPTER_LEGS = [
    ("eth_arb_native", 0, "from", None, ETH_SENDER, "native", 25078025957049748,
     "0xa73bbde6860120ce7dfa6a8767b8314f0dda1e4a903c48ee437d057c22546f5a"),
    ("bnb_eth_batched_fill", 0, "to", "_T221", BNB_MAKER, ETH_USDC, 299399173274,
     "0xe568758c8490ec89da01036343920ee243b56e227c13adb9887d02cdb5df6128"),
    ("bnb_eth_batched_fill", 1, "to", "_T233",
     "0x0c8a7f58ab64a1075d54b4b2fb108f27f4bcfc84", ETH_USDT, 418082679,
     "0x7016cb63f3153c745e5937885104c7f9dc3f1b9d0c91642bb0fc53360c959410"),
    ("avax_eth_old_fill", 0, "to", "_T680", AVAX_MAKER, ETH_USDC, 8153473306,
     "0x45e24834b5f73a7e50cca0c37358c68948251994522756bc9abd9c108f13b9a8"),
]  # fmt: skip


@pytest.mark.parametrize(
    "case, position, side, suffix, address, asset, amount, transfer_id", ADAPTER_LEGS
)
def test_the_eth_halves_name_the_adapters_legs(
    case, position, side, suffix, address, asset, amount, transfer_id
):
    # a bare (native) source leg is paid by the tx's sender: the adapter's
    # address for it is that sender
    sender = address if side == "from" else "0x" + "11" * 20
    half = half_bridges("eth", db_rows(case), ("debridge",), tx_sender=sender)[position]
    payment = "fromPayment" if side == "from" else "toPayment"
    assert getattr(half, payment) == bare(case) + (suffix or "")
    assert getattr(half, f"{side}Address") == address
    assert (getattr(half, f"{side}Asset"), getattr(half, f"{side}Amount")) == (
        asset,
        amount,
    )
    assert (half.protocol, half.transferId) == ("debridge", transfer_id)
