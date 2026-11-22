"""Relay half bridges (defi/bridging/halves/relay.py).

The fixture holds the eth receipt logs of seven real mainnet txs, copied from
the external backend adapter's recordings (tests/data/bridges/relay/gslib/
eth_logs.json): a native deposit, a USDC deposit, a USDC deposit paid by a
contract (filled on arb), a USDT deposit filled on Solana, a USDT deposit that
was refunded, that refund, and a native fill.
"""

import copy
import json
from pathlib import Path

import pytest

from graphsenselib.defi.bridging.delegated import DELEGATED_BRIDGES, half_bridges
from graphsenselib.defi.bridging.models import Bridge

FIXTURE = json.loads(
    (Path(__file__).parent / "data" / "bridges" / "relay" / "eth_logs.json").read_text()
)
USDC = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
USDT = "0xdac17f958d2ee523a2206206994597c13d831ec7"
SPOOF = "0x" + "5f" * 20
SENDER = "0x" + "11" * 20


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


def test_relay_is_a_delegated_bridge():
    assert "relay" in DELEGATED_BRIDGES


def test_relay_eth_deposit_half():
    # the leg is the Transfer into the Depository, paid by the contract that
    # deposited for the user (half_bridges: whoever paid the leg); a deposit
    # says nothing about where it is filled
    [half] = half_bridges("eth", db_rows("eth_arb_usdc"), ("relay",), tx_sender=SENDER)
    assert half == Bridge(
        fromAddress="0x9a47f3289794e9bbc6a3c571f6d96ad4e7baed16",
        fromAsset=USDC,
        fromAmount=83743881,
        fromNetwork="eth",
        fromPayment=f"{bare('eth_arb_usdc')}_T115",
        toAddress="unknown",
        toAsset="unknown",
        toAmount=0,
        toNetwork="unknown",
        toPayment="pending",
        protocol="relay",
        protocolApp=None,
        transferId="0xa03f8bcf3729836411c47bdcf049fa0acdf33ad64ee0f0d7edaa887da436ea37",
    )


def test_relay_native_deposit_is_the_tx_paid_by_its_sender():
    [half] = half_bridges(
        "eth", db_rows("eth_native_deposit"), ("relay",), tx_sender=SENDER
    )
    assert (half.fromPayment, half.fromAddress) == (bare("eth_native_deposit"), SENDER)
    assert (half.fromAsset, half.fromAmount) == ("native", 6775771778909430)


# The adapter's own reading of the same receipts (its services/bridges/relay.py
# over tests/data/bridges/relay/): leg, payer, asset, amount and transfer id. A
# pair the adapter answers for a delegated half replaces it only when it names
# the same leg.
ADAPTER_LEGS = [
    ("eth_native_deposit", None, "0x68bbe4ff1dd925ad8e21f90551af342a223fac60",
     "native", 6775771778909430,
     "0xb278138a886ce306942ff60db4232a4c1193bcf8e7def91091e1a8eaf75ad16a"),
    ("eth_usdc_deposit", "_T349", "0x5da070e8be8a1aef4aaa98898e11e86af4d806d4",
     USDC, 179842013,
     "0x96735c7bb1920b796cd2bb66933982d887db39e72ba0ab3823cac43ba95c85d2"),
    ("eth_arb_usdc", "_T115", "0x9a47f3289794e9bbc6a3c571f6d96ad4e7baed16",
     USDC, 83743881,
     "0xa03f8bcf3729836411c47bdcf049fa0acdf33ad64ee0f0d7edaa887da436ea37"),
    ("eth_solana", "_T162", "0x0038dfb2bbc353daa6ad22cb11ee1d78f41b59cd",
     USDT, 11282149,
     "0xd362c534a14b0c89daf412a7a323aca1321d4692edcd1ca57e3d534c47472885"),
    ("eth_refund", "_T1234", "0x63cc0b58dd84300df66b11eb89dbce43f78afe5d",
     USDT, 4304880,
     "0x1139866f67a6560a943820be476f6f95430535aa5a97588d2ef7e1dd52dbb679"),
]  # fmt: skip


@pytest.mark.parametrize("case, suffix, address, asset, amount, order", ADAPTER_LEGS)
def test_the_eth_halves_name_the_adapters_legs(
    case, suffix, address, asset, amount, order
):
    # a bare (native) leg is paid by the tx's sender: the adapter's address for it
    [half] = half_bridges("eth", db_rows(case), ("relay",), tx_sender=address)
    assert half.fromPayment == bare(case) + (suffix or "")
    assert (half.fromAddress, half.fromAsset, half.fromAmount) == (
        address,
        asset,
        amount,
    )
    assert (half.protocol, half.protocolApp, half.transferId) == ("relay", None, order)
    assert (half.toNetwork, half.toPayment) == ("unknown", "pending")


def test_a_deposit_event_of_another_contract_is_no_half():
    logs = copy.deepcopy(FIXTURE["eth_arb_usdc"]["logs"])
    for log in logs:
        if log["log_index"] == 116:
            log["address"] = SPOOF
    assert half_bridges("eth", db_rows("eth_arb_usdc", logs), ("relay",)) == ()


@pytest.mark.parametrize("case", ["eth_refund_tx", "eth_native_fill"])
def test_a_fill_is_no_half(case):
    """A fill logs nothing of Relay's (its order id is in the calldata), and a
    refund looks the same: eth fills are not delegated."""
    assert half_bridges("eth", db_rows(case), ("relay",)) == ()


def test_relay_is_read_on_eth_only_and_only_when_included():
    rows = db_rows("eth_arb_usdc")
    assert half_bridges("arb", rows, ("relay",)) == ()
    assert half_bridges("eth", rows, ("wormhole",)) == ()
