"""Wormhole Portal Token Bridge half bridges (defi/bridging/halves/wormhole.py).

The fixture holds the eth receipt logs of eight real mainnet txs, copied from
the external backend adapter's recordings (tests/data/bridges/wormhole/gslib/
eth_logs.json): a send to arb, a send with payload (tBTC) to base, two native
sends to Solana, the redeem of a base send, a redeem released straight from the
bridge, a native redeem, and a Core message that is not the Token Bridge's.
"""

import copy
import json
from pathlib import Path

import pytest

from graphsenselib.defi.bridging.delegated import half_bridges
from graphsenselib.defi.bridging.halves import wormhole
from graphsenselib.defi.bridging.models import Bridge

FIXTURE = json.loads(
    (
        Path(__file__).parent / "data" / "bridges" / "wormhole" / "eth_logs.json"
    ).read_text()
)
LMP = "0x6eb224fb001ed210e379b335e35efe88672a8ce935d981a6896b27ffdf52a3b2"
REDEEMED = "0xcaf280c8cfeba144da67230d9b009c8f868a75bac9a528fa0474be1ba317c169"
ETH_BRIDGE = "0000000000000000000000003ee18b2214aff97000d974cf647e7c347e8fa585"
BASE_BRIDGE = "0000000000000000000000008d2de8d2f73f1f4cab472ac9a881c9b123c79627"
SOLANA_BRIDGE = "ec7372995d5cc8732397fb0ad35c0121e0eaa90d26f828a534cab54391b3a4f5"
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


def test_wormhole_eth_source_half():
    # the leg is the Transfer into the Token Bridge, paid by a router contract
    # (half_bridges: whoever paid the leg), which is also the recipient on arb
    [half] = half_bridges("eth", db_rows("eth_arb"), ("wormhole",), tx_sender=SENDER)
    assert half == Bridge(
        fromAddress="0xd7ca08ec1aee9cce8a8eda9365343ef197674e1a",
        fromAsset="0x3010ccb5419f1ef26d40a7cd3f0d707a0fa127dc",
        fromAmount=108599448261580000000000,
        fromNetwork="eth",
        fromPayment=f"{bare('eth_arb')}_T104",
        toAddress="0xd7ca08ec1aee9cce8a8eda9365343ef197674e1a",
        toAsset="unknown",
        toAmount=0,
        toNetwork="arb",
        toPayment="pending",
        protocol="wormhole",
        protocolApp=None,
        transferId=f"2/{ETH_BRIDGE}/704561",
    )


def test_wormhole_eth_destination_half():
    # the bridge releases the locked token right after TransferRedeemed; what
    # the recipient contract does with it afterwards is not the payout
    [half] = half_bridges("eth", db_rows("base_eth"), ("wormhole",))
    assert half == Bridge(
        fromAddress="unknown",
        fromAsset="unknown",
        fromAmount=0,
        fromNetwork="base",
        fromPayment="unknown",
        toAddress="0xca0a2515effdb383a572eea1edbb52b2d11a862c",
        toAsset="0x582d872a1b094fc48f5de31d3b73f2d9be47def1",
        toAmount=158888900610,
        toNetwork="eth",
        toPayment=f"{bare('base_eth')}_T1230",
        protocol="wormhole",
        protocolApp=None,
        transferId=f"30/{BASE_BRIDGE}/285063",
    )


def test_a_native_send_half_is_the_bare_tx_to_an_unnamed_chain():
    # wrapAndTransferETH to Solana (chain 1): a WETH Deposit to the bridge, no
    # Transfer; the same tx holds a Core message of another sender, no half
    [half] = half_bridges(
        "eth", db_rows("eth_solana_native"), ("wormhole",), tx_sender=SENDER
    )
    assert (half.fromPayment, half.fromAsset, half.fromAmount, half.fromAddress) == (
        bare("eth_solana_native"),
        "native",
        49950000000000000,
        SENDER,
    )
    assert (half.toNetwork, half.toAddress, half.toPayment) == (
        "unknown",
        "unknown",
        "pending",
    )


def test_a_native_redeem_half_is_the_bare_tx_and_names_no_recipient():
    # completeTransferAndUnwrapETH: a WETH Withdrawal by the bridge; the coin
    # goes out in an internal call, which no log shows
    [half] = half_bridges("eth", db_rows("eth_native_redeem"), ("wormhole",))
    assert (half.toPayment, half.toAsset, half.toAmount, half.toAddress) == (
        bare("eth_native_redeem"),
        "native",
        2035100000000000000,
        "unknown",
    )
    assert (half.fromNetwork, half.fromPayment) == ("unknown", "unknown")
    assert half.transferId == f"1/{SOLANA_BRIDGE}/632049"


def test_a_core_message_of_another_sender_is_no_half():
    assert wormhole.read("eth", db_rows("eth_core_other_sender")) == []


def test_a_spoofed_emitter_is_ignored():
    for case, topic0 in [("eth_arb", LMP), ("base_eth", REDEEMED)]:
        logs = copy.deepcopy(FIXTURE[case]["logs"])
        for log in logs:
            if log["topics"][0] == topic0:
                log["address"] = SPOOF
        assert wormhole.read("eth", db_rows(case, logs)) == []


def test_registered_for_eth_only():
    assert half_bridges("eth", db_rows("eth_arb"), ("layerzero",)) == ()
    assert wormhole.read("eth", db_rows("eth_arb")) != []
    assert half_bridges("arb", db_rows("eth_arb"), ("wormhole",)) == ()


# The adapter's own reading of the same receipts (its services/bridges/wormhole.py
# over tests/data/bridges/wormhole/, the eth legs, 2026-11-20): leg, address,
# asset, amount and transfer id. A pair the adapter answers for a delegated half
# names exactly these legs, so the two readers must agree.
# (case, side, leg, address, asset, amount, transfer id)
ADAPTER_LEGS = [
    ("eth_arb", "from", "_T104", "0xd7ca08ec1aee9cce8a8eda9365343ef197674e1a",
     "0x3010ccb5419f1ef26d40a7cd3f0d707a0fa127dc", 108599448261580000000000,
     f"2/{ETH_BRIDGE}/704561"),
    ("eth_base_with_payload", "from", "_T993",
     "0x5b3a59d923cff08e3d53f80d5ea2b9e94a0b3b86",
     "0x18084fba666a33d37592fa2633fd49a74dd93a88", 300706830000000000,
     f"2/{ETH_BRIDGE}/704559"),
    ("eth_solana_native", "from", None, "0x95aaefab86f5f9332390a2c1b8114353f341a5f7",
     "native", 49950000000000000, f"2/{ETH_BRIDGE}/704590"),
    ("eth_native_send_2023", "from", None,
     "0x53617da7e1022dd39d16a93644f7f79a56de902e", "native", 324756680000000000,
     f"2/{ETH_BRIDGE}/108258"),
    ("base_eth", "to", "_T1230", "0xca0a2515effdb383a572eea1edbb52b2d11a862c",
     "0x582d872a1b094fc48f5de31d3b73f2d9be47def1", 158888900610,
     f"30/{BASE_BRIDGE}/285063"),
    ("eth_release_0x7af368d5", "to", "_T386",
     "0xfdff0b569f14af593d446e51b3e42f502124ac85",
     "0x73d7c860998ca3c01ce8c808f5577d94d545d1b4", 5083230640160000000000,
     f"30/{BASE_BRIDGE}/258014"),
    ("eth_native_redeem", "to", None, "unknown", "native", 2035100000000000000,
     f"1/{SOLANA_BRIDGE}/632049"),
]  # fmt: skip


@pytest.mark.parametrize(
    "case, side, suffix, address, asset, amount, transfer_id", ADAPTER_LEGS
)
def test_the_eth_halves_name_the_adapters_legs(
    case, side, suffix, address, asset, amount, transfer_id
):
    # a bare (native) source leg is paid by the tx's sender: the adapter's
    # address for it is that sender
    sender = address if side == "from" else SENDER
    [half] = half_bridges("eth", db_rows(case), ("wormhole",), tx_sender=sender)
    payment = "fromPayment" if side == "from" else "toPayment"
    assert getattr(half, payment) == bare(case) + (suffix or "")
    assert getattr(half, f"{side}Address") == address
    assert (getattr(half, f"{side}Asset"), getattr(half, f"{side}Amount")) == (
        asset,
        amount,
    )
    assert (half.protocol, half.protocolApp, half.transferId) == (
        "wormhole",
        None,
        transfer_id,
    )
