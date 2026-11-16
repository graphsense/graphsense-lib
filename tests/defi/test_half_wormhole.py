"""Wormhole Portal Token Bridge half bridges on eth (defi/bridging/halves/wormhole.py).

The fixture holds the eth receipt logs of five real mainnet txs, recorded by the
external backend adapter (tests/data/bridges/wormhole/gslib/eth_logs.json there):
three Token Bridge sends (to arb, to base with payload, native ETH to Solana), one
redeem of a base send, and a Core message from a sender that is not the bridge.
"""

import json
from pathlib import Path

from graphsenselib.defi.bridging.halves import wormhole
from graphsenselib.defi.bridging.models import Bridge

FIXTURE = json.loads(
    (
        Path(__file__).parent / "data" / "bridges" / "wormhole" / "eth_logs.json"
    ).read_text()
)
LMP = "0x6eb224fb001ed210e379b335e35efe88672a8ce935d981a6896b27ffdf52a3b2"
REDEEMED = "0xcaf280c8cfeba144da67230d9b009c8f868a75bac9a528fa0474be1ba317c169"


def unhex(s):
    return bytes.fromhex(s.removeprefix("0x"))


def db_rows(part, logs=None):
    """Logs of one fixture tx as rows of the raw log table."""
    tx = FIXTURE[part]
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


def tx(part):
    return FIXTURE[part]["tx_hash"].removeprefix("0x")


def test_wormhole_eth_source_half():
    assert wormhole.read("eth", db_rows("eth_arb")) == [
        Bridge(
            fromAddress="0xd7ca08ec1aee9cce8a8eda9365343ef197674e1a",
            fromAsset="0x3010ccb5419f1ef26d40a7cd3f0d707a0fa127dc",
            # payload amount 10859944826158 (8 decimals) of an 18-decimal token
            fromAmount=108599448261580000000000,
            fromNetwork="eth",
            fromPayment=tx("eth_arb") + "_T104",
            toAddress="0xd7ca08ec1aee9cce8a8eda9365343ef197674e1a",
            toAsset="unknown",
            toAmount=0,
            toNetwork="arb",
            toPayment="pending",
        )
    ]


def test_wormhole_eth_source_half_with_payload():
    [half] = wormhole.read("eth", db_rows("eth_base_with_payload"))
    assert (half.fromPayment, half.fromAmount) == (
        tx("eth_base_with_payload") + "_T993",
        300706830000000000,
    )
    assert (half.toNetwork, half.toAddress, half.toPayment) == (
        "base",
        "0x09959798b95d00a3183d20fac298e4594e599eab",
        "pending",
    )


def test_wormhole_eth_native_source_half_to_solana():
    """WETH Deposit to the bridge, no Transfer: the leg is the tx; Solana is no
    network here, so the other side is all unknown. The second Core message,
    from sender 0xbf5f3f65, is not the Token Bridge's."""
    assert wormhole.read("eth", db_rows("eth_solana_native")) == [
        Bridge(
            fromAddress="unknown",  # the payer is the tx sender, not in the logs
            fromAsset="native",
            fromAmount=49950000000000000,
            fromNetwork="eth",
            fromPayment=tx("eth_solana_native"),
            toAddress="unknown",
            toAsset="unknown",
            toAmount=0,
            toNetwork="unknown",
            toPayment="pending",
        )
    ]


def test_wormhole_eth_destination_half():
    assert wormhole.read("eth", db_rows("base_eth")) == [
        Bridge(
            fromAddress="unknown",
            fromAsset="unknown",
            fromAmount=0,
            fromNetwork="base",
            fromPayment="unknown",
            toAddress="0xca0a2515effdb383a572eea1edbb52b2d11a862c",
            toAsset="0x582d872a1b094fc48f5de31d3b73f2d9be47def1",
            toAmount=158888900610,
            toNetwork="eth",
            toPayment=tx("base_eth") + "_T1230",
        )
    ]


def test_wormhole_destination_half_pays_the_recipient_not_the_relayer_fee():
    """A legacy type-1 redeem with fee > 0: the Token Bridge pays the fee to the
    relayer (msg.sender) first, then the rest to the recipient. The recorded
    base_eth redeem with a fee Transfer of the same token inserted before its
    payout; the external backend adapter names the same leg."""
    redeem, payout, *rest = FIXTURE["base_eth"]["logs"]
    fee = dict(
        payout,
        topics=[payout["topics"][0], payout["topics"][1], "0x" + "0" * 24 + "fe" * 20],
        data="0x" + f"{12345:064x}",
    )
    logs = [redeem] + [
        dict(log, log_index=1230 + n) for n, log in enumerate([fee, payout, *rest])
    ]
    [half] = wormhole.read("eth", db_rows("base_eth", logs))
    assert (half.toAddress, half.toAsset, half.toAmount, half.toPayment) == (
        "0xca0a2515effdb383a572eea1edbb52b2d11a862c",
        "0x582d872a1b094fc48f5de31d3b73f2d9be47def1",
        158888900610,
        tx("base_eth") + "_T1231",
    )


def test_wormhole_spoofed_emitter_is_ignored():
    # a real Core message whose sender is not the Token Bridge
    assert wormhole.read("eth", db_rows("eth_core_other_sender")) == []
    # the recorded send and redeem, emitted by another contract
    impostor = "0x" + "66" * 20
    for part, topic0 in (("eth_arb", LMP), ("base_eth", REDEEMED)):
        logs = [
            dict(log, address=impostor) if log["topics"][0] == topic0 else log
            for log in FIXTURE[part]["logs"]
        ]
        assert wormhole.read("eth", db_rows(part, logs)) == []


def test_wormhole_other_network_has_no_halves_without_its_contracts():
    assert wormhole.read("gnosis", db_rows("eth_arb")) == []
