"""Wormhole Portal Token Bridge half bridges on eth (defi/bridging/halves/wormhole.py).

The fixture holds the eth receipt logs of five real mainnet txs, recorded by the
external backend adapter (tests/data/bridges/wormhole/gslib/eth_logs.json there):
three Token Bridge sends (to arb, to base with payload, native ETH to Solana), one
redeem of a base send, and a Core message from a sender that is not the bridge.
"""

import json
from pathlib import Path

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
            protocol="wormhole",
            # the VAA id: eth's chain id, its Token Bridge, the sequence
            transferId="2/"
            + "0" * 24
            + "3ee18b2214aff97000d974cf647e7c347e8fa585"
            + "/704561",
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
            protocol="wormhole",
            transferId="2/"
            + "0" * 24
            + "3ee18b2214aff97000d974cf647e7c347e8fa585"
            + "/704590",
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
            protocol="wormhole",
            # TransferRedeemed's topics: base's chain id, its Token Bridge, seq
            transferId="30/"
            + "0" * 24
            + "8d2de8d2f73f1f4cab472ac9a881c9b123c79627"
            + "/285063",
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


TOKEN_BRIDGE = "0x3ee18b2214aff97000d974cf647e7c347e8fa585"
WETH = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
USDC = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
WITHDRAWAL = "0x7fcf532c15f0a6db0bd6d0e038bea71d30d808c7d98cb3bf7268a95bf5081b65"
TRANSFER = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"


def word(value):
    return "0x" + (f"{value:064x}" if isinstance(value, int) else value.rjust(64, "0"))


def indexed(address):
    return word(address.removeprefix("0x"))


def test_wormhole_native_redeem_pays_the_recipient_not_the_relayer_fee():
    """completeTransferAndUnwrapETH with fee > 0: the Token Bridge withdraws the
    fee from WETH and sends it to the relayer, then withdraws the rest for the
    recipient. Two Withdrawals by the bridge: the payout is the second."""
    redeem = FIXTURE["base_eth"]["logs"][0]
    withdrawals = [
        {
            "log_index": 1230 + n,
            "address": WETH,
            "topics": [WITHDRAWAL, indexed(TOKEN_BRIDGE)],
            "data": word(value),
        }
        for n, value in enumerate([12345, 10**18])
    ]
    [half] = wormhole.read("eth", db_rows("base_eth", [redeem, *withdrawals]))
    assert (half.toAddress, half.toAsset, half.toAmount, half.toPayment) == (
        "unknown",  # the coin's recipient is in no log
        "native",
        10**18,
        tx("base_eth"),
    )


def test_wormhole_redeem_payout_is_not_a_later_cctp_mint_of_the_same_token():
    """A redeem releasing USDC from the Token Bridge, then a CCTP V1 mint of USDC
    in the same tx: the mint's Transfer (from 0x0) comes before its own
    MintAndWithdraw, so it lies inside the redeem's segment; it is CCTP's, not a
    second Wormhole payout."""
    redeem, payout = FIXTURE["base_eth"]["logs"][:2]
    payout = dict(payout, address=USDC)
    minted, recipient = 777 * 10**6, "0x" + "ab" * 20
    cctp_mint = [
        {
            "log_index": 1231,
            "address": USDC,
            "topics": [TRANSFER, word(0), indexed(recipient)],
            "data": word(minted),
        },
        {  # MintAndWithdraw(mintRecipient, amount, mintToken), V1 TokenMessenger
            "log_index": 1232,
            "address": "0xbd3fa81b58ba92a82136038b25adec7066af3155",
            "topics": [
                "0x1b2a7ff080b8cb6ff436ce0372e399692bbfb6d4ae5766fd8d58a7b8cc6142e6",
                indexed(recipient),
                indexed(USDC),
            ],
            "data": word(minted),
        },
    ]
    rows = db_rows("base_eth", [redeem, payout, *cctp_mint])
    expected = (
        "0xca0a2515effdb383a572eea1edbb52b2d11a862c",
        USDC,
        158888900610,
        tx("base_eth") + "_T1230",
    )
    [half] = wormhole.read("eth", rows)
    assert (half.toAddress, half.toAsset, half.toAmount, half.toPayment) == expected
    halves = half_bridges("eth", rows, ("wormhole", "cctp"))
    assert [(h.protocol, h.toAddress, h.toPayment) for h in halves] == [
        ("cctp", recipient, tx("base_eth") + "_T1231"),
        ("wormhole", expected[0], expected[3]),
    ]


def test_wormhole_redeem_payout_ends_at_the_next_bridge_event_of_any_family():
    """A Transfer after another bridge's event belongs to that bridge's
    operation, even one the Token Bridge sends of the redeemed token: an Across
    FilledRelay from eth's SpokePool fences the redeem's payout segment."""
    redeem, payout = FIXTURE["base_eth"]["logs"][:2]
    filled = {
        "log_index": 1231,
        "address": "0x5c7bcd6e7de5423a257d81b442095a1a6ced35c5",
        "topics": [
            "0x44b559f101f8fbcc8a0ea43fa91a05a729a5ea6e14a7c75aa750374690137208",
            word(1),
            word(2),
            word(3),
        ],
        "data": "0x",
    }
    later = dict(payout, log_index=1232, data=word(5))
    rows = db_rows("base_eth", [redeem, payout, filled, later])
    [half] = half_bridges("eth", rows, ("wormhole",))
    assert (half.toAmount, half.toPayment) == (158888900610, tx("base_eth") + "_T1230")


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
