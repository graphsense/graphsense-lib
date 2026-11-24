"""Bridges graphsense-lib finds only half of (defi/bridging/delegated.py).

The fixture holds the eth receipt logs of four real mainnet Circle CCTP txs: a
V1 and a V2 burn (DepositForBurn) and a V1 and a V2 mint (MintAndWithdraw),
each emitted by Circle's eth TokenMessenger of that version.
"""

import json
import logging
from pathlib import Path

import pytest

from graphsenselib.defi.bridging.delegated import DELEGATED_BRIDGES, half_bridges
from graphsenselib.defi.bridging.halves import layerzero, wormhole
from graphsenselib.defi.bridging.models import Bridge

FIXTURE = json.loads((Path(__file__).parent / "cctp_eth_logs.json").read_text())

V1_TOKEN_MESSENGER = "0xbd3fa81b58ba92a82136038b25adec7066af3155"
V2_TOKEN_MESSENGER = "0x28b5a0e9c621a5badaa536219b3a228c8168cf5d"
V2_DEPOSIT_FOR_BURN = (
    "0x0c8c1cbdc5190613ebd485511d4e2812cfa45eecb79d845893331fedad5130a5"
)
USDC = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
CCTP = ("cctp",)


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


def without_cctp_events(part):
    """The tx's logs minus those its TokenMessenger emitted."""
    return [
        log
        for log in FIXTURE[part]["logs"]
        if log["address"] not in (V1_TOKEN_MESSENGER, V2_TOKEN_MESSENGER)
    ]


def bare(part):
    return FIXTURE[part]["tx_hash"].removeprefix("0x")


# Each one's eth leg is the Transfer the external backend names too: the
# burn's funding Transfer to Circle's TokenMinter, the mint's payout Transfer.
# Protocol and transfer id are the adapter's for the same receipts (its
# tests/test_cctp_rows.py over tests/data/cctp_logs.json, 2026-11-22): Circle's
# nonce, decimal on V1 (the burn logs it), 0x + 64 hex on V2, where only the
# mint has it (Circle assigns a V2 burn's off chain).
HALF_BRIDGES = {
    "v1_burn": Bridge(
        fromAddress="0x55b887d413a320674f80c84d370742b114d090b5",
        fromAsset=USDC,
        fromAmount=69_830_000,
        fromNetwork="eth",
        fromPayment=f"{bare('v1_burn')}_T46",
        toAddress="0x06ce011757b5b721a27fbfc279fc1bce580c42f2",
        toAsset="unknown",
        toAmount=0,
        toNetwork="arb",
        toPayment="pending",
        protocol="cctp",
        transferId="506370",
    ),
    "v2_burn": Bridge(
        fromAddress="0x9e9852ba17ab249fd668cdc8f5f11cbcdff306d4",
        fromAsset=USDC,
        fromAmount=86_114_739,
        fromNetwork="eth",
        fromPayment=f"{bare('v2_burn')}_T694",
        toAddress="0x7ae6b9a6d79627099b0c46bf3363b8ac400277cc",
        toAsset="unknown",
        toAmount=0,
        toNetwork="arb",
        toPayment="pending",
        protocol="cctp",
    ),
    "v1_mint": Bridge(
        fromAddress="unknown",
        fromAsset="unknown",
        fromAmount=0,
        fromNetwork="arb",
        fromPayment="unknown",
        toAddress="0xf817f4038acf4a3c2b6c3fcc592c3647631a43fd",
        toAsset=USDC,
        toAmount=540_864_324,
        toNetwork="eth",
        toPayment=f"{bare('v1_mint')}_T519",
        protocol="cctp",
        transferId="919387",
    ),
    "v2_mint": Bridge(
        fromAddress="unknown",
        fromAsset="unknown",
        fromAmount=0,
        fromNetwork="pol",
        fromPayment="unknown",
        toAddress="0x298542744149d89ee87df4018b8fe3e53cd3ce54",
        toAsset=USDC,
        toAmount=213_844_154,
        toNetwork="eth",
        toPayment=f"{bare('v2_mint')}_T565",
        protocol="cctp",
        transferId="0x3bfde92aaa92d71d8fa947c466bfe7433345e56b302a2b45e53a5f598a7760cb",
    ),
}


@pytest.mark.parametrize("part", list(HALF_BRIDGES))
def test_a_cctp_burn_or_mint_on_eth_is_a_half_bridge(part):
    """A burn knows where the USDC goes; a mint where it came from, not which
    burn it was."""
    assert half_bridges("eth", db_rows(part), CCTP) == (HALF_BRIDGES[part],)


def test_logs_in_any_order_are_read_in_log_order():
    assert half_bridges("eth", db_rows("v2_burn")[::-1], CCTP) == (
        HALF_BRIDGES["v2_burn"],
    )


@pytest.mark.parametrize("included_bridges", [(), ("thorchain", "symbiosis")])
def test_only_when_cctp_is_an_included_bridge(included_bridges):
    assert half_bridges("eth", db_rows("v2_burn"), included_bridges) == ()


@pytest.mark.parametrize("part", list(HALF_BRIDGES))
def test_the_same_tx_without_its_cctp_event_is_none(part):
    """The rest of a CCTP receipt (the USDC Transfer and Burn, the
    MessageTransmitter's MessageSent/MessageReceived) does not count."""
    rows = db_rows(part, without_cctp_events(part))
    assert half_bridges("eth", rows, CCTP) == ()


def test_a_burn_without_its_funding_transfer_is_not_a_bridge(caplog):
    """A DepositForBurn that no Transfer to the TokenMinter funded moved
    nothing: it is no bridge, not a bridge with the tx as its leg."""
    logs = [
        log
        for log in FIXTURE["v2_burn"]["logs"]
        if log["log_index"] != 694  # the funding Transfer
    ]
    with caplog.at_level(logging.DEBUG, logger="graphsenselib.defi.bridging"):
        assert half_bridges("eth", db_rows("v2_burn", logs), CCTP) == ()
    assert "not a burn" in caplog.text
    assert bare("v2_burn") in caplog.text and "698" in caplog.text


@pytest.mark.parametrize("part, burn", [("v1_burn", 48), ("v2_burn", 696)])
def test_a_funded_burn_event_without_the_minters_burn_is_not_a_bridge(part, burn):
    """USDC sent to the TokenMinter beside a replacement looks funded; only
    the minter's burn (Transfer to the zero address) shows a real burn."""
    logs = [log for log in FIXTURE[part]["logs"] if log["log_index"] != burn]
    assert half_bridges("eth", db_rows(part, logs), CCTP) == ()


def v1_replacement(offset=0):
    """What V1's replaceDepositForBurn emits: MessageSent and DepositForBurn,
    the amount, nonce and depositor of the original, no Transfer."""
    return [
        {**log, "log_index": log["log_index"] + offset}
        for log in FIXTURE["v1_burn"]["logs"]
        if log["log_index"] in (49, 50)
    ]


def test_a_v1_replacement_is_not_a_half_bridge():
    assert half_bridges("eth", db_rows("v1_burn", v1_replacement()), CCTP) == ()


def test_a_replacement_beside_a_real_burn_leaves_the_real_one():
    """Each DepositForBurn is judged by its own segment of the logs."""
    logs = v1_replacement(offset=-10) + FIXTURE["v1_burn"]["logs"]
    assert half_bridges("eth", db_rows("v1_burn", logs), CCTP) == (
        HALF_BRIDGES["v1_burn"],
    )


def test_a_real_burn_followed_by_a_replacement_is_one_half_bridge():
    """The replacement does not reuse the burn's funding Transfer."""
    logs = FIXTURE["v1_burn"]["logs"] + v1_replacement(offset=10)
    assert half_bridges("eth", db_rows("v1_burn", logs), CCTP) == (
        HALF_BRIDGES["v1_burn"],
    )


def test_a_leg_with_two_fitting_transfers_is_the_tx():
    """Two Transfers of the same amount to the TokenMinter cannot be told
    apart; the burn is funded, so it is a bridge, with the tx as its leg,
    paid (as every bare-tx leg) by the tx's sender."""
    logs = FIXTURE["v2_burn"]["logs"]
    funding = next(log for log in logs if log["log_index"] == 694)
    twice = [{**funding, "log_index": 690}, *logs]
    sender = "0x" + "11" * 20
    assert half_bridges("eth", db_rows("v2_burn", twice), CCTP, tx_sender=sender) == (
        HALF_BRIDGES["v2_burn"].model_copy(
            update={"fromPayment": bare("v2_burn"), "fromAddress": sender}
        ),
    )


def test_two_mints_in_one_tx_each_come_from_their_own_source():
    """Each MintAndWithdraw is followed by its own MessageReceived."""
    logs = FIXTURE["v1_mint"]["logs"] + FIXTURE["v2_mint"]["logs"]
    bridges = half_bridges("eth", db_rows("v1_mint", logs), CCTP)
    assert [(b.fromNetwork, b.toPayment) for b in bridges] == [
        ("arb", f"{bare('v1_mint')}_T519"),
        ("pol", f"{bare('v1_mint')}_T565"),
    ]


def test_a_mint_without_its_message_received_comes_from_an_unknown_network():
    logs = [log for log in FIXTURE["v2_mint"]["logs"] if log["log_index"] != 567]
    (bridge,) = half_bridges("eth", db_rows("v2_mint", logs), CCTP)
    # nor does anything name its nonce
    assert bridge == HALF_BRIDGES["v2_mint"].model_copy(
        update={"fromNetwork": "unknown", "transferId": None}
    )


def test_a_burn_to_a_domain_without_a_network_name_goes_to_an_unknown_one():
    """Domain 5 is Solana, whose recipient is no EVM address."""
    logs = [dict(log) for log in FIXTURE["v2_burn"]["logs"]]
    burn = next(log for log in logs if log["log_index"] == 698)
    data = burn["data"].removeprefix("0x")
    burn["data"] = "0x" + data[:128] + f"{5:064x}" + data[192:]
    (bridge,) = half_bridges("eth", db_rows("v2_burn", logs), CCTP)
    assert (bridge.toNetwork, bridge.toAddress) == ("unknown", "unknown")


def test_two_burns_in_one_tx_are_two_half_bridges():
    """Each finds its funding Transfer among the logs since the previous one."""
    logs = FIXTURE["v2_burn"]["logs"]
    again = [{**log, "log_index": log["log_index"] + 100} for log in logs]
    bridges = half_bridges("eth", db_rows("v2_burn", logs + again), CCTP)
    assert [b.fromPayment for b in bridges] == [
        f"{bare('v2_burn')}_T694",
        f"{bare('v2_burn')}_T794",
    ]


def test_a_cctp_event_from_another_contract_is_none():
    """Anyone can emit an event with DepositForBurn's signature."""
    burn = next(
        log
        for log in FIXTURE["v2_burn"]["logs"]
        if log["topics"][0] == V2_DEPOSIT_FOR_BURN
    )
    fake = {**burn, "address": "0x1111111111111111111111111111111111111111"}
    assert half_bridges("eth", db_rows("v2_burn", [fake]), CCTP) == ()


def test_the_v1_event_from_the_v2_messenger_is_none():
    """Each version's events count from that version's own TokenMessenger."""
    v1_burn = next(
        log
        for log in FIXTURE["v1_burn"]["logs"]
        if log["address"] == V1_TOKEN_MESSENGER
    )
    moved = {**v1_burn, "address": V2_TOKEN_MESSENGER}
    assert half_bridges("eth", db_rows("v1_burn", [moved]), CCTP) == ()


def test_a_cctp_event_that_does_not_decode_is_left_out():
    burn = next(
        log
        for log in FIXTURE["v2_burn"]["logs"]
        if log["topics"][0] == V2_DEPOSIT_FOR_BURN
    )
    cut = {**burn, "topics": burn["topics"][:1]}
    assert half_bridges("eth", db_rows("v2_burn", [cut]), CCTP) == ()


def test_a_cctp_event_with_too_little_data_is_left_out():
    burn = next(
        log
        for log in FIXTURE["v2_burn"]["logs"]
        if log["topics"][0] == V2_DEPOSIT_FOR_BURN
    )
    short = {**burn, "data": burn["data"][: 2 + 128]}  # two of three words
    assert half_bridges("eth", db_rows("v2_burn", [short]), CCTP) == ()


def test_the_eth_deployments_count_on_eth_only():
    """The table is per network: these are Circle's eth contracts."""
    assert half_bridges("trx", db_rows("v2_burn"), CCTP) == ()


def test_no_logs_or_a_log_without_topics_is_none():
    assert half_bridges("eth", [], CCTP) == ()
    bare_log = {"address": unhex(V2_TOKEN_MESSENGER), "topics": [], "log_index": 0}
    assert half_bridges("eth", [bare_log], CCTP) == ()


# Made-up logs whose shape a reader does not expect. Any contract can emit
# them, so a reader that raises on one costs only its own protocol's halves
# of that tx (half_bridges).
STRANGER = bytes.fromhex("55" * 20)
WH_CORE = bytes.fromhex("98f3c9e6e3face36baad05fe09d375ef1464288b")
WH_BRIDGE = bytes.fromhex("3ee18b2214aff97000d974cf647e7c347e8fa585")
TRANSFER = bytes.fromhex(
    "ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
)
ODD_TX = bytes.fromhex("ab" * 32)


def word(n):
    return n.to_bytes(32, "big")


def padded(address):
    return bytes(12) + address


def odd_log(index, address, topics, data=b""):
    return {
        "tx_hash": ODD_TX,
        "log_index": index,
        "address": address,
        "topics": topics,
        "data": data,
    }


def token_bridge_message(index, amount, token):
    """A Core LogMessagePublished of the Token Bridge: a transfer of amount
    of token (payload type 1) to chain 23."""
    payload = (
        bytes([1])
        + word(amount)
        + padded(token)
        + (2).to_bytes(2, "big")
        + padded(STRANGER)
        + (23).to_bytes(2, "big")
        + word(0)
    )
    data = word(7) + word(0) + word(128) + word(1) + word(len(payload)) + payload
    return odd_log(
        index, WH_CORE, [wormhole._LOG_MESSAGE_PUBLISHED, padded(WH_BRIDGE)], data
    )


ODD_LOGS = {
    "lz_oft_sent_one_topic": (
        "layerzero",
        [odd_log(0, STRANGER, [layerzero._OFT_SENT])],
    ),
    "lz_oft_received_one_topic": (
        "layerzero",
        [odd_log(0, STRANGER, [layerzero._OFT_RECEIVED])],
    ),
    "wh_message_of_amount_zero": (
        "wormhole",
        [
            odd_log(
                0, STRANGER, [TRANSFER, padded(STRANGER), padded(WH_BRIDGE)], word(0)
            ),
            token_bridge_message(1, 0, STRANGER),
        ],
    ),
    "wh_deposit_one_topic_before_the_message": (
        "wormhole",
        [
            odd_log(0, STRANGER, [wormhole._DEPOSIT]),
            token_bridge_message(1, 5, STRANGER),
        ],
    ),
    "wh_withdrawal_one_topic_after_a_redeem": (
        "wormhole",
        [
            odd_log(
                0,
                WH_BRIDGE,
                [wormhole._TRANSFER_REDEEMED, word(23), padded(STRANGER), word(9)],
            ),
            odd_log(1, STRANGER, [wormhole._WITHDRAWAL]),
        ],
    ),
}


@pytest.mark.parametrize("case", list(ODD_LOGS))
def test_a_log_a_reader_cannot_read_is_no_half_bridge(case, caplog):
    protocol, logs = ODD_LOGS[case]
    with caplog.at_level(logging.ERROR):
        assert (
            half_bridges("eth", logs, DELEGATED_BRIDGES, tx_sender="0x" + "00" * 20)
            == ()
        )
    [record] = caplog.records
    assert record.exc_info is not None
    assert protocol in record.getMessage()
    assert ODD_TX.hex() in record.getMessage()


def test_a_log_a_reader_cannot_read_leaves_the_other_protocols_halves():
    stranger = {**ODD_LOGS["lz_oft_sent_one_topic"][1][0], "log_index": 10_000}
    rows = db_rows("v2_burn") + [stranger]
    assert half_bridges("eth", rows, ("cctp", "layerzero")) == (
        HALF_BRIDGES["v2_burn"],
    )
