"""Bridges graphsense-lib finds only half of (defi/bridging/delegated.py).

The fixture holds the eth receipt logs of four real mainnet Circle CCTP txs: a
V1 and a V2 burn (DepositForBurn) and a V1 and a V2 mint (MintAndWithdraw),
each emitted by Circle's eth TokenMessenger of that version.
"""

import json
from pathlib import Path

import pytest

from graphsenselib.defi.bridging.delegated import half_bridges
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


def test_a_leg_whose_transfer_is_not_found_is_the_tx():
    """Without its one funding Transfer the burn still is a bridge; its leg is
    then the tx itself."""
    logs = [
        log
        for log in FIXTURE["v2_burn"]["logs"]
        if log["log_index"] != 694  # the funding Transfer
    ]
    (bridge,) = half_bridges("eth", db_rows("v2_burn", logs), CCTP)
    assert bridge == HALF_BRIDGES["v2_burn"].model_copy(
        update={"fromPayment": bare("v2_burn")}
    )


def test_a_leg_with_two_fitting_transfers_is_the_tx():
    """Two Transfers of the same amount to the TokenMinter cannot be told
    apart."""
    logs = FIXTURE["v2_burn"]["logs"]
    funding = next(log for log in logs if log["log_index"] == 694)
    twice = [{**funding, "log_index": 690}, *logs]
    (bridge,) = half_bridges("eth", db_rows("v2_burn", twice), CCTP)
    assert bridge.fromPayment == bare("v2_burn")


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
    assert bridge == HALF_BRIDGES["v2_mint"].model_copy(
        update={"fromNetwork": "unknown"}
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
