"""Bridge events graphsense-lib recognizes but cannot pair
(defi/bridging/delegated.py).

The fixture holds the eth receipt logs of four real mainnet Circle CCTP txs: a
V1 and a V2 burn (DepositForBurn) and a V1 and a V2 mint (MintAndWithdraw),
each emitted by Circle's eth TokenMessenger of that version.
"""

import json
from pathlib import Path

import pytest

from graphsenselib.defi.bridging.delegated import UnresolvedBridge, unresolved_bridges

FIXTURE = json.loads((Path(__file__).parent / "cctp_eth_logs.json").read_text())

V1_TOKEN_MESSENGER = "0xbd3fa81b58ba92a82136038b25adec7066af3155"
V2_TOKEN_MESSENGER = "0x28b5a0e9c621a5badaa536219b3a228c8168cf5d"
V2_DEPOSIT_FOR_BURN = (
    "0x0c8c1cbdc5190613ebd485511d4e2812cfa45eecb79d845893331fedad5130a5"
)


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


@pytest.mark.parametrize(
    "part, log_index",
    [("v1_burn", 50), ("v2_burn", 698), ("v1_mint", 520), ("v2_mint", 566)],
)
def test_a_cctp_burn_or_mint_on_eth_is_an_unresolved_cctp_bridge(part, log_index):
    tx_hash = FIXTURE[part]["tx_hash"]
    assert unresolved_bridges("eth", db_rows(part)) == (
        UnresolvedBridge("cctp", "eth", tx_hash, log_index),
    )


@pytest.mark.parametrize("part", ["v1_burn", "v2_burn", "v1_mint", "v2_mint"])
def test_the_same_tx_without_its_cctp_event_is_not(part):
    """The rest of a CCTP receipt (the USDC Transfer and Burn, the
    MessageTransmitter's MessageSent/MessageReceived) does not count."""
    rows = db_rows(part, without_cctp_events(part))
    assert unresolved_bridges("eth", rows) == ()


def test_a_cctp_event_from_another_contract_is_not():
    """Anyone can emit an event with DepositForBurn's signature."""
    fake = {
        "log_index": 7,
        "address": "0x1111111111111111111111111111111111111111",
        "topics": [V2_DEPOSIT_FOR_BURN],
        "data": "0x",
    }
    assert unresolved_bridges("eth", db_rows("v2_burn", [fake])) == ()


def test_a_token_messenger_event_of_another_signature_is_not():
    other = {
        "log_index": 7,
        "address": V2_TOKEN_MESSENGER,
        "topics": ["0x" + "ab" * 32],
        "data": "0x",
    }
    assert unresolved_bridges("eth", db_rows("v2_burn", [other])) == ()


def test_the_v1_event_from_the_v2_messenger_is_not():
    """Each version's events count from that version's own TokenMessenger."""
    v1_burn = next(
        log
        for log in FIXTURE["v1_burn"]["logs"]
        if log["address"] == V1_TOKEN_MESSENGER
    )
    moved = {**v1_burn, "address": V2_TOKEN_MESSENGER}
    assert unresolved_bridges("eth", db_rows("v1_burn", [moved])) == ()


def test_the_eth_deployments_count_on_eth_only():
    """The table is per network: these are Circle's eth contracts."""
    assert unresolved_bridges("trx", db_rows("v2_burn")) == ()


def test_no_logs_or_a_log_without_topics_is_not():
    assert unresolved_bridges("eth", []) == ()
    bare = {"address": unhex(V2_TOKEN_MESSENGER), "topics": [], "log_index": 0}
    assert unresolved_bridges("eth", [bare]) == ()
