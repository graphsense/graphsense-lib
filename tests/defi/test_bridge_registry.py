"""The registry of half-bridge readers (defi/bridging/delegated.py) and the
local bridge strategy selection (defi/conversions.py) next to it."""

import pytest

from graphsenselib.defi.bridging.delegated import (
    DELEGATED_BRIDGES,
    _HALF_BRIDGE_READERS,
    _cctp_half_bridges,
)
from graphsenselib.defi.bridging.models import BridgeStrategy
from graphsenselib.defi.conversions import get_bridge_strategy_from_decoded_logs

from tests.defi.test_delegated_bridges import db_rows

FOUR = ("layerzero", "across", "wormhole", "debridge")


def test_delegated_bridges_lists_all_six():
    assert set(DELEGATED_BRIDGES) == {"cctp", *FOUR, "relay"}
    assert list(DELEGATED_BRIDGES) == sorted(DELEGATED_BRIDGES)


@pytest.mark.parametrize("bridge", FOUR)
def test_the_new_readers_are_registered_on_eth_and_read_nothing_yet(bridge):
    read = _HALF_BRIDGE_READERS["eth"][bridge]
    assert read("eth", db_rows("v1_burn")) == []


def dlog(name, tags):
    return {"name": name, "log_def": {"tags": tags}, "parameters": {}}


def test_thorchain_not_preempted_by_wormhole_tag():
    """A wormhole-tagged log, even as the tx's final LogMessagePublished,
    no longer hides a THORChain Deposit in the same tx."""
    dlogs = [
        dlog("Deposit", ["thorchain", "deposit", "bridging"]),
        dlog("LogMessagePublished", ["wormhole", "bridging"]),
    ]
    assert get_bridge_strategy_from_decoded_logs(dlogs) == BridgeStrategy.THORCHAIN_SEND


def test_symbiosis_not_preempted_by_stargate_tag():
    dlogs = [
        dlog("Swap", ["stargate", "swap", "bridging"]),
        dlog("SynthesizeRequest", ["symbiosis", "bridging", "send"]),
    ]
    assert get_bridge_strategy_from_decoded_logs(dlogs) == BridgeStrategy.SYMBIOSIS


@pytest.mark.parametrize(
    "part, side", [("v1_burn", "fromNetwork"), ("v1_mint", "toNetwork")]
)
def test_cctp_reads_its_own_network_from_the_parameter(part, side):
    """The eth fixture read as if on base: the reader's own side names base,
    so the network comes from the parameter, not from a constant."""
    (bridge,) = _cctp_half_bridges("base", db_rows(part))
    assert getattr(bridge, side) == "base"


def _fixture_rows(path):
    """(network, part, raw log rows) of each tx of a recorded bridge fixture."""
    import json
    from pathlib import Path

    network = Path(path).name.split("_")[0]
    for part, tx in json.loads((Path(__file__).parent / path).read_text()).items():
        yield (
            network,
            part,
            [
                {
                    "tx_hash": bytes.fromhex(tx["tx_hash"].removeprefix("0x")),
                    "block_id": tx["block_id"],
                    "log_index": log["log_index"],
                    "address": bytes.fromhex(log["address"].removeprefix("0x")),
                    "topics": [
                        bytes.fromhex(t.removeprefix("0x")) for t in log["topics"]
                    ],
                    "data": bytes.fromhex(log["data"].removeprefix("0x")),
                }
                for log in tx["logs"]
            ],
        )


@pytest.mark.parametrize(
    "family, path",
    [
        ("across", "data/bridges/across/eth_logs.json"),
        ("debridge", "data/bridges/debridge/eth_logs.json"),
        ("debridge", "data/bridges/debridge/bnb_logs.json"),
        ("layerzero", "data/bridges/layerzero/eth_logs.json"),
        ("relay", "data/bridges/relay/eth_logs.json"),
        ("wormhole", "data/bridges/wormhole/eth_logs.json"),
    ],
)
def test_every_half_bridge_event_is_a_bridge_event(family, path):
    """is_bridge_event (what bounds a Wormhole redeem's payout) names at least
    one event per half bridge of every recorded tx, and never a Transfer."""
    from graphsenselib.defi.bridging import halves
    from graphsenselib.defi.bridging.delegated import is_bridge_event

    module = getattr(halves, family)
    transfer = bytes.fromhex(
        "ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
    )
    for network, part, rows in _fixture_rows(path):
        events = [log for log in rows if module.is_event(network, log)]
        assert len(events) >= len(module.read(network, rows)), part
        assert all(is_bridge_event(network, log) for log in events), part
        assert not any(log["topics"][0] == transfer for log in events), part


def test_every_cctp_half_bridge_event_is_a_bridge_event():
    from graphsenselib.defi.bridging.delegated import is_bridge_event
    from tests.defi.test_delegated_bridges import FIXTURE

    for part in FIXTURE:
        rows = db_rows(part)
        events = [log for log in rows if is_bridge_event("eth", log)]
        # a burn is its DepositForBurn, a mint its MintAndWithdraw and the
        # MessageReceived after it
        assert len(events) >= len(_cctp_half_bridges("eth", rows)), part
