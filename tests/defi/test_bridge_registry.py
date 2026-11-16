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


def test_delegated_bridges_lists_all_five():
    assert set(DELEGATED_BRIDGES) == {"cctp", *FOUR}
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
