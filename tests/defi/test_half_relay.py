"""Relay half bridges (defi/bridging/halves/relay.py) over real eth receipts
recorded by the adapter (tests/defi/data/bridges/relay/eth_logs.json): a
Depository deposit is the source half, its destination pending; a fill has no
Relay event and is not read."""

import json
import pathlib

import pytest

from graphsenselib.defi.bridging.delegated import half_bridges
from graphsenselib.defi.bridging.half_common import PENDING, UNKNOWN
from graphsenselib.defi.bridging.halves import relay

FIXTURE = json.loads(
    (
        pathlib.Path(__file__).parent / "data" / "bridges" / "relay" / "eth_logs.json"
    ).read_text()
)
DEPOSITORY = "0x4cd00e387622c35bddb9b4c962c136462338bc31"


def unhex(s):
    return bytes.fromhex(s.removeprefix("0x"))


def rows(part, edit=lambda log: log):
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
        for log in map(edit, tx["logs"])
    ]


def bare(part):
    return FIXTURE[part]["tx_hash"].removeprefix("0x")


def test_relay_eth_source_half_erc20():
    (bridge,) = relay.read("eth", rows("eth_erc20_deposit"))
    assert bridge.fromNetwork == "eth"
    assert bridge.fromAddress == "0x5da070e8be8a1aef4aaa98898e11e86af4d806d4"
    assert bridge.fromAsset == "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
    assert bridge.fromAmount == 0xAB82BDD
    # the USDC Transfer into the Depository (log 349), not the event (log 350)
    assert bridge.fromPayment == f"{bare('eth_erc20_deposit')}_T349"
    assert (bridge.toPayment, bridge.toNetwork, bridge.toAddress) == (
        PENDING,
        UNKNOWN,
        UNKNOWN,
    )
    assert (bridge.toAsset, bridge.toAmount) == (UNKNOWN, 0)


def test_relay_eth_source_half_native_is_the_bare_tx():
    (bridge,) = relay.read("eth", rows("eth_native_deposit"))
    assert bridge.fromAsset == "native"
    assert bridge.fromAddress == "0x68bbe4ff1dd925ad8e21f90551af342a223fac60"
    assert bridge.fromAmount == 0x1812874DF604F6
    assert bridge.fromPayment == bare("eth_native_deposit")
    assert bridge.toPayment == PENDING


@pytest.mark.parametrize("part", ["eth_usdc_fill", "eth_native_fill"])
def test_relay_eth_destination_half_is_not_read(part):
    """A fill carries its id in calldata only: no log to read, no half bridge
    (eth fills are not delegated)."""
    assert relay.read("eth", rows(part)) == []


def test_relay_spoofed_emitter_is_ignored():
    spoof = "0x" + "66" * 20

    def spoofed(log):
        return dict(log, address=spoof) if log["address"] == DEPOSITORY else log

    for part in ("eth_erc20_deposit", "eth_native_deposit"):
        assert relay.read("eth", rows(part, spoofed)) == []


def test_relay_unfunded_erc20_deposit_names_the_tx():
    """No Transfer into the Depository fits: the leg is the tx, not a guess."""
    logs = [
        log for log in rows("eth_erc20_deposit") if log["address"] == unhex(DEPOSITORY)
    ]
    (bridge,) = relay.read("eth", logs)
    assert bridge.fromPayment == bare("eth_erc20_deposit")


def test_relay_network_without_a_depository_reads_nothing():
    assert relay.read("base", rows("eth_erc20_deposit")) == []


def test_relay_is_delegated_on_eth():
    (bridge,) = half_bridges("eth", rows("eth_erc20_deposit"), ["relay"])
    assert bridge.fromPayment.endswith("_T349")
    assert half_bridges("eth", rows("eth_erc20_deposit"), ["cctp"]) == ()


@pytest.mark.parametrize("part", ["eth_erc20_deposit", "eth_native_deposit"])
def test_relay_malformed_deposit_is_no_half_and_no_error(part):
    """A Depository event whose data is too short to decode: no half bridge, no
    exception (an exception would be a 500 on gs-rest's eth conversions)."""

    def truncated(log):
        return dict(log, data=log["data"][:82]) if log["address"] == DEPOSITORY else log

    assert relay.read("eth", rows(part, truncated)) == []
