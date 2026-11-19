"""LayerZero V2 OFT / Stargate V2 / USDT0 half bridges
(defi/bridging/halves/layerzero.py).

The fixture holds the eth receipt logs of four real mainnet txs, copied from the
external backend adapter's recordings (tests/data/bridges/layerzero/gslib/
eth_logs.json): a Stargate USDC send to arb through LiFi, the eth deliveries of
an arb Stargate USDC send and of an arb USDT0 send, and a native Stargate bus
ride to Linea.
"""

import copy
import json
from pathlib import Path

import pytest

from graphsenselib.defi.bridging.delegated import half_bridges
from graphsenselib.defi.bridging.halves import layerzero
from graphsenselib.defi.bridging.models import Bridge

FIXTURE = json.loads(
    (
        Path(__file__).parent / "data" / "bridges" / "layerzero" / "eth_logs.json"
    ).read_text()
)
OFT_SENT = "0x85496b760a4b7f8d66384b9df21b381f5d1b1e79f229a47aaf4c232edc2fe59a"
OFT_RECEIVED = "0xefed6d3500546b29533b128a29e3a94d70788727f0507505ac12eaf2e578fd9c"
PACKET_SENT = "0x1ab700d4ced0c005b164c0f789fd09fcbb0156d4c2041b8a3bfbcd961cd1567f"
PACKET_DELIVERED = "0x3cd5e48f9730b129dc7550f0fcea9c767b7be37837cd10e55eb35f734f4bca04"
ETH_USDC = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
ETH_USDT = "0xdac17f958d2ee523a2206206994597c13d831ec7"
LIFI = "0x1231deb6f5749ef6ce6943a275a1d3e7486f4eae"
SPOOF = "0x" + "5f" * 20


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


def edited(case, topic0, drop=False, **fields):
    """The case's logs with the log of topic0 changed (or dropped)."""
    logs = []
    for log in copy.deepcopy(FIXTURE[case]["logs"]):
        if log["topics"][0] == topic0:
            if drop:
                continue
            log.update(fields)
        logs.append(log)
    return db_rows(case, logs)


def test_layerzero_eth_source_half():
    # a Stargate USDC send through LiFi: the leg is the Transfer into the pool,
    # paid by the LiFi diamond (half_bridges: whoever paid the leg)
    [half] = half_bridges(
        "eth", db_rows("eth_arb_usdc_lifi"), ("layerzero",), tx_sender="0x" + "11" * 20
    )
    assert half == Bridge(
        fromAddress=LIFI,
        fromAsset=ETH_USDC,
        fromAmount=144379358,
        fromNetwork="eth",
        fromPayment=f"{bare('eth_arb_usdc_lifi')}_T379",
        toAddress="unknown",
        toAsset="unknown",
        toAmount=0,
        toNetwork="arb",
        toPayment="pending",
        protocol="layerzero",
        protocolApp="stargate",
        transferId="0x53232cde0d501c67ba7636b541a1a50730f6532c68f4ce7bbc3cc94b8f239ff8",
    )


def test_layerzero_eth_destination_half():
    [half] = half_bridges("eth", db_rows("arb_eth_usdc"), ("layerzero",))
    assert half == Bridge(
        fromAddress="unknown",
        fromAsset="unknown",
        fromAmount=0,
        fromNetwork="arb",
        fromPayment="unknown",
        toAddress="0x61a819c28df80da5926ded8f8e415e1dfb974912",
        toAsset=ETH_USDC,
        toAmount=44971559,
        toNetwork="eth",
        toPayment=f"{bare('arb_eth_usdc')}_T195",
        protocol="layerzero",
        protocolApp="stargate",
        transferId="0x25232a057570c02289df4b6868dff8716f7a6737a9569a1a78f704071c762ffe",
    )


def test_a_usdt0_delivery_names_usdt0():
    # the eth USDT0 lockbox pays Tether's USDT
    [half] = layerzero.read("eth", db_rows("arb_eth_usdt0"))
    assert (half.toAsset, half.toPayment) == (ETH_USDT, f"{bare('arb_eth_usdt0')}_T476")
    assert (half.protocol, half.protocolApp, half.fromNetwork) == (
        "layerzero",
        "usdt0",
        "arb",
    )


def test_a_bus_ride_half_is_the_bare_tx():
    sender = "0x6a738c309beac3c68ef8f50f3c5880c97dbc7726"
    [half] = half_bridges(
        "eth", db_rows("eth_linea_native_bus"), ("layerzero",), tx_sender=sender
    )
    assert (half.fromPayment, half.fromAsset, half.fromAmount) == (
        bare("eth_linea_native_bus"),
        "native",
        10**16,
    )
    assert (half.fromAddress, half.toNetwork, half.transferId) == (
        sender,
        "unknown",  # Linea is not named
        None,
    )
    # a guid-0 send from a contract that is not a Stargate pool: no half
    logs = edited("eth_linea_native_bus", OFT_SENT, address=SPOOF)
    assert layerzero.read("eth", logs) == []


def test_a_spoofed_emitter_is_ignored():
    for case, topic0 in [
        ("eth_arb_usdc_lifi", OFT_SENT),
        ("arb_eth_usdc", OFT_RECEIVED),
        ("arb_eth_usdt0", OFT_RECEIVED),
    ]:
        assert layerzero.read("eth", edited(case, topic0, address=SPOOF)) == []


def test_an_oft_event_without_its_endpoint_packet_is_ignored():
    for case, packet in [
        ("eth_arb_usdc_lifi", PACKET_SENT),
        ("arb_eth_usdc", PACKET_DELIVERED),
    ]:
        assert layerzero.read("eth", edited(case, packet, drop=True)) == []
        assert layerzero.read("eth", edited(case, packet, address=SPOOF)) == []


def test_registered_for_eth_only():
    assert half_bridges("eth", db_rows("arb_eth_usdc"), ("debridge",)) == ()
    assert layerzero.read("eth", db_rows("arb_eth_usdc")) != []
    assert half_bridges("arb", db_rows("arb_eth_usdc"), ("layerzero",)) == ()


# The adapter's own reading of the same receipts (its services/bridges/layerzero.py
# over tests/data/bridges/layerzero/, the eth legs, 2026-11-19): leg, address,
# asset, amount, app and transfer id. A pair the adapter answers for a delegated
# half names exactly these legs, so the two readers must agree.
# (case, side, leg, address, asset, amount, app, transfer id)
ADAPTER_LEGS = [
    ("eth_arb_usdc_lifi", "from", "_T379", LIFI, ETH_USDC, 144379358, "stargate",
     "0x53232cde0d501c67ba7636b541a1a50730f6532c68f4ce7bbc3cc94b8f239ff8"),
    ("arb_eth_usdc", "to", "_T195", "0x61a819c28df80da5926ded8f8e415e1dfb974912",
     ETH_USDC, 44971559, "stargate",
     "0x25232a057570c02289df4b6868dff8716f7a6737a9569a1a78f704071c762ffe"),
    ("arb_eth_usdt0", "to", "_T476", "0xc60a49041fcdb8d669968fc23999684181e245f9",
     ETH_USDT, 39680000, "usdt0",
     "0x91389304683b71c924031545755f1da8edffb74947ef8928776a2706d307a07f"),
    ("eth_linea_native_bus", "from", None, "0x6a738c309beac3c68ef8f50f3c5880c97dbc7726",
     "native", 10**16, "stargate", None),
]  # fmt: skip


@pytest.mark.parametrize(
    "case, side, suffix, address, asset, amount, app, transfer_id", ADAPTER_LEGS
)
def test_the_eth_halves_name_the_adapters_legs(
    case, side, suffix, address, asset, amount, app, transfer_id
):
    # a bare (native) source leg is paid by the tx's sender: the adapter's
    # address for it is that sender
    sender = address if side == "from" else "0x" + "11" * 20
    [half] = half_bridges("eth", db_rows(case), ("layerzero",), tx_sender=sender)
    payment = "fromPayment" if side == "from" else "toPayment"
    assert getattr(half, payment) == bare(case) + (suffix or "")
    assert getattr(half, f"{side}Address") == address
    assert (getattr(half, f"{side}Asset"), getattr(half, f"{side}Amount")) == (
        asset,
        amount,
    )
    assert (half.protocol, half.protocolApp, half.transferId) == (
        "layerzero",
        app,
        transfer_id,
    )


FEE_BURNS = json.loads(
    (
        Path(__file__).parent / "data" / "bridges" / "layerzero" / "fee_oft_burns.json"
    ).read_text()
)


@pytest.mark.parametrize("c", FEE_BURNS["cases"], ids=lambda c: c["network"])
def test_a_fee_charging_oft_send_is_funded_by_its_burn(c):
    # base 0x0fd194ce and bnb 0x9e73e79b (no eth case is recorded; the reader
    # reads their logs as it reads eth's): the OFT pays a fee apart and burns
    # amountReceivedLD, so the leg is the burn, as the adapter names it
    rows = [
        {
            "tx_hash": unhex(c["tx_hash"]),
            "block_id": int(c["receipt"]["blockNumber"], 16),
            "log_index": int(log["logIndex"], 16),
            "address": unhex(log["address"]),
            "topics": [unhex(t) for t in log["topics"]],
            "topic0": unhex(log["topics"][0]),
            "data": unhex(log["data"]),
        }
        for log in c["receipt"]["logs"]
    ]
    [half] = layerzero.read("eth", rows)
    burn = next(e for e in c["receipt"]["logs"] if int(e["logIndex"], 16) == c["burn"])
    assert half.fromPayment == f"{c['tx_hash'].removeprefix('0x')}_T{c['burn']}"
    assert (half.fromAsset, half.fromAmount) == (burn["address"], int(burn["data"], 16))
