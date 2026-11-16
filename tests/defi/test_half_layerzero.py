"""LayerZero V2 OFT / Stargate V2 half bridges on eth (defi/bridging/halves/layerzero.py).

The fixture (data/bridges/layerzero/eth_logs.json, recorded for the external
backend adapter) holds the eth receipt logs of four mainnet txs: a Stargate USDC
send to arb, a Stargate USDC and a USDT0 (generic OFT) delivery from arb, and a
Stargate native-ETH bus ride to Linea.
"""

import copy
import json
from pathlib import Path

from graphsenselib.defi.bridging.delegated import half_bridges
from graphsenselib.defi.bridging.half_common import PENDING, UNKNOWN
from graphsenselib.defi.bridging.halves.layerzero import read
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
SPOOF = "0x" + "5f" * 20
USDC = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
USDT = "0xdac17f958d2ee523a2206206994597c13d831ec7"


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


def edited(part, topic0, **fields):
    """The tx's logs, the one with `topic0` edited (drop=True: removed)."""
    logs = []
    for log in copy.deepcopy(FIXTURE[part]["logs"]):
        if log["topics"][0] == topic0:
            if fields.get("drop"):
                continue
            log.update(fields)
        logs.append(log)
    return db_rows(part, logs)


def test_layerzero_eth_source_half():
    [bridge] = read("eth", db_rows("eth_arb_usdc_lifi"))
    assert bridge == Bridge(
        # sent through LiFi: OFTSent's fromAddress is the LiFi diamond
        fromAddress="0x1231deb6f5749ef6ce6943a275a1d3e7486f4eae",
        fromAsset=USDC,
        fromAmount=144379358,
        fromNetwork="eth",
        fromPayment="ae9bd3150b06819d654214f57c4b89703048b250b820555866fd6cffc30264be_T379",
        toAddress=UNKNOWN,
        toAsset=UNKNOWN,
        toAmount=0,
        toNetwork="arb",
        toPayment=PENDING,
    )
    # registered for delegation under its included_bridges name
    assert half_bridges("eth", db_rows("eth_arb_usdc_lifi"), ["layerzero"]) == (bridge,)


def test_layerzero_eth_destination_half():
    [bridge] = read("eth", db_rows("arb_eth_usdc"))
    assert bridge == Bridge(
        fromAddress=UNKNOWN,
        fromAsset=UNKNOWN,
        fromAmount=0,
        fromNetwork="arb",
        fromPayment=UNKNOWN,
        toAddress="0x61a819c28df80da5926ded8f8e415e1dfb974912",
        toAsset=USDC,
        toAmount=44971559,
        toNetwork="eth",
        toPayment="5d07f7cd5e8abaf339802da7b477d7ffc7f3086f5911d0fe9ae147bfe16d9de9_T195",
    )


def test_generic_oft_destination_half():
    [bridge] = read("eth", db_rows("arb_eth_usdt0"))
    assert (bridge.fromNetwork, bridge.fromPayment) == ("arb", UNKNOWN)
    assert (bridge.toAddress, bridge.toAsset, bridge.toAmount) == (
        "0xc60a49041fcdb8d669968fc23999684181e245f9",
        USDT,
        39680000,
    )
    assert bridge.toPayment == (
        "bfc6a533e654beeaf4a0be6151361be7c5c5bd821e2b4a7ae35c9919bf4fa800_T476"
    )


def test_bus_ride_half_is_the_bare_tx():
    [bridge] = read("eth", db_rows("eth_linea_native_bus"))
    assert (bridge.fromAsset, bridge.fromAmount, bridge.fromPayment) == (
        "native",
        10**16,
        "8f8a7a1838c0e66ced5c968ab7da83a6b28d41469ae240c9a9ac8d091f7da5c3",
    )
    # Linea (eid 30183) is no network the conversions name
    assert (bridge.toNetwork, bridge.toPayment) == (UNKNOWN, PENDING)


def test_layerzero_spoofed_emitter_is_ignored():
    assert read("eth", edited("eth_arb_usdc_lifi", OFT_SENT, address=SPOOF)) == []
    assert read("eth", edited("arb_eth_usdc", OFT_RECEIVED, address=SPOOF)) == []
    # a guid-0 send counts only from a Stargate pool
    assert read("eth", edited("eth_linea_native_bus", OFT_SENT, address=SPOOF)) == []


def test_oft_without_endpoint_packet_is_ignored():
    assert read("eth", edited("eth_arb_usdc_lifi", PACKET_SENT, drop=True)) == []
    assert read("eth", edited("eth_arb_usdc_lifi", PACKET_SENT, address=SPOOF)) == []
    assert read("eth", edited("arb_eth_usdc", PACKET_DELIVERED, drop=True)) == []
    assert read("eth", edited("arb_eth_usdc", PACKET_DELIVERED, address=SPOOF)) == []
    # a PacketDelivered of the next nonce recomputes to another guid
    [delivered] = [
        log
        for log in FIXTURE["arb_eth_usdc"]["logs"]
        if log["topics"][0] == PACKET_DELIVERED
    ]
    words = delivered["data"][2:]
    nonce = int(words[128:192], 16)
    shifted = "0x" + words[:128] + f"{nonce + 1:064x}" + words[192:]
    assert read("eth", edited("arb_eth_usdc", PACKET_DELIVERED, data=shifted)) == []


def test_other_networks_read_nothing():
    # the endpoint table names eth only: no leg is confirmed elsewhere
    assert read("bnb", db_rows("eth_arb_usdc_lifi")) == []


def test_undecodable_event_is_skipped(caplog):
    logs = edited("eth_arb_usdc_lifi", OFT_SENT, data="0x" + "00" * 31)
    assert read("eth", logs) == []
    assert "does not decode" in caplog.text
