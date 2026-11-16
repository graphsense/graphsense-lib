"""deBridge DLN half bridges (defi/bridging/halves/debridge.py).

The fixtures hold real receipt logs, recorded for the external backend
adapter's deBridge tests (its tests/data/bridges/debridge/): on eth a native
CreatedOrder, an old-signature fill of an avax order and a batched fill of a
bnb and a Solana order; on bnb an ERC20 CreatedOrder paid by a forwarder.
"""

import json
from pathlib import Path

import pytest

from graphsenselib.defi.bridging.delegated import half_bridges
from graphsenselib.defi.bridging.halves import debridge
from graphsenselib.defi.bridging.models import Bridge

DATA = Path(__file__).parent / "data" / "bridges" / "debridge"
FIXTURE = {
    **json.loads((DATA / "eth_logs.json").read_text()),
    **json.loads((DATA / "bnb_logs.json").read_text()),
}

DLN_SOURCE = "0xef4fb24ad0916217251f553c0596f8edc630eb66"
DLN_DESTINATION = "0xe7351fd770a37282b91d153ee690b63579d6dd7f"
SPOOF = "0x" + "5f" * 20

ETH_USDC = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
ETH_USDT = "0xdac17f958d2ee523a2206206994597c13d831ec7"
BNB_USDC = "0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d"
AVAX_USDC = "0xb97ef9ef8734c71904d8002f8b6bc66dd9c48a6e"


def unhex(s):
    return bytes.fromhex(s.removeprefix("0x"))


def bare(part):
    return FIXTURE[part]["tx_hash"].removeprefix("0x")


def db_rows(part, emitter=None):
    """Logs of one fixture tx as rows of the raw log table; `emitter` replaces
    the address of every DLN contract's log."""
    tx = FIXTURE[part]
    rows = []
    for log in tx["logs"]:
        address = log["address"]
        if emitter is not None and address in (DLN_SOURCE, DLN_DESTINATION):
            address = emitter
        rows.append(
            {
                "tx_hash": unhex(tx["tx_hash"]),
                "block_id": tx["block_id"],
                "log_index": log["log_index"],
                "address": unhex(address),
                "topics": [unhex(t) for t in log["topics"]],
                "topic0": unhex(log["topics"][0]),
                "data": unhex(log["data"]),
            }
        )
    return rows


def test_debridge_eth_source_half():
    """A native order: the leg is the tx; it pays giveAmount + percentFee +
    nativeFixFee, and the order names what arrives on arb."""
    maker = "0x8706d76ac9de89cd392ad4f3c966ab46312de8ce"
    assert debridge.read("eth", db_rows("eth_arb_native")) == [
        Bridge(
            fromAddress=maker,
            fromAsset="native",
            fromAmount=24068394746666929 + 9631210382819 + 10**15,
            fromNetwork="eth",
            fromPayment=bare("eth_arb_native"),
            toAddress=maker,
            toAsset="native",
            toAmount=23920851830400002,
            toNetwork="arb",
            toPayment="pending",
        )
    ]


def test_debridge_eth_destination_half():
    """An old-signature fill of an avax order: the payout is its Transfer, the
    source side what the order declares."""
    maker = "0x6a699b852e381bfd36bee888546d5478a324440a"
    assert debridge.read("eth", db_rows("avax_eth_old_fill")) == [
        Bridge(
            fromAddress=maker,
            fromAsset=AVAX_USDC,
            fromAmount=8157964454,
            fromNetwork="avax",
            fromPayment="unknown",
            toAddress=maker,
            toAsset=ETH_USDC,
            toAmount=8153473306,
            toNetwork="eth",
            toPayment=f"{bare('avax_eth_old_fill')}_T680",
        )
    ]


def test_a_batched_fill_is_one_half_per_order():
    """Log 222 fills a bnb order (payout log 221), log 234 a Solana one (payout
    log 233), whose maker and token are not EVM addresses."""
    maker = "0x98fc13632ff112e4667fc4f21ae980571f122b5a"
    assert debridge.read("eth", db_rows("bnb_eth_batched_fill")) == [
        Bridge(
            fromAddress=maker,
            fromAsset=BNB_USDC,
            fromAmount=299523393997777679677480,
            fromNetwork="bnb",
            fromPayment="unknown",
            toAddress=maker,
            toAsset=ETH_USDC,
            toAmount=299399173274,
            toNetwork="eth",
            toPayment=f"{bare('bnb_eth_batched_fill')}_T221",
        ),
        Bridge(
            fromAddress="unknown",
            fromAsset="unknown",
            fromAmount=424115001,
            fromNetwork="unknown",
            fromPayment="unknown",
            toAddress="0x0c8a7f58ab64a1075d54b4b2fb108f27f4bcfc84",
            toAsset=ETH_USDT,
            toAmount=418082679,
            toNetwork="eth",
            toPayment=f"{bare('bnb_eth_batched_fill')}_T233",
        ),
    ]


def test_an_erc20_order_names_who_paid_into_dln_source():
    """fromAddress is the sender of the Transfer into DlnSource (a forwarder),
    not the order's maker."""
    [bridge] = debridge.read("bnb", db_rows("bnb_order_via_forwarder"))
    assert bridge.fromAddress == "0x663dc15d3c1ac63ff12e45ab68fea3f0a883c251"
    assert bridge.fromPayment == f"{bare('bnb_order_via_forwarder')}_T28"
    assert (bridge.fromAsset, bridge.fromAmount) == (
        BNB_USDC,
        299643251298296998476870,
    )
    assert (bridge.toAddress, bridge.toAsset, bridge.toNetwork) == (
        "0x98fc13632ff112e4667fc4f21ae980571f122b5a",
        ETH_USDC,
        "eth",
    )
    assert bridge.toPayment == "pending"


@pytest.mark.parametrize(
    "part", ["eth_arb_native", "avax_eth_old_fill", "bnb_eth_batched_fill"]
)
def test_debridge_spoofed_emitter_is_ignored(part):
    assert debridge.read("eth", db_rows(part, emitter=SPOOF)) == []


def test_a_network_without_dln_contracts_reads_none():
    assert debridge.read("gnosis", db_rows("eth_arb_native")) == []


def test_delegated_half_bridges_include_debridge_on_eth():
    rows = db_rows("bnb_eth_batched_fill")[::-1]
    assert half_bridges("eth", rows, ("debridge",)) == tuple(
        debridge.read("eth", db_rows("bnb_eth_batched_fill"))
    )
    assert half_bridges("eth", rows, ("cctp",)) == ()


@pytest.mark.parametrize("part", ["eth_arb_native", "avax_eth_old_fill"])
def test_debridge_malformed_event_is_no_half_and_no_error(part):
    """A DLN event whose data is too short to decode: no half bridge, no
    exception (an exception would be a 500 on gs-rest's eth conversions)."""
    logs = [
        dict(log, data=log["data"][:40])
        if log["address"] in (unhex(DLN_SOURCE), unhex(DLN_DESTINATION))
        else log
        for log in db_rows(part)
    ]
    assert debridge.read("eth", logs) == []


@pytest.mark.parametrize("error", [IndexError, ValueError])
def test_debridge_decode_errors_are_contained_like_the_other_readers(
    monkeypatch, error
):
    def broken(*args):
        raise error("malformed")

    monkeypatch.setattr(debridge, "_created", broken)
    monkeypatch.setattr(debridge, "_fulfilled", broken)
    for part in ("eth_arb_native", "avax_eth_old_fill"):
        assert debridge.read("eth", db_rows(part)) == []
