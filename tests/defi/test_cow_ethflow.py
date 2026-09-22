"""CoW eth-flow: an ETH sell order linked from its placement to its settlement.

The fixture is a real pair: the cream finance exploiter placed an order
selling 730 ETH for DAI (tx 0x25ee8327…, block 18264564), which a settlement
filled four blocks later (tx 0x3cdece86…, 1,234,402.59 DAI).
"""

import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import graphsenselib.defi.conversions as conversions_mod
from graphsenselib.datatypes.abi import decode_logs_dict
from graphsenselib.defi.models import Trace
from graphsenselib.defi.swapping import cow_ethflow
from graphsenselib.defi.swapping.cow_ethflow import (
    domain_separator,
    get_eth_flow_swaps,
    order_uid,
)

FIXTURE = json.loads((Path(__file__).parent / "cow_ethflow_25ee8327.json").read_text())
ETH_FLOW = "0x40a50cf069e992aa4536211b23f286ef88752187"
SETTLEMENT = "0x9008D19f58AAbD9eD0D60971565AA8510560ab41"
EXPLOITER = "0x70747df6ac244979a2ae9ca1e1a82899d02bbea4"
DAI = "0x6b175474e89094c44da98b954eedeac495271d0f"
# the order uid in the settlement's Trade event
UID = (
    "5b15b5afc3530d8a49e714079d0454fda5522e94362a174f210cad5d4cdfe6f8"
    "40a50cf069e992aa4536211b23f286ef88752187ffffffff"
)


def unhex(s):
    return bytes.fromhex(s.removeprefix("0x"))


def db_rows(part):
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
        for log in tx["logs"]
    ]


def tx_of(part):
    return {
        "tx_hash": unhex(FIXTURE[part]["tx_hash"]),
        "block_id": FIXTURE[part]["block_id"],
    }


# the exploiter's 730 ETH to the eth-flow contract (GraphSense id _I754)
PAYMENT = Trace(
    from_address=EXPLOITER,
    to_address=ETH_FLOW,
    value=730 * 10**18,
    is_call=True,
    trace_index=754,
    trace_address="0",
)


def fake_db(unrelated_block=18264566):
    """The eth-flow contract moved funds in an unrelated block first."""
    db = MagicMock()
    settlement = FIXTURE["settlement"]
    db.list_address_txs = AsyncMock(
        return_value=(
            [
                {"height": unrelated_block, "tx_hash": b"\x01" * 32},
                {
                    "height": settlement["block_id"],
                    "tx_hash": unhex(settlement["tx_hash"]),
                },
            ],
            None,
        )
    )

    async def logs_in_block(network, block, topic=None):
        rows = db_rows("settlement") if block == settlement["block_id"] else []
        return [r for r in rows if topic is None or r["topic0"] == topic]

    db.get_logs_in_block_eth = AsyncMock(side_effect=logs_in_block)
    db.get_tx_by_hash = AsyncMock(return_value=tx_of("settlement"))
    db.fetch_transaction_logs = AsyncMock(return_value=db_rows("settlement"))
    db.fetch_transaction_traces = AsyncMock(return_value=[])
    return db


def placement_dlogs():
    return [dlog for dlog, _ in decode_logs_dict(db_rows("placement"))]


def test_the_order_uid_is_computed_from_the_placement():
    [placement] = placement_dlogs()

    assert placement["name"] == "OrderPlacement"
    # GPv2 mainnet domain separator
    assert domain_separator(1, SETTLEMENT).hex() == (
        "c078f884a2676e1345748b1feace7b0abee5d00ecadb6e574dcdd109a63e8943"
    )
    uid = order_uid(placement["parameters"]["order"], ETH_FLOW, 1, SETTLEMENT)
    assert uid.hex() == UID


async def test_the_placement_is_linked_to_its_settlement():
    db = fake_db()

    [swap] = await get_eth_flow_swaps(
        "eth", db, tx_of("placement"), placement_dlogs(), [PAYMENT]
    )

    placement_tx = FIXTURE["placement"]["tx_hash"]
    settlement_tx = FIXTURE["settlement"]["tx_hash"]
    assert (swap.fromAddress, swap.toAddress) == (EXPLOITER, EXPLOITER)
    assert (swap.fromAsset, swap.toAsset) == ("native", DAI)
    assert swap.fromAmount == 730 * 10**18
    assert swap.toAmount == 1234402594740056975474688
    # from the ETH payment of the placement to the DAI payout of the settlement
    assert swap.fromPayment == f"{placement_tx}_I754"
    assert swap.toPayment == f"{settlement_tx}_T111"
    # only the eth-flow contract's txs after the placement were searched
    kwargs = db.list_address_txs.await_args.kwargs
    assert kwargs["min_height"] == FIXTURE["placement"]["block_id"]
    assert kwargs["direction"] == "out"


async def test_an_order_that_was_not_settled_yields_no_swap():
    db = fake_db()
    db.list_address_txs = AsyncMock(return_value=([], None))

    swaps = await get_eth_flow_swaps(
        "eth", db, tx_of("placement"), placement_dlogs(), [PAYMENT]
    )

    assert swaps == []
    db.get_tx_by_hash.assert_not_awaited()


async def test_the_search_stops_at_its_page_limit():
    db = fake_db()
    db.list_address_txs = AsyncMock(return_value=([], "next-page"))

    with patch.object(cow_ethflow, "SEARCH_MAX_PAGES", 3):
        swaps = await get_eth_flow_swaps(
            "eth", db, tx_of("placement"), placement_dlogs(), [PAYMENT]
        )

    assert swaps == []
    assert db.list_address_txs.await_count == 3


async def test_other_networks_are_not_searched():
    db = fake_db()

    assert await get_eth_flow_swaps("trx", db, tx_of("placement"), [], []) == []
    db.list_address_txs.assert_not_awaited()


async def test_conversions_link_placements_unless_turned_off():
    db = MagicMock()
    db.fetch_transaction_logs = AsyncMock(return_value=db_rows("placement"))
    db.fetch_transaction_traces = AsyncMock(return_value=[])
    linked = AsyncMock(return_value=["LINKED"])

    with (
        patch.object(Trace, "dicts_to_normalized", return_value=[PAYMENT]),
        patch.object(cow_ethflow, "get_eth_flow_swaps", new=linked),
    ):
        on = await conversions_mod.get_conversions_from_db(
            "eth", db, tx_of("placement")
        )
        off = await conversions_mod.get_conversions_from_db(
            "eth", db, tx_of("placement"), cow_protocol_swaps=False
        )

    assert on == ["LINKED"]
    assert off == []
    linked.assert_awaited_once()
