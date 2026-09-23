"""CoW Protocol settlements: one swap per order."""

import json
from pathlib import Path

from graphsenselib.datatypes.abi import decode_logs_dict
from graphsenselib.defi.models import Trace
from graphsenselib.defi.swapping.models import (
    SwapStrategy,
    get_swap_strategy_from_decoded_logs,
)
from graphsenselib.defi.swaps import get_swap_from_decoded_logs

SETTLEMENT = "0x9008d19f58aabd9ed0d60971565aa8510560ab41"
# the Trade event uses 0xeee...e for native ETH
ETH = "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
TX_HASH = bytes.fromhex("ab" * 32)
FIXTURE = Path(__file__).parent / "cow_settlement_f536d216.json"


def load_fixture():
    """Raw logs of mainnet tx 0xf536d216…e8a1 (block 15000000), one order."""

    def unhex(s):
        return bytes.fromhex(s.removeprefix("0x"))

    rows = json.loads(FIXTURE.read_text())
    return [
        {
            "tx_hash": unhex(r["txHash"]),
            "log_index": r["logIndex"],
            "address": unhex(r["address"]),
            "data": unhex(r["data"]),
            "topics": [unhex(t) for t in r["topics"]],
        }
        for r in rows
    ]


def settle(logs_raw, traces=(), **kwargs):
    dlogs, raw = zip(*decode_logs_dict(logs_raw))
    return get_swap_from_decoded_logs(list(dlogs), list(raw), list(traces), **kwargs)


def test_a_real_settlement_yields_the_order():
    [swap] = settle(load_fixture())

    tx = "0xf536d21611d50d9abe91d22f7f81456acc4d6d0c0c42cfb67d13908c05d0e8a1"
    owner = "0xb6353ca4d6b2a2d9eef6d37a79cec59c4e404fe6"
    assert (swap.fromAddress, swap.toAddress) == (owner, owner)
    # 3094.6 CVX for 13323.104431 USDC
    assert swap.fromAsset == "0x4e3fbd56cd56c3e72c1403e103b45db9da5b9d2b"
    assert swap.toAsset == "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
    assert (swap.fromAmount, swap.toAmount) == (3094600000000000000000, 13323104431)
    # the owner's transfer in and the payout, not the solver's AMM hops
    assert swap.fromPayment == f"{tx}_T191"
    assert swap.toPayment == f"{tx}_T217"


def test_the_toggle_turns_settlements_off():
    assert settle(load_fixture(), cow_protocol=False) == []


# synthetic settlements, as decoded logs


def log(name, address, tags=("token", "erc20"), **parameters):
    return {
        "name": name,
        "address": address,
        "parameters": parameters,
        "log_def": {"tags": list(tags)},
    }


def trade(owner, sell, buy, sell_amount, buy_amount, fee=0):
    return log(
        "Trade",
        SETTLEMENT,
        tags=("cow-protocol", "trade", "swap", "dex"),
        owner=owner,
        sellToken=sell,
        buyToken=buy,
        sellAmount=sell_amount,
        buyAmount=buy_amount,
        feeAmount=fee,
        orderUid=b"",
    )


def transfer(token, frm, to, value):
    return log("Transfer", token, **{"from": frm, "to": to, "value": value})


def settlement_event():
    return log("Settlement", SETTLEMENT, tags=("cow-protocol", "settlement"))


def detect(dlogs, traces=()):
    raw = [{"tx_hash": TX_HASH, "log_index": i} for i in range(len(dlogs))]
    return get_swap_from_decoded_logs(dlogs, raw, list(traces))


USDC, DAI, WETH = "0xusdc", "0xdai", "0xweth"
ALICE, BOB, CAROL, POOL = "0xalice", "0xbob", "0xcarol", "0xpool"


def test_orders_of_several_owners_are_separate_swaps():
    swaps = detect(
        [
            trade(ALICE, USDC, DAI, 100, 99),
            trade(BOB, DAI, USDC, 50, 51),
            transfer(USDC, ALICE, SETTLEMENT, 100),  # 2
            transfer(DAI, BOB, SETTLEMENT, 50),  # 3
            # the solver's interaction, no order's transfer
            transfer(USDC, SETTLEMENT, POOL, 49),
            transfer(DAI, POOL, SETTLEMENT, 49),
            transfer(DAI, SETTLEMENT, ALICE, 99),  # 6
            transfer(USDC, SETTLEMENT, BOB, 51),  # 7
            settlement_event(),
        ]
    )

    tx = "0x" + TX_HASH.hex()
    assert [
        (s.fromAddress, s.fromAsset, s.toAsset, s.fromPayment, s.toPayment)
        for s in swaps
    ] == [
        (ALICE, USDC, DAI, f"{tx}_T2", f"{tx}_T6"),
        (BOB, DAI, USDC, f"{tx}_T3", f"{tx}_T7"),
    ]


def test_the_receiver_can_differ_from_the_owner():
    [swap] = detect(
        [
            trade(ALICE, USDC, DAI, 100, 99),
            transfer(USDC, ALICE, SETTLEMENT, 100),
            transfer(DAI, SETTLEMENT, CAROL, 99),
            settlement_event(),
        ]
    )

    assert (swap.fromAddress, swap.toAddress) == (ALICE, CAROL)


def test_a_fee_pulled_on_top_of_the_sell_amount():
    [swap] = detect(
        [
            trade(ALICE, USDC, DAI, 100, 99, fee=2),
            transfer(USDC, ALICE, SETTLEMENT, 102),
            transfer(DAI, SETTLEMENT, ALICE, 99),
            settlement_event(),
        ]
    )

    assert swap.fromAmount == 102


def test_equal_orders_do_not_share_transfers():
    swaps = detect(
        [
            trade(ALICE, USDC, DAI, 100, 99),
            trade(ALICE, USDC, DAI, 100, 99),
            transfer(USDC, ALICE, SETTLEMENT, 100),  # 2
            transfer(USDC, ALICE, SETTLEMENT, 100),  # 3
            transfer(DAI, SETTLEMENT, ALICE, 99),  # 4
            transfer(DAI, SETTLEMENT, ALICE, 99),  # 5
            settlement_event(),
        ]
    )

    tx = "0x" + TX_HASH.hex()
    assert [(s.fromPayment, s.toPayment) for s in swaps] == [
        (f"{tx}_T2", f"{tx}_T4"),
        (f"{tx}_T3", f"{tx}_T5"),
    ]


def test_eth_is_paid_out_by_an_internal_call():
    [swap] = detect(
        [
            trade(ALICE, USDC, ETH, 100, 5),
            transfer(USDC, ALICE, SETTLEMENT, 100),
            log("Withdrawal", WETH, tags=("weth",), src=SETTLEMENT, wad=5),
            settlement_event(),
        ],
        traces=[
            Trace(
                from_address="0xsolver",
                to_address=SETTLEMENT,
                value=0,
                is_call=True,
                trace_index=0,
                trace_address="",
            ),
            Trace(
                from_address=WETH,
                to_address=SETTLEMENT,
                value=5,
                is_call=True,
                trace_index=3,
                trace_address="0_1",
            ),
            Trace(
                from_address=SETTLEMENT,
                to_address=CAROL,
                value=5,
                is_call=True,
                trace_index=4,
                trace_address="0_2",
            ),
        ],
    )

    assert swap.toAsset == "native"
    assert swap.toAddress == CAROL
    assert swap.toPayment == f"0x{TX_HASH.hex()}_I4"


def test_an_order_without_its_transfers_is_skipped():
    swaps = detect(
        [
            trade(ALICE, USDC, DAI, 100, 99),
            trade(BOB, DAI, USDC, 50, 51),
            transfer(USDC, ALICE, SETTLEMENT, 100),
            transfer(DAI, SETTLEMENT, ALICE, 99),
            # Bob's order is not paid out
            transfer(DAI, BOB, SETTLEMENT, 50),
            settlement_event(),
        ]
    )

    assert [s.fromAddress for s in swaps] == [ALICE]


def test_trade_events_of_other_contracts_are_no_cow_settlement():
    other = trade(ALICE, USDC, DAI, 100, 99)
    other["address"] = "0xnotcow"

    # left to the other strategies (here: a swap, by its tags)
    assert get_swap_strategy_from_decoded_logs([other]) is SwapStrategy.SWAP


def test_strategy_follows_the_toggle():
    dlogs = [trade(ALICE, USDC, DAI, 100, 99), settlement_event()]

    assert get_swap_strategy_from_decoded_logs(dlogs) is SwapStrategy.COW_SETTLEMENT
    assert (
        get_swap_strategy_from_decoded_logs(dlogs, cow_protocol=False)
        is SwapStrategy.IGNORE
    )
