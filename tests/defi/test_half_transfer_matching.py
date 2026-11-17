"""How the half readers (defi/bridging/halves/) pick the ERC20 Transfer that is
a leg, pinned through each reader's `read` over the recorded fixtures with
that Transfer edited: which of token, sender, receiver and value must match,
that two matches leave the leg the bare tx, and that a log of another shape
(an ERC-721 Transfer, a 2-topic Transfer, a log without topics) is passed over
without an error."""

import copy
import json
from pathlib import Path
from typing import NamedTuple

import pytest

from graphsenselib.defi.bridging.half_common import _transfers
from graphsenselib.defi.bridging.halves import across, debridge, layerzero, relay

DATA = Path(__file__).parent / "data" / "bridges"
OTHER = bytes.fromhex("77" * 20)
TRANSFER = bytes.fromhex(
    "ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
)


def unhex(s):
    return bytes.fromhex(s.removeprefix("0x"))


def topic(address: bytes) -> bytes:
    return bytes(12) + address


def fixture_rows(path, part):
    tx = json.loads((DATA / path).read_text())[part]
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


class Case(NamedTuple):
    reader: object
    network: str
    path: str
    part: str
    side: str  # "from" or "to": the half's leg the Transfer is
    log_index: int  # the Transfer that is the leg
    unmatched: str  # "tx": the leg falls back to the tx; "none": no half
    any_token: bool = False
    any_value: bool = False

    def rows(self):
        return fixture_rows(self.path, self.part)

    def bare(self):
        return self.rows()[0]["tx_hash"].hex()

    def read(self, rows):
        return self.reader.read(self.network, rows)

    def legs(self, rows):
        return [getattr(b, f"{self.side}Payment") for b in self.read(rows)]

    def matched(self):
        return [f"{self.bare()}_T{self.log_index}"]

    def unmatched_legs(self):
        return [self.bare()] if self.unmatched == "tx" else []


CASES = {
    "across_fill": Case(
        across, "eth", "across/eth_logs.json", "pol_eth_usdc_fill", "to", 867, "tx"
    ),
    "debridge_fill": Case(
        debridge,
        "eth",
        "debridge/eth_logs.json",
        "avax_eth_old_fill",
        "to",
        680,
        "none",
    ),
    "debridge_order": Case(
        debridge,
        "bnb",
        "debridge/bnb_logs.json",
        "bnb_order_via_forwarder",
        "from",
        28,
        "none",
        any_value=True,
    ),
    "relay_deposit": Case(
        relay, "eth", "relay/eth_logs.json", "eth_erc20_deposit", "from", 349, "tx"
    ),
    "layerzero_send": Case(
        layerzero,
        "eth",
        "layerzero/eth_logs.json",
        "eth_arb_usdc_lifi",
        "from",
        379,
        "none",
        any_token=True,
    ),
    "layerzero_receive": Case(
        layerzero,
        "eth",
        "layerzero/eth_logs.json",
        "arb_eth_usdc",
        "to",
        195,
        "none",
        any_token=True,
    ),
}


def edit(rows, log_index, **fields):
    """rows with the log at log_index updated by fields (values: callables of
    the old log)."""
    out = []
    for log in rows:
        if log["log_index"] == log_index:
            log = dict(log, **{k: f(log) for k, f in fields.items()})
        out.append(log)
    return out


def insert(rows, log_index, make, after):
    """rows with make(the log at log_index) put right before or after it."""
    out = []
    for log in rows:
        if log["log_index"] == log_index:
            extra = make(copy.deepcopy(log))
            out.extend([log, *extra] if after else [*extra, log])
        else:
            out.append(log)
    return out


def value(log):
    return int.from_bytes(log["data"], "big")


@pytest.fixture(params=list(CASES), ids=list(CASES))
def case(request):
    return CASES[request.param]


def test_the_recorded_transfer_is_the_leg(case):
    assert case.legs(case.rows()) == case.matched()


def test_token(case):
    rows = edit(case.rows(), case.log_index, address=lambda log: OTHER)
    if case.any_token:
        # any token: the half's asset is the one the Transfer moved
        (bridge,) = case.read(rows)
        assert getattr(bridge, f"{case.side}Payment") == case.matched()[0]
        assert getattr(bridge, f"{case.side}Asset") == "0x" + OTHER.hex()
    else:
        assert case.legs(rows) == case.unmatched_legs()


def test_receiver(case):
    rows = edit(
        case.rows(),
        case.log_index,
        topics=lambda log: [log["topics"][0], log["topics"][1], topic(OTHER)],
    )
    assert case.legs(rows) == case.unmatched_legs()


def test_sender_is_anyone(case):
    rows = edit(
        case.rows(),
        case.log_index,
        topics=lambda log: [log["topics"][0], topic(OTHER), log["topics"][2]],
    )
    assert case.legs(rows) == case.matched()


@pytest.mark.parametrize("delta", [1, -1])
def test_exact_value(case, delta):
    rows = edit(
        case.rows(),
        case.log_index,
        data=lambda log: (value(log) + delta).to_bytes(32, "big"),
    )
    if case.any_value:
        assert case.legs(rows) == case.matched()
    else:
        assert case.legs(rows) == case.unmatched_legs()


def test_two_matches_leave_the_leg_the_tx(case):
    rows = insert(case.rows(), case.log_index, lambda log: [log], after=True)
    assert case.legs(rows) == [case.bare()]


def test_other_shapes_are_passed_over(case):
    def neighbours(log):
        tokenid = (1).to_bytes(32, "big")
        return [
            dict(log, topics=[*log["topics"], tokenid], data=b""),  # ERC-721
            dict(log, topics=log["topics"][:2]),
            dict(log, topics=[]),
            {k: v for k, v in log.items() if k not in ("topics", "topic0")},
        ]

    rows = insert(case.rows(), case.log_index, neighbours, after=False)
    assert case.legs(rows) == case.matched()


# reader-specific consequences of what matched


def test_debridge_order_reports_what_the_transfer_paid():
    case = CASES["debridge_order"]
    rows = edit(case.rows(), case.log_index, data=lambda log: (5).to_bytes(32, "big"))
    (bridge,) = case.read(rows)
    assert (bridge.fromPayment, bridge.fromAmount) == (case.matched()[0], 5)


def test_debridge_order_with_two_fundings_sums_them_and_names_the_maker():
    case = CASES["debridge_order"]
    (one,) = case.read(case.rows())
    rows = insert(case.rows(), case.log_index, lambda log: [log], after=True)
    (bridge,) = case.read(rows)
    assert bridge.fromPayment == case.bare()
    assert bridge.fromAmount == 2 * one.fromAmount
    assert bridge.fromAddress != one.fromAddress  # the maker, not the forwarder


def across_deposit_rows(transfers):
    """The native Across deposit with `transfers` (sender, value, log_index) of
    the input token into the SpokePool put right before its event."""
    rows = fixture_rows("across/eth_logs.json", "eth_base_native")
    wrap = next(
        log for log in rows if log["topics"][0] != TRANSFER and len(log["topics"]) == 2
    )
    weth = wrap["address"]
    spoke = unhex("0x5c7bcd6e7de5423a257d81b442095a1a6ced35c5")
    event = next(i for i, log in enumerate(rows) if log["address"] == spoke)
    made = [
        dict(
            rows[event],
            log_index=index,
            address=weth,
            topics=[TRANSFER, topic(sender), topic(spoke)],
            data=amount.to_bytes(32, "big"),
        )
        for sender, amount, index in transfers
    ]
    return rows[:event] + made + rows[event:]


AMOUNT = 12628350647202970  # the deposit's inputAmount


def test_across_erc20_deposit_is_funded_by_its_transfer_from_anyone():
    (bridge,) = across.read("eth", across_deposit_rows([(OTHER, AMOUNT, 9000)]))
    tx = fixture_rows("across/eth_logs.json", "eth_base_native")[0]["tx_hash"].hex()
    assert bridge.fromPayment == f"{tx}_T9000"
    assert bridge.fromAddress == "0x" + OTHER.hex()
    assert bridge.fromAsset != "native"


def test_across_deposit_needs_the_exact_input_amount():
    (bridge,) = across.read("eth", across_deposit_rows([(OTHER, AMOUNT - 1, 9000)]))
    assert bridge.fromAsset == "native"
    assert not bridge.fromPayment.endswith("_T9000")


def test_across_deposit_with_two_fundings_names_the_depositor():
    (bridge,) = across.read(
        "eth", across_deposit_rows([(OTHER, AMOUNT, 9000), (OTHER, AMOUNT, 9001)])
    )
    tx = fixture_rows("across/eth_logs.json", "eth_base_native")[0]["tx_hash"].hex()
    assert bridge.fromPayment == tx
    assert bridge.fromAddress == "0x42fda7e1f87cc910fcdaaa18ee0f7a8ecb0c8f64"


def lz_send_rows():
    case = CASES["layerzero_send"]
    rows = case.rows()
    oft_sent = unhex(
        "0x85496b760a4b7f8d66384b9df21b381f5d1b1e79f229a47aaf4c232edc2fe59a"
    )
    sender = next(log for log in rows if log["topics"][0] == oft_sent)["topics"][2]
    return case, rows, sender


def test_layerzero_send_burnt_from_its_sender_is_funded():
    case, rows, sender = lz_send_rows()
    burnt = edit(
        rows,
        case.log_index,
        topics=lambda log: [TRANSFER, sender, topic(bytes(20))],
    )
    assert case.legs(burnt) == case.matched()


def test_layerzero_burn_from_another_sender_funds_nothing():
    case, rows, _ = lz_send_rows()
    burnt = edit(
        rows,
        case.log_index,
        topics=lambda log: [TRANSFER, topic(OTHER), topic(bytes(20))],
    )
    assert case.legs(burnt) == []


def test_layerzero_send_prefers_the_lock_over_a_burn():
    case, rows, sender = lz_send_rows()

    def burn(log):
        return [dict(log, log_index=9000, topics=[TRANSFER, sender, topic(bytes(20))])]

    assert case.legs(insert(rows, case.log_index, burn, after=True)) == case.matched()


# the shared matcher (half_common._transfers) itself

TOKEN, FROM, TO = (bytes.fromhex(c * 40) for c in "abc")


def transfer(log_index, token=TOKEN, sender=FROM, receiver=TO, amount=5):
    return {
        "log_index": log_index,
        "address": token,
        "topics": [TRANSFER, topic(sender), topic(receiver)],
        "data": amount.to_bytes(32, "big"),
    }


def matched(logs, **given):
    return [log["log_index"] for log in _transfers(logs, **given)]


def test_matcher_without_criteria_is_every_erc20_transfer_in_log_order():
    logs = [transfer(3), transfer(1, token=OTHER), transfer(2, amount=7)]
    assert matched(logs) == [3, 1, 2]


@pytest.mark.parametrize(
    "field, wrong",
    [("token", OTHER), ("sender", OTHER), ("receiver", OTHER), ("value", 6)],
)
def test_matcher_each_given_field_must_match(field, wrong):
    right = {"token": TOKEN, "sender": FROM, "receiver": TO, "value": 5}
    logs = [transfer(0)]
    assert matched(logs, **right) == [0]
    assert matched(logs, **dict(right, **{field: wrong})) == []


def test_matcher_value_is_exact_and_the_whole_data_word():
    logs = [transfer(0, amount=5), transfer(1, amount=50), transfer(2, amount=4)]
    assert matched(logs, value=5) == [0]
    padded = dict(transfer(3), data=(5).to_bytes(64, "big"))
    assert matched([padded], value=5) == [3]  # the data read as one integer


def test_matcher_compares_the_low_20_bytes_of_an_address_topic():
    log = transfer(0)
    log["topics"][2] = b"\xff" * 12 + TO
    assert matched([log], receiver=TO) == [0]


def test_matcher_passes_over_logs_of_another_shape():
    base = transfer(0)
    logs = [
        dict(base, log_index=1, topics=[*base["topics"], bytes(32)]),  # ERC-721
        dict(base, log_index=2, topics=base["topics"][:2]),
        dict(base, log_index=3, topics=[bytes(32), *base["topics"][1:]]),
        dict(base, log_index=4, topics=[]),
        dict(base, log_index=5, topics=None),
        {k: v for k, v in base.items() if k != "topics"} | {"log_index": 6},
        base,
    ]
    assert matched(logs, token=TOKEN, sender=FROM, receiver=TO, value=5) == [0]
    assert matched(logs) == [0]


def test_matcher_keeps_every_match():
    logs = [transfer(0), transfer(1, sender=OTHER), transfer(2)]
    assert matched(logs, token=TOKEN, receiver=TO, value=5) == [0, 1, 2]
