"""Across half bridges (defi/bridging/halves/across.py).

The fixture holds the eth receipt logs of five real mainnet Across txs on eth's
SpokePool, copied from the external backend adapter's recordings
(tests/data/bridges/across/gslib/eth_logs.json): two deposits (one in each ABI
era) and three fills, native and ERC20.
"""

import json

import pytest
from pathlib import Path

from graphsenselib.defi.bridging.delegated import half_bridges
from graphsenselib.defi.bridging.half_common import PENDING, UNKNOWN
from graphsenselib.defi.bridging.halves import across
from graphsenselib.defi.bridging.models import Bridge

FIXTURE = json.loads(
    (
        Path(__file__).parent / "data" / "bridges" / "across" / "eth_logs.json"
    ).read_text()
)
SPOKE_POOL = "0x5c7bcd6e7de5423a257d81b442095a1a6ced35c5"
USDC = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
HANDLER = "0x0f7ae28de1c8532170ad4ee566b5801485c13a0e"  # Across multicall handler


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


def test_across_eth_source_half():
    # native ETH: the SpokePool wraps msg.value (WETH Deposit), no Transfer, so
    # the leg is the tx; base is the destination the deposit names
    assert across.read("eth", db_rows("eth_base_native")) == [
        Bridge(
            fromAddress="0x42fda7e1f87cc910fcdaaa18ee0f7a8ecb0c8f64",
            fromAsset="native",
            fromAmount=12628350647202970,
            fromNetwork="eth",
            fromPayment=bare("eth_base_native"),
            toAddress="0x42fda7e1f87cc910fcdaaa18ee0f7a8ecb0c8f64",
            toAsset="0x4200000000000000000000000000000000000006",
            toAmount=12623766337738166,
            toNetwork="base",
            toPayment="pending",
            protocol="across",
            transferId="3487163",  # FundsDeposited topic 2, the depositId
        )
    ]


def test_across_eth_legacy_source_half():
    # V3FundsDeposited: address-typed fields, uint32 depositId
    [half] = across.read("eth", db_rows("eth_pol_v3_legacy"))
    assert (half.fromAsset, half.fromPayment, half.fromAmount) == (
        "native",
        bare("eth_pol_v3_legacy"),
        0x4536EE82B69AD8AC,
    )
    assert (half.toNetwork, half.toAddress, half.toAsset, half.toPayment) == (
        "pol",
        "0x89f423567c2648bb828c3997f60c47b54f57fa6e",
        "0x7ceb23fd6bc0add59e62ac25578270cff1b9f619",
        "pending",
    )


def test_across_eth_autotoken_deposit_names_no_asset():
    # an old-era deposit may name outputToken 0x0 ("any"): no asset declared
    [half] = across.read("eth", db_rows("eth_opt_v3_legacy_autotoken"))
    assert (half.toNetwork, half.toAsset) == ("opt", "unknown")


def test_across_eth_destination_half():
    # USDC paid by the relayer to the recipient, the multicall handler, after
    # the fill's event; pol is the origin the fill names
    assert across.read("eth", db_rows("pol_eth_usdc_fill")) == [
        Bridge(
            # the depositor is not the deposit's payer (a router may pay)
            fromAddress="unknown",
            fromAsset="0x3c499c542cef5e3811e1192ce70d8cc03d5c3359",
            fromAmount=4995039812,
            fromNetwork="pol",
            fromPayment="unknown",
            toAddress=HANDLER,
            toAsset=USDC,
            toAmount=4993359522,
            toNetwork="eth",
            toPayment=f"{bare('pol_eth_usdc_fill')}_T867",
            protocol="across",
            transferId="2014742",  # FilledRelay topic 2, pol's depositId
        )
    ]


def test_across_eth_native_destination_half():
    # the SpokePool unwraps the relayer's WETH (Withdrawal) and pays ETH: the tx
    [half] = across.read("eth", db_rows("base_eth_weth_fill"))
    assert (half.toAsset, half.toPayment, half.toAmount, half.toAddress) == (
        "native",
        bare("base_eth_weth_fill"),
        17736178764405541,
        "0xa9c9259ca0efe93e2af7bda7d8f438683d4b1fd6",
    )
    assert (half.fromNetwork, half.fromPayment) == ("base", "unknown")


def test_across_spoofed_emitter_is_ignored():
    for case in FIXTURE:
        logs = [
            dict(log, address="0x" + "66" * 20) if log["address"] == SPOKE_POOL else log
            for log in FIXTURE[case]["logs"]
        ]
        assert across.read("eth", db_rows(case, logs)) == []


def test_unserved_destination_is_unknown():
    logs = [dict(log) for log in FIXTURE["eth_base_native"]["logs"]]
    deposit = next(log for log in logs if log["address"] == SPOKE_POOL)
    # Across' chain id for Solana
    deposit["topics"] = [
        deposit["topics"][0],
        "0x" + f"{34268394551451:064x}",
        *deposit["topics"][2:],
    ]
    [half] = across.read("eth", db_rows("eth_base_native", logs))
    assert (half.toNetwork, half.toAddress, half.toAsset, half.toPayment) == (
        "unknown",
        "unknown",
        "unknown",
        "pending",
    )


def test_registered_for_eth():
    halves = half_bridges("eth", db_rows("pol_eth_usdc_fill"), ("across",))
    assert [h.toPayment for h in halves] == [f"{bare('pol_eth_usdc_fill')}_T867"]
    assert half_bridges("eth", db_rows("pol_eth_usdc_fill"), ("cctp",)) == ()


# The adapter's own reading of the same receipts (its services/bridges/across.py
# over tests/data/bridges/across/, the eth legs, 2026-11-19): leg, address,
# asset, amount and transfer id. A pair the adapter answers for a delegated
# half names exactly these legs, so the two readers must agree.
# (case, side, leg, address, asset, amount, transfer id; tx sender for bare legs)
ROUTER = "0x3a23f943181408eac424116af7b7790c94cb97a5"  # paid into the SpokePool

ADAPTER_LEGS = [
    ("eth_base_native", "from", None, "0x42fda7e1f87cc910fcdaaa18ee0f7a8ecb0c8f64",
     "native", 12628350647202970, "3487163"),
    ("eth_pol_v3_legacy", "from", None, "0xfee1a21c1373577c08b49d543835e0346b138cae",
     "native", 4987435882536163500, "1642578"),
    ("eth_opt_v3_legacy_autotoken", "from", None,
     "0xd43fe10d2642c1e5b38bb219f15c871ae015d622", "native", 2000000000000000,
     "1642579"),
    ("base_eth_weth_fill", "to", None, "0xa9c9259ca0efe93e2af7bda7d8f438683d4b1fd6",
     "native", 17736178764405541, "5154354"),
    ("pol_eth_usdc_fill", "to", "_T867", HANDLER, USDC, 4993359522, "2014742"),
    # Across V2.5 (May 2023 to V3): other events, a fill pays before its event
    ("eth_opt_v2_usdc", "from", "_T210", ROUTER, USDC, 48657000, "1020463"),
    ("eth_arb_v2_native", "from", None, "0xae5ec9e91c11ed7e96fda9c2b9febd0458cd6154",
     "native", 60000000000000000, "1020459"),
    ("arb_eth_v2_usdc_fill", "to", "_T258", "0x2f60ac622f4091316fa15d612fed88bc337351ed",
     USDC, 493780871, "1082928"),
    ("pol_eth_v2_native_fill", "to", None, "0xe4afb444784f16f653c0b71c53634371a2534ae4",
     "native", 56482383258513981, "1037835"),
]  # fmt: skip


@pytest.mark.parametrize(
    "case, side, suffix, address, asset, amount, transfer_id", ADAPTER_LEGS
)
def test_the_eth_halves_name_the_adapters_legs(
    case, side, suffix, address, asset, amount, transfer_id
):
    # a bare (native) source leg is paid by the tx's sender: the adapter's
    # address for it is that sender
    sender = address if side == "from" else "0x" + "11" * 20
    [half] = half_bridges("eth", db_rows(case), ("across",), tx_sender=sender)
    payment = "fromPayment" if side == "from" else "toPayment"
    assert getattr(half, payment) == bare(case) + (suffix or "")
    assert getattr(half, f"{side}Address") == address
    assert (getattr(half, f"{side}Asset"), getattr(half, f"{side}Amount")) == (
        asset,
        amount,
    )
    assert (half.protocol, half.transferId) == ("across", transfer_id)


def test_a_bare_source_legs_payer_is_the_tx_sender_not_the_depositor():
    sender = "0x" + "77" * 20
    [half] = half_bridges(
        "eth", db_rows("eth_base_native"), ("across",), tx_sender=sender
    )
    assert half.fromAddress == sender


def test_a_v2_deposit_names_no_destination_asset():
    # a V2 deposit names no output token or amount: the route picked them
    [half] = across.read("eth", db_rows("eth_opt_v2_usdc"))
    assert (half.toNetwork, half.toAsset, half.toAmount, half.toPayment) == (
        "opt",
        UNKNOWN,
        0,
        PENDING,
    )
    assert half.toAddress == "0xa8db9142ed818df037867bdbf0810f03057d9851"


def test_a_v2_fill_names_no_origin_asset():
    # the fill gives the pre-fee amount but not the origin token
    [half] = across.read("eth", db_rows("arb_eth_v2_usdc_fill"))
    assert (half.fromNetwork, half.fromAsset, half.fromAmount) == (
        "arb",
        UNKNOWN,
        499972774,
    )
