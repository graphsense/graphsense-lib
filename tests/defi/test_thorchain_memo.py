"""THORChain memo detection must not swallow other protocols' OP_RETURN memos.

The recorded txs are Bridgers BTC deposits (mempool.space, blocks 961912-961941)
whose memo tag happens to start with "s"/"S", e.g.
``Sys:to:USDT(TRON):TTbUenkf5gj1iWLzwZsd2w3cog4Pni2qpY``. A first-character test
took them for THORChain swaps, so the UTXO branch of get_conversions_from_db
routed them into the THORChain path, which found no swap and answered ``[]``.
"""

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from graphsenselib.defi.bridging import thorchain
from graphsenselib.defi.conversions import get_conversions_from_db

FIXTURE = Path(__file__).parent / "btc_non_thorchain_memo_txs.json"


def _io(entry, script_hex=None):
    address = entry.get("address")
    return SimpleNamespace(
        address=[address] if address else [],
        value=entry.get("value"),
        script_hex=bytes.fromhex(script_hex) if script_hex else None,
    )


def to_cassandra_tx(recorded):
    """A mempool.space tx in the shape the Cassandra UTXO tx row has."""
    inputs = [_io(i) for i in recorded["vin"]]
    outputs = [_io(o, o["scriptpubkey"]) for o in recorded["vout"]]
    return {
        "tx_hash": bytes.fromhex(recorded["txid"]),
        "block_id": recorded["block_height"],
        "inputs": inputs,
        "outputs": outputs,
        "total_input": sum(i.value for i in inputs),
        "total_output": sum(o.value for o in outputs),
    }


RECORDED = json.loads(FIXTURE.read_text())


@pytest.mark.parametrize("recorded", RECORDED, ids=lambda r: r["txid"][:16])
def test_bridgers_memo_is_not_a_thorchain_deposit(recorded):
    tx = to_cassandra_tx(recorded)
    memo = thorchain.extract_memo_from_utxo_tx(tx)
    assert memo[0] in "sS" and memo[3:7] == ":to:"
    assert not thorchain.is_thorchain_memo(memo)
    assert not thorchain.is_thorchain_utxo_deposit(tx)
    assert not thorchain.is_thorchain_utxo_receive(tx)


@pytest.mark.asyncio
async def test_bridgers_memo_never_reaches_the_thorchain_send_path(monkeypatch):
    async def _fail(*_args, **_kwargs):
        raise AssertionError("THORChain send path must not run")
        yield  # pragma: no cover

    monkeypatch.setattr(
        "graphsenselib.defi.conversions.get_bridges_from_thorchain_utxo_send", _fail
    )
    tx = to_cassandra_tx(RECORDED[0])
    result = await get_conversions_from_db(
        "btc", db=None, tx=tx, included_bridges=("thorchain",)
    )
    assert result == []


def _old_is_thorchain_memo(memo):
    """The predicate before this fix, kept verbatim to pin what changed."""
    if not memo or len(memo) < 2:
        return False
    return memo[0] in ["=", "s", "S", "+", "-", "~"]


# Detected before the fix and still detected: every THORChain function the
# first-character test reached (= s S + - ~), in long, short and any case.
STILL_DETECTED = [
    "=:b:bc1q6fxj6446h2gr40wtfkzqyfk848gw4q4t8akr2p:0/1/0:dx:30",
    "=:ETH.ETH:0x19317e026ef473d44D746d364062539Ba7Cb0fa3:117890211/1/0:wr:100",
    "SWAP:BTC.BTC:bc1qxyz",
    "swap:BTC.BTC:bc1qxyz",
    "SwAp:BTC.BTC:bc1qxyz",
    "s:e:0x19317e026ef473d44D746d364062539Ba7Cb0fa3",
    "S:e:0x19317e026ef473d44D746d364062539Ba7Cb0fa3",
    "=<:ETH.ETH:0x19317e026ef473d44D746d364062539Ba7Cb0fa3:1e6",
    "+:BTC.BTC:thor1abc",
    "-:BTC.BTC:10000",
    "-:",
    "~:name:BTC:bc1qxyz",
    "SECURE+:thor1abc",
    "secure-:bc1qxyz",
    "STABLE+",
    "stable+",
    "stable+|a comment",
    "stable-:thor1abc",
    "SWITCH:thor1abc",
    "=:BTC.BTC:bc1qxyz|a comment",
]

# Not detected before the fix and still not detected: THORChain functions the
# first-character test never reached. Out of scope here, pinned as unchanged.
STILL_NOT_DETECTED = [
    "ADD:BTC.BTC",
    "a:BTC.BTC:thor1abc",
    "wd:BTC.BTC:10000",
    "WITHDRAW:BTC.BTC:10000",
    "$+:ETH.ETH:0xabc",
    "loan-:BTC.BTC:bc1qxyz",
    "TRADE+:thor1abc",
    "POOL-:10000",
    "n:name:BTC:bc1qxyz",
    "BOND:thor1abc",
    "LEAVE:thor1abc",
    "DONATE:BTC.BTC",
    "NOOP:NOVAULT",
    "r:12345",
    "TCY:thor1abc",
    " s:BTC.BTC:bc1qxyz",
    " SwAp:BTC.BTC:bc1qxyz",
    "OUT:EA7D80B3EB709319A6577AF6CF4DEFF67975D4F5A93CD8817E7FF04A048D1C5C",
    "REFUND:1234567890ABCDEF",
    "MIGRATE:1234",
    "RAGNAROK:1234",
    "consolidate:x",
    "=",
    "s",
    "-",
    "",
    None,
]

# The only change: detected before, rejected now. Each starts with = s S + - ~
# but its function is none THORChain knows.
NEWLY_REJECTED = [
    "Sys:to:USDT(TRON):TTbUenkf5gj1iWLzwZsd2w3cog4Pni2qpY",
    "sU2:to:USDT(TRON):TLJUUbebT9prhuTnNgKyFeR7n6BYbLefjx",
    "Smo:to:USDT(TRON):TJHXRCWw7pjUikB88fK2yhoMNy8VhUXJzY",
    "scn:to:USDT(TRON):TLWMbX1yP1ZaqfGENHY1bJCZLsQnXW5GQK",
    "S2Z:to:USDT(TRON):TXWrNZKVA2KURSUro5NxL1Viz9qBCzzwma",
    "swapper:BTC.BTC:bc1q",
    "+abc",
    "~hello",
    "sending to a friend",
]


@pytest.mark.parametrize("memo", STILL_DETECTED)
def test_thorchain_memos_detected_before_are_still_detected(memo):
    assert _old_is_thorchain_memo(memo)
    assert thorchain.is_thorchain_memo(memo)


@pytest.mark.parametrize("memo", STILL_NOT_DETECTED)
def test_memos_rejected_before_are_still_rejected(memo):
    assert not _old_is_thorchain_memo(memo)
    assert not thorchain.is_thorchain_memo(memo)


@pytest.mark.parametrize("memo", NEWLY_REJECTED)
def test_only_memos_with_an_unknown_function_change(memo):
    assert _old_is_thorchain_memo(memo)
    assert not thorchain.is_thorchain_memo(memo)


def test_the_fix_accepts_nothing_the_old_check_rejected():
    """New ⊆ old over every memo in this module plus the recorded txs."""
    recorded = [
        thorchain.extract_memo_from_utxo_tx(to_cassandra_tx(r)) for r in RECORDED
    ]
    corpus = STILL_DETECTED + STILL_NOT_DETECTED + NEWLY_REJECTED + recorded
    corpus += [f"{f}:x" for f in thorchain.THORCHAIN_DEPOSIT_FUNCTIONS]
    corpus += [f"{f.upper()}:x" for f in thorchain.THORCHAIN_DEPOSIT_FUNCTIONS]
    for memo in corpus:
        if thorchain.is_thorchain_memo(memo):
            assert _old_is_thorchain_memo(memo), memo


@pytest.mark.parametrize("memo", ["OUT:ABCDEF", "out:abcdef", "REFUND:ABCDEF"])
def test_receive_memos_stay_receives_not_deposits(memo):
    script = "6a" + "{:02x}".format(len(memo.encode())) + memo.encode().hex()
    tx = {"outputs": [SimpleNamespace(address=[], value=0, script_hex=script)]}
    assert thorchain.is_thorchain_utxo_receive(tx)
    assert not thorchain.is_thorchain_utxo_deposit(tx)
