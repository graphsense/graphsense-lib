"""The back-to-back driver.

The hazard a comparison harness carries is that its failure mode is SILENCE: a
call that did not really run, an exception that was swallowed, or a model that
compared by identity all produce a clean-looking report. These tests are aimed
at that, not at the plumbing.
"""

import asyncio
from types import SimpleNamespace

from graphsense_v3 import backtest
from graphsense_v3.db.legacy import NotAvailable

LTC_P2PKH = "LLcHNPNWE7s6FfLzkt4fD8kJPbsK1V8pyT"
BTC_VERSIONED = "12PL7B4g9Td2zreqak5Mw7gYBPW2vmsiUj"


def services(fn):
    """A stand-in container whose every service returns what `fn` returns."""
    service = SimpleNamespace(
        get_address=fn,
        list_address_txs=fn,
        list_address_neighbors=fn,
        get_address_entity=fn,
        get_block=fn,
        list_block_txs=fn,
        get_tx=fn,
        get_currency_statistics=fn,
    )
    return SimpleNamespace(
        addresses_service=service,
        blocks_service=service,
        txs_service=service,
        stats_service=service,
    )


def call(invoke=None):
    return backtest.Call(
        "get_address",
        backtest.ADDRESS,
        invoke or (lambda s, n, v: s.addresses_service.get_address(n, v)),
    )


def run(coro):
    return asyncio.run(coro)


def test_an_unavailable_v3_call_is_skipped_not_an_agreement() -> None:
    """The failure this prevents: v3 has no cluster tables, and a report that
    scored those nine methods as "no differences" would claim parity for the
    largest missing feature."""

    async def v2(n, v):
        return {"balance": 1}

    async def v3(n, v):
        raise NotAvailable("no cluster tables in v3")

    report = run(
        backtest.run_call(call(), services(v2), services(v3), "ltc", LTC_P2PKH)
    )
    assert report.skipped == "no cluster tables in v3"
    assert report.agrees is False


def test_both_sides_raising_the_same_exception_agree() -> None:
    """A missing address is an answer, and both backends giving it is parity."""

    async def missing(n, v):
        raise KeyError("AddressNotFound")

    report = run(
        backtest.run_call(
            call(), services(missing), services(missing), "ltc", LTC_P2PKH
        )
    )
    assert report.agrees
    assert report.skipped is None


def test_one_side_raising_is_a_difference() -> None:
    """v2 raising AddressNotFound while v3 returns an empty address is the
    exact bug this harness exists to catch -- it must not pass."""

    async def found(n, v):
        return {"balance": 0}

    async def missing(n, v):
        raise KeyError("AddressNotFound")

    report = run(
        backtest.run_call(call(), services(missing), services(found), "ltc", LTC_P2PKH)
    )
    assert not report.agrees
    assert "raised" in str(report.differences[0])


def test_the_v2_side_is_asked_with_v2s_spelling_of_the_address() -> None:
    """Same hash160, different version byte. Passing v3's spelling to v2 looks
    up an address that does not exist there and reports a difference that is an
    artifact of the fixture."""
    asked = {}

    def spy(side):
        async def fn(n, v):
            asked[side] = v
            return {"balance": 1}

        return fn

    run(
        backtest.run_call(
            call(), services(spy("v2")), services(spy("v3")), "ltc", BTC_VERSIONED
        )
    )
    assert asked["v2"] == LTC_P2PKH  # re-versioned
    assert asked["v3"] == BTC_VERSIONED  # left as the lake wrote it


def test_a_non_address_fixture_is_never_reversioned() -> None:
    """A block height or a tx hash passed through `reversion_address` would be
    a silent corruption; only ADDRESS calls are rewritten."""
    asked = {}

    async def fn(n, v):
        asked["v"] = v
        return {}

    block_call = backtest.Call(
        "get_block", backtest.BLOCK, lambda s, n, v: s.blocks_service.get_block(n, v)
    )
    run(backtest.run_call(block_call, services(fn), services(fn), "ltc", 3171361))
    assert asked["v"] == 3171361


def test_models_are_dumped_before_comparison() -> None:
    """Two pydantic models compare by identity, so without this every single
    call would report a difference and the report would be worthless."""

    class Model:
        def __init__(self, balance):
            self.balance = balance

        def model_dump(self):
            return {"balance": self.balance}

    async def v2(n, v):
        return Model(5)

    async def v3(n, v):
        return Model(5)

    report = run(
        backtest.run_call(call(), services(v2), services(v3), "ltc", LTC_P2PKH)
    )
    assert report.agrees

    async def different(n, v):
        return Model(6)

    report = run(
        backtest.run_call(call(), services(v2), services(different), "ltc", LTC_P2PKH)
    )
    assert not report.agrees


def test_to_plain_walks_lists_and_dicts_of_models() -> None:
    class Model:
        def model_dump(self):
            return {"a": 1}

    assert backtest.to_plain([Model(), Model()]) == [{"a": 1}, {"a": 1}]
    assert backtest.to_plain({"k": [Model()]}) == {"k": [{"a": 1}]}


def test_every_call_runs_against_every_fixture_it_applies_to() -> None:
    seen = []

    async def fn(n, v):
        seen.append(v)
        return {}

    fixtures = backtest.Fixtures(
        network="ltc", addresses=[LTC_P2PKH, BTC_VERSIONED], blocks=[7]
    )
    calls = [
        call(),
        backtest.Call(
            "get_block",
            backtest.BLOCK,
            lambda s, n, v: s.blocks_service.get_block(n, v),
        ),
    ]
    reports = run(backtest.run(services(fn), services(fn), fixtures, calls))
    assert len(reports) == 3  # two addresses, one block
    assert {r.label for r in reports} == {
        f"get_address({LTC_P2PKH})",
        f"get_address({BTC_VERSIONED})",
        "get_block(7)",
    }


def test_a_call_needing_nothing_runs_exactly_once() -> None:
    async def fn(n):
        return {"no_blocks": 1}

    stats = backtest.Call(
        "stats",
        backtest.NOTHING,
        lambda s, n, v: s.stats_service.get_currency_statistics(n),
    )
    fixtures = backtest.Fixtures(network="ltc", addresses=[LTC_P2PKH])
    reports = run(backtest.run(services(fn), services(fn), fixtures, [stats]))
    assert len(reports) == 1
    assert reports[0].label == "stats"


def test_fixtures_from_v3_decode_to_strings_and_deduplicate(monkeypatch) -> None:
    """The probe finds addresses as packed BYTES; the services take strings.
    Passing bytes through would look up nothing on either side and report
    agreement on two empty answers."""
    from graphsense_v3 import probe as prober
    from graphsense_v3.codec import encode_address

    encoded = encode_address("ltc", LTC_P2PKH)
    found = SimpleNamespace(
        address=encoded,
        busiest_address=encoded,  # the same address, found twice
        tx_hash=b"\xab\xcd",
        block_id=3171361,
    )
    monkeypatch.setattr(prober, "configuration", lambda *a, **k: {})
    monkeypatch.setattr(
        prober, "Prober", lambda *a, **k: SimpleNamespace(fixtures=lambda: found)
    )

    fixtures = backtest.fixtures_from_v3(
        None, "ltc_raw_v3_x", "ltc_derived_v3_x", "ltc"
    )
    assert fixtures.addresses == [LTC_P2PKH]  # decoded, and not repeated
    assert fixtures.tx_hashes == ["abcd"]
    assert fixtures.blocks == [3171361]
