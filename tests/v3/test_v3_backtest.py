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
        list_address_links=fn,
        get_block_by_date=fn,
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
        link_src=encoded,
        link_dst=encoded,
    )
    monkeypatch.setattr(prober, "configuration", lambda *a, **k: {})
    monkeypatch.setattr(
        prober, "Prober", lambda *a, **k: SimpleNamespace(fixtures=lambda: found)
    )

    fixtures = backtest.fixtures_from_v3(
        RatedSession(3171361), "ltc_raw_v3_x", "ltc_derived_v3_x", "ltc"
    )
    assert fixtures.addresses == [LTC_P2PKH]  # decoded, and not repeated
    assert fixtures.tx_hashes == ["abcd"]
    assert fixtures.blocks == [3171361]


def test_the_v3_session_uses_the_configs_port() -> None:
    """`database.port` reaches the v2 DAL as a field but the v3 session only
    through the contact string. Unreconciled, the two sides would talk to
    different endpoints and the run would compare two clusters."""
    assert backtest.with_port(["10.0.0.1", "10.0.0.2"], 9043) == [
        "10.0.0.1:9043",
        "10.0.0.2:9043",
    ]
    # a node naming its own port keeps it, and no port changes nothing
    assert backtest.with_port(["10.0.0.1:9999"], 9043) == ["10.0.0.1:9999"]
    assert backtest.with_port(["10.0.0.1"], None) == ["10.0.0.1"]


class RatedSession:
    """A sync driver session answering exchange_rates for a fixed rated tip."""

    def __init__(self, tip):
        self.tip = tip
        self.asked = []

    def execute(self, cql, params):
        asset, group, upper = params
        self.asked.append((group, upper))
        # Partition-aware: a partition answers only for blocks it holds, which
        # is what makes the walk-down actually walk.
        in_group = self.tip // 100 == group
        if in_group and self.tip <= upper:
            return [SimpleNamespace(block_id=self.tip)]
        return []


def test_a_rated_block_is_returned_unchanged() -> None:
    session = RatedSession(3170999)
    assert (
        backtest.rated_block(session, "ltc_derived_v3_x", "ltc", 3170999, 100)
        == 3170999
    )


def test_an_unrated_block_walks_down_to_the_last_rated_one() -> None:
    """A one-day rate lag turned into four "differences" in the first run: the
    fixture sat in a block with no rate, so every call touching it failed on
    the v3 side for a reason unrelated to the row under test."""
    session = RatedSession(3170999)
    found = backtest.rated_block(session, "ltc_derived_v3_x", "ltc", 3171361, 100)
    assert found == 3170999
    # It asks the block's own partition first, then walks down.
    assert session.asked[0] == (31713, 3171361)
    assert session.asked[1][0] == 31712


def test_the_walk_down_asks_each_partition_for_its_own_top_block() -> None:
    """Carrying the original height down would ask partition 31712 for a block
    <= 3171361, which every row in it satisfies -- masking the bug where the
    partition bound and the block bound disagree."""
    session = RatedSession(3170999)
    backtest.rated_block(session, "ltc_derived_v3_x", "ltc", 3171361, 100)
    group, upper = session.asked[1]
    assert (group, upper) == (31712, 3171299)


def test_giving_up_returns_none_rather_than_an_unrated_block() -> None:
    """None says "no comparable fixture here"; the original block would look
    like a fixture and fail every call made against it."""
    session = RatedSession(tip=10**18)  # never satisfies the bound
    assert backtest.rated_block(session, "ltc_derived_v3_x", "ltc", 500, 100) is None


class BucketSession:
    """Answers a (bucket, floor) point read from a canned set of addresses."""

    def __init__(self, addresses, *, empty_below=None):
        self.addresses = addresses
        self.empty_below = empty_below
        self.asked = []

    def execute(self, cql, params):
        assert "address_bucket = " in cql, "sampling must address a partition"
        bucket, floor = tuple(params)
        self.asked.append((bucket, floor))
        if self.empty_below is not None and floor and floor >= self.empty_below:
            return []  # the floor landed past the end of this bucket
        index = len(self.asked) % len(self.addresses)
        return [SimpleNamespace(address=self.addresses[index])]


def test_sampling_spreads_over_buckets_and_within_them() -> None:
    """`address_stats` is PRIMARY KEY (address_bucket, address, epoch), so the
    partition key is the BUCKET alone. Taking each partition's first row would
    always return its lowest-sorting address -- and since the encoded form
    starts with a type marker, that is systematically the same address TYPE."""
    from graphsense_v3.codec import encode_address

    session = BucketSession(
        [encode_address("ltc", LTC_P2PKH), encode_address("ltc", BTC_VERSIONED)]
    )
    found = backtest.sample_addresses(
        session, "ltc_derived_v3_x", "ltc", 20, buckets=100_000
    )
    assert set(found) == {LTC_P2PKH, BTC_VERSIONED}
    assert len({b for b, _ in session.asked}) > 1, "buckets must vary"
    assert len({f for _, f in session.asked}) > 1, "floors must vary"


def test_a_floor_past_the_end_of_a_bucket_retries_from_its_start() -> None:
    """Otherwise the draw is silently lost and a `--sample 50` quietly becomes
    a sample of 30."""
    from graphsense_v3.codec import encode_address

    session = BucketSession([encode_address("ltc", LTC_P2PKH)], empty_below=bytes([0]))
    found = backtest.sample_addresses(session, "ltc_derived_v3_x", "ltc", 3, buckets=10)
    assert found == [LTC_P2PKH]
    assert any(floor == b"" for _bucket, floor in session.asked)


def test_sampling_returns_strings_without_duplicates() -> None:
    """The DAL keys on bytes, the services take strings; and a duplicate draw
    would compare the same address twice and inflate the agreement count."""
    from graphsense_v3.codec import encode_address

    session = BucketSession([encode_address("ltc", LTC_P2PKH)])
    found = backtest.sample_addresses(session, "ltc_derived_v3_x", "ltc", 5, buckets=10)
    assert found == [LTC_P2PKH]


def test_every_sampled_bucket_is_within_range() -> None:
    """A bucket outside [0, entity_buckets) addresses a partition that cannot
    exist, and the draw comes back empty every time."""
    from graphsense_v3.codec import encode_address

    session = BucketSession([encode_address("ltc", LTC_P2PKH)])
    backtest.sample_addresses(session, "ltc_derived_v3_x", "ltc", 50, buckets=8)
    assert all(0 <= bucket < 8 for bucket, _floor in session.asked)


class TipSession:
    """summary_statistics plus a rated tip, for the coverage preflight."""

    def __init__(self, block_tip, rated_tip):
        self.block_tip = block_tip
        self.rated_tip = rated_tip

    def execute(self, cql, params=()):
        if "summary_statistics" in cql:
            return [SimpleNamespace(highest_block=self.block_tip)]
        _asset, group, upper = tuple(params)
        if self.rated_tip is None:
            return []
        if self.rated_tip // 100 == group and self.rated_tip <= upper:
            return [SimpleNamespace(block_id=self.rated_tip)]
        return []


def test_a_rated_tip_produces_no_warning() -> None:
    session = TipSession(block_tip=3171361, rated_tip=3171361)
    assert (
        backtest.rate_coverage_warning(
            session, "ltc_raw_v3_x", "ltc_derived_v3_x", "ltc", 100
        )
        is None
    )


def test_an_unrated_tip_is_reported_once_with_its_cause() -> None:
    """One missing rate row becomes an identical BlockNotFoundException on
    get_address and both neighbour listings -- five copies of one fact, which
    is the noise a real finding hides in."""
    session = TipSession(block_tip=3171361, rated_tip=3171011)
    warning = backtest.rate_coverage_warning(
        session, "ltc_raw_v3_x", "ltc_derived_v3_x", "ltc", 100
    )
    assert warning is not None
    assert "3171361" in warning and "3171011" in warning
    # It must say this is a keyspace property, not a backend difference.
    assert "not a difference between the backends" in warning


def test_no_statistics_row_is_not_reported_as_a_rate_problem() -> None:
    session = TipSession(block_tip=None, rated_tip=None)
    assert (
        backtest.rate_coverage_warning(
            session, "ltc_raw_v3_x", "ltc_derived_v3_x", "ltc", 100
        )
        is None
    )


def test_both_sides_are_timed_when_both_complete() -> None:
    async def fn(n, v):
        return {"a": 1}

    report = run(
        backtest.run_call(call(), services(fn), services(fn), "ltc", LTC_P2PKH)
    )
    assert report.left_ms is not None and report.right_ms is not None


def test_a_failed_call_is_not_timed() -> None:
    """Timing a raised call measures how fast something FAILED, which would
    flatter whichever side broke earlier."""

    async def ok(n, v):
        return {"a": 1}

    async def boom(n, v):
        raise KeyError("nope")

    report = run(
        backtest.run_call(call(), services(ok), services(boom), "ltc", LTC_P2PKH)
    )
    assert report.left_ms is None and report.right_ms is None


def test_both_ends_of_a_link_get_v2s_spelling() -> None:
    """Re-versioning only the source would look a real address up against a
    neighbour that does not exist on that side, and report the empty result as
    a difference."""
    asked = {}

    def spy(side):
        async def fn(n, src, dst, **kwargs):
            asked[side] = (src, dst)
            return {}

        return fn

    link_call = backtest.Call(
        "list_address_links",
        backtest.LINK,
        lambda s, n, v: s.addresses_service.list_address_links(n, v[0], v[1]),
    )
    v2 = services(spy("v2"))
    v3 = services(spy("v3"))
    run(backtest.run_call(link_call, v2, v3, "ltc", (BTC_VERSIONED, BTC_VERSIONED)))
    assert asked["v2"] == (LTC_P2PKH, LTC_P2PKH)
    assert asked["v3"] == (BTC_VERSIONED, BTC_VERSIONED)


def test_the_second_page_call_uses_each_backends_own_cursor() -> None:
    """Feeding v2's token to v3 would test nothing -- the formats are
    unrelated. Each side pages itself and the CONTENT is compared."""
    seen = []

    class Listing:
        def __init__(self, next_page, mark):
            self.next_page = next_page
            self.mark = mark

        def model_dump(self):
            return {"mark": self.mark}

    async def fn(n, v, pagesize=None, page=None):
        seen.append(page)
        return Listing("TOKEN" if page is None else None, "page2" if page else "page1")

    result = run(backtest._second_page(services(fn), "ltc", LTC_P2PKH))
    assert seen == [None, "TOKEN"]
    assert result.mark == "page2"


def test_an_address_with_one_page_compares_that_page() -> None:
    """No second page must not mean "skipped": comparing page one again is a
    real comparison, and both sides agreeing there IS the answer."""

    class Listing:
        next_page = None

        def model_dump(self):
            return {"mark": "only"}

    async def fn(n, v, pagesize=None, page=None):
        return Listing()

    result = run(backtest._second_page(services(fn), "ltc", LTC_P2PKH))
    assert result.model_dump() == {"mark": "only"}


class ChainSession:
    """Blocks, their transactions, and link edges, for the fixture samplers."""

    def __init__(self, blocks, txs=None, links=None):
        self.blocks = blocks  # {height: timestamp}
        self.txs = txs or {}  # {height: tx_hash bytes}
        self.links = links or []
        self.tokens = []

    def execute(self, cql, params):
        if ".block " in cql:
            _group, height = tuple(params)
            if height not in self.blocks:
                return []
            return [SimpleNamespace(block_id=height, timestamp=self.blocks[height])]
        if ".transaction " in cql:
            _group, low, _high = tuple(params)
            height = low >> 32
            if height not in self.txs:
                return []
            return [SimpleNamespace(tx_hash=self.txs[height])]
        assert "token(src_address, dst_bucket)" in cql, cql
        (token,) = tuple(params)
        self.tokens.append(token)
        index = len(self.tokens) % len(self.links)
        src, dst = self.links[index]
        return [SimpleNamespace(src_address=src, dst_address=dst)]


def test_blocks_are_sampled_across_the_whole_chain() -> None:
    # Left un-pinned deliberately: this one asserts SPREAD, which is the very
    # thing a pinned sequence would fake. Every height exists, so it cannot
    # come up short.
    """A block near the tip and one from 2009 exercise different partitions and
    different rate coverage -- zero-filled head against real rates."""
    session = ChainSession(blocks={h: 1000 + h for h in range(0, 1000)})
    found = backtest.sample_blocks(session, "bch_raw_v3_t", 999, 100, 10)
    assert len(found) == 10
    assert len({h for h, _ in found}) == 10  # no repeats
    assert max(h for h, _ in found) - min(h for h, _ in found) > 100


def _draws(monkeypatch, heights):
    """Pin the sampler's random draws, so a test asserts the LOGIC rather than
    hoping a random height lands on the one block the fake holds."""
    import random

    sequence = iter(heights)
    monkeypatch.setattr(random, "randrange", lambda *_: next(sequence, heights[-1]))


def test_a_sampled_block_carries_its_own_timestamp(monkeypatch) -> None:
    """The date fixtures come from these, so date -> block has a knowable
    answer rather than a guess at what the chain was doing."""
    _draws(monkeypatch, [5])
    session = ChainSession(blocks={5: 1_600_000_000})
    assert backtest.sample_blocks(session, "bch_raw_v3_t", 5, 100, 1) == [
        (5, 1_600_000_000)
    ]


def test_a_height_the_chain_does_not_hold_is_skipped(monkeypatch) -> None:
    """Random draws overshoot; a missing block must not become a fixture that
    fails on both sides for a reason unrelated to the backends."""
    _draws(monkeypatch, [3, 7, 11])
    session = ChainSession(blocks={7: 1})
    found = backtest.sample_blocks(session, "bch_raw_v3_t", 20, 100, 3)
    assert found == [(7, 1)]


def test_transaction_fixtures_come_from_the_sampled_blocks() -> None:
    """So a tx fixture inherits the blocks' spread instead of being one hash
    from one arbitrary block."""
    session = ChainSession(blocks={}, txs={3: b"\xaa\xbb", 9: b"\xcc\xdd"})
    assert backtest.sample_txs(session, "bch_raw_v3_t", [3, 9], 1) == ["aabb", "ccdd"]


def test_links_are_sampled_by_token_over_the_whole_partition_key() -> None:
    """(src_address, dst_bucket) IS the whole partition key here, so unlike
    address_stats a token draw is valid -- and /links was the weakest link, so
    one pair was never a measurement."""
    from graphsense_v3.codec import encode_address

    a = encode_address("ltc", LTC_P2PKH)
    b = encode_address("ltc", BTC_VERSIONED)
    session = ChainSession(blocks={}, links=[(a, b), (b, a)])
    found = backtest.sample_links(session, "ltc_derived_v3_x", "ltc", 5)
    assert set(found) == {(LTC_P2PKH, BTC_VERSIONED), (BTC_VERSIONED, LTC_P2PKH)}
    assert len(set(session.tokens)) > 1


def test_a_date_fixture_is_its_own_kind() -> None:
    """Reusing BLOCK for date -> block passed a height where a datetime was
    wanted, and the call quietly compared a fixed hard-coded day instead."""
    from datetime import datetime, timezone

    when = datetime(2020, 1, 1, tzinfo=timezone.utc)
    fixtures = backtest.Fixtures(network="bch", blocks=[7], dates=[when])
    assert fixtures.values_for(backtest.DATE) == [when]
    assert fixtures.values_for(backtest.BLOCK) == [7]
