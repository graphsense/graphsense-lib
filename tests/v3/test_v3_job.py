"""The end-to-end driver.

It exists to be run once against a cluster, so the things worth testing are the
ones that would waste that run: refusing early, and the dry run actually
checking every frame.
"""

import logging

import pytest

from graphsense_v3.settings import RunSettings
from graphsense_v3.spark import job

pytest.importorskip("pyspark")


def _settings(network: str) -> RunSettings:
    return RunSettings(
        network=network,
        env="prod",
        lake_root="/nonexistent",
        raw_keyspace=f"{network}_raw_v3",
        derived_keyspace=f"{network}_derived_v3",
        rates_keyspace=f"{network}_raw_20260101",
        tokens_keyspace=f"{network}_transformed_20260101",
        cassandra_nodes=["node"],
    )


@pytest.mark.parametrize("network", ["btc", "eth", "trx"])
def test_every_network_can_run_both_stages(spark, network: str) -> None:
    """Both families transform now, so nothing is refused on family grounds --
    a run fails on the missing lake, not on the stage selection."""
    with pytest.raises(Exception) as caught:
        job.run(spark, _settings(network), dry_run=True)
    assert "UTXO-only" not in str(caught.value)


@pytest.mark.parametrize("network", ["eth", "trx"])
def test_account_run_refuses_without_a_tokens_keyspace(spark, network: str) -> None:
    """`token_configuration` is in the v2 TRANSFORMED keyspace, never the raw
    one, so an account run that has only the rates keyspace cannot read it --
    and must say so at submit time rather than after writing the raw stage."""
    from dataclasses import replace

    with pytest.raises(SystemExit) as caught:
        job.run(spark, replace(_settings(network), tokens_keyspace=""), dry_run=True)
    assert "token_configuration" in str(caught.value)


def test_utxo_run_needs_no_tokens_keyspace(spark) -> None:
    """The same omission is harmless for UTXO, which never reads the table."""
    from dataclasses import replace

    with pytest.raises(Exception) as caught:
        job.run(spark, replace(_settings("btc"), tokens_keyspace=""), dry_run=True)
    assert "token_configuration" not in str(caught.value)


V2_RAW = "date STRING, fiat_values MAP<STRING,DOUBLE>"
V2_TOKEN = "asset STRING, date STRING, fiat_values MAP<STRING,DOUBLE>"
V3_RAW = "asset STRING, date STRING, fiat_values MAP<STRING,DOUBLE>"
V3_DERIVED = (
    "asset STRING, block_id_group INT, block_id INT, fiat_values MAP<STRING,DOUBLE>"
)


@pytest.fixture
def blocks(spark):
    return spark.createDataFrame(
        [{"block_id": 1, "timestamp": 0}, {"block_id": 2, "timestamp": 86_400}],
        schema="block_id INT, timestamp BIGINT",
    )


def test_a_v2_rate_table_gains_an_asset_column(spark) -> None:
    """v2's exchange_rates is keyed by date alone and holds the native coin
    only -- no asset column at all. v3 merged the two rate tables, so the asset
    has to be synthesised before anything can join on it."""
    native = spark.createDataFrame(
        [{"date": "1970-01-01", "fiat_values": {"EUR": 1.5}}], schema=V2_RAW
    )
    rows = job.normalise_rates(native, symbol="LTC").collect()
    assert [(r["asset"], r["date"]) for r in rows] == [("LTC", "1970-01-01")]


def test_a_v3_rate_table_keeps_its_own_asset(spark) -> None:
    """A v3 source already carries every asset, native included, so nothing is
    synthesised and no token table is unioned."""
    rates = spark.createDataFrame(
        [
            {"asset": "LTC", "date": "1970-01-01", "fiat_values": {"EUR": 1.5}},
            {"asset": "USDT", "date": "1970-01-01", "fiat_values": {"EUR": 0.9}},
        ],
        schema=V3_RAW,
    )
    rows = job.normalise_rates(rates, symbol="IGNORED").collect()
    assert {r["asset"] for r in rows} == {"LTC", "USDT"}


def test_a_per_block_source_needs_no_date_join(spark, blocks) -> None:
    """A v3 derived keyspace has already resolved dates to blocks."""
    rates = spark.createDataFrame(
        [
            {
                "asset": "LTC",
                "block_id_group": 0,
                "block_id": 2,
                "fiat_values": {"EUR": 9.0},
            }
        ],
        schema=V3_DERIVED,
    )
    normalised = job.normalise_rates(rates, symbol="LTC")
    assert "block_id" in normalised.columns and "date" not in normalised.columns
    rows = job.rates_by_block(blocks, normalised).collect()
    assert [(r["asset"], r["block_id"]) for r in rows] == [("LTC", 2)]


def test_a_positional_fiat_list_is_refused(spark) -> None:
    """v2's *transformed* tables store fiat_values as a list whose meaning
    depends on an ordering held elsewhere. Reading it against the wrong
    ordering is the defect v3's map replaced, so refuse rather than guess."""
    rates = spark.createDataFrame(
        [{"block_id": 1, "fiat_values": [1.5, 2.0]}],
        schema="block_id INT, fiat_values ARRAY<DOUBLE>",
    )
    with pytest.raises(SystemExit, match="positional list"):
        job.normalise_rates(rates, symbol="LTC")


def test_rates_join_by_date(spark, blocks) -> None:
    """A block whose date has no rate gets no row at all, so the transform
    contributes no fiat for it -- "unknown", not "worthless".

    The derived stage then refuses to COVER such a block at all; see
    `bound_to_rated_blocks`. An unrated block is not servable, so leaving it in
    would trade a missing fiat value for a failed request."""
    rates = spark.createDataFrame(
        [{"asset": "LTC", "date": "1970-01-01", "fiat_values": {"EUR": 1.5}}],
        schema=V3_RAW,
    )
    rows = {
        (r["asset"], r["block_id"]): r["fiat_values"]
        for r in job.rates_by_block(
            blocks, job.normalise_rates(rates, symbol="LTC")
        ).collect()
    }
    assert rows == {("LTC", 1): {"EUR": 1.5}}


def test_a_v2_account_source_unions_its_token_table(spark) -> None:
    """token_exchange_rates exists on v2 account keyspaces and nowhere else."""
    native = job.normalise_rates(
        spark.createDataFrame(
            [{"date": "1970-01-01", "fiat_values": {"EUR": 1.5}}], schema=V2_RAW
        ),
        symbol="ETH",
    )
    tokens = job.normalise_rates(
        spark.createDataFrame(
            [{"asset": "USDT", "date": "1970-01-01", "fiat_values": {"EUR": 0.9}}],
            schema=V2_TOKEN,
        ),
        symbol="ETH",
    )
    rows = native.unionByName(tokens).collect()
    assert {r["asset"] for r in rows} == {"ETH", "USDT"}


def test_stage_reports_its_cost(caplog) -> None:
    """The whole output of a benchmark run, so it had better say something."""
    with caplog.at_level(logging.INFO):
        with job.Stage("a thing"):
            pass
    assert "START a thing" in caplog.text
    assert "done  a thing" in caplog.text


def test_stage_reports_a_failure(caplog) -> None:
    with caplog.at_level(logging.INFO), pytest.raises(ValueError):
        with job.Stage("a thing"):
            raise ValueError("nope")
    assert "FAILED" in caplog.text


def test_a_dry_run_executes_rather_than_only_typechecks(spark, caplog) -> None:
    """Conformance checks resolve schemas and run nothing, so a pandas UDF that
    fails on the executors would pass a dry run and then fail hours into the
    real one. `sample` materialises a few rows through the whole DAG."""
    import logging

    frame = spark.createDataFrame(
        [{"a": i} for i in range(50)], schema="a INT"
    ).withColumn("b", frame_doubler())
    with caplog.at_level(logging.INFO):
        job.sample({"thing": frame}, "raw", rows=3)
    assert "thing produced 3 of the first 3 rows" in caplog.text


def frame_doubler():
    """A column that only evaluates when the plan actually runs."""
    from pyspark.sql import functions as F

    return F.col("a") * 2


RATES_SCHEMA = "asset STRING, block_id INT, fiat_values MAP<STRING, DOUBLE>"
BLOCKS_SCHEMA = "block_id INT, timestamp LONG"
DAY = 86400


def test_rated_tip_is_the_highest_block_with_a_rate(spark) -> None:
    rates = spark.createDataFrame(
        [
            {"asset": "LTC", "block_id": 5, "fiat_values": {"EUR": 1.0}},
            {"asset": "LTC", "block_id": 9, "fiat_values": {"EUR": 1.0}},
        ],
        schema=RATES_SCHEMA,
    )
    assert job.rated_tip(rates) == 9


def test_rated_tip_is_none_when_nothing_is_rated(spark) -> None:
    """Distinguishable from 0, which is a real block. A keyspace with no rate
    anywhere is unservable and the run refuses rather than writing it."""
    assert job.rated_tip(spark.createDataFrame([], schema=RATES_SCHEMA)) is None


def test_recent_unrated_blocks_get_the_last_known_rate(spark) -> None:
    """Rates land a day at a time, so the chain tip is ALWAYS unrated for up to
    ~24h. Refusing those blocks pins the backend a day behind the chain."""
    rates = spark.createDataFrame(
        [{"asset": "LTC", "block_id": 5, "fiat_values": {"EUR": 1.5}}],
        schema=RATES_SCHEMA,
    )
    blocks = spark.createDataFrame(
        [
            {"block_id": 5, "timestamp": 1_000_000},
            {"block_id": 6, "timestamp": 1_000_000 + DAY},
        ],
        schema=BLOCKS_SCHEMA,
    )
    filled = {
        r["block_id"]: r["fiat_values"]
        for r in job.forward_fill_rates(rates, blocks, within_seconds=2 * DAY).collect()
    }
    assert filled == {5: {"EUR": 1.5}, 6: {"EUR": 1.5}}


def test_a_block_beyond_the_window_is_not_filled(spark) -> None:
    """The cap is the point: past it the rate feed is broken rather than
    lagging, and a stale rate carried forever would hide that behind plausible
    numbers."""
    rates = spark.createDataFrame(
        [{"asset": "LTC", "block_id": 5, "fiat_values": {"EUR": 1.5}}],
        schema=RATES_SCHEMA,
    )
    blocks = spark.createDataFrame(
        [
            {"block_id": 5, "timestamp": 1_000_000},
            {"block_id": 9, "timestamp": 1_000_000 + 10 * DAY},
        ],
        schema=BLOCKS_SCHEMA,
    )
    filled = job.forward_fill_rates(rates, blocks, within_seconds=2 * DAY)
    assert [r["block_id"] for r in filled.collect()] == [5]


def test_the_fill_never_moves_a_rate_backwards(spark) -> None:
    """Only blocks AFTER the last rate are filled. An earlier block with no
    rate is a hole in the feed, and filling it forward would be filling it
    backward -- a different number than the day actually had."""
    rates = spark.createDataFrame(
        [{"asset": "LTC", "block_id": 5, "fiat_values": {"EUR": 1.5}}],
        schema=RATES_SCHEMA,
    )
    blocks = spark.createDataFrame(
        [
            {"block_id": 3, "timestamp": 1_000_000 - DAY},
            {"block_id": 5, "timestamp": 1_000_000},
        ],
        schema=BLOCKS_SCHEMA,
    )
    filled = job.forward_fill_rates(rates, blocks, within_seconds=2 * DAY)
    assert [r["block_id"] for r in filled.collect()] == [5]


def test_each_asset_carries_its_own_last_rate(spark) -> None:
    """A token whose feed stopped earlier must not borrow the native coin's
    freshness -- that would invent a rate for a token nobody priced."""
    rates = spark.createDataFrame(
        [
            {"asset": "ETH", "block_id": 5, "fiat_values": {"EUR": 1.5}},
            {"asset": "USDT", "block_id": 3, "fiat_values": {"EUR": 0.9}},
        ],
        schema=RATES_SCHEMA,
    )
    blocks = spark.createDataFrame(
        [
            {"block_id": 3, "timestamp": 1_000_000},
            {"block_id": 5, "timestamp": 1_000_000 + 60},
            {"block_id": 6, "timestamp": 1_000_000 + 120},
        ],
        schema=BLOCKS_SCHEMA,
    )
    filled = {
        (r["asset"], r["block_id"]): r["fiat_values"]
        for r in job.forward_fill_rates(rates, blocks, within_seconds=2 * DAY).collect()
    }
    # ETH extends from 5; USDT extends from its OWN last rate at 3.
    assert filled[("ETH", 6)] == {"EUR": 1.5}
    assert filled[("USDT", 5)] == {"EUR": 0.9}
    assert filled[("USDT", 6)] == {"EUR": 0.9}


def test_a_block_before_the_feed_began_gets_zeros_like_v2(spark) -> None:
    """v2 materialises a row for EVERY block: `ltc_transformed_20260727` holds
    block 1000 as [0, 0], because the rate source starts 2015-01-01 and LTC
    genesis is 2011. "No rate" is not a state v2 ever serves, and an absent row
    is a FAILED REQUEST in v3, not a missing number."""
    rates = spark.createDataFrame(
        [{"asset": "LTC", "block_id": 5, "fiat_values": {"EUR": 1.5, "USD": 2.0}}],
        schema=RATES_SCHEMA,
    )
    blocks = spark.createDataFrame(
        [{"block_id": 1}, {"block_id": 5}], schema="block_id INT"
    )
    filled = {
        r["block_id"]: r["fiat_values"]
        for r in job.zero_fill_rates(rates, blocks).collect()
    }
    assert filled[5] == {"EUR": 1.5, "USD": 2.0}
    assert filled[1] == {"EUR": 0.0, "USD": 0.0}


def test_the_zero_row_carries_every_currency_the_feed_uses(spark) -> None:
    """A zero map missing a currency is not the same answer as a zero for it --
    the fiat list is positional in v2's response."""
    rates = spark.createDataFrame(
        [{"asset": "LTC", "block_id": 5, "fiat_values": {"EUR": 1.5, "USD": 2.0}}],
        schema=RATES_SCHEMA,
    )
    blocks = spark.createDataFrame([{"block_id": 1}], schema="block_id INT")
    row = job.zero_fill_rates(rates, blocks).collect()[0]
    assert sorted(row["fiat_values"]) == ["EUR", "USD"]


def test_every_asset_gets_a_row_for_every_block(spark) -> None:
    """A chain with tokens needs a row per (block, asset); one null-asset row
    per block would leave every token unservable."""
    rates = spark.createDataFrame(
        [
            {"asset": "ETH", "block_id": 5, "fiat_values": {"EUR": 1.5}},
            {"asset": "USDT", "block_id": 5, "fiat_values": {"EUR": 0.9}},
        ],
        schema=RATES_SCHEMA,
    )
    blocks = spark.createDataFrame(
        [{"block_id": 1}, {"block_id": 5}], schema="block_id INT"
    )
    found = {
        (r["asset"], r["block_id"])
        for r in job.zero_fill_rates(rates, blocks).collect()
    }
    assert found == {("ETH", 1), ("ETH", 5), ("USDT", 1), ("USDT", 5)}


def test_no_block_is_left_without_a_rate(spark) -> None:
    """The property the whole thing exists for: after filling, every block the
    keyspace holds has a rate row, so no request can fail for want of one."""
    rates = spark.createDataFrame(
        [{"asset": "LTC", "block_id": 7, "fiat_values": {"EUR": 1.5}}],
        schema=RATES_SCHEMA,
    )
    blocks = spark.createDataFrame(
        [{"block_id": b} for b in range(1, 11)], schema="block_id INT"
    )
    filled = job.zero_fill_rates(rates, blocks)
    assert {r["block_id"] for r in filled.collect()} == set(range(1, 11))


def test_preflight_problems_stop_a_run_and_name_their_own_override() -> None:
    """The message used to say "fix or override" while no override existed --
    an instruction the reader could not follow. The default stays refusal: a
    problem found and then silently ignored is worse than one never sought."""
    import inspect

    source = inspect.getsource(job.run)
    assert "accept_preflight" in inspect.signature(job.run).parameters
    # Refuses unless the flag is given, and the refusal names it.
    assert "if problems and not accept_preflight" in source
    assert "--accept-preflight" in source
    # And says so loudly when overridden, rather than proceeding quietly.
    assert "PROCEEDING PAST" in source


# --------------------------------------------------------------------------- #
# A snapshot is consistent, but not necessarily one height                     #
# --------------------------------------------------------------------------- #

BOUND = ("block", "transaction")


def test_the_run_stops_where_the_shortest_dense_table_stops() -> None:
    """The BCH tear, exactly: `block` reached 967027 and `transaction` stopped
    at 967021, so six blocks were written with headers and no transactions and
    `list_block_txs(967027)` answered 118 on v2 and 0 on v3."""
    maxima = {"block": 967027, "transaction": 967021}
    end_block, note = job.bounded_end_block(maxima, BOUND, None)
    assert end_block == 967021
    assert note and "967021" in note and "6 block(s)" in note


def test_a_snapshot_that_lines_up_is_not_bounded() -> None:
    """The normal case must not be cut, and must not log a warning that would
    train the reader to ignore this one."""
    maxima = {"block": 900, "transaction": 900}
    # The request comes back UNCHANGED. A note on a healthy run would train the
    # reader to skip past the one that matters.
    assert job.bounded_end_block(maxima, BOUND, None) == (None, None)


def test_an_explicit_end_block_below_the_bound_is_left_alone() -> None:
    """The caller asked for less than the snapshot can serve. That is not a
    problem to report."""
    maxima = {"block": 967027, "transaction": 967021}
    assert job.bounded_end_block(maxima, BOUND, 500_000) == (500_000, None)


def test_an_explicit_end_block_above_the_bound_is_cut() -> None:
    maxima = {"block": 967027, "transaction": 967021}
    end_block, note = job.bounded_end_block(maxima, BOUND, 967_027)
    assert end_block == 967021
    assert note


def test_a_sparse_table_never_cuts_the_run() -> None:
    """`log`, `trace` and `fee` are legitimately short at the tip -- a block
    with no logs contributes no rows -- so bounding on them would truncate
    every run by however long the chain last went without a log."""
    maxima = {"block": 900, "transaction": 900, "log": 300, "trace": 880}
    assert job.bounded_end_block(maxima, BOUND, None) == (None, None)


def test_the_note_names_every_table_that_is_short() -> None:
    """Two tables tied at the bound means both stopped, and saying only one
    sends the reader to the wrong ingest stage."""
    maxima = {"block": 1000, "transaction": 900, "trace": 900}
    _, note = job.bounded_end_block(maxima, ("block", "transaction", "trace"), None)
    assert "trace, transaction" in note


def test_nothing_to_bound_on_is_not_an_error() -> None:
    """A loader whose bound tables are all absent from the maxima -- none of
    them carry block_id -- leaves the request untouched rather than failing."""
    assert job.bounded_end_block({"trc10": 5}, BOUND, 42) == (42, None)


def test_the_loaders_bound_on_tables_they_actually_read() -> None:
    """A bound table missing from LAKE_TABLES is never pinned, so it would be
    silently skipped and the run would go back to being unbounded."""
    from graphsense_v3.spark import raw_account, raw_utxo

    for loader in (raw_utxo, raw_account):
        assert set(loader.BOUND_TABLES) <= set(loader.LAKE_TABLES)
        assert loader.BOUND_TABLES


# --------------------------------------------------------------------------- #
# An empty block is complete at zero transactions                              #
# --------------------------------------------------------------------------- #


def test_empty_blocks_at_the_tip_are_not_a_tear() -> None:
    """ETH permits a block with no transactions. It contributes no rows to
    `transaction`, so the table legitimately ends below `block` -- bounding
    there would drop a block that was complete."""
    assert job.extend_over_empty({901: 0, 902: 0}, 900, 902) == 902


def test_the_walk_stops_at_the_first_block_that_claims_transactions() -> None:
    """The BCH case sits right after an empty one: block 901 held nothing and
    902 held 118 that were never written. Reaching past 901 is correct,
    reaching past 902 is the bug."""
    assert job.extend_over_empty({901: 0, 902: 118, 903: 0}, 900, 903) == 901


def test_a_block_missing_from_the_map_stops_the_walk() -> None:
    """Absence is not evidence of completeness. Guessing the other way writes
    a block whose transactions do not exist, which is the failure this whole
    path exists to prevent."""
    assert job.extend_over_empty({902: 0}, 900, 902) == 900


def test_nothing_above_the_transaction_tip_leaves_it_alone() -> None:
    assert job.extend_over_empty({}, 900, 900) == 900


def test_the_walk_is_capped() -> None:
    """A gap of thousands of empty blocks is not a run of empty blocks, it is
    a tear -- and extending over it is exactly the mistake being prevented."""
    huge = {block: 0 for block in range(901, 9001)}
    assert job.extend_over_empty(huge, 900, 9000) == 900 + job.MAX_EMPTY_TIP


def test_a_utxo_snapshot_never_needs_the_extension() -> None:
    """Every UTXO block carries a coinbase, so `transaction` cannot end below
    `block` for a legitimate reason. The extension is account-only in practice
    and must not invent headroom here."""
    assert job.extend_over_empty({967022: 1, 967023: 14}, 967021, 967023) == 967021


# --------------------------------------------------------------------------- #
# Seeding the v3 raw rate table, which the lake cannot fill                    #
# --------------------------------------------------------------------------- #


def test_a_v2_coin_table_gains_the_asset_it_never_had(spark) -> None:
    """v2's coin rates carry no `asset` column at all -- there was only ever
    one asset -- so the ticker has to be supplied to reach v3's merged shape."""
    v2 = spark.createDataFrame(
        [{"date": "2026-06-20", "fiat_values": {"EUR": 172.7, "USD": 198.0}}],
        schema=V2_RAW,
    )
    rows = job.raw_rate_rows(v2, None, symbol="BCH").collect()
    assert [(r["asset"], r["date"]) for r in rows] == [("BCH", "2026-06-20")]
    assert rows[0]["fiat_values"] == {"EUR": 172.7, "USD": 198.0}


def test_the_account_token_table_is_merged_in(spark) -> None:
    """v2 splits rates in two and v3 keeps one table, so seeding an account
    keyspace has to union them -- otherwise every token rate is dropped."""
    coin = spark.createDataFrame(
        [{"date": "2026-06-20", "fiat_values": {"EUR": 2000.0}}], schema=V2_RAW
    )
    tokens = spark.createDataFrame(
        [{"asset": "USDT", "date": "2026-06-20", "fiat_values": {"EUR": 0.9}}],
        schema=V2_TOKEN,
    )
    rows = job.raw_rate_rows(coin, tokens, symbol="ETH").collect()
    assert sorted((r["asset"], r["date"]) for r in rows) == [
        ("ETH", "2026-06-20"),
        ("USDT", "2026-06-20"),
    ]


def test_a_v3_source_needs_no_union_and_keeps_its_assets(spark) -> None:
    v3 = spark.createDataFrame(
        [
            {"asset": "ETH", "date": "2026-06-20", "fiat_values": {"EUR": 2000.0}},
            {"asset": "USDT", "date": "2026-06-20", "fiat_values": {"EUR": 0.9}},
        ],
        schema=V3_RAW,
    )
    rows = job.raw_rate_rows(v3, None, symbol="IGNORED").collect()
    assert sorted(r["asset"] for r in rows) == ["ETH", "USDT"]


def test_one_row_per_asset_and_date(spark) -> None:
    """(asset, date) is the primary key, so a duplicated source row would be a
    silent upsert rather than the two rows the count reports."""
    v2 = spark.createDataFrame(
        [
            {"date": "2026-06-20", "fiat_values": {"EUR": 1.0}},
            {"date": "2026-06-20", "fiat_values": {"EUR": 1.0}},
        ],
        schema=V2_RAW,
    )
    assert job.raw_rate_rows(v2, None, symbol="BCH").count() == 1


def test_a_per_block_source_cannot_seed_a_date_keyed_table(spark) -> None:
    """A derived keyspace has already resolved dates to blocks, so it cannot
    fill a table keyed by date -- and failing loudly beats writing a table with
    a `date` column full of nulls."""
    derived = spark.createDataFrame(
        [
            {
                "asset": "BCH",
                "block_id_group": 0,
                "block_id": 2,
                "fiat_values": {"EUR": 9.0},
            }
        ],
        schema=V3_DERIVED,
    )
    with pytest.raises(SystemExit, match="keyed by block"):
        job.raw_rate_rows(derived, None, symbol="BCH")
