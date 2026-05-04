"""Unit tests for auto-catch-up of diverged sinks in append-mode ingest."""

import logging

import pytest

from graphsenselib.ingest.common import BlockRangeContent, Sink, Source
from graphsenselib.ingest.dump import (
    _abort_on_sink_divergence,
    _catch_up_diverged_sinks,
    _diverged_sinks,
)
from graphsenselib.ingest.ingestrunner import IngestRunner


class FakeSource(Source):
    """Source that yields one row per block; tracks read ranges for assertions."""

    def __init__(self, max_block=10_000):
        self._max_block = max_block
        self.read_ranges: list[tuple[int, int]] = []

    def read_blockrange(self, start_block, end_block):
        self.read_ranges.append((start_block, end_block))
        blocks = [
            {"block_id": i, "timestamp": 1_700_000_000 + i * 12}
            for i in range(start_block, end_block + 1)
        ]
        return BlockRangeContent(
            table_contents={"block": blocks},
            start_block=start_block,
            end_block=end_block,
        )

    def read_blockindep(self):
        return BlockRangeContent(table_contents={})

    def get_last_synced_block(self):
        return self._max_block


class FakeTransformer:
    """Identity transformer that satisfies IngestRunner's contract."""

    network = "eth"

    def transform(self, data):
        return data

    def transform_blockindep(self, data):
        return data


class FakeSink(Sink):
    """In-memory sink that records writes and exposes a mutable head."""

    def __init__(self, name: str, monotonic: bool = True, initial_height=None):
        self.name = name
        self.requires_monotonic_append = monotonic
        self._height = initial_height
        self.writes: list[tuple[int, int]] = []

    def write(self, block_range_content: BlockRangeContent):
        start = block_range_content.start_block
        end = block_range_content.end_block
        if start is None or end is None:
            return
        self.writes.append((start, end))
        self._height = max(self._height or -1, end)

    def highest_block(self):
        return self._height


def _build_runner(sinks):
    runner = IngestRunner(partition_batch_size=1000, file_batch_size=100)
    runner.addSource(FakeSource())
    runner.addTransformer(FakeTransformer())
    for s in sinks:
        runner.addSink(s)
    return runner


# -----------------------------------------------------------------------------
# _diverged_sinks
# -----------------------------------------------------------------------------


@pytest.mark.parametrize(
    "heights, expected_target",
    [
        ([("a", 100), ("b", 100)], None),  # aligned
        ([("a", None), ("b", None)], None),  # both empty
        ([("a", 100)], None),  # single sink, never divergent
        ([("a", 100), ("b", 90)], 100),  # clean lag
        ([("a", 100), ("b", None)], 100),  # one empty
        ([("a", 50), ("b", 100), ("c", 100)], 100),  # one laggard among many
    ],
)
def test_diverged_sinks_classification(heights, expected_target):
    target, laggards = _diverged_sinks(heights)
    assert target == expected_target
    if expected_target is None:
        assert laggards == []
    else:
        assert all(h != expected_target for _, h in laggards)


# -----------------------------------------------------------------------------
# _catch_up_diverged_sinks
# -----------------------------------------------------------------------------


def test_catchup_noop_when_aligned():
    leader = FakeSink("delta", monotonic=True, initial_height=200)
    follower = FakeSink("cassandra", monotonic=False, initial_height=200)
    runner = _build_runner([leader, follower])
    source = runner.source
    transformer = runner.transformers[0]

    _catch_up_diverged_sinks(
        runner, source, transformer, "eth", max_auto_catchup=10_000
    )

    # No catch-up writes should have happened on either sink.
    assert leader.writes == []
    assert follower.writes == []
    # Source untouched.
    assert source.read_ranges == []


def test_catchup_fills_idempotent_laggard():
    """Cassandra empty, delta at 199 → cassandra catches up [0, 199]."""
    delta = FakeSink("delta", monotonic=True, initial_height=199)
    cassandra = FakeSink("cassandra", monotonic=False, initial_height=None)
    runner = _build_runner([delta, cassandra])

    _catch_up_diverged_sinks(
        runner,
        runner.source,
        runner.transformers[0],
        "eth",
        max_auto_catchup=10_000,
    )

    # Delta untouched; cassandra writes the missing range.
    assert delta.writes == []
    assert cassandra.highest_block() == 199
    written = sorted(cassandra.writes)
    assert written[0][0] == 0 and written[-1][1] == 199
    # Continuous coverage [0, 199] across all chunks.
    covered = set()
    for s, e in written:
        covered.update(range(s, e + 1))
    assert covered == set(range(0, 200))


def test_catchup_fills_monotonic_laggard_clean_tail():
    """Delta at 50, cassandra at 199 → delta catches up [51, 199] cleanly."""
    delta = FakeSink("delta", monotonic=True, initial_height=50)
    cassandra = FakeSink("cassandra", monotonic=False, initial_height=199)
    runner = _build_runner([delta, cassandra])

    _catch_up_diverged_sinks(
        runner,
        runner.source,
        runner.transformers[0],
        "eth",
        max_auto_catchup=10_000,
    )

    assert delta.highest_block() == 199
    assert cassandra.writes == []
    # Coverage [51, 199] — exactly the tail above delta's prior head.
    covered = set()
    for s, e in delta.writes:
        covered.update(range(s, e + 1))
    assert covered == set(range(51, 200))


def test_catchup_handles_multiple_laggards():
    """Two laggards behind a leader; both are caught up to the leader."""
    leader = FakeSink("delta", monotonic=True, initial_height=199)
    lag1 = FakeSink("cassandra", monotonic=False, initial_height=None)
    lag2 = FakeSink("postgres", monotonic=False, initial_height=49)
    runner = _build_runner([leader, lag1, lag2])

    _catch_up_diverged_sinks(
        runner,
        runner.source,
        runner.transformers[0],
        "eth",
        max_auto_catchup=10_000,
    )

    assert leader.writes == []
    assert lag1.highest_block() == 199
    assert lag2.highest_block() == 199


def test_catchup_aborts_when_gap_exceeds_limit(caplog):
    """A laggard whose gap exceeds the limit must call SystemExit (sys.exit(13))."""
    leader = FakeSink("delta", monotonic=True, initial_height=10_000)
    laggard = FakeSink("cassandra", monotonic=False, initial_height=None)
    runner = _build_runner([leader, laggard])

    with pytest.raises(SystemExit) as exc:
        with caplog.at_level(logging.ERROR):
            _catch_up_diverged_sinks(
                runner,
                runner.source,
                runner.transformers[0],
                "eth",
                max_auto_catchup=100,
            )
    assert exc.value.code == 13
    # Recovery command is printed via _abort_on_sink_divergence.
    assert any("--sinks cassandra" in r.message for r in caplog.records)
    # The laggard was *not* partially written before the abort.
    assert laggard.writes == []


def test_catchup_warns_on_entry(caplog):
    """The catch-up entry must emit a WARNING (not just INFO)."""
    leader = FakeSink("delta", monotonic=True, initial_height=199)
    laggard = FakeSink("cassandra", monotonic=False, initial_height=None)
    runner = _build_runner([leader, laggard])

    with caplog.at_level(logging.WARNING):
        _catch_up_diverged_sinks(
            runner,
            runner.source,
            runner.transformers[0],
            "eth",
            max_auto_catchup=10_000,
        )
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert any("Sink divergence" in r.message for r in warnings)
    assert any("Catching up cassandra" in r.message for r in warnings)


def test_catchup_aborts_when_residual_divergence(caplog):
    """If a laggard's height doesn't advance after catch-up, abort."""
    leader = FakeSink("delta", monotonic=True, initial_height=199)

    # Sink whose write is a no-op (simulates a broken sink).
    class StuckSink(FakeSink):
        def write(self, brc):
            pass  # never advances height

    stuck = StuckSink("cassandra", monotonic=False, initial_height=None)
    runner = _build_runner([leader, stuck])

    with pytest.raises(SystemExit) as exc:
        with caplog.at_level(logging.ERROR):
            _catch_up_diverged_sinks(
                runner,
                runner.source,
                runner.transformers[0],
                "eth",
                max_auto_catchup=10_000,
            )
    assert exc.value.code == 13
    assert any("still disagree" in r.message for r in caplog.records)


# -----------------------------------------------------------------------------
# _abort_on_sink_divergence (recovery command shape)
# -----------------------------------------------------------------------------


def test_abort_recovery_command_uses_laggard_plus_one(caplog):
    """Recovery command must start at laggard_h+1 and end at the leader."""
    with pytest.raises(SystemExit) as exc:
        with caplog.at_level(logging.ERROR):
            _abort_on_sink_divergence("trx", [("delta", 199), ("cassandra", 49)])
    assert exc.value.code == 13
    msg = "\n".join(r.message for r in caplog.records)
    assert "--sinks cassandra --start-block 50 --end-block 199" in msg


def test_abort_recovery_command_uses_zero_for_empty_laggard(caplog):
    """A None laggard height must surface as start-block 0."""
    with pytest.raises(SystemExit):
        with caplog.at_level(logging.ERROR):
            _abort_on_sink_divergence("eth", [("delta", 199), ("cassandra", None)])
    msg = "\n".join(r.message for r in caplog.records)
    assert "--sinks cassandra --start-block 0 --end-block 199" in msg
