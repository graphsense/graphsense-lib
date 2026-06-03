"""BCH pre-fork start_block default in PubkeyUpdate.run.

BCH shares BTC history before block 478558; defaulting BCH's start_block to the
fork (when none is given) avoids extracting the shared pre-fork blocks, which
would trivially collide with BTC. Tested without Spark by stubbing the run()
internals (the session is injected, not created).
"""

from unittest.mock import MagicMock

from graphsenselib.pubkey.job import BCH_FORK_BLOCK, PubkeyUpdate


def _make_job(currency, monkeypatch):
    job = PubkeyUpdate(
        spark=MagicMock(),
        currency=currency,
        source_path="/src",
        sink_path="/sink",
        sink_type="delta",
    )
    captured = {}
    monkeypatch.setattr(job, "_read_state", lambda: -1)  # fresh: no prior state
    monkeypatch.setattr(
        job,
        "_extract_pubkeys_df",
        lambda start, end: captured.update(start=start, end=end) or MagicMock(),
    )
    monkeypatch.setattr(job, "_append_observed", lambda df: None)
    monkeypatch.setattr(
        job,
        "_detect_and_materialise_cross_chain",
        lambda: captured.update(detected=True),
    )
    monkeypatch.setattr(
        job, "_write_state", lambda end: captured.update(state_written=end)
    )
    return job, captured


def test_bch_defaults_start_block_to_fork(monkeypatch):
    job, captured = _make_job("bch", monkeypatch)
    job.run(start_block=None, end_block=500000)
    # effective start is the fork; source filter is block_id > start_block,
    # so the shared pre-fork blocks (<=478558) are skipped.
    assert captured["start"] == BCH_FORK_BLOCK


def test_bch_explicit_start_block_overrides(monkeypatch):
    job, captured = _make_job("bch", monkeypatch)
    job.run(start_block=100, end_block=500000)
    assert captured["start"] == 100


def test_btc_has_no_fork_default(monkeypatch):
    job, captured = _make_job("btc", monkeypatch)
    job.run(start_block=None, end_block=500000)
    assert captured["start"] == -1  # no default; resumes from state (last_done)


def test_detect_runs_by_default(monkeypatch):
    job, captured = _make_job("btc", monkeypatch)
    job.run(start_block=None, end_block=500000)
    assert captured.get("detected") is True
    assert captured["state_written"] == 500000


def test_skip_detect_defers_detection_but_bumps_state(monkeypatch):
    job, captured = _make_job("btc", monkeypatch)
    job.run(start_block=None, end_block=500000, skip_detect=True)
    # detection is deferred (run a standalone pubkey-detect later) ...
    assert "detected" not in captured
    # ... but the blocks are still recorded as processed.
    assert captured["state_written"] == 500000
