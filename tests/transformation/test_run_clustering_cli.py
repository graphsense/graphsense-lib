"""Wiring tests for `transformation cluster` safety behavior.

Exercises the command with Cassandra/Spark stubbed out: the --end-block
guard against the delta updater's watermark (both regimes and the
--disable-safety-checks override) and the ordering guarantee that
summary_statistics is read only after the keyspace lock is held.
"""

import contextlib
from types import SimpleNamespace

from click.testing import CliRunner

from graphsenselib.transformation.cli import transformation_cli


class _FakeTransformed:
    def __init__(self, watermark, fresh_active, events):
        self._watermark = watermark
        self._fresh_active = fresh_active
        self._events = events

    def get_summary_statistics(self):
        self._events.append("stats")
        return SimpleNamespace(no_addresses=100)

    def get_highest_block_delta_updater(self, sanity_check=True):
        return self._watermark

    def is_fresh_clustering_active(self):
        return self._fresh_active

    def get_cluster_id_bucket_size(self):
        return 5000

    def get_coinjoin_filtering(self):
        return True


def _invoke(monkeypatch, args, watermark=500, fresh_active=False):
    """Run the command with all external I/O stubbed; return (result, calls, events)."""
    events = []
    calls = {}

    fake_db = SimpleNamespace(
        transformed=_FakeTransformed(watermark, fresh_active, events)
    )

    @contextlib.contextmanager
    def fake_from_config(self, env, currency):
        yield fake_db

    @contextlib.contextmanager
    def fake_create_lock(name, **kwargs):
        events.append("lock")
        yield

    def fake_run_clustering_spark(spark, **kwargs):
        calls.update(kwargs)

    monkeypatch.setattr(
        "graphsenselib.db.factory.DbFactory.from_config", fake_from_config
    )
    monkeypatch.setattr(
        "graphsenselib.schema.schema.GraphsenseSchemas.apply_migrations",
        lambda self, *a, **k: None,
    )
    monkeypatch.setattr("graphsenselib.utils.locking.create_lock", fake_create_lock)
    monkeypatch.setattr(
        "graphsenselib.transformation.spark.create_spark_session",
        lambda **kwargs: SimpleNamespace(stop=lambda: None),
    )
    monkeypatch.setattr(
        "graphsenselib.transformation.clustering.run_clustering_spark",
        fake_run_clustering_spark,
    )
    monkeypatch.setattr(
        "graphsenselib.db.state.mark_fresh_clustering_active",
        lambda db: events.append("marker"),
    )

    result = CliRunner().invoke(
        transformation_cli,
        ["transformation", "cluster", "-e", "pytest", "-c", "btc", "--local", *args],
    )
    return result, calls, events


def test_end_block_below_watermark_refused_first_bootstrap(monkeypatch):
    result, calls, _ = _invoke(
        monkeypatch, ["--end-block", "400"], watermark=500, fresh_active=False
    )
    assert result.exit_code != 0
    assert "below the delta updater's last synced block 500" in result.output
    assert "never be clustered" in result.output
    assert not calls  # clustering never ran


def test_end_block_below_watermark_refused_rerun(monkeypatch):
    result, calls, _ = _invoke(
        monkeypatch, ["--end-block", "400"], watermark=500, fresh_active=True
    )
    assert result.exit_code != 0
    assert "regress" in result.output
    assert not calls


def test_end_block_below_watermark_allowed_with_override(monkeypatch):
    result, calls, events = _invoke(
        monkeypatch,
        ["--end-block", "400", "--disable-safety-checks"],
        watermark=500,
    )
    assert result.exit_code == 0, result.output
    assert calls["end_block"] == 400
    assert "marker" in events


def test_end_block_at_watermark_allowed(monkeypatch):
    result, calls, _ = _invoke(monkeypatch, ["--end-block", "500"], watermark=500)
    assert result.exit_code == 0, result.output
    assert calls["end_block"] == 500


def test_no_end_block_runs_full_clustering(monkeypatch):
    result, calls, _ = _invoke(monkeypatch, [], watermark=500)
    assert result.exit_code == 0, result.output
    assert calls["end_block"] is None


def test_summary_statistics_read_after_lock(monkeypatch):
    # The union-find bound must reflect the address space as of lock
    # acquisition — a pre-lock read can go stale while a delta run finishes.
    result, _, events = _invoke(monkeypatch, [], watermark=500)
    assert result.exit_code == 0, result.output
    assert events.index("lock") < events.index("stats")
