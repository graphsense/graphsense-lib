"""`tagpack sync` refreshes the tagstore views once, after the last step that
changes what they read: after the cluster mapping when one runs, else after
the tag import. A refresh before the mapping would only be redone after it."""

from __future__ import annotations

import click
import pytest

from graphsenselib.tagpack import cli


@pytest.fixture
def steps(monkeypatch):
    """Record the sync steps instead of running them against a database."""
    calls: list[str] = []

    def invoke(self, callback, *args, **kwargs):
        calls.append(getattr(callback, "name", None) or callback.__name__)

    monkeypatch.setattr(click.Context, "invoke", invoke)
    monkeypatch.setattr(
        cli, "calc_quality_measures", lambda *a, **k: calls.append("quality")
    )
    return calls


def _sync(tmp_path, monkeypatch, *, rate=0.0, **envs):
    repos = tmp_path / "repos.txt"
    repos.write_text("")
    monkeypatch.setattr(
        cli,
        "check_cluster_mapping_staleness",
        lambda **k: (rate, {"btc": {"diverged": 0, "checked": 1, "rate": rate}}),
    )
    kwargs = dict(
        run_cluster_mapping_with_env=None,
        rerun_cluster_mapping_with_env=None,
        auto_rerun_cluster_mapping_with_env=None,
    )
    kwargs.update(envs)
    cli._sync_impl(
        repos=str(repos),
        force=False,
        url="postgresql://unused",
        cluster_staleness_sample_size=10,
        cluster_staleness_threshold=0.05,
        n_workers=1,
        no_validation=True,
        dont_update_quality_metrics=False,
        repo_cache_dir=str(tmp_path / "cache"),
        **kwargs,
    )


def _after_init(calls):
    return calls[calls.index("remove-duplicates") :]


def test_no_mapping_refreshes_once(tmp_path, monkeypatch, steps):
    _sync(tmp_path, monkeypatch)
    assert _after_init(steps) == ["remove-duplicates", "quality", "refresh-views"]


@pytest.mark.parametrize(
    "envs",
    [
        {"run_cluster_mapping_with_env": "prod"},
        {"rerun_cluster_mapping_with_env": "prod"},
        {"auto_rerun_cluster_mapping_with_env": "prod"},
    ],
)
def test_mapping_refreshes_once_afterwards(tmp_path, monkeypatch, steps, envs):
    _sync(tmp_path, monkeypatch, **envs)
    assert _after_init(steps) == [
        "remove-duplicates",
        "quality",
        "insert-cluster-mappings",
        "refresh-views",
    ]
