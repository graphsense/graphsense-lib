"""Tests for the persistent repo-cache sync helpers used by `tagpack sync`.

`_sync_repo` keeps cloned repositories between runs and refreshes them with a
git fetch, but must stay equivalent to a fresh clone: a reused checkout ends
up matching the remote exactly, and any unusable cache is re-cloned.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

from git import Repo

from graphsenselib.tagpack.cli import (
    _repo_cache_dir,
    _repo_workdir,
    _sync_repo,
)

# A marker dropped inside .git/ (which `git clean` never touches) lets a test
# tell a fetch-refresh apart from a re-clone: a re-clone removes the whole
# workdir, a fetch leaves the marker in place.
MARKER = Path(".git") / "SYNC_MARKER"


def _init_repo(path: Path) -> Repo:
    path.mkdir(parents=True, exist_ok=True)
    repo = Repo.init(path)
    with repo.config_writer() as cw:
        cw.set_value("user", "name", "Test User")
        cw.set_value("user", "email", "test@example.com")
    return repo


def _commit(repo: Repo, repo_path: Path, relpath: str, content: str, msg: str):
    f = repo_path / relpath
    f.parent.mkdir(parents=True, exist_ok=True)
    f.write_text(content)
    repo.index.add([str(f)])
    repo.index.commit(msg)


@pytest.fixture
def upstream(tmp_path: Path):
    """A bare-enough upstream repo (a normal on-disk repo used as clone URL)."""
    repo_path = tmp_path / "upstream"
    repo = _init_repo(repo_path)
    _commit(repo, repo_path, "packs/p1.yaml", "p1: v1\n", "add p1")
    return repo, repo_path, str(repo_path)


@pytest.fixture
def cache_dir(tmp_path: Path) -> Path:
    return tmp_path / "cache"


class TestRepoWorkdir:
    def test_stable_and_distinct(self, cache_dir: Path):
        a1 = _repo_workdir(cache_dir, "https://example.com/foo/bar.git")
        a2 = _repo_workdir(cache_dir, "https://example.com/foo/bar.git")
        b = _repo_workdir(cache_dir, "https://example.com/foo/other.git")

        assert a1 == a2  # deterministic for the same URL
        assert a1 != b  # different URLs never collide
        assert a1.parent == cache_dir
        assert "bar" in a1.name  # human-readable slug retained

    def test_unparseable_url_does_not_raise(self, cache_dir: Path):
        wd = _repo_workdir(cache_dir, "not-a-real-url")
        assert wd.parent == cache_dir


class TestRepoCacheDir:
    def test_default_is_under_system_temp(self):
        d = _repo_cache_dir(None)
        assert d.exists()
        assert d.parent == Path(tempfile.gettempdir())

    def test_explicit_dir_is_created(self, tmp_path: Path):
        target = tmp_path / "nested" / "repo-cache"
        d = _repo_cache_dir(str(target))
        assert d == target
        assert d.is_dir()


class TestSyncRepo:
    def test_clone_on_first_run(self, upstream, cache_dir: Path):
        _, _, url = upstream
        workdir = _repo_workdir(cache_dir, url)

        repo = _sync_repo(url, None, workdir)

        assert workdir.is_dir()
        assert (workdir / "packs" / "p1.yaml").read_text() == "p1: v1\n"
        assert url in list(repo.remotes.origin.urls)

    def test_fetch_reuses_existing_checkout(self, upstream, cache_dir: Path):
        up_repo, up_path, url = upstream
        workdir = _repo_workdir(cache_dir, url)

        _sync_repo(url, None, workdir)
        (workdir / MARKER).write_text("marker")  # survives a fetch, not a re-clone

        # New commit upstream; the second sync must pick it up via fetch.
        _commit(up_repo, up_path, "packs/p2.yaml", "p2: v1\n", "add p2")
        _sync_repo(url, None, workdir)

        assert (workdir / MARKER).exists()  # not re-cloned
        assert (workdir / "packs" / "p2.yaml").read_text() == "p2: v1\n"

    def test_reset_hard_discards_local_changes(self, upstream, cache_dir: Path):
        _, _, url = upstream
        workdir = _repo_workdir(cache_dir, url)

        _sync_repo(url, None, workdir)
        # Tamper with the checkout: modify a tracked file, add an untracked one.
        (workdir / "packs" / "p1.yaml").write_text("p1: TAMPERED\n")
        (workdir / "stray.yaml").write_text("stray\n")

        _sync_repo(url, None, workdir)

        # Working tree is authoritative again, exactly like a fresh clone.
        assert (workdir / "packs" / "p1.yaml").read_text() == "p1: v1\n"
        assert not (workdir / "stray.yaml").exists()

    def test_reclone_on_corrupt_cache(self, upstream, cache_dir: Path):
        _, _, url = upstream
        workdir = _repo_workdir(cache_dir, url)
        # A non-git directory sitting where the checkout should be.
        workdir.mkdir(parents=True)
        (workdir / "garbage.txt").write_text("not a repo")

        repo = _sync_repo(url, None, workdir)

        assert (workdir / "packs" / "p1.yaml").read_text() == "p1: v1\n"
        assert not (workdir / "garbage.txt").exists()
        assert url in list(repo.remotes.origin.urls)

    def test_reclone_on_wrong_remote(self, upstream, cache_dir: Path, tmp_path):
        _, _, url_a = upstream
        workdir = _repo_workdir(cache_dir, url_a)
        _sync_repo(url_a, None, workdir)
        (workdir / MARKER).write_text("marker")

        # A different upstream reusing the same workdir must trigger a re-clone.
        other_path = tmp_path / "other"
        other = _init_repo(other_path)
        _commit(other, other_path, "packs/q1.yaml", "q1: v1\n", "add q1")
        url_b = str(other_path)

        repo = _sync_repo(url_b, None, workdir)

        assert not (workdir / MARKER).exists()  # re-cloned, marker gone
        assert (workdir / "packs" / "q1.yaml").read_text() == "q1: v1\n"
        assert url_b in list(repo.remotes.origin.urls)

    def test_branch_checkout(self, upstream, cache_dir: Path):
        up_repo, up_path, url = upstream
        main_branch = up_repo.active_branch.name

        up_repo.git.checkout("-b", "feature")
        _commit(up_repo, up_path, "packs/p1.yaml", "p1: feature\n", "edit on branch")
        up_repo.git.checkout(main_branch)

        workdir = _repo_workdir(cache_dir, url)
        repo = _sync_repo(url, "feature", workdir)

        assert repo.active_branch.name == "feature"
        assert (workdir / "packs" / "p1.yaml").read_text() == "p1: feature\n"

    def test_branch_refresh_picks_up_new_commits(self, upstream, cache_dir: Path):
        up_repo, up_path, url = upstream
        main_branch = up_repo.active_branch.name
        up_repo.git.checkout("-b", "feature")
        _commit(up_repo, up_path, "packs/p1.yaml", "p1: feature\n", "branch v1")
        up_repo.git.checkout(main_branch)

        workdir = _repo_workdir(cache_dir, url)
        _sync_repo(url, "feature", workdir)

        # Advance the feature branch upstream, then re-sync.
        up_repo.git.checkout("feature")
        _commit(up_repo, up_path, "packs/p1.yaml", "p1: feature2\n", "branch v2")
        up_repo.git.checkout(main_branch)

        repo = _sync_repo(url, "feature", workdir)

        assert repo.active_branch.name == "feature"
        assert (workdir / "packs" / "p1.yaml").read_text() == "p1: feature2\n"
