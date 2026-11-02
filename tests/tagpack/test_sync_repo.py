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


class TestIndexRefresh:
    """A checkout written in the same second as its index has "racily clean"
    entries that every `git diff` re-reads; read-only commands never fix them,
    so a reused cache stayed slow (1-2 s per diff on a large repo). _sync_repo
    refreshes the index once, after a clone and after a fetch."""

    @staticmethod
    def _spy(monkeypatch):
        from git.cmd import Git

        calls = []
        original = Git._call_process

        def spy(self, method, *args, **kwargs):
            calls.append((method, args))
            return original(self, method, *args, **kwargs)

        monkeypatch.setattr(Git, "_call_process", spy)
        return calls

    def test_refresh_after_clone_and_after_fetch(
        self, upstream, cache_dir: Path, monkeypatch
    ):
        _, _, url = upstream
        workdir = _repo_workdir(_repo_cache_dir(str(cache_dir)), url)
        calls = self._spy(monkeypatch)
        _sync_repo(url, None, workdir)  # clone
        _sync_repo(url, None, workdir)  # fetch
        refreshes = [c for c in calls if c == ("update_index", ("-q", "--refresh"))]
        assert len(refreshes) == 2

    def test_index_matches_the_working_tree_after_sync(self, upstream, cache_dir: Path):
        _, _, url = upstream
        workdir = _repo_workdir(_repo_cache_dir(str(cache_dir)), url)
        _sync_repo(url, None, workdir)
        repo = _sync_repo(url, None, workdir)
        assert not repo.is_dirty()
        assert repo.git.diff_files() == ""


class TestPartialClone:
    """`--clone-filter blob:none`: a clone without old file contents. lastmod
    (get_last_commit_times) must come out the same as from a full clone and
    must not need the missing contents."""

    @pytest.fixture
    def filtered_upstream(self, tmp_path: Path):
        """Upstream with history (p1 edited twice) that serves filters; a
        file:// URL, since git ignores --filter for a plain local path."""
        repo_path = tmp_path / "upstream"
        repo = _init_repo(repo_path)
        with repo.config_writer() as cw:
            cw.set_value("uploadpack", "allowFilter", "true")
            cw.set_value("uploadpack", "allowAnySHA1InWant", "true")
        _commit(repo, repo_path, "packs/p1.yaml", "p1: v1\n", "add p1")
        old_blob = repo.git.rev_parse("HEAD:packs/p1.yaml")
        _commit(repo, repo_path, "packs/p2.yaml", "p2: v1\n", "add p2")
        _commit(repo, repo_path, "packs/p1.yaml", "p1: v2\n", "edit p1")
        return repo, repo_path, repo_path.as_uri(), old_blob

    @staticmethod
    def _offline(repo: Repo, monkeypatch):
        """No lazy fetches, and no reachable remote either."""
        monkeypatch.setenv("GIT_NO_LAZY_FETCH", "1")
        repo.git.remote("set-url", "origin", "file:///nonexistent/upstream")

    @staticmethod
    def _lastmod(workdir: Path) -> dict:
        from graphsenselib.tagpack.tagpack import get_last_commit_times

        files = [workdir / "packs" / "p1.yaml", workdir / "packs" / "p2.yaml"]
        return {
            Path(k).name: v for k, v in get_last_commit_times(workdir, files).items()
        }

    def test_clone_is_partial_and_lastmod_matches_full_clone(
        self, filtered_upstream, tmp_path: Path, monkeypatch
    ):
        _, _, url, old_blob = filtered_upstream
        full_dir = tmp_path / "full"
        part_dir = tmp_path / "partial"
        _sync_repo(url, None, full_dir)
        part = _sync_repo(url, None, part_dir, clone_filter="blob:none")

        assert part.git.config("remote.origin.partialclonefilter") == "blob:none"
        assert (part_dir / "packs" / "p1.yaml").read_text() == "p1: v2\n"
        expected = self._lastmod(full_dir)
        assert set(expected) == {"p1.yaml", "p2.yaml"}

        self._offline(part, monkeypatch)
        # the old p1 contents really are not local ...
        assert (
            part.git.cat_file(
                "-e", old_blob, with_exceptions=False, with_extended_output=True
            )[0]
            != 0
        )
        # ... and neither lastmod nor the dirty check needs them
        assert self._lastmod(part_dir) == expected
        assert not part.is_dirty()

    def test_refresh_keeps_partial_clone(
        self, filtered_upstream, tmp_path: Path, monkeypatch
    ):
        up_repo, up_path, url, _ = filtered_upstream
        workdir = tmp_path / "partial"
        _sync_repo(url, None, workdir, clone_filter="blob:none")
        (workdir / MARKER).write_text("marker")

        _commit(up_repo, up_path, "packs/p2.yaml", "p2: v2\n", "edit p2")
        repo = _sync_repo(url, None, workdir, clone_filter="blob:none")

        assert (workdir / MARKER).exists()  # fetched, not re-cloned
        assert (workdir / "packs" / "p2.yaml").read_text() == "p2: v2\n"
        assert repo.git.config("remote.origin.partialclonefilter") == "blob:none"
        expected = self._lastmod(up_path)  # the upstream is a full repo
        self._offline(repo, monkeypatch)
        assert self._lastmod(workdir) == expected
        assert not repo.is_dirty()

    def test_unchanged_refresh_needs_no_lazy_fetch(
        self, filtered_upstream, tmp_path: Path, monkeypatch
    ):
        """A night without upstream changes: fetch, reset, clean and the
        index refresh touch no missing contents. (New commits do fetch their
        new file versions lazily, during the reset: that is the point of a
        blobless clone, and only current versions are fetched.)"""
        _, _, url, _ = filtered_upstream
        workdir = tmp_path / "partial"
        _sync_repo(url, None, workdir, clone_filter="blob:none")
        expected = self._lastmod(workdir)

        monkeypatch.setenv("GIT_NO_LAZY_FETCH", "1")
        repo = _sync_repo(url, None, workdir, clone_filter="blob:none")

        assert (workdir / "packs" / "p1.yaml").read_text() == "p1: v2\n"
        assert self._lastmod(workdir) == expected
        assert not repo.is_dirty()

    def test_full_cache_kept_when_filter_requested(
        self, filtered_upstream, tmp_path: Path
    ):
        up_repo, up_path, url, _ = filtered_upstream
        workdir = tmp_path / "cache"
        _sync_repo(url, None, workdir)  # full clone from an earlier version
        (workdir / MARKER).write_text("marker")

        _commit(up_repo, up_path, "packs/p3.yaml", "p3: v1\n", "add p3")
        repo = _sync_repo(url, None, workdir, clone_filter="blob:none")

        assert (workdir / MARKER).exists()  # kept, not re-cloned
        assert (workdir / "packs" / "p3.yaml").read_text() == "p3: v1\n"
        assert (
            repo.git.config(
                "--get", "remote.origin.partialclonefilter", with_exceptions=False
            )
            == ""
        )
