"""Tests for the single-pass git commit-time lookup used by tagpack insert.

`get_last_commit_times` replaces a per-file `git log` walk; these tests pin
its correctness and that it stays equivalent to the previous per-file
`repo.iter_commits(paths=...)` approach.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pytest

from git import Repo

from graphsenselib.tagpack.tagpack import (
    get_last_commit_times,
    get_uri_for_tagpack,
)

# Strictly increasing commit timestamps (epoch seconds, fixed UTC offset).
T1 = 1_700_000_000
T2 = 1_700_000_100
T3 = 1_700_000_200
T4 = 1_700_000_300


def _commit(repo: Repo, files: list[Path], message: str, ts: int) -> str:
    """Stage `files` and commit them at a fixed author/committer time."""
    repo.index.add([str(f) for f in files])
    when = f"{ts} +0000"
    commit = repo.index.commit(message, author_date=when, commit_date=when)
    return commit.hexsha


def _iter_commits_last_time(repo: Repo, file: Path) -> datetime:
    """The previous approach: most recent commit via a per-file history walk."""
    commits = list(repo.iter_commits(paths=str(file)))
    return datetime.fromtimestamp(commits[0].committed_date)


@pytest.fixture
def git_repo(tmp_path: Path):
    """A repo with a small linear history and one uncommitted file.

    Returns (repo, repo_path, files, expected) where `expected` maps each
    tracked file path-string to the datetime of its most recent commit.
    """
    repo = Repo.init(tmp_path)
    with repo.config_writer() as cw:
        cw.set_value("user", "name", "Test User")
        cw.set_value("user", "email", "test@example.com")
    repo.create_remote("origin", "https://github.com/example/tagpacks.git")

    p1 = tmp_path / "packs" / "a" / "p1.yaml"
    p2 = tmp_path / "packs" / "a" / "p2.yaml"
    p3 = tmp_path / "packs" / "b" / "p3.yaml"
    untracked = tmp_path / "packs" / "b" / "untracked.yaml"
    for p in (p1, p2, p3, untracked):
        p.parent.mkdir(parents=True, exist_ok=True)

    p1.write_text("p1: v1\n")
    _commit(repo, [p1], "add p1", T1)

    p2.write_text("p2: v1\n")
    _commit(repo, [p2], "add p2", T2)

    p3.write_text("p3: v1\n")
    p1.write_text("p1: v2\n")  # p1 modified again in this commit
    _commit(repo, [p1, p3], "add p3, edit p1", T3)

    untracked.write_text("untracked: v1\n")  # never committed

    expected = {
        str(p1): datetime.fromtimestamp(T3),
        str(p2): datetime.fromtimestamp(T2),
        str(p3): datetime.fromtimestamp(T3),
    }
    files = {"p1": p1, "p2": p2, "p3": p3, "untracked": untracked}
    return repo, tmp_path, files, expected


class TestGetLastCommitTimes:
    def test_basic_correctness(self, git_repo):
        repo, repo_path, files, expected = git_repo
        tracked = [files["p1"], files["p2"], files["p3"]]

        result = get_last_commit_times(repo_path, tracked)

        assert result == expected

    def test_equivalent_to_iter_commits(self, git_repo):
        """Single-pass result matches the per-file iter_commits walk."""
        repo, repo_path, files, _ = git_repo
        tracked = [files["p1"], files["p2"], files["p3"]]

        result = get_last_commit_times(repo_path, tracked)

        for f in tracked:
            assert result[str(f)] == _iter_commits_last_time(repo, f)

    def test_uncommitted_file_absent(self, git_repo):
        """A file with no commit history is omitted from the result."""
        repo, repo_path, files, _ = git_repo

        result = get_last_commit_times(repo_path, list(files.values()))

        assert str(files["untracked"]) not in result
        assert set(result) == {
            str(files["p1"]),
            str(files["p2"]),
            str(files["p3"]),
        }

    def test_empty_input(self, git_repo):
        repo, repo_path, _, _ = git_repo

        assert get_last_commit_times(repo_path, []) == {}

    def test_keys_match_caller_path_strings(self, git_repo):
        """The map is keyed by the exact path strings passed in."""
        repo, repo_path, files, _ = git_repo
        # Pass a non-normalized path; the key must be that same string.
        messy = Path(str(files["p1"]).replace("/p1.yaml", "/./p1.yaml"))

        result = get_last_commit_times(repo_path, [messy])

        assert str(messy) in result
        assert result[str(messy)] == datetime.fromtimestamp(T3)


class TestGetUriForTagpackCommitDate:
    def test_with_and_without_map_agree(self, git_repo):
        """get_uri_for_tagpack yields the same commit date on both paths."""
        repo, repo_path, files, _ = git_repo
        tracked = [files["p1"], files["p2"], files["p3"]]
        commit_times = get_last_commit_times(repo_path, tracked)

        for f in tracked:
            with_map = get_uri_for_tagpack(
                repo_path, str(f), False, False, commit_times
            )
            without_map = get_uri_for_tagpack(repo_path, str(f), False, False)
            assert with_map[3] == without_map[3]
            # ... and the rest of the resolved URI tuple is unaffected.
            assert with_map[:3] == without_map[:3]

    def test_fallback_matches_full_history_walk(self, git_repo):
        """commit_times=None (max_count=1) matches the old full-list [0]."""
        repo, repo_path, files, _ = git_repo

        for f in (files["p1"], files["p2"], files["p3"]):
            _, _, _, commit_date = get_uri_for_tagpack(repo_path, str(f), False, False)
            assert commit_date == _iter_commits_last_time(repo, f)


def test_equivalent_with_merge(tmp_path: Path):
    """Equivalence holds across a history that contains a merge commit."""
    repo = Repo.init(tmp_path)
    with repo.config_writer() as cw:
        cw.set_value("user", "name", "Test User")
        cw.set_value("user", "email", "test@example.com")

    base = tmp_path / "packs" / "base.yaml"
    feat = tmp_path / "packs" / "feat.yaml"
    base.parent.mkdir(parents=True, exist_ok=True)

    base.write_text("base: v1\n")
    _commit(repo, [base], "add base", T1)
    main_branch = repo.active_branch.name

    # Feature branch edits feat.yaml, then is merged back with a merge commit.
    repo.git.checkout("-b", "feature")
    feat.write_text("feat: v1\n")
    _commit(repo, [feat], "add feat on branch", T2)

    repo.git.checkout(main_branch)
    base.write_text("base: v2\n")
    _commit(repo, [base], "edit base on main", T3)

    merge_when = f"{T4} +0000"
    repo.git.merge(
        "feature",
        "--no-ff",
        "-m",
        "merge feature",
        env={"GIT_AUTHOR_DATE": merge_when, "GIT_COMMITTER_DATE": merge_when},
    )

    tracked = [base, feat]
    result = get_last_commit_times(tmp_path, tracked)

    for f in tracked:
        assert result[str(f)] == _iter_commits_last_time(repo, f)
    # base last changed on main (T3); feat last changed on the branch (T2).
    assert result[str(base)] == datetime.fromtimestamp(T3)
    assert result[str(feat)] == datetime.fromtimestamp(T2)
