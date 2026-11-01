"""The modification check of a tagpack repo runs once, not once per file.

`repo.is_dirty()` diffs the whole working tree. `get_uri_for_tagpack` used to
run it for every tagpack file, which cost about four hours per night on a
reused repo cache (1,961 files x two `git diff`s of 1-2 s each). Callers now
check once with `exit_if_repo_modified` and pass `strict_check=False`.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from git import Repo

from graphsenselib.tagpack.tagpack import exit_if_repo_modified, get_uri_for_tagpack


@pytest.fixture
def repo_with_packs(tmp_path: Path):
    repo = Repo.init(tmp_path)
    with repo.config_writer() as cw:
        cw.set_value("user", "name", "Test User")
        cw.set_value("user", "email", "test@example.com")
    repo.create_remote("origin", "https://github.com/example/tagpacks.git")
    files = []
    for name in ("p1", "p2", "p3"):
        f = tmp_path / "packs" / f"{name}.yaml"
        f.parent.mkdir(parents=True, exist_ok=True)
        f.write_text(f"{name}: v1\n")
        files.append(f)
    repo.index.add([str(f) for f in files])
    repo.index.commit("add packs")
    return tmp_path, files


def test_clean_repo_passes(repo_with_packs):
    path, _ = repo_with_packs
    exit_if_repo_modified(str(path))  # no exit


def test_modified_repo_exits_zero(repo_with_packs):
    path, files = repo_with_packs
    files[0].write_text("p1: changed\n")
    with pytest.raises(SystemExit) as e:
        exit_if_repo_modified(str(path))
    assert e.value.code == 0


def test_strict_check_still_exits_on_a_modified_repo(repo_with_packs):
    # unchanged behaviour for direct callers that keep strict_check=True
    path, files = repo_with_packs
    files[0].write_text("p1: changed\n")
    with pytest.raises(SystemExit):
        get_uri_for_tagpack(str(path), str(files[1]), True, False)


def test_without_strict_check_no_diff_per_file(repo_with_packs, monkeypatch):
    path, files = repo_with_packs
    calls = []
    monkeypatch.setattr(Repo, "is_dirty", lambda self, *a, **k: calls.append(1))
    for f in files:
        uri, rel, _, date = get_uri_for_tagpack(str(path), str(f), False, False)
        assert rel == f"packs/{f.name}"
        assert uri.endswith(f"/packs/{f.name}")
        assert date is not None
    assert calls == []


class _StopAfterPreparation(Exception):
    pass


class _StubTagStore:
    """No database: any DB call after loading the actor mappings (i.e. once
    insert_tagpack has prepared the files) ends the test there."""

    def __init__(self, *a, **k):
        pass

    def get_actor_alias_mapping(self):
        return {}

    def __getattr__(self, name):
        def stop(*a, **k):
            raise _StopAfterPreparation(name)

        return stop


def test_insert_tagpack_checks_the_repo_once(repo_with_packs, monkeypatch):
    from graphsenselib.tagpack import cli

    path, files = repo_with_packs
    monkeypatch.setattr(cli, "TagStore", _StubTagStore)
    calls = []

    def counting_is_dirty(self, *a, **k):
        calls.append(1)
        return False

    monkeypatch.setattr(Repo, "is_dirty", counting_is_dirty)
    with pytest.raises(_StopAfterPreparation):
        cli.insert_tagpack(
            url="postgresql://unused",
            schema="public",
            path=str(path),
            batch_size=1,
            public=False,
            force=False,
            add_new=False,
            no_strict_check=False,
            no_git=False,
            n_workers=1,
            no_validation=True,
            tag_type_default="actor",
            config=None,
            update_flag=True,
        )
    assert len(files) == 3
    assert len(calls) == 1  # was one per file
