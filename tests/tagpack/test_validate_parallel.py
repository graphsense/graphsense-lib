"""Parallel validation (--jobs). Synthetic packs only."""

import logging

import pytest
from click.testing import CliRunner

pytest.importorskip("yaml_include")
pytest.importorskip("rapidfuzz")

from graphsenselib.tagpack.cli import tagpacktool_cli

ACTORPACK = """title: Test
creator: Test
description: Test
lastmod: 2024-01-01
actors:
- id: exchangea
  uri: https://example.com
  label: Exchange A
  categories: [exchange]
"""

HEADER = """title: Test pack
creator: Test
source: http://example.com
currency: BTC
lastmod: 2024-01-01
"""


def _pack(i, extra=""):
    return f"""header: !include header.yaml
tags:
- address: 1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa
  label: Label {i}
  actor: exchangea{extra}
"""


@pytest.fixture
def packs(tmp_path):
    root = tmp_path / "packs"
    (root / "group").mkdir(parents=True)
    (root / "header.yaml").write_text(HEADER)
    for i in range(4):
        (root / "group" / f"pack{i}.yaml").write_text(_pack(i))
    actorpack = tmp_path / "actors.yaml"
    actorpack.write_text(ACTORPACK)
    return root, actorpack


def _validate(root, actorpack, *args):
    return CliRunner().invoke(
        tagpacktool_cli,
        [
            "tagpack-tool",
            "tagpack",
            "validate",
            str(root),
            "--actorpack-path",
            str(actorpack),
            *args,
        ],
    )


@pytest.mark.parametrize("jobs", ["1", "2", "0"])
def test_valid_packs_pass_with_any_number_of_jobs(packs, jobs):
    root, actorpack = packs
    result = _validate(root, actorpack, "--jobs", jobs)
    assert result.exit_code == 0, result.output


def test_parallel_run_reports_every_failure(packs, caplog):
    root, actorpack = packs
    # an unknown field and an unknown actor (strict actor references)
    (root / "group" / "pack1.yaml").write_text(_pack(1) + "  bogus_field: x\n")
    (root / "group" / "pack3.yaml").write_text(_pack(3).replace("exchangea", "nope"))
    with caplog.at_level(logging.INFO):
        result = _validate(root, actorpack, "--jobs", "2")
    assert result.exit_code == 1
    failed = [r.getMessage() for r in caplog.records if "FAILED" in r.getMessage()]
    assert any("bogus_field" in m for m in failed)
    assert any("pack3.yaml" in m for m in failed)
    assert any("2/4 TagPacks passed" in r.getMessage() for r in caplog.records)


def test_sequential_run_stops_at_first_invalid_pack(packs, caplog):
    root, actorpack = packs
    for i in range(4):
        (root / "group" / f"pack{i}.yaml").write_text(_pack(i) + "  bogus_field: x\n")
    with caplog.at_level(logging.INFO):
        result = _validate(root, actorpack, "--jobs", "1")
    assert result.exit_code == 1
    failed = [r for r in caplog.records if r.getMessage().startswith("FAILED")]
    assert len(failed) == 1
