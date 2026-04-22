"""Pinned regression tests for tests/testfiles/gs_files/v25_09_examples.gs.

This file is a real Pathfinder v1 dashboard save. The assertions lock the
decoded shape so future parser refactors don't silently change behavior.
"""

from __future__ import annotations

import json
from collections import Counter
from pathlib import Path

import pytest
from click.testing import CliRunner

from graphsenselib.convert.gs_files import (
    PathfinderData,
    decode_gs,
    structure,
    summarize,
    to_jsonable,
)
from graphsenselib.convert.gs_files.cli import gs_files_cli

FIXTURE = Path(__file__).parent.parent.parent / "testfiles" / "gs_files" / "example1.gs"

pytestmark = pytest.mark.skipif(
    not FIXTURE.exists(), reason="example1.gs fixture missing"
)


@pytest.fixture(scope="module")
def raw():
    return decode_gs(FIXTURE)


@pytest.fixture(scope="module")
def data(raw) -> PathfinderData:
    result = structure(raw)
    assert isinstance(result, PathfinderData)
    return result


class TestRawShape:
    def test_discriminator(self, raw):
        assert raw[0] == "pathfinder"
        assert raw[1] == "1"
        assert raw[2] == "graph"

    def test_raw_list_lengths(self, raw):
        # [kind, version, name, addrs, txs, annots, aggEdges]
        assert len(raw[3]) == 14
        assert len(raw[4]) == 13
        assert len(raw[5]) == 1
        assert len(raw[6]) == 12


class TestStructured:
    def test_identity(self, data: PathfinderData):
        assert data.kind == "pathfinder"
        assert data.version == "1"
        assert data.name == "graph"

    def test_counts(self, data: PathfinderData):
        assert len(data.addresses) == 14
        assert len(data.txs) == 13
        assert len(data.annotations) == 1
        assert len(data.agg_edges) == 12

    def test_currencies_present(self, data: PathfinderData):
        currencies = Counter(a.id.currency for a in data.addresses)
        # Sanity — the fixture spans BTC and ETH.
        assert "btc" in currencies
        assert "eth" in currencies
        assert sum(currencies.values()) == 14

    def test_address_ids_unique(self, data: PathfinderData):
        ids = [(a.id.currency, a.id.id) for a in data.addresses]
        assert len(ids) == len(set(ids)), "duplicate (currency, id) pairs"

    def test_tx_ids_unique(self, data: PathfinderData):
        ids = [(t.id.currency, t.id.id) for t in data.txs]
        assert len(ids) == len(set(ids))

    def test_agg_edges_reference_known_addresses(self, data: PathfinderData):
        known = {(a.id.currency, a.id.id) for a in data.addresses}
        for edge in data.agg_edges:
            assert (edge.a.currency, edge.a.id) in known
            assert (edge.b.currency, edge.b.id) in known

    def test_agg_edge_tx_refs_known(self, data: PathfinderData):
        known_txs = {(t.id.currency, t.id.id) for t in data.txs}
        for edge in data.agg_edges:
            for tx in edge.txs:
                assert (tx.currency, tx.id) in known_txs

    def test_coordinates_are_floats(self, data: PathfinderData):
        for a in data.addresses:
            assert isinstance(a.x, float)
            assert isinstance(a.y, float)


class TestSummaryOnFixture:
    def test_expected_summary(self, data: PathfinderData):
        assert summarize(data) == {
            "kind": "pathfinder",
            "version": "1",
            "name": "graph",
            "n_addresses": 14,
            "n_txs": 13,
            "n_annotations": 1,
            "n_agg_edges": 12,
        }


class TestSerializationRoundtrip:
    def test_jsonable_is_serializable(self, data: PathfinderData):
        # Ensures no dataclass / tuple leaks through.
        text = json.dumps(to_jsonable(data))
        reparsed = json.loads(text)
        assert reparsed["kind"] == "pathfinder"
        assert len(reparsed["addresses"]) == 14


class TestCliOnFixture:
    def test_summary_cli(self):
        runner = CliRunner()
        result = runner.invoke(gs_files_cli, ["summary", str(FIXTURE)])
        assert result.exit_code == 0, result.output
        assert json.loads(result.output) == {
            "kind": "pathfinder",
            "version": "1",
            "name": "graph",
            "n_addresses": 14,
            "n_txs": 13,
            "n_annotations": 1,
            "n_agg_edges": 12,
        }

    def test_decode_both_writes_files(self, tmp_path: Path):
        runner = CliRunner()
        out = tmp_path / "out.json"
        result = runner.invoke(
            gs_files_cli,
            ["decode", "--format", "both", "-o", str(out), str(FIXTURE)],
        )
        assert result.exit_code == 0, result.output
        raw_path = tmp_path / "out.raw.json"
        structured_path = tmp_path / "out.structured.json"
        assert raw_path.exists() and structured_path.exists()

        raw_reparsed = json.loads(raw_path.read_text())
        structured_reparsed = json.loads(structured_path.read_text())
        assert raw_reparsed[0] == "pathfinder"
        assert structured_reparsed["kind"] == "pathfinder"
        assert len(structured_reparsed["addresses"]) == 14

    def test_decode_compact_indent(self, tmp_path: Path):
        runner = CliRunner()
        out = tmp_path / "compact.json"
        result = runner.invoke(
            gs_files_cli,
            [
                "decode",
                "--format",
                "structured",
                "--indent",
                "0",
                "-o",
                str(out),
                str(FIXTURE),
            ],
        )
        assert result.exit_code == 0, result.output
        text = out.read_text()
        assert "\n" not in text.strip()  # compact single line
        assert json.loads(text)["kind"] == "pathfinder"
