"""Unit tests for the .gs file parser (no fixture files required)."""

from __future__ import annotations

import base64
import json
import struct

import pytest

from graphsenselib.convert.gs_files.parser import (
    Color,
    GraphData,
    PathfinderData,
    decode_gs_bytes,
    lzw_unpack,
    structure,
)


# ---------------------------------------------------------------------------
# LZW: reference encoder for round-trip tests (matches npm `lzwcompress`).
# ---------------------------------------------------------------------------


def _lzw_pack(text: str) -> list[int]:
    if not text:
        return []
    dictionary = {chr(i): i for i in range(256)}
    out: list[int] = []
    w = ""
    for c in text:
        wc = w + c
        if wc in dictionary:
            w = wc
        else:
            out.append(dictionary[w])
            dictionary[wc] = len(dictionary)
            w = c
    if w:
        out.append(dictionary[w])
    return out


def _build_gs_bytes(obj) -> bytes:
    b64 = base64.b64encode(json.dumps(obj).encode()).decode("ascii")
    codes = _lzw_pack(b64)
    return struct.pack(f"<{len(codes)}I", *codes)


# ---------------------------------------------------------------------------
# Stage 1
# ---------------------------------------------------------------------------


class TestLzw:
    def test_empty(self):
        assert lzw_unpack([]) == ""

    @pytest.mark.parametrize(
        "text",
        [
            "a",
            "abc",
            "aaaa",
            "TOBEORNOTTOBEORTOBEORNOT",
            "hello world, hello world",
            "the quick brown fox jumps over the lazy dog",
        ],
    )
    def test_roundtrip(self, text: str):
        assert lzw_unpack(_lzw_pack(text)) == text

    def test_invalid_code(self):
        with pytest.raises(ValueError, match="invalid LZW code"):
            lzw_unpack([65, 9999])


class TestDecodeGsBytes:
    def test_rejects_empty(self):
        with pytest.raises(ValueError, match="not a .gs file"):
            decode_gs_bytes(b"")

    def test_rejects_non_multiple_of_4(self):
        with pytest.raises(ValueError, match="not a .gs file"):
            decode_gs_bytes(b"\x00\x01\x02")

    def test_roundtrip_simple_payload(self):
        payload = ["1.0.0", [], [], []]
        assert decode_gs_bytes(_build_gs_bytes(payload)) == payload


# ---------------------------------------------------------------------------
# Color helpers
# ---------------------------------------------------------------------------


class TestColor:
    def test_from_rgba_array(self):
        c = Color.from_rgba_array([0.1, 0.2, 0.3, 0.4])
        assert (c.r, c.g, c.b, c.a) == (0.1, 0.2, 0.3, 0.4)

    def test_from_hex_rgb(self):
        c = Color.from_hex("#ff8040")
        assert c.r == pytest.approx(1.0)
        assert c.g == pytest.approx(128 / 255)
        assert c.b == pytest.approx(64 / 255)
        assert c.a == 1.0

    def test_from_hex_rgba(self):
        c = Color.from_hex("ff804080")
        assert c.a == pytest.approx(128 / 255)

    def test_from_hex_invalid(self):
        with pytest.raises(ValueError, match="bad color hex"):
            Color.from_hex("nope")


# ---------------------------------------------------------------------------
# Stage 2: structure() dispatch per version
# ---------------------------------------------------------------------------


class TestStructurePathfinder:
    def test_minimal(self):
        raw = ["pathfinder", "1", "my-graph", [], [], []]
        data = structure(raw)
        assert isinstance(data, PathfinderData)
        assert data.version == "1"
        assert data.name == "my-graph"
        assert data.addresses == data.txs == data.annotations == data.agg_edges == []

    def test_with_items(self):
        addr = [["btc", "abc"], 1.0, 2.0, True, 7]
        tx = [["btc", "xyz"], 3.0, 4.0, False]
        annot = [["btc", "abc"], "label", [0.1, 0.2, 0.3, 1.0]]
        agg = [["btc", "a"], ["btc", "b"], [["btc", "t1"], ["btc", "t2"]]]
        raw = ["pathfinder", "1", "g", [addr], [tx], [annot], [agg]]
        data = structure(raw)
        assert isinstance(data, PathfinderData)
        assert data.addresses[0].id.id == "abc"
        assert data.addresses[0].index == 7
        assert data.addresses[0].is_starting_point is True
        assert data.txs[0].is_starting_point is False
        assert data.annotations[0].label == "label"
        assert data.annotations[0].color is not None
        assert len(data.agg_edges[0].txs) == 2

    def test_unknown_version(self):
        with pytest.raises(ValueError, match="unknown pathfinder version"):
            structure(["pathfinder", "99"])


class TestStructureGraphV100:
    def test_minimal(self):
        raw = ["1.0.0", [], [], []]
        data = structure(raw)
        assert isinstance(data, GraphData)
        assert data.version == "1.0.0"
        assert data.addresses == data.entities == data.highlights == []

    def test_with_items(self):
        addr = [["btc", 0, "a1"], 10.0, 20.0, ["lab", "src", "cat", "ab"], None]
        ent = [["btc", 0, 42], "root", 5.0, 6.0, [0.1, 0.2, 0.3, 1.0], None]
        hl = ["title", [0.5, 0.5, 0.5, 1.0]]
        raw = ["1.0.0", [addr], [ent], [hl]]
        data = structure(raw)
        assert isinstance(data, GraphData)
        assert data.addresses[0].user_tag.label == "lab"
        assert data.entities[0].entity_id == 42
        assert data.entities[0].root_address == "root"
        assert data.entities[0].color is not None
        assert data.highlights[0].title == "title"


class TestStructureGraphOld:
    def test_v05_minimal(self):
        # raw = [version, tagsPair, layers, highlightsWrap]
        raw = ["0.5.2", [[], []], [[], [], [], [], [], []], [None, None, []]]
        data = structure(raw)
        assert isinstance(data, GraphData)
        assert data.version == "0.5.2"

    def test_v04_minimal(self):
        raw = ["0.4.5", [[], []], [[], [], [], [], [], []]]
        data = structure(raw)
        assert isinstance(data, GraphData)
        assert data.version == "0.4.5"

    def test_v05_with_address_entity_highlight(self):
        addr = [["addr1", 0, "btc"], [1.0, 2.0, 0.5, 0.5, "#ff0000"]]
        ent = [[123, 0, "btc"], [3.0, 4.0, 0.0, 0.0, ["m1", "m2", "m3"], "#00ff00"]]
        hl = ["#abcdef", "hl-title"]
        layers = [[], [], [], [], [ent], [addr]]
        raw = ["0.5.1", [[], []], layers, [None, None, [hl]]]
        data = structure(raw)
        assert data.addresses[0].x == pytest.approx(1.5)
        assert data.addresses[0].color.r == pytest.approx(1.0)
        assert data.entities[0].no_addresses == 3
        assert data.highlights[0].title == "hl-title"


class TestStructureErrors:
    def test_empty_payload(self):
        with pytest.raises(ValueError, match="unexpected payload"):
            structure([])

    def test_not_a_list(self):
        with pytest.raises(ValueError, match="unexpected payload"):
            structure({"not": "a list"})

    def test_non_string_version(self):
        with pytest.raises(ValueError, match="unexpected version marker"):
            structure([123, []])

    def test_unknown_graph_version(self):
        with pytest.raises(ValueError, match="unknown graph version"):
            structure(["9.9.9", [], [], []])
