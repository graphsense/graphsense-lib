"""Tests for the .gs file encoder and round-trip equivalence."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from graphsenselib.convert.gs_files import (
    GsBuilder,
    PathfinderData,
    builder_from_spec,
    decode_gs_bytes,
    encode_gs_payload,
    lzw_pack,
    lzw_unpack,
    normalize_address_id,
    normalize_tx_id,
    structure,
)
from graphsenselib.convert.gs_files.encoder import (
    _LABEL_LINE_HEIGHT,
    _label_line_count,
)

PAYJOIN_FIXTURE = (
    Path(__file__).parent.parent.parent
    / "testfiles"
    / "gs_files"
    / "payjoin_real_ids.json"
)


# ---------------------------------------------------------------------------
# LZW pack/unpack inverse property
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "text",
    [
        "",
        "a",
        "abc",
        "TOBEORNOTTOBEORTOBEORNOT",  # canonical LZW example
        "aaaaaaaaaaaaaa",  # repeats — exercises the dict_size == k branch
        "ABABABABABABABABABABAB",
        "the quick brown fox jumps over the lazy dog 1234567890",
    ],
)
def test_lzw_roundtrip(text: str) -> None:
    assert lzw_unpack(lzw_pack(text)) == text


# ---------------------------------------------------------------------------
# Payload-level round-trip (encode_gs_payload <-> decode_gs_bytes)
# ---------------------------------------------------------------------------


def test_encode_decode_payload_roundtrip() -> None:
    payload = ["pathfinder", "1", "tiny", [], [], [], []]
    buf = encode_gs_payload(payload)
    assert isinstance(buf, bytes)
    assert len(buf) > 0 and len(buf) % 4 == 0
    assert decode_gs_bytes(buf) == payload


def test_encode_unicode_safe() -> None:
    # The dashboard JSON.stringifies; non-ASCII names round-trip through
    # base64+UTF-8.
    payload = ["pathfinder", "1", "münze ✨", [], [], [], []]
    assert decode_gs_bytes(encode_gs_payload(payload)) == payload


# ---------------------------------------------------------------------------
# Builder smoke + structure preservation
# ---------------------------------------------------------------------------


def _sample_builder() -> GsBuilder:
    return (
        GsBuilder(name="selftest", default_network="btc")
        .add_address(
            "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh",
            label="genesis-ish",
            color=(0.9, 0.1, 0.1, 1.0),
            starting_point=True,
        )
        .add_address("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa")
        .add_tx(
            "4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b",
            index=0,
            label="block 0 coinbase",
        )
        .add_agg_edge(
            "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh",
            "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa",
            tx_ids=["4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b"],
        )
    )


def test_builder_to_payload_shape() -> None:
    raw = _sample_builder().to_payload()
    assert raw[0] == "pathfinder"
    assert raw[1] == "1"
    assert raw[2] == "selftest"
    assert len(raw[3]) == 2  # addresses
    assert len(raw[4]) == 1  # txs
    assert len(raw[5]) == 2  # 1 labelled addr + 1 labelled tx
    assert len(raw[6]) == 1  # agg edges
    assert raw[3][0][0] == ["btc", "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh"]
    assert raw[3][0][3] is True  # is_starting_point
    assert raw[4][0][4] == 0  # tx index


def test_builder_decodes_to_pathfinder_data() -> None:
    """Encoding via the builder and then running the decoder produces a
    PathfinderData with the same counts and node identifiers.
    """
    g = _sample_builder()
    raw = decode_gs_bytes(g.to_bytes())
    data = structure(raw)
    assert isinstance(data, PathfinderData)
    assert data.version == "1"
    assert data.name == "selftest"
    assert len(data.addresses) == 2
    assert len(data.txs) == 1
    assert len(data.agg_edges) == 1
    assert data.addresses[0].id.id == "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh"
    assert data.addresses[0].is_starting_point is True
    assert data.txs[0].id.id.startswith("4a5e1e4b")


def test_builder_side_aware_columns() -> None:
    """Side-aware addresses go into the input/output columns; default-side
    addresses fall back to the address column."""
    g = (
        GsBuilder(default_network="btc")
        .add_address("a1", side="input")
        .add_address("a2", side="input")
        .add_address("b1", side="output")
        .add_address("c1")  # default side
    )
    payload = g.to_payload()
    addrs = payload[3]
    # inputs share the input X, outputs share the output X
    assert addrs[0][1] == addrs[1][1] == GsBuilder._INPUT_COL_X
    assert addrs[2][1] == GsBuilder._OUTPUT_COL_X
    assert addrs[3][1] == GsBuilder._ADDR_COL_X
    # Y advances per column independently
    assert addrs[1][2] - addrs[0][2] == GsBuilder._ROW
    assert addrs[3][2] == 0.0


def test_builder_explicit_xy_overrides_layout() -> None:
    g = GsBuilder().add_address("only", x=1.5, y=-2.5)
    a = g.to_payload()[3][0]
    assert a[1] == 1.5 and a[2] == -2.5


def test_label_line_count() -> None:
    """Labels wrap at ~12 chars per line; explicit newlines are honoured."""
    assert _label_line_count(None) == 1
    assert _label_line_count("") == 1
    assert _label_line_count("a" * 12) == 1
    assert _label_line_count("a" * 13) == 2
    assert _label_line_count("a" * 25) == 3
    assert _label_line_count("two\nlines") == 2


def test_multiline_label_widens_columnar_spacing() -> None:
    """A multi-line label widens the whole column's uniform row step, so
    the column stays evenly aligned rather than ragged."""
    plain = (
        GsBuilder().add_address("a").add_address("b").add_address("c").to_payload()[3]
    )
    assert [row[2] for row in plain] == [0.0, GsBuilder._ROW, 2 * GsBuilder._ROW]

    wide_step = GsBuilder._ROW + _LABEL_LINE_HEIGHT
    wide = (
        GsBuilder()
        .add_address("a", label="victim deposit addr")  # 19 chars -> 2 lines
        .add_address("b")
        .add_address("c")
        .to_payload()[3]
    )
    # Every row uses the same widened step -> the column stays aligned.
    assert [row[2] for row in wide] == [0.0, wide_step, 2 * wide_step]


def test_write_creates_file(tmp_path: Path) -> None:
    out = tmp_path / "tiny.gs"
    g = GsBuilder(name="t").add_address("a")
    written = g.write(out)
    assert written == out
    assert out.exists() and out.stat().st_size > 0
    # Round-trips through the parser
    structure(decode_gs_bytes(out.read_bytes()))


# ---------------------------------------------------------------------------
# Identifier canonicalization + duplicate merging
#
# Regression for the 2026-06-10 report: a .gs file carried the same ETH
# address twice (checksummed + lowercase), rendering as two nodes in the
# pathfinder UI. GraphSense stores EVM hex ids lowercase; EIP-55 casing
# is display-only.
# ---------------------------------------------------------------------------

CHECKSUMMED = "0x4e1773615dFc62A5dDc901b36223F1eAedB8F6Df"
LOWERCASE = CHECKSUMMED.lower()
ETH_TX = "0xAB" + "cd" * 31  # 0x + 64 hex chars, mixed case


def test_normalize_address_id() -> None:
    assert normalize_address_id(CHECKSUMMED) == LOWERCASE
    assert normalize_address_id(LOWERCASE) == LOWERCASE
    # Non-EVM ids pass through untouched (base58 is case-sensitive).
    assert (
        normalize_address_id("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa")
        == "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa"
    )
    assert (
        normalize_address_id("TN3W4H6rK2ce4vX9YnFQHwKENnHjoxb3m9")
        == "TN3W4H6rK2ce4vX9YnFQHwKENnHjoxb3m9"
    )


def test_normalize_tx_id() -> None:
    assert normalize_tx_id(ETH_TX) == ETH_TX.lower()
    bare = "4A5E1E4BAAB89F3A32518A88C31BC87F618F76673E2CC77AB2127B7AFDEDA33B"
    assert normalize_tx_id(bare) == bare.lower()
    # Account-model sub-payment suffix keeps its uppercase marker.
    sub = ETH_TX + "_T3"
    assert normalize_tx_id(sub) == ETH_TX.lower() + "_T3"
    assert normalize_tx_id(ETH_TX + "_I948") == ETH_TX.lower() + "_I948"
    # Anything else passes through.
    assert normalize_tx_id("not-a-hash") == "not-a-hash"


def test_builder_lowercases_evm_address() -> None:
    payload = GsBuilder(default_network="eth").add_address(CHECKSUMMED).to_payload()
    assert payload[3][0][0] == ["eth", LOWERCASE]


def test_builder_merges_case_variant_addresses() -> None:
    g = (
        GsBuilder(default_network="eth")
        .add_address(CHECKSUMMED, starting_point=True)
        .add_address(LOWERCASE, label="Subject")
    )
    payload = g.to_payload()
    assert len(payload[3]) == 1
    addr = payload[3][0]
    assert addr[0] == ["eth", LOWERCASE]
    assert addr[3] is True  # starting_point survives the merge
    # Label from the duplicate is folded into the surviving node.
    assert payload[5] == [[["eth", LOWERCASE], "Subject", None]]


def test_builder_merges_duplicate_txs_and_edges() -> None:
    g = (
        GsBuilder(default_network="eth")
        .add_tx(ETH_TX, label="hop 1")
        .add_tx(ETH_TX.lower())
        .add_agg_edge(CHECKSUMMED, "0x" + "ab" * 20, tx_ids=[ETH_TX])
        .add_agg_edge(LOWERCASE, "0x" + "AB" * 20, tx_ids=[ETH_TX.lower()])
    )
    payload = g.to_payload()
    assert len(payload[4]) == 1  # one tx node
    assert payload[4][0][0] == ["eth", ETH_TX.lower()]
    assert len(payload[6]) == 1  # one edge, tx_ids deduped
    assert payload[6][0] == [
        ["eth", LOWERCASE],
        ["eth", "0x" + "ab" * 20],
        [["eth", ETH_TX.lower()]],
    ]


def test_builder_same_id_on_different_networks_not_merged() -> None:
    g = (
        GsBuilder(default_network="eth")
        .add_address(LOWERCASE)
        .add_address(LOWERCASE, network="trx")
    )
    assert len(g.to_payload()[3]) == 2


def test_builder_from_spec_merges_case_variants() -> None:
    spec = {
        "addresses": [
            {"id": CHECKSUMMED, "starting_point": True},
            {"id": LOWERCASE, "label": "Subject"},
        ],
    }
    data = structure(
        decode_gs_bytes(builder_from_spec(spec, default_network="eth").to_bytes())
    )
    assert isinstance(data, PathfinderData)
    assert [a.id.id for a in data.addresses] == [LOWERCASE]


# ---------------------------------------------------------------------------
# builder_from_spec — used by the CLI
# ---------------------------------------------------------------------------


def test_builder_from_spec_minimal_strings() -> None:
    spec = {
        "addresses": ["a1"],
        "txs": ["t1"],
        "agg_edges": [{"a": "a1", "b": "a2"}],
    }
    g = builder_from_spec(spec, name="x", default_network="btc")
    payload = g.to_payload()
    assert payload[2] == "x"
    assert payload[3][0][0] == ["btc", "a1"]
    assert payload[4][0][0] == ["btc", "t1"]
    assert payload[6][0][0] == ["btc", "a1"]
    assert payload[6][0][1] == ["btc", "a2"]


def test_builder_from_spec_rich_objects() -> None:
    spec = {
        "addresses": [
            {
                "id": "a1",
                "label": "exch",
                "color": [1, 0.5, 0, 1],
                "starting_point": True,
                "side": "input",
                "network": "ltc",
            }
        ],
        "txs": [{"id": "t1", "index": 7, "label": "spike"}],
        "agg_edges": [{"a": "a1", "b": "a2", "tx_ids": ["t1"]}],
    }
    payload = builder_from_spec(spec).to_payload()
    addr = payload[3][0]
    assert addr[0] == ["ltc", "a1"]
    assert addr[3] is True  # starting point
    annotations = payload[5]
    # 1 labelled+coloured addr + 1 labelled tx
    assert len(annotations) == 2
    addr_ann = next(a for a in annotations if a[0] == ["ltc", "a1"])
    assert addr_ann[1] == "exch"
    assert addr_ann[2] == [1.0, 0.5, 0.0, 1.0]


def test_builder_from_spec_rejects_bad_color() -> None:
    with pytest.raises(ValueError, match="color"):
        builder_from_spec({"addresses": [{"id": "a", "color": [1, 0, 0]}]})


# ---------------------------------------------------------------------------
# Real-fixture regression: PayJoin demo (block 557792, tx 7104bae6...)
#
# The fixture mirrors `scripts/payjoin_showcase_gs.py` — same INPUTS,
# OUTPUTS, PAYJOIN_TX, and UPSTREAM_HASHES tables. Locking the encoded
# layout here so encoder refactors can't silently break the showcase
# and the .gs file imports cleanly into the Pathfinder dashboard.
# ---------------------------------------------------------------------------


PAYJOIN_TX = "7104bae698587b3e75563b7ea7a9aada41d9c787788bc2bf26dd201fd7eca8a2"

PAYJOIN_INPUTS = [
    "38CNad4Jf6daL7Xki36S2uvWDrZr6hDKir",  # in0
    "3FWi8EByrUXaH1DMNe2T9aGRrCsP6Y9AwD",  # in1
    "366LLyvYr2Jm652q7kt1Wiq3QD1vQYbtQu",  # in2
    "3MSyNbmsee6c3iwEKJrRnMiwJ4jswAeJHc",  # in3 (sender)
    "38J7d7X92QiMVkN5Qk8Mu2f7ti9dZdFUgv",  # in4 (sender)
]

PAYJOIN_OUTPUTS = [
    "38vbyF9hyvB7bbibuey6BDUddEAggokYLu",  # out0 — sender change
    "3QLPbQnwPk6fnGy1t8Sj3R4pGSPBeirzNR",  # out1 — receiver payment
]

PAYJOIN_UPSTREAM_HASHES = [
    "86ca0da39f87016194549421a75ab75ae75f1b6dccfc3db9a2a1475175a87767",  # in0
    "9e1bda94536c568f1572d75c2e8c57792848ebf08a2d4e13f7ebc1a95c875ea1",  # in1
    "ee0c170a57b9b30553ec54d560afe8074a3b268fbf3241f00c03e4b84ccc2169",  # in2
    "db8b8ebd86ecb581cd5f0656a2f67b7210937e786f1cbb232ad656b23cf6b17e",  # in3 (JM)
    "2274ddc97d87d64d2a01ed918c246cfa03c61d5ba3e24f572a6a317468a357c5",  # in4 (JM)
]


@pytest.fixture
def payjoin_spec() -> dict:
    """Load the PayJoin demo fixture spec from disk."""
    return json.loads(PAYJOIN_FIXTURE.read_text(encoding="utf-8"))


def test_payjoin_fixture_present() -> None:
    assert PAYJOIN_FIXTURE.exists(), (
        f"missing fixture: {PAYJOIN_FIXTURE}. "
        "It pins the showcase's ground-truth IDs alongside the encoder."
    )


def test_payjoin_fixture_ids_match_showcase(payjoin_spec: dict) -> None:
    """The fixture's IDs must stay aligned with payjoin_showcase_gs.py.

    If this drifts, regenerate the fixture from INPUTS / OUTPUTS /
    PAYJOIN_TX / UPSTREAM_HASHES in the showcase script.
    """
    addr_ids = [a["id"] for a in payjoin_spec["addresses"]]
    tx_ids = [t["id"] for t in payjoin_spec["txs"]]
    assert addr_ids == [*PAYJOIN_INPUTS, *PAYJOIN_OUTPUTS]
    assert tx_ids == [*PAYJOIN_UPSTREAM_HASHES, PAYJOIN_TX]


def test_payjoin_encodes_and_decodes(payjoin_spec: dict) -> None:
    """Build via spec, encode to .gs bytes, decode, structure — counts
    and IDs survive the full pipeline.
    """
    builder = builder_from_spec(payjoin_spec, name="payjoin", default_network="btc")
    data = structure(decode_gs_bytes(builder.to_bytes()))

    assert isinstance(data, PathfinderData)
    assert data.version == "1"
    assert data.name == "payjoin"
    assert len(data.addresses) == 7
    assert len(data.txs) == 6  # 5 upstream + 1 PayJoin
    assert len(data.agg_edges) == 10  # 5 inputs × 2 outputs

    decoded_addrs = {a.id.id for a in data.addresses}
    assert decoded_addrs == set(PAYJOIN_INPUTS) | set(PAYJOIN_OUTPUTS)

    decoded_txs = {t.id.id for t in data.txs}
    assert decoded_txs == {*PAYJOIN_UPSTREAM_HASHES, PAYJOIN_TX}


def test_payjoin_layout_pins(payjoin_spec: dict) -> None:
    """Lock the four-column layout that the dashboard renders against.

    upstream txs (x=-14) → input addrs (x=-8) → PayJoin (x=0) → outputs (x=8)
    """
    data = structure(decode_gs_bytes(builder_from_spec(payjoin_spec).to_bytes()))

    upstream = [t for t in data.txs if t.id.id != PAYJOIN_TX]
    payjoin = next(t for t in data.txs if t.id.id == PAYJOIN_TX)

    assert all(t.x == -14.0 for t in upstream), "upstream txs must share x=-14"
    assert [t.y for t in upstream] == [0, 3, 6, 9, 12], "upstream Y stack"
    assert (payjoin.x, payjoin.y) == (0.0, 6.0), "PayJoin tx centred"

    inputs = [a for a in data.addresses if a.id.id in PAYJOIN_INPUTS]
    outputs = [a for a in data.addresses if a.id.id in PAYJOIN_OUTPUTS]
    assert all(a.x == GsBuilder._INPUT_COL_X for a in inputs)
    assert all(a.x == GsBuilder._OUTPUT_COL_X for a in outputs)


def test_payjoin_edges_are_input_output_cross_product(payjoin_spec: dict) -> None:
    """Every input-output pair gets one agg_edge carrying the PayJoin tx."""
    data = structure(decode_gs_bytes(builder_from_spec(payjoin_spec).to_bytes()))

    edges = {(e.a.id, e.b.id) for e in data.agg_edges}
    expected = {(i, o) for i in PAYJOIN_INPUTS for o in PAYJOIN_OUTPUTS}
    assert edges == expected

    for e in data.agg_edges:
        assert [t.id for t in e.txs] == [PAYJOIN_TX]


def test_payjoin_fixture_via_cli(payjoin_spec: dict, tmp_path: Path) -> None:
    """CLI path: `gs-files encode -i fixture.json` produces a decodable
    .gs whose summary matches the expected counts.
    """
    from click.testing import CliRunner

    from graphsenselib.convert.gs_files.cli import gs_files_cli

    out_path = tmp_path / "payjoin.gs"
    runner = CliRunner()
    result = runner.invoke(
        gs_files_cli,
        [
            "encode",
            "-i",
            str(PAYJOIN_FIXTURE),
            "-o",
            str(out_path),
            "--name",
            "payjoin-cli",
            "--verify",
        ],
    )
    assert result.exit_code == 0, result.output
    assert "verify ok" in result.output
    assert out_path.exists() and out_path.stat().st_size > 0

    data = structure(decode_gs_bytes(out_path.read_bytes()))
    assert isinstance(data, PathfinderData)
    assert data.name == "payjoin-cli"
    assert len(data.addresses) == 7
    assert len(data.txs) == 6
    assert len(data.agg_edges) == 10
