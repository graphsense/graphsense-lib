"""``convert()``: to=gs / to=graphml, with and without layout=auto, against
the real :class:`RestBackend` on a mocked REST API."""

from __future__ import annotations

import io

import httpx
import networkx as nx
import pytest

from graphsenselib.convert.gs_files import decode_gs_bytes, structure
from graphsenselib.convert.graph_convert import (
    FORMATS,
    convert,
    graph_from_payload,
    networks,
)
from graphsenselib.pathfinder import RestBackend

from .test_graphml import A, B, BODIES, E1, ETH_TOKEN_TX, SWAP_OUT_TX, _payload


TAG_SUMMARIES = {
    ("btc", B): {
        "broad_category": "exchange",
        "tag_count": 2,
        "best_label": "some exchange",
        "best_actor": "someexchange",
        "label_summary": {
            "some exchange": {
                "label": "some exchange",
                "count": 2,
                "confidence": 0.8,
                "relevance": 1.0,
                "creators": ["test"],
                "sources": ["test"],
                "concepts": ["exchange", "hot_wallet"],
                "lastmod": 1700000000,
            }
        },
        "concept_tag_cloud": {},
    },
    ("btc", A): {"broad_category": "unknown", "tag_count": 0, "label_summary": {}},
}


def _handler(request: httpx.Request) -> httpx.Response:
    parts = request.url.path.strip("/").split("/")
    network, kind, tx_id, *rest = parts
    if kind == "addresses":
        if tx_id == E1.lower():
            return httpx.Response(500, json={"detail": "boom"})
        summary = TAG_SUMMARIES.get((network, tx_id))
        return (
            httpx.Response(404)
            if summary is None
            else httpx.Response(200, json=summary)
        )
    if rest == ["conversions"]:
        if tx_id == ETH_TOKEN_TX.split("_")[0]:
            return httpx.Response(
                200,
                json=[
                    {
                        "conversion_type": "dex_swap",
                        "from_asset_transfer": ETH_TOKEN_TX,
                        "to_asset_transfer": SWAP_OUT_TX,
                    }
                ],
            )
        return httpx.Response(200, json=[])
    if (network, tx_id) not in BODIES:
        return httpx.Response(500, json={"detail": "boom"})
    body = BODIES[(network, tx_id)]
    return httpx.Response(404) if body is None else httpx.Response(200, json=body)


@pytest.fixture
async def backend():
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(_handler), base_url="http://test"
    ) as client:
        yield RestBackend(client)


def _positions(content: bytes) -> dict[str, tuple[float, float]]:
    data = structure(decode_gs_bytes(content))
    return {t.id.id: (t.x, t.y) for t in (*data.addresses, *data.txs)}


def test_formats() -> None:
    assert FORMATS == ("gs", "graphml")


async def test_gs_keep_round_trips_the_json_payload(backend) -> None:
    out = await convert(_payload(), backend, to="gs")
    assert out.filename == "export_test.gs"
    assert out.media_type == "application/octet-stream"
    assert graph_from_payload(out.content) == graph_from_payload(_payload())


async def test_gs_auto_lays_out_every_node_and_keeps_the_rest(backend) -> None:
    out = await convert(_payload(), backend, to="gs", layout="auto")
    before, after = graph_from_payload(_payload()), graph_from_payload(out.content)
    assert {t.id.id for t in after.txs} == {t.id.id for t in before.txs}
    assert after.agg_edges == before.agg_edges
    assert after.annotations == before.annotations
    # Every tx was at (5, 5) in the input; the layout spreads them out.
    assert len(set(_positions(out.content).values())) > 1


async def test_graphml_auto_carries_the_new_positions(backend) -> None:
    gs = await convert(_payload(), backend, to="gs", layout="auto")
    gml = await convert(_payload(), backend, to="graphml", layout="auto")
    g = nx.read_graphml(io.BytesIO(gml.content))
    xy = {
        g.nodes[n]["gs:v0:address"]: (g.nodes[n]["gs:v0:x"], g.nodes[n]["gs:v0:y"])
        for n in g.nodes
        if g.nodes[n]["gs:v0:kind"] == "address"
    }
    laid = _positions(gs.content)
    assert all(laid[a] == xy[a] for a in xy if a in laid)


async def test_graphml_fetches_each_tx_once_across_layout_and_export() -> None:
    calls: list[str] = []

    def counting(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        return _handler(request)

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(counting), base_url="http://test"
    ) as client:
        await convert(_payload(), RestBackend(client), to="graphml", layout="auto")
    tx_calls = [c for c in calls if not c.endswith("/conversions")]
    # The failing tx is not cached (it raised), so it may be asked twice.
    assert len(tx_calls) - len(set(tx_calls)) <= 2


async def test_graphml_without_backend_has_no_flow_edges() -> None:
    out = await convert(_payload(), None, to="graphml")
    assert out.graph is not None
    assert {e.kind for e in out.graph.edges} == {"agg"}
    assert "no backend: exported without flow edges or attribution" in out.warnings


async def test_rejects_unknown_format_and_tx_mode_for_gs(backend) -> None:
    with pytest.raises(ValueError, match="unknown format"):
        await convert(_payload(), backend, to="gexf")
    with pytest.raises(ValueError, match="not to gs"):
        await convert(_payload(), backend, to="gs", tx_mode="aggregate")
    with pytest.raises(ValueError, match="not to gs"):
        await convert(_payload(), backend, to="gs", include=["tags"])
    with pytest.raises(ValueError, match="unknown include"):
        await convert(_payload(), backend, include=["clusters"])


def test_rejects_old_graph_files() -> None:
    with pytest.raises(ValueError, match="only Pathfinder"):
        graph_from_payload(["1.0.0", [], [], []])


async def test_include_tags_adds_address_level_attribution(backend) -> None:
    out = await convert(_payload(), backend, include=["tags"])
    g = nx.read_graphml(io.BytesIO(out.content))
    b = g.nodes[networks.address_id("btc", B)]
    assert b["gs:v0:attribution.source"] == "graphsense-tagstore"
    assert b["gs:v0:attribution.label"] == "some exchange"
    assert b["gs:v0:attribution.actor"] == "someexchange"
    assert b["gs:v0:attribution.category"] == "exchange"
    assert b["gs:v0:attribution.confidence"] == 0.8
    assert b["gs:v0:attribution.concepts"] == "exchange hot_wallet"
    assert b["gs:v0:attribution.tag_count"] == 2
    # No tags, unknown address: no attribution block at all.
    a = g.nodes[networks.address_id("btc", A)]
    assert not any(k.startswith("gs:v0:attribution.") for k in a)
    assert any("tag lookup failed for 1 address" in w for w in out.warnings)


async def test_tags_are_not_fetched_unless_included() -> None:
    paths: list[str] = []

    def recording(request: httpx.Request) -> httpx.Response:
        paths.append(request.url.path)
        return _handler(request)

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(recording), base_url="http://test"
    ) as client:
        await convert(_payload(), RestBackend(client))
    assert not any(p.endswith("/tag_summary") for p in paths)


async def test_aggregate_through_convert(backend) -> None:
    out = await convert(_payload(), backend, tx_mode="aggregate")
    g = nx.read_graphml(io.BytesIO(out.content))
    assert g.graph["gs:v0:tx_mode"] == "aggregate"
    assert {g.nodes[n]["gs:v0:kind"] for n in g.nodes} == {"address"}


def test_cli_convert(tmp_path, monkeypatch) -> None:
    import json as _json

    from click.testing import CliRunner

    import graphsenselib.pathfinder as pathfinder
    from graphsenselib.convert.gs_files.cli import gs_files_cli

    class _MockedRest(RestBackend):
        def __init__(self, _client) -> None:
            super().__init__(
                httpx.AsyncClient(
                    transport=httpx.MockTransport(_handler), base_url="http://test"
                )
            )

    monkeypatch.setattr(pathfinder, "RestBackend", _MockedRest)
    src = tmp_path / "graph.json"
    src.write_text(_json.dumps(_payload()))
    out = tmp_path / "out.graphml"
    result = CliRunner().invoke(
        gs_files_cli,
        [
            "convert",
            str(src),
            "-o",
            str(out),
            "--tx-mode",
            "aggregate",
            "--include",
            "tags",
            "--api-url",
            "http://test",
        ],
    )
    assert result.exit_code == 0, result.output
    g = nx.read_graphml(out)
    assert g.graph["gs:v0:tx_mode"] == "aggregate"
    assert "warning:" in result.output

    # .gs output, no API: named after the graph in the working directory.
    monkeypatch.chdir(tmp_path)
    for var in ("GRAPHSENSE_HOST", "IKNAIO_HOST", "GS_HOST"):
        monkeypatch.delenv(var, raising=False)
    result = CliRunner().invoke(gs_files_cli, ["convert", str(src), "--to", "gs"])
    assert result.exit_code == 0, result.output
    assert graph_from_payload((tmp_path / "export_test.gs").read_bytes()).name == (
        "export test"
    )
    result = CliRunner().invoke(
        gs_files_cli, ["convert", str(src), "--to", "gs", "--include", "tags"]
    )
    assert result.exit_code != 0
    assert "not to gs" in result.output
