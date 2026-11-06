"""POST /graph/convert on a minimal app: the graph router plus fake tx
routes, so the in-process lookups run without a database. (Kept out of
tests/web, whose fixtures start the testcontainers.)"""

from __future__ import annotations

import io
import json

import httpx
import networkx as nx
import pytest
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from graphsenselib.convert.gs_files import GsBuilder, decode_gs_bytes, structure
from graphsenselib.convert.graph_convert import FORMATS
from graphsenselib.errors import BadUserInputException
from graphsenselib.web.routes import graph as graph_routes

from .test_graphml import BODIES, _payload


def _app(seen_headers: list[dict]) -> FastAPI:
    app = FastAPI()
    app.include_router(graph_routes.router)

    @app.exception_handler(BadUserInputException)
    async def _bad_input(request: Request, exc: BadUserInputException):
        return JSONResponse(status_code=400, content={"detail": exc.get_user_msg()})

    @app.get("/{network}/txs/{tx_id}/conversions")
    async def _conversions(network: str, tx_id: str):
        return []

    @app.get("/{network}/txs/{tx_id}")
    async def _tx(network: str, tx_id: str, request: Request):
        seen_headers.append(dict(request.headers))
        if (network, tx_id) not in BODIES:
            return JSONResponse(status_code=500, content={"detail": "boom"})
        body = BODIES[(network, tx_id)]
        if body is None:
            return JSONResponse(status_code=404, content={"detail": "not found"})
        return body

    return app


@pytest.fixture
def seen() -> list[dict]:
    return []


@pytest.fixture
async def client(seen):
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=_app(seen)), base_url="http://test"
    ) as c:
        yield c


def test_enums_match_the_library() -> None:
    from graphsenselib.convert.graph_convert import TX_MODES

    assert tuple(f.value for f in graph_routes.ConvertFormat) == FORMATS
    assert tuple(m.value for m in graph_routes.ConvertTxMode) == TX_MODES


async def test_json_payload_to_graphml(client, seen) -> None:
    r = await client.post(
        "/graph/convert?to=graphml",
        json=_payload(),
        headers={"x-consumer-username": "alice"},
    )
    assert r.status_code == 200, r.text
    assert r.headers["content-type"] == "application/graphml+xml"
    assert r.headers["content-disposition"] == (
        'attachment; filename="export_test.graphml"'
    )
    assert int(r.headers["x-convert-warnings"]) >= 2  # unknown + failing tx
    g = nx.read_graphml(io.BytesIO(r.content))
    assert g.graph["gs:v0:name"] == "export test"
    # The caller's identity reaches the lookups; its body headers don't.
    assert seen and all(h.get("x-consumer-username") == "alice" for h in seen)
    assert all("content-length" not in h for h in seen)


async def test_gs_bytes_to_gs_with_auto_layout(client) -> None:
    gs = GsBuilder(name="raw file")
    gs.add_address("1AAAinputAddress", network="btc", x=0, y=0)
    r = await client.post(
        "/graph/convert?to=gs&layout=auto",
        content=gs.to_bytes(),
        headers={"content-type": "application/octet-stream"},
    )
    assert r.status_code == 200, r.text
    assert r.headers["content-disposition"] == 'attachment; filename="raw_file.gs"'
    assert structure(decode_gs_bytes(r.content)).name == "raw file"


@pytest.mark.parametrize(
    "body,content_type,query,status",
    [
        (b"\x01\x02\x03", "application/octet-stream", "", 400),  # not a .gs
        (b"{not json", "application/json", "", 400),
        (json.dumps({"a": 1}).encode(), "application/json", "", 400),
        (json.dumps(["1.0.0", [], [], []]).encode(), "application/json", "", 400),
        (json.dumps(_payload()).encode(), "application/json", "?to=gexf", 422),
        (
            json.dumps(_payload()).encode(),
            "application/json",
            "?tx_mode=hyperedge",
            422,
        ),
        (
            json.dumps(_payload()).encode(),
            "application/json",
            "?to=gs&tx_mode=node",
            400,
        ),
        (json.dumps(_payload()).encode(), "application/json", "?include=x", 400),
        (
            json.dumps(_payload()).encode(),
            "application/json",
            "?to=gs&include=tags",
            400,
        ),
    ],
)
async def test_rejects_bad_requests(client, body, content_type, query, status) -> None:
    r = await client.post(
        f"/graph/convert{query}", content=body, headers={"content-type": content_type}
    )
    assert r.status_code == status, r.text


async def test_size_and_node_caps(client, monkeypatch) -> None:
    monkeypatch.setattr(graph_routes, "MAX_CONVERT_BYTES", 10)
    r = await client.post("/graph/convert", json=_payload())
    assert r.status_code == 413
    monkeypatch.setattr(graph_routes, "MAX_CONVERT_BYTES", 10**6)
    monkeypatch.setattr(graph_routes, "MAX_CONVERT_NODES", 3)
    r = await client.post("/graph/convert", json=_payload())
    assert r.status_code == 400
    assert "at most 3" in r.text


def test_body_example_is_a_valid_pathfinder_graph() -> None:
    from graphsenselib.convert.graph_convert import graph_from_payload

    data = graph_from_payload(graph_routes.CONVERT_BODY_EXAMPLE)
    assert len(data.addresses) == 2 and len(data.txs) == 1
    assert len(data.agg_edges) == 1


async def test_aggregate_with_tags(client) -> None:
    r = await client.post(
        "/graph/convert?tx_mode=aggregate&include=tags", json=_payload()
    )
    assert r.status_code == 200, r.text
    g = nx.read_graphml(io.BytesIO(r.content))
    assert g.graph["gs:v0:tx_mode"] == "aggregate"
