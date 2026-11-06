"""Graph API routes (currency-less).

A graph node set is a collection of transactions and/or addresses, each
carrying its own network, so a set may span chains. The summary is derived
from header fields only and works for every supported chain; the compare
endpoint (added separately) runs the BTC-only fingerprinting analysis.
"""

import json
from enum import Enum
from typing import Optional

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import Response

from graphsenselib.convert import graph_convert
from graphsenselib.errors import BadUserInputException
from graphsenselib.pathfinder import RestBackend

from graphsenselib.db.asynchronous.services.models import MAX_GRAPH_NODES
from graphsenselib.web.service import ServiceContext
from graphsenselib.web.models import (
    GraphCompareRequest,
    GraphComparison,
    GraphSummary,
    GraphSummaryRequest,
)
from graphsenselib.web.routes.base import (
    PluginRoute,
    get_ctx,
    parse_comma_separated_strings,
)
import graphsenselib.web.service.graph_service as service

router = APIRouter(route_class=PluginRoute)


@router.post(
    "/graph/summary",
    summary="Summarize a set of transactions and/or addresses (beta)",
    description=(
        "**BETA**: this endpoint is new and its contract may still "
        "change without a deprecation cycle. "
        "Returns aggregate stats over the transactions and/or addresses in "
        "the request body. Every item carries its own network, so the set "
        "may span chains. Each node-type block holds a network-agnostic "
        "overall part (fiat totals per code, timestamp span) and one full "
        "per-network block (native base-unit values via the Values pattern) "
        "per network in the request. Each block is present iff the request "
        "carried that node type. Each non-empty list must hold at least 2 "
        f"distinct entries; together they may hold at most {MAX_GRAPH_NODES}. "
        "References are canonicalized before processing (tx hashes "
        "lowercased, 0x stripped; addresses network-canonicalized), and "
        "duplicates — including spelling variants of one node — are "
        "collapsed and counted once. Unknown references are dropped and "
        "reported per network in a nodes_not_found note (its items list "
        "carries the refs); the request only fails when fewer than 2 of a "
        "list's references exist. Value totals are gross: UTXO txs "
        "contribute their full output sum (change included), so sets "
        "containing linked txs (e.g. a peel chain) count the same coins "
        "once per hop."
    ),
    operation_id="graph_summary",
    response_model=GraphSummary,
    response_model_exclude_none=True,
    openapi_extra={"x-beta": True},
    responses={
        400: {
            "description": (
                "Invalid request. Causes: both lists empty; a non-empty list "
                "with fewer than 2 distinct entries; more than "
                f"{MAX_GRAPH_NODES} entries combined; an unsupported "
                "network; a sub-transaction identifier instead of a base "
                "tx hash."
            )
        },
        404: {
            "description": (
                "Fewer than 2 of a list's references exist (the message "
                "names the missing ones). Unknown references in an "
                "otherwise viable request do not 404 — they are dropped "
                "and reported in a nodes_not_found note."
            )
        },
    },
)
async def graph_summary(
    request: Request,
    body: GraphSummaryRequest,
    ctx: ServiceContext = Depends(get_ctx),
):
    """Aggregate stats over a set of transactions and/or addresses."""
    return await service.summary(ctx, txs=body.txs, addresses=body.addresses)


@router.post(
    "/graph/compare",
    summary="Compare multiple transactions (beta)",
    description=(
        "**BETA**: this endpoint is new and its contract may still "
        "change without a deprecation cycle. "
        "Returns per-tx characteristics, pairwise similarity signals, and a "
        "rollup verdict on whether the supplied transactions are likely "
        "linked to the same actor. The fingerprinting analysis is BTC-only; "
        "every ref's network must be btc. For chain-agnostic aggregate "
        "stats over a node set use POST /graph/summary instead. Tx refs are "
        "canonicalized (hashes lowercased, 0x stripped) and duplicates "
        "collapsed; the response echoes the canonical hashes, and all "
        "positional references — signal per_tx entries and lineage "
        "from_idx/to_idx — index into the response's txs list, which may be "
        "shorter than the request's."
    ),
    operation_id="graph_compare",
    response_model=GraphComparison,
    response_model_exclude_none=True,
    openapi_extra={"x-beta": True},
    responses={
        400: {
            "description": (
                "Invalid request. Causes: fewer than 2 distinct tx refs; a "
                "non-BTC network; a sub-transaction identifier instead of "
                "a base tx hash; combined inputs/outputs above the "
                "comparison work limit."
            )
        },
        404: {
            "description": (
                "One or more transactions were not found; the message "
                "names every missing hash. The analysis is all-or-nothing "
                "— there is no partial comparison."
            )
        },
    },
)
async def graph_compare(
    request: Request,
    body: GraphCompareRequest,
    ctx: ServiceContext = Depends(get_ctx),
):
    """Compare two or more transactions (BTC-only fingerprinting analysis)."""
    return await service.compare(ctx, txs=body.txs, include=body.include)


# --------------------------------------------------------------- convert --

# A Pathfinder graph is converted with one tx lookup (plus one conversions
# lookup) per tx node, so the node cap bounds the fan-out per request.
MAX_CONVERT_BYTES = 4 * 1024 * 1024
MAX_CONVERT_NODES = 500


class ConvertFormat(str, Enum):
    """Target formats of POST /graph/convert; mirrors graph_convert.FORMATS."""

    gs = "gs"
    graphml = "graphml"


class ConvertTxMode(str, Enum):
    """Mirrors graph_convert.TX_MODES."""

    node = "node"
    aggregate = "aggregate"


class ConvertLayout(str, Enum):
    keep = "keep"
    auto = "auto"


# Request headers that describe this request's own body or connection and
# must not be copied onto the in-process lookups.
_NOT_FORWARDED = frozenset(
    {
        "host",
        "content-length",
        "content-type",
        "content-encoding",
        "transfer-encoding",
        "connection",
        "keep-alive",
        "accept",
        "accept-encoding",
        "expect",
        "upgrade",
    }
)


def _lookup_client(request: Request) -> httpx.AsyncClient:
    """An httpx client that sends the tx lookups to this app in-process.

    The caller's headers are forwarded: the app resolves tag access
    groups and private-tag visibility from them, so the lookups see what
    the caller may see (as the MCP tools do, mcp/tools/consolidated.py).
    """
    headers = {
        k: v for k, v in request.headers.items() if k.lower() not in _NOT_FORWARDED
    }
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=request.app),
        base_url="http://graphsense-internal",
        headers=headers,
        timeout=60.0,
    )


async def _read_payload(request: Request) -> bytes | list:
    body = await request.body()
    if len(body) > MAX_CONVERT_BYTES:
        raise HTTPException(
            status_code=413,
            detail=f"graph larger than {MAX_CONVERT_BYTES} bytes",
        )
    content_type = request.headers.get("content-type", "").split(";")[0].strip()
    if content_type == "application/json":
        try:
            payload = json.loads(body)
        except ValueError as exc:
            raise BadUserInputException(f"invalid JSON body: {exc}") from exc
        if not isinstance(payload, list):
            raise BadUserInputException(
                "JSON body must be the Pathfinder payload list "
                '["pathfinder", "1", name, addresses, txs, annotations, agg_edges]'
            )
        return payload
    return body


# Real on-chain data: the first bitcoin transfer (block 170), sender and
# receiver joined by an agg edge, the sender as starting point.
_FIRST_TX = "f4184fc596403b9d638783cf57adfe4c75c605f6356fbc91338530e9831e9e16"
_SENDER = "12cbQLTFMXRnSzktFkuoG3eHoMeFtpTu3S"
_RECEIVER = "1Q2TWHE3GMdB6BZKafqwxXtWAWgFt5Jvm3"
CONVERT_BODY_EXAMPLE = [
    "pathfinder",
    "1",
    "first bitcoin transfer",
    [
        [["btc", _SENDER], 0, 0, True, 0],
        [["btc", _RECEIVER], 8, 0, False, 0],
    ],
    [[["btc", _FIRST_TX], 4, 0, False, 0]],
    [],
    [[["btc", _SENDER], ["btc", _RECEIVER], [["btc", _FIRST_TX]], 0]],
]

_CONVERT_REQUEST_BODY = {
    "required": True,
    # The python client's doc snippet assigns this literal as the body
    # (templates/api_doc_example.mustache). Model bodies use
    # x-graphsense-python-body-example instead, see
    # _add_request_body_examples_for_generated_clients in web/app.py.
    "x-graphsense-python-body-literal": repr(CONVERT_BODY_EXAMPLE),
    "content": {
        "application/json": {
            "schema": {
                "type": "array",
                "items": {},
                "description": (
                    "The Pathfinder graph as the dashboard encodes it before "
                    'packing: ["pathfinder", "1", name, addresses, txs, '
                    "annotations, agg_edges]."
                ),
                "example": CONVERT_BODY_EXAMPLE,
            }
        },
        "application/octet-stream": {
            "schema": {
                "type": "string",
                "format": "binary",
                "description": "A .gs file as Pathfinder saves it.",
            }
        },
    },
}


@router.post(
    "/graph/convert",
    summary="Convert a Pathfinder graph to another format, or lay it out (beta)",
    description=(
        "**BETA**: this endpoint is new and its contract may still change "
        "without a deprecation cycle. "
        "Takes a Pathfinder graph, either a .gs file "
        "(application/octet-stream) or the JSON payload the dashboard "
        "encodes (application/json), and returns it in the format `to`. "
        "`to=graphml` writes a GraphML document: every transaction is a "
        "node, every input or output touching an address of the graph an "
        "edge with its value in base units; the attribute keys "
        "(`gs:v0:*`) are experimental. "
        "`tx_mode=aggregate` writes one edge per sender, receiver and asset "
        "instead (total value, tx count, first/last timestamp, the txs); its "
        "value is exact or absent, never estimated: a UTXO tx with several "
        "sender addresses does not say who paid whom. "
        "`include=tags` adds the address-level attribution (no cluster "
        "tags), one lookup per address. "
        "`to=gs` returns a .gs file. "
        "`layout=auto` places every node afresh first, with senders left of "
        "their transaction. Each transaction is looked up once; a lookup "
        "that fails leaves that transaction without flow edges or direction "
        "and is reported in the X-Convert-Warnings header (count) and, for "
        "graph formats, in the document. The response is the file itself, "
        "named after the graph. At most "
        f"{MAX_CONVERT_NODES} nodes and {MAX_CONVERT_BYTES} bytes."
    ),
    operation_id="convert_graph",
    response_class=Response,
    openapi_extra={"x-beta": True, "requestBody": _CONVERT_REQUEST_BODY},
    responses={
        200: {
            "description": "The converted graph, as a file download.",
            "content": {
                "application/graphml+xml": {
                    "schema": {"type": "string", "format": "binary"}
                },
                "application/octet-stream": {
                    "schema": {"type": "string", "format": "binary"}
                },
            },
        },
        400: {
            "description": (
                "The body is not a Pathfinder graph (undecodable .gs file, "
                "invalid JSON, or an old Graph file), an unknown include, "
                "tx_mode or include given with to=gs, or the graph has more "
                "than "
                f"{MAX_CONVERT_NODES} nodes."
            )
        },
        413: {"description": f"The body is larger than {MAX_CONVERT_BYTES} bytes."},
    },
)
async def convert_graph(
    request: Request,
    to: ConvertFormat = Query(
        ConvertFormat.graphml, description="Target format.", examples=["graphml"]
    ),
    tx_mode: ConvertTxMode = Query(
        ConvertTxMode.node,
        description=(
            "How transactions are written in graph formats: `node`, one "
            "node per transaction (lossless), or `aggregate`, one edge per "
            "sender, receiver and asset."
        ),
        examples=["node"],
    ),
    layout: ConvertLayout = Query(
        ConvertLayout.keep,
        description=(
            "`keep` the positions of the input, or `auto`: lay the graph out afresh."
        ),
        examples=["keep"],
    ),
    include: Optional[str] = Query(
        None,
        description=(
            "Comma-separated enrichment for graph formats: `tags` "
            "(address-level attribution, one lookup per address)."
        ),
        examples=["tags"],
    ),
):
    """Convert a Pathfinder graph (.gs or dashboard JSON) to `to`."""
    payload = await _read_payload(request)
    try:
        data = graph_convert.graph_from_payload(payload)
    except (ValueError, TypeError, IndexError, KeyError) as exc:
        raise BadUserInputException(f"not a Pathfinder graph: {exc}") from exc
    nodes = len(data.addresses) + len(data.txs)
    if nodes > MAX_CONVERT_NODES:
        raise BadUserInputException(
            f"graph has {nodes} nodes; at most {MAX_CONVERT_NODES} are supported"
        )
    includes = parse_comma_separated_strings(include) or []
    unknown = sorted(set(includes) - set(graph_convert.INCLUDES))
    if unknown:
        raise BadUserInputException(
            f"unknown include {', '.join(unknown)}; known: "
            f"{', '.join(graph_convert.INCLUDES)}"
        )
    if to is ConvertFormat.gs and ("tx_mode" in request.query_params or includes):
        raise BadUserInputException(
            "tx_mode and include apply to graph formats, not to gs"
        )

    async with _lookup_client(request) as client:
        result = await graph_convert.convert(
            data,
            RestBackend(client),
            to=to.value,
            tx_mode=tx_mode.value,
            layout=layout.value,
            include=includes,
        )
    return Response(
        content=result.content,
        media_type=result.media_type,
        headers={
            "Content-Disposition": f'attachment; filename="{result.filename}"',
            "X-Convert-Warnings": str(len(result.warnings)),
        },
    )
