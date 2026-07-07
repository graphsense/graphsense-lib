"""Graph API routes (currency-less).

A graph node set is a collection of transactions and/or addresses, each
carrying its own network, so a set may span chains. The summary is derived
from header fields only and works for every supported chain; the compare
endpoint (added separately) runs the BTC-only fingerprinting analysis.
"""

from fastapi import APIRouter, Depends, Request

from graphsenselib.db.asynchronous.services.models import MAX_GRAPH_NODES
from graphsenselib.web.service import ServiceContext
from graphsenselib.web.models import (
    GraphCompareRequest,
    GraphComparison,
    GraphSummary,
    GraphSummaryRequest,
)
from graphsenselib.web.routes.base import PluginRoute, get_ctx
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
                f"{MAX_GRAPH_NODES} entries combined; an unsupported network."
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
                "non-BTC network; combined inputs/outputs above the "
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
