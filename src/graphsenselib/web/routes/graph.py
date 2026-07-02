"""Graph API routes (currency-less).

A graph node set is a collection of transactions and/or addresses, each
carrying its own network, so a set may span chains. The summary is derived
from header fields only and works for every supported chain; the compare
endpoint (added separately) runs the BTC-only fingerprinting analysis.
"""

from fastapi import APIRouter, Depends, Request

from graphsenselib.web.service import ServiceContext
from graphsenselib.web.models import (
    GraphCompareRequest,
    GraphSummary,
    GraphSummaryRequest,
    TransactionComparison,
)
from graphsenselib.web.routes.base import PluginRoute, get_ctx
import graphsenselib.web.service.graph_service as service

router = APIRouter(route_class=PluginRoute)


@router.post(
    "/graph/summary",
    summary="Summarize a set of transactions and/or addresses",
    description=(
        "Returns aggregate stats over the transactions and/or addresses in "
        "the request body. Every item carries its own network, so the set "
        "may span chains. Each node-type block holds a network-agnostic "
        "overall part (fiat totals per code, timestamp span) and one full "
        "per-network block (native base-unit values via the Values pattern) "
        "per network in the request. Each block is present iff the request "
        "carried that node type. Each non-empty list must hold at least 2 "
        "distinct entries; together they may hold at most 100."
    ),
    operation_id="graph_summary",
    response_model=GraphSummary,
    response_model_exclude_none=True,
    responses={
        400: {
            "description": (
                "Invalid request (each non-empty list needs at least 2 "
                "distinct entries, at most 100 nodes combined, networks "
                "must be supported)."
            )
        },
        404: {"description": "One of the transactions or addresses was not found."},
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
    summary="Compare multiple transactions",
    description=(
        "Returns per-tx characteristics, pairwise similarity signals, and a "
        "rollup verdict on whether the supplied transactions are likely "
        "linked to the same actor. The fingerprinting analysis is BTC-only; "
        "every ref's network must be btc. For chain-agnostic aggregate "
        "stats over a node set use POST /graph/summary instead."
    ),
    operation_id="graph_compare",
    response_model=TransactionComparison,
    response_model_exclude_none=True,
    responses={
        400: {
            "description": (
                "Invalid request (need 2+ distinct tx refs, or a non-BTC network)."
            )
        },
        404: {"description": "One of the transactions was not found."},
    },
)
async def graph_compare(
    request: Request,
    body: GraphCompareRequest,
    ctx: ServiceContext = Depends(get_ctx),
):
    """Compare two or more transactions (BTC-only fingerprinting analysis)."""
    return await service.compare(ctx, txs=body.txs, include=body.include)
