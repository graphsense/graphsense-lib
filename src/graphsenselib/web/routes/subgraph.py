"""Subgraph API routes.

A subgraph is a set of graph nodes (transactions and/or addresses). These
endpoints describe such a set; the summary is chain-agnostic, so unlike
/txs/compare it works for every chain.
"""

from fastapi import APIRouter, Depends, Request

from graphsenselib.web.service import ServiceContext
from graphsenselib.web.models import SubgraphSummary, SubgraphSummaryRequest
from graphsenselib.web.routes.base import PluginRoute, get_ctx
from graphsenselib.web.routes.params import CurrencyPath
import graphsenselib.web.service.subgraph_service as service

router = APIRouter(route_class=PluginRoute)


@router.post(
    "/graph/summary",
    summary="Summarize a set of transactions and/or addresses",
    description=(
        "Returns aggregate stats over the transactions and/or addresses in "
        "the request body, split into a txs block (value, fee, input/output "
        "counts, block and timestamp ranges) and an addresses block (value "
        "totals, balance, usage span, tag overview). Each block is derived "
        "from header fields only, so it works for every supported chain and "
        "is present iff the request carried that node type. Each non-empty "
        "list must hold at least 2 distinct entries; together they may hold "
        "at most 100."
    ),
    operation_id="graph_summary",
    response_model=SubgraphSummary,
    response_model_exclude_none=True,
    responses={
        400: {
            "description": (
                "Invalid request (each non-empty list needs at least 2 "
                "distinct entries, at most 100 nodes combined)."
            )
        },
        404: {"description": "One of the transactions or addresses was not found."},
    },
)
async def subgraph_summary(
    request: Request,
    currency: CurrencyPath,
    body: SubgraphSummaryRequest,
    ctx: ServiceContext = Depends(get_ctx),
):
    """Aggregate stats over a set of transactions and/or addresses."""
    result = await service.summary(
        ctx,
        currency=currency.lower(),
        txs=body.txs,
        addresses=body.addresses,
        fiat_currency=body.fiat_currency,
    )
    return result
