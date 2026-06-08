"""Subgraph API routes.

A subgraph is a set of graph nodes (currently transactions; addresses are
reserved for a future extension). These endpoints describe such a set; the
summary is chain-agnostic, so unlike /txs/compare it works for every chain.
"""

from fastapi import APIRouter, Depends, Request

from graphsenselib.web.service import ServiceContext
from graphsenselib.web.models import SubgraphSummary, SubgraphSummaryRequest
from graphsenselib.web.routes.base import PluginRoute, get_ctx
from graphsenselib.web.routes.params import CurrencyPath
import graphsenselib.web.service.subgraph_service as service

router = APIRouter(route_class=PluginRoute)


@router.post(
    "/subgraph/summary",
    summary="Summarize a set of transactions",
    description=(
        "Returns aggregate stats (value, fee, input/output counts, block and "
        "timestamp ranges) over the transactions in the request body. The "
        "summary is derived from tx headers only, so it works for every "
        "supported chain. The node set (txs + addresses) must hold at least 2 "
        "and at most 100 distinct nodes; addresses are reserved for a future "
        "extension and must be empty for now."
    ),
    operation_id="subgraph_summary",
    response_model=SubgraphSummary,
    response_model_exclude_none=True,
    responses={
        400: {
            "description": (
                "Invalid request (need 2-100 nodes, or addresses supplied "
                "which are not yet supported)."
            )
        },
        404: {"description": "One of the transactions was not found."},
    },
)
async def subgraph_summary(
    request: Request,
    currency: CurrencyPath,
    body: SubgraphSummaryRequest,
    ctx: ServiceContext = Depends(get_ctx),
):
    """Aggregate stats over a set of transactions."""
    result = await service.summary(
        ctx,
        currency=currency.lower(),
        txs=body.txs,
        addresses=body.addresses,
        fiat_currency=body.fiat_currency,
    )
    return result
