from graphsenselib.db.asynchronous.services.subgraph_service import (
    summary as _db_summary,
)
from graphsenselib.web.translators import to_api_subgraph_summary


async def summary(
    ctx,
    currency: str,
    txs: list[str],
    addresses: list[str],
):
    result = await _db_summary(
        ctx.services.txs_service,
        currency,
        txs,
        addresses,
        tagstore_groups=ctx.tagstore_groups,
    )
    return to_api_subgraph_summary(result)
