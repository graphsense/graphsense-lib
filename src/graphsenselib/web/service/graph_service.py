from graphsenselib.db.asynchronous.services.graph_service import (
    summary as _db_summary,
)
from graphsenselib.db.asynchronous.services.models import (
    AddressRefInternal,
    TxRefInternal,
)
from graphsenselib.web.translators import to_api_graph_summary


async def summary(ctx, txs, addresses):
    result = await _db_summary(
        ctx.services.txs_service,
        ctx.services.addresses_service,
        ctx.services.tags_service,
        txs=[TxRefInternal(network=t.network, tx_hash=t.tx_hash) for t in txs],
        addresses=[
            AddressRefInternal(network=a.network, address=a.address) for a in addresses
        ],
        tagstore_groups=ctx.tagstore_groups,
    )
    return to_api_graph_summary(result)
