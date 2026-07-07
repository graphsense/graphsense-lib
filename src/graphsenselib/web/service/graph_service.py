from typing import get_args

from graphsenselib.db.asynchronous.services.comparison_service import (
    compare_txs as _db_compare_txs,
)
from graphsenselib.db.asynchronous.services.graph_service import (
    summary as _db_summary,
)
from graphsenselib.db.asynchronous.services.models import (
    AddressRefInternal,
    TxRefInternal,
)
from graphsenselib.errors import BadUserInputException
from graphsenselib.web.models.graph import CompareComponent
from graphsenselib.web.translators import (
    to_api_graph_summary,
    to_api_transaction_comparison,
)

# Response components selectable via the compare ``include`` list, derived
# from the request model's Literal so the two cannot drift.
_COMPARE_COMPONENTS = get_args(CompareComponent)


def _expand_include(include: list[str]) -> set[str]:
    """Resolve the include list to a set of component names. ``all`` expands
    to every component; the Literal type already rejects unknown entries at
    the request boundary."""
    components = set(include)
    if "all" in components:
        return set(_COMPARE_COMPONENTS)
    return components & set(_COMPARE_COMPONENTS)


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


async def compare(ctx, txs, include):
    non_btc = sorted({t.network.lower() for t in txs} - {"btc"})
    if non_btc:
        raise BadUserInputException(
            "/graph/compare is BTC-only; unsupported network(s): "
            + ", ".join(non_btc)
            + ". Use /graph/summary for aggregate stats."
        )
    components = _expand_include(include)
    result = await _db_compare_txs(
        ctx.services.txs_service,
        "btc",
        [t.tx_hash for t in txs],
        include_details="details" in components,
        include_characteristics="characteristics" in components,
        include_signals="signals" in components,
        include_lineage="lineage" in components,
        include_verdict="verdict" in components,
        tagstore_groups=ctx.tagstore_groups,
    )
    return to_api_transaction_comparison(result)
