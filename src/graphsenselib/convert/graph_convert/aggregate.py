"""tx_mode=aggregate: address-to-address edges derived from the node graph.

Every writer gets this view without knowing about it, since it is computed
on the model.

* One ``flow`` edge per (network, sender, receiver, asset): every pair of
  an on-graph input address and an on-graph output address of a tx. Assets
  are never summed together, nor networks.
* Tx nodes are dropped; each flow edge lists its txs, with count and
  first/last timestamp.
* ``value`` is exact or absent, never estimated. A UTXO tx does not say
  which input paid which output, so its payment from sender to receiver is
  known only when the tx has a single sender address (then it is that
  receiver's outputs). An account-model tx always has one sender and one
  receiver. ``value`` is set only when every tx of the edge is exact;
  ``value_determined`` says whether it is.
* A sender paying itself (UTXO change, account self-transfer) is no flow
  edge; node mode keeps those.
* Agg edges are kept; conversions run between their address legs.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import replace

from .model import (
    AddressNode,
    ConversionEdge,
    Edge,
    FlowEdge,
    Graph,
    PairEdge,
    TxNode,
)


def _pair_values(
    tx: TxNode, inputs: list[FlowEdge], outputs: list[FlowEdge]
) -> dict[tuple[str, str], int | None]:
    """Payment per (sender, receiver) of one tx; None where not exact."""
    senders = list(dict.fromkeys(e.source for e in inputs))
    paid: dict[str, int | None] = defaultdict(int)
    for e in outputs:
        exact = e.value is not None and not e.address_count
        prev = paid[e.target]
        paid[e.target] = None if not exact or prev is None else prev + int(e.value)
    if tx.tx_model == "account":
        exact_sender = True
    else:
        exact_sender = tx.input_address_count == 1
    return {
        (s, r): (v if exact_sender else None)
        for s in senders
        for r, v in paid.items()
        if s != r
    }


def aggregate(graph: Graph) -> Graph:
    """The tx_mode=aggregate view of a node-mode ``graph``."""
    if graph.tx_mode != "node":
        raise ValueError("aggregate() takes a node-mode graph")
    txs = {n.id: n for n in graph.nodes if isinstance(n, TxNode)}
    flows: dict[str, tuple[list[FlowEdge], list[FlowEdge]]] = {
        tx_id: ([], []) for tx_id in txs
    }
    for e in graph.edges:
        if isinstance(e, FlowEdge):
            flows[e.tx][0 if e.kind == "input" else 1].append(e)

    pairs: dict[tuple[str, str, str, str | None], PairEdge] = {}
    without_pair = 0
    for tx_id, (inputs, outputs) in flows.items():
        tx = txs[tx_id]
        values = _pair_values(tx, inputs, outputs)
        if not values:
            without_pair += 1
            continue
        for (s, r), value in values.items():
            key = (tx.network, s, r, tx.asset)
            if key not in pairs:
                pairs[key] = PairEdge(
                    id=f"flow/{len(pairs)}",
                    source=s,
                    target=r,
                    network=tx.network,
                    asset=tx.asset,
                    asset_id=tx.asset_id,
                )
            pairs[key].add(tx, value)

    edges: list[Edge] = list(pairs.values())
    for e in graph.edges:
        if isinstance(e, ConversionEdge):
            edges.append(replace(e, source=e.from_address, target=e.to_address))
        elif not isinstance(e, FlowEdge):
            edges.append(e)

    warnings = list(graph.warnings)
    if without_pair:
        warnings.append(
            f"{without_pair} tx(s) connect no two different addresses of the "
            "graph and are not in the aggregate view; use tx_mode=node to "
            "keep them"
        )
    return replace(
        graph,
        nodes=[n for n in graph.nodes if isinstance(n, AddressNode)],
        edges=edges,
        warnings=warnings,
        tx_mode="aggregate",
    )
