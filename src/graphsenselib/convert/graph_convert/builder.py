"""Build the export :class:`~.model.Graph` from a decoded Pathfinder file.

The builder is pure: it takes the decoded file plus whatever the backend
lookup returned (tx bodies, conversions, warnings; see :mod:`.lookup`) and
does no I/O, so it can be tested against fixed inputs.

A ``.gs`` file stores which txs are on the graph but not which addresses
they connect, so the flow edges come entirely from the tx bodies. Only
inputs and outputs that touch an address on the graph become edges; the
tx node keeps the totals, so nothing about the rest of the tx is implied.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any, Iterable

from graphsenselib.convert.gs_files import (
    Color,
    PathfinderData,
    PathfinderId,
    PathfinderThing,
    normalize_tx_id,
)

from . import networks
from .model import (
    AddressNode,
    AggEdge,
    Attribution,
    ConversionEdge,
    FlowEdge,
    Graph,
    Layout,
    TxNode,
    Visual,
)

# Tx response bodies (``GET /{network}/txs/{id}?include_io=true``) by
# ``(network, tx id as the file spells it)``. ``None``: no such tx; key
# missing: the lookup failed.
TxBodies = Mapping[tuple[str, str], "dict[str, Any] | None"]

# Address tag summaries (``GET /{network}/addresses/{address}/tag_summary``)
# by ``(network, normalized address)``.
TagSummaries = Mapping[tuple[str, str], "dict[str, Any] | None"]

TAG_SOURCE = "graphsense-tagstore"

_SUB_PAYMENT_RE = re.compile(r"^(.+)_([IT]\d+)$")


def _hex_color(color: Color | None) -> str | None:
    if color is None:
        return None
    parts = (color.r, color.g, color.b, color.a)
    return "#" + "".join(f"{round(max(0.0, min(1.0, p)) * 255):02x}" for p in parts)


def attribution_from_tag_summary(body: dict[str, Any]) -> Attribution | None:
    """The export's view of a tag summary; None when it has no tags."""
    if not body.get("tag_count"):
        return None
    label = body.get("best_label")
    best = (body.get("label_summary") or {}).get(label) if label else None
    best = best if isinstance(best, dict) else {}
    confidence = best.get("confidence")
    return Attribution(
        source=TAG_SOURCE,
        label=label,
        actor=body.get("best_actor"),
        category=body.get("broad_category"),
        confidence=float(confidence) if isinstance(confidence, (int, float)) else None,
        concepts=sorted(c for c in best.get("concepts") or [] if isinstance(c, str)),
        tag_count=body.get("tag_count"),
    )


def _input_address_count(inputs: Any) -> int | None:
    if not isinstance(inputs, list):
        return None
    senders: set[str] = set()
    anonymous = 0
    for entry in inputs:
        raw = entry.get("address") if isinstance(entry, dict) else None
        names = [a for a in (raw if isinstance(raw, list) else [raw]) if a]
        if len(names) == 1 and isinstance(names[0], str):
            senders.add(networks.normalize_address(names[0]))
        else:
            # No address, or a bare multisig: nobody single-handedly sent it.
            anonymous += 1
    return len(senders) + anonymous


def _tx_key(tx_id: str) -> str:
    return normalize_tx_id(tx_id).removeprefix("0x")


def _base_units(values: Any) -> str | None:
    value = values.get("value") if isinstance(values, dict) else None
    return str(value) if isinstance(value, int) else None


class _Builder:
    def __init__(self, data: PathfinderData) -> None:
        self.data = data
        self.annotations = {(a.id.currency, a.id.id): a for a in data.annotations}
        self.graph = Graph(name=data.name)
        # (network, normalized address) -> address node id
        self.addresses: dict[tuple[str, str], str] = {}
        # (network, tx key) -> tx node, and tx key alone for conversions,
        # whose leg ids carry no network
        self.txs: dict[tuple[str, str], TxNode] = {}
        self.txs_by_key: dict[str, TxNode] = {}

    def visual(self, thing: PathfinderThing | None, pid: PathfinderId) -> Visual:
        annotation = self.annotations.get((pid.currency, pid.id))
        return Visual(
            layout=Layout(thing.x, thing.y) if thing else None,
            color=_hex_color(annotation.color) if annotation else None,
            annotation=(annotation.label or None) if annotation else None,
            starting_point=bool(thing and thing.is_starting_point),
        )

    def add_address(self, pid: PathfinderId, thing: PathfinderThing | None) -> str:
        network = pid.currency
        key = (network, networks.normalize_address(pid.id))
        if key not in self.addresses:
            node = AddressNode(
                id=networks.address_id(network, pid.id),
                network=network,
                address=key[1],
                visual=self.visual(thing, pid),
            )
            self.graph.nodes.append(node)
            self.addresses[key] = node.id
        return self.addresses[key]

    def on_graph(self, network: str, address: str) -> str | None:
        return self.addresses.get((network, networks.normalize_address(address)))

    def add_tx(self, thing: PathfinderThing, bodies: TxBodies) -> None:
        network = thing.id.currency
        identifier = normalize_tx_id(thing.id.id)
        if (network, _tx_key(identifier)) in self.txs:
            return
        m = _SUB_PAYMENT_RE.match(identifier)
        tx_hash, sub_index = (m.group(1), m.group(2)) if m else (identifier, None)
        lookup = (network, thing.id.id)
        body = bodies.get(lookup)
        node = TxNode(
            id=networks.tx_id(network, identifier),
            network=network,
            identifier=identifier,
            tx_hash=tx_hash,
            sub_index=sub_index,
            resolved=isinstance(body, dict),
            visual=self.visual(thing, thing.id),
        )
        self.graph.nodes.append(node)
        self.txs[(network, _tx_key(identifier))] = node
        self.txs_by_key.setdefault(_tx_key(identifier), node)
        if lookup not in bodies:
            self.graph.warnings.append(
                f"tx {identifier} ({network}): lookup failed, exported without "
                "flow edges"
            )
        elif body is None:
            self.graph.warnings.append(
                f"tx {identifier} ({network}): not found on the backend, "
                "exported without flow edges"
            )
        else:
            self.add_flows(node, body)

    def add_flows(self, tx: TxNode, body: dict[str, Any]) -> None:
        tx.height = body.get("height")
        tx.timestamp = body.get("timestamp")
        if body.get("tx_type") == "account":
            self.add_account_flows(tx, body)
        else:
            self.add_utxo_flows(tx, body)

    def add_account_flows(self, tx: TxNode, body: dict[str, Any]) -> None:
        tx.tx_model = "account"
        tx.asset = str(body.get("currency") or tx.network).lower()
        tx.asset_id = networks.asset_id(tx.network, tx.asset)
        tx.value = _base_units(body.get("value"))
        ends = (
            ("input", body.get("from_address"), "in"),
            ("output", body.get("to_address"), "out"),
        )
        for kind, address, suffix in ends:
            if not isinstance(address, str):
                continue
            node = self.on_graph(tx.network, address)
            if node is None:
                continue
            source, target = (node, tx.id) if kind == "input" else (tx.id, node)
            self.graph.edges.append(
                FlowEdge(
                    id=f"{tx.id}/{suffix}",
                    kind=kind,  # type: ignore[arg-type]
                    source=source,
                    target=target,
                    network=tx.network,
                    tx=tx.id,
                    value=tx.value,
                    asset=tx.asset,
                    asset_id=tx.asset_id,
                )
            )

    def add_utxo_flows(self, tx: TxNode, body: dict[str, Any]) -> None:
        tx.tx_model = "utxo"
        tx.asset = tx.network
        tx.asset_id = networks.asset_id(tx.network, tx.asset)
        tx.total_input = _base_units(body.get("total_input"))
        tx.total_output = _base_units(body.get("total_output"))
        tx.no_inputs = body.get("no_inputs")
        tx.no_outputs = body.get("no_outputs")
        tx.coinbase = body.get("coinbase")
        tx.input_address_count = _input_address_count(body.get("inputs"))
        for kind, entries, suffix in (
            ("input", body.get("inputs"), "in"),
            ("output", body.get("outputs"), "out"),
        ):
            for pos, entry in enumerate(entries or []):
                if not isinstance(entry, dict):
                    continue
                # The lookup asks for non-standard io too, so the position
                # in the list is the vin / vout unless the body says so.
                index = entry.get("index")
                index = index if isinstance(index, int) else pos
                raw = entry.get("address")
                addresses = [
                    a
                    for a in (raw if isinstance(raw, list) else [raw])
                    if isinstance(a, str) and a
                ]
                shared = len(addresses) if len(addresses) > 1 else None
                for address in addresses:
                    node = self.on_graph(tx.network, address)
                    if node is None:
                        continue
                    edge_id = f"{tx.id}/{suffix}/{index}"
                    if shared:
                        edge_id += f"/{networks.normalize_address(address)}"
                    source, target = (node, tx.id) if kind == "input" else (tx.id, node)
                    self.graph.edges.append(
                        FlowEdge(
                            id=edge_id,
                            kind=kind,  # type: ignore[arg-type]
                            source=source,
                            target=target,
                            network=tx.network,
                            tx=tx.id,
                            value=_base_units(entry.get("value")),
                            asset=tx.asset,
                            asset_id=tx.asset_id,
                            io_index=index,
                            outpoint=(
                                f"{tx.tx_hash}:{index}" if kind == "output" else None
                            ),
                            address_count=shared,
                        )
                    )

    def add_agg_edges(self) -> None:
        for i, e in enumerate(self.data.agg_edges):
            source = self.add_address(e.a, None)
            target = self.add_address(e.b, None)
            self.graph.edges.append(
                AggEdge(
                    id=f"agg/{i}",
                    source=source,
                    target=target,
                    network=e.a.currency,
                    txs=[
                        networks.tx_id(t.currency, normalize_tx_id(t.id)) for t in e.txs
                    ],
                )
            )

    def add_conversions(self, conversions: Iterable[dict[str, Any]]) -> None:
        for i, c in enumerate(conversions):
            src = self.txs_by_key.get(_tx_key(str(c.get("input_tx", ""))))
            dst = self.txs_by_key.get(_tx_key(str(c.get("output_tx", ""))))
            if src is None or dst is None:
                continue
            from_node = self.on_graph(src.network, str(c.get("edge_from", "")))
            to_node = self.on_graph(dst.network, str(c.get("edge_to", "")))
            if from_node is None or to_node is None:
                continue
            self.graph.edges.append(
                ConversionEdge(
                    id=f"conversion/{i}",
                    source=src.id,
                    target=dst.id,
                    conversion_type=c.get("type"),
                    input_tx=src.id,
                    output_tx=dst.id,
                    from_address=from_node,
                    to_address=to_node,
                )
            )


def build_graph(
    data: PathfinderData,
    bodies: TxBodies,
    *,
    conversions: Iterable[dict[str, Any]] = (),
    tag_summaries: TagSummaries | None = None,
    warnings: Iterable[str] = (),
) -> Graph:
    """The node-mode graph of ``data``.

    ``conversions`` are the entries :func:`graphsenselib.pathfinder.annotate_conversions`
    writes onto a spec; ``tag_summaries`` (optional) add attribution to
    address nodes; ``warnings`` are passed through from the lookup.
    Nodes come in file order (addresses, then txs), edges as flows per tx,
    then agg edges, then conversions.
    """
    b = _Builder(data)
    b.graph.warnings.extend(warnings)
    for thing in data.addresses:
        b.add_address(thing.id, thing)
    for thing in data.txs:
        b.add_tx(thing, bodies)
    b.add_agg_edges()
    b.add_conversions(conversions)
    for node in b.graph.nodes:
        if isinstance(node, AddressNode) and tag_summaries:
            summary = tag_summaries.get((node.network, node.address))
            if isinstance(summary, dict):
                node.attribution = attribution_from_tag_summary(summary)
    return b.graph
