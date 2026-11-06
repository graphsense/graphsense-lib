"""Format-neutral graph model for exporting Pathfinder graphs.

The model is lossless with respect to what the exporter knows: every
transaction is a node, and each input or output that touches an address of
the graph is its own edge with its own value. Aggregated views
(one edge per address pair) are derived from it, never stored in it, so
every writer can offer them without a second builder.

Writers see each node and edge as a flat mapping from attribute name to a
scalar (``attributes()``). Names are format-neutral; a writer adds its own
namespace (GraphML: ``gs:v0:<name>``).

Conventions, fixed here so that every format shares them:

* ids: CAIP-10 for addresses, ``<caip2>:tx:<identifier>`` for txs (see
  :mod:`.networks`);
* amounts: integer base units as decimal strings, with ``asset`` (the
  GraphSense code) and ``asset_id`` (CAIP-19, when known). No floats;
* timestamps: UTC, ISO 8601 with second precision and a ``Z`` suffix;
* layout (``x``, ``y``) and visuals (``color``, ``annotation``) are
  optional and may be ignored by consumers.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Literal, Union

FORMAT_VERSION = "0"

Scalar = Union[str, int, float, bool]
TxMode = Literal["node", "aggregate"]


def iso_utc(timestamp: int | None) -> str | None:
    """Unix seconds -> ``YYYY-MM-DDTHH:MM:SSZ``."""
    if timestamp is None:
        return None
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )


def _compact(items: list[tuple[str, Scalar | None]]) -> dict[str, Scalar]:
    return {k: v for k, v in items if v is not None}


@dataclass
class Layout:
    x: float
    y: float


@dataclass
class Visual:
    """What the user drew: position, highlight color and annotation."""

    layout: Layout | None = None
    color: str | None = None  # '#rrggbbaa'
    annotation: str | None = None
    starting_point: bool = False

    def items(self) -> list[tuple[str, Scalar | None]]:
        return [
            ("starting_point", self.starting_point),
            ("annotation", self.annotation),
            ("color", self.color),
            ("x", self.layout.x if self.layout else None),
            ("y", self.layout.y if self.layout else None),
        ]


@dataclass
class Attribution:
    """What a tag source says about an address, with its provenance.

    A vendor-side assessment, not chain data: kept apart from the core
    attributes (all names start with ``attribution.``). Address-level
    only; cluster-level attribution is never exported.
    """

    source: str
    label: str | None = None
    actor: str | None = None
    category: str | None = None
    confidence: float | None = None
    """Confidence of ``label`` in the source's own 0..1 scale."""
    concepts: list[str] = field(default_factory=list)
    tag_count: int | None = None

    def items(self) -> list[tuple[str, Scalar | None]]:
        return [
            ("attribution.source", self.source),
            ("attribution.label", self.label),
            ("attribution.actor", self.actor),
            ("attribution.category", self.category),
            ("attribution.confidence", self.confidence),
            ("attribution.concepts", " ".join(self.concepts) or None),
            ("attribution.tag_count", self.tag_count),
        ]


@dataclass
class AddressNode:
    id: str
    network: str
    address: str
    visual: Visual = field(default_factory=Visual)
    attribution: Attribution | None = None

    kind = "address"

    @property
    def label(self) -> str:
        return self.visual.annotation or self.address

    def attributes(self) -> dict[str, Scalar]:
        return _compact(
            [
                ("kind", self.kind),
                ("network", self.network),
                ("address", self.address),
                *self.visual.items(),
                *(self.attribution.items() if self.attribution else []),
            ]
        )


@dataclass
class TxNode:
    id: str
    network: str
    identifier: str
    """The tx id as the graph names it, with any sub-payment suffix."""
    tx_hash: str
    resolved: bool
    """False when the backend lookup failed or found no such tx; then only
    the identity and visuals are known and the tx has no flow edges."""
    sub_index: str | None = None
    """Account-model sub-payment: ``T<n>`` (token transfer), ``I<n>``
    (internal tx). None for the base transfer and for UTXO txs."""
    tx_model: Literal["utxo", "account"] | None = None
    height: int | None = None
    timestamp: int | None = None
    asset: str | None = None
    asset_id: str | None = None
    value: str | None = None
    """Account model: the transferred amount."""
    total_input: str | None = None
    total_output: str | None = None
    no_inputs: int | None = None
    no_outputs: int | None = None
    coinbase: bool | None = None
    input_address_count: int | None = None
    """UTXO: distinct addresses over all inputs, on the graph or not; an
    input without an address counts as one of its own. 1 means a single
    sender, which makes its payment to each output address exact."""
    visual: Visual = field(default_factory=Visual)

    kind = "tx"

    @property
    def label(self) -> str:
        return self.visual.annotation or self.identifier

    def attributes(self) -> dict[str, Scalar]:
        return _compact(
            [
                ("kind", self.kind),
                ("network", self.network),
                ("tx_hash", self.tx_hash),
                ("identifier", self.identifier),
                ("sub_index", self.sub_index),
                ("resolved", self.resolved),
                ("tx_model", self.tx_model),
                ("height", self.height),
                ("timestamp", iso_utc(self.timestamp)),
                ("asset", self.asset),
                ("asset_id", self.asset_id),
                ("value", self.value),
                ("total_input", self.total_input),
                ("total_output", self.total_output),
                ("no_inputs", self.no_inputs),
                ("no_outputs", self.no_outputs),
                ("coinbase", self.coinbase),
                ("input_address_count", self.input_address_count),
                *self.visual.items(),
            ]
        )


Node = Union[AddressNode, TxNode]


@dataclass
class FlowEdge:
    """One input (``address -> tx``) or output (``tx -> address``)."""

    id: str
    kind: Literal["input", "output"]
    source: str
    target: str
    network: str
    tx: str
    """Id of the tx node, also on the edge so it reads on its own."""
    value: str | None
    asset: str | None
    asset_id: str | None
    io_index: int | None = None
    """UTXO: position among the tx's inputs (vin) or outputs (vout)."""
    outpoint: str | None = None
    """UTXO outputs: ``<tx_hash>:<vout>``. Inputs carry none, because the
    tx body does not say which output they spend."""
    address_count: int | None = None
    """Set when one input or output names several addresses (bare
    multisig): each gets its own edge, all with the full value."""

    def attributes(self) -> dict[str, Scalar]:
        return _compact(
            [
                ("kind", self.kind),
                ("network", self.network),
                ("tx", self.tx),
                ("value", self.value),
                ("asset", self.asset),
                ("asset_id", self.asset_id),
                ("io_index", self.io_index),
                ("outpoint", self.outpoint),
                ("address_count", self.address_count),
            ]
        )


@dataclass
class AggEdge:
    """An address-to-address edge drawn in the file.

    Pathfinder does not store its direction: ``source``/``target`` are just
    the two ends in file order, so ``directed`` is always False.
    """

    id: str
    source: str
    target: str
    network: str
    txs: list[str]
    """Ids of the tx nodes the edge stands for."""

    kind = "agg"

    def attributes(self) -> dict[str, Scalar]:
        return _compact(
            [
                ("kind", self.kind),
                ("network", self.network),
                ("directed", False),
                ("txs", " ".join(self.txs) or None),
            ]
        )


@dataclass
class ConversionEdge:
    """A swap or bridge.

    tx_mode=node: from the input-leg tx to the output-leg tx;
    tx_mode=aggregate: from ``from_address`` to ``to_address``.
    """

    id: str
    source: str
    target: str
    conversion_type: str | None
    input_tx: str
    """Tx node id of the input leg."""
    output_tx: str
    """Tx node id of the output leg."""
    from_address: str
    """Address node the conversion leaves from (first output of the input
    leg), as Pathfinder draws it."""
    to_address: str
    """Address node it arrives at (first input of the output leg)."""

    kind = "conversion"

    def attributes(self) -> dict[str, Scalar]:
        return _compact(
            [
                ("kind", self.kind),
                ("conversion_type", self.conversion_type),
                ("input_tx", self.input_tx),
                ("output_tx", self.output_tx),
                ("from_address", self.from_address),
                ("to_address", self.to_address),
            ]
        )


@dataclass
class PairEdge:
    """tx_mode=aggregate: all payments from one address to another in one
    asset (see :mod:`.aggregate`)."""

    id: str
    source: str
    target: str
    network: str
    asset: str | None
    asset_id: str | None
    txs: list[str] = field(default_factory=list)
    value: int | None = 0
    """Sum over ``txs`` in base units; None once one tx is not exact."""
    first_timestamp: int | None = None
    last_timestamp: int | None = None

    kind = "flow"

    def add(self, tx: TxNode, value: int | None) -> None:
        self.txs.append(tx.id)
        self.value = None if value is None or self.value is None else self.value + value
        if tx.timestamp is not None:
            self.first_timestamp = min(
                t for t in (self.first_timestamp, tx.timestamp) if t is not None
            )
            self.last_timestamp = max(
                t for t in (self.last_timestamp, tx.timestamp) if t is not None
            )

    def attributes(self) -> dict[str, Scalar]:
        return _compact(
            [
                ("kind", self.kind),
                ("network", self.network),
                ("asset", self.asset),
                ("asset_id", self.asset_id),
                ("value", str(self.value) if self.value is not None else None),
                ("value_determined", self.value is not None),
                ("tx_count", len(self.txs)),
                ("first_timestamp", iso_utc(self.first_timestamp)),
                ("last_timestamp", iso_utc(self.last_timestamp)),
                ("txs", " ".join(self.txs)),
            ]
        )


Edge = Union[FlowEdge, AggEdge, ConversionEdge, PairEdge]


@dataclass
class Graph:
    name: str
    nodes: list[Node] = field(default_factory=list)
    edges: list[Edge] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    source: str = "graphsense-pathfinder"
    tx_mode: TxMode = "node"

    def attributes(self) -> dict[str, Scalar]:
        return _compact(
            [
                ("format_version", FORMAT_VERSION),
                ("name", self.name),
                ("source", self.source),
                ("tx_mode", self.tx_mode),
                ("warnings", "\n".join(self.warnings) or None),
            ]
        )
