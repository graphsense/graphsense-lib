"""GraphML writer for the export :class:`~.model.Graph`.

Every model attribute ``<name>`` becomes a GraphML key with
``attr.name="gs:v0:<name>"``. The ``v0`` marks the keys as experimental:
they may change until the format is settled. One key is not
namespaced: ``label`` on nodes, the display text that yEd, Gephi and
Cytoscape pick up by convention.

The graph is directed. Agg edges, whose direction Pathfinder does not
store, are written as directed too with ``gs:v0:directed=false``: GraphML
allows mixed graphs, but networkx (and with it many importers) rejects
them.

Keys are declared only when used, in first-use order, so the output is
deterministic for a given graph.
"""

from __future__ import annotations

import xml.etree.ElementTree as ET

from .model import Graph, Scalar

NAMESPACE = "gs:v0:"
MEDIA_TYPE = "application/graphml+xml"
EXTENSION = "graphml"

_GRAPHML_NS = "http://graphml.graphdrawing.org/xmlns"
_XSI_NS = "http://www.w3.org/2001/XMLSchema-instance"
_SCHEMA_LOCATION = (
    f"{_GRAPHML_NS} http://graphml.graphdrawing.org/xmlns/1.0/graphml.xsd"
)


def _type_of(value: Scalar) -> str:
    # bool first: it is a subclass of int.
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "long"
    if isinstance(value, float):
        return "double"
    return "string"


def _text(value: Scalar) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


class _Keys:
    """GraphML ``<key>`` declarations, collected while writing elements."""

    def __init__(self) -> None:
        # (domain, attr name) -> [key id, type]
        self.keys: dict[tuple[str, str], list[str]] = {}

    def id_for(self, domain: str, name: str, value: Scalar) -> str:
        entry = self.keys.get((domain, name))
        if entry is None:
            entry = [f"{domain[0]}.{name.replace(':', '.')}", _type_of(value)]
            self.keys[(domain, name)] = entry
        elif entry[1] != _type_of(value):
            # Mixed types under one name: fall back to string, which every
            # value can be written as.
            entry[1] = "string"
        return entry[0]

    def elements(self) -> list[ET.Element]:
        out = []
        for (domain, name), (key_id, attr_type) in self.keys.items():
            out.append(
                ET.Element(
                    "key",
                    {
                        "id": key_id,
                        "for": domain,
                        "attr.name": name,
                        "attr.type": attr_type,
                    },
                )
            )
        return out


def _add_data(
    parent: ET.Element, keys: _Keys, domain: str, attrs: dict[str, Scalar]
) -> None:
    for name, value in attrs.items():
        data = ET.SubElement(parent, "data", {"key": keys.id_for(domain, name, value)})
        data.text = _text(value)


def _namespaced(attrs: dict[str, Scalar]) -> dict[str, Scalar]:
    return {NAMESPACE + k: v for k, v in attrs.items()}


def write_graphml(graph: Graph) -> bytes:
    """Serialize ``graph`` as a GraphML document (UTF-8)."""
    keys = _Keys()
    g = ET.Element("graph", {"id": "G", "edgedefault": "directed"})
    _add_data(g, keys, "graph", _namespaced(graph.attributes()))
    for node in graph.nodes:
        el = ET.SubElement(g, "node", {"id": node.id})
        _add_data(el, keys, "node", {"label": node.label})
        _add_data(el, keys, "node", _namespaced(node.attributes()))
    for edge in graph.edges:
        el = ET.SubElement(
            g, "edge", {"id": edge.id, "source": edge.source, "target": edge.target}
        )
        _add_data(el, keys, "edge", _namespaced(edge.attributes()))

    root = ET.Element(
        "graphml",
        {
            "xmlns": _GRAPHML_NS,
            "xmlns:xsi": _XSI_NS,
            "xsi:schemaLocation": _SCHEMA_LOCATION,
        },
    )
    root.extend(keys.elements())
    root.append(g)
    ET.indent(root)
    return ET.tostring(root, encoding="utf-8", xml_declaration=True) + b"\n"
