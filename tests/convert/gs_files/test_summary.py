"""Tests for the summary endpoint."""

from __future__ import annotations

import pytest

from graphsenselib.convert.gs_files.parser import (
    Color,
    GraphAddress,
    GraphData,
    GraphEntity,
    Highlight,
    PathfinderAnnotation,
    PathfinderData,
    PathfinderId,
    PathfinderThing,
)
from graphsenselib.convert.gs_files.summary import summarize


def _thing(cur: str, identifier: str) -> PathfinderThing:
    return PathfinderThing(
        id=PathfinderId(currency=cur, id=identifier),
        x=0.0,
        y=0.0,
        is_starting_point=False,
        index=0,
    )


class TestSummarize:
    def test_pathfinder(self):
        data = PathfinderData(
            version="1",
            name="case-42",
            addresses=[_thing("btc", "a"), _thing("btc", "b")],
            txs=[_thing("btc", "t1")],
            annotations=[
                PathfinderAnnotation(
                    id=PathfinderId(currency="btc", id="a"),
                    label="l",
                    color=None,
                )
            ],
            agg_edges=[],
        )
        assert summarize(data) == {
            "kind": "pathfinder",
            "version": "1",
            "name": "case-42",
            "n_addresses": 2,
            "n_txs": 1,
            "n_annotations": 1,
            "n_agg_edges": 0,
        }

    def test_graph(self):
        data = GraphData(
            version="1.0.0",
            addresses=[
                GraphAddress(
                    currency="btc",
                    layer=0,
                    address="a",
                    x=0.0,
                    y=0.0,
                    color=None,
                    user_tag=None,
                )
            ],
            entities=[
                GraphEntity(
                    currency="btc",
                    layer=0,
                    entity_id=1,
                    root_address=None,
                    x=0.0,
                    y=0.0,
                    color=None,
                    no_addresses=0,
                )
                for _ in range(3)
            ],
            highlights=[
                Highlight(title="hl", color=Color(0.0, 0.0, 0.0, 1.0)),
                Highlight(title="hl2", color=Color(1.0, 1.0, 1.0, 1.0)),
            ],
        )
        assert summarize(data) == {
            "kind": "graph",
            "version": "1.0.0",
            "n_addresses": 1,
            "n_entities": 3,
            "n_highlights": 2,
        }

    def test_unknown_type(self):
        with pytest.raises(TypeError, match="unknown structured type"):
            summarize("not a valid structured type")  # type: ignore[arg-type]
