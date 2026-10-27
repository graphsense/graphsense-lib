"""Paged tag queries need a total order.

They are paged with OFFSET/LIMIT. Ordered by confidence alone, which many
tags share, Postgres may return the tied rows in a different order for each
page, so pages repeat some tags and drop others (a cluster with 842 tags
came back with 20 repeated and 20 missing in pages of 100). The tag id breaks
the ties.
"""

import pytest
from sqlalchemy.dialects import postgresql

from graphsenselib.tagstore.db import queries as q


def _order_by(stmt) -> str:
    sql = str(stmt.compile(dialect=postgresql.dialect()))
    return sql.split("ORDER BY", 1)[1].split("LIMIT", 1)[0].strip()


@pytest.mark.parametrize(
    "stmt",
    [
        q._get_tags_by_subjectid_stmt("1abc", 0, 10, ["public"], None),
        q._get_tags_by_actorid_stmt("binance", 0, 10, ["public"], None),
        q._get_tags_by_clusterid_stmt(7648699, "BTC", 0, 10, ["public"], None),
        q._get_tags_by_label_stmt("binance", 0, 10, ["public"], None),
    ],
)
def test_paged_tag_queries_end_with_the_tag_id(stmt):
    assert _order_by(stmt).endswith("tag.id")
