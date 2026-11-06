"""Paged tag queries need a total order.

They are paged with OFFSET/LIMIT. Ordered by confidence alone, which many
tags share, Postgres may return the tied rows in a different order for each
page, so pages repeat some tags and drop others (a cluster with 842 tags
came back with 20 repeated and 20 missing in pages of 100). The tag id breaks
the ties.
"""

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.dialects import postgresql

from graphsenselib.tagstore.db import queries as q
from graphsenselib.tagstore.db.models import Tag


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


def test_paged_actor_tags_end_with_the_tag_id():
    sql = str(
        q._get_tags_by_actorid_stmt("x", 0, 10, ["public", "private"], None).compile(
            dialect=postgresql.dialect()
        )
    )
    assert sql.rstrip().endswith("tag.id")
    assert ", confidence_head.id" in sql


def test_actor_tags_cannot_be_paged_over_the_primary_key():
    """With `confidence == x`, Postgres can read the per-confidence ids from
    tag_pkey and picks that for a big table: a walk over all tags per page."""
    sql = str(
        q._get_tags_by_actorid_stmt("x", 0, 10, ["public", "private"], None).compile(
            dialect=postgresql.dialect()
        )
    )
    assert "tag_1.confidence BETWEEN" in sql
    assert "ORDER BY tag_1.confidence, tag_1.id" in sql

    index = next(i for i in Tag.__table__.indexes if i.name.startswith("tag_actor_"))
    assert [c.name for c in index.columns] == ["actor", "confidence", "id"]


def test_actor_tags_without_private_access_start_from_the_visible_tagpacks():
    """Reading every tag of the actor to find the few public ones took seconds
    for a big actor. The plain join lets Postgres start from the tagpacks."""
    sql = str(
        q._get_tags_by_actorid_stmt("x", 0, 10, ["public"], None).compile(
            dialect=postgresql.dialect()
        )
    )
    assert "LATERAL" not in sql


# (confidence, tagpack): override/ownership and service_data/forensic share a
# level, so the ids have to break the ties across confidences too.
_ACTOR_TAGS = [
    ("web_crawl", "paging-public"),
    ("service_data", "paging-public"),
    ("ownership", "paging-public"),
    ("forensic", "paging-private"),
    ("override", "paging-public"),
    ("forensic", "paging-public"),
    ("ownership", "paging-private"),
    ("service_data", "paging-public"),
    ("unknown", "paging-public"),
    ("override", "paging-public"),
    ("heuristic", "paging-private"),
]
_LEVELS = {
    "override": 100,
    "ownership": 100,
    "service_data": 50,
    "forensic": 50,
    "web_crawl": 20,
    "heuristic": 10,
    "unknown": 5,
}


@pytest.fixture
def actor_with_tags(db_setup):
    engine = create_engine(db_setup["db_connection_string_psycopg2"])
    packs = {"paging-public": "public", "paging-private": "private"}
    with engine.begin() as c:
        c.execute(
            text(
                "INSERT INTO actor (id, label, lastmod) "
                "VALUES ('paging-actor', 'Paging', now())"
            )
        )
        for pack, group in packs.items():
            c.execute(
                text(
                    "INSERT INTO tagpack "
                    "(id, title, description, creator, acl_group, lastmod) "
                    "VALUES (:p, :p, '', 'test', :g, now())"
                ),
                {"p": pack, "g": group},
            )
        for i, (confidence, pack) in enumerate(_ACTOR_TAGS):
            c.execute(
                text(
                    "INSERT INTO tag (label, source, identifier, asset, network, "
                    "confidence, tag_type, tag_subject, tagpack, actor) VALUES "
                    "(:l, 's', :i, 'BTC', 'BTC', :c, 'actor', 'address', :p, "
                    "'paging-actor')"
                ),
                {"l": f"tag-{i}", "i": f"addr-{i}", "c": confidence, "p": pack},
            )
    try:
        yield
    finally:
        with engine.begin() as c:
            c.execute(text("DELETE FROM tag WHERE actor = 'paging-actor'"))
            c.execute(text("DELETE FROM actor WHERE id = 'paging-actor'"))
            c.execute(text("DELETE FROM tagpack WHERE id LIKE 'paging-%'"))
        engine.dispose()


def _expected_labels(groups):
    pack_groups = {"paging-public": "public", "paging-private": "private"}
    visible = [
        (-_LEVELS[confidence], i)
        for i, (confidence, pack) in enumerate(_ACTOR_TAGS)
        if pack_groups[pack] in groups
    ]
    return [f"tag-{i}" for _, i in sorted(visible)]


@pytest.mark.asyncio
@pytest.mark.parametrize("groups", [["public"], ["public", "private"]])
@pytest.mark.parametrize("page_size", [1, 3, 4, 100])
async def test_actor_tags_page_by_confidence_level_then_id(
    actor_with_tags, async_tagstore_db, groups, page_size
):
    labels = []
    while True:
        page = await async_tagstore_db.get_tags_by_actorid(
            "paging-actor", offset=len(labels), page_size=page_size, groups=groups
        )
        labels += [t.label for t in page]
        if len(page) < page_size:
            break

    assert labels == _expected_labels(groups)


@pytest.mark.asyncio
async def test_actor_tags_without_paging(actor_with_tags, async_tagstore_db):
    tags = await async_tagstore_db.get_tags_by_actorid(
        "paging-actor", offset=None, page_size=None, groups=["public", "private"]
    )
    assert [t.label for t in tags] == _expected_labels(["public", "private"])
