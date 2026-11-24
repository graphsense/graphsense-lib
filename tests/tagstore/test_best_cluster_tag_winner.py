"""best_cluster_tag_winner: the precomputed per-(cluster, ACL group) winner
must pick the same tag as the request-time pick over all candidates.

Runs in its own databases inside the shared Postgres container. The data is
random but seeded: many definer tags per cluster spread over ACL groups,
confidence levels and tagpacks, with deliberate ties, so that the tie-break
(confidence, then tagpack id, then tag id) decides often.
"""

import itertools
import logging
import random

import psycopg2
import pytest
from sqlalchemy import text
from sqlmodel.ext.asyncio.session import AsyncSession

from graphsenselib.tagpack.tagstore import TagStore
from graphsenselib.tagstore.db import TagstoreDbAsync
from graphsenselib.tagstore.db.database import get_db_engine, init_database
from graphsenselib.tagstore.db.queries import _get_best_cluster_tag_stmt
from graphsenselib.utils.constants import FRESH_CLUSTER_ID_OFFSET

GROUPS = ("public", "private", "team")
LEGACY_CLUSTERS = list(range(1, 41))
FRESH_RAW_CLUSTERS = list(range(1, 11))
SINGLETON_CLUSTER = 999  # one address, no definer: its tags are candidates
NO_TAG_CLUSTER = 1000  # mapped, but no definer tag


def _with_db(url: str, name: str) -> str:
    base, _ = url.rsplit("/", 1)
    return f"{base}/{name}"


def _seed(engine, seed=7):
    rnd = random.Random(seed)
    with engine.begin() as c:
        levels = [
            c.execute(
                text("SELECT id FROM confidence WHERE level = :l ORDER BY id LIMIT 1"),
                {"l": level},
            ).scalar_one()
            for level in (20, 50, 90, 100)
        ]
        packs = []
        for i, group in enumerate(GROUPS * 3):
            # ids that sort differently byte-wise and by locale ("C" collation)
            tp = f"{'Ab'[i % 2]}pack_{i:02d}"
            packs.append(tp)
            c.execute(
                text(
                    "INSERT INTO tagpack (id, title, description, creator, uri, "
                    "acl_group) VALUES (:id, :id, 'd', 'creator', :uri, :g)"
                ),
                {"id": tp, "uri": f"https://example.com/{tp}.yaml", "g": group},
            )

        def add_tag(ident, label, conf, tp, definer):
            tid = c.execute(
                text(
                    "INSERT INTO tag (label, source, is_cluster_definer, identifier, "
                    "network, confidence, tag_type, tag_subject, tagpack) "
                    "VALUES (:label, 'src', :d, :i, 'BTC', :c, 'actor', 'address', "
                    ":tp) RETURNING id"
                ),
                {"label": label, "d": definer, "i": ident, "c": conf, "tp": tp},
            ).scalar_one()
            c.execute(
                text(
                    "INSERT INTO tag_concept (tag_id, concept_id, "
                    "concept_relation_annotation_id) VALUES (:t, 'exchange', "
                    "'primary')"
                ),
                {"t": tid},
            )

        def map_cluster(table, cid, members, size=None):
            for a in members:
                c.execute(
                    text(
                        f"INSERT INTO {table} (address, network, gs_cluster_id, "
                        "gs_cluster_def_addr, gs_cluster_no_addr) "
                        "VALUES (:a, 'BTC', :cid, :d, :n)"
                    ),
                    {
                        "a": a,
                        "cid": cid,
                        "d": members[0],
                        "n": size or len(members) + 100,
                    },
                )

        for table, clusters, prefix in (
            ("address_cluster_mapping", LEGACY_CLUSTERS, "1L"),
            ("address_cluster_mapping_v2", FRESH_RAW_CLUSTERS, "1F"),
        ):
            for cid in clusters:
                members = [f"{prefix}{cid:03d}a{j:02d}" for j in range(8)]
                map_cluster(table, cid, members)
                for j, addr in enumerate(members):
                    # few levels and packs -> many ties; some non-definers
                    for k in range(rnd.randint(0, 3)):
                        add_tag(
                            addr,
                            f"L{cid}-{j}-{k}",
                            rnd.choice(levels[:3]),
                            rnd.choice(packs),
                            definer=rnd.random() < 0.8,
                        )

        add_tag("1Single", "Single A", levels[1], packs[0], definer=False)
        add_tag("1Single", "Single B", levels[2], packs[3], definer=False)
        map_cluster("address_cluster_mapping", SINGLETON_CLUSTER, ["1Single"], 1)
        add_tag("1NoDef", "Plain", levels[2], packs[0], definer=False)
        map_cluster("address_cluster_mapping", NO_TAG_CLUSTER, ["1NoDef", "1NoDef2"])

        for view in (
            "best_cluster_tag",
            "best_cluster_tag_v2",
            "best_cluster_tag_winner",
            "best_cluster_tag_winner_v2",
        ):
            c.execute(text(f"REFRESH MATERIALIZED VIEW {view}"))


def _key(tag):
    """Identifies a tag across lookups and databases (TagPublic has no id)."""
    return None if tag is None else (tag.identifier, tag.label, tag.tagpack_title)


def _make_db(db_setup, name, drop_winner_views=False):
    admin = psycopg2.connect(db_setup["db_connection_string"])
    admin.autocommit = True
    with admin.cursor() as cur:
        cur.execute(f"DROP DATABASE IF EXISTS {name}")
        cur.execute(f"CREATE DATABASE {name}")
    admin.close()

    engine = get_db_engine(_with_db(db_setup["db_connection_string_psycopg2"], name))
    init_database(engine)
    _seed(engine)
    if drop_winner_views:
        with engine.begin() as c:
            c.execute(text("DROP MATERIALIZED VIEW best_cluster_tag_winner"))
            c.execute(text("DROP MATERIALIZED VIEW best_cluster_tag_winner_v2"))
    engine.dispose()
    return {
        "url": _with_db(db_setup["db_connection_string"], name),
        "async_url": _with_db(db_setup["db_connection_string_async"], name),
    }


@pytest.fixture(scope="module")
def winner_db(db_setup):
    return _make_db(db_setup, "bct_winner")


@pytest.fixture(scope="module")
def refreshed_db(db_setup):
    # its own database: the test adds a tag, which the comparisons must not see
    return _make_db(db_setup, "bct_winner_refresh")


@pytest.fixture(scope="module")
def no_winner_db(db_setup):
    return _make_db(db_setup, "bct_winner_missing", drop_winner_views=True)


ALL_CLUSTERS = (
    LEGACY_CLUSTERS
    + [FRESH_CLUSTER_ID_OFFSET + c for c in FRESH_RAW_CLUSTERS]
    + [SINGLETON_CLUSTER, NO_TAG_CLUSTER, 123456]  # last: not mapped
)
GROUP_SETS = [
    list(g)
    for n in range(1, len(GROUPS) + 1)
    for g in itertools.combinations(GROUPS, n)
]


async def _pick(session, cluster_id, groups, use_winner_view):
    row = (
        await session.exec(
            _get_best_cluster_tag_stmt(
                cluster_id, "BTC", groups, use_winner_view=use_winner_view
            )
        )
    ).first()
    return None if row is None else row[0].id


async def test_winner_view_picks_the_same_tag_as_all_candidates(winner_db):
    db = TagstoreDbAsync.from_url(winner_db["async_url"])
    compared = decided = 0
    try:
        async with AsyncSession(db.engine) as session:
            for groups in GROUP_SETS:
                for cid in ALL_CLUSTERS:
                    old = await _pick(session, cid, groups, use_winner_view=False)
                    new = await _pick(session, cid, groups, use_winner_view=True)
                    assert new == old, (cid, groups)
                    compared += 1
                    decided += old is not None
    finally:
        await db.engine.dispose()
    # the data must actually exercise the comparison
    assert compared == len(GROUP_SETS) * len(ALL_CLUSTERS)
    assert decided > compared // 2


async def test_single_and_batch_lookups_agree_and_use_the_view(winner_db):
    db = TagstoreDbAsync.from_url(winner_db["async_url"])
    try:
        for groups in GROUP_SETS:
            batch = await db.get_best_cluster_tags_for_clusters(
                ALL_CLUSTERS, "BTC", groups
            )
            for cid in ALL_CLUSTERS:
                single = await db.get_best_cluster_tag(cid, "BTC", groups)
                got = batch.get(cid)
                assert _key(single) == _key(got), (cid, groups)
        assert db._winner_view_state[False][0] is True
        assert db._winner_view_state[True][0] is True
    finally:
        await db.engine.dispose()


async def test_singleton_cluster_and_missing_ones(winner_db):
    db = TagstoreDbAsync.from_url(winner_db["async_url"])
    try:
        single = await db.get_best_cluster_tag(SINGLETON_CLUSTER, "BTC", ["public"])
        none_def = await db.get_best_cluster_tag(NO_TAG_CLUSTER, "BTC", ["public"])
        unmapped = await db.get_best_cluster_tag(123456, "BTC", ["public"])
    finally:
        await db.engine.dispose()
    assert single is not None and single.label == "Single B"  # higher confidence
    assert none_def is None
    assert unmapped is None


async def test_without_the_view_lookups_fall_back(no_winner_db, winner_db, caplog):
    fallback = TagstoreDbAsync.from_url(no_winner_db["async_url"])
    reference = TagstoreDbAsync.from_url(winner_db["async_url"])
    try:
        with caplog.at_level(logging.WARNING):
            for groups in GROUP_SETS:
                got = await fallback.get_best_cluster_tags_for_clusters(
                    ALL_CLUSTERS, "BTC", groups
                )
                want = await reference.get_best_cluster_tags_for_clusters(
                    ALL_CLUSTERS, "BTC", groups
                )
                # same seed, same insert order -> same tags in both dbs
                assert {k: _key(v) for k, v in got.items()} == {
                    k: _key(v) for k, v in want.items()
                }
                for cid in (LEGACY_CLUSTERS[0], FRESH_CLUSTER_ID_OFFSET + 1):
                    s = await fallback.get_best_cluster_tag(cid, "BTC", groups)
                    w = await reference.get_best_cluster_tag(cid, "BTC", groups)
                    assert _key(s) == _key(w)
        assert fallback._winner_view_state[False][0] is False
        # one warning per regime, not one per lookup
        warnings = [
            r for r in caplog.records if "best_cluster_tag_winner" in r.getMessage()
        ]
        assert len(warnings) == 2
    finally:
        await fallback.engine.dispose()
        await reference.engine.dispose()


def test_refresh_db_refreshes_the_winner_views(refreshed_db):
    url = refreshed_db["url"]
    with psycopg2.connect(url) as conn, conn.cursor() as cur:
        # a new top definer in cluster 1 for the public group
        cur.execute("SELECT id FROM confidence WHERE level = 100 ORDER BY id LIMIT 1")
        conf = cur.fetchone()[0]
        cur.execute(
            "SELECT id FROM tagpack WHERE acl_group = 'public' ORDER BY id LIMIT 1"
        )
        tp = cur.fetchone()[0]
        cur.execute(
            "INSERT INTO tag (label, source, is_cluster_definer, identifier, "
            "network, confidence, tag_type, tag_subject, tagpack) VALUES "
            "('New Top', 'src', true, '1L001a00', 'BTC', %s, 'actor', 'address', "
            "%s) RETURNING id",
            (conf, tp),
        )
        new_id = cur.fetchone()[0]

    TagStore(url, "public").refresh_db()  # CONCURRENTLY: needs the unique index

    with psycopg2.connect(url) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT tag_id FROM best_cluster_tag_winner "
            "WHERE network = 'BTC' AND cluster_id = 1 AND acl_group = 'public'"
        )
        assert cur.fetchone()[0] == new_id


async def test_unreadable_view_falls_back(winner_db, db_setup, caplog):
    # a read-only role without SELECT on the winner views (created by another
    # role, no default privileges) must not turn lookups into errors
    admin = psycopg2.connect(winner_db["url"])
    admin.autocommit = True
    with admin.cursor() as cur:
        cur.execute("DROP ROLE IF EXISTS bct_reader")
        cur.execute("CREATE ROLE bct_reader LOGIN PASSWORD 'r'")
        cur.execute("GRANT USAGE ON SCHEMA public TO bct_reader")
        cur.execute("GRANT SELECT ON ALL TABLES IN SCHEMA public TO bct_reader")
        cur.execute(
            "REVOKE SELECT ON best_cluster_tag_winner, best_cluster_tag_winner_v2 "
            "FROM bct_reader"
        )
    admin.close()
    scheme, rest = winner_db["async_url"].split("://", 1)
    reader_url = f"{scheme}://bct_reader:r@{rest.split('@', 1)[1]}"

    reader = TagstoreDbAsync.from_url(reader_url)
    reference = TagstoreDbAsync.from_url(winner_db["async_url"])
    try:
        with caplog.at_level(logging.WARNING):
            for groups in GROUP_SETS:
                got = await reader.get_best_cluster_tags_for_clusters(
                    ALL_CLUSTERS, "BTC", groups
                )
                want = await reference.get_best_cluster_tags_for_clusters(
                    ALL_CLUSTERS, "BTC", groups
                )
                assert {k: _key(v) for k, v in got.items()} == {
                    k: _key(v) for k, v in want.items()
                }
                single = await reader.get_best_cluster_tag(
                    LEGACY_CLUSTERS[0], "BTC", groups
                )
                assert _key(single) == _key(want.get(LEGACY_CLUSTERS[0]))
        assert reader._winner_view_state[False][0] is False
        assert reference._winner_view_state[False][0] is True
        assert any("not readable" in r.getMessage() for r in caplog.records)
    finally:
        await reader.engine.dispose()
        await reference.engine.dispose()
