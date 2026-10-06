"""ranking-conflicts / actor-conflicts against a real tagstore schema.

Runs in its own database inside the shared Postgres container, so the rows
seeded here never leak into the other tagstore tests.
"""

import json

import psycopg2
import pytest
from click.testing import CliRunner
from sqlalchemy import text

from graphsenselib.tagpack.cli import cli as tagpack_cli
from graphsenselib.tagstore.algorithms.ranking_conflicts import Reason
from graphsenselib.tagstore.db import TagstoreDbAsync
from graphsenselib.tagstore.db.database import get_db_engine, init_database
from graphsenselib.tagstore.ranking_conflicts import detect, ranking_rows
from graphsenselib.utils.constants import FRESH_CLUSTER_ID_OFFSET

DB_NAME = "ranking_conflicts"

ADDR = "1TestHotWa11et0000000000000000000"
ICO = "1TestIcoDefiner000000000000000000"
CLUSTER_TIE = 1001
CLUSTER_GAMBLING = 200
CLUSTER_OVERRIDE = 300
CLUSTER_SWARM = 400
CLUSTER_VOTE_TIE = 600
FRESH_RAW = 5


def _with_db(url: str, name: str) -> str:
    base, _ = url.rsplit("/", 1)
    return f"{base}/{name}"


def _seed(engine):
    with engine.begin() as c:

        def conf(level):
            return c.execute(
                text("SELECT id FROM confidence WHERE level = :l ORDER BY id LIMIT 1"),
                {"l": level},
            ).scalar_one()

        c100, c90, c50, c20 = conf(100), conf(90), conf(50), conf(20)

        c.execute(
            text(
                "INSERT INTO actorpack (id, title, creator, description, uri) "
                "VALUES ('ap', 'ap', 'me', 'd', 'u')"
            )
        )
        for actor in ("exchange_a", "exchange_b"):
            c.execute(
                text(
                    "INSERT INTO actor (id, uri, label, context, actorpack) "
                    "VALUES (:a, 'u', :a, NULL, 'ap')"
                ),
                {"a": actor},
            )
            c.execute(
                text(
                    "INSERT INTO actor_concept (actor_id, concept_id) "
                    "VALUES (:a, 'exchange')"
                ),
                {"a": actor},
            )
        # a rebrand: two actor entries of one organisation, declared same_as
        for actor, context in (
            ("rebrand_new", None),
            ("rebrand_old", '{"same_as": ["rebrand_new"]}'),
        ):
            c.execute(
                text(
                    "INSERT INTO actor (id, uri, label, context, actorpack) "
                    "VALUES (:a, 'u', :a, :ctx, 'ap')"
                ),
                {"a": actor, "ctx": context},
            )
        for tp, group in (
            ("tp0", "public"),
            ("tp1", "public"),
            ("tp2", "public"),
            ("tpp", "private"),
        ):
            c.execute(
                text(
                    "INSERT INTO tagpack (id, title, description, creator, uri, "
                    "acl_group) VALUES (:id, :id, 'd', 'creator', :uri, :g)"
                ),
                {"id": tp, "uri": f"https://example.com/{tp}.yaml", "g": group},
            )

        def add_tag(
            ident,
            label,
            confidence,
            concept,
            actor=None,
            tag_type="actor",
            definer=False,
            tp="tp1",
        ):
            tid = c.execute(
                text(
                    "INSERT INTO tag (label, source, is_cluster_definer, identifier, "
                    "network, confidence, tag_type, tag_subject, tagpack, actor) "
                    "VALUES (:label, 'src', :d, :i, 'BTC', :c, :tt, 'address', :tp, "
                    ":a) RETURNING id"
                ),
                {
                    "label": label,
                    "d": definer,
                    "i": ident,
                    "c": confidence,
                    "tt": tag_type,
                    "tp": tp,
                    "a": actor,
                },
            ).scalar_one()
            c.execute(
                text(
                    "INSERT INTO tag_concept (tag_id, concept_id, "
                    "concept_relation_annotation_id) VALUES (:t, :c, 'primary')"
                ),
                {"t": tid, "c": concept},
            )

        # Exchange A cluster: ICO X ties with two Exchange A definers at 90;
        # its pack sorts first
        add_tag(ICO, "ICO X", c90, "ico_wallet", definer=True, tp="tp0")
        add_tag("1ExA1", "Exchange A", c90, "exchange", "exchange_a", definer=True)
        add_tag("1ExA2", "Exchange A", c90, "exchange", "exchange_a", definer=True)
        add_tag("1ExB", "Exchange B", c50, "exchange", "exchange_b", definer=True)
        add_tag(ADDR, "Exchange A Hot Wallet", c20, "exchange", "exchange_a")
        add_tag(ADDR, "Exchange A Hot Wallet", c20, "exchange", "exchange_a", tp="tp2")
        add_tag(ADDR, "Transaction mention", c20, "hacking", tag_type="mention")
        tie_members = [ICO, "1ExA1", "1ExA2", "1ExB", ADDR]

        # Deterministic cluster: one non-exchange definer clearly on top
        add_tag("1G", "Gambling X", c90, "gambling", definer=True)
        add_tag("1P", "Exchange A", c20, "exchange", "exchange_a", definer=True)
        add_tag("1E", "Exchange D deposit", c20, "exchange")
        gambling_members = ["1G", "1P", "1E"]

        # A confidence-100 (override/ownership) definer outranks any majority
        add_tag("1S", "Supercluster", c100, "gambling", definer=True)
        add_tag("1B1", "Exchange B", c50, "exchange", "exchange_b", definer=True)
        add_tag("1B2", "Exchange B", c50, "exchange", "exchange_b", definer=True)
        override_members = ["1S", "1B1", "1B2"]

        # Two actors on one unclustered address; one private tag
        add_tag("1Multi", "Exchange A", c50, "exchange", "exchange_a")
        add_tag("1Multi", "Exchange B", c50, "exchange", "exchange_b", tp="tpp")

        # one strong cluster tag against a swarm of weak ones from one bulk
        # pack (gambling, so the exchange checks are not affected); counted
        # per tag the swarm would win (30 x e^2 > e^5)
        add_tag("1Strong", "Strong", c50, "gambling", "rebrand_new", definer=True)
        swarm_members = ["1Strong"]
        for i in range(30):
            add_tag(
                f"1Weak{i:02d}",
                "Weak",
                c20,
                "gambling",
                "rebrand_old",
                definer=True,
                tp="tp2",
            )
            swarm_members.append(f"1Weak{i:02d}")

        # a tied vote: two actors, one tag each at the same confidence; the
        # actor whose id sorts first has the lower tag id but the later pack
        add_tag(
            "1VoteZ",
            "Ay Exchange",
            c90,
            "gambling",
            "rebrand_new",
            definer=True,
            tp="tp1",
        )
        add_tag(
            "1VoteA",
            "Zed Exchange",
            c90,
            "gambling",
            "rebrand_old",
            definer=True,
            tp="tp0",
        )
        vote_tie_members = ["1VoteA", "1VoteZ"]

        for table, cid, members in (
            ("address_cluster_mapping", CLUSTER_TIE, tie_members),
            ("address_cluster_mapping", CLUSTER_GAMBLING, gambling_members),
            ("address_cluster_mapping", CLUSTER_OVERRIDE, override_members),
            ("address_cluster_mapping", CLUSTER_SWARM, swarm_members),
            ("address_cluster_mapping", CLUSTER_VOTE_TIE, vote_tie_members),
            ("address_cluster_mapping_v2", FRESH_RAW, gambling_members),
        ):
            for a in members:
                c.execute(
                    text(
                        f"INSERT INTO {table} (address, network, gs_cluster_id, "
                        "gs_cluster_def_addr, gs_cluster_no_addr) "
                        "VALUES (:a, 'BTC', :cid, :d, :n)"
                    ),
                    {"a": a, "cid": cid, "d": members[0], "n": len(members) + 1000},
                )
        add_tag("1Rebrand", "Old Name", c50, "gambling", "rebrand_old")
        add_tag("1Rebrand", "New Name", c50, "gambling", "rebrand_new")

        # an exchange-categorised mention is no exchange attribution
        add_tag("1Mention", "Dark Web", c20, "exchange", tag_type="mention")

        # text that tagpack validation now rejects
        add_tag("1Txt1", "Name: " + "Иван".encode().decode("latin-1"), c50, "scam")
        add_tag("1Txt2", "bell\x07label", c50, "scam")
        add_tag("1Txt3", "Zürich Café", c50, "gambling")  # fine

        for view in (
            "best_cluster_tag",
            "best_cluster_tag_v2",
            "tag_count_by_cluster",
            "tag_count_by_cluster_v2",
        ):
            c.execute(text(f"REFRESH MATERIALIZED VIEW {view}"))


@pytest.fixture(scope="module")
def rc_db(db_setup):
    admin = psycopg2.connect(db_setup["db_connection_string"])
    admin.autocommit = True
    with admin.cursor() as cur:
        cur.execute(f"DROP DATABASE IF EXISTS {DB_NAME}")
        cur.execute(f"CREATE DATABASE {DB_NAME}")
    admin.close()

    engine = get_db_engine(_with_db(db_setup["db_connection_string_psycopg2"], DB_NAME))
    init_database(engine)
    _seed(engine)
    engine.dispose()
    return {
        "url": _with_db(db_setup["db_connection_string"], DB_NAME),
        "async_url": _with_db(db_setup["db_connection_string_async"], DB_NAME),
    }


async def _detect(rc_db, **kw):
    db = TagstoreDbAsync.from_url(rc_db["async_url"])
    try:
        return await detect(db, "btc", **kw)
    finally:
        await db.engine.dispose()


async def test_ranking_conflicts(rc_db):
    result = await _detect(rc_db, actors=False, clustering="legacy")
    assert result.n_candidates["ranking/legacy"] == 9

    clusters = {f.cluster_id: f for f in result.cluster_findings}
    addresses = {f.address: f for f in result.address_findings}

    g = clusters[CLUSTER_GAMBLING]
    assert g.selected_label == "Gambling X" and g.majority_actor == "exchange_a"
    assert g.n_addresses == 1003
    assert set(g.reasons) == {
        Reason.DEFINER_NOT_MAJORITY,
        Reason.NON_EXCHANGE_CLUSTER_DEFINER,
    }
    e = addresses["1E"]
    assert e.best_label == "Gambling X" and e.cluster_id == CLUSTER_GAMBLING
    assert set(e.reasons) == {
        Reason.INHERITED_WINS,
        Reason.NON_EXCHANGE_CLUSTER_DEFINER,
    }

    # The Exchange A cluster's pick is a 90/90/90 tie, decided by Postgres.
    p = clusters[CLUSTER_TIE]
    assert Reason.DEFINER_TIE in p.reasons
    if p.selected_label == "ICO X":
        assert Reason.DEFINER_NOT_MAJORITY in p.reasons
        assert set(addresses[ADDR].reasons) == {
            Reason.INHERITED_WINS,
            Reason.ACTOR_THRESHOLD_DROPPED,
            Reason.NON_EXCHANGE_CLUSTER_DEFINER,
            Reason.DEFINER_TIE,
        }
    else:
        assert p.selected_actor == "exchange_a"
        assert ADDR not in addresses

    rows = ranking_rows(result)
    assert [r["level"] for r in rows] == sorted(r["level"] for r in rows)


async def test_actor_conflicts_and_groups(rc_db):
    result = await _detect(rc_db, ranking=False, clustering="legacy")
    by_addr = {f.address: f for f in result.address_conflicts}
    assert [a.actor for a in by_addr["1Multi"].actors] == ["exchange_a", "exchange_b"]

    clusters = {f.cluster_id: f for f in result.cluster_conflicts}
    assert Reason.DEFINERS_DISAGREE in clusters[CLUSTER_TIE].reasons
    assert Reason.DEFINER_WITHOUT_ACTOR in clusters[CLUSTER_GAMBLING].reasons
    assert clusters[CLUSTER_GAMBLING].n_addresses == 1003

    # rebrand_old / rebrand_new are declared same_as in the actorpack
    assert "1Rebrand" not in by_addr

    # the exchange_b tag on 1Multi is private
    public_only = await _detect(
        rc_db, ranking=False, clustering="legacy", groups=["public"]
    )
    assert "1Multi" not in {f.address for f in public_only.address_conflicts}


async def test_auto_clustering_prefers_v2_when_mapped(rc_db):
    result = await _detect(rc_db, actors=False, limit=1)
    assert list(result.n_candidates) == ["ranking/v2"]
    assert result.n_candidates["ranking/v2"] == 1
    # --limit also limits the cluster checks to the candidates' clusters;
    # the one candidate (14yF…) has no v2 mapping, so no cluster is checked
    assert result.cluster_findings == []


async def test_explicit_addresses(rc_db):
    result = await _detect(
        rc_db, actors=False, clustering="legacy", addresses=["1E", "1nothere"]
    )
    assert result.n_candidates["ranking/legacy"] == 2
    assert result.n_mapped["ranking/legacy"] == 1
    assert result.n_clusters["ranking/legacy"] == 1
    assert [f.address for f in result.address_findings] == ["1E"]
    assert [f.cluster_id for f in result.cluster_findings] == [CLUSTER_GAMBLING]


async def test_both_clusterings_shift_fresh_ids(rc_db):
    result = await _detect(rc_db, actors=False, clustering="both")
    ids = {(f.clustering, f.cluster_id) for f in result.cluster_findings}
    assert ("v2", FRESH_RAW + FRESH_CLUSTER_ID_OFFSET) in ids
    assert ("legacy", CLUSTER_GAMBLING) in ids
    assert {f.clustering for f in result.address_findings} == {"legacy", "v2"}


def test_cli_ranking_conflicts(rc_db, tmp_path):
    out = tmp_path / "rc.json"
    res = CliRunner().invoke(
        tagpack_cli,
        [
            "quality",
            "-u",
            rc_db["url"],
            "ranking-conflicts",
            "--network",
            "btc",
            "--format",
            "json",
            "--out",
            str(out),
        ],
        catch_exceptions=False,
    )
    assert res.exit_code == 0, res.output
    rows = json.loads(out.read_text())
    assert any(r["address"] == "1E" for r in rows)
    assert "address:" in res.stderr and "INHERITED_WINS=" in res.stderr
    assert "ranking/v2: checked 9 addresses" in res.stderr

    res = CliRunner().invoke(
        tagpack_cli,
        ["quality", "-u", rc_db["url"], "actor-conflicts", "--network", "btc"],
        catch_exceptions=False,
    )
    assert res.exit_code == 0, res.output
    header, *lines = res.stdout.splitlines()
    assert header.startswith("level,network,clustering,cluster_id")
    assert any("1Multi" in line for line in lines)


async def test_best_cluster_tag_tie_break(rc_db):
    # equally confident definers go to the pack whose id sorts first, then to
    # its first tag; batch and single lookups agree
    db = TagstoreDbAsync.from_url(rc_db["async_url"])
    try:
        picks = await db.get_best_cluster_tags_for_clusters(
            [CLUSTER_TIE, CLUSTER_GAMBLING, CLUSTER_OVERRIDE, CLUSTER_VOTE_TIE],
            "BTC",
            ["public", "private"],
        )
        singles = {
            cid: await db.get_best_cluster_tag(cid, "BTC", ["public", "private"])
            for cid in picks
        }
    finally:
        await db.engine.dispose()
    labels = {cid: t.label for cid, t in picks.items()}
    assert labels == {cid: t.label for cid, t in singles.items()}
    assert labels[CLUSTER_TIE] == "ICO X"  # 90/90/90 tie, its pack sorts first
    assert labels[CLUSTER_GAMBLING] == "Gambling X"  # no tie
    assert labels[CLUSTER_OVERRIDE] == "Supercluster"
    # the first pack wins although the other tag has the lower tag id
    assert labels[CLUSTER_VOTE_TIE] == "Zed Exchange"


def test_cli_list_bad_text(rc_db, tmp_path):
    out = tmp_path / "bad.json"
    res = CliRunner().invoke(
        tagpack_cli,
        [
            "quality",
            "-u",
            rc_db["url"],
            "list-bad-text",
            "--format",
            "json",
            "--out",
            str(out),
        ],
        catch_exceptions=False,
    )
    assert res.exit_code == 0, res.output
    rows = {r["example_subject"]: r for r in json.loads(out.read_text())}
    assert set(rows) == {"1Txt1", "1Txt2"}
    assert rows["1Txt1"]["problem"] == "mis-encoded"
    assert rows["1Txt1"]["suggested_fix"] == "Name: Иван"
    assert rows["1Txt2"]["value"] == "bell\\x07label"
    assert rows["1Txt2"]["pack_uri"] == "https://example.com/tp1.yaml"
    assert "2 bad values in 1 packs" in res.stderr
