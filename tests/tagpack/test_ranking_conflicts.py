"""Unit tests for the tag-summary ranking / actor conflict checks.

Synthetic, anonymised tag lists, no database. The tie-case fixture keeps
only the tag structure of a real case: an exchange hot wallet with weak
exchange tags, inside a cluster whose best tag is an actorless ICO tag tied
at confidence 90 with two exchange definers.
"""

import math

import pytest

pytest.importorskip("sqlmodel", reason="tagstore extras required")

from graphsenselib.tagstore.algorithms.ranking_conflicts import (  # noqa: E402
    ActorRelations,
    ActorStats,
    ClusterContext,
    definer_groups_from_tags,
    Reason,
    address_actor_conflict,
    cluster_actor_conflict,
    evaluate_address,
    evaluate_cluster,
    order_like_rest,
)
from graphsenselib.tagstore.db.queries import InheritedFrom, TagPublic  # noqa: E402

EXCHANGES = {"exchange_a", "exchange_b", "exchange_c"}
ADDR = "1TestHotWa11et0000000000000000000"
ICO_ADDR = "1TestIcoDefiner000000000000000000"


def tag(
    label,
    conf,
    actor=None,
    category=None,
    tag_type="actor",
    identifier=ADDR,
    inherited=False,
    definer=False,
    uri="https://example.com/pack.yaml",
    extra_concepts=(),
):
    return TagPublic(
        identifier=identifier,
        label=label,
        source="src",
        creator="creator",
        confidence=f"c{conf}",
        confidence_level=conf,
        tag_subject="address",
        tag_type=tag_type,
        actor=actor,
        primary_concept=category,
        additional_concepts=list(extra_concepts),
        is_cluster_definer=definer,
        network="BTC",
        lastmod=1642118400,
        group="public",
        inherited_from=InheritedFrom.CLUSTER if inherited else None,
        tagpack_title="pack",
        tagpack_uri=uri,
    )


def ico_definer(inherited=True):
    return tag(
        "ICO X",
        90,
        category="ico_wallet",
        identifier=ICO_ADDR,
        inherited=inherited,
        definer=True,
    )


def exchange_a_definer(conf, identifier):
    return tag(
        "Exchange A",
        conf,
        actor="exchange_a",
        category="exchange",
        identifier=identifier,
        definer=True,
    )


@pytest.fixture
def tie_cluster():
    definers = (
        ico_definer(inherited=False),
        exchange_a_definer(90, "1ExA1"),
        exchange_a_definer(90, "1ExA2"),
        *[exchange_a_definer(70, f"1ExA70_{i}") for i in range(23)],
        tag(
            "Exchange B",
            70,
            actor="exchange_b",
            category="exchange",
            identifier="1ExB",
            definer=True,
        ),
    )
    return ClusterContext(
        cluster_id=1001,
        n_addresses=50_000,
        selected=ico_definer(inherited=True),
        definers=definer_groups_from_tags(definers, EXCHANGES),
    )


@pytest.fixture
def hot_wallet_tags():
    return [
        tag("Exchange A Hot Wallet", 20, actor="exchange_a", category="exchange"),
        tag(
            "Exchange A Hot Wallet",
            20,
            actor="exchange_a",
            category="exchange",
            uri="https://example.com/other.yaml",
        ),
        tag(
            "Transaction mention",
            20,
            category="hacking",
            tag_type="mention",
            extra_concepts=["dark_web"],
        ),
    ]


def test_tie_case_address(hot_wallet_tags, tie_cluster):
    f = evaluate_address("BTC", ADDR, hot_wallet_tags, tie_cluster, EXCHANGES)
    assert f is not None
    # what REST serves today
    assert f.best_label == "ICO X"
    assert f.best_actor is None
    assert f.broad_category == "entity"
    assert f.actor_confidence_threshold == 90
    assert f.hidden_exchange_actors == ["exchange_a"]
    assert f.winner_inherited and f.winner_label == "ICO X"
    assert set(f.reasons) == {
        Reason.INHERITED_WINS,
        Reason.ACTOR_THRESHOLD_DROPPED,
        Reason.NON_EXCHANGE_CLUSTER_DEFINER,
        Reason.DEFINER_TIE,
    }
    assert f.cluster_id == 1001 and f.n_addresses == 50_000


def test_tie_cluster(tie_cluster):
    f = evaluate_cluster("BTC", "legacy", tie_cluster, EXCHANGES, {"exchange_a"})
    assert f is not None
    assert f.majority_actor == "exchange_a"
    assert f.selected_label == "ICO X"
    assert set(f.reasons) == {
        Reason.DEFINER_TIE,
        Reason.DEFINER_NOT_MAJORITY,
        Reason.NON_EXCHANGE_CLUSTER_DEFINER,
    }


def test_tie_cluster_actor_conflict(tie_cluster):
    members = {
        "exchange_a": ActorStats(actor="exchange_a", n_tags=40, max_confidence=90),
        "exchange_b": ActorStats(actor="exchange_b", n_tags=1, max_confidence=70),
    }
    f = cluster_actor_conflict("BTC", tie_cluster, members)
    assert f is not None
    assert set(f.reasons) == {
        Reason.DEFINERS_DISAGREE,
        Reason.DEFINER_WITHOUT_ACTOR,
    }
    assert [a.actor for a in f.actors] == ["exchange_a", "exchange_b"]
    assert all(a.is_definer for a in f.actors)
    assert members["exchange_a"].is_definer is False  # input not mutated


def test_exchange_winning_is_not_flagged():
    tags = [tag("Exchange C", 50, actor="exchange_c", category="exchange")]
    assert evaluate_address("BTC", ADDR, tags, None, EXCHANGES) is None


def test_address_without_exchange_tag_is_not_a_candidate():
    tags = [tag("Some ICO", 90, category="ico_wallet")]
    assert evaluate_address("BTC", ADDR, tags, None, EXCHANGES) is None


def test_exchange_by_actorpack_category_only():
    # no exchange category on the tag, but its actor is an exchange
    tags = [
        tag("hot wallet", 20, actor="exchange_c", category="wallet_service"),
        tag("Scam", 90, actor="scammer", category="scam"),
    ]
    f = evaluate_address("BTC", ADDR, tags, None, EXCHANGES)
    assert f is not None and f.hidden_exchange_actors == ["exchange_c"]


def test_mention_wins():
    tags = [
        tag("Exchange D deposit", 50, category="exchange"),
        tag("Mentioned in hack report", 90, category="hacking", tag_type="mention"),
    ]
    f = evaluate_address("BTC", ADDR, tags, None, EXCHANGES)
    assert f is not None
    assert Reason.MENTION_WINS in f.reasons
    assert Reason.DIRECT_HIGHER_CONF not in f.reasons


def test_direct_higher_conf():
    tags = [
        tag("Exchange C", 20, actor="exchange_c", category="exchange"),
        tag("Mixer X", 90, actor="mixerx", category="mixing_service"),
    ]
    f = evaluate_address("BTC", ADDR, tags, None, EXCHANGES)
    assert f is not None
    assert f.best_actor == "mixerx"
    assert Reason.DIRECT_HIGHER_CONF in f.reasons
    assert Reason.ACTOR_THRESHOLD_DROPPED in f.reasons


def test_direct_outvoted():
    tags = [
        tag("Kraken", 50, category="exchange"),
        tag("Gambling site", 50, category="gambling"),
        tag("Gambling site", 50, category="gambling", uri="https://x/2.yaml"),
    ]
    f = evaluate_address("BTC", ADDR, tags, None, EXCHANGES)
    assert f is not None
    assert f.reasons == [Reason.DIRECT_OUTVOTED]


def test_exchange_tag_not_actor_type():
    tags = [
        tag(
            "Exchange C",
            50,
            actor="exchange_c",
            category="exchange",
            tag_type="attribute",
        ),
        tag("Other", 60, category="gambling"),
    ]
    f = evaluate_address("BTC", ADDR, tags, None, EXCHANGES)
    assert f is not None
    assert Reason.EXCHANGE_TAG_NOT_ACTOR_TYPE in f.reasons


def test_inherited_tag_of_the_address_itself_is_not_added():
    # REST skips the best cluster tag when the address is its own definer
    own = tag("ICO X", 90, category="ico_wallet", definer=True)
    ctx = ClusterContext(cluster_id=1, n_addresses=5, selected=own)
    tags = [tag("Exchange A", 90, actor="exchange_a", category="exchange")]
    assert evaluate_address("BTC", ADDR, tags, ctx, EXCHANGES) is None


def test_no_tie_when_tied_definers_agree():
    a = exchange_a_definer(90, "1A")
    b = exchange_a_definer(90, "1B")
    ctx = ClusterContext(
        cluster_id=1,
        n_addresses=5,
        selected=a,
        definers=definer_groups_from_tags((a, b), EXCHANGES),
    )
    assert evaluate_cluster("BTC", "legacy", ctx, EXCHANGES, {"exchange_a"}) is None


def test_majority_tie_is_a_definer_tie():
    a = exchange_a_definer(70, "1A")
    b = tag("Exchange B", 70, actor="exchange_b", category="exchange", identifier="1B")
    ctx = ClusterContext(
        cluster_id=1,
        n_addresses=5,
        selected=a,
        definers=definer_groups_from_tags((a, b), EXCHANGES),
    )
    f = evaluate_cluster("BTC", "legacy", ctx, EXCHANGES, set())
    assert f is not None and Reason.DEFINER_TIE in f.reasons


def test_order_like_rest_inserts_inherited_after_equal_confidence():
    d90 = tag("A", 90)
    d20 = tag("B", 20)
    inh = ico_definer()
    assert order_like_rest([d20, d90], inh) == [d90, inh, d20]


def test_address_actor_conflict_and_relations():
    tags = [
        tag("Exchange C", 90, actor="exchange_c", category="exchange"),
        tag("Exchange A", 50, actor="exchange_a", category="exchange"),
        tag("Exchange A", 70, actor="exchange_a", category="exchange", uri="x"),
    ]
    f = address_actor_conflict("BTC", ADDR, tags)
    assert f is not None
    stats = {a.actor: a for a in f.actors}
    assert stats["exchange_a"].n_tags == 2 and stats["exchange_a"].max_confidence == 70
    allow = ActorRelations([("exchange_a", "exchange_c")])
    assert address_actor_conflict("BTC", ADDR, tags, allow) is None


def test_sub_service_is_no_conflict_and_not_hidden():
    # the address's exchange tag names the parent; a stronger tag names its
    # sub-service, which therefore wins the summary
    tags = [
        tag("Exchange A Pool", 90, actor="exchange_a_pool", category="mining_service"),
        tag("Exchange A", 20, actor="exchange_a", category="exchange"),
    ]
    assert address_actor_conflict("BTC", ADDR, tags) is not None
    assert evaluate_address("BTC", ADDR, tags, None, EXCHANGES) is not None

    rel = ActorRelations(same_org=[("exchange_a_pool", "exchange_a")])
    assert address_actor_conflict("BTC", ADDR, tags, rel) is None
    assert evaluate_address("BTC", ADDR, tags, None, EXCHANGES, relations=rel) is None
    assert rel.same_organisation("exchange_a", "exchange_a_pool")

    # a related (distinct) actor is no conflict, but still hides the exchange
    rel = ActorRelations(pairs=[("exchange_a_pool", "exchange_a")])
    assert address_actor_conflict("BTC", ADDR, tags, rel) is None
    assert evaluate_address("BTC", ADDR, tags, None, EXCHANGES, relations=rel)


def test_nested_service_counts_one_way():
    rel = ActorRelations(
        nested=[("service_x", "exchange_a"), ("exchange_b", "custodian")]
    )

    # a gambling service running on exchange_a's accounts is shown on an
    # exchange_a deposit address: the more specific operator, nothing hidden
    nested_shown = [
        tag("Service X", 90, actor="service_x", category="gambling"),
        tag("Exchange A Deposit", 20, actor="exchange_a", category="exchange"),
    ]
    assert evaluate_address("BTC", ADDR, nested_shown, None, EXCHANGES) is not None
    assert (
        evaluate_address("BTC", ADDR, nested_shown, None, EXCHANGES, relations=rel)
        is None
    )

    # exchange_b keeps its funds with a custodian; showing the custodian hides
    # the exchange, so it stays a finding
    host_shown = [
        tag("Custodian", 90, actor="custodian", category="wallet_service"),
        tag("Exchange B", 20, actor="exchange_b", category="exchange"),
    ]
    assert evaluate_address("BTC", ADDR, host_shown, None, EXCHANGES, relations=rel)

    # either way the pair is no actor conflict
    for tags in (nested_shown, host_shown):
        assert address_actor_conflict("BTC", ADDR, tags) is not None
        assert address_actor_conflict("BTC", ADDR, tags, rel) is None


def test_cluster_without_definer_members_disagree():
    ctx = ClusterContext(cluster_id=7, n_addresses=10, selected=None)
    members = {"a": ActorStats(actor="a"), "b": ActorStats(actor="b")}
    f = cluster_actor_conflict("BTC", ctx, members)
    assert f is not None and f.reasons == [Reason.MEMBERS_DISAGREE]
    assert cluster_actor_conflict("BTC", ctx, {"a": ActorStats(actor="a")}) is None


def test_same_actor_different_category_is_no_tie():
    a = tag(
        "Processor P",
        50,
        actor="processor_p",
        category="payment_processor",
        identifier="1A",
    )
    b = tag(
        "Processor P Online",
        50,
        actor="processor_p",
        category="exchange",
        identifier="1B",
    )
    ctx = ClusterContext(
        cluster_id=1,
        n_addresses=5,
        selected=a,
        definers=definer_groups_from_tags((a, b), EXCHANGES),
    )
    assert evaluate_cluster("BTC", "v2", ctx, EXCHANGES, set()) is None


def test_actorless_definers_of_different_category_tie():
    a = tag("Shop", 50, category="shop", identifier="1A")
    b = tag("Casino", 50, category="gambling", identifier="1B")
    ctx = ClusterContext(
        cluster_id=1,
        n_addresses=5,
        selected=a,
        definers=definer_groups_from_tags((a, b), EXCHANGES),
    )
    f = evaluate_cluster("BTC", "v2", ctx, EXCHANGES, set())
    assert f is not None and f.reasons == [Reason.DEFINER_TIE]


def test_stray_exchange_member_does_not_flag_non_exchange_definer():
    sel = tag("Processor Q", 50, actor="processor_q", category="payment_processor")
    ctx = ClusterContext(
        cluster_id=1,
        n_addresses=5,
        selected=sel,
        definers=definer_groups_from_tags((sel,), EXCHANGES),
    )
    assert (
        evaluate_cluster(
            "BTC", "v2", ctx, EXCHANGES, {"exchange_e"}, member_exchange_share=1 / 28
        )
        is None
    )
    f = evaluate_cluster(
        "BTC", "v2", ctx, EXCHANGES, {"exchange_e"}, member_exchange_share=0.6
    )
    assert f is not None and f.reasons == [Reason.NON_EXCHANGE_CLUSTER_DEFINER]
    assert f.exchange_member_share == 0.6


def test_own_actor_exchange_members_do_not_flag():
    sel = tag("Wallet W", 70, actor="wallet_w", category="wallet_service")
    ctx = ClusterContext(
        cluster_id=1,
        n_addresses=5,
        selected=sel,
        definers=definer_groups_from_tags((sel,), EXCHANGES),
    )
    assert (
        evaluate_cluster(
            "BTC", "v2", ctx, EXCHANGES, {"wallet_w"}, member_exchange_share=1.0
        )
        is None
    )


# ---- tag digest: ambiguities --------------------------------------------

from graphsenselib.tagstore.algorithms.tag_digest import (  # noqa: E402
    TagDigestComputationConfig,
    compute_tag_digest,
)


def _cfg():
    return TagDigestComputationConfig().with_only_propagate_high_confidence_actors(True)


def _inherited(conf, actor=None):
    return tag(
        "Cluster Label",
        conf,
        actor=actor,
        category="ico_wallet" if actor is None else "exchange",
        identifier="1Definer",
        inherited=True,
        definer=True,
    )


def _own(conf, actor="exchange_a", tag_type="actor"):
    return tag("Own Label", conf, actor=actor, category="exchange", tag_type=tag_type)


def test_digest_lets_inherited_tag_win():
    d = compute_tag_digest([_inherited(90), _own(50)], _cfg())
    assert d.best_label == "Cluster Label" and d.best_actor is None


def test_digest_ambiguities():
    agree = compute_tag_digest([_inherited(90, actor="exchange_a"), _own(50)], _cfg())
    assert agree.ambiguities == []

    d = compute_tag_digest(
        [_inherited(90), _own(50), _own(20, actor="exchange_b")], _cfg()
    )
    assert d.ambiguities == ["cluster_tag_vs_direct", "direct_actors_disagree"]
    # the address's own tags alone
    assert compute_tag_digest(
        [_own(50), _own(50, actor="exchange_b")], _cfg()
    ).ambiguities == ["direct_actors_disagree"]
    # a mention names no actor for the summary
    assert (
        compute_tag_digest(
            [_own(50), _own(50, actor="exchange_b", tag_type="mention")], _cfg()
        ).ambiguities
        == []
    )


def test_exchange_category_on_a_mention_is_no_exchange_attribution():
    # a dark-web mention categorised as exchange does not make the address
    # an exchange candidate
    tags = [
        tag("Dark Web", 20, category="exchange", tag_type="mention"),
        tag("Doubler scam", 50, category="scam", tag_type="mention"),
    ]
    assert evaluate_address("BTC", ADDR, tags, None, EXCHANGES) is None


def test_majority_vote_resists_a_swarm_of_weak_packs():
    from graphsenselib.tagstore.algorithms.ranking_conflicts import (
        DefinerGroup,
        definer_actor_weights,
        majority_actor,
    )

    def weak(n_packs, n_tags=1):
        return [
            DefinerGroup(
                actor="weak",
                category="exchange",
                confidence=20,
                is_exchange=True,
                n_tags=n_tags,
                tagpack=f"pack_{i}",
            )
            for i in range(n_packs)
        ]

    strong = DefinerGroup(
        actor="strong",
        category="exchange",
        confidence=50,
        is_exchange=True,
        tagpack="curated",
    )
    # under confidence^2 eleven tags at 20 (4,400) beat one at 50 (2,500)
    assert majority_actor(definer_actor_weights([strong, *weak(11)])) == (
        "strong",
        False,
    )
    # but enough packs still win: one pack at 50 is worth about 20 at 20
    assert majority_actor(definer_actor_weights([strong, *weak(21)]))[0] == "weak"
    # a single pack votes once, however many tags it holds
    assert (
        majority_actor(definer_actor_weights([strong, *weak(1, n_tags=1000)]))[0]
        == "strong"
    )


def test_a_pack_votes_once_at_its_best_confidence():
    from graphsenselib.tagstore.algorithms.ranking_conflicts import (
        DefinerGroup,
        definer_actor_weights,
    )

    groups = [
        DefinerGroup(
            actor="a",
            category=None,
            confidence=c,
            is_exchange=False,
            n_tags=5,
            tagpack="p",
        )
        for c in (20, 50)
    ]
    assert definer_actor_weights(groups) == pytest.approx({"a": math.exp(5.0)})


def test_risk_label_outranked():
    risk = tag("Scam Site", 50, category="scam")
    strong = tag("Exchange A", 100, actor="exchange_a", category="exchange")
    d = compute_tag_digest([risk, strong], _cfg())
    assert d.best_label == "Exchange A"
    assert d.ambiguities == ["risk_label_outranked"]

    # a mention with a risk concept is no risk attribution
    mention = tag("Forum Post", 20, category="scam", tag_type="mention")
    assert compute_tag_digest([mention, strong], _cfg()).ambiguities == []


def test_tied_vote_has_no_majority():
    from graphsenselib.tagstore.algorithms.ranking_conflicts import majority_actor

    assert majority_actor({"exchange_b": 8100.0, "exchange_a": 8100.0}) == (None, True)
    assert majority_actor({"exchange_b": 8100.0, "exchange_a": 4900.0}) == (
        "exchange_b",
        False,
    )
