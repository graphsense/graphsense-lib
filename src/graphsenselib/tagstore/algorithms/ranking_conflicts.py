"""Find exchange attributions hidden by the tag summary, and actor conflicts.

Pure functions over ``TagPublic`` lists; the database side lives in
``graphsenselib.tagstore.ranking_conflicts``. The ranking itself is never
reimplemented here: every address is run through ``compute_tag_digest`` with
the configuration REST uses, on the tag list REST builds (direct tags plus the
cluster's best definer tag), and the reason codes only explain the result.
"""

import math
from collections import Counter, defaultdict
from dataclasses import dataclass, field, replace
from enum import Enum
from typing import Dict, FrozenSet, Iterable, List, Optional, Sequence, Set, Tuple

from ..db import InheritedFrom, TagPublic
from .tag_digest import (
    TagDigestComputationConfig,
    _normalizeWord,
    compute_actor_confidence_threshold,
    compute_tag_digest,
)

EXCHANGE_CONCEPT = "exchange"

# Share of a cluster's tagged members that must carry an exchange tag before
# the members alone make a non-exchange cluster definer suspicious.
MIN_MEMBER_EXCHANGE_SHARE = 0.5


# The checks' estimate of which actor a cluster's definers point to: each
# tagpack votes once per actor it names, weighted exp(confidence /
# CLUSTER_VOTE_SCALE) by its best tag for that actor, so every 10 confidence
# points count about 2.7 times as much, a bulk-generated pack does not
# outvote curated tags by volume, and a swarm of weak packs only outvotes a
# strong one in large numbers (one at 50 = 20 at 20). Used to report
# DEFINER_NOT_MAJORITY; REST does not vote.
CLUSTER_VOTE_SCALE = 10.0


def rest_digest_config() -> TagDigestComputationConfig:
    """The digest configuration the REST tag_summary endpoints use."""
    return TagDigestComputationConfig().with_only_propagate_high_confidence_actors(True)


class Reason(str, Enum):
    # address level (B1)
    INHERITED_WINS = "INHERITED_WINS"
    ACTOR_THRESHOLD_DROPPED = "ACTOR_THRESHOLD_DROPPED"
    EXCHANGE_TAG_NOT_ACTOR_TYPE = "EXCHANGE_TAG_NOT_ACTOR_TYPE"
    MENTION_WINS = "MENTION_WINS"
    DIRECT_HIGHER_CONF = "DIRECT_HIGHER_CONF"
    DIRECT_OUTVOTED = "DIRECT_OUTVOTED"
    # address and cluster level (B1)
    NON_EXCHANGE_CLUSTER_DEFINER = "NON_EXCHANGE_CLUSTER_DEFINER"
    DEFINER_TIE = "DEFINER_TIE"
    # cluster level only (B1)
    DEFINER_NOT_MAJORITY = "DEFINER_NOT_MAJORITY"
    # actor conflicts (B2)
    ADDRESS_ACTORS_DISAGREE = "ADDRESS_ACTORS_DISAGREE"
    DEFINERS_DISAGREE = "DEFINERS_DISAGREE"
    DEFINER_WITHOUT_ACTOR = "DEFINER_WITHOUT_ACTOR"
    DEFINER_VS_MEMBERS = "DEFINER_VS_MEMBERS"
    MEMBERS_DISAGREE = "MEMBERS_DISAGREE"


_REASON_ORDER = {r: i for i, r in enumerate(Reason)}


def _sorted_reasons(reasons: Iterable[Reason]) -> List[Reason]:
    return sorted(set(reasons), key=_REASON_ORDER.__getitem__)


def tag_actor(t: TagPublic) -> Optional[str]:
    if t.actor is None or not t.actor.strip():
        return None
    return t.actor


def tag_category(t: TagPublic) -> Optional[str]:
    """Primary concept, else the alphabetically first one (the same rule the
    SQL aggregation in ``load_cluster_contexts`` applies)."""
    return t.primary_concept or (min(t.concepts) if t.concepts else None)


def is_inherited(t: TagPublic) -> bool:
    return t.inherited_from in (InheritedFrom.CLUSTER, InheritedFrom.PUBKEY_AND_CLUSTER)


def is_exchange_tag(t: TagPublic, exchange_actors: Set[str]) -> bool:
    """An exchange attribution: exchange category/concept, or an actor whose
    actorpack says exchange. Mentions never count: a mention says where an
    address was seen (e.g. on a dark-web page), and its category describes
    that context, not who owns the address."""
    if t.tag_type == "mention":
        return False
    return EXCHANGE_CONCEPT in t.concepts or tag_actor(t) in exchange_actors


def _stable_key(t: TagPublic) -> Tuple:
    return (-t.confidence_level, t.tagpack_uri or "", t.label, t.source, t.creator)


def order_like_rest(
    direct_tags: Sequence[TagPublic], inherited: Optional[TagPublic]
) -> List[TagPublic]:
    """Tag list as ``TagsService.list_tags_by_address_raw`` builds it.

    Direct tags by confidence descending (the DB orders by level only; the
    secondary keys just make runs reproducible), the inherited cluster tag
    inserted before the first direct tag of lower confidence. Order matters:
    the digest's ``most_common`` breaks weight ties by insertion order.
    """
    tags = sorted(direct_tags, key=_stable_key)
    if inherited is not None:
        pos = next(
            (
                i
                for i, t in enumerate(tags)
                if t.confidence_level < inherited.confidence_level
            ),
            len(tags),
        )
        tags.insert(pos, inherited)
    return tags


def inherited_tag(address: str, selected: Optional[TagPublic]) -> Optional[TagPublic]:
    """The cluster tag REST adds to an address: none if the address is the
    definer itself (its own tag is already a direct tag)."""
    if selected is None or selected.identifier == address:
        return None
    return selected


def address_summary(
    address: str,
    direct_tags: Sequence[TagPublic],
    selected: Optional[TagPublic],
    config: Optional[TagDigestComputationConfig] = None,
):
    """The tag digest REST serves for an address whose cluster's best tag is
    ``selected``."""
    tags = order_like_rest(direct_tags, inherited_tag(address, selected))
    return compute_tag_digest(tags, config=config or rest_digest_config())


# ---------------------------------------------------------------- clusters


@dataclass(frozen=True)
class DefinerGroup:
    """Definer tags of one cluster sharing actor, category and confidence.

    Clusters are checked on these aggregates rather than on the tags: a
    big exchange cluster carries thousands of definer tags, and loading them
    all as objects does not scale to a whole network.
    """

    actor: Optional[str]
    category: Optional[str]
    confidence: int
    is_exchange: bool
    n_tags: int = 1
    # the tags' tagpack; None counts the group as a pack of its own
    tagpack: Optional[str] = None


def definer_groups_from_tags(
    tags: Iterable[TagPublic], exchange_actors: Set[str]
) -> Tuple[DefinerGroup, ...]:
    counts: Counter = Counter(
        (
            tag_actor(t),
            tag_category(t),
            t.confidence_level,
            is_exchange_tag(t, exchange_actors),
            t.tagpack_uri,
        )
        for t in tags
    )
    return tuple(
        DefinerGroup(
            actor=a, category=c, confidence=conf, is_exchange=ex, n_tags=n, tagpack=tp
        )
        for (a, c, conf, ex, tp), n in sorted(counts.items(), key=lambda kv: str(kv[0]))
    )


@dataclass(frozen=True)
class ClusterContext:
    """A cluster's definer tags (aggregated) and the one REST picked."""

    cluster_id: int
    n_addresses: Optional[int]
    selected: Optional[TagPublic]
    definers: Tuple[DefinerGroup, ...] = ()


def _definer_identity(actor: Optional[str], category: Optional[str]) -> Tuple:
    # Tags of one actor are interchangeable for the summary whatever their
    # category; tags without an actor only by category.
    return (actor, None) if actor is not None else (None, category)


def definer_tie(ctx: ClusterContext) -> bool:
    """The selected definer ties on confidence with a definer of another actor
    (or, both without actor, another category), so which of them wins is
    decided only by the tie-break (tagpack id, then tag order), not by the
    evidence."""
    sel = ctx.selected
    if sel is None:
        return False
    identity = _definer_identity(tag_actor(sel), tag_category(sel))
    return any(
        g.confidence == sel.confidence_level
        and _definer_identity(g.actor, g.category) != identity
        for g in ctx.definers
    )


def definer_actor_weights(definers: Iterable[DefinerGroup]) -> Dict[str, float]:
    """Vote per actor (see CLUSTER_VOTE_SCALE): one vote per tagpack, weighted
    by the pack's best tag for that actor."""
    best: Dict[Tuple[str, object], int] = {}
    for i, g in enumerate(definers):
        if g.actor is not None:
            key = (g.actor, g.tagpack if g.tagpack is not None else i)
            best[key] = max(best.get(key, 0), g.confidence or 0)
    weights: Dict[str, float] = defaultdict(float)
    for (actor, _), level in best.items():
        weights[actor] += math.exp(level / CLUSTER_VOTE_SCALE)
    return dict(weights)


def majority_actor(weights: Dict[str, float]) -> Tuple[Optional[str], bool]:
    """(majority actor, whether the best vote is tied). A tie has no majority
    actor, so it never goes to whichever actor id sorts first."""
    if not weights:
        return None, False
    ranked = sorted(weights.items(), key=lambda kv: (-kv[1], kv[0]))
    tied = len(ranked) > 1 and ranked[1][1] >= ranked[0][1] * (1 - 1e-9)
    return (None if tied else ranked[0][0]), tied


@dataclass
class ClusterRankingFinding:
    network: str
    clustering: str
    cluster_id: int
    n_addresses: Optional[int]
    selected_label: Optional[str]
    selected_actor: Optional[str]
    selected_category: Optional[str]
    selected_confidence: Optional[int]
    selected_identifier: Optional[str]
    selected_tagpack_uri: Optional[str]
    majority_actor: Optional[str]
    actor_weights: Dict[str, float]
    n_definers: int
    exchange_member_actors: List[str]
    exchange_member_share: Optional[float]
    reasons: List[Reason]


def evaluate_cluster(
    network: str,
    clustering: str,
    ctx: ClusterContext,
    exchange_actors: Set[str],
    member_exchange_actors: Set[str],
    config: Optional[TagDigestComputationConfig] = None,
    member_exchange_share: Optional[float] = None,
) -> Optional[ClusterRankingFinding]:
    """Cluster-level pass: is the selected definer the one the definers agree on?

    ``member_exchange_actors`` are exchange actors on direct tags of the
    cluster's member addresses (definers or not); ``member_exchange_share``
    the share of tagged members that carry an exchange tag. Members alone
    make a non-exchange definer suspicious only from
    ``MIN_MEMBER_EXCHANGE_SHARE`` on, so one stray exchange tag in a payment
    processor's cluster does not flag it.
    """
    config = config or rest_digest_config()
    sel = ctx.selected
    if sel is None:
        return None

    weights = definer_actor_weights(ctx.definers)
    major, major_tied = majority_actor(weights)

    reasons = set()
    if definer_tie(ctx) or major_tied:
        reasons.add(Reason.DEFINER_TIE)
    if major is not None and tag_actor(sel) != major:
        reasons.add(Reason.DEFINER_NOT_MAJORITY)
    sel_actor = tag_actor(sel)
    members_say_exchange = bool(member_exchange_actors - {sel_actor}) and (
        member_exchange_share is None
        or member_exchange_share >= MIN_MEMBER_EXCHANGE_SHARE
    )
    other_exchange_definer = any(
        g.is_exchange and (g.actor is None or g.actor != sel_actor)
        for g in ctx.definers
    )
    if not is_exchange_tag(sel, exchange_actors) and (
        members_say_exchange or other_exchange_definer
    ):
        reasons.add(Reason.NON_EXCHANGE_CLUSTER_DEFINER)

    if not reasons:
        return None

    return ClusterRankingFinding(
        network=network,
        clustering=clustering,
        cluster_id=ctx.cluster_id,
        n_addresses=ctx.n_addresses,
        selected_label=sel.label,
        selected_actor=tag_actor(sel),
        selected_category=tag_category(sel),
        selected_confidence=sel.confidence_level,
        selected_identifier=sel.identifier,
        selected_tagpack_uri=sel.tagpack_uri,
        majority_actor=major,
        actor_weights=dict(sorted(weights.items())),
        n_definers=sum(g.n_tags for g in ctx.definers),
        exchange_member_actors=sorted(member_exchange_actors),
        exchange_member_share=member_exchange_share,
        reasons=_sorted_reasons(reasons),
    )


# --------------------------------------------------------------- addresses


@dataclass
class AddressRankingFinding:
    network: str
    clustering: str
    address: str
    cluster_id: Optional[int]
    n_addresses: Optional[int]
    best_label: Optional[str]
    best_actor: Optional[str]
    broad_category: str
    hidden_exchange_actors: List[str]
    hidden_exchange_labels: List[str]
    hidden_exchange_max_confidence: int
    actor_confidence_threshold: float
    winner_label: Optional[str]
    winner_actor: Optional[str]
    winner_tag_type: Optional[str]
    winner_confidence: Optional[int]
    winner_tagpack_uri: Optional[str]
    winner_inherited: bool
    reasons: List[Reason]


def _winning_tag(
    tags: Sequence[TagPublic], best_key: Optional[str], best_actor: Optional[str]
) -> Optional[TagPublic]:
    """The tag that put ``best_label`` on top: highest confidence among the tags
    with that (normalised) label, restricted to ``best_actor`` when set. An
    inherited tag counts only if no direct tag carries the label, mirroring
    the digest's ``inherited_from`` display flag."""
    if best_key is None:
        return None
    matching = [
        t
        for t in tags
        if _normalizeWord(t.label) == best_key
        and (best_actor is None or tag_actor(t) == best_actor)
    ]
    direct = [t for t in matching if not is_inherited(t)]
    pool = direct or matching
    if not pool:
        return None
    return max(pool, key=lambda t: t.confidence_level)


def evaluate_address(
    network: str,
    address: str,
    direct_tags: Sequence[TagPublic],
    cluster: Optional[ClusterContext],
    exchange_actors: Set[str],
    config: Optional[TagDigestComputationConfig] = None,
    clustering: str = "",
    relations: Optional["ActorRelations"] = None,
) -> Optional[AddressRankingFinding]:
    """B1 for one address: does the tag summary hide its exchange attribution?

    Flags when ``best_actor`` is none of the address's exchange actors (nor
    the same organisation as one or nested in one, see
    ActorRelations.shows_attributed) and
    ``best_label`` is not the label of one of its exchange tags.
    """
    config = config or rest_digest_config()

    exchange_tags = [t for t in direct_tags if is_exchange_tag(t, exchange_actors)]
    if not exchange_tags:
        return None

    inherited = inherited_tag(address, cluster.selected if cluster else None)
    tags = order_like_rest(direct_tags, inherited)
    digest = compute_tag_digest(tags, config=config)

    ex_actors = {a for a in (tag_actor(t) for t in exchange_tags) if a is not None}
    ex_labels = {_normalizeWord(t.label) for t in exchange_tags}
    best_key = _normalizeWord(digest.best_label) if digest.best_label else None

    if digest.best_actor in ex_actors or best_key in ex_labels:
        return None
    if (
        relations is not None
        and digest.best_actor is not None
        and any(relations.shows_attributed(digest.best_actor, a) for a in ex_actors)
    ):
        return None

    threshold = compute_actor_confidence_threshold(tags, config)
    max_ex_conf = max(t.confidence_level for t in exchange_tags)
    winner = _winning_tag(tags, best_key, digest.best_actor)

    reasons = set()
    if any(
        t.tag_type == "actor"
        and tag_actor(t) is not None
        and (t.confidence_level or 0.1) < threshold
        for t in exchange_tags
    ):
        reasons.add(Reason.ACTOR_THRESHOLD_DROPPED)
    if config.only_propagate_high_confidence_actors and any(
        tag_actor(t) is not None and t.tag_type != "actor" for t in exchange_tags
    ):
        reasons.add(Reason.EXCHANGE_TAG_NOT_ACTOR_TYPE)
    if winner is not None:
        if is_inherited(winner):
            reasons.add(Reason.INHERITED_WINS)
        if winner.tag_type == "mention":
            reasons.add(Reason.MENTION_WINS)
        elif not is_inherited(winner):
            reasons.add(
                Reason.DIRECT_HIGHER_CONF
                if winner.confidence_level > max_ex_conf
                else Reason.DIRECT_OUTVOTED
            )
    if inherited is not None and not is_exchange_tag(inherited, exchange_actors):
        reasons.add(Reason.NON_EXCHANGE_CLUSTER_DEFINER)
    if cluster is not None and definer_tie(cluster):
        reasons.add(Reason.DEFINER_TIE)

    return AddressRankingFinding(
        network=network,
        clustering=clustering,
        address=address,
        cluster_id=cluster.cluster_id if cluster is not None else None,
        n_addresses=cluster.n_addresses if cluster is not None else None,
        best_label=digest.best_label,
        best_actor=digest.best_actor,
        broad_category=digest.broad_concept,
        hidden_exchange_actors=sorted(ex_actors),
        hidden_exchange_labels=sorted({t.label for t in exchange_tags}),
        hidden_exchange_max_confidence=max_ex_conf,
        actor_confidence_threshold=threshold,
        winner_label=winner.label if winner else None,
        winner_actor=tag_actor(winner) if winner else None,
        winner_tag_type=winner.tag_type if winner else None,
        winner_confidence=winner.confidence_level if winner else None,
        winner_tagpack_uri=winner.tagpack_uri if winner else None,
        winner_inherited=is_inherited(winner) if winner else False,
        reasons=_sorted_reasons(reasons),
    )


# ---------------------------------------------------------- actor conflicts


@dataclass
class ActorStats:
    actor: str
    n_tags: int = 0
    n_addresses: int = 0
    max_confidence: int = 0
    tagpacks: List[str] = field(default_factory=list)
    creators: List[str] = field(default_factory=list)
    categories: List[str] = field(default_factory=list)
    is_definer: bool = False
    is_selected_definer: bool = False


def actor_stats_from_tags(tags: Iterable[TagPublic]) -> Dict[str, ActorStats]:
    acc: Dict[str, dict] = {}
    for t in tags:
        actor = tag_actor(t)
        if actor is None:
            continue
        a = acc.setdefault(
            actor,
            {
                "n": 0,
                "addr": set(),
                "max": 0,
                "tp": set(),
                "cr": set(),
                "cat": set(),
                "def": False,
            },
        )
        a["n"] += 1
        a["addr"].add(t.identifier)
        a["max"] = max(a["max"], t.confidence_level)
        a["tp"].add(t.tagpack_uri or t.tagpack_title)
        a["cr"].add(t.creator)
        a["cat"].update(t.concepts)
        a["def"] = a["def"] or t.is_cluster_definer
    return {
        actor: ActorStats(
            actor=actor,
            n_tags=a["n"],
            n_addresses=len(a["addr"]),
            max_confidence=a["max"],
            tagpacks=sorted(a["tp"]),
            creators=sorted(a["cr"]),
            categories=sorted(a["cat"]),
            is_definer=a["def"],
        )
        for actor, a in sorted(acc.items())
    }


class ActorRelations:
    """Actor pairs that may tag the same addresses without a conflict.

    Built from the actorpacks' ``same_as`` (same organisation: rebrand,
    duplicate entry), ``sub_service_of`` (a product or division of the same
    organisation), ``nested_in`` (a separate organisation running on
    another's addresses or accounts) and ``related_actors`` (distinct, but
    legitimately on the same addresses). Pairs are unordered, except
    ``nested`` (service, host).
    """

    def __init__(
        self,
        pairs: Iterable[Tuple[str, str]] = (),
        same_org: Iterable[Tuple[str, str]] = (),
        nested: Iterable[Tuple[str, str]] = (),
    ):
        self.same_org: Set[FrozenSet[str]] = {frozenset(p) for p in same_org}
        self.nested: Set[Tuple[str, str]] = set(nested)
        self.pairs: Set[FrozenSet[str]] = (
            {frozenset(p) for p in pairs}
            | self.same_org
            | {frozenset(p) for p in self.nested}
        )

    def same_organisation(self, a: str, b: str) -> bool:
        return a == b or frozenset((a, b)) in self.same_org

    def shows_attributed(self, shown: str, attributed: str) -> bool:
        """Showing actor ``shown`` hides nothing about ``attributed``: the same
        organisation, or a service nested in it (the more specific operator).
        Showing the host of a nested service does hide it."""
        return (
            self.same_organisation(shown, attributed)
            or (shown, attributed) in self.nested
        )

    def covers(self, actors: Iterable[str]) -> bool:
        """True if every pair of distinct actors in ``actors`` is related."""
        actors = sorted(set(actors))
        return all(
            frozenset((a, b)) in self.pairs
            for i, a in enumerate(actors)
            for b in actors[i + 1 :]
        )


@dataclass
class ActorConflictFinding:
    level: str  # "address" | "cluster"
    network: str
    clustering: str
    address: Optional[str]
    cluster_id: Optional[int]
    n_addresses: Optional[int]
    selected_label: Optional[str]
    selected_actor: Optional[str]
    actors: List[ActorStats]
    reasons: List[Reason]


def address_actor_conflict(
    network: str,
    address: str,
    direct_tags: Sequence[TagPublic],
    relations: Optional[ActorRelations] = None,
    cluster_id: Optional[int] = None,
    clustering: str = "",
) -> Optional[ActorConflictFinding]:
    """B2 address level: two or more distinct actors among the direct tags."""
    stats = actor_stats_from_tags(direct_tags)
    if len(stats) < 2:
        return None
    if relations is not None and relations.covers(stats):
        return None
    return ActorConflictFinding(
        level="address",
        network=network,
        clustering=clustering,
        address=address,
        cluster_id=cluster_id,
        n_addresses=None,
        selected_label=None,
        selected_actor=None,
        actors=list(stats.values()),
        reasons=[Reason.ADDRESS_ACTORS_DISAGREE],
    )


def cluster_actor_conflict(
    network: str,
    ctx: ClusterContext,
    member_stats: Dict[str, ActorStats],
    relations: Optional[ActorRelations] = None,
    clustering: str = "",
) -> Optional[ActorConflictFinding]:
    """B2 cluster level, comparing the selected definer with the other definers
    and with the actors on member addresses' direct tags (``member_stats``,
    which includes the definers' own tags). A cluster without any definer is
    flagged only if its members carry two or more actors."""
    sel = ctx.selected
    sel_actor = tag_actor(sel) if sel is not None else None
    definer_actors = {g.actor for g in ctx.definers if g.actor}
    member_actors = set(member_stats)

    reasons = set()
    if len(definer_actors) > 1:
        reasons.add(Reason.DEFINERS_DISAGREE)
    if sel is not None and sel_actor is None and (definer_actors or member_actors):
        reasons.add(Reason.DEFINER_WITHOUT_ACTOR)
    if sel_actor is not None and member_actors - {sel_actor}:
        reasons.add(Reason.DEFINER_VS_MEMBERS)
    if sel is None and len(member_actors) > 1:
        reasons.add(Reason.MEMBERS_DISAGREE)
    if not reasons:
        return None

    involved = definer_actors | member_actors | ({sel_actor} if sel_actor else set())
    if (
        relations is not None
        and len(involved) > 1
        and Reason.DEFINER_WITHOUT_ACTOR not in reasons
        and relations.covers(involved)
    ):
        return None

    actors = []
    for actor in sorted(involved):
        s = member_stats.get(actor) or ActorStats(actor=actor)
        actors.append(
            replace(
                s,
                is_definer=s.is_definer or actor in definer_actors,
                is_selected_definer=actor == sel_actor,
            )
        )

    return ActorConflictFinding(
        level="cluster",
        network=network,
        clustering=clustering,
        address=None,
        cluster_id=ctx.cluster_id,
        n_addresses=ctx.n_addresses,
        selected_label=sel.label if sel is not None else None,
        selected_actor=sel_actor,
        actors=actors,
        reasons=_sorted_reasons(reasons),
    )
