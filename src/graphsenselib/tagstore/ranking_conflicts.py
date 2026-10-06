"""Tagstore side of the ranking-conflict / actor-conflict detection.

Read-only. Loads what REST would load for an address (direct tags via
``get_tags_by_subjectids``, the cluster's best definer via
``get_best_cluster_tags_for_clusters``) and hands it to the pure checks in
``algorithms.ranking_conflicts``. Cluster ids come from the tagstore's
address->cluster mapping rather than Cassandra, so a run needs nothing but
the tagstore; only addresses the feeder has mapped get a cluster.
"""

import asyncio
import csv
import json
import logging
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass, field
from typing import (
    Awaitable,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Sequence,
    Tuple,
)

from sqlalchemy import BigInteger, String, bindparam
from sqlalchemy.dialects.postgresql import ARRAY
from sqlmodel import text
from sqlmodel.ext.asyncio.session import AsyncSession

from graphsenselib.utils.constants import FRESH_CLUSTER_ID_OFFSET

from .algorithms.ranking_conflicts import (
    EXCHANGE_CONCEPT,
    ActorConflictFinding,
    ActorRelations,
    ActorStats,
    AddressRankingFinding,
    ClusterContext,
    ClusterRankingFinding,
    DefinerGroup,
    address_actor_conflict,
    cluster_actor_conflict,
    evaluate_address,
    evaluate_cluster,
    rest_digest_config,
)
from .algorithms.tag_digest import TagDigestComputationConfig
from .db.queries import (
    TagPublic,
    TagstoreDbAsync,
)

logger = logging.getLogger(__name__)

CLUSTERING_CHOICES = ("auto", "legacy", "v2", "both")

# Stands in for "exchange-category tag without an actor" among a cluster's
# member exchange actors.
NO_ACTOR = "<no actor>"


def _regime_tables(fresh: bool) -> Tuple[str, str, int]:
    """(mapping table, best-tag view, raw->public id shift) of one regime."""
    if fresh:
        return (
            "address_cluster_mapping_v2",
            "best_cluster_tag_v2",
            (FRESH_CLUSTER_ID_OFFSET),
        )
    return "address_cluster_mapping", "best_cluster_tag", 0


def _groups_param(groups: List[str]):
    return bindparam("groups", value=list(groups), type_=ARRAY(String))


def _batches(items: Sequence, size: int) -> Iterable[Sequence]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


async def _rows(db: TagstoreDbAsync, stmt) -> List[tuple]:
    async with AsyncSession(db.engine) as session:
        return list(await session.exec(stmt))


async def all_acl_groups(db: TagstoreDbAsync) -> List[str]:
    rows = await _rows(db, text("SELECT DISTINCT acl_group FROM tagpack ORDER BY 1"))
    return [r[0] for r in rows]


async def exchange_actors(db: TagstoreDbAsync) -> set:
    rows = await _rows(
        db,
        text(
            "SELECT DISTINCT actor_id FROM actor_concept WHERE concept_id = :c"
        ).bindparams(c=EXCHANGE_CONCEPT),
    )
    return {r[0] for r in rows}


async def actor_relations(db: TagstoreDbAsync) -> ActorRelations:
    """Pairs from the actors' ``same_as`` and ``related_actors`` (actor
    context), which ``actor-conflicts`` does not report."""
    rows = await _rows(
        db,
        text(
            "SELECT id, context FROM actor "
            "WHERE context LIKE '%same_as%' OR context LIKE '%related_actors%'"
        ),
    )
    pairs = []
    for actor_id, context in rows:
        try:
            ctx = json.loads(context)
        except (TypeError, ValueError):
            continue
        for other in (ctx.get("same_as") or []) + (ctx.get("related_actors") or []):
            pairs.append((actor_id, other))
    return ActorRelations(pairs)


async def resolve_regimes(
    db: TagstoreDbAsync, network: str, clustering: str
) -> List[Tuple[str, bool]]:
    """[(name, fresh)] to evaluate. ``auto`` mirrors REST, which reads fresh
    cluster ids where the keyspace has fresh clustering: v2 if the feeder has
    mapped any address of the network into ``address_cluster_mapping_v2``."""
    if clustering == "legacy":
        return [("legacy", False)]
    if clustering == "v2":
        return [("v2", True)]
    if clustering == "both":
        return [("legacy", False), ("v2", True)]
    if clustering != "auto":
        raise ValueError(f"clustering must be one of {CLUSTERING_CHOICES}")
    rows = await _rows(
        db,
        text(
            "SELECT 1 FROM address_cluster_mapping_v2 WHERE network = :n LIMIT 1"
        ).bindparams(n=network),
    )
    return [("v2", True)] if rows else [("legacy", False)]


def _cids_param(public_ids: Optional[Sequence[int]], shift: int):
    raw = [int(c) - shift for c in public_ids or ()]
    return bindparam("cids", value=raw, type_=ARRAY(BigInteger))


async def _map_batches(
    items: Sequence,
    size: int,
    fn: Callable[[Sequence], Awaitable],
    concurrency: int,
    label: str,
) -> List:
    """``fn`` over ``items`` in batches, at most ``concurrency`` in flight
    (each batch takes its own pooled connection). Results in batch order."""
    batches = list(_batches(list(items), size))
    if not batches:
        return []
    sem = asyncio.Semaphore(concurrency)
    done = 0
    step = max(1, len(batches) // 20)

    async def run(batch):
        nonlocal done
        async with sem:
            r = await fn(batch)
        done += 1
        if done % step == 0 or done == len(batches):
            logger.info(f"{label}: {done}/{len(batches)} batches")
        return r

    return await asyncio.gather(*(run(b) for b in batches))


async def load_cluster_contexts(
    db: TagstoreDbAsync,
    network: str,
    groups: List[str],
    fresh: bool,
    cluster_ids: Optional[Sequence[int]] = None,
    batch_size: int = 1000,
    concurrency: int = 4,
) -> Dict[int, ClusterContext]:
    """Multi-address clusters with definer tags, keyed by public id; all of
    them, or only ``cluster_ids`` (public ids).

    Definer tags are aggregated in SQL into (actor, category, confidence,
    is_exchange) groups; only the tag REST selects is loaded as an object.
    Singleton clusters are left out: their "definers" are just the address's
    own tags (see the best_cluster_tag view), which REST never adds as an
    inherited tag and which the address-level checks already cover.
    """
    acm, bct, shift = _regime_tables(fresh)
    only = "AND b.cluster_id = ANY(:cids)" if cluster_ids is not None else ""
    params = [
        bindparam("network", value=network),
        _groups_param(groups),
        bindparam("exchange", value=EXCHANGE_CONCEPT),
    ]
    if cluster_ids is not None:
        params.append(_cids_param(cluster_ids, shift))
    rows = await _rows(
        db,
        text(
            f"""
            WITH d AS (
                SELECT b.cluster_id,
                       NULLIF(t.actor, '') AS actor,
                       c.level,
                       t.tagpack,
                       (SELECT coalesce(
                                 min(tc.concept_id) FILTER (
                                   WHERE tc.concept_relation_annotation_id
                                         = 'primary'),
                                 min(tc.concept_id))
                        FROM tag_concept tc WHERE tc.tag_id = t.id) AS category,
                       (t.tag_type <> 'mention' AND (
                          EXISTS (SELECT 1 FROM tag_concept tc
                                  WHERE tc.tag_id = t.id
                                    AND tc.concept_id = :exchange)
                          OR t.actor IN (SELECT actor_id FROM actor_concept
                                         WHERE concept_id = :exchange)))
                         AS is_exchange
                FROM {bct} b
                JOIN tag t ON t.id = b.tag_id
                JOIN tagpack tp ON tp.id = t.tagpack
                JOIN confidence c ON c.id = t.confidence
                WHERE b.network = :network
                  AND tp.acl_group = ANY(:groups)
                  {only}
            ), g AS (
                SELECT cluster_id, actor, category, level, is_exchange,
                       tagpack, count(*) AS n
                FROM d
                GROUP BY cluster_id, actor, category, level, is_exchange,
                         tagpack
            )
            SELECT g.cluster_id, g.actor, g.category, g.level, g.is_exchange,
                   g.n, g.tagpack, n.no_addr
            FROM g
            CROSS JOIN LATERAL (
                SELECT a.gs_cluster_no_addr AS no_addr
                FROM {acm} a
                WHERE a.network = :network AND a.gs_cluster_id = g.cluster_id
                LIMIT 1
            ) n
            WHERE n.no_addr > 1
            """
        ).bindparams(*params),
    )

    definers: Dict[int, List[DefinerGroup]] = defaultdict(list)
    n_addr: Dict[int, Optional[int]] = {}
    for raw_cid, actor, category, level, is_ex, n, tagpack, no_addr in rows:
        cid = raw_cid + shift
        definers[cid].append(
            DefinerGroup(
                actor=actor,
                category=category,
                confidence=level,
                is_exchange=bool(is_ex),
                n_tags=n,
                tagpack=tagpack,
            )
        )
        n_addr[cid] = no_addr
    logger.info(
        f"{network}: {len(definers)} multi-address clusters with definers, "
        f"{sum(g.n_tags for gs in definers.values() for g in gs)} definer tags"
    )

    # The one REST picks, through the very query REST runs. On a confidence
    # tie it is decided only by the tie-break, which is what DEFINER_TIE flags.
    selected = await select_cluster_tags(
        db, network, groups, sorted(definers), batch_size, concurrency
    )

    return {
        cid: ClusterContext(
            cluster_id=cid,
            n_addresses=n_addr[cid],
            selected=selected.get(cid),
            definers=tuple(
                sorted(gs, key=lambda g: (-g.confidence, str(g.actor), str(g.category)))
            ),
        )
        for cid, gs in definers.items()
    }


async def select_cluster_tags(
    db: TagstoreDbAsync,
    network: str,
    groups: List[str],
    cluster_ids: Sequence[int],
    batch_size: int = 1000,
    concurrency: int = 4,
) -> Dict[int, TagPublic]:
    """Best cluster tag per public cluster id, through the query REST runs."""

    async def pick(batch):
        return await db.get_best_cluster_tags_for_clusters(list(batch), network, groups)

    selected: Dict[int, TagPublic] = {}
    for part in await _map_batches(
        cluster_ids,
        batch_size,
        pick,
        concurrency,
        f"{network} cluster picks",
    ):
        selected.update(part)
    return selected


async def list_multi_address_clusters(
    db: TagstoreDbAsync,
    network: str,
    groups: List[str],
    fresh: bool,
    cluster_ids: Optional[Sequence[int]] = None,
) -> Dict[int, int]:
    """{public id: address count} of multi-address clusters with definers."""
    acm, bct, shift = _regime_tables(fresh)
    only = "AND b.cluster_id = ANY(:cids)" if cluster_ids is not None else ""
    params = [bindparam("network", value=network), _groups_param(groups)]
    if cluster_ids is not None:
        params.append(_cids_param(cluster_ids, shift))
    rows = await _rows(
        db,
        text(
            f"""
            SELECT g.cluster_id, n.no_addr
            FROM (
                SELECT DISTINCT b.cluster_id
                FROM {bct} b
                JOIN tag t ON t.id = b.tag_id
                JOIN tagpack tp ON tp.id = t.tagpack
                WHERE b.network = :network
                  AND tp.acl_group = ANY(:groups)
                  {only}
            ) g
            CROSS JOIN LATERAL (
                SELECT a.gs_cluster_no_addr AS no_addr
                FROM {acm} a
                WHERE a.network = :network AND a.gs_cluster_id = g.cluster_id
                LIMIT 1
            ) n
            WHERE n.no_addr > 1
            """
        ).bindparams(*params),
    )
    return {cid + shift: no_addr for cid, no_addr in rows}


async def load_member_exchange_actors(
    db: TagstoreDbAsync,
    network: str,
    groups: List[str],
    fresh: bool,
    cluster_ids: Optional[Sequence[int]] = None,
) -> Dict[int, Tuple[set, float]]:
    """Per multi-address cluster with exchange-tagged members: the actors of
    those exchange tags (``NO_ACTOR`` for exchange-category tags without one)
    and the share of tagged members that carry one."""
    acm, _, shift = _regime_tables(fresh)
    only = "AND a.gs_cluster_id = ANY(:cids)" if cluster_ids is not None else ""
    params = [
        bindparam("network", value=network),
        _groups_param(groups),
        bindparam("exchange", value=EXCHANGE_CONCEPT),
    ]
    if cluster_ids is not None:
        params.append(_cids_param(cluster_ids, shift))
    rows = await _rows(
        db,
        text(
            f"""
            SELECT cid,
                   array_agg(DISTINCT actor) FILTER (WHERE is_ex),
                   count(DISTINCT identifier) FILTER (WHERE is_ex),
                   count(DISTINCT identifier)
            FROM (
                SELECT a.gs_cluster_id AS cid,
                       t.identifier,
                       coalesce(NULLIF(t.actor, ''), :no_actor) AS actor,
                       (t.tag_type <> 'mention' AND (
                          EXISTS (SELECT 1 FROM tag_concept tc
                                  WHERE tc.tag_id = t.id
                                    AND tc.concept_id = :exchange)
                          OR t.actor IN (SELECT actor_id FROM actor_concept
                                         WHERE concept_id = :exchange))) AS is_ex
                FROM {acm} a
                JOIN tag t ON t.identifier = a.address AND t.network = a.network
                JOIN tagpack tp ON tp.id = t.tagpack
                WHERE a.network = :network
                  AND a.gs_cluster_no_addr > 1
                  AND tp.acl_group = ANY(:groups)
                  {only}
            ) m
            GROUP BY cid
            HAVING bool_or(is_ex)
            """
        ).bindparams(*params, bindparam("no_actor", value=NO_ACTOR)),
    )
    return {
        cid + shift: (set(actors), n_ex / n_all) for cid, actors, n_ex, n_all in rows
    }


async def load_member_actor_stats(
    db: TagstoreDbAsync,
    network: str,
    groups: List[str],
    fresh: bool,
    cluster_ids: Optional[Sequence[int]] = None,
) -> Tuple[Dict[int, Dict[str, ActorStats]], Dict[int, int]]:
    """Per multi-address cluster, per actor: stats over the network's tags on
    member addresses known to the tagstore mapping. Also returns each
    cluster's address count."""
    acm, _, shift = _regime_tables(fresh)
    only = "AND a.gs_cluster_id = ANY(:cids)" if cluster_ids is not None else ""
    params = [bindparam("network", value=network), _groups_param(groups)]
    if cluster_ids is not None:
        params.append(_cids_param(cluster_ids, shift))
    rows = await _rows(
        db,
        text(
            f"""
            SELECT a.gs_cluster_id,
                   t.actor,
                   count(DISTINCT t.id),
                   count(DISTINCT t.identifier),
                   max(c.level),
                   array_agg(DISTINCT coalesce(tp.uri, tp.title)),
                   array_agg(DISTINCT tp.creator),
                   array_remove(array_agg(DISTINCT tc.concept_id), NULL),
                   bool_or(t.is_cluster_definer),
                   max(a.gs_cluster_no_addr)
            FROM {acm} a
            JOIN tag t ON t.identifier = a.address AND t.network = a.network
            JOIN tagpack tp ON tp.id = t.tagpack
            JOIN confidence c ON c.id = t.confidence
            LEFT JOIN tag_concept tc ON tc.tag_id = t.id
            WHERE a.network = :network
              AND a.gs_cluster_no_addr > 1
              AND tp.acl_group = ANY(:groups)
              AND NULLIF(t.actor, '') IS NOT NULL
              {only}
            GROUP BY a.gs_cluster_id, t.actor
            """
        ).bindparams(*params),
    )
    out: Dict[int, Dict[str, ActorStats]] = defaultdict(dict)
    n_addr: Dict[int, int] = {}
    for cid, actor, n_tags, n_ad, max_c, tps, crs, cats, is_def, no_addr in rows:
        n_addr[cid + shift] = no_addr
        out[cid + shift][actor] = ActorStats(
            actor=actor,
            n_tags=n_tags,
            n_addresses=n_ad,
            max_confidence=max_c,
            tagpacks=sorted(x for x in tps if x),
            creators=sorted(x for x in crs if x),
            categories=sorted(cats),
            is_definer=bool(is_def),
        )
    return dict(out), n_addr


async def _candidate_addresses(
    db: TagstoreDbAsync, sql: str, network: str, groups: List[str], limit
) -> List[str]:
    if limit:
        sql += f" LIMIT {int(limit)}"
    params = [bindparam("network", value=network), _groups_param(groups)]
    if ":exchange" in sql:
        params.append(bindparam("exchange", value=EXCHANGE_CONCEPT))
    rows = await _rows(db, text(sql).bindparams(*params))
    return [r[0] for r in rows]


_EXCHANGE_CANDIDATES_SQL = """
    SELECT DISTINCT t.identifier
    FROM tag t
    JOIN tagpack tp ON tp.id = t.tagpack
    WHERE t.network = :network
      AND tp.acl_group = ANY(:groups)
      AND t.tag_type <> 'mention'  -- see is_exchange_tag
      AND (
        EXISTS (SELECT 1 FROM tag_concept tc
                WHERE tc.tag_id = t.id AND tc.concept_id = :exchange)
        OR t.actor IN (SELECT actor_id FROM actor_concept
                       WHERE concept_id = :exchange)
      )
    ORDER BY t.identifier
"""

_MULTI_ACTOR_CANDIDATES_SQL = """
    SELECT t.identifier
    FROM tag t
    JOIN tagpack tp ON tp.id = t.tagpack
    WHERE t.network = :network
      AND tp.acl_group = ANY(:groups)
      AND NULLIF(t.actor, '') IS NOT NULL
    GROUP BY t.identifier
    HAVING count(DISTINCT t.actor) > 1
    ORDER BY t.identifier
"""


async def _address_clusters(
    db: TagstoreDbAsync,
    network: str,
    addresses: Sequence[str],
    fresh: bool,
    batch_size: int,
    concurrency: int,
) -> Dict[str, int]:
    acm, _, shift = _regime_tables(fresh)

    async def lookup(batch):
        return await _rows(
            db,
            text(
                f"SELECT address, gs_cluster_id FROM {acm} "
                "WHERE network = :network AND address = ANY(:addrs)"
            ).bindparams(
                bindparam("network", value=network),
                bindparam("addrs", value=list(batch), type_=ARRAY(String)),
            ),
        )

    parts = await _map_batches(
        addresses, batch_size * 10, lookup, concurrency, f"{network} cluster ids"
    )
    return {addr: cid + shift for rows in parts for addr, cid in rows}


@dataclass
class DetectionResult:
    address_findings: List[AddressRankingFinding] = field(default_factory=list)
    cluster_findings: List[ClusterRankingFinding] = field(default_factory=list)
    address_conflicts: List[ActorConflictFinding] = field(default_factory=list)
    cluster_conflicts: List[ActorConflictFinding] = field(default_factory=list)
    # per "check/regime": candidate addresses, of those with a cluster
    # mapping, and clusters checked
    n_candidates: Counter = field(default_factory=Counter)
    n_mapped: Counter = field(default_factory=Counter)
    n_clusters: Counter = field(default_factory=Counter)


async def detect(
    db: TagstoreDbAsync,
    network: str,
    groups: Optional[List[str]] = None,
    clustering: str = "auto",
    limit: Optional[int] = None,
    addresses: Optional[Sequence[str]] = None,
    ranking: bool = True,
    actors: bool = True,
    relations: Optional[ActorRelations] = None,
    config: Optional[TagDigestComputationConfig] = None,
    batch_size: int = 1000,
    concurrency: int = 4,
) -> DetectionResult:
    """Run B1 (``ranking``) and/or B2 (``actors``) for one network.

    ``groups`` defaults to every ACL group in the tagstore, i.e. the internal
    view. ``relations`` (actor pairs that are no conflict) default to the
    actorpacks' ``same_as`` / ``related_actors``. ``limit`` caps the candidate addresses per check and restricts the
    cluster-level checks to those addresses' clusters; without it every
    multi-address cluster is checked. ``addresses`` replaces the candidate
    query with the given addresses and, like ``limit``, restricts the
    cluster checks to their clusters.
    """
    network = network.upper()
    config = config or rest_digest_config()
    if groups is None:
        groups = await all_acl_groups(db)
    ex_actors = await exchange_actors(db)
    result = DetectionResult()

    for regime, fresh in await resolve_regimes(db, network, clustering):
        label = f"{network}/{regime}"

        async def candidates_and_clusters(sql, check):
            if addresses is not None:
                cands = list(dict.fromkeys(a.strip() for a in addresses))
            else:
                cands = await _candidate_addresses(db, sql, network, groups, limit)
            key = f"{check}/{regime}"
            result.n_candidates[key] = len(cands)
            logger.info(f"{label}: {len(cands)} {check} candidate addresses")
            cluster_of = await _address_clusters(
                db, network, cands, fresh, batch_size, concurrency
            )
            result.n_mapped[key] = len(cluster_of)
            restrict = limit or addresses is not None
            only = sorted(set(cluster_of.values())) if restrict else None
            return key, cands, cluster_of, only

        if ranking:
            key, cands, cluster_of, only = await candidates_and_clusters(
                _EXCHANGE_CANDIDATES_SQL, "ranking"
            )
            clusters = await load_cluster_contexts(
                db, network, groups, fresh, only, batch_size, concurrency
            )
            result.n_clusters[key] = len(clusters)
            member_ex = await load_member_exchange_actors(
                db, network, groups, fresh, only
            )

            async def rank(batch):
                tags_by = await db.get_tags_by_subjectids(list(batch), groups)
                found = []
                for addr in batch:
                    cid = cluster_of.get(addr)
                    f = evaluate_address(
                        network,
                        addr,
                        tags_by.get(addr, []),
                        clusters.get(cid) if cid is not None else None,
                        ex_actors,
                        config=config,
                        clustering=regime,
                    )
                    if f is not None:
                        found.append(f)
                return found

            for part in await _map_batches(
                cands, batch_size, rank, concurrency, f"{label} addresses"
            ):
                result.address_findings.extend(part)

            for cid, ctx in clusters.items():
                ex_members, ex_share = member_ex.get(cid, (set(), None))
                f = evaluate_cluster(
                    network,
                    regime,
                    ctx,
                    ex_actors,
                    ex_members,
                    config=config,
                    member_exchange_share=ex_share,
                )
                if f is not None:
                    result.cluster_findings.append(f)

        if actors:
            if relations is None:
                relations = await actor_relations(db)
            key, cands, cluster_of, only = await candidates_and_clusters(
                _MULTI_ACTOR_CANDIDATES_SQL, "actors"
            )

            async def conflicts(batch):
                tags_by = await db.get_tags_by_subjectids(list(batch), groups)
                found = []
                for addr in batch:
                    f = address_actor_conflict(
                        network,
                        addr,
                        tags_by.get(addr, []),
                        relations,
                        cluster_id=cluster_of.get(addr),
                        clustering=regime,
                    )
                    if f is not None:
                        found.append(f)
                return found

            for part in await _map_batches(
                cands, batch_size, conflicts, concurrency, f"{label} addresses"
            ):
                result.address_conflicts.extend(part)

            clusters = await load_cluster_contexts(
                db, network, groups, fresh, only, batch_size, concurrency
            )
            members, member_n_addr = await load_member_actor_stats(
                db, network, groups, fresh, only
            )
            result.n_clusters[key] = len(set(clusters) | set(members))
            for cid in sorted(set(clusters) | set(members)):
                ctx = clusters.get(cid) or ClusterContext(
                    cluster_id=cid, n_addresses=member_n_addr.get(cid), selected=None
                )
                f = cluster_actor_conflict(
                    network, ctx, members.get(cid, {}), relations, regime
                )
                if f is not None:
                    result.cluster_conflicts.append(f)

    return result


RANKING_COLUMNS = [
    "level",
    "network",
    "clustering",
    "cluster_id",
    "n_addresses",
    "address",
    "reasons",
    "best_label",
    "best_actor",
    "broad_category",
    "hidden_exchange_actors",
    "hidden_exchange_labels",
    "hidden_exchange_max_confidence",
    "actor_confidence_threshold",
    "winner_label",
    "winner_actor",
    "winner_tag_type",
    "winner_confidence",
    "winner_inherited",
    "winner_tagpack_uri",
    "selected_label",
    "selected_actor",
    "selected_category",
    "selected_confidence",
    "selected_identifier",
    "selected_tagpack_uri",
    "majority_actor",
    "actor_weights",
    "n_definers",
    "exchange_member_actors",
    "exchange_member_share",
]

ACTOR_COLUMNS = [
    "level",
    "network",
    "clustering",
    "cluster_id",
    "n_addresses",
    "address",
    "reasons",
    "selected_label",
    "selected_actor",
    "actors",
]


def _sort_key(row: dict):
    return (
        row["level"],
        row["network"],
        row["clustering"],
        row["cluster_id"] if row["cluster_id"] is not None else -1,
        row["address"] or "",
    )


def ranking_rows(result: DetectionResult) -> List[dict]:
    rows = []
    for f in result.cluster_findings:
        rows.append({"level": "cluster", "address": None, **asdict(f)})
    for f in result.address_findings:
        rows.append({"level": "address", **asdict(f)})
    rows = [{c: r.get(c) for c in RANKING_COLUMNS} for r in rows]
    for r in rows:
        r["reasons"] = [x.value for x in r["reasons"]]
    return sorted(rows, key=_sort_key)


# Big clusters collect hundreds of tagpacks per actor; past this many the
# list is cut and ends in "+N more".
MAX_LISTED = 5


def _capped(items: List[str]) -> List[str]:
    if len(items) <= MAX_LISTED:
        return items
    return items[:MAX_LISTED] + [f"+{len(items) - MAX_LISTED} more"]


def actor_rows(result: DetectionResult) -> List[dict]:
    rows = []
    for f in result.cluster_conflicts + result.address_conflicts:
        d = asdict(f)
        for a in d["actors"]:
            for k in ("tagpacks", "creators", "categories"):
                a[k] = _capped(a[k])
        d["reasons"] = [x.value for x in f.reasons]
        rows.append({c: d.get(c) for c in ACTOR_COLUMNS})
    return sorted(rows, key=_sort_key)


def _flat_actor(a: dict) -> str:
    flags = [k for k in ("is_definer", "is_selected_definer") if a.get(k)]
    parts = [
        f"tags={a['n_tags']}",
        f"addrs={a['n_addresses']}",
        f"max_conf={a['max_confidence']}",
        *[f.removeprefix("is_") for f in flags],
        f"categories={','.join(a['categories'])}",
        f"creators={','.join(a['creators'])}",
        f"tagpacks={','.join(a['tagpacks'])}",
    ]
    return f"{a['actor']}[{'; '.join(parts)}]"


def _flat(value) -> str:
    if value is None:
        return ""
    if isinstance(value, dict):
        return "|".join(f"{k}:{v:g}" for k, v in value.items())
    if isinstance(value, list):
        return " | ".join(
            _flat_actor(v) if isinstance(v, dict) else str(v) for v in value
        )
    if isinstance(value, float):
        return f"{value:g}"
    return str(value)


def write_rows(rows: List[dict], columns: List[str], fmt: str, out) -> None:
    if fmt == "json":
        json.dump(rows, out, indent=1, ensure_ascii=False)
        out.write("\n")
        return
    w = csv.DictWriter(out, fieldnames=columns, lineterminator="\n")
    w.writeheader()
    for r in rows:
        w.writerow({c: _flat(r[c]) for c in columns})


def reason_counts(rows: List[dict]) -> Dict[str, Counter]:
    counts: Dict[str, Counter] = defaultdict(Counter)
    for r in rows:
        counts[r["level"]]["rows"] += 1
        counts[r["level"]].update(r["reasons"])
    return dict(counts)
