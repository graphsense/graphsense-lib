import re
from collections import Counter, defaultdict
from functools import lru_cache
from importlib.resources import files
from typing import Dict, FrozenSet, List, Optional

from pydantic import BaseModel

from ..db import InheritedFrom, TagPublic

_FILTER_WORDS = dict.fromkeys(["to", "in", "the", "by", "of", "at", "", "vault"], True)


class LabelDigest(BaseModel):
    label: str
    count: int
    confidence: float
    relevance: float
    creators: List[str]
    sources: List[str]
    concepts: List[str]
    lastmod: int
    inherited_from: Optional[str]


class TagCloudEntry(BaseModel):
    count: int
    weighted: float


class TagDigest(BaseModel):
    broad_concept: str
    nr_tags: int
    nr_tags_indirect: int
    best_actor: Optional[str]
    best_label: Optional[str]
    label_digest: Dict[str, LabelDigest]
    concept_tag_cloud: Dict[str, TagCloudEntry]
    # Why best_actor/best_label may be contested, in AMBIGUITIES order; empty
    # if nothing disagrees.
    ambiguities: List[str] = []


# The address's own actor tags name another actor than the tag it inherits
# from its cluster (whichever of them wins).
CLUSTER_TAG_VS_DIRECT = "cluster_tag_vs_direct"
# The address's own actor tags name two or more actors.
DIRECT_ACTORS_DISAGREE = "direct_actors_disagree"
# A non-mention tag with a risk concept (see is_risk_tag) is present, but
# best_label is not one.
RISK_LABEL_OUTRANKED = "risk_label_outranked"
AMBIGUITIES = (
    CLUSTER_TAG_VS_DIRECT,
    DIRECT_ACTORS_DISAGREE,
    RISK_LABEL_OUTRANKED,
)

# Concept subtrees (with all narrower concepts) that count as risk: abuse
# (scams, sanctions, blacklists, ...) and mixing.
RISK_ROOT_CONCEPTS = ("abuse", "gray_usage", "mixing_service")


class wCounter:
    def __init__(self):
        self.wctr = Counter()
        self.ctr = Counter()

    def add(self, item, weight=1.0):
        self.ctr.update({item: 1.0})
        self.wctr.update({item: weight})

    def update(self, items):
        self.ctr.update(items)
        self.wctr.update(items)

    def getcntr(self, weighted=False):
        return self.wctr if weighted else self.ctr

    def get_total(self, weighted=False):
        return sum(dict(self.getcntr(weighted)).values())

    def get(self, item, weighted=False):
        return self.getcntr(weighted)[item]

    def most_common(self, n=None, weighted=False):
        return self.getcntr(weighted).most_common(n)

    def __len__(self):
        return len(self.ctr)


def _map_concept_to_broad_concept(concept: str) -> str:
    if concept == "exchange":
        return concept
    else:
        return "entity"


def _remove_mulit_spaces(istr: str) -> str:
    return re.sub(" +", " ", istr)


def _normalizeWord(istr: str) -> str:
    return _remove_mulit_spaces(re.sub(r"[^0-9a-zA-Z_ ]+", " ", istr.strip().lower()))


def _get_concept_weight(c: str) -> float:
    if c == "defi":
        return 0.5
    elif c == "exchange":
        return 1.1
    elif c == "dark_web" or c == "unknown":
        return 0.1

    return 1.0


def _skipTag(t) -> bool:
    return False


def _calcTagCloud(wctr: wCounter, at_most=None) -> Dict[str, TagCloudEntry]:
    total_weight = wctr.get_total(weighted=True)
    return {
        word: TagCloudEntry(count=wctr.get(word), weighted=cnt / total_weight)
        for word, cnt in wctr.most_common(n=at_most, weighted=True)
    }


class TagDigestComputationConfig(BaseModel):
    only_propagate_high_confidence_actors: bool = False
    consider_n_confidence_buckets: int = 2
    max_confidence_drop: int = 20
    # Exponent applied to confidence_level when used as a ranking weight
    # (labels, actors, concepts). 1.0 = linear; higher values sharpen
    # emphasis on high-confidence tags. Does not affect the displayed
    # per-label confidence average or confidence-bucket thresholding.
    confidence_weight_exponent: float = 2.0

    def with_only_propagate_high_confidence_actors(self, only: bool = True):
        self.only_propagate_high_confidence_actors = only
        return self

    def with_confidence_weight_exponent(self, exponent: float):
        self.confidence_weight_exponent = exponent
        return self


@lru_cache(maxsize=1)
def risk_concepts() -> FrozenSet[str]:
    """RISK_ROOT_CONCEPTS and every concept below them in the bundled concept
    taxonomy."""
    import yaml

    data = yaml.safe_load(
        (files("graphsenselib.tagpack.db") / "concepts.yaml").read_text(
            encoding="utf-8"
        )
    )
    parent = {
        k: v.get("broader")
        for k, v in data.items()
        if isinstance(v, dict) and v.get("type") == "concept"
    }

    def is_risk(c: Optional[str]) -> bool:
        seen = set()
        while c is not None and c not in seen:
            if c in RISK_ROOT_CONCEPTS:
                return True
            seen.add(c)
            c = parent.get(c)
        return False

    return frozenset(c for c in parent if is_risk(c))


def is_risk_tag(t: TagPublic) -> bool:
    """A tag with a risk concept; mentions only report where an address was
    seen and do not count."""
    return t.tag_type != "mention" and not risk_concepts().isdisjoint(t.concepts)


def _is_cluster_inherited(t: TagPublic) -> bool:
    return t.inherited_from in (InheritedFrom.CLUSTER, InheritedFrom.PUBKEY_AND_CLUSTER)


def _actor_of(t: TagPublic) -> Optional[str]:
    return t.actor if t.actor is not None and t.actor.strip() else None


def _direct_actors(tags: List[TagPublic]) -> Dict[str, int]:
    """Actor -> highest confidence, over the direct actor-type tags."""
    out: Dict[str, int] = {}
    for t in tags:
        actor = _actor_of(t)
        if actor is not None and t.tag_type == "actor" and not _is_cluster_inherited(t):
            out[actor] = max(out.get(actor, 0), t.confidence_level or 0)
    return out


def tag_summary_ambiguities(
    tags: List[TagPublic], best_label_concepts: Optional[List[str]] = None
) -> List[str]:
    """Why the summary of ``tags`` may be contested; see AMBIGUITIES.

    best_label_concepts: the concepts of the summary's best label; None skips
    RISK_LABEL_OUTRANKED."""
    found = set()
    if (
        best_label_concepts is not None
        and risk_concepts().isdisjoint(best_label_concepts)
        and any(is_risk_tag(t) for t in tags)
    ):
        found.add(RISK_LABEL_OUTRANKED)
    direct = _direct_actors(tags)
    for t in tags:
        if _is_cluster_inherited(t) and set(direct) - {_actor_of(t)}:
            found.add(CLUSTER_TAG_VS_DIRECT)
    if len(direct) > 1:
        found.add(DIRECT_ACTORS_DISAGREE)
    return [a for a in AMBIGUITIES if a in found]


def compute_actor_confidence_threshold(
    tags: List[TagPublic], config: TagDigestComputationConfig
) -> float:
    """Minimum confidence an actor tag needs to count towards best_actor.

    0.0 unless only_propagate_high_confidence_actors is set. Exposed so the
    ranking-conflict detection can tell which actor tags the digest dropped.
    """
    if not config.only_propagate_high_confidence_actors:
        return 0.0

    confidences_for_actor_inheritance = set()
    for t in tags:
        if t.tag_type == "actor":
            conf = t.confidence_level or 0.1
            confidences_for_actor_inheritance.add(conf)

    highest_n = list(reversed(sorted(list(confidences_for_actor_inheritance))))[
        : config.consider_n_confidence_buckets
    ]

    # only keep confidences if drop is less than max_confidence_drop
    lastc = None
    considered_confs = []
    for c in highest_n:
        if lastc is not None and (lastc - c) > config.max_confidence_drop:
            break
        lastc = c
        considered_confs.append(c)

    return min(considered_confs) if len(considered_confs) > 0 else 0.0


def compute_tag_digest(
    tags: List[TagPublic],
    config: TagDigestComputationConfig = TagDigestComputationConfig(),
) -> TagDigest:
    tags_count = 0
    total_words = 0
    tags_count_cluster = 0
    actor_counter = wCounter()
    label_word_counter = wCounter()
    full_label_counter = wCounter()
    concepts_counter = wCounter()
    actor_labels = defaultdict(wCounter)

    label_summary = defaultdict(
        lambda: {
            "cnt": 0,
            "lbl": None,
            "src": set(),
            "sumConfidence": 0,
            "creators": set(),
            "concepts": set(),
            "lastmod": 0,
            # AND-folded across the label's tags below: stays True only if every
            # tag contributing to this label is inherited. Must start True (the
            # previous default of False made the flag permanently False).
            "inherited": True,
        }
    )

    def add_tag_data(
        t,
        tags_count: int,
        total_words: int,
        tags_count_cluster: int,
        actor_confidence_threshold: float = 0.0,
    ):
        if not _skipTag(t):
            conf = t.confidence_level or 0.1
            w = conf**config.confidence_weight_exponent

            tags_count += 1

            if (
                t.inherited_from == InheritedFrom.CLUSTER
                or t.inherited_from == InheritedFrom.PUBKEY_AND_CLUSTER
            ):
                tags_count_cluster += 1

            # compute words
            norm_words = [_normalizeWord(w) for w in _normalizeWord(t.label).split(" ")]
            filtered_words = [w for w in norm_words if w not in _FILTER_WORDS]
            total_words += len(filtered_words)

            # add words
            label_word_counter.update(Counter(filtered_words))

            # add labels
            nlabel = _normalizeWord(t.label)
            ls = label_summary[nlabel]
            full_label_counter.add(nlabel, w)

            # add actor
            if (
                t.actor is not None
                and len(t.actor.strip()) > 0
                and conf >= actor_confidence_threshold
                and (
                    not config.only_propagate_high_confidence_actors
                    or t.tag_type == "actor"
                )
            ):
                actor_labels[t.actor].add(nlabel, weight=w)
                actor_counter.add(t.actor, weight=w)

            if t.concepts:
                for x in t.concepts:
                    concepts_counter.add(x, weight=w * _get_concept_weight(x))

                    ls["concepts"].add(x)
            else:
                # tags without categorization are added to unknown category in wordcloud
                x = "unknown"
                concepts_counter.add(x, weight=w * _get_concept_weight(x))

                ls["concepts"].add(x)

            ls["cnt"] += 1
            ls["lbl"] = t.label
            ls["src"].add(t.source)
            ls["creators"].add(t.creator)
            ls["sumConfidence"] += conf
            ls["lastmod"] = max(ls["lastmod"], t.lastmod)
            ls["inherited"] = (
                t.inherited_from == InheritedFrom.CLUSTER
                or t.inherited_from == InheritedFrom.PUBKEY_AND_CLUSTER
            ) and (ls["inherited"])

        return tags_count, total_words, tags_count_cluster

    actor_confidence_threshold = compute_actor_confidence_threshold(tags, config)

    for t in tags:
        tags_count, total_words, tags_count_cluster = add_tag_data(
            t,
            tags_count,
            total_words,
            tags_count_cluster,
            actor_confidence_threshold=actor_confidence_threshold,
        )

    # create a relevance score, prefer items where similar labels exist.
    # Precompute the recurring words once (the sum is order-independent), instead
    # of re-sorting the whole word counter for every label.
    frequent_words = [
        (word, occurrence)
        for word, occurrence in label_word_counter.most_common()
        if occurrence > 1
    ]
    sw_full_label_counter = wCounter()
    data = full_label_counter.most_common(weighted=True)
    for lbl, v in data:
        multiplier = sum(
            occurrence for word, occurrence in frequent_words if word in lbl
        )
        n = 1 + multiplier / total_words if total_words > 0 else 1
        sw_full_label_counter.add(lbl, v * n)

    ltc = _calcTagCloud(sw_full_label_counter)

    label_digest = {
        key: LabelDigest(
            label=value["lbl"],
            count=value["cnt"],
            confidence=value["sumConfidence"] / (value["cnt"] * 100),
            relevance=ltc[key].weighted,
            creators=list(value["creators"]),
            sources=list(value["src"]),
            concepts=sorted(value["concepts"]),
            lastmod=value["lastmod"],
            inherited_from="cluster" if value["inherited"] else None,
        )
        for (key, value) in label_summary.items()
    }

    # get broad category
    broad_concept = "entity"
    if len(concepts_counter) > 0:
        broad_concept = _map_concept_to_broad_concept(
            concepts_counter.most_common(1, weighted=True)[0][0]
        )

    # get most common actor (weighted by tag confidence)
    # get best label (within actor if actor is specified)
    p_actor = None
    best_label = None
    best_key = None
    actor_mc = actor_counter.most_common(1, weighted=True)
    if len(actor_mc) > 0:
        p_actor = actor_mc[0][0]
        best_key = actor_labels[p_actor].most_common(1, weighted=True)[0][0]
    elif len(full_label_counter) > 0:
        best_key = full_label_counter.most_common(1, weighted=True)[0][0]
    if best_key is not None:
        best_label = label_digest[best_key].label

    return TagDigest(
        broad_concept=broad_concept,
        nr_tags=tags_count,
        nr_tags_indirect=tags_count_cluster,
        best_actor=p_actor,
        best_label=best_label,
        concept_tag_cloud=_calcTagCloud(concepts_counter),
        label_digest=label_digest,
        ambiguities=tag_summary_ambiguities(
            tags, label_digest[best_key].concepts if best_key is not None else None
        ),
    )
