import re
from collections import Counter, defaultdict
from typing import Dict, List, Optional

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


class wCounter:
    def __init__(self):
        self.wctr = Counter()
        self.ctr = Counter()

    def add(self, item, weight=1):
        self.ctr.update({item: 1})
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

    return 1.0


def _skipTag(t) -> bool:
    return False


def _calcTagCloud(wctr: wCounter, at_most=None) -> Dict[str, TagCloudEntry]:
    total_weight = wctr.get_total(weighted=True)
    return {
        word: TagCloudEntry(count=wctr.get(word), weighted=cnt / total_weight)
        for word, cnt in wctr.most_common(n=at_most, weighted=True)
    }


def compute_tag_digest(tags: List[TagPublic]) -> TagDigest:
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
            "inherited": False,
        }
    )

    def add_tag_data(t, tags_count: int, total_words: int, tags_count_cluster: int):
        if not _skipTag(t):
            conf = t.confidence_level or 0.1

            tags_count += 1

            if t.inherited_from == InheritedFrom.CLUSTER:
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
            full_label_counter.add(nlabel, conf)

            # add actor
            if t.actor:
                actor_labels[t.actor].add(nlabel, weight=conf)
                actor_counter.add(t.actor, weight=conf)

            if t.concepts:
                for x in t.concepts:
                    concepts_counter.add(x, weight=conf * _get_concept_weight(x))

                    ls["concepts"].add(x)
            else:
                # tags without categorization are added to unknown category in wordcloud
                x = "unknown"
                concepts_counter.add(x, weight=conf * _get_concept_weight(x))

                ls["concepts"].add(x)

            ls["cnt"] += 1
            ls["lbl"] = t.label
            ls["src"].add(t.source)
            ls["creators"].add(t.creator)
            ls["sumConfidence"] += conf
            ls["lastmod"] = max(ls["lastmod"], t.lastmod)
            ls["inherited"] = (
                t.inherited_from == InheritedFrom.CLUSTER and (ls["inherited"])
            )

        return tags_count, total_words, tags_count_cluster

    for t in tags:
        tags_count, total_words, tags_count_cluster = add_tag_data(
            t, tags_count, total_words, tags_count_cluster
        )

    # create a relevance score, prefer items where similar labels exist.
    sw_full_label_counter = wCounter()
    data = full_label_counter.most_common(weighted=True)
    for lbl, v in data:
        multiplier = sum(
            [
                occurrence
                for word, occurrence in label_word_counter.most_common()
                if word in lbl and occurrence > 1
            ]
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
            concepts=list(value["concepts"]),
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
    actor_mc = actor_counter.most_common(1, weighted=True)
    if len(actor_mc) > 0:
        p_actor = actor_mc[0][0]
        key = actor_labels[p_actor].most_common(1, weighted=True)[0][0]
        best_label = label_digest[key].label
    else:
        if len(full_label_counter) > 0:
            key = full_label_counter.most_common(1, weighted=True)[0][0]
            best_label = label_digest[key].label

    return TagDigest(
        broad_concept=broad_concept,
        nr_tags=tags_count,
        nr_tags_indirect=tags_count_cluster,
        best_actor=p_actor,
        best_label=best_label,
        concept_tag_cloud=_calcTagCloud(concepts_counter),
        label_digest=label_digest,
    )
