"""Inherited cluster tags resolve via the FRESH cluster id when active.

An address merged into a big cluster only by fresh clustering (its legacy
cluster is an unmerged delta-updater singleton — the legacy path assigns
every new address its own cluster id and never re-clusters) must still
inherit the cluster's tags. The tagstore cluster relations are self-routing
on public ids: fresh ids are shifted above ``FRESH_CLUSTER_ID_OFFSET`` and
route to the ``*_v2`` relations, legacy ids to the v1 relations. So the tag
lookup has to be fed the fresh public id; fed the legacy singleton id it
finds nothing and the address renders untagged ("possible service") even
though its fresh cluster carries a cluster-definer tag.

Only the tagstore lookups switch — ``tag.entity`` fields keep the legacy id
(the public API exposes ``entity`` as legacy plus ``fresh_cluster_id``
separately).

DB-free: real TagsService methods over fake db/tagstore boundaries.
"""

import asyncio
import logging

from graphsenselib.db.asynchronous.services.models import AddressTagQueryInput
from graphsenselib.db.asynchronous.services.tags_service import (
    MockConceptProtocol,
    TagsService,
)
from graphsenselib.tagstore.db import TagPublic
from graphsenselib.tagstore.db.queries import InheritedFrom
from graphsenselib.utils.constants import to_public_fresh_cluster_id

DEPOSIT_ADDRESS = "3Q1CZNKeFGcch3vGzPwWZsRL17N7pjvyky"
CLUSTER_DEFINER_ADDRESS = "3AmDN4AXBTuhRMLar63PeU7eu59uBdGqE8"
ADDRESS_ID = 1523823048
LEGACY_SINGLETON_ID = 1523823048
FRESH_RAW_CLUSTER_ID = 319038931
FRESH_PUBLIC_CLUSTER_ID = to_public_fresh_cluster_id(FRESH_RAW_CLUSTER_ID)


class FakeDb:
    """Keyspace where fresh clustering merged the address but legacy did not."""

    def __init__(self, fresh_active):
        self.fresh_active = fresh_active

    async def get_address_id_id_group(self, currency, address):
        return ADDRESS_ID, ADDRESS_ID // 5000

    async def get_fresh_cluster_id(self, currency, address_id):
        if not self.fresh_active:
            return None
        return FRESH_PUBLIC_CLUSTER_ID

    async def get_address_entity_id(self, currency, address):
        return LEGACY_SINGLETON_ID


class FakeTagstore:
    """Tags exist only under the cluster ids in ``best_by_public_id``."""

    def __init__(self, best_by_public_id):
        self.best_by_public_id = best_by_public_id

    async def get_tags_by_subjectid(self, address, offset, limit, groups):
        return []

    async def get_tags_by_subjectids(self, subject_ids, groups, network=None):
        return {}

    async def get_best_cluster_tag(self, cluster_id, currency, groups):
        return self.best_by_public_id.get(cluster_id)

    async def get_best_cluster_tags_for_clusters(self, cluster_ids, network, groups):
        return {
            cid: self.best_by_public_id[cid]
            for cid in cluster_ids
            if cid in self.best_by_public_id
        }


def _kraken_cluster_tag():
    return TagPublic(
        identifier=CLUSTER_DEFINER_ADDRESS,
        label="Kraken.com",
        source="https://kraken.com",
        creator="test",
        confidence="ownership",
        confidence_level=100,
        tag_subject="address",
        tag_type="actor",
        actor="kraken",
        primary_concept="exchange",
        additional_concepts=[],
        is_cluster_definer=True,
        network="BTC",
        lastmod=0,
        group="public",
        inherited_from=InheritedFrom.CLUSTER,
        tagpack_title="tp",
        tagpack_uri=None,
    )


def _make_service(db, tagstore):
    return TagsService(db, tagstore, MockConceptProtocol(), logging.getLogger(__name__))


def test_tag_summary_inherits_cluster_tag_via_fresh_id():
    # v2 world: the kraken tag is only reachable under the fresh public id.
    svc = _make_service(
        FakeDb(fresh_active=True),
        FakeTagstore({FRESH_PUBLIC_CLUSTER_ID: _kraken_cluster_tag()}),
    )
    summary = asyncio.run(
        svc.get_tag_summary_by_addresses(
            [AddressTagQueryInput(network="btc", address=DEPOSIT_ADDRESS)],
            ["public"],
            include_best_cluster_tag=True,
        )
    )
    assert summary.best_actor == "kraken"
    assert summary.best_label == "Kraken.com"


def test_batch_tag_summaries_inherit_cluster_tag_via_fresh_id():
    svc = _make_service(
        FakeDb(fresh_active=True),
        FakeTagstore({FRESH_PUBLIC_CLUSTER_ID: _kraken_cluster_tag()}),
    )
    summaries = asyncio.run(
        svc.get_tag_summaries_by_subject_ids(
            "btc",
            [DEPOSIT_ADDRESS],
            ["public"],
            include_best_cluster_tag=True,
        )
    )
    assert summaries[DEPOSIT_ADDRESS].best_actor == "kraken"


def test_legacy_lookup_unchanged_when_fresh_inactive():
    # v1 world: fresh clustering off, the tag lives under the legacy id.
    svc = _make_service(
        FakeDb(fresh_active=False),
        FakeTagstore({LEGACY_SINGLETON_ID: _kraken_cluster_tag()}),
    )
    summary = asyncio.run(
        svc.get_tag_summary_by_addresses(
            [AddressTagQueryInput(network="btc", address=DEPOSIT_ADDRESS)],
            ["public"],
            include_best_cluster_tag=True,
        )
    )
    assert summary.best_actor == "kraken"
