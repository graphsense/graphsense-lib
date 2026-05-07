"""Parity tests: per-address get_tag_summary_by_address vs batched
get_tag_summaries_by_subject_ids must produce identical TagSummary
objects per address.

The batched path is the fix for the 2026-05-04 pool-exhaustion incident
(per-call AsyncSession fan-out). Behavioural equivalence with the
per-address path is the property that lets us replace one with the other.
"""

import logging
from typing import Dict, List, Optional

import pytest

from graphsenselib.db.asynchronous.services.tags_service import (
    MockConceptProtocol,
    TagsService,
)
from graphsenselib.errors import AddressNotFoundException
from graphsenselib.tagstore.db.queries import InheritedFrom, TagPublic


def _make_tag(
    identifier: str,
    label: str = "tag",
    confidence_level: int = 50,
    primary_concept: Optional[str] = None,
    inherited_from: Optional[InheritedFrom] = None,
    actor: Optional[str] = None,
) -> TagPublic:
    return TagPublic(
        identifier=identifier,
        label=label,
        source="test",
        creator="tester",
        confidence="confidence_id",
        confidence_level=confidence_level,
        tag_subject="address",
        tag_type="tag" if actor is None else "actor",
        actor=actor,
        primary_concept=primary_concept,
        additional_concepts=[],
        is_cluster_definer=False,
        network="btc",
        lastmod=0,
        group="public",
        inherited_from=inherited_from,
        tagpack_title="test pack",
        tagpack_uri=None,
    )


class FakeTagstore:
    """A tagstore mock that serves both singular and plural call shapes
    consistently from the same backing data — so the per-address and
    batched paths cannot disagree because of mock drift.
    """

    def __init__(
        self,
        tags_by_subject: Dict[str, List[TagPublic]],
        best_cluster_tag_by_cluster: Dict[int, Optional[TagPublic]],
    ):
        self.tags_by_subject = tags_by_subject
        self.best_cluster_tag_by_cluster = best_cluster_tag_by_cluster

    async def get_tags_by_subjectid(
        self,
        subject_id: str,
        offset: Optional[int],
        page_size: Optional[int],
        groups: List[str],
        network: Optional[str] = None,
    ) -> List[TagPublic]:
        return list(self.tags_by_subject.get(subject_id.strip(), []))

    async def get_tags_by_subjectids(
        self,
        subject_ids: List[str],
        groups: List[str],
        network: Optional[str] = None,
    ) -> Dict[str, List[TagPublic]]:
        return {
            sid.strip(): list(self.tags_by_subject.get(sid.strip(), []))
            for sid in subject_ids
        }

    async def get_best_cluster_tag(
        self, cluster_id: int, currency: str, groups: List[str]
    ) -> Optional[TagPublic]:
        bct = self.best_cluster_tag_by_cluster.get(cluster_id)
        if bct is None:
            return None
        # Match the real tagstore behaviour: cluster definer carries
        # InheritedFrom.CLUSTER even if the test data didn't set it.
        if bct.inherited_from is None:
            return bct.model_copy(update={"inherited_from": InheritedFrom.CLUSTER})
        return bct

    async def get_best_cluster_tags_for_clusters(
        self, cluster_ids: List[int], network: str, groups: List[str]
    ) -> Dict[int, TagPublic]:
        out: Dict[int, TagPublic] = {}
        for cid in cluster_ids:
            bct = self.best_cluster_tag_by_cluster.get(cid)
            if bct is None:
                continue
            if bct.inherited_from is None:
                out[cid] = bct.model_copy(
                    update={"inherited_from": InheritedFrom.CLUSTER}
                )
            else:
                out[cid] = bct
        return out


class FakeDB:
    def __init__(self, cluster_id_by_address: Dict[str, Optional[int]]):
        self.cluster_id_by_address = cluster_id_by_address

    async def get_address_entity_id(self, currency: str, address: str) -> int:
        cid = self.cluster_id_by_address.get(address)
        if cid is None:
            raise AddressNotFoundException(currency, address)
        return cid


def _build_service(
    tags_by_subject: Dict[str, List[TagPublic]],
    best_cluster_tag_by_cluster: Optional[Dict[int, Optional[TagPublic]]] = None,
    cluster_id_by_address: Optional[Dict[str, Optional[int]]] = None,
) -> TagsService:
    return TagsService(
        db=FakeDB(cluster_id_by_address or {}),
        tagstore=FakeTagstore(tags_by_subject, best_cluster_tag_by_cluster or {}),
        concepts_cache_service=MockConceptProtocol(),
        logger=logging.getLogger("test"),
    )


async def _per_address_baseline(
    svc: TagsService,
    network: str,
    addresses: List[str],
    tagstore_groups: List[str],
    include_best_cluster_tag: bool,
):
    """Reference: call the legacy per-address method once per address."""
    return {
        addr: await svc.get_tag_summary_by_address(
            currency=network,
            address=addr,
            tagstore_groups=tagstore_groups,
            include_best_cluster_tag=include_best_cluster_tag,
        )
        for addr in addresses
    }


@pytest.mark.asyncio
async def test_parity_empty_input():
    svc = _build_service({})
    batched = await svc.get_tag_summaries_by_subject_ids(
        network="btc", subject_ids=[], tagstore_groups=["public"]
    )
    assert batched == {}


@pytest.mark.asyncio
async def test_parity_direct_tags_only_no_cluster_definer():
    addrs = ["addr1", "addr2", "addr3"]
    tags_by_subject = {
        "addr1": [
            _make_tag("addr1", confidence_level=80, primary_concept="exchange"),
            _make_tag("addr1", confidence_level=50, primary_concept="exchange"),
        ],
        "addr2": [
            _make_tag("addr2", confidence_level=70, primary_concept="defi"),
        ],
        "addr3": [],
    }
    svc = _build_service(tags_by_subject)

    expected = await _per_address_baseline(
        svc, "btc", addrs, ["public"], include_best_cluster_tag=False
    )
    actual = await svc.get_tag_summaries_by_subject_ids(
        network="btc",
        subject_ids=addrs,
        tagstore_groups=["public"],
        include_best_cluster_tag=False,
    )

    assert set(actual) == set(expected)
    for addr in addrs:
        assert actual[addr] == expected[addr], f"mismatch for {addr}"


@pytest.mark.asyncio
async def test_parity_with_cluster_definer():
    addrs = ["addr1", "addr2", "addr3"]
    tags_by_subject = {
        "addr1": [_make_tag("addr1", confidence_level=50, primary_concept="defi")],
        "addr2": [],
        "addr3": [_make_tag("addr3", confidence_level=70, primary_concept="exchange")],
    }
    cluster_id_by_address = {"addr1": 100, "addr2": 200, "addr3": 300}
    best_cluster_tag_by_cluster = {
        100: _make_tag(
            "cluster_definer_for_100",
            confidence_level=90,
            primary_concept="exchange",
        ),
        200: _make_tag(
            "cluster_definer_for_200",
            confidence_level=60,
            primary_concept="exchange",
        ),
        300: _make_tag(
            "cluster_definer_for_300",
            confidence_level=40,
            primary_concept="defi",
        ),
    }
    svc = _build_service(
        tags_by_subject,
        best_cluster_tag_by_cluster=best_cluster_tag_by_cluster,
        cluster_id_by_address=cluster_id_by_address,
    )

    expected = await _per_address_baseline(
        svc, "btc", addrs, ["public"], include_best_cluster_tag=True
    )
    actual = await svc.get_tag_summaries_by_subject_ids(
        network="btc",
        subject_ids=addrs,
        tagstore_groups=["public"],
        include_best_cluster_tag=True,
    )

    for addr in addrs:
        assert actual[addr] == expected[addr], f"mismatch for {addr}"


@pytest.mark.asyncio
async def test_parity_cluster_definer_is_self():
    """When the cluster definer's identifier == the address itself, it
    must NOT be added a second time. Both paths share this rule.
    """
    addrs = ["addr1"]
    tags_by_subject = {
        "addr1": [_make_tag("addr1", confidence_level=80, primary_concept="exchange")],
    }
    cluster_id_by_address = {"addr1": 100}
    best_cluster_tag_by_cluster = {
        100: _make_tag(
            "addr1", confidence_level=90, primary_concept="exchange"
        ),  # cluster definer IS the address
    }
    svc = _build_service(
        tags_by_subject,
        best_cluster_tag_by_cluster=best_cluster_tag_by_cluster,
        cluster_id_by_address=cluster_id_by_address,
    )

    expected = await _per_address_baseline(
        svc, "btc", addrs, ["public"], include_best_cluster_tag=True
    )
    actual = await svc.get_tag_summaries_by_subject_ids(
        network="btc",
        subject_ids=addrs,
        tagstore_groups=["public"],
        include_best_cluster_tag=True,
    )

    assert actual["addr1"] == expected["addr1"]


@pytest.mark.asyncio
async def test_parity_missing_cluster_id():
    """An address whose cluster_id lookup fails (AddressNotFoundException)
    must not contribute a cluster definer. Same in both paths.
    """
    addrs = ["addr1", "missing_addr"]
    tags_by_subject = {
        "addr1": [_make_tag("addr1", confidence_level=50, primary_concept="defi")],
        "missing_addr": [
            _make_tag("missing_addr", confidence_level=40, primary_concept="exchange"),
        ],
    }
    cluster_id_by_address = {"addr1": 100}  # missing_addr → AddressNotFoundException
    best_cluster_tag_by_cluster = {
        100: _make_tag(
            "cluster_definer_for_100",
            confidence_level=90,
            primary_concept="exchange",
        ),
    }
    svc = _build_service(
        tags_by_subject,
        best_cluster_tag_by_cluster=best_cluster_tag_by_cluster,
        cluster_id_by_address=cluster_id_by_address,
    )

    expected = await _per_address_baseline(
        svc, "btc", addrs, ["public"], include_best_cluster_tag=True
    )
    actual = await svc.get_tag_summaries_by_subject_ids(
        network="btc",
        subject_ids=addrs,
        tagstore_groups=["public"],
        include_best_cluster_tag=True,
    )

    for addr in addrs:
        assert actual[addr] == expected[addr], f"mismatch for {addr}"


@pytest.mark.asyncio
async def test_parity_eth_skips_cluster_definer():
    """ETH-like networks must not consult cluster definer in either path."""
    addrs = ["0x000000000000000000000000000000000000abcd"]
    tags_by_subject = {
        "0x000000000000000000000000000000000000abcd": [
            _make_tag(
                "0x000000000000000000000000000000000000abcd",
                confidence_level=50,
                primary_concept="defi",
            )
        ],
    }
    cluster_id_by_address = {"0x000000000000000000000000000000000000abcd": 100}
    best_cluster_tag_by_cluster = {
        100: _make_tag(
            "cluster_definer_for_100",
            confidence_level=90,
            primary_concept="exchange",
        ),
    }
    svc = _build_service(
        tags_by_subject,
        best_cluster_tag_by_cluster=best_cluster_tag_by_cluster,
        cluster_id_by_address=cluster_id_by_address,
    )

    expected = await _per_address_baseline(
        svc, "eth", addrs, ["public"], include_best_cluster_tag=True
    )
    actual = await svc.get_tag_summaries_by_subject_ids(
        network="eth",
        subject_ids=addrs,
        tagstore_groups=["public"],
        include_best_cluster_tag=True,
    )

    for addr in addrs:
        assert actual[addr] == expected[addr], f"mismatch for {addr}"


@pytest.mark.asyncio
async def test_parity_duplicate_subject_ids_dedupe():
    """Duplicate inputs must collapse to one canonical entry; both inputs
    must receive the same summary.
    """
    addrs = ["addr1", "addr1", "addr2"]
    tags_by_subject = {
        "addr1": [_make_tag("addr1", confidence_level=80, primary_concept="exchange")],
        "addr2": [_make_tag("addr2", confidence_level=50, primary_concept="defi")],
    }
    svc = _build_service(tags_by_subject)

    expected_unique = await _per_address_baseline(
        svc, "btc", ["addr1", "addr2"], ["public"], include_best_cluster_tag=False
    )
    actual = await svc.get_tag_summaries_by_subject_ids(
        network="btc",
        subject_ids=addrs,
        tagstore_groups=["public"],
        include_best_cluster_tag=False,
    )

    assert actual["addr1"] == expected_unique["addr1"]
    assert actual["addr2"] == expected_unique["addr2"]
