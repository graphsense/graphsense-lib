"""Tests for best_cluster_tag sorted insertion in list_tags_by_address_raw."""

import logging
from typing import Any, Dict, List, Optional, Tuple
from unittest.mock import AsyncMock

import pytest
import pytest_asyncio

from graphsenselib.db.asynchronous.services.tags_service import (
    TagsService,
    MockConceptProtocol,
)
from graphsenselib.tagstore.db.queries import TagPublic, InheritedFrom


def _make_tag(
    identifier: str = "addr1",
    label: str = "tag",
    confidence_level: int = 50,
    inherited_from: Optional[InheritedFrom] = None,
) -> TagPublic:
    return TagPublic(
        identifier=identifier,
        label=label,
        source="test",
        creator="tester",
        confidence="confidence_id",
        confidence_level=confidence_level,
        tag_subject="address",
        tag_type="tag",
        actor=None,
        primary_concept=None,
        additional_concepts=[],
        is_cluster_definer=False,
        network="btc",
        lastmod=0,
        group="public",
        inherited_from=inherited_from,
        tagpack_title="test pack",
        tagpack_uri=None,
    )


def _make_cluster_tag(
    identifier: str = "cluster_addr",
    confidence_level: int = 80,
) -> TagPublic:
    return _make_tag(
        identifier=identifier,
        label="cluster_tag",
        confidence_level=confidence_level,
        inherited_from=InheritedFrom.CLUSTER,
    )


def _build_service(
    address_tags: List[TagPublic],
    best_cluster_tag: Optional[TagPublic],
    cluster_id: Optional[int] = 42,
) -> TagsService:
    """Build a TagsService with mocked tagstore and db."""
    tagstore = AsyncMock()
    tagstore.get_tags_by_subjectid = AsyncMock(return_value=address_tags)
    tagstore.get_best_cluster_tag = AsyncMock(return_value=best_cluster_tag)

    db = AsyncMock()
    if cluster_id is not None:
        db.get_address_entity_id = AsyncMock(return_value=cluster_id)
    else:
        db.get_address_entity_id = AsyncMock(return_value=None)

    return TagsService(
        db=db,
        tagstore=tagstore,
        concepts_cache_service=MockConceptProtocol(),
        logger=logging.getLogger("test"),
    )


@pytest.mark.asyncio
async def test_cluster_tag_inserted_sorted_by_confidence():
    """Best cluster tag with highest confidence should be first."""
    address_tags = [
        _make_tag(confidence_level=70),
        _make_tag(confidence_level=50),
        _make_tag(confidence_level=30),
    ]
    cluster_tag = _make_cluster_tag(confidence_level=80)
    svc = _build_service(address_tags, cluster_tag)

    tags, is_last = await svc.list_tags_by_address_raw(
        currency="btc",
        address="addr1",
        tagstore_groups=["public"],
        page=0,
        pagesize=25,
        include_best_cluster_tag=True,
    )

    assert tags[0] is cluster_tag
    levels = [t.confidence_level for t in tags]
    assert levels == [80, 70, 50, 30]


@pytest.mark.asyncio
async def test_cluster_tag_inserted_in_middle():
    """Best cluster tag should be inserted between tags with higher/lower confidence."""
    address_tags = [
        _make_tag(confidence_level=90),
        _make_tag(confidence_level=50),
        _make_tag(confidence_level=10),
    ]
    cluster_tag = _make_cluster_tag(confidence_level=60)
    svc = _build_service(address_tags, cluster_tag)

    tags, _ = await svc.list_tags_by_address_raw(
        currency="btc",
        address="addr1",
        tagstore_groups=["public"],
        page=0,
        pagesize=25,
        include_best_cluster_tag=True,
    )

    levels = [t.confidence_level for t in tags]
    assert levels == [90, 60, 50, 10]
    assert tags[1] is cluster_tag


@pytest.mark.asyncio
async def test_cluster_tag_appended_when_lowest_confidence():
    """Best cluster tag with lowest confidence should be last."""
    address_tags = [
        _make_tag(confidence_level=90),
        _make_tag(confidence_level=50),
    ]
    cluster_tag = _make_cluster_tag(confidence_level=10)
    svc = _build_service(address_tags, cluster_tag)

    tags, _ = await svc.list_tags_by_address_raw(
        currency="btc",
        address="addr1",
        tagstore_groups=["public"],
        page=0,
        pagesize=25,
        include_best_cluster_tag=True,
    )

    levels = [t.confidence_level for t in tags]
    assert levels == [90, 50, 10]
    assert tags[-1] is cluster_tag


@pytest.mark.asyncio
async def test_cluster_tag_equal_confidence_goes_after():
    """At equal confidence, cluster tag should be placed after direct tags."""
    address_tags = [
        _make_tag(confidence_level=50, label="direct1"),
        _make_tag(confidence_level=50, label="direct2"),
    ]
    cluster_tag = _make_cluster_tag(confidence_level=50)
    svc = _build_service(address_tags, cluster_tag)

    tags, _ = await svc.list_tags_by_address_raw(
        currency="btc",
        address="addr1",
        tagstore_groups=["public"],
        page=0,
        pagesize=25,
        include_best_cluster_tag=True,
    )

    assert tags[-1] is cluster_tag
    assert len(tags) == 3


@pytest.mark.asyncio
async def test_cluster_tag_skipped_if_direct_tag():
    """If best cluster tag identifier matches the address, skip it."""
    address_tags = [_make_tag(confidence_level=50)]
    cluster_tag = _make_cluster_tag(identifier="addr1", confidence_level=80)
    svc = _build_service(address_tags, cluster_tag)

    tags, _ = await svc.list_tags_by_address_raw(
        currency="btc",
        address="addr1",
        tagstore_groups=["public"],
        page=0,
        pagesize=25,
        include_best_cluster_tag=True,
    )

    assert len(tags) == 1
    assert all(t is not cluster_tag for t in tags)


@pytest.mark.asyncio
async def test_cluster_tag_not_added_when_flag_false():
    """Without include_best_cluster_tag, no cluster tag should appear."""
    address_tags = [_make_tag(confidence_level=50)]
    cluster_tag = _make_cluster_tag(confidence_level=80)
    svc = _build_service(address_tags, cluster_tag)

    tags, _ = await svc.list_tags_by_address_raw(
        currency="btc",
        address="addr1",
        tagstore_groups=["public"],
        page=0,
        pagesize=25,
        include_best_cluster_tag=False,
    )

    assert len(tags) == 1


@pytest.mark.asyncio
async def test_cluster_tag_not_added_for_eth():
    """ETH-like currencies should not get a best cluster tag."""
    address_tags = [_make_tag(confidence_level=50)]
    cluster_tag = _make_cluster_tag(confidence_level=80)
    svc = _build_service(address_tags, cluster_tag)

    tags, _ = await svc.list_tags_by_address_raw(
        currency="eth",
        address="0x1234",
        tagstore_groups=["public"],
        page=0,
        pagesize=25,
        include_best_cluster_tag=True,
    )

    assert len(tags) == 1


@pytest.mark.asyncio
async def test_no_cluster_tag_when_no_cluster_id():
    """If no cluster_id is found, no cluster tag should be added."""
    address_tags = [_make_tag(confidence_level=50)]
    cluster_tag = _make_cluster_tag(confidence_level=80)
    svc = _build_service(address_tags, cluster_tag, cluster_id=None)

    tags, _ = await svc.list_tags_by_address_raw(
        currency="btc",
        address="addr1",
        tagstore_groups=["public"],
        page=0,
        pagesize=25,
        include_best_cluster_tag=True,
    )

    assert len(tags) == 1


@pytest.mark.asyncio
async def test_full_page_forces_extra_page():
    """When page is full, is_last_page should be False to force another page."""
    address_tags = [_make_tag(confidence_level=50 - i) for i in range(3)]
    # Return 3 tags for pagesize=2 → is_last_page=False initially
    # But we need the "last page that is full" case:
    # Return exactly pagesize+1 tags so is_last_page=False
    address_tags_with_extra = address_tags  # 3 tags, pagesize=2 → not last page

    cluster_tag = _make_cluster_tag(confidence_level=80)
    svc = _build_service(address_tags_with_extra, cluster_tag)

    tags, is_last = await svc.list_tags_by_address_raw(
        currency="btc",
        address="addr1",
        tagstore_groups=["public"],
        page=0,
        pagesize=2,
        include_best_cluster_tag=True,
    )

    # Page is not the last page (3 tags returned for pagesize=2), so no cluster tag
    assert len(tags) == 2
    assert is_last is False


@pytest.mark.asyncio
async def test_last_full_page_forces_extra_page():
    """When last page is exactly full, force an extra page for the cluster tag."""
    # Return exactly pagesize tags (2), so is_last_page=True, is_page_full=True
    address_tags = [
        _make_tag(confidence_level=70),
        _make_tag(confidence_level=50),
    ]
    cluster_tag = _make_cluster_tag(confidence_level=80)
    svc = _build_service(address_tags, cluster_tag)

    tags, is_last = await svc.list_tags_by_address_raw(
        currency="btc",
        address="addr1",
        tagstore_groups=["public"],
        page=0,
        pagesize=2,
        include_best_cluster_tag=True,
    )

    # Last page is full → forces another page
    assert len(tags) == 2
    assert is_last is False


@pytest.mark.asyncio
async def test_empty_tags_with_cluster_tag():
    """When no direct tags exist, cluster tag should be the only result."""
    cluster_tag = _make_cluster_tag(confidence_level=80)
    svc = _build_service([], cluster_tag)

    tags, is_last = await svc.list_tags_by_address_raw(
        currency="btc",
        address="addr1",
        tagstore_groups=["public"],
        page=0,
        pagesize=25,
        include_best_cluster_tag=True,
    )

    assert len(tags) == 1
    assert tags[0] is cluster_tag
    assert is_last is True


@pytest.mark.asyncio
async def test_paged_vs_non_paged_ordering_mismatch():
    """Demonstrates that paged and non-paged results currently differ in ordering.

    Non-paged: cluster tag is sorted by confidence among all tags.
    Paged: cluster tag only appears on the last page.

    This test documents the known limitation. If pagination is fixed in the
    future, this test should be updated to assert equality instead.
    """
    all_address_tags = [
        _make_tag(label=f"tag{i}", confidence_level=level)
        for i, level in enumerate([70, 60, 50, 40])
    ]
    cluster_tag = _make_cluster_tag(confidence_level=80)

    # --- Non-paged result ---
    svc = _build_service(list(all_address_tags), cluster_tag)
    non_paged_tags, _ = await svc.list_tags_by_address_raw(
        currency="btc",
        address="addr1",
        tagstore_groups=["public"],
        page=0,
        pagesize=None,
        include_best_cluster_tag=True,
    )
    non_paged_levels = [t.confidence_level for t in non_paged_tags]

    # Non-paged: cluster tag (80) is sorted in correctly
    assert non_paged_levels == [80, 70, 60, 50, 40]

    # --- Paged result (pagesize=2) ---
    # Page 0: returns tags [70, 60, 50] (fetches pagesize+1=3), trims to 2
    svc = _build_service(list(all_address_tags[:3]), cluster_tag)
    page0_tags, is_last_p0 = await svc.list_tags_by_address_raw(
        currency="btc",
        address="addr1",
        tagstore_groups=["public"],
        page=0,
        pagesize=2,
        include_best_cluster_tag=True,
    )
    assert is_last_p0 is False
    assert [t.confidence_level for t in page0_tags] == [70, 60]

    # Page 1: returns tag [50] (only 1 tag, less than pagesize=2), last page
    svc = _build_service([all_address_tags[2]], cluster_tag)
    page1_tags, is_last_p1 = await svc.list_tags_by_address_raw(
        currency="btc",
        address="addr1",
        tagstore_groups=["public"],
        page=0,
        pagesize=2,
        include_best_cluster_tag=True,
    )
    assert is_last_p1 is True
    paged_levels = [t.confidence_level for t in page0_tags + page1_tags]

    # Paged: cluster tag (80) ends up on last page, not at position 0
    assert paged_levels == [70, 60, 80, 50]

    # Known limitation: ordering differs
    assert non_paged_levels != paged_levels
