"""Tag listings by actor and by label page with a bounded page size.

Without a pagesize they used to fetch every matching tag in one response;
now a missing pagesize means the default and larger ones are capped, while
next_page keeps working (a client that pages on gets more, smaller pages).

DB-free: the real TagsService over a fake tagstore that records the
offset and limit it is asked for.
"""

import asyncio
import logging
from types import SimpleNamespace

import pytest

from graphsenselib.db.asynchronous.services.models import AddressTag
from graphsenselib.db.asynchronous.services.tags_service import (
    TAG_LIST_DEFAULT_PAGESIZE,
    TAG_LIST_MAX_PAGESIZE,
    MockConceptProtocol,
    TagsService,
    tag_list_pagesize,
)


class FakeTagstore:
    """Has `available` tags; returns the slice the service asks for."""

    def __init__(self, available):
        self.available = available
        self.calls = []

    async def _page(self, offset, page_size):
        self.calls.append((offset, page_size))
        n = max(0, min(page_size, self.available - offset))
        return [
            SimpleNamespace(identifier=f"tag{offset + i}", network="BTC")
            for i in range(n)
        ]

    async def get_tags_by_actorid(self, actor, offset, page_size, groups):
        return await self._page(offset, page_size)

    async def get_tags_by_label(self, label, offset, page_size, groups):
        return await self._page(offset, page_size)


def make_service(tagstore):
    service = TagsService(
        db=None,
        tagstore=tagstore,
        concepts_cache_service=MockConceptProtocol(),
        logger=logging.getLogger("test"),
    )

    async def no_entities(db, tags):
        return {(t.identifier, t.network): None for t in tags}

    service._get_entities_dict = no_entities
    # unvalidated AddressTag: only the page mechanics are under test
    service._address_tag_from_public_tag = lambda tag, entity: (
        AddressTag.model_construct(address=tag.identifier)
    )
    return service


LISTINGS = [
    ("get_actor_tags", "binance"),
    ("list_address_tags_by_label", "binance"),
]


@pytest.mark.parametrize(
    "requested, effective",
    [
        (None, TAG_LIST_DEFAULT_PAGESIZE),
        (1, 1),
        (TAG_LIST_MAX_PAGESIZE, TAG_LIST_MAX_PAGESIZE),
        (TAG_LIST_MAX_PAGESIZE + 1, TAG_LIST_MAX_PAGESIZE),
        (5000, TAG_LIST_MAX_PAGESIZE),
    ],
)
def test_effective_pagesize(requested, effective):
    assert tag_list_pagesize(requested) == effective


@pytest.mark.parametrize("method, arg", LISTINGS)
def test_missing_pagesize_is_bounded(method, arg):
    store = FakeTagstore(available=1_000_000)
    result = asyncio.run(getattr(make_service(store), method)(arg, ["public"]))
    # one extra tag is fetched to know whether there is a next page
    assert store.calls == [(0, TAG_LIST_DEFAULT_PAGESIZE + 1)]
    assert len(result.address_tags) == TAG_LIST_DEFAULT_PAGESIZE
    assert result.next_page == "1"


@pytest.mark.parametrize("method, arg", LISTINGS)
def test_large_pagesize_is_capped_and_paging_continues(method, arg):
    store = FakeTagstore(available=TAG_LIST_MAX_PAGESIZE * 2 + 7)
    service = make_service(store)
    seen, page = [], None
    while True:
        result = asyncio.run(
            getattr(service, method)(
                arg, ["public"], page=int(page) if page else None, pagesize=5000
            )
        )
        seen.extend(t.address for t in result.address_tags)
        page = result.next_page
        if page is None:
            break
    assert [o for o, _ in store.calls] == [
        0,
        TAG_LIST_MAX_PAGESIZE,
        2 * TAG_LIST_MAX_PAGESIZE,
    ]
    assert all(limit == TAG_LIST_MAX_PAGESIZE + 1 for _, limit in store.calls)
    # every tag exactly once, in order: smaller pages lose nothing
    assert seen == [f"tag{i}" for i in range(TAG_LIST_MAX_PAGESIZE * 2 + 7)]


@pytest.mark.parametrize("method, arg", LISTINGS)
def test_small_listing_has_no_next_page(method, arg):
    store = FakeTagstore(available=3)
    result = asyncio.run(
        getattr(make_service(store), method)(arg, ["public"], pagesize=10)
    )
    assert [t.address for t in result.address_tags] == ["tag0", "tag1", "tag2"]
    assert result.next_page is None
