"""Paging of the tag listings by actor and by label.

- A missing pagesize means the default, larger ones are capped.
- With tag_list_max_items, a caller pages by number through the first N
  tags only; deeper pages and keyset tokens are refused.
- Without a limit, the actor listing hands out keyset tokens ("k<level>.<id>")
  so every page costs the same; page numbers from older clients still work.

DB-free: the real TagsService over a fake tagstore that orders tags like the
real query (confidence level descending, then id) and records the calls.
"""

import asyncio
import base64
import logging
import re
from types import SimpleNamespace

import pytest

from graphsenselib.db.asynchronous.services.models import AddressTag
from graphsenselib.db.asynchronous.services.tags_service import (
    TAG_LIST_DEFAULT_PAGESIZE,
    TAG_LIST_MAX_PAGESIZE,
    MockConceptProtocol,
    TagsService,
    parse_tag_keyset_token,
    tag_keyset_token,
    tag_list_pagesize,
)
from graphsenselib.errors import BadUserInputException
from graphsenselib.web.routes.base import get_tag_list_max_items


def make_tags(n):
    """n tags over three confidence levels, ids not in level order."""
    levels = [100, 60, 60, 20]  # two confidences share level 60
    return [(levels[i % 4], 1000 + (i * 7919) % (10 * n)) for i in range(n)]


class FakeTagstore:
    def __init__(self, n):
        # the real order: level descending, then tag id
        self.positions = sorted(make_tags(n), key=lambda p: (-p[0], p[1]))
        self.calls = []

    @staticmethod
    def _tag(position):
        return SimpleNamespace(identifier=f"t{position[1]}", network="BTC")

    async def get_actor_tag_page(
        self, actor, page_size, groups, offset=None, after=None
    ):
        self.calls.append(("actor", offset, after, page_size))
        rows = self.positions
        if after is not None:
            level, tag_id = after
            rows = [
                p for p in rows if p[0] < level or (p[0] == level and p[1] > tag_id)
            ]
        elif offset:
            rows = rows[offset:]
        return [(self._tag(p), p) for p in rows[:page_size]]

    async def get_tags_by_label(self, label, offset, page_size, groups):
        self.calls.append(("label", offset, None, page_size))
        return [self._tag(p) for p in self.positions[offset : offset + page_size]]


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
    # unvalidated AddressTag: only the paging is under test
    service._address_tag_from_public_tag = lambda tag, entity: (
        AddressTag.model_construct(address=tag.identifier)
    )
    return service


def call(service, method, page=None, pagesize=None, max_items=None):
    return asyncio.run(
        getattr(service, method)(
            "x", ["public"], page=page, pagesize=pagesize, max_items=max_items
        )
    )


def page_all(service, method, pagesize=None, max_items=None):
    """Follow next_page to the end: the tags in order and the tokens seen.
    Page numbers go back as int, as the web layer passes them."""
    seen, tokens, page = [], [], None
    while True:
        r = call(service, method, page, pagesize, max_items)
        seen.extend(t.address for t in r.address_tags)
        if r.next_page is None:
            return seen, tokens
        tokens.append(r.next_page)
        page = int(r.next_page) if r.next_page.isdigit() else r.next_page


def expected(store, n=None):
    return [f"t{p[1]}" for p in store.positions][:n]


METHODS = ["get_actor_tags", "list_address_tags_by_label"]


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


def test_keyset_token_roundtrip_and_garbage():
    token = tag_keyset_token((60, 12345))
    assert parse_tag_keyset_token(token) == (60, 12345)
    # opaque: no visible structure, URL-safe, no padding
    assert "60" not in token and "12345" not in token
    assert re.fullmatch(r"[A-Za-z0-9_-]+", token)
    assert parse_tag_keyset_token(tag_keyset_token((-1, 7))) == (-1, 7)
    unversioned = base64.urlsafe_b64encode(b"60:12345").decode().rstrip("=")
    v2 = base64.urlsafe_b64encode(b"v2:60:12345").decode().rstrip("=")
    for bad in ["", "k60.12345", "!!!", "djE6", unversioned, v2, "%%%%", "a"]:
        with pytest.raises(BadUserInputException):
            parse_tag_keyset_token(bad)


@pytest.mark.parametrize("method", METHODS)
def test_missing_pagesize_is_bounded(method):
    store = FakeTagstore(1000)
    r = call(make_service(store), method)
    assert len(r.address_tags) == TAG_LIST_DEFAULT_PAGESIZE
    assert store.calls[0][3] == TAG_LIST_DEFAULT_PAGESIZE + 1


def test_actor_unlimited_pages_by_keyset_and_sees_every_tag_once():
    n = TAG_LIST_MAX_PAGESIZE * 3 + 7
    store = FakeTagstore(n)
    seen, tokens = page_all(make_service(store), "get_actor_tags", pagesize=5000)
    assert seen == expected(store)
    assert tokens and not any(t.isdigit() for t in tokens)
    # every page after the first is read from a position, never by offset
    assert all(
        after is not None and not offset for _, offset, after, _ in store.calls[1:]
    )


def test_actor_unlimited_old_page_number_continues_with_keyset():
    store = FakeTagstore(250)
    service = make_service(store)
    r = call(service, "get_actor_tags", page=1, pagesize=100)
    assert [t.address for t in r.address_tags] == expected(store)[100:200]
    assert not r.next_page.isdigit()
    rest = call(service, "get_actor_tags", page=r.next_page, pagesize=100)
    assert [t.address for t in rest.address_tags] == expected(store)[200:]
    assert rest.next_page is None


@pytest.mark.parametrize("method", METHODS)
def test_limited_caller_pages_by_number_up_to_max_items(method):
    store = FakeTagstore(5000)
    seen, tokens = page_all(make_service(store), method, pagesize=300, max_items=1000)
    assert seen == expected(store, 1000)
    assert tokens == ["1", "2", "3"]


@pytest.mark.parametrize("method", METHODS)
def test_limited_caller_short_list_ends_normally(method):
    store = FakeTagstore(30)
    seen, _ = page_all(make_service(store), method, pagesize=10, max_items=1000)
    assert seen == expected(store)


@pytest.mark.parametrize("method", METHODS)
def test_limited_caller_refused_beyond_max_items(method):
    service = make_service(FakeTagstore(5000))
    with pytest.raises(BadUserInputException):
        call(service, method, page=10, pagesize=100, max_items=1000)


def test_limited_caller_refused_keyset_token():
    service = make_service(FakeTagstore(5000))
    with pytest.raises(BadUserInputException):
        call(
            service,
            "get_actor_tags",
            page=tag_keyset_token((20, 1)),
            pagesize=100,
            max_items=1000,
        )


def request_with(config, headers):
    return SimpleNamespace(
        app=SimpleNamespace(state=SimpleNamespace(config=config)),
        state=SimpleNamespace(plugin_state={}),
        headers=headers,
    )


EXPORT = {"on_header": {"X-Consumer-Groups": r"(^|.*,)tags-export(,|$)"}}


@pytest.mark.parametrize(
    "max_items, unrestricted, headers, result",
    [
        (None, None, {}, None),
        (1000, None, {}, 1000),
        (1000, None, {"X-Consumer-Groups": "tags-export"}, 1000),
        (1000, EXPORT, {}, 1000),
        (1000, EXPORT, {"X-Consumer-Groups": "tags-private,tags-export"}, None),
        (1000, EXPORT, {"X-Consumer-Groups": "tags-export"}, None),
        (1000, EXPORT, {"X-Consumer-Groups": "tags-exporter"}, 1000),
        (1000, EXPORT, {"X-Consumer-Groups": "my-tags-export"}, 1000),
    ],
)
def test_max_items_per_caller(max_items, unrestricted, headers, result):
    config = SimpleNamespace(
        tag_list_max_items=max_items, tag_list_unrestricted=unrestricted
    )
    assert get_tag_list_max_items(request_with(config, headers)) == result


@pytest.mark.parametrize(
    "page, passed",
    [(None, None), ("0", 0), ("12", 12), ("djE6NjA6MTIzNDU", "djE6NjA6MTIzNDU")],
)
def test_web_layer_passes_numbers_as_int_and_tokens_as_is(page, passed, monkeypatch):
    from graphsenselib.web.service import tags_service as web

    seen = {}

    class Services:
        class tags_service:
            @staticmethod
            async def get_actor_tags(actor, groups, page, pagesize, max_items=None):
                seen["page"] = page
                return SimpleNamespace(next_page=None, address_tags=[])

    ctx = SimpleNamespace(services=Services, tagstore_groups=["public"])
    monkeypatch.setattr(web, "to_api_address_tag_result", lambda r: r)
    asyncio.run(web.get_actor_tags(ctx, "x", page=page))
    assert seen["page"] == passed
