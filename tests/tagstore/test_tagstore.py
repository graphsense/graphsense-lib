# -*- coding: utf-8 -*-
import pytest
from types import SimpleNamespace

pytest.importorskip("yamlinclude", reason="PyYAML is required for tagpack tests")

from graphsenselib.tagpack.tagstore import (
    _perform_address_modifications,
    _get_tag,
    TagStore,
)
from graphsenselib.tagpack.cli import insert_tagpack, DEFAULT_SCHEMA

from graphsenselib.tagstore.db.queries import UserReportedAddressTag
from graphsenselib.tagstore.db import TagAlreadyExistsException
from pathlib import Path


def test_bch_conversion():
    cashaddr = "bitcoincash:prseh0a4aejjcewhc665wjqhppgwrz2lw5txgn666a"

    # as per https://bch.btc.com/tools/address-converter
    expected = "3NFvYKuZrxTDJxgqqJSfouNHjT1dAG1Fta"
    result = _perform_address_modifications(cashaddr, "BCH")

    assert expected == result


def test_eth_conversion():
    checksumaddr = "0xC61b9BB3A7a0767E3179713f3A5c7a9aeDCE193C"

    expected = "0xc61b9bb3a7a0767e3179713f3a5c7a9aedce193c"
    result = _perform_address_modifications(checksumaddr, "ETH")

    assert expected == result


def test_update_of_tagpack_file(db_setup):
    testfiles_dir = Path(__file__).parent.resolve() / ".." / "testfiles"
    file = testfiles_dir / "simple" / "with_concepts.yaml"

    m_succ, n_tagpacks = insert_tagpack(
        db_setup["db_connection_string"],
        DEFAULT_SCHEMA,
        str(file.resolve()),
        batch_size=100,
        public=True,
        force=False,
        add_new=False,
        no_strict_check=True,
        no_git=True,
        n_workers=1,
        no_validation=True,
        tag_type_default="actor",
        config=None,
        update_flag=True,
    )

    assert n_tagpacks == m_succ == 0, f"Update even though no change tagpack {file}"

    file.touch()  # update modification time to force update

    m_succ, n_tagpacks = insert_tagpack(
        db_setup["db_connection_string"],
        DEFAULT_SCHEMA,
        str(file.resolve()),
        batch_size=100,
        public=True,
        force=False,
        add_new=False,
        no_strict_check=True,
        no_git=True,
        n_workers=1,
        no_validation=True,
        tag_type_default="actor",
        config=None,
        update_flag=True,
    )

    assert n_tagpacks == m_succ == 1, f"Failed to update modified tagpack {file}"


def test_get_actor_alias_mapping(db_setup):
    """Test that get_actor_alias_mapping loads aliases from context field."""
    ts = TagStore(db_setup["db_connection_string"], "public")

    mapping = ts.get_actor_alias_mapping()

    # Actor IDs should map to themselves
    assert mapping["internet_archive"] == "internet_archive"
    assert mapping["binance"] == "binance"

    # Aliases from context should map to actor ID
    # (internet_archive has aliases: ["internetarchive"] in context)
    assert mapping["internetarchive"] == "internet_archive"


def test_db_consistency(db_setup):
    # this is all based on the tagpacks inserted in conftest.py

    ts = TagStore(db_setup["db_connection_string"], "public")

    repos = ts.tagstore_source_repos()

    assert len(repos) == 5

    addresses = ts.get_addresses(update_existing=True)

    assert list(addresses) == [
        ("3bacadsfg3sdfafd2deddg32", "BTC"),
        ("1bacdeddg32dsfk5692dmn23", "BTC"),
    ]

    composition = ts.get_tagstore_composition(by_network=True)

    assert list(composition) == [
        ("GraphSense Team", "private", "BTC", 2, 2),
        ("GraphSense Team", "public", "BTC", 2, 6),
    ]

    actorc = ts.get_tags_with_actors_count()

    assert actorc == 1

    usedActorC = ts.get_used_actors_count()

    assert usedActorC == 1

    tags = ts.list_tags()

    full_tags = ts.dump_tags()

    assert len(tags) == len(full_tags)

    tags = ts.list_tags(unique=True)

    label_index = 2
    indent_index = 13
    tag_type_index = 17
    tag_subject_index = 18

    assert {x[label_index] for x in tags} == {
        "othertag",
        "sometag",
        "sometag.info",
        "test",
    }
    assert {x[indent_index] for x in full_tags} == {
        "0xdeadbeef",
        "1bacdeddg32dsfk5692dmn23",
        "3bacadsfg3sdfafd2deddg32",
    }
    assert {x[tag_type_index] for x in full_tags} == {"actor"}
    assert {x[tag_subject_index] for x in full_tags} == {"address", "tx"}

    assert {x[indent_index] for x in full_tags if x[tag_subject_index] == "tx"} == {
        "0xdeadbeef"
    }

    actors = ts.list_actors()

    actor_id_index = 1

    assert {x[actor_id_index] for x in actors} == {"binance", "internet_archive"}


@pytest.mark.asyncio
async def test_db_url(async_tagstore_db):
    db = async_tagstore_db
    assert list(await db.get_acl_groups()) == ["private", "public"]

    tags = await db.get_tags_by_subjectid(
        "1bacdeddg32dsfk5692dmn23",
        offset=None,
        page_size=None,
        groups=["private", "public"],
    )

    tags_pub = await db.get_tags_by_subjectid(
        "1bacdeddg32dsfk5692dmn23", offset=None, page_size=None, groups=["private"]
    )

    assert len(tags) == 5
    assert len(tags_pub) == 0

    addr = {t.identifier for t in tags}
    assert addr == {"1bacdeddg32dsfk5692dmn23"}

    tags = await db.get_tags_by_subjectid(
        "0xdeadbeef", offset=None, page_size=None, groups=["private", "public"]
    )

    tags_pub = await db.get_tags_by_subjectid(
        "0xdeadbeef", offset=None, page_size=None, groups=["public"]
    )

    assert len(tags) == 1
    assert len(tags_pub) == 0

    addr = {t.identifier for t in tags}
    assert addr == {"0xdeadbeef"}


@pytest.mark.asyncio
async def test_insert_user_tag(async_tagstore_db):
    db = async_tagstore_db
    address = "ABC-insert-user-test"

    tagsBefore = await db.get_tags_by_subjectid(
        address, offset=None, page_size=None, groups=["public"]
    )
    taxonomiesBefore = await db.get_taxonomies()

    tag = UserReportedAddressTag(
        address=address,
        network="Btc",
        actor="binance",
        label="binance",
        description="this is helpful",
    )

    await db.add_user_reported_tag(tag)

    tagsAfter = await db.get_tags_by_subjectid(
        address, offset=None, page_size=None, groups=["public"]
    )

    assert len(tagsBefore) == 0

    assert len(tagsAfter) == 1

    tagNew = tagsAfter[0]

    assert tagNew.identifier == address
    assert tagNew.network == "BTC"
    assert tagNew.source == tag.description
    assert tagNew.confidence_level == 5
    assert tagNew.tag_type == "actor"
    assert tagNew.tag_subject == "address"
    assert tagNew.additional_concepts == ["exchange"]
    assert tagNew.actor == tag.actor
    assert tagNew.label == tag.label

    tag2 = UserReportedAddressTag(
        address=address,
        network="Btc",
        actor="binanceblub",
        label="binanceblub",
        description="this is helpfuld",
    )

    await db.add_user_reported_tag(tag2)

    with pytest.raises(TagAlreadyExistsException):
        await db.add_user_reported_tag(tag2)

    tagsAfter2 = await db.get_tags_by_subjectid(
        address, offset=None, page_size=None, groups=["public"]
    )

    del tag, tagsAfter

    assert len(tagsAfter2) == 2

    tagNew2 = tagsAfter2[1]

    assert tagNew2.identifier == address
    assert tagNew2.network == "BTC"
    assert tagNew2.source == tag2.description
    assert tagNew2.confidence_level == 5
    assert tagNew2.tag_type == "actor"
    assert tagNew2.tag_subject == "address"
    assert tagNew2.additional_concepts == []
    assert tagNew2.actor is None
    assert tagNew2.label == tag2.label

    taxonomiesAfter = await db.get_taxonomies()

    assert len(taxonomiesAfter.concept) == len(taxonomiesBefore.concept)
    assert len(taxonomiesAfter.country) == len(taxonomiesBefore.country)
    assert len(taxonomiesAfter.tag_subject) == len(taxonomiesBefore.tag_subject)
    assert len(taxonomiesAfter.country) == len(taxonomiesBefore.country)
    assert len(taxonomiesAfter.confidence) == len(taxonomiesBefore.confidence)


def test_get_tag_resolves_actor_alias():
    """Test that _get_tag() resolves actor aliases to main IDs"""
    tag = SimpleNamespace(
        all_fields={
            "label": "Test Label",
            "source": "http://example.com",
            "address": "1ABC123",
            "currency": "BTC",
            "network": "BTC",
            "is_cluster_definer": False,
            "confidence": "web_crawl",
            "context": None,
            "actor": "binanceexchange",  # This is an alias
            "tag_type": "actor",
        }
    )

    actor_resolve_mapping = {
        "binance": "binance",
        "binanceexchange": "binance",  # Alias maps to main ID
        "coinbase": "coinbase",
    }

    result = _get_tag(tag, "test-tagpack-id", "actor", actor_resolve_mapping)

    # Result is a tuple, actor is at index 10
    assert result[10] == "binance", "Actor alias should be resolved to main ID"


def test_get_tag_keeps_actor_when_no_mapping():
    """Test that _get_tag() keeps original actor when no mapping provided"""
    tag = SimpleNamespace(
        all_fields={
            "label": "Test Label",
            "source": "http://example.com",
            "address": "1ABC123",
            "currency": "BTC",
            "network": "BTC",
            "is_cluster_definer": False,
            "confidence": "web_crawl",
            "context": None,
            "actor": "binanceexchange",
            "tag_type": "actor",
        }
    )

    result = _get_tag(tag, "test-tagpack-id", "actor", None)

    assert result[10] == "binanceexchange", (
        "Actor should remain unchanged without mapping"
    )


def test_get_tag_keeps_actor_when_not_in_mapping():
    """Test that _get_tag() keeps original actor when not found in mapping"""
    tag = SimpleNamespace(
        all_fields={
            "label": "Test Label",
            "source": "http://example.com",
            "address": "1ABC123",
            "currency": "BTC",
            "network": "BTC",
            "is_cluster_definer": False,
            "confidence": "web_crawl",
            "context": None,
            "actor": "unknownactor",
            "tag_type": "actor",
        }
    )

    actor_resolve_mapping = {
        "binance": "binance",
        "binanceexchange": "binance",
    }

    result = _get_tag(tag, "test-tagpack-id", "actor", actor_resolve_mapping)

    assert result[10] == "unknownactor", "Unknown actor should remain unchanged"


def test_get_tag_handles_none_actor():
    """Test that _get_tag() handles None actor correctly"""
    tag = SimpleNamespace(
        all_fields={
            "label": "Test Label",
            "source": "http://example.com",
            "address": "1ABC123",
            "currency": "BTC",
            "network": "BTC",
            "is_cluster_definer": False,
            "confidence": "web_crawl",
            "context": None,
            "actor": None,
            "tag_type": "actor",
        }
    )

    actor_resolve_mapping = {
        "binance": "binance",
    }

    result = _get_tag(tag, "test-tagpack-id", "actor", actor_resolve_mapping)

    assert result[10] is None, "None actor should remain None"


def test_tagpack_insertion_is_atomic(db_setup, tmp_path):
    """Test that tagpack insertion is atomic - if a tag fails, the entire
    tagpack (including header) should be rolled back."""

    # Count tagpacks before insertion attempt
    ts_before = TagStore(db_setup["db_connection_string"], "public")
    ts_before.cursor.execute("SELECT COUNT(*) FROM tagpack")
    count_before = ts_before.cursor.fetchone()[0]
    ts_before.conn.close()

    # Create a tagpack with an invalid confidence level that will cause
    # a foreign key violation
    invalid_tagpack_content = """
title: Atomic Test TagPack
creator: Test Creator
source: http://example.com/atomic_test
confidence: this_confidence_does_not_exist_in_db
currency: BTC
lastmod: 2021-04-21
tags:
- address: 1atomictestaddress123
  label: atomictest
"""
    tagpack_file = tmp_path / "atomic_test_tagpack.yaml"
    tagpack_file.write_text(invalid_tagpack_content)

    # Try to insert - this should fail due to invalid confidence FK
    m_succ, n_tagpacks = insert_tagpack(
        db_setup["db_connection_string"],
        DEFAULT_SCHEMA,
        str(tagpack_file.resolve()),
        batch_size=100,
        public=True,
        force=False,
        add_new=False,
        no_strict_check=True,
        no_git=True,
        n_workers=1,
        no_validation=True,  # Skip validation to let FK violation happen at DB level
        tag_type_default="actor",
        config=None,
        update_flag=False,
    )

    # Insertion should have failed
    assert m_succ == 0, "Expected insertion to fail due to FK violation"

    # Count tagpacks after - should be the same (no orphaned header)
    ts_after = TagStore(db_setup["db_connection_string"], "public")
    ts_after.cursor.execute("SELECT COUNT(*) FROM tagpack")
    count_after = ts_after.cursor.fetchone()[0]
    ts_after.conn.close()

    assert count_after == count_before, (
        f"Tagpack count changed from {count_before} to {count_after} - "
        "orphaned tagpack header was left in DB after failed tag insertion. "
        "Transaction should have been rolled back atomically."
    )


def test_remove_duplicates_keeps_tags_with_different_context(db_setup):
    ts = TagStore(db_setup["db_connection_string"], "public")
    tagpack_id_a = "tests/context/duplicate-with-context-a"
    tagpack_id_b = "tests/context/duplicate-with-context-b"
    source = "http://example.com/context_duplicates"

    tag_a = SimpleNamespace(
        all_fields={
            "label": "contexttag",
            "source": source,
            "address": "1bacdeddg32dsfk5692dmn23",
            "currency": "BTC",
            "network": "BTC",
            "is_cluster_definer": True,
            "confidence": "web_crawl",
            "context": "source-a",
            "tag_type": "actor",
            "concepts": ["exchange"],
        }
    )

    tag_b = SimpleNamespace(
        all_fields={
            "label": "contexttag",
            "source": source,
            "address": "1bacdeddg32dsfk5692dmn23",
            "currency": "BTC",
            "network": "BTC",
            "is_cluster_definer": True,
            "confidence": "web_crawl",
            "context": "source-b",
            "tag_type": "actor",
            "concepts": ["exchange"],
        }
    )

    tagpack_a = SimpleNamespace(
        contents={
            "title": "Context Duplicate Test TagPack A",
            "creator": "GraphSense Team",
            "description": "Regression fixture",
        },
        uri="http://example.com/context_dup_pack_a",
        get_unique_tags=lambda: [tag_a],
    )

    tagpack_b = SimpleNamespace(
        contents={
            "title": "Context Duplicate Test TagPack B",
            "creator": "GraphSense Team",
            "description": "Regression fixture",
        },
        uri="http://example.com/context_dup_pack_b",
        get_unique_tags=lambda: [tag_b],
    )

    ts.insert_tagpack(
        tagpack=tagpack_a,
        is_public=True,
        tag_type_default="actor",
        force_insert=True,
        lastmod=None,
        prefix="",
        rel_path=tagpack_id_a,
        batch=100,
        actor_resolve_mapping=None,
    )

    ts.insert_tagpack(
        tagpack=tagpack_b,
        is_public=True,
        tag_type_default="actor",
        force_insert=True,
        lastmod=None,
        prefix="",
        rel_path=tagpack_id_b,
        batch=100,
        actor_resolve_mapping=None,
    )

    ts.cursor.execute(
        """
        SELECT COUNT(*)
        FROM tag t
        JOIN tagpack tp ON t.tagpack = tp.id
        WHERE tp.id IN (%s, %s)
        """,
        (tagpack_id_a, tagpack_id_b),
    )
    before_count = ts.cursor.fetchone()[0]

    ts.remove_duplicates()

    ts.cursor.execute(
        """
        SELECT COUNT(*), ARRAY_AGG(t.context ORDER BY t.context)
        FROM tag t
        JOIN tagpack tp ON t.tagpack = tp.id
        WHERE tp.id IN (%s, %s)
        """,
        (tagpack_id_a, tagpack_id_b),
    )
    after_count, contexts = ts.cursor.fetchone()
    ts.conn.close()

    assert before_count == 2
    assert after_count == 2
    assert contexts == ["source-a", "source-b"]
