# -*- coding: utf-8 -*-
import pytest

pytest.importorskip("yamlinclude", reason="PyYAML is required for tagpack tests")

from graphsenselib.tagpack.tagstore import _perform_address_modifications, TagStore
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
    file = (
        Path(__file__).parent.resolve()
        / ".."
        / "testfiles"
        / "simple"
        / "with_concepts.yaml"
    )

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
