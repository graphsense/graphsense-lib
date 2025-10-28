import json

import pytest

pytest.importorskip("yamlinclude", reason="PyYAML is required for tagpack tests")

from graphsenselib.tagstore.algorithms.tag_digest import (
    TagDigestComputationConfig,
    compute_tag_digest,
)
from graphsenselib.tagstore.algorithms.obfuscate import obfuscate_tag_if_not_public
from graphsenselib.tagstore.db.queries import TagPublic


def loadtestFile(file):
    with open(file) as f:
        data = json.load(f)
    return [TagPublic(**t) for t in data]


@pytest.fixture
def tagsCryptoDogs():
    return loadtestFile(
        "tests/testfiles/TagPublic/0xdeadbeefdeadbeefdeadbeefdeadbeef_tags.json"
    )


@pytest.fixture
def tagsIA():
    return loadtestFile(
        "tests/testfiles/TagPublic/1Archive1n2C579dMsAu3iC6tWzuQJz8dN_tags.json"
    )


@pytest.fixture
def tagsExchange():
    return loadtestFile("tests/testfiles/TagPublic/exchange_tags.json")


def test_tag_digest_cryptoDogs(tagsCryptoDogs):
    digest = compute_tag_digest(tagsCryptoDogs)

    assert digest.best_actor == "CryptoDogs"

    assert digest.best_label == "CDST (CryptoDogs USD) Token"
    assert digest.broad_concept == "entity"
    assert digest.nr_tags == 6

    assert [x.label for x in digest.label_digest.values()] == [
        "CDST (CryptoDogs USD) Token",
        "Optimism Gateway: CryptoDogs USD",
        "CryptoDogs USD",
        "Bad Stuff",
        "CryptoDogsToken",
    ]
    assert list(digest.label_digest.keys()) == [
        "cdst cryptodogs usd token",
        "optimism gateway cryptodogs usd",
        "cryptodogs usd",
        "bad stuff",
        "cryptodogstoken",
    ]
    assert list(digest.label_digest.values())[0].label == digest.best_label

    assert list(digest.concept_tag_cloud.keys())[0] == "payment_processor"
    assert list(digest.concept_tag_cloud.keys()) == [
        "payment_processor",
        "defi_bridge",
        "unknown",
        "search_engine",
        "service",
    ]


def test_tag_digest_cryptoDogs_obfuscated(tagsCryptoDogs):
    digest = compute_tag_digest(
        [obfuscate_tag_if_not_public(t) for t in tagsCryptoDogs]
    )

    assert digest.best_actor == "CryptoDogs"

    assert digest.best_label == "CryptoDogs USD"
    assert digest.broad_concept == "entity"
    assert digest.nr_tags == 6

    assert [x.label for x in digest.label_digest.values()] == [
        "",
        "CryptoDogs USD",
    ]
    assert list(digest.label_digest.keys()) == ["", "cryptodogs usd"]
    assert list(digest.label_digest.values())[1].label == digest.best_label

    # all the concepts are still there
    assert list(digest.concept_tag_cloud.keys())[0] == "payment_processor"
    assert list(digest.concept_tag_cloud.keys()) == [
        "payment_processor",
        "defi_bridge",
        "unknown",
        "search_engine",
        "service",
    ]


def test_tag_digest_IA(tagsIA):
    digest = compute_tag_digest(
        tagsIA,
        config=TagDigestComputationConfig().with_only_propagate_high_confidence_actors(
            True
        ),
    )

    assert digest.best_actor == "internet_archive"

    assert digest.best_label == "Internet Archive"
    assert digest.broad_concept == "entity"
    assert digest.nr_tags == 3

    assert [x.label for x in digest.label_digest.values()] == [
        "OFAC SDN Listed Entity",
        "Internet Archive",
        "Bad Stuff with Low Confidence",
    ]
    assert list(digest.label_digest.keys()) == [
        "OFAC SDN Listed Entity".lower(),
        "internet archive",
        "bad stuff with low confidence",
    ]
    assert list(digest.label_digest.values())[1].label == digest.best_label

    assert list(digest.concept_tag_cloud.keys())[0] == "sanction_list"
    assert list(digest.concept_tag_cloud.keys()) == [
        "sanction_list",
        "organization",
        "filesharing",
    ]


def test_tag_digest_exchange(tagsExchange):
    digest = compute_tag_digest(tagsExchange)

    assert digest.best_actor == "someexchange"

    assert digest.best_label == "SomeExchange.com"
    assert digest.broad_concept == "exchange"
    assert digest.nr_tags == 2

    assert [x.label for x in digest.label_digest.values()] == [
        "SomeExchange.com",
        "SomeExchange",
    ]
    assert list(digest.label_digest.keys()) == ["someexchange com", "someexchange"]
    assert list(digest.label_digest.values())[0].label == digest.best_label

    assert list(digest.concept_tag_cloud.keys())[0] == "exchange"
    assert list(digest.concept_tag_cloud.keys()) == ["exchange"]
