import json

import pytest

pytest.importorskip("yamlinclude", reason="PyYAML is required for tagpack tests")

from graphsenselib.tagstore.algorithms.tag_digest import compute_tag_digest
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


def test_tag_digest_IA(tagsIA):
    digest = compute_tag_digest(tagsIA)

    assert digest.best_actor == "internet_archive"

    assert digest.best_label == "Internet Archive"
    assert digest.broad_concept == "entity"
    assert digest.nr_tags == 2

    assert [x.label for x in digest.label_digest.values()] == [
        "Internet Archive",
        "Bad Stuff with Low Confidence",
    ]
    assert list(digest.label_digest.keys()) == [
        "internet archive",
        "bad stuff with low confidence",
    ]
    assert list(digest.label_digest.values())[0].label == digest.best_label

    assert list(digest.concept_tag_cloud.keys())[0] == "organization"
    assert list(digest.concept_tag_cloud.keys()) == ["organization", "filesharing"]


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
