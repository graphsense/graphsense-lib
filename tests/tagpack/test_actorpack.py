from datetime import date

import pytest

pytest.importorskip("yamlinclude", reason="PyYAML is required for tagpack tests")

from graphsenselib.tagpack import ValidationError
from graphsenselib.tagpack.actorpack import ActorPack
from graphsenselib.tagpack.actorpack_schema import ActorPackSchema
from graphsenselib.tagpack.taxonomy import Taxonomy


@pytest.fixture
def schema(monkeypatch):
    tagpack_schema = ActorPackSchema()

    return tagpack_schema


@pytest.fixture
def taxonomies():
    tax_concept = Taxonomy("concept", "http://example.com/concept")
    tax_concept.add_concept("exchange", "Exchange", None, "Some description")
    tax_concept.add_concept("organization", "Orga", None, "Some description")
    tax_concept.add_concept("bad_coding", "Bad coding", None, "Really bad")

    country = Taxonomy("country", "http://example.com/abuse")
    country.add_concept("AT", "Austria", None, "nice for vacations")
    country.add_concept("BE", "Belgium", None, "nice for vacations")
    country.add_concept("US", "USA", None, "nice for vacations")

    taxonomies = {"concept": tax_concept, "country": country}
    return taxonomies


@pytest.fixture
def actorpack(schema, taxonomies):
    return ActorPack(
        "http://example.com",
        {
            "title": "ETH Defilama Actors",
            "creator": "GraphSense Team",
            "lastmod": date.fromisoformat("2021-04-21"),
            "categories": ["exchange"],
            "actors": [
                {
                    "id": "0xnodes",
                    "label": "0x nodes",
                    "uri": "https://0xnodes.io/",
                    "jurisdictions": ["AT", "BE"],
                    "context": '{"blub": 1234}',
                },  # inherits all header fields
            ],
        },
        schema,
        taxonomies,
    )


@pytest.fixture
def actorpack2(schema, taxonomies):
    return ActorPack(
        "http://example.com",
        {
            "title": "ETH Defilama Actors",
            "creator": "GraphSense Team",
            "lastmod": "2021-04-21",
            "categories": ["exchange"],
            "actors": [
                {
                    "id": "0xnodes",
                    "label": "0x nodes",
                    "uri": "https://0xnodes.io/",
                    "jurisdictions": ["AT", "BE"],
                    "context": '{"blub": 1234}',
                },  # inherits all header fields
            ],
        },
        schema,
        taxonomies,
    )


@pytest.fixture
def actorpack_broken_context(schema, taxonomies):
    return ActorPack(
        "http://example.com",
        {
            "title": "ETH Defilama Actors",
            "creator": "GraphSense Team",
            "lastmod": "2021-04-21",
            "categories": ["exchange"],
            "actors": [
                {
                    "id": "0xnodes",
                    "label": "0x nodes",
                    "uri": "https://0xnodes.io/",
                    "jurisdictions": ["AT", "BE"],
                    "context": '"blub": 1234}',
                },  # inherits all header fields
            ],
        },
        schema,
        taxonomies,
    )


@pytest.fixture
def actorpack_context_obj(schema, taxonomies):
    return ActorPack(
        "http://example.com",
        {
            "title": "ETH Defilama Actors",
            "creator": "GraphSense Team",
            "lastmod": "2021-04-21",
            "categories": ["exchange"],
            "actors": [
                {
                    "id": "0xnodes",
                    "label": "0x nodes",
                    "uri": "https://0xnodes.io/",
                    "jurisdictions": ["AT", "BE"],
                    "context": {"blub": 1234},
                },  # inherits all header fields
            ],
        },
        schema,
        taxonomies,
    )


@pytest.fixture
def actorpack_wrong_context_field_type(schema, taxonomies):
    return ActorPack(
        "http://example.com",
        {
            "title": "ETH Defilama Actors",
            "creator": "GraphSense Team",
            "lastmod": "2021-04-21",
            "categories": ["exchange"],
            "actors": [
                {
                    "id": "0xnodes",
                    "label": "0x nodes",
                    "uri": "https://0xnodes.io/",
                    "jurisdictions": ["AT", "BE"],
                    "context": {"coingecko_ids": [123]},
                },  # inherits all header fields
            ],
        },
        schema,
        taxonomies,
    )


@pytest.fixture
def actorpack_wrong_with_mandatory_context_field(schema, taxonomies):
    schema.schema["context"]["refs"]["mandatory"] = True
    ap = ActorPack(
        "http://example.com",
        {
            "title": "ETH Defilama Actors",
            "creator": "GraphSense Team",
            "lastmod": "2021-04-21",
            "categories": ["exchange"],
            "actors": [
                {
                    "id": "0xnodes",
                    "label": "0x nodes",
                    "uri": "https://0xnodes.io/",
                    "jurisdictions": ["AT", "BE"],
                    "context": {"coingecko_ids": ["123"]},
                },  # inherits all header fields
            ],
        },
        schema,
        taxonomies,
    )
    return ap


def test_context_there(actorpack):
    assert actorpack.actors[0].contents["context"] == '{"blub": 1234}'


def test_validate_context_can_be_obj(actorpack_context_obj):
    assert actorpack_context_obj.validate()


def test_validate_wrong_context_field_type(actorpack_wrong_context_field_type):
    with pytest.raises(ValidationError) as e:
        assert actorpack_wrong_context_field_type.validate()
    assert "Field coingecko_ids[0] must be of type text" in str(e.value)


def test_validate_wrong_with_mandatory_context_field(
    actorpack_wrong_with_mandatory_context_field,
):
    with pytest.raises(ValidationError) as e:
        assert actorpack_wrong_with_mandatory_context_field.validate()
    assert "Mandatory field refs not in" in str(e.value)


def test_validate(actorpack):
    assert actorpack.validate()


def test_validate_with_broken_context(actorpack_broken_context):
    with pytest.raises(ValidationError) as e:
        assert actorpack_broken_context.validate()
    assert "Invalid JSON in field context" in str(e.value)


def test_validate_with_string_date(actorpack2):
    assert actorpack2.validate()


def test_load_actorpack_from_file(taxonomies):
    ap = ActorPack.load_from_file(
        "test uri",
        "tests/testfiles/actors/ex.actorpack.yaml",
        ActorPackSchema(),
        taxonomies,
    )

    assert ap.validate()
