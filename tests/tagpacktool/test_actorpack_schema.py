from datetime import date

import pytest

from tagpack.actorpack_schema import ActorPackSchema
from tagpack.tagpack_schema import ValidationError
from tagpack.taxonomy import Taxonomy

field_types = {
    "title": "text",
    "creator": "text",
    "description": "text",
    "is_public": "boolean",
    "actors": "list",
    "id": "text",
    "uri": "text",
    "label": "text",
    "lastmod": "datetime",
    "categories": "list",
    "jurisdictions": "list",
}

field_values = {
    "title": "some text string",
    "creator": "some text string",
    "description": "some text string",
    "is_public": True,
    "actors": [1, 2, 3],
    "id": "some text string",
    "uri": "some text string",
    "label": "some text string",
    "lastmod": date.fromisoformat("2022-01-01"),
    "categories": ["1", "2", "3"],
    "jurisdictions": ["3", "d", "3"],
}


@pytest.fixture
def schema(monkeypatch):
    actorpack_schema = ActorPackSchema()

    return actorpack_schema


@pytest.fixture
def taxonomies():
    tax_concept = Taxonomy("concept", "http://example.com/concept")
    tax_concept.add_concept("scam", "Scam", None, "Some description")
    tax_concept.add_concept("exchange", "Exchange", None, "Some description")

    tax_country = Taxonomy("country", "http://example.com/country")
    tax_country.add_concept("MX", "Mexico", None, None)

    taxonomies = {"concept": tax_concept, "country": tax_country}
    return taxonomies


def test_init(schema):
    assert isinstance(schema, ActorPackSchema)
    assert schema.definition == "actorpack_schema.yaml"


def test_header_fields(schema):
    assert isinstance(schema.header_fields, dict)
    fields = {"title", "creator", "description", "is_public", "actors"}
    assert fields - set(schema.header_fields) == set()
    for field in fields:
        assert field in schema.header_fields
        assert "type" in schema.header_fields[field]
        assert "mandatory" in schema.header_fields[field]


def test_mandatory_header_fields(schema):
    assert isinstance(schema.mandatory_header_fields, dict)
    fields = ["title", "creator", "actors"]
    for field in fields:
        assert field in schema.mandatory_header_fields
        assert schema.header_fields[field]["mandatory"] is True


def test_actor_fields(schema):
    assert isinstance(schema.actor_fields, dict)
    fields = {"id", "uri", "label", "lastmod", "categories", "jurisdictions"}
    assert fields - set(schema.actor_fields) == set()
    for field in fields:
        assert field in schema.actor_fields
        assert "type" in schema.actor_fields[field]
        assert "mandatory" in schema.actor_fields[field]


def test_mandatory_actor_fields(schema):
    assert isinstance(schema.mandatory_actor_fields, dict)
    fields = ["id", "uri", "label", "lastmod", "categories"]
    for field in fields:
        assert field in schema.mandatory_actor_fields
        assert schema.actor_fields[field]["mandatory"] is True


def test_field_type(schema):
    for field, ftype in field_types.items():
        assert schema.field_type(field) == ftype


def test_field_taxonomy(schema):
    assert sorted(schema.field_taxonomy("categories")) == ["concept"]
    assert schema.field_taxonomy("jurisdictions") == "country"


def test_field_no_taxonomy(schema):
    assert schema.field_taxonomy("title") is None


def test_check_type(schema):
    for field, value in field_values.items():
        assert schema.check_type(field, value)
        with pytest.raises(ValidationError) as e:
            assert schema.check_type(field, 5)
        msg = f"Field {field} must be of type {field_types[field]}"
        assert msg in str(e.value)


def test_check_taxonomies(schema, taxonomies):
    schema.schema["actor"]["test"] = {"taxonomy": "nonexistent"}
    with pytest.raises(ValidationError) as e:
        assert schema.check_taxonomies("test", "invalid", None)
    assert "No taxonomies loaded" in str(e.value)

    schema.schema["actor"]["invalidtax"] = {"taxonomy": "nonexistent"}
    with pytest.raises(ValidationError) as e:
        assert schema.check_taxonomies("invalidtax", "value", taxonomies)
    assert "Unknown taxonomy" in str(e.value)

    assert schema.check_taxonomies("categories", "exchange", taxonomies)
    with pytest.raises(ValidationError) as e:
        assert schema.check_taxonomies("categories", "test", taxonomies)
    assert "Undefined concept test for categories field" in str(e.value)

    assert schema.check_taxonomies("jurisdictions", "MX", taxonomies)
    with pytest.raises(ValidationError) as e:
        assert schema.check_taxonomies("jurisdictions", "test", taxonomies)
    assert "Undefined concept test for jurisdictions field" in str(e.value)
