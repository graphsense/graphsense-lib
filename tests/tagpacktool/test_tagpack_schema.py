from datetime import date

import pytest

from tagpack.tagpack_schema import TagPackSchema, ValidationError
from tagpack.taxonomy import Taxonomy


@pytest.fixture
def schema(monkeypatch):
    tagpack_schema = TagPackSchema()

    return tagpack_schema


@pytest.fixture
def taxonomies():
    tax_entity = Taxonomy("concept", "http://example.com/entity")
    tax_entity.add_concept("exchange", "Exchange", None, "Some description")

    tax_abuse = Taxonomy("abuse", "http://example.com/abuse")
    tax_abuse.add_concept("bad_coding", "Bad coding", None, "Really bad")

    taxonomies = {"concept": tax_entity, "abuse": tax_abuse}
    return taxonomies


def test_init(schema):
    assert isinstance(schema, TagPackSchema)
    assert schema.definition == "tagpack_schema.yaml"


def test_header_fields(schema):
    assert isinstance(schema.header_fields, dict)
    assert "tags" in schema.header_fields
    assert "title" in schema.header_fields
    assert "type" in schema.header_fields["title"]
    assert "text" in schema.header_fields["title"]["type"]
    assert "mandatory" in schema.header_fields["title"]
    assert schema.header_fields["title"]["mandatory"] is True
    assert schema.header_fields["creator"]["mandatory"] is True
    assert schema.header_fields["tags"]["mandatory"] is True


def test_mandatory_header_fields(schema):
    assert isinstance(schema.mandatory_header_fields, dict)
    assert "title" in schema.mandatory_header_fields
    assert "tags" in schema.mandatory_header_fields
    assert "creator" in schema.mandatory_header_fields
    assert "notmandatory" not in schema.mandatory_header_fields


def test_tag_fields(schema):
    assert isinstance(schema.tag_fields, dict)
    assert "label" in schema.tag_fields
    assert "type" in schema.tag_fields["label"]
    assert "mandatory" in schema.tag_fields["label"]
    assert "address" in schema.tag_fields


def test_mandatory_tag_fields(schema):
    assert isinstance(schema.mandatory_tag_fields, dict)
    # assert "address" in schema.mandatory_tag_fields
    assert "label" in schema.mandatory_tag_fields
    assert "source" in schema.mandatory_tag_fields
    assert "currency" in schema.mandatory_tag_fields

    assert "lastmod" not in schema.mandatory_tag_fields


def test_all_tag_fields(schema):
    assert isinstance(schema.tag_fields, dict)
    assert "address" in schema.tag_fields
    assert "label" in schema.tag_fields


def test_all_fields(schema):
    assert isinstance(schema.all_fields, dict)
    assert all(field in schema.all_fields for field in ["title", "label", "address"])


def test_field_type(schema):
    assert schema.field_type("title") == "text"
    assert schema.field_type("creator") == "text"
    assert schema.field_type("description") == "text"
    assert schema.field_type("address") == "text"
    assert schema.field_type("label") == "text"
    assert schema.field_type("source") == "text"
    assert schema.field_type("currency") == "text"
    assert schema.field_type("context") == "text"
    assert schema.field_type("confidence") == "text"
    assert schema.field_type("category") == "text"
    assert schema.field_type("abuse") == "text"

    assert schema.field_type("lastmod") == "datetime"

    assert schema.field_type("is_cluster_definer") == "boolean"
    assert schema.field_type("is_public") == "boolean"

    assert schema.field_type("tags") == "list"


def test_field_taxonomy(schema):
    assert schema.field_taxonomy("category") == "concept"


def test_field_no_taxonomy(schema):
    assert schema.field_taxonomy("title") is None


def test_check_type(schema):
    assert schema.check_type("title", "some test string")
    with pytest.raises(ValidationError) as e:
        assert schema.check_type("title", 5)
    assert "Field title must be of type text" in str(e.value)

    assert schema.check_type("lastmod", date.fromisoformat("2021-04-21"))
    with pytest.raises(ValidationError) as e:
        assert schema.check_type("lastmod", 5)
    assert "Field lastmod must be of type datetime" in str(e.value)

    assert schema.check_type("address", "string")
    with pytest.raises(ValidationError) as e:
        assert schema.check_type("address", 0x2342)
    assert "Field address must be of type text" in str(e.value)

    assert schema.check_type("tags", [{"a": 1}, {"b": 2}])
    with pytest.raises(ValidationError) as e:
        assert schema.check_type("tags", "56abc")
    assert "Field tags must be of type list" in str(e.value)


def test_check_taxonomies(schema, taxonomies):
    assert schema.check_taxonomies("category", "exchange", taxonomies)
    with pytest.raises(ValidationError) as e:
        assert schema.check_taxonomies("category", "test", taxonomies)
    assert "Undefined concept test for category field" in str(e.value)

    schema.schema["tag"]["dummy"] = {"taxonomy": "test"}
    with pytest.raises(ValidationError) as e:
        assert schema.check_taxonomies("dummy", "test", taxonomies)
    assert "Unknown taxonomy" in str(e.value) and "test" in str(e.value)
