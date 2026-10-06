from datetime import date

import pytest

pytest.importorskip("yaml_include", reason="PyYAML is required for tagpack tests")

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


def test_get_resolve_mapping_reads_aliases_from_context(schema, taxonomies):
    """Test that get_resolve_mapping reads aliases from context.aliases field."""
    ap = ActorPack(
        "http://example.com",
        {
            "title": "Test Actors",
            "creator": "Test",
            "lastmod": "2021-04-21",
            "categories": ["exchange"],
            "actors": [
                {
                    "id": "binance",
                    "label": "Binance",
                    "uri": "https://binance.com",
                    "jurisdictions": ["US"],
                    "context": '{"aliases": ["binanceexchange", "binance_ex"]}',
                },
                {
                    "id": "kraken",
                    "label": "Kraken",
                    "uri": "https://kraken.com",
                    "jurisdictions": ["US"],
                    "context": '{"other_data": "foo"}',  # no aliases
                },
            ],
        },
        schema,
        taxonomies,
    )

    mapping = ap.get_resolve_mapping()

    # Actor IDs map to themselves
    assert mapping["binance"] == "binance"
    assert mapping["kraken"] == "kraken"

    # Aliases from context map to actor ID
    assert mapping["binanceexchange"] == "binance"
    assert mapping["binance_ex"] == "binance"

    # No extra mappings for kraken (no aliases in context)
    assert len([k for k, v in mapping.items() if v == "kraken"]) == 1


def test_get_resolve_mapping_handles_invalid_json_context(schema, taxonomies):
    """Test that get_resolve_mapping skips actors with invalid JSON in context."""
    ap = ActorPack(
        "http://example.com",
        {
            "title": "Test Actors",
            "creator": "Test",
            "lastmod": "2021-04-21",
            "categories": ["exchange"],
            "actors": [
                {
                    "id": "broken",
                    "label": "Broken",
                    "uri": "https://broken.com",
                    "jurisdictions": ["US"],
                    "context": "not valid json{",
                },
                {
                    "id": "valid",
                    "label": "Valid",
                    "uri": "https://valid.com",
                    "jurisdictions": ["US"],
                    "context": '{"aliases": ["valid_alias"]}',
                },
            ],
        },
        schema,
        taxonomies,
    )

    # Should not raise, just skip the broken one
    mapping = ap.get_resolve_mapping()

    assert mapping["broken"] == "broken"  # ID still maps
    assert mapping["valid"] == "valid"
    assert mapping["valid_alias"] == "valid"
    assert "broken_alias" not in mapping  # no alias for broken actor


def test_validate_warns_on_mojibake_in_actor_label(actorpack, caplog):
    actorpack.contents["actors"][0]["label"] = "MÃ¼nchen Nodes"
    actorpack.validate()
    assert "'München Nodes'" in caplog.text


def test_validate_fail_bad_text_when_errors(actorpack, monkeypatch):
    import graphsenselib.tagpack.tagpack as tp

    monkeypatch.setattr(tp, "TEXT_PROBLEMS_ARE_ERRORS", True)
    actorpack.contents["title"] = "Actors\x1b[31m"
    with pytest.raises(ValidationError, match="Field 'title' contains non-printable"):
        actorpack.validate()


def test_validate_actor_relations(actorpack, caplog):
    a = actorpack.contents["actors"][0]
    a["context"] = '{"same_as": ["other_pack_actor"], "related_actors": ["x"]}'
    actorpack.validate()
    assert "refers to actors not in this pack" in caplog.text
    assert actorpack.actors[0].same_as == ["other_pack_actor"]


@pytest.mark.parametrize(
    "context,match",
    [
        ('{"same_as": ["0xnodes"]}', "lists itself"),
        ('{"same_as": ["x"], "related_actors": ["x"]}', "in both"),
    ],
)
def test_validate_actor_relations_errors(actorpack, context, match):
    actorpack.contents["actors"][0]["context"] = context
    with pytest.raises(ValidationError, match=match):
        actorpack.validate()


def test_no_merge_hint_for_actors_declared_same_as(actorpack, caplog):
    first = actorpack.contents["actors"][0]
    actorpack.contents["actors"].append(
        {
            "id": "0xnodesold",
            "label": "0x nodes (old)",
            "uri": "https://www.0xnodes.io/old",
            "context": '{"same_as": ["0xnodes"]}',
        }
    )
    actorpack.validate()
    assert "share the same domain" not in caplog.text

    # without the declaration the shared domain is still reported
    caplog.clear()
    actorpack.contents["actors"][1]["context"] = "{}"
    actorpack.validate()
    assert "share the same domain 0xnodes.io" in caplog.text
    assert first["id"] == "0xnodes"
