import json
from datetime import date, datetime

import pytest

pytest.importorskip("yamlinclude", reason="PyYAML is required for tagpack tests")

from graphsenselib.tagpack import ValidationError
from graphsenselib.tagpack.tagpack import (
    Tag,
    TagPack,
    collect_tagpack_files,
    TagPackContents,
)
from graphsenselib.tagpack.tagpack_schema import TagPackSchema
from graphsenselib.tagpack.taxonomy import Taxonomy


@pytest.fixture
def schema(monkeypatch):
    tagpack_schema = TagPackSchema()

    return tagpack_schema


@pytest.fixture
def taxonomies():
    tax_entity = Taxonomy("entity", "http://example.com/entity")
    tax_entity.add_concept("exchange", "Exchange", None, "Some description")

    tax_abuse = Taxonomy("abuse", "http://example.com/abuse")
    tax_abuse.add_concept("bad_coding", "Bad coding", None, "Really bad")

    tax_conf = Taxonomy("confidence", "http://example.com/abuse")
    tax_conf.add_concept("web_crawl", "web_crawl", None, "Really bad")
    tax_conf.add_concept("unknown", "unknown", None, "Really bad")
    tax_conf.add_concept("heuristic", "heuristic", None, "")

    tax_conc = Taxonomy("concept", "http://example.com/abuse")
    tax_conc.add_concept("web_crawl", "web_crawl", None, "Really bad")
    tax_conc.add_concept("bad_coding", "bad_coding", None, "Really bad")
    tax_conc.add_concept("exchange", "exchange", None, "Really bad")
    tax_conc.add_concept("stuff", "stuff", None, "Really bad")
    tax_conc.add_concept("organization", "organization", None, "Really bad")

    taxonomies = {
        "entity": tax_entity,
        "abuse": tax_abuse,
        "confidence": tax_conf,
        "concept": tax_conc,
    }
    return taxonomies


@pytest.fixture
def tagpack_w_network(schema, taxonomies):
    return TagPack(
        "http://example.com",
        {
            "title": "Test TagPack",
            "creator": "GraphSense Team",
            "source": "http://example.com/my_addresses",
            "currency": "BTC",
            "abuse": "bad_coding",
            "category": "exchange",
            "concepts": ["stuff"],
            "lastmod": date.fromisoformat("2021-04-21"),
            "tags": [
                {
                    "label": "Some attribution tag",
                    "address": "123Bitcoin45",
                },  # inherits all header fields
                {
                    "label": "Another attribution tag",
                    "address": "123Bitcoin66",
                    "context": '{"counts": 1}',
                    "currency": "USDT",
                    "network": "ETH23",
                },  # overrides currency
                {
                    "label": "Another attribution tag",
                    "address": "123Bitcoin66",
                    "context": '{"counts": 1}',
                    "currency": "USDT",
                },  # overrides currency
                {
                    "label": "asdf",
                    "address": "asf",
                    "context": '{"counts": 1}',
                    "currency": "CCOIN",
                },  # overrides currency
            ],
        },
        schema,
        taxonomies,
    )


@pytest.fixture
def tagpack(schema, taxonomies):
    return TagPack(
        "http://example.com",
        {
            "title": "Test TagPack",
            "creator": "GraphSense Team",
            "source": "http://example.com/my_addresses",
            "currency": "BTC",
            "abuse": "bad_coding",
            "category": "exchange",
            "concepts": ["stuff"],
            "lastmod": date.fromisoformat("2021-04-21"),
            "tags": [
                {
                    "label": "Some attribution tag",
                    "address": "123Bitcoin45",
                },  # inherits all header fields
                {
                    "label": "Another attribution tag",
                    "address": "123Bitcoin66",
                    "context": '{"counts": 1}',
                    "currency": "ETH",
                },  # overrides currency
            ],
        },
        schema,
        taxonomies,
    )


@pytest.fixture
def tagpack_str_date(schema, taxonomies):
    return TagPack(
        "http://example.com",
        {
            "title": "Test TagPack",
            "creator": "GraphSense Team",
            "source": "http://example.com/my_addresses",
            "currency": "BTC",
            "lastmod": "2021-04-21",
            "tags": [{"label": "a", "address": "b"}],
        },
        schema,
        taxonomies,
    )


@pytest.fixture
def tagpack_context_obj(schema, taxonomies):
    return TagPack(
        "http://example.com",
        {
            "title": "Test TagPack",
            "creator": "GraphSense Team",
            "source": "http://example.com/my_addresses",
            "currency": "BTC",
            "lastmod": "2021-04-21",
            "tags": [{"label": "a", "address": "b", "context": {"a": "b"}}],
        },
        schema,
        taxonomies,
    )


@pytest.fixture
def tagpack_conf(schema, taxonomies):
    return TagPack(
        "http://example.com",
        {
            "title": "Test TagPack",
            "creator": "GraphSense Team",
            "source": "http://example.com/my_addresses",
            "confidence": "web_crawl",
            "currency": "BTC",
            "lastmod": date.fromisoformat("2021-04-21"),
            "tags": [
                {
                    "label": "Some attribution tag",
                    "address": "123Bitcoin45",
                    "confidence": "heuristic",
                },  # inherits all header fields
                {
                    "label": "Another attribution tag",
                    "address": "123Bitcoin66",
                    "context": '{"counts": 1}',
                    "currency": "ETH",
                },  # overrides currency
            ],
        },
        schema,
        taxonomies,
    )


def test_all_header_fields(tagpack, schema):
    h = [
        "confidence",
        "title",
        "creator",
        "source",
        "currency",
        "network",
        "lastmod",
        "tags",
        "category",
        "abuse",
        "concepts",
    ]

    assert all(field in tagpack.all_header_fields for field in h)
    assert len(h) == len(tagpack.all_header_fields)

    assert tagpack.all_header_fields.get("creator") == "GraphSense Team"
    assert tagpack.all_header_fields.get("title") == "Test TagPack"


def test_header_fields(tagpack):
    h = ["title", "creator", "tags"]

    assert all(field in tagpack.header_fields for field in h)
    assert len(h) == len(tagpack.header_fields)


def test_explicit_tag_fields(tagpack):
    tag = tagpack.tags[0]
    t = ["label", "address", "concepts"]

    assert all(field in tag.explicit_fields for field in t)
    assert len(t) == len(tag.explicit_fields)

    tag = tagpack.tags[1]
    t = ["label", "address", "context", "currency", "network", "concepts"]
    assert all(field in tag.explicit_fields for field in t)
    assert len(t) == len(tag.explicit_fields)


def test_tag_inherits_from_header(tagpack):
    t = tagpack.tags[0]

    assert t.all_fields.get("currency") == "BTC"
    assert t.all_fields.get("network") == "BTC"


def test_tag_overrides_header(tagpack):
    t = tagpack.tags[1]

    assert t.all_fields.get("currency") == "ETH"
    assert t.all_fields.get("network") == "ETH"


def test_init(tagpack):
    assert tagpack.uri == "http://example.com"
    assert isinstance(tagpack.contents, TagPackContents)
    assert tagpack.contents["title"] == "Test TagPack"
    assert isinstance(tagpack.schema, TagPackSchema)
    assert "title" in tagpack.schema.header_fields
    assert isinstance(tagpack.taxonomies, dict)


def test_tag_fields(tagpack):
    assert "lastmod" in tagpack.tag_fields
    assert "creator" not in tagpack.tag_fields
    tagpack.contents["currency"] = "BTC"
    assert "currency" in tagpack.tag_fields


def test_tags(tagpack):
    assert len(tagpack.tags) == 2
    tagpack.contents["tags"] = [
        {"label": "Some attribution tag", "address": "123Bitcoin45"},
        {"label": "Some attribution tag", "address": 1234},
    ]
    assert len(tagpack.tags) == 2
    assert isinstance(tagpack.tags[0], Tag)
    assert isinstance(tagpack.tags[1], Tag)


def test_tags_from_contents(tagpack):
    contents = {"label": "Some attribution tag", "address": "12dv44"}
    assert isinstance(Tag.from_contents(contents, tagpack), Tag)


def test_tags_explicit_fields(tagpack):
    assert len(tagpack.tags) == 2
    all(x in ["label", "address"] for x in tagpack.tags[0].explicit_fields)
    tagpack.contents["tags"] = [
        {"label": "Some attribution tag", "entity": 1234},
    ]
    all(x in ["label", "entity"] for x in tagpack.tags[0].explicit_fields)


def test_tags_all_fields(tagpack):
    assert len(tagpack.tags) == 2
    all(x in ["label", "address", "lastmod"] for x in tagpack.tags[0].all_fields)
    tagpack.contents["tags"] = [
        {"label": "Some attribution tag", "entity": 1234},
    ]
    all(x in ["label", "entity", "lastmod"] for x in tagpack.tags[0].all_fields)


def test_tag_to_json(tagpack):
    for tag in tagpack.tags:
        json = tag.to_json()
        assert "lastmod" in json
        assert "address" in json
        assert "label" in json


def test_tag_to_str(tagpack):
    assert "label" in tagpack.tags[0].__str__()
    assert "lastmod" in tagpack.tags[0].__str__()
    assert "123Bitcoin45" in tagpack.tags[0].__str__()


def test_tagpack_to_json(tagpack):
    json = tagpack.to_json()
    assert "uri" in json
    assert "title" in json
    assert "creator" in json
    assert "lastmod" not in json


def test_tagpack_to_str(tagpack):
    s = tagpack.__str__()
    assert "Test TagPack" in s
    assert "GraphSense Team" in s


# TagPack validation tests


def test_validate(tagpack):
    assert tagpack.validate()


def test_validate_str_date(tagpack_str_date):
    assert tagpack_str_date.validate()


def test_validate_context_obj(tagpack_context_obj):
    assert tagpack_context_obj.validate()


def test_verify_addresses(tagpack):
    assert tagpack.verify_addresses() is None


def test_valid_addresses(tagpack, caplog):
    tagpack.contents["tags"] = [
        {"label": "binance", "address": "1NDyJtNTjmwk5xPNhjgAMu4HDHigtobu1s"},
    ]
    tagpack.verify_addresses()

    # Should have no warning messages for valid addresses
    warning_messages = [
        record.message for record in caplog.records if record.levelname == "WARNING"
    ]
    assert len(warning_messages) == 0


def test_addresses_whitespace(tagpack, caplog):
    tagpack.contents["tags"] = [
        {"label": "binance1", "address": "1NDyJtNTjmwk5xPNhjgAMu4HDHigtobu1s "},
        {"label": "binance2", "address": "1NDyJtNTjmwk5xPNhjgAMu4HDHigtobu1s\t"},
        {"label": "binance3", "address": "\n1NDyJtNTjmwk5xPNhjgAMu4HDHigtobu1s"},
    ]
    tagpack.verify_addresses()

    log_messages = [
        record.message for record in caplog.records if record.levelname == "WARNING"
    ]
    log_text = " ".join(log_messages)

    msg1 = "Address contains whitespace: '1NDyJtNTjmwk5xPNhjgAMu4HDHigtobu1s '"
    msg2 = "Address contains whitespace: '1NDyJtNTjmwk5xPNhjgAMu4HDHigtobu1s\\t'"
    msg3 = "Address contains whitespace: '\\n1NDyJtNTjmwk5xPNhjgAMu4HDHigtobu1s'"
    assert msg1 in log_text
    assert msg2 in log_text
    assert msg3 in log_text


def test_invalid_addresses(tagpack, caplog):
    tagpack.verify_addresses()

    log_messages = [
        record.message for record in caplog.records if record.levelname == "WARNING"
    ]
    log_text = " ".join(log_messages)

    assert "Possible invalid BTC address: 123Bitcoin45" in log_text
    assert "Possible invalid ETH address: 123Bitcoin66" in log_text


def test_context_is_valid_json(tagpack):
    assert json.loads(tagpack.contents["tags"][1]["context"])

    assert "context" not in tagpack.contents["tags"][0].keys()


def test_validate_undefined_field(tagpack):
    tagpack.contents["failfield"] = "some value"

    with pytest.raises(ValidationError) as e:
        tagpack.validate()
    assert "Field failfield not allowed in header" in str(e.value)


def test_validate_fail_type_text(tagpack):
    tagpack.contents["title"] = 5

    with pytest.raises(ValidationError) as e:
        tagpack.validate()
    assert "Field title must be of type text" in str(e.value)


def test_validate_fail_type_date(tagpack):
    tagpack.contents["lastmod"] = 5

    with pytest.raises(ValidationError) as e:
        tagpack.validate()
    assert "Field lastmod must be of type datetime" in str(e.value)


def test_validate_type_datetime_succeedes(tagpack):
    from dateutil.parser import parse

    tagpack.contents["lastmod"] = parse("2024-04-08T18:14:01")

    tagpack.validate()
    assert isinstance(tagpack.contents["lastmod"], datetime)


def test_validate_fail_missing(tagpack):
    del tagpack.contents["creator"]

    with pytest.raises(ValidationError) as e:
        tagpack.validate()
    assert "Mandatory header field creator missing" in str(e.value)


def test_validate_fail_missing_body(tagpack):
    del tagpack.contents["tags"][0]["label"]

    with pytest.raises(ValidationError) as e:
        tagpack.validate()
    assert "Mandatory tag field label missing" in str(e.value)


def test_validate_fail_missing_address(tagpack):
    del tagpack.contents["tags"][0]["address"]

    with pytest.raises(ValidationError) as e:
        tagpack.validate()

    assert "Mandatory tag field address missing" in str(e.value)


def test_validate_fail_is_cluster_definer(tagpack):
    tagpack.contents["is_cluster_definer"] = 3

    with pytest.raises(ValidationError) as e:
        tagpack.validate()
    assert "Field is_cluster_definer must be of type boolean" in str(e.value)


def test_validate_fail_taxonomy(tagpack):
    tagpack.contents["tags"][1]["category"] = "UNKNOWN"

    with pytest.raises(ValidationError) as e:
        tagpack.validate()
    assert "Undefined concept UNKNOWN" in str(e.value)


def test_validate_fail_taxonomy_header(tagpack):
    tagpack.contents["category"] = "unknown"

    with pytest.raises(ValidationError) as e:
        tagpack.validate()
    assert "Undefined concept unknown" in str(e.value)


def test_validate_fail_empty_header_field(tagpack):
    tagpack.contents["label"] = None

    with pytest.raises(ValidationError) as e:
        tagpack.validate()
    assert "Value of header field label must not be empty (None)" in str(e.value)


def test_validate_fail_empty_body_field(tagpack):
    tagpack.contents["tags"][0]["label"] = None

    with pytest.raises(ValidationError) as e:
        tagpack.validate()
    assert "Value of body field label must not be empty (None)" in str(e.value)


def test_simple_file_collection():
    prefix = "tests/testfiles/simple"
    h_files = collect_tagpack_files(prefix)
    header_dir, files = h_files.popitem()

    assert len(files) == 5
    assert f"{prefix}/ex_addr_tagpack.yaml" in files
    assert f"{prefix}/duplicate_tag.yaml" in files
    assert f"{prefix}/empty_tag_list.yaml" in files
    assert f"{prefix}/multiple_tags_for_address.yaml" in files
    assert f"{prefix}/with_concepts.yaml" in files


def test_file_collection_with_yaml_include():
    bdir = "tests/testfiles/yaml_inclusion"
    h_files = collect_tagpack_files(bdir)
    header_dir, files = h_files.popitem()

    assert len(files) == 4
    assert f"{bdir}/2021/01/20210101.yaml" in files
    assert f"{bdir}/2021/01/20210102.yaml" in files
    assert f"{bdir}/2021/02/20210201.yaml" in files
    assert f"{bdir}/2021/01/special/20210106-special.yaml" in files

    assert header_dir == bdir


def test_file_collection_with_multiple_yaml_include():
    bdir = "tests/testfiles/yaml_inclusion_multiple"
    h_files = collect_tagpack_files(bdir)
    files = [f for fs in h_files.values() for f in fs]
    header_dirs = h_files.keys()

    assert len(files) == 4
    assert f"{bdir}/2021/01/20210101.yaml" in files
    assert f"{bdir}/2021/01/20210102.yaml" in files
    assert f"{bdir}/2021/02/20210201.yaml" in files
    assert f"{bdir}/2021/01/special/20210106-special.yaml" in files

    assert len(header_dirs) == 2
    assert f"{bdir}" in header_dirs
    assert f"{bdir}/2021/01" in header_dirs

    files = h_files.pop(f"{bdir}")
    assert f"{bdir}/2021/01/20210101.yaml" in files
    assert f"{bdir}/2021/01/20210102.yaml" in files
    assert f"{bdir}/2021/02/20210201.yaml" in files
    assert f"{bdir}/2021/01/special/20210106-special.yaml" not in files

    files = h_files.pop(f"{bdir}/2021/01")
    assert f"{bdir}/2021/01/20210101.yaml" not in files
    assert f"{bdir}/2021/01/20210102.yaml" not in files
    assert f"{bdir}/2021/02/20210201.yaml" not in files
    assert f"{bdir}/2021/01/special/20210106-special.yaml" in files


def test_file_collection_with_missing_yaml_include_raises_exception():
    bdir = "tests/testfiles/yaml_inclusion_missing_header"
    h_files = collect_tagpack_files(bdir)
    headerfile_path, files = h_files.popitem()
    files = list(files)

    assert not headerfile_path

    with pytest.raises(FileNotFoundError) as e:
        TagPack.load_from_file(None, files[0], None, None, headerfile_path)
    assert "No such file or directory: 'header.yaml'" in str(e.value)


def test_file_collection_simple_with_concepts(taxonomies):
    bdir = "tests/testfiles/simple/with_concepts.yaml"
    h_files = collect_tagpack_files(bdir)
    headerfile_path, files = h_files.popitem()
    files = list(files)

    assert not headerfile_path

    tagpack = TagPack.load_from_file(
        None, files[0], TagPackSchema(), taxonomies, headerfile_path
    )

    assert tagpack.tags[0].all_fields.get("concepts") == ["organization"]
    assert tagpack.tags[1].all_fields.get("concepts") == ["exchange"]

    tagpack.validate()

    tagpack.tags[1].contents["concepts"] = ["asdf"]
    with pytest.raises(ValidationError):
        tagpack.validate()


def test_load_from_file_addr_tagpack(taxonomies):
    tagpack = TagPack.load_from_file(
        "http://example.com/packs/ex_addr_tagpack.yaml",
        "tests/testfiles/simple/ex_addr_tagpack.yaml",
        TagPackSchema(),
        taxonomies,
    )
    assert isinstance(tagpack, TagPack)
    assert tagpack.uri == "http://example.com/packs/ex_addr_tagpack.yaml"
    assert tagpack.contents["title"] == "Test Address TagPack"
    assert isinstance(tagpack.schema, TagPackSchema)
    assert "title" in tagpack.schema.header_fields
    assert isinstance(tagpack.taxonomies, dict)


def test_yaml_inclusion(taxonomies):
    tagpack = TagPack.load_from_file(
        "http://example.com/",
        "tests/testfiles/yaml_inclusion/2021/01/20210101.yaml",
        TagPackSchema(),
        taxonomies,
        "tests/testfiles/yaml_inclusion",
    )
    assert isinstance(tagpack, TagPack)
    assert "title" in tagpack.schema.header_fields
    assert tagpack.contents["title"] == "BadHack TagPack"
    assert tagpack.contents["abuse"] == "scam"
    assert (
        tagpack.tags[0].contents["address"]
        == "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh"
    )
    assert tagpack.tags[0].contents["context"] == '{"validated": true}'


def test_yaml_inclusion_overwrite_abuse(taxonomies):
    tagpack = TagPack.load_from_file(
        "http://example.com/",
        "tests/testfiles/yaml_inclusion/2021/02/20210201.yaml",
        TagPackSchema(),
        taxonomies,
        "tests/testfiles/yaml_inclusion",
    )
    assert isinstance(tagpack, TagPack)
    assert tagpack.contents["title"] == "BadHack TagPack"
    assert tagpack.tags[0].contents["address"] == "1Ai52Uw6usjhpcDrwSmkUvjuqLpcznUuyF"
    assert tagpack.tags[0].contents["abuse"] == "sextortion"


# def test_empty_tag_list_raises(taxonomies):
#     tagpack = TagPack.load_from_file(
#         "http://example.com/packs",
#         "tests/testfiles/simple/empty_tag_list.yaml",
#         TagPackSchema(),
#         taxonomies,
#     )

#     with pytest.raises(ValidationError) as _:
#         tagpack.validate()


def test_multiple_tags_for_one_address_work(taxonomies):
    tagpack = TagPack.load_from_file(
        "http://example.com/packs",
        "tests/testfiles/simple/multiple_tags_for_address.yaml",
        TagPackSchema(),
        taxonomies,
    )
    assert len(tagpack.tags) == 2
    tagpack.validate()


def test_duplicate_does_not_raise_only_inform(caplog, taxonomies):
    tagpack = TagPack.load_from_file(
        "http://example.com/packs",
        "tests/testfiles/simple/duplicate_tag.yaml",
        TagPackSchema(),
        taxonomies,
    )

    tagpack.validate()

    log_messages = [
        record.message for record in caplog.records if record.levelname == "WARNING"
    ]
    log_text = " ".join(log_messages)

    assert "1 duplicate(s) found" in log_text
    assert len(tagpack.get_unique_tags()) == 2


def test_conf_level_mandatory_if_not_set_default(tagpack):
    # tag without confidence validates
    tagpack.validate()
    # because minimal value is set on init if not present.
    assert tagpack.contents["confidence"] == "unknown"


def test_conf_level_mandatory_preserve_user_set_values(tagpack_conf):
    # tag with multi confidence values validates
    tagpack_conf.validate()
    # because minimal value is set on init if not present.
    assert tagpack_conf.contents["confidence"] == "web_crawl"

    assert tagpack_conf.tags[0].all_fields.get("confidence") == "heuristic"
    assert tagpack_conf.tags[1].all_fields.get("confidence") == "web_crawl"


def test_missing_concepts_dont_validate(tagpack):
    tagpack.contents["concepts"] = ["web_crawl", "bad_coding", "blub"]
    with pytest.raises(ValidationError) as e:
        tagpack.validate()
    assert "Undefined concept blub for concepts field" in str(e.value)


def test_existing_concepts_validate(tagpack):
    tagpack.contents["concepts"] = ["web_crawl", "bad_coding"]
    tagpack.validate()


def test_concepts_always_contain_abuse_and_category(tagpack):
    assert set(tagpack.tags[0].contents["concepts"]) == {
        "exchange",
        "bad_coding",
        "stuff",
    }
    assert set(tagpack.tags[1].contents["concepts"]) == {
        "exchange",
        "bad_coding",
        "stuff",
    }
    tagpack.validate()


def test_network_from_currency(caplog, tagpack_w_network):
    t0 = tagpack_w_network.tags[0]
    t1 = tagpack_w_network.tags[1]

    assert t0.all_fields.get("network") == "BTC"
    assert t0.all_fields.get("currency") == "BTC"
    assert t1.all_fields.get("network") == "ETH23"
    assert t1.all_fields.get("currency") == "USDT"

    tagpack_w_network.validate()

    log_messages = [
        record.message for record in caplog.records if record.levelname == "WARNING"
    ]
    log_text = " ".join(log_messages)

    assert "ETH23 is not a known network" in log_text
    assert "CCOIN is not a known network" in log_text
    assert "CCOIN is not a known currency" in log_text
    assert "USDT is not a known network" in log_text
    assert "Did you mean one of: ETH, ARB, ETC, BSC, TRX" in log_text


def test_validate_fail_null_characters_in_header(schema, taxonomies):
    """Test that validation fails when header fields contain null characters"""
    tagpack = TagPack(
        "http://example.com",
        {
            "title": "Test TagPack\x00",  # null character in title
            "creator": "GraphSense Team",
            "source": "http://example.com/my_addresses",
            "currency": "BTC",
            "tags": [{"label": "a", "address": "b"}],
        },
        schema,
        taxonomies,
    )

    with pytest.raises(ValidationError, match="Field 'title' contains null characters"):
        tagpack.validate()


def test_validate_fail_null_characters_in_tag_field(schema, taxonomies):
    """Test that validation fails when tag fields contain null characters"""
    tagpack = TagPack(
        "http://example.com",
        {
            "title": "Test TagPack",
            "creator": "GraphSense Team",
            "source": "http://example.com/my_addresses",
            "currency": "BTC",
            "tags": [
                {
                    "label": "Some attribution\u0000tag",  # null character in label
                    "address": "123Bitcoin45",
                }
            ],
        },
        schema,
        taxonomies,
    )

    with pytest.raises(ValidationError, match="Field 'label' contains null characters"):
        tagpack.validate()


def test_validate_fail_null_characters_in_list_field(schema, taxonomies):
    """Test that validation fails when list fields contain null characters"""
    tagpack = TagPack(
        "http://example.com",
        {
            "title": "Test TagPack",
            "creator": "GraphSense Team",
            "source": "http://example.com/my_addresses",
            "currency": "BTC",
            "tags": [{"label": "a\x00", "address": "b"}],
        },
        schema,
        taxonomies,
    )

    with pytest.raises(ValidationError, match="Field 'label' contains null characters"):
        tagpack.validate()
