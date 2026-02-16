"""Tests for the ObfuscateTags builtin plugin."""

import pytest
from unittest.mock import MagicMock

from starlette.requests import Request
from starlette.datastructures import Headers, QueryParams

from graphsenselib.web.builtin.plugins.obfuscate_tags.obfuscate_tags import (
    ObfuscateTags,
    GROUPS_HEADER_NAME,
    OBFUSCATION_MARKER_GROUP,
    has_no_obfuscation_group,
    obfuscate_private_tags,
    obfuscate_tagpack_uri_by_rule,
)
from graphsenselib.web.models import (
    AddressTag,
    AddressTags,
    Entity,
    LabeledItemRef,
    NeighborEntities,
    NeighborEntity,
    Rate,
    TxSummary,
    Values,
)


# --- Factories ---


def make_values():
    return Values(fiat_values=[Rate(code="eur", value=0.0)], value=0)


def make_tag(is_public=True, label="Label", source="source", uri="uri", actor="actor"):
    return AddressTag(
        label=label,
        category="exchange",
        concepts=[],
        actor=actor,
        abuse=None,
        tagpack_uri=uri,
        source=source,
        lastmod=0,
        tagpack_title="Title",
        tagpack_is_public=is_public,
        tagpack_creator="Creator",
        is_cluster_definer=True,
        confidence="ownership",
        confidence_level=100,
        tag_type="actor",
        currency="btc",
        address="addr",
        entity=123,
    )


def make_entity(tag=None, actors=None):
    return Entity(
        currency="btc",
        entity=123,
        root_address="addr",
        balance=make_values(),
        first_tx=TxSummary(timestamp=0, height=1, tx_hash="tx"),
        last_tx=TxSummary(timestamp=0, height=1, tx_hash="tx"),
        in_degree=1,
        out_degree=1,
        no_addresses=1,
        no_incoming_txs=1,
        no_outgoing_txs=1,
        total_received=make_values(),
        total_spent=make_values(),
        actors=actors,
        best_address_tag=tag,
        no_address_tags=1,
    )


def make_request(path, headers=None, query=""):
    req = MagicMock(spec=Request)
    req.url = MagicMock(path=path)
    req.headers = Headers(headers or {})
    req.query_params = QueryParams(query)
    req.state = MagicMock(header_modifications={})
    return req


# --- Helper Function Tests ---


@pytest.mark.parametrize(
    "groups,expected",
    [
        (["private"], False),  # Only 'tags-private' is recognized, not bare 'private'
        (["tags-private"], True),
        (["public", "tags-private"], True),
        (["public"], False),
        (["obfuscate"], False),
        ([], False),
    ],
)
def test_has_no_obfuscation_group(groups, expected):
    assert has_no_obfuscation_group(groups) is expected


class TestObfuscatePrivateTags:
    def test_obfuscates_private_tag(self):
        tag = make_tag(is_public=False, label="Private")
        obfuscate_private_tags(tag)
        assert tag.label == "" and tag.source == "" and tag.actor == ""

    def test_preserves_public_tag(self):
        tag = make_tag(is_public=True, label="Public")
        obfuscate_private_tags(tag)
        assert tag.label == "Public"

    def test_handles_list(self):
        tags = [make_tag(is_public=False), make_tag(is_public=True, label="Kept")]
        obfuscate_private_tags(tags)
        assert tags[0].label == "" and tags[1].label == "Kept"

    @pytest.mark.parametrize("value", [None, []])
    def test_handles_empty(self, value):
        obfuscate_private_tags(value)  # Should not raise


class TestObfuscateTagpackUriByRule:
    def test_obfuscates_matching(self):
        tag = make_tag(uri="internal/tp.yaml")
        obfuscate_tagpack_uri_by_rule(r"internal/.*", tag)
        assert tag.tagpack_uri == ""

    def test_preserves_non_matching(self):
        tag = make_tag(uri="public/tp.yaml")
        obfuscate_tagpack_uri_by_rule(r"internal/.*", tag)
        assert tag.tagpack_uri == "public/tp.yaml"


# --- before_request Tests ---


@pytest.mark.parametrize(
    "path,headers,query",
    [
        # Only 'tags-private' is recognized, not bare 'private'
        ("/btc/entities/123", {GROUPS_HEADER_NAME: "tags-private"}, ""),
        ("/btc/entities/123/neighbors", {}, "include_labels=true"),
        ("/btc/entities/123/neighbors", {}, "INCLUDE_LABELS=TRUE"),
        ("/search", {}, ""),
        ("/btc/addresses/bulk", {}, ""),
        ("/tags", {}, ""),
        ("/btc/addresses/abc123", {}, ""),
        ("/eth/addresses/0x123", {}, ""),
    ],
)
def test_before_request_skips(path, headers, query):
    req = make_request(path, headers, query)
    assert ObfuscateTags.before_request({}, req) is None


@pytest.mark.parametrize(
    "path",
    [
        "/btc/entities/123",
        "/btc/addresses/abc123/neighbors",
        "/btc/entities/123/neighbors",
        "/btc/addresses/abc123/tags",
    ],
)
def test_before_request_applies(path):
    req = make_request(path)
    assert ObfuscateTags.before_request({}, req) == {
        GROUPS_HEADER_NAME: OBFUSCATION_MARKER_GROUP
    }


# --- before_response Tests ---


class TestBeforeResponseEntity:
    def test_obfuscates_private_tag(self):
        entity = make_entity(tag=make_tag(is_public=False, label="Private"))
        req = make_request("/btc/entities/123")
        req.state.header_modifications = {GROUPS_HEADER_NAME: OBFUSCATION_MARKER_GROUP}
        ObfuscateTags.before_response({}, req, entity)
        assert entity.best_address_tag.label == ""

    def test_preserves_public_tag(self):
        entity = make_entity(tag=make_tag(is_public=True, label="Public"))
        req = make_request("/btc/entities/123")
        req.state.header_modifications = {GROUPS_HEADER_NAME: OBFUSCATION_MARKER_GROUP}
        ObfuscateTags.before_response({}, req, entity)
        assert entity.best_address_tag.label == "Public"

    def test_obfuscates_actors_with_private_tag(self):
        actors = [LabeledItemRef(id="a1", label="Actor1")]
        entity = make_entity(tag=make_tag(is_public=False), actors=actors)
        req = make_request("/btc/entities/123")
        req.state.header_modifications = {GROUPS_HEADER_NAME: OBFUSCATION_MARKER_GROUP}
        ObfuscateTags.before_response({}, req, entity)
        assert entity.actors[0].id == "" and entity.actors[0].label == ""


class TestBeforeResponseAddressTags:
    def test_obfuscates_private_tags_in_list(self):
        tags = AddressTags(
            address_tags=[
                make_tag(is_public=False),
                make_tag(is_public=True, label="Kept"),
            ],
            next_page=None,
        )
        req = make_request("/btc/addresses/abc/tags")
        req.state.header_modifications = {GROUPS_HEADER_NAME: OBFUSCATION_MARKER_GROUP}
        ObfuscateTags.before_response({}, req, tags)
        assert tags.address_tags[0].label == "" and tags.address_tags[1].label == "Kept"


class TestBeforeResponseNeighborEntities:
    def test_obfuscates_neighbor_tags(self):
        neighbors = NeighborEntities(
            neighbors=[
                NeighborEntity(
                    entity=make_entity(tag=make_tag(is_public=False)),
                    value=make_values(),
                    no_txs=1,
                ),
                NeighborEntity(
                    entity=make_entity(tag=make_tag(is_public=True, label="Kept")),
                    value=make_values(),
                    no_txs=1,
                ),
            ],
            next_page=None,
        )
        req = make_request("/btc/entities/123/neighbors")
        req.state.header_modifications = {GROUPS_HEADER_NAME: OBFUSCATION_MARKER_GROUP}
        ObfuscateTags.before_response({}, req, neighbors)
        assert neighbors.neighbors[0].entity.best_address_tag.label == ""
        assert neighbors.neighbors[1].entity.best_address_tag.label == "Kept"


@pytest.mark.parametrize(
    "group", ["tags-private"]
)  # Only 'tags-private' skips obfuscation
def test_before_response_skips_with_no_obfuscation_group(group):
    entity = make_entity(tag=make_tag(is_public=False, label="Private"))
    req = make_request("/btc/entities/123", {GROUPS_HEADER_NAME: group})
    ObfuscateTags.before_response({}, req, entity)
    assert entity.best_address_tag.label == "Private"


class TestTagpackUriRule:
    def test_applies_rule_to_matching_uri(self):
        entity = make_entity(tag=make_tag(is_public=True, uri="internal/tp.yaml"))
        req = make_request("/btc/entities/123")
        req.state.header_modifications = {GROUPS_HEADER_NAME: OBFUSCATION_MARKER_GROUP}
        ObfuscateTags.before_response(
            {"config": {"obfuscate_tagpack_uri_rule": r"internal/.*"}}, req, entity
        )
        assert entity.best_address_tag.tagpack_uri == ""
        assert entity.best_address_tag.label == "Label"  # Public tag label preserved

    def test_preserves_non_matching_uri(self):
        entity = make_entity(tag=make_tag(is_public=True, uri="public/tp.yaml"))
        req = make_request("/btc/entities/123")
        req.state.header_modifications = {GROUPS_HEADER_NAME: OBFUSCATION_MARKER_GROUP}
        ObfuscateTags.before_response(
            {"config": {"obfuscate_tagpack_uri_rule": r"internal/.*"}}, req, entity
        )
        assert entity.best_address_tag.tagpack_uri == "public/tp.yaml"
