"""Back-compat smoke tests for the deprecated `/entities/...` routes.

The primary test coverage for cluster endpoints lives in
`test_clusters_controller.py`. This file only verifies that the legacy
`entity`-named surface is still reachable and returns the same payload shape.
Delete this file when the deprecation window ends and `/entities/...` is
removed.
"""

from tests.web.helpers import get_json
from tests.web.testdata.addresses import entityWithTags
from tests.web.testdata.entities import eth_entity


def _strip(result):
    """Remove the top-level next_page key so single-entity results compare."""
    return {k: v for k, v in result.items() if k != "next_page"}


def test_legacy_entities_path_matches_clusters_path(client):
    """`/entities/{entity}` must return identical JSON to `/clusters/{cluster}`."""
    entity_id = entityWithTags.entity
    via_entities = get_json(
        client, "/{currency}/entities/{entity}", currency="btc", entity=entity_id
    )
    via_clusters = get_json(
        client, "/{currency}/clusters/{cluster}", currency="btc", cluster=entity_id
    )
    assert via_entities == via_clusters


def test_legacy_entity_addresses_matches_cluster_addresses(client):
    entity_id = entityWithTags.entity
    via_entities = get_json(
        client,
        "/{currency}/entities/{entity}/addresses",
        currency="btc",
        entity=entity_id,
    )
    via_clusters = get_json(
        client,
        "/{currency}/clusters/{cluster}/addresses",
        currency="btc",
        cluster=entity_id,
    )
    assert via_entities == via_clusters


def test_legacy_response_contains_both_entity_and_cluster_keys(client):
    """Dual-emit contract: responses carry both keys during the deprecation window."""
    result = get_json(
        client,
        "/{currency}/entities/{entity}",
        currency="eth",
        entity=eth_entity.entity,
    )
    assert "entity" in result
    assert "cluster" in result
    assert result["entity"] == result["cluster"]
