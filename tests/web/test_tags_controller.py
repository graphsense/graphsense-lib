from tests.web.helpers import get_json, request_with_status
from tests.web.testdata.tags import (
    base_tagpack_src,
    btc_tag_actorX,
    eth_tag3,
    eth_tag_actor,
    tag5,
    tag6,
    tag7,
)


async def test_get_actor_tags(client):
    result = await get_json(client, "/tags/actors/actorX/tags")
    assert [btc_tag_actorX.to_dict(), eth_tag_actor.to_dict()] == result["address_tags"]

    result = await get_json(client, "/tags/actors/actorY/tags")

    expected_result = [
        {
            "address": "addressE",
            "confidence": "ownership",
            "category": "organization",
            "concepts": [],
            "lastmod": 1562112000,
            "confidence_level": 100,
            "currency": "BTC",
            "entity": 17642138,
            "is_cluster_definer": False,
            "label": "labelX",
            "actor": "actorY",
            "source": "https://source",
            "tagpack_is_public": True,
            "tagpack_title": "GraphSense",
            "tagpack_uri": base_tagpack_src + "tagpack_public.yaml",
            "tagpack_creator": "GraphSense Core Team",
            "tag_type": "actor",
        },
        {
            "address": "0x123456",
            "confidence": "ownership",
            "category": "organization",
            "concepts": [],
            "lastmod": 1562112000,
            "confidence_level": 100,
            "currency": "ETH",
            "entity": 107925001,
            "is_cluster_definer": False,
            "label": "LabelY",
            "actor": "actorY",
            "source": "sourceY",
            "tagpack_is_public": True,
            "tagpack_title": "GraphSense uriY",
            "tagpack_uri": base_tagpack_src + "tagpack_uriY.yaml",
            "tagpack_creator": "GraphSense Core Team",
            "tag_type": "actor",
        },
    ]
    assert expected_result == result["address_tags"]

    result = await get_json(client, "/tags/actors/actorZ/tags")
    assert [] == result["address_tags"]


async def test_get_actor(client):
    result = await get_json(client, "/tags/actors/actorX")
    assert {
        "categories": [
            {"id": "organization", "label": "Organization"},
            {"id": "exchange", "label": "Exchange"},
        ],
        "id": "actorX",
        "jurisdictions": [
            {"id": "SC", "label": "Seychelles"},
            {"id": "VU", "label": "Vanuatu"},
        ],
        "label": "Actor X",
        "nr_tags": 2,
        "uri": "http://actorX",
    } == result

    result = await get_json(client, "/tags/actors/actorY")
    assert {
        "categories": [{"id": "defi_dex", "label": "Decentralized Exchange (DEX)"}],
        "id": "actorY",
        "jurisdictions": [{"id": "AT", "label": "Austria"}],
        "label": "Actor Y",
        "nr_tags": 2,
        "uri": "http://actorY",
    } == result

    result = await request_with_status(client, "/tags/actors/actorZ", 404)
    assert result is None


async def test_list_address_tags(client):
    path = "/tags?label={label}"
    result = await get_json(client, path, label="isolinks")
    t1 = tag5.to_dict()
    t2 = {**t1}
    t2["address"] = "addressY"
    t2["category"] = "organization"
    t2["tagpack_uri"] = t2["tagpack_uri"].replace("public", "private")
    t2["tagpack_is_public"] = False
    t2["is_cluster_definer"] = True
    t2["tagpack_title"] += " Private"
    t2["entity"] = 456
    assert [t1, t2] == result["address_tags"]

    result = await get_json(client, path, auth="unauthorized", label="isolinks")
    assert [t1] == result["address_tags"]

    result = await get_json(client, path, label="cimedy")
    assert [tag6.to_dict(), tag7.to_dict()] == result["address_tags"]

    # test paging
    path_with_page = path + "&pagesize={pagesize}"
    result = await get_json(
        client, path_with_page, label="isolinks", pagesize=1, page=None
    )
    assert [t1] == result["address_tags"]
    path_with_page2 = path + "&pagesize={pagesize}&page={page}"
    result = await get_json(
        client, path_with_page2, label="isolinks", pagesize=1, page=result["next_page"]
    )
    assert [t2] == result["address_tags"]

    assert result.get("next_page") is None

    result = await get_json(client, path, label="TagA")
    assert [eth_tag3.to_dict()] == result["address_tags"]


async def test_list_concepts(client):
    path = "/tags/taxonomies/{taxonomy}/concepts"
    result = await get_json(client, path, taxonomy="entity")
    entity_ids = {e["id"] for e in result}
    taxonomies = {e["taxonomy"] for e in result}
    assert taxonomies == {"concept"}

    assert "exchange" in entity_ids
    assert "organization" in entity_ids

    result = await get_json(client, path, taxonomy="abuse")
    abuse_ids = {e["id"] for e in result}
    taxonomies = {e["taxonomy"] for e in result}
    assert len(entity_ids.intersection(abuse_ids)) == len(abuse_ids)
    assert taxonomies == {"concept"}


async def test_list_taxonomies(client):
    result = await get_json(client, "/tags/taxonomies")
    assert len(result) == 5
