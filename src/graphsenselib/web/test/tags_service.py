from graphsenselib.web.models import AddressTag, Concept

base_tagpack_src = ""

tag1 = AddressTag(
    category="organization",
    label="Internet, Archive",
    abuse=None,
    lastmod=1562112000,
    source="https://archive.org/donate/cryptocurrency",
    address="addressA",
    currency="BTC",
    tagpack_uri=base_tagpack_src + "tagpack_public.yaml",
    tagpack_is_public=True,
    is_cluster_definer=True,
    confidence="ownership",
    confidence_level=100,
    tagpack_creator="GraphSense Core Team",
    tagpack_title="GraphSense",
    concepts=[],
    entity=17642138,
    tag_type="actor",
)

tag2 = AddressTag(
    abuse=None,
    address="addressA",
    category="organization",
    currency="BTC",
    label="addressTag1",
    lastmod=1562112000,
    source="https://archive.org/donate/cryptocurrency",
    tagpack_uri=base_tagpack_src + "tagpack_public.yaml",
    tagpack_is_public=True,
    is_cluster_definer=False,
    confidence="forensic",
    confidence_level=50,
    tagpack_creator="GraphSense Core Team",
    tagpack_title="GraphSense",
    concepts=[],
    entity=17642138,
    tag_type="actor",
)

tag3 = AddressTag(
    category="organization",
    label="Internet Archive 2",
    abuse="scam",
    lastmod=1562112000,
    source="https://archive.org/donate/cryptocurrency",
    address="addressA",
    tagpack_uri=base_tagpack_src + "tagpack_private.yaml",
    currency="BTC",
    tagpack_is_public=False,
    is_cluster_definer=True,
    confidence="web_crawl",
    confidence_level=20,
    tagpack_creator="GraphSense Core Team",
    tagpack_title="GraphSense Private",
    concepts=["scam"],
    entity=17642138,
    tag_type="actor",
)

tag4 = AddressTag(
    abuse=None,
    address="addressH",
    category="organization",
    currency="BTC",
    label="addressTag2",
    lastmod=1562112000,
    source="https://archive.org/donate/cryptocurrency",
    tagpack_uri=base_tagpack_src + "tagpack_public.yaml",
    tagpack_is_public=True,
    is_cluster_definer=False,
    tagpack_creator="GraphSense Core Team",
    tagpack_title="GraphSense",
    confidence="ownership",
    confidence_level=100,
    concepts=[],
    entity=17642138,
    tag_type="actor",
)

eth_tag1 = AddressTag(
    category="organization",
    label="TagA",
    abuse=None,
    lastmod=1562112000,
    source="sourceX",
    address="0xabcdef",
    tagpack_uri=base_tagpack_src + "tagpack_uriX.yaml",
    currency="ETH",
    tagpack_is_public=False,
    is_cluster_definer=True,
    confidence="ownership",
    confidence_level=100,
    tagpack_creator="GraphSense Core Team",
    tagpack_title="GraphSense uriX",
    concepts=[],
    entity=107925000,
    tag_type="actor",
)

eth_tag2 = AddressTag(
    category="organization",
    label="TagB",
    abuse=None,
    lastmod=1562112000,
    source="sourceY",
    address="0xabcdef",
    currency="ETH",
    tagpack_is_public=True,
    tagpack_uri=base_tagpack_src + "tagpack_uriY.yaml",
    is_cluster_definer=False,
    confidence="ownership",
    confidence_level=100,
    tagpack_creator="GraphSense Core Team",
    tagpack_title="GraphSense uriY",
    concepts=[],
    entity=107925000,
    tag_type="actor",
)

etag1 = AddressTag(
    category=tag1.category,
    label=tag1.label,
    abuse=tag1.abuse,
    lastmod=tag1.lastmod,
    source=tag1.source,
    address=tag1.address,
    tagpack_uri=tag1.tagpack_uri,
    currency=tag1.currency,
    confidence=tag1.confidence,
    confidence_level=tag1.confidence_level,
    tagpack_creator=tag1.tagpack_creator,
    tagpack_title=tag1.tagpack_title,
    is_cluster_definer=True,
    tagpack_is_public=tag1.tagpack_is_public,
    concepts=[],
    tag_type="actor",
)

tag5 = AddressTag(
    tagpack_uri=base_tagpack_src + "tagpack_public.yaml",
    lastmod=1562112000,
    label="isolinks",
    source="Unspecified",
    category="exchange",
    currency="BTC",
    address="addressX",
    abuse=None,
    confidence="ownership",
    confidence_level=100,
    tagpack_creator="GraphSense Core Team",
    tagpack_title="GraphSense",
    tagpack_is_public=True,
    is_cluster_definer=True,
    concepts=[],
    entity=123,
    tag_type="actor",
)

tag6 = AddressTag(
    lastmod=1562112000,
    source="Unspecified",
    category="organization",
    abuse=None,
    address="addressY",
    tagpack_uri=base_tagpack_src + "tagpack_public.yaml",
    currency="BTC",
    label="cimedy",
    confidence="ownership",
    confidence_level=100,
    tagpack_creator="GraphSense Core Team",
    tagpack_title="GraphSense",
    tagpack_is_public=True,
    is_cluster_definer=False,
    concepts=[],
    entity=456,
    tag_type="actor",
)

tag7 = AddressTag(
    lastmod=1562112000,
    source="source",
    abuse=None,
    address="addressA",
    category="exchange",
    tagpack_uri=base_tagpack_src + "tagpack_public.yaml",
    currency="LTC",
    label="cimedy",
    confidence="ownership",
    confidence_level=100,
    tagpack_creator="GraphSense Core Team",
    tagpack_title="GraphSense",
    tagpack_is_public=True,
    is_cluster_definer=False,
    concepts=[],
    tag_type="actor",
)

tag8 = AddressTag(
    category="organization",
    label="labelX",
    abuse=None,
    lastmod=1562112000,
    source="https://source",
    address="address2818641",
    currency="BTC",
    is_cluster_definer=True,
    confidence="ownership",
    confidence_level=100,
    tagpack_creator="GraphSense Core Team",
    tagpack_is_public=True,
    tagpack_title="GraphSense",
    tagpack_uri=base_tagpack_src + "tagpack_public.yaml",
    concepts=[],
    entity=2818641,
    tag_type="actor",
)

eth_tag3 = AddressTag(
    lastmod=1562112000,
    source="sourceX",
    abuse=None,
    address="0xabcdef",
    category="organization",
    tagpack_uri=base_tagpack_src + "tagpack_uriX.yaml",
    currency="ETH",
    label="TagA",
    tagpack_is_public=False,
    is_cluster_definer=True,
    confidence="ownership",
    confidence_level=100,
    tagpack_creator="GraphSense Core Team",
    tagpack_title="GraphSense uriX",
    concepts=[],
    entity=107925000,
    tag_type="actor",
)

eth_etag1 = eth_tag1.to_dict()
eth_etag1 = AddressTag(**eth_etag1)

etag4 = AddressTag(
    category=tag2.category,
    label=tag2.label,
    abuse=tag2.abuse,
    lastmod=tag2.lastmod,
    source=tag2.source,
    address=tag2.address,
    tagpack_uri=tag2.tagpack_uri,
    currency=tag2.currency,
    is_cluster_definer=True,
    tagpack_is_public=tag2.tagpack_is_public,
    tag_type="actor",
)

eth_tag_actor = AddressTag(
    confidence_level=100,
    label="LabelX",
    confidence="ownership",
    category="organization",
    lastmod=1562112000,
    source="sourceX",
    actor="actorX",
    address="0x123456",
    concepts=[],
    entity=107925001,
    currency="ETH",
    tagpack_creator="GraphSense Core Team",
    tagpack_title="GraphSense uriX",
    tagpack_uri=base_tagpack_src + "tagpack_uriX.yaml",
    is_cluster_definer=False,
    tagpack_is_public=False,
    tag_type="actor",
)

btc_tag_actorX = AddressTag(
    confidence_level=100,
    label="abcdefgLabel",
    confidence="ownership",
    category="organization",
    lastmod=1562112000,
    tagpack_title="GraphSense",
    tagpack_creator="GraphSense Core Team",
    source="https://source",
    actor="actorX",
    address="abcdefg",
    concepts=[],
    entity=456,
    currency="BTC",
    tagpack_uri=base_tagpack_src + "tagpack_public.yaml",
    is_cluster_definer=False,
    tagpack_is_public=True,
    tag_type="actor",
)


async def get_actor_tags(test_case):
    result = await test_case.request("/tags/actors/actorX/tags")
    assert [btc_tag_actorX.to_dict(), eth_tag_actor.to_dict()] == result["address_tags"]

    result = await test_case.request("/tags/actors/actorY/tags")

    expexted_result = [
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
    assert expexted_result == result["address_tags"]

    result = await test_case.request("/tags/actors/actorZ/tags")
    assert [] == result["address_tags"]


async def get_actor(test_case):
    result = await test_case.request("/tags/actors/actorX")
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

    result = await test_case.request("/tags/actors/actorY")
    assert {
        "categories": [{"id": "defi_dex", "label": "Decentralized Exchange (DEX)"}],
        "id": "actorY",
        "jurisdictions": [{"id": "AT", "label": "Austria"}],
        "label": "Actor Y",
        "nr_tags": 2,
        "uri": "http://actorY",
    } == result

    result = await test_case.requestWithCodeAndBody("/tags/actors/actorZ", 404, None)
    test_case.assertEqual(None, result)


async def list_address_tags(test_case):
    path = "/tags?label={label}"
    result = await test_case.request(path, label="isolinks")
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

    result = await test_case.request(path, auth="unauthorized", label="isolinks")
    assert [t1] == result["address_tags"]

    result = await test_case.request(path, label="cimedy")
    assert [tag6.to_dict(), tag7.to_dict()] == result["address_tags"]

    # test paging

    path_with_page = path + "&pagesize={pagesize}"
    result = await test_case.request(
        path_with_page, label="isolinks", pagesize=1, page=None
    )
    assert [t1] == result["address_tags"]
    path_with_page += "&page={page}"
    result = await test_case.request(
        path_with_page, label="isolinks", pagesize=1, page=result["next_page"]
    )
    assert [t2] == result["address_tags"]

    assert result.get("next_page") is None
    # result = await test_case.request(
    #     path_with_page, label="isolinks", pagesize=1, page=result["next_page"]
    # )
    # test_case.assertEqual(AddressTags(address_tags=[]).to_dict(), result)

    result = await test_case.request(path, label="TagA")
    assert [eth_tag3.to_dict()] == result["address_tags"]


organization = Concept(
    uri="https://organization",
    id="organization",
    taxonomy="entity",
    label="An organization",
    description="An organization is foo.",
)

exchange = Concept(
    uri="https://exchange",
    id="exchange",
    taxonomy="entity",
    label="An exchange",
    description="An exchange is foo.",
)

conceptB = Concept(
    uri="https://conceptB",
    id="conceptB",
    taxonomy="abuse",
    label="Concept B",
    description="A concept B.",
)


async def list_concepts(test_case):
    path = "/tags/taxonomies/{taxonomy}/concepts"
    result = await test_case.request(path, taxonomy="entity")
    entity_ids = {e["id"] for e in result}
    taxonomies = {e["taxonomy"] for e in result}
    assert taxonomies == {"concept"}

    assert "exchange" in entity_ids
    assert "organization" in entity_ids

    result = await test_case.request(path, taxonomy="abuse")
    abuse_ids = {e["id"] for e in result}
    taxonomies = {e["taxonomy"] for e in result}
    assert len(entity_ids.intersection(abuse_ids)) == len(abuse_ids)
    assert taxonomies == {"concept"}


async def list_taxonomies(test_case):
    result = await test_case.request("/tags/taxonomies")
    assert len(result) == 5
