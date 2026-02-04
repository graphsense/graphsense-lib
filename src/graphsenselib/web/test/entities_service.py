import copy

import graphsenselib.web.test.tags_service as ts
from graphsenselib.web.service.rates_service import list_rates
from graphsenselib.web.test.addresses_service import (
    addressD,
    addressE,
    entity2,
    entity3,
    entity4,
    entity5,
    entityWithTags,
    eth_address,
    eth_address2,
    eth_addressWithTagsOutNeighbors,
    eth_entityWithTags,
    eth_entityWithTokens,
)
from graphsenselib.web.models import (
    AddressTag,
    AddressTxUtxo,
    AddressTxs,
    Entity,
    EntityAddresses,
    LabeledItemRef,
    Links,
    NeighborEntities,
    NeighborEntity,
    TxAccount,
    TxSummary,
)
from graphsenselib.web.test.txs_service import tx1_eth, tx2_eth, tx4_eth, tx22_eth
from graphsenselib.web.util.values_legacy import convert_value, make_values

tagstore_public_tp_uri = "tagpack_public.yaml"

eth_entity = Entity(
    currency="eth",
    no_outgoing_txs=eth_address.no_outgoing_txs,
    last_tx=eth_address.last_tx,
    total_spent=eth_address.total_spent,
    in_degree=eth_address.in_degree,
    no_addresses=1,
    no_address_tags=2,
    total_received=eth_address.total_received,
    no_incoming_txs=eth_address.no_incoming_txs,
    entity=107925000,
    root_address=eth_address.address,
    out_degree=eth_address.out_degree,
    first_tx=eth_address.first_tx,
    balance=eth_address.balance,
    best_address_tag=ts.eth_tag1,
)

eth_entity2 = Entity(
    currency="eth",
    actors=[
        LabeledItemRef(id="actorX", label="Actor X"),
        LabeledItemRef(id="actorY", label="Actor Y"),
    ],
    no_outgoing_txs=eth_address2.no_outgoing_txs,
    last_tx=eth_address2.last_tx,
    total_spent=eth_address2.total_spent,
    in_degree=eth_address2.in_degree,
    no_addresses=1,
    no_address_tags=2,
    total_received=eth_address2.total_received,
    no_incoming_txs=eth_address2.no_incoming_txs,
    entity=107925001,
    root_address=eth_address2.address,
    out_degree=eth_address2.out_degree,
    first_tx=eth_address2.first_tx,
    balance=eth_address2.balance,
    total_tokens_received=eth_address2.total_tokens_received,
)

eth_neighbors = []
for n in eth_addressWithTagsOutNeighbors.neighbors:
    d = n.to_dict()
    d.pop("address")
    nn = NeighborEntity(**d)
    # nn.labels = []
    eth_neighbors.append(nn)

eth_neighbors[0].entity = eth_entity
eth_neighbors[0].entity.best_address_tag.inherited_from = "cluster"
# eth_neighbors[0].labels = [ts.eth_etag1.label]
eth_neighbors[1].entity = eth_entity2

eth_entityWithTagsOutNeighbors = NeighborEntities(
    next_page=None, neighbors=eth_neighbors
)

entityWithTagsOutNeighbors = NeighborEntities(
    next_page=None,
    neighbors=[
        NeighborEntity(
            entity=entity2,
            value=make_values(eur=2411.06, usd=3074.92, value=48610000000),
            labels=["labelX", "labelY"],
            no_txs=1,
        ),
        NeighborEntity(
            entity=entity3,
            value=make_values(eur=1078.04, usd=1397.54, value=3375700000),
            labels=[],
            no_txs=3,
        ),
    ],
)

entityWithTagsInNeighbors = NeighborEntities(
    next_page=None,
    neighbors=[
        NeighborEntity(
            entity=entity4,
            value=make_values(usd=0.96, eur=0.72, value=190000),
            labels=[],
            no_txs=10,
        ),
        NeighborEntity(
            entity=entity5,
            value=make_values(eur=295.7, usd=404.02, value=50000000),
            labels=[],
            no_txs=1,
        ),
    ],
)

entityWithTagsAddresses = EntityAddresses(
    next_page=None, addresses=[addressD, addressE]
)

tag_entityA = Entity(
    currency="btc",
    no_address_tags=2,
    no_outgoing_txs=0,
    last_tx=TxSummary(timestamp=1434554207, height=1, tx_hash="4567"),
    total_spent=make_values(usd=0.0, value=0, eur=0.0),
    in_degree=0,
    no_addresses=2,
    total_received=make_values(usd=0.0, value=0, eur=0.0),
    no_incoming_txs=0,
    entity=12,
    root_address="tag_addressA",
    out_degree=0,
    first_tx=TxSummary(timestamp=1434554207, height=1, tx_hash="4567"),
    balance=make_values(eur=0.0, usd=0.0, value=0),
    best_address_tag=None,
)

tag_entityB = Entity(
    currency="btc",
    no_address_tags=2,
    no_outgoing_txs=0,
    last_tx=TxSummary(timestamp=1434554207, height=1, tx_hash="4567"),
    total_spent=make_values(usd=0.0, value=0, eur=0.0),
    in_degree=0,
    no_addresses=2,
    total_received=make_values(usd=0.0, value=0, eur=0.0),
    no_incoming_txs=0,
    entity=14,
    root_address="tag_addressC",
    out_degree=0,
    first_tx=TxSummary(timestamp=1434554207, height=1, tx_hash="4567"),
    balance=make_values(eur=0.0, usd=0.0, value=0),
    best_address_tag=AddressTag(
        category="organization",
        label="x",
        abuse=None,
        lastmod=1562112000,
        source="Unspecified",
        address="tag_addressC",
        currency="BTC",
        tagpack_is_public=True,
        is_cluster_definer=True,
        confidence="ownership",
        confidence_level=100,
        tagpack_creator="GraphSense Core Team",
        tagpack_title="GraphSense",
        inherited_from="cluster",
        tagpack_uri=tagstore_public_tp_uri,
        concepts=[],
        entity=14,
        tag_type="actor",
    ),
)

tag_entityC = Entity(
    currency="btc",
    no_address_tags=3,
    no_outgoing_txs=0,
    last_tx=TxSummary(timestamp=1434554207, height=1, tx_hash="4567"),
    total_spent=make_values(usd=0.0, value=0, eur=0.0),
    in_degree=0,
    no_addresses=3,
    total_received=make_values(usd=0.0, value=0, eur=0.0),
    no_incoming_txs=0,
    entity=16,
    root_address="tag_addressE",
    out_degree=0,
    first_tx=TxSummary(timestamp=1434554207, height=1, tx_hash="4567"),
    balance=make_values(eur=0.0, usd=0.0, value=0),
    best_address_tag=AddressTag(
        category="organization",
        label="x",
        abuse=None,
        lastmod=1562112000,
        source="Unspecified",
        address="tag_addressE",
        currency="BTC",
        tagpack_is_public=True,
        is_cluster_definer=True,
        confidence="ownership",
        confidence_level=100,
        tagpack_creator="GraphSense Core Team",
        tagpack_title="GraphSense",
        tagpack_uri=tagstore_public_tp_uri,
        inherited_from="cluster",
        concepts=[],
        entity=16,
        tag_type="actor",
    ),
)

tag_entityD = Entity(
    currency="btc",
    no_address_tags=1,
    no_outgoing_txs=0,
    last_tx=TxSummary(timestamp=1434554207, height=1, tx_hash="4567"),
    total_spent=make_values(usd=0.0, value=0, eur=0.0),
    in_degree=0,
    no_addresses=1,
    total_received=make_values(usd=0.0, value=0, eur=0.0),
    no_incoming_txs=0,
    entity=19,
    root_address="tag_addressH",
    out_degree=0,
    first_tx=TxSummary(timestamp=1434554207, height=1, tx_hash="4567"),
    balance=make_values(eur=0.0, usd=0.0, value=0),
    best_address_tag=AddressTag(
        category="organization",
        label="x",
        abuse=None,
        lastmod=1562112000,
        source="Unspecified",
        address="tag_addressH",
        currency="BTC",
        tagpack_is_public=True,
        is_cluster_definer=False,
        confidence="ownership",
        confidence_level=100,
        tagpack_creator="GraphSense Core Team",
        tagpack_title="GraphSense",
        tagpack_uri=tagstore_public_tp_uri,
        concepts=[],
        inherited_from="cluster",
        entity=19,
        tag_type="actor",
    ),
)

tag_entityE = Entity(
    currency="btc",
    no_address_tags=3,
    no_outgoing_txs=0,
    last_tx=TxSummary(timestamp=1434554207, height=1, tx_hash="4567"),
    total_spent=make_values(usd=0.0, value=0, eur=0.0),
    in_degree=0,
    no_addresses=1,
    total_received=make_values(usd=0.0, value=0, eur=0.0),
    no_incoming_txs=0,
    entity=20,
    root_address="tag_addressI",
    out_degree=0,
    first_tx=TxSummary(timestamp=1434554207, height=1, tx_hash="4567"),
    balance=make_values(eur=0.0, usd=0.0, value=0),
    best_address_tag=AddressTag(
        category="organization",
        label="x",
        abuse=None,
        lastmod=1562112000,
        source="Unspecified",
        address="tag_addressI",
        currency="BTC",
        tagpack_is_public=True,
        is_cluster_definer=False,
        confidence="ownership",
        confidence_level=100,
        tagpack_creator="GraphSense Core Team",
        tagpack_title="GraphSense",
        tagpack_uri=tagstore_public_tp_uri,
        inherited_from="cluster",
        concepts=[],
        entity=20,
        tag_type="actor",
    ),
)


async def get_entity(test_case):
    path = "/{currency}/entities/{entity}"
    result = await test_case.request(path, currency="btc", entity=entityWithTags.entity)
    ewt = entityWithTags.to_dict()
    ewt["best_address_tag"]["inherited_from"] = "cluster"
    assert ewt == result

    result = await test_case.request(
        path, auth="unauthorized", currency="btc", entity=entityWithTags.entity
    )
    ewt["no_address_tags"] = 3
    assert ewt == result

    result = await test_case.request(path, currency="eth", entity=eth_entity.entity)

    assert eth_entity.to_dict() == result

    path_actors = path + "?include_actors={include_actors}"
    result = await test_case.request(
        path_actors, currency="eth", entity=eth_entity2.entity, include_actors=True
    )

    assert eth_entity2.to_dict() == result

    result = await test_case.request(
        path, currency="eth", entity=eth_entity2.entity, include_actors=False
    )

    ee = eth_entity2.to_dict()
    ee.pop("actors")
    assert ee == result

    # test best_address_tag:

    # a cluster with multiple addresses, none cluster definer
    #   -> no best address tag

    result = await test_case.request(path, currency="btc", entity=tag_entityA.entity)

    assert tag_entityA.to_dict() == result

    # a cluster with multiple addresses, one cluster definer
    #   -> this one tag is best address tag

    result = await test_case.request(path, currency="btc", entity=tag_entityB.entity)

    assert tag_entityB.to_dict() == result

    # a cluster with multiple addresses, multiple cluster definers
    #   -> the one with highest confidence

    result = await test_case.request(path, currency="btc", entity=tag_entityC.entity)

    assert tag_entityC.to_dict() == result

    # If cluster size = 1 and there is an address tag on that single address
    #   -> the one tag is best address tag

    result = await test_case.request(path, currency="btc", entity=tag_entityD.entity)

    assert tag_entityD.to_dict() == result

    # If cluster size = 1 and there are several address tags on that address
    #   -> the one with highest confidence

    result = await test_case.request(path, currency="btc", entity=tag_entityE.entity)

    assert tag_entityE.to_dict() == result

    # omit best_address_tag

    path_excl = path + "?exclude_best_address_tag={exclude_best_address_tag}"
    result = await test_case.request(
        path_excl,
        currency="btc",
        entity=tag_entityE.entity,
        exclude_best_address_tag=True,
    )

    t = tag_entityE.to_dict()
    t.pop("best_address_tag")
    assert t == result


async def list_address_tags_by_entity(test_case):
    path = "/{currency}/entities/{entity}/tags"
    result = await test_case.request(path, currency="btc", entity=entityWithTags.entity)
    expected = [ts.tag1, ts.tag4, ts.tag2, ts.tag3]

    assert [e.to_dict() for e in expected] == result["address_tags"]

    result = await test_case.request(path, currency="eth", entity=eth_entity.entity)
    t1 = ts.eth_tag1.to_dict()
    t1.pop("inherited_from")
    expected = [t1, ts.eth_tag2.to_dict()]
    assert expected == result["address_tags"]

    result = await test_case.request(
        path,
        auth="unauthorized",
        currency="eth",
        entity=eth_entity.entity,
        level="address",
    )
    public_address_tags = [tag for tag in expected if tag["tagpack_is_public"]]
    test_case.assertEqual(public_address_tags, result["address_tags"])


async def list_entity_neighbors(test_case):
    basepath = "/{currency}/entities/{entity}/neighbors?direction={direction}"
    path = basepath + "&include_labels={include_labels}"
    path_actors = path + "&include_actors={include_actors}"
    ewton = entityWithTagsOutNeighbors.to_dict()
    result = await test_case.request(
        path,
        currency="btc",
        entity=entityWithTags.entity,
        include_labels=True,
        direction="out",
    )
    assert ewton == result

    result = await test_case.request(
        path,
        auth="unauthorized",
        currency="btc",
        entity=entityWithTags.entity,
        include_labels=True,
        direction="out",
    )
    ewton["neighbors"][0]["labels"] = ["labelX"]
    ewton["neighbors"][0]["entity"]["no_address_tags"] = 1
    assert ewton == result

    result = await test_case.request(
        path,
        currency="btc",
        entity=entityWithTags.entity,
        include_labels=True,
        direction="in",
    )
    assert entityWithTagsInNeighbors.to_dict() == result

    result = await test_case.request(
        path_actors,
        currency="eth",
        entity=eth_entity.entity,
        include_labels=True,
        include_actors=True,
        direction="out",
    )

    assert eth_entityWithTagsOutNeighbors.to_dict() == result

    result = await test_case.request(
        path_actors,
        currency="eth",
        entity=eth_entity.entity,
        include_labels=False,
        include_actors=False,
        direction="out",
    )

    ewton = eth_entityWithTagsOutNeighbors.to_dict()
    for n in ewton["neighbors"]:
        n.pop("labels", None)
        n["entity"].pop("actors", None)
    assert ewton == result

    path = basepath + "&only_ids={only_ids}"
    result = await test_case.request(
        path, currency="btc", entity="17642138", direction="in", only_ids="67065,144534"
    )

    test_case.assertEqual(
        [n.entity.entity for n in entityWithTagsInNeighbors.neighbors],
        [n["entity"]["entity"] for n in result["neighbors"]],
    )

    result = await test_case.request(
        path, currency="btc", entity="17642138", direction="in", only_ids="144534"
    )

    test_case.assertEqual(
        [entityWithTagsInNeighbors.neighbors[1].entity.entity],
        [n["entity"]["entity"] for n in result["neighbors"]],
    )

    result = await test_case.request(
        path,
        currency="eth",
        entity=eth_entity.entity,
        direction="out",
        only_ids=eth_entityWithTagsOutNeighbors.neighbors[0].entity.entity,
    )

    test_case.assertEqual(
        [eth_entityWithTagsOutNeighbors.neighbors[0].entity.entity],
        [n["entity"]["entity"] for n in result["neighbors"]],
    )


async def list_entity_addresses(test_case):
    path = "/{currency}/entities/{entity}/addresses"
    result = await test_case.request(path, currency="btc", entity=entityWithTags.entity)

    test_case.assertEqual(entityWithTagsAddresses.to_dict(), result)

    result = await test_case.request(path, currency="eth", entity=eth_entity.entity)

    test_case.assertEqual(
        EntityAddresses(next_page=None, addresses=[eth_address]).to_dict(), result
    )


async def search_entity_neighbors(test_case):
    # Test category matching

    path = (
        "/{currency}/entities/{entity}/search"
        "?direction={direction}"
        "&key={key}"
        "&value={value}"
        "&depth={depth}"
        "&breadth={breadth}"
    )

    category = "exchange"
    result = await test_case.request(
        path,
        currency="btc",
        entity=entityWithTags.entity,
        direction="out",
        depth=2,
        breadth=10,
        key="category",
        value=",".join([category]),
    )

    test_case.assertEqual(2818641, result[0]["neighbor"]["entity"]["entity"])
    test_case.assertEqual(123, result[0]["paths"][0]["neighbor"]["entity"]["entity"])
    test_case.assertEqual(
        category,
        result[0]["paths"][0]["neighbor"]["entity"]["best_address_tag"]["category"],
    )  # noqa

    category = "exchange"
    result = await test_case.request(
        path,
        currency="btc",
        entity=entityWithTags.entity,
        direction="in",
        depth=2,
        breadth=10,
        key="category",
        value=",".join([category]),
    )
    test_case.assertEqual(67065, result[0]["neighbor"]["entity"]["entity"])
    test_case.assertEqual(123, result[0]["paths"][0]["neighbor"]["entity"]["entity"])
    test_case.assertEqual(
        category,
        result[0]["paths"][0]["neighbor"]["entity"]["best_address_tag"]["category"],
    )  # noqa

    # Test addresses matching

    addresses = ["abcdefg", "xyz1278"]
    result = await test_case.request(
        path,
        currency="btc",
        entity=entityWithTags.entity,
        direction="out",
        depth=2,
        breadth=10,
        key="addresses",
        value=",".join(addresses),
    )
    test_case.assertEqual(2818641, result[0]["neighbor"]["entity"]["entity"])
    test_case.assertEqual(456, result[0]["paths"][0]["neighbor"]["entity"]["entity"])
    test_case.assertEqual(
        addresses, [a["address"] for a in result[0]["paths"][0]["matching_addresses"]]
    )

    result = await test_case.request(
        path,
        currency="btc",
        entity=entityWithTags.entity,
        direction="out",
        depth=2,
        breadth=10,
        key="entities",
        value=",".join(["123"]),
    )
    test_case.assertEqual(2818641, result[0]["neighbor"]["entity"]["entity"])
    test_case.assertEqual(123, result[0]["paths"][0]["neighbor"]["entity"]["entity"])

    addresses = ["abcdefg"]
    result = await test_case.request(
        path,
        currency="btc",
        entity=entityWithTags.entity,
        direction="out",
        depth=2,
        breadth=10,
        key="addresses",
        value=",".join(addresses),
    )
    test_case.assertEqual(2818641, result[0]["neighbor"]["entity"]["entity"])
    test_case.assertEqual(456, result[0]["paths"][0]["neighbor"]["entity"]["entity"])
    test_case.assertEqual(
        addresses, [a["address"] for a in result[0]["paths"][0]["matching_addresses"]]
    )

    addresses = ["0x234567"]
    result = await test_case.request(
        path,
        currency="eth",
        entity=eth_entity.entity,
        direction="out",
        depth=2,
        breadth=10,
        key="addresses",
        value=",".join(addresses),
    )
    test_case.assertEqual(107925001, result[0]["neighbor"]["entity"]["entity"])
    test_case.assertEqual(
        107925002, result[0]["paths"][0]["neighbor"]["entity"]["entity"]
    )
    test_case.assertEqual(
        addresses, [a["address"] for a in result[0]["paths"][0]["matching_addresses"]]
    )

    # Test value matching

    result = await test_case.request(
        path,
        currency="btc",
        entity=entityWithTags.entity,
        direction="out",
        depth=2,
        breadth=10,
        key="total_received",
        value=",".join(["value", "5", "150"]),
    )
    test_case.assertEqual(2818641, result[0]["neighbor"]["entity"]["entity"])
    test_case.assertEqual(789, result[0]["paths"][0]["neighbor"]["entity"]["entity"])
    test_case.assertEqual(
        10, result[0]["paths"][0]["neighbor"]["entity"]["total_received"]["value"]
    )

    # Test value matching

    result = await test_case.request(
        path,
        currency="btc",
        entity=entityWithTags.entity,
        direction="out",
        depth=2,
        breadth=10,
        key="total_received",
        value=",".join(["value", "5", "8"]),
    )
    test_case.assertEqual([], result)
    #
    # Test value matching

    result = await test_case.request(
        path,
        currency="btc",
        entity=entityWithTags.entity,
        direction="out",
        depth=2,
        breadth=10,
        key="total_received",
        value=",".join(["eur", "50", "100"]),
    )
    test_case.assertEqual(2818641, result[0]["neighbor"]["entity"]["entity"])
    test_case.assertEqual(789, result[0]["paths"][0]["neighbor"]["entity"]["entity"])
    test_case.assertEqual(
        100.0,
        result[0]["paths"][0]["neighbor"]["entity"]["total_received"]["fiat_values"][0][
            "value"
        ],
    )

    addresses = ["abcdefg", "xyz1278"]
    result = await test_case.request(
        path,
        currency="btc",
        entity=entityWithTags.entity,
        direction="out",
        depth=7,
        breadth=10,
        key="addresses",
        value=",".join(addresses),
    )
    test_case.assertEqual(2818641, result[0]["neighbor"]["entity"]["entity"])
    test_case.assertEqual(456, result[0]["paths"][0]["neighbor"]["entity"]["entity"])
    test_case.assertEqual(
        addresses, [a["address"] for a in result[0]["paths"][0]["matching_addresses"]]
    )


async def list_entity_txs(test_case):
    """Test case for list_entity_txs

    Get all transactions an entity has been involved in
    """
    path = "/{currency}/entities/{entity}/txs"
    path_with_pagesize = path + "?pagesize={pagesize}&page={page}"
    rates = await list_rates(test_case, currency="btc", heights=[2])
    txs = [
        AddressTxUtxo(
            tx_hash="123456",
            currency="btc",
            value=convert_value("btc", 1260000, rates[2]),
            coinbase=False,
            height=3,
            timestamp=1510347493,
        ),
        AddressTxUtxo(
            tx_hash="abcdef",
            currency="btc",
            value=convert_value("btc", -1260000, rates[2]),
            coinbase=False,
            height=2,
            timestamp=1511153263,
        ),
        AddressTxUtxo(
            tx_hash="ab1880",
            currency="btc",
            value=convert_value("btc", -1, rates[2]),
            coinbase=False,
            height=1,
            timestamp=1434554207,
        ),
    ]
    entity_txs = AddressTxs(next_page=None, address_txs=txs)
    result = await test_case.request(
        path_with_pagesize, currency="btc", entity=144534, pagesize=2, page=""
    )

    test_case.assertEqual(
        entity_txs.to_dict()["address_txs"][0:2], result["address_txs"]
    )
    test_case.assertNotEqual(result["next_page"], None)

    result = await test_case.request(
        path_with_pagesize,
        currency="btc",
        entity=144534,
        pagesize=2,
        page=result["next_page"],
    )

    test_case.assertEqual(
        entity_txs.to_dict()["address_txs"][2:3], result["address_txs"]
    )
    test_case.assertEqual(result.get("next_page", None), None)

    path_with_order = path + "?order={order}"
    _reversed = list(reversed(entity_txs.to_dict()["address_txs"]))
    result = await test_case.request(
        path_with_order, currency="btc", entity=144534, order="asc"
    )
    test_case.assertEqual(_reversed, result["address_txs"])

    path_with_order_and_page = path_with_order + "&pagesize={pagesize}&page={page}"
    result = await test_case.request(
        path_with_order_and_page,
        currency="btc",
        entity=144534,
        order="asc",
        pagesize=2,
        page="",
    )
    test_case.assertEqual(_reversed[0:2], result["address_txs"])
    test_case.assertNotEqual(result.get("next_page", None), None)

    result = await test_case.request(
        path_with_order_and_page,
        currency="btc",
        entity=144534,
        order="asc",
        pagesize=2,
        page=result["next_page"],
    )

    test_case.assertEqual(_reversed[2:3], result["address_txs"])
    test_case.assertEqual(result.get("next_page", None), None)

    path_with_direction = "/{currency}/entities/{entity}/txs?direction={direction}"
    result = await test_case.request(
        path_with_direction, currency="btc", entity=144534, direction="out"
    )
    entity_txs.address_txs = txs[1:]
    test_case.assertEqualWithList(
        entity_txs.to_dict(), result, "address_txs", "tx_hash"
    )

    result = await test_case.request(
        path_with_direction, currency="btc", entity=144534, direction="in"
    )
    entity_txs.address_txs = txs[0:1]
    test_case.assertEqualWithList(
        entity_txs.to_dict(), result, "address_txs", "tx_hash"
    )

    path_with_range = (
        path_with_direction + "&min_height={min_height}&max_height={max_height}"
    )
    result = await test_case.request(
        path_with_range,
        currency="btc",
        entity=144534,
        direction="",
        min_height=2,
        max_height="",
    )
    entity_txs.address_txs = txs[0:2]
    test_case.assertEqualWithList(
        entity_txs.to_dict(), result, "address_txs", "tx_hash"
    )

    result = await test_case.request(
        path_with_range,
        currency="btc",
        entity=144534,
        direction="",
        min_height="",
        max_height=2,
    )
    entity_txs.address_txs = txs[1:3]
    test_case.assertEqualWithList(
        entity_txs.to_dict(), result, "address_txs", "tx_hash"
    )

    result = await test_case.request(
        path_with_range,
        currency="btc",
        entity=144534,
        direction="",
        min_height=2,
        max_height=2,
    )
    entity_txs.address_txs = txs[1:2]
    test_case.assertEqualWithList(
        entity_txs.to_dict(), result, "address_txs", "tx_hash"
    )

    def reverse(tx):
        tx_r = TxAccount.from_dict(copy.deepcopy(tx.to_dict()))
        tx_r.value.value = -tx_r.value.value
        for v in tx_r.value.fiat_values:
            v.value = -v.value
        return tx_r

    tx2_eth_r = reverse(tx2_eth)
    tx22_eth_r = reverse(tx22_eth)
    txs = AddressTxs(address_txs=[tx4_eth, tx22_eth_r, tx2_eth_r, tx1_eth])
    result = await test_case.request(
        path, currency="eth", entity=eth_entityWithTags.entity
    )
    test_case.assertEqual(txs.to_dict(), result)

    result = await test_case.request(
        path_with_direction,
        currency="eth",
        entity=eth_entityWithTags.entity,
        direction="out",
    )
    test_case.assertEqual(txs.to_dict()["address_txs"][1:3], result["address_txs"])

    path_with_range_and_tc = path_with_range + "&token_currency={token_currency}"
    result = await test_case.request(
        path_with_range_and_tc,
        currency="eth",
        entity=eth_entityWithTags.entity,
        direction="",
        min_height=3,
        max_height="",
        token_currency="",
    )
    test_case.assertEqual(txs.to_dict()["address_txs"][0:2], result["address_txs"])

    result = await test_case.request(
        path_with_range_and_tc,
        currency="eth",
        entity=eth_entityWithTags.entity,
        direction="",
        min_height=1,
        max_height=2,
        token_currency="",
    )
    test_case.assertEqual(txs.to_dict()["address_txs"][2:4], result["address_txs"])

    result = await test_case.request(
        path_with_range_and_tc,
        currency="eth",
        entity=eth_entityWithTags.entity,
        direction="",
        min_height="",
        max_height=3,
        token_currency="",
    )
    test_case.assertEqual(txs.to_dict()["address_txs"][1:4], result["address_txs"])

    result = await test_case.request(
        path, currency="eth", entity=eth_entityWithTokens.entity
    )
    assert len(result["address_txs"]) == 5
    assert [x["currency"] for x in result["address_txs"]] == [
        "eth",
        "eth",
        "weth",
        "usdt",
        "eth",
    ]

    assert [x["value"]["value"] for x in result["address_txs"]] == [
        124000000000000000000,
        123000000000000000000,
        -6818627949560085517,
        -3360488227,
        -123000000000000000000,
    ]
    assert [x["height"] for x in result["address_txs"]] == [3, 2, 2, 2, 1]

    result = await test_case.request(
        path_with_range_and_tc,
        currency="eth",
        entity=eth_entityWithTokens.entity,
        direction="",
        min_height=2,
        max_height=2,
        token_currency="weth",
    )

    assert len(result["address_txs"]) == 1
    assert [x["currency"] for x in result["address_txs"]] == ["weth"]
    assert [x["height"] for x in result["address_txs"]] == [2]


async def list_entity_links(test_case):
    path = "/{currency}/entities/{entity}/links?neighbor={neighbor}"
    result = await test_case.request(
        path, currency="btc", entity=144534, neighbor=10102718
    )
    link = Links(
        links=[
            # LinkUtxo(
            #    tx_hash="abcdef",
            #    currency="btc",
            #    input_value=make_values(eur=-0.01, usd=-0.03, value=-1260000),
            #    output_value=make_values(eur=0.01, usd=0.03, value=1260000),
            #    timestamp=1511153263,
            #    height=2,
            # )
        ]
    )
    test_case.assertEqual(link.to_dict(), result)

    result = await test_case.request(
        path, currency="btc", entity=10102718, neighbor=144534
    )
    link = Links(
        links=[
            # LinkUtxo(
            #    tx_hash="123456",
            #    currency="btc",
            #    input_value=make_values(eur=-0.01, usd=-0.03, value=-1260000),
            #    output_value=make_values(eur=0.01, usd=0.03, value=1260000),
            #    timestamp=1510347493,
            #    height=3,
            # )
        ]
    )
    test_case.assertEqual(link.to_dict(), result)

    result = await test_case.request(
        path, currency="eth", entity=107925000, neighbor=107925001
    )
    txs = Links(links=[tx2_eth, tx22_eth])
    test_case.assertEqualWithList(txs.to_dict(), result, "links", "tx_hash")

    result = await test_case.request(
        path + "&order=asc", currency="eth", entity=107925000, neighbor=107925001
    )
    test_case.assertEqual(
        ["af6e0003", "af6e0004"], [x["tx_hash"] for x in result["links"]]
    )

    result = await test_case.request(
        path + "&order=desc", currency="eth", entity=107925000, neighbor=107925001
    )
    test_case.assertEqual(
        ["af6e0004", "af6e0003"], [x["tx_hash"] for x in result["links"]]
    )
