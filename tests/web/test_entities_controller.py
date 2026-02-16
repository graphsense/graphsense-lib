import copy

from graphsenselib.web.models import (
    AddressTxUtxo,
    AddressTxs,
    EntityAddresses,
    Links,
    TxAccount,
)
from graphsenselib.web.util.values_legacy import convert_value
from tests.web.helpers import assert_equal_sorted, get_json
from tests.web.testdata.addresses import (
    entityWithTags,
    eth_address,
    eth_entityWithTags,
    eth_entityWithTokens,
)
from tests.web.testdata.entities import (
    eth_entity,
    eth_entity2,
    eth_entityWithTagsOutNeighbors,
    entityWithTagsAddresses,
    entityWithTagsInNeighbors,
    entityWithTagsOutNeighbors,
    tag_entityA,
    tag_entityB,
    tag_entityC,
    tag_entityD,
    tag_entityE,
)
from tests.web.testdata.tags import eth_tag1, eth_tag2, tag1, tag2, tag3, tag4
from tests.web.testdata.txs import tx1_eth, tx2_eth, tx4_eth, tx22_eth


def test_get_entity(client):
    path = "/{currency}/entities/{entity}"
    result = get_json(client, path, currency="btc", entity=entityWithTags.entity)
    ewt = entityWithTags.to_dict()
    ewt["best_address_tag"]["inherited_from"] = "cluster"
    assert ewt == result

    result = get_json(
        client, path, auth="unauthorized", currency="btc", entity=entityWithTags.entity
    )
    ewt["no_address_tags"] = 3
    assert ewt == result

    result = get_json(client, path, currency="eth", entity=eth_entity.entity)
    assert eth_entity.to_dict() == result

    path_actors = path + "?include_actors={include_actors}"
    result = get_json(
        client, path_actors, currency="eth", entity=eth_entity2.entity, include_actors=True
    )
    assert eth_entity2.to_dict() == result

    result = get_json(
        client, path, currency="eth", entity=eth_entity2.entity, include_actors=False
    )
    ee = eth_entity2.to_dict()
    ee.pop("actors")
    assert ee == result

    # test best_address_tag:

    # a cluster with multiple addresses, none cluster definer
    #   -> no best address tag
    result = get_json(client, path, currency="btc", entity=tag_entityA.entity)
    assert tag_entityA.to_dict() == result

    # a cluster with multiple addresses, one cluster definer
    #   -> this one tag is best address tag
    result = get_json(client, path, currency="btc", entity=tag_entityB.entity)
    assert tag_entityB.to_dict() == result

    # a cluster with multiple addresses, multiple cluster definers
    #   -> the one with highest confidence
    result = get_json(client, path, currency="btc", entity=tag_entityC.entity)
    assert tag_entityC.to_dict() == result

    # If cluster size = 1 and there is an address tag on that single address
    #   -> the one tag is best address tag
    result = get_json(client, path, currency="btc", entity=tag_entityD.entity)
    assert tag_entityD.to_dict() == result

    # If cluster size = 1 and there are several address tags on that address
    #   -> the one with highest confidence
    result = get_json(client, path, currency="btc", entity=tag_entityE.entity)
    assert tag_entityE.to_dict() == result

    # omit best_address_tag
    path_excl = path + "?exclude_best_address_tag={exclude_best_address_tag}"
    result = get_json(
        client,
        path_excl,
        currency="btc",
        entity=tag_entityE.entity,
        exclude_best_address_tag=True,
    )
    t = tag_entityE.to_dict()
    t.pop("best_address_tag")
    assert t == result


def test_list_address_tags_by_entity(client):
    path = "/{currency}/entities/{entity}/tags"
    result = get_json(client, path, currency="btc", entity=entityWithTags.entity)
    expected = [tag1, tag4, tag2, tag3]
    assert [e.to_dict() for e in expected] == result["address_tags"]

    result = get_json(client, path, currency="eth", entity=eth_entity.entity)
    t1 = eth_tag1.to_dict()
    t1.pop("inherited_from")
    expected_eth = [t1, eth_tag2.to_dict()]
    assert expected_eth == result["address_tags"]

    result = get_json(
        client,
        path,
        auth="unauthorized",
        currency="eth",
        entity=eth_entity.entity,
        level="address",
    )
    public_address_tags = [tag for tag in expected_eth if tag["tagpack_is_public"]]
    assert public_address_tags == result["address_tags"]


def test_list_entity_neighbors(client):
    basepath = "/{currency}/entities/{entity}/neighbors?direction={direction}"
    path = basepath + "&include_labels={include_labels}"
    path_actors = path + "&include_actors={include_actors}"
    ewton = entityWithTagsOutNeighbors.to_dict()
    result = get_json(
        client,
        path,
        currency="btc",
        entity=entityWithTags.entity,
        include_labels=True,
        direction="out",
    )
    assert ewton == result

    result = get_json(
        client,
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

    result = get_json(
        client,
        path,
        currency="btc",
        entity=entityWithTags.entity,
        include_labels=True,
        direction="in",
    )
    assert entityWithTagsInNeighbors.to_dict() == result

    result = get_json(
        client,
        path_actors,
        currency="eth",
        entity=eth_entity.entity,
        include_labels=True,
        include_actors=True,
        direction="out",
    )
    assert eth_entityWithTagsOutNeighbors.to_dict() == result

    result = get_json(
        client,
        path_actors,
        currency="eth",
        entity=eth_entity.entity,
        include_labels=False,
        include_actors=False,
        direction="out",
    )
    ewton_eth = eth_entityWithTagsOutNeighbors.to_dict()
    for n in ewton_eth["neighbors"]:
        n.pop("labels", None)
        n["entity"].pop("actors", None)
    assert ewton_eth == result

    path_only = basepath + "&only_ids={only_ids}"
    result = get_json(
        client,
        path_only,
        currency="btc",
        entity="17642138",
        direction="in",
        only_ids="67065,144534",
    )
    assert (
        [n.entity.entity for n in entityWithTagsInNeighbors.neighbors]
        == [n["entity"]["entity"] for n in result["neighbors"]]
    )

    result = get_json(
        client,
        path_only,
        currency="btc",
        entity="17642138",
        direction="in",
        only_ids="144534",
    )
    assert (
        [entityWithTagsInNeighbors.neighbors[1].entity.entity]
        == [n["entity"]["entity"] for n in result["neighbors"]]
    )

    result = get_json(
        client,
        path_only,
        currency="eth",
        entity=eth_entity.entity,
        direction="out",
        only_ids=eth_entityWithTagsOutNeighbors.neighbors[0].entity.entity,
    )
    assert (
        [eth_entityWithTagsOutNeighbors.neighbors[0].entity.entity]
        == [n["entity"]["entity"] for n in result["neighbors"]]
    )


def test_list_entity_addresses(client):
    path = "/{currency}/entities/{entity}/addresses"
    result = get_json(client, path, currency="btc", entity=entityWithTags.entity)
    assert entityWithTagsAddresses.to_dict() == result

    result = get_json(client, path, currency="eth", entity=eth_entity.entity)
    assert (
        EntityAddresses(next_page=None, addresses=[eth_address]).to_dict() == result
    )


def test_search_entity_neighbors(client):
    path = (
        "/{currency}/entities/{entity}/search"
        "?direction={direction}"
        "&key={key}"
        "&value={value}"
        "&depth={depth}"
        "&breadth={breadth}"
    )

    # Test category matching
    category = "exchange"
    result = get_json(
        client,
        path,
        currency="btc",
        entity=entityWithTags.entity,
        direction="out",
        depth=2,
        breadth=10,
        key="category",
        value=",".join([category]),
    )
    assert 2818641 == result[0]["neighbor"]["entity"]["entity"]
    assert 123 == result[0]["paths"][0]["neighbor"]["entity"]["entity"]
    assert (
        category
        == result[0]["paths"][0]["neighbor"]["entity"]["best_address_tag"]["category"]
    )

    result = get_json(
        client,
        path,
        currency="btc",
        entity=entityWithTags.entity,
        direction="in",
        depth=2,
        breadth=10,
        key="category",
        value=",".join([category]),
    )
    assert 67065 == result[0]["neighbor"]["entity"]["entity"]
    assert 123 == result[0]["paths"][0]["neighbor"]["entity"]["entity"]
    assert (
        category
        == result[0]["paths"][0]["neighbor"]["entity"]["best_address_tag"]["category"]
    )

    # Test addresses matching
    addresses = ["abcdefg", "xyz1278"]
    result = get_json(
        client,
        path,
        currency="btc",
        entity=entityWithTags.entity,
        direction="out",
        depth=2,
        breadth=10,
        key="addresses",
        value=",".join(addresses),
    )
    assert 2818641 == result[0]["neighbor"]["entity"]["entity"]
    assert 456 == result[0]["paths"][0]["neighbor"]["entity"]["entity"]
    assert addresses == [
        a["address"] for a in result[0]["paths"][0]["matching_addresses"]
    ]

    result = get_json(
        client,
        path,
        currency="btc",
        entity=entityWithTags.entity,
        direction="out",
        depth=2,
        breadth=10,
        key="entities",
        value=",".join(["123"]),
    )
    assert 2818641 == result[0]["neighbor"]["entity"]["entity"]
    assert 123 == result[0]["paths"][0]["neighbor"]["entity"]["entity"]

    addresses = ["abcdefg"]
    result = get_json(
        client,
        path,
        currency="btc",
        entity=entityWithTags.entity,
        direction="out",
        depth=2,
        breadth=10,
        key="addresses",
        value=",".join(addresses),
    )
    assert 2818641 == result[0]["neighbor"]["entity"]["entity"]
    assert 456 == result[0]["paths"][0]["neighbor"]["entity"]["entity"]
    assert addresses == [
        a["address"] for a in result[0]["paths"][0]["matching_addresses"]
    ]

    addresses = ["0x234567"]
    result = get_json(
        client,
        path,
        currency="eth",
        entity=eth_entity.entity,
        direction="out",
        depth=2,
        breadth=10,
        key="addresses",
        value=",".join(addresses),
    )
    assert 107925001 == result[0]["neighbor"]["entity"]["entity"]
    assert 107925002 == result[0]["paths"][0]["neighbor"]["entity"]["entity"]
    assert addresses == [
        a["address"] for a in result[0]["paths"][0]["matching_addresses"]
    ]

    # Test value matching
    result = get_json(
        client,
        path,
        currency="btc",
        entity=entityWithTags.entity,
        direction="out",
        depth=2,
        breadth=10,
        key="total_received",
        value=",".join(["value", "5", "150"]),
    )
    assert 2818641 == result[0]["neighbor"]["entity"]["entity"]
    assert 789 == result[0]["paths"][0]["neighbor"]["entity"]["entity"]
    assert (
        10 == result[0]["paths"][0]["neighbor"]["entity"]["total_received"]["value"]
    )

    result = get_json(
        client,
        path,
        currency="btc",
        entity=entityWithTags.entity,
        direction="out",
        depth=2,
        breadth=10,
        key="total_received",
        value=",".join(["value", "5", "8"]),
    )
    assert [] == result

    result = get_json(
        client,
        path,
        currency="btc",
        entity=entityWithTags.entity,
        direction="out",
        depth=2,
        breadth=10,
        key="total_received",
        value=",".join(["eur", "50", "100"]),
    )
    assert 2818641 == result[0]["neighbor"]["entity"]["entity"]
    assert 789 == result[0]["paths"][0]["neighbor"]["entity"]["entity"]
    assert (
        100.0
        == result[0]["paths"][0]["neighbor"]["entity"]["total_received"]["fiat_values"][
            0
        ]["value"]
    )

    addresses = ["abcdefg", "xyz1278"]
    result = get_json(
        client,
        path,
        currency="btc",
        entity=entityWithTags.entity,
        direction="out",
        depth=7,
        breadth=10,
        key="addresses",
        value=",".join(addresses),
    )
    assert 2818641 == result[0]["neighbor"]["entity"]["entity"]
    assert 456 == result[0]["paths"][0]["neighbor"]["entity"]["entity"]
    assert addresses == [
        a["address"] for a in result[0]["paths"][0]["matching_addresses"]
    ]


def test_list_entity_txs(client):
    path = "/{currency}/entities/{entity}/txs"
    path_with_pagesize = path + "?pagesize={pagesize}&page={page}"
    rate_data = get_json(client, "/btc/rates/2")
    txs = [
        AddressTxUtxo(
            tx_hash="123456",
            currency="btc",
            value=convert_value("btc", 1260000, rate_data),
            coinbase=False,
            height=3,
            timestamp=1510347493,
        ),
        AddressTxUtxo(
            tx_hash="abcdef",
            currency="btc",
            value=convert_value("btc", -1260000, rate_data),
            coinbase=False,
            height=2,
            timestamp=1511153263,
        ),
        AddressTxUtxo(
            tx_hash="ab1880",
            currency="btc",
            value=convert_value("btc", -1, rate_data),
            coinbase=False,
            height=1,
            timestamp=1434554207,
        ),
    ]
    entity_txs = AddressTxs(next_page=None, address_txs=txs)
    result = get_json(
        client, path_with_pagesize, currency="btc", entity=144534, pagesize=2, page=""
    )

    assert entity_txs.to_dict()["address_txs"][0:2] == result["address_txs"]
    assert result["next_page"] is not None

    result = get_json(
        client,
        path_with_pagesize,
        currency="btc",
        entity=144534,
        pagesize=2,
        page=result["next_page"],
    )

    assert entity_txs.to_dict()["address_txs"][2:3] == result["address_txs"]
    assert result.get("next_page", None) is None

    path_with_order = path + "?order={order}"
    _reversed = list(reversed(entity_txs.to_dict()["address_txs"]))
    result = get_json(
        client, path_with_order, currency="btc", entity=144534, order="asc"
    )
    assert _reversed == result["address_txs"]

    path_with_order_and_page = path_with_order + "&pagesize={pagesize}&page={page}"
    result = get_json(
        client,
        path_with_order_and_page,
        currency="btc",
        entity=144534,
        order="asc",
        pagesize=2,
        page="",
    )
    assert _reversed[0:2] == result["address_txs"]
    assert result.get("next_page", None) is not None

    result = get_json(
        client,
        path_with_order_and_page,
        currency="btc",
        entity=144534,
        order="asc",
        pagesize=2,
        page=result["next_page"],
    )

    assert _reversed[2:3] == result["address_txs"]
    assert result.get("next_page", None) is None

    path_with_direction = "/{currency}/entities/{entity}/txs?direction={direction}"
    result = get_json(
        client, path_with_direction, currency="btc", entity=144534, direction="out"
    )
    entity_txs.address_txs = txs[1:]
    assert_equal_sorted(entity_txs.to_dict(), result, "address_txs", "tx_hash")

    result = get_json(
        client, path_with_direction, currency="btc", entity=144534, direction="in"
    )
    entity_txs.address_txs = txs[0:1]
    assert_equal_sorted(entity_txs.to_dict(), result, "address_txs", "tx_hash")

    path_with_range = (
        path_with_direction + "&min_height={min_height}&max_height={max_height}"
    )
    result = get_json(
        client,
        path_with_range,
        currency="btc",
        entity=144534,
        direction="",
        min_height=2,
        max_height="",
    )
    entity_txs.address_txs = txs[0:2]
    assert_equal_sorted(entity_txs.to_dict(), result, "address_txs", "tx_hash")

    result = get_json(
        client,
        path_with_range,
        currency="btc",
        entity=144534,
        direction="",
        min_height="",
        max_height=2,
    )
    entity_txs.address_txs = txs[1:3]
    assert_equal_sorted(entity_txs.to_dict(), result, "address_txs", "tx_hash")

    result = get_json(
        client,
        path_with_range,
        currency="btc",
        entity=144534,
        direction="",
        min_height=2,
        max_height=2,
    )
    entity_txs.address_txs = txs[1:2]
    assert_equal_sorted(entity_txs.to_dict(), result, "address_txs", "tx_hash")

    def reverse(tx):
        tx_r = TxAccount.from_dict(copy.deepcopy(tx.to_dict()))
        tx_r.value.value = -tx_r.value.value
        for v in tx_r.value.fiat_values:
            v.value = -v.value
        return tx_r

    tx2_eth_r = reverse(tx2_eth)
    tx22_eth_r = reverse(tx22_eth)
    txs_eth = AddressTxs(address_txs=[tx4_eth, tx22_eth_r, tx2_eth_r, tx1_eth])
    result = get_json(
        client, path, currency="eth", entity=eth_entityWithTags.entity
    )
    assert txs_eth.to_dict() == result

    result = get_json(
        client,
        path_with_direction,
        currency="eth",
        entity=eth_entityWithTags.entity,
        direction="out",
    )
    assert txs_eth.to_dict()["address_txs"][1:3] == result["address_txs"]

    path_with_range_and_tc = path_with_range + "&token_currency={token_currency}"
    result = get_json(
        client,
        path_with_range_and_tc,
        currency="eth",
        entity=eth_entityWithTags.entity,
        direction="",
        min_height=3,
        max_height="",
        token_currency="",
    )
    assert txs_eth.to_dict()["address_txs"][0:2] == result["address_txs"]

    result = get_json(
        client,
        path_with_range_and_tc,
        currency="eth",
        entity=eth_entityWithTags.entity,
        direction="",
        min_height=1,
        max_height=2,
        token_currency="",
    )
    assert txs_eth.to_dict()["address_txs"][2:4] == result["address_txs"]

    result = get_json(
        client,
        path_with_range_and_tc,
        currency="eth",
        entity=eth_entityWithTags.entity,
        direction="",
        min_height="",
        max_height=3,
        token_currency="",
    )
    assert txs_eth.to_dict()["address_txs"][1:4] == result["address_txs"]

    result = get_json(
        client, path, currency="eth", entity=eth_entityWithTokens.entity
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

    result = get_json(
        client,
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


# async def test_list_entity_links(client):
#     path = "/{currency}/entities/{entity}/links?neighbor={neighbor}"
#     result = await get_json(
#         client, path, currency="btc", entity=144534, neighbor=10102718
#     )
#     link = Links(links=[])
#     assert link.to_dict() == result
#
#     result = await get_json(
#         client, path, currency="btc", entity=10102718, neighbor=144534
#     )
#     link = Links(links=[])
#     assert link.to_dict() == result
#
#     result = await get_json(
#         client, path, currency="eth", entity=107925000, neighbor=107925001
#     )
#     txs_links = Links(links=[tx2_eth, tx22_eth])
#     assert_equal_sorted(txs_links.to_dict(), result, "links", "tx_hash")
#
#     result = await get_json(
#         client, path + "&order=asc", currency="eth", entity=107925000, neighbor=107925001
#     )
#     assert ["af6e0003", "af6e0004"] == [x["tx_hash"] for x in result["links"]]
#
#     result = await get_json(
#         client, path + "&order=desc", currency="eth", entity=107925000, neighbor=107925001
#     )
#     assert ["af6e0004", "af6e0003"] == [x["tx_hash"] for x in result["links"]]
