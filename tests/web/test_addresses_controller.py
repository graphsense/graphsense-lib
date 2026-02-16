import copy

from graphsenselib.utils.rest_utils import omit
from graphsenselib.utils.tron import evm_to_tron_address_string
from graphsenselib.web.models import (
    Address,
    AddressTxUtxo,
    AddressTxs,
    TxAccount,
)
from graphsenselib.web.util.values_legacy import convert_value
from tests.web.helpers import get_json, raw_request
from tests.web.testdata.addresses import (
    address,
    address2,
    addressF,
    addressWithTagsInNeighbors,
    addressWithTagsOutNeighbors,
    addressWithTags,
    addressWithTotalSpent0,
    addressWithoutTags,
    entityWithTags,
    eth_address,
    eth_address2,
    eth_address2WithTokenFlows,
    eth_addressWithTags,
    eth_addressWithTagsOutNeighbors,
    eth_entityWithTags,
)
from tests.web.testdata.tags import tag1, tag3
from tests.web.testdata.txs import tx1_eth, tx2_eth, tx4_eth, tx22_eth


def test_get_address(client):
    basepath = "/{currency}/addresses/{address}"
    path = basepath + "?include_tags={include_tags}"
    result = get_json(
        client, path, currency="btc", address=addressWithoutTags.address, include_tags=True
    )

    assert addressWithoutTags.to_dict() == result

    result = get_json(
        client, path, currency="btc", address=addressWithTags.address, include_tags=True
    )
    awt = addressWithTags.to_dict()
    assert awt == result
    awt_public = Address(**awt)
    awt_public.tags = [tag1, tag3]
    result = get_json(
        client,
        path,
        currency="btc",
        auth="unauthorized",
        address=addressWithTags.address,
        include_tags=True,
    )
    assert awt_public.to_dict() == result

    result = get_json(
        client, basepath, currency="btc", address=addressWithTotalSpent0.address
    )
    assert addressWithTotalSpent0.to_dict() == result

    # ETH
    result = get_json(
        client, basepath, currency="eth", address=eth_addressWithTags.address
    )
    assert eth_address.to_dict() == result

    status, body = raw_request(
        client,
        "/{currency}/addresses/{address}",
        currency="eth",
        address="1Archi6M1r5b41Rvn1SY2FfJAzsrEUT7aT",
    )
    assert status == 400
    assert "The address provided does not look like a ETH address" in body

    # non supported currency
    status, body = raw_request(
        client,
        "/{currency}/addresses/{address}",
        currency="DOGE",
        address="DBgS3X3hveRppkeywm9C6HMJKZb2CG8nGV",
    )
    assert status == 404
    assert "Network doge not supported" in body

    result = get_json(
        client,
        path,
        currency="trx",
        address=evm_to_tron_address_string("0xabcdef"),
        include_tags=False,
    )

    assert result["address"] == evm_to_tron_address_string("0xabcdef")

    result = get_json(
        client,
        path,
        currency="trx",
        address=evm_to_tron_address_string("0x123456"),
        include_tags=False,
    )

    assert result["address"] == evm_to_tron_address_string("0x123456")


def test_list_address_txs(client):
    path = "/{currency}/addresses/{address}/txs"
    path_with_pagesize = path + "?pagesize={pagesize}&page={page}"
    rate_data = get_json(client, "/btc/rates/2")
    txs = [
        AddressTxUtxo(
            tx_hash="123456",
            currency="btc",
            value=convert_value("btc", 1260000, rate_data),
            height=3,
            coinbase=False,
            timestamp=1510347493,
        ),
        AddressTxUtxo(
            tx_hash="abcdef",
            currency="btc",
            value=convert_value("btc", -1260000, rate_data),
            height=2,
            coinbase=False,
            timestamp=1511153263,
        ),
        AddressTxUtxo(
            tx_hash="ab1880",
            currency="btc",
            value=convert_value("btc", -1, rate_data),
            height=1,
            coinbase=False,
            timestamp=1434554207,
        ),
    ]
    address_txs = AddressTxs(next_page=None, address_txs=txs)
    result = get_json(
        client,
        path_with_pagesize,
        currency="btc",
        address=address2.address,
        pagesize=2,
        page="",
    )
    assert address_txs.to_dict()["address_txs"][0:2] == result["address_txs"]
    assert result.get("next_page", None) is not None

    result = get_json(
        client,
        path_with_pagesize,
        currency="btc",
        address=address2.address,
        pagesize=2,
        page=result["next_page"],
    )

    assert address_txs.to_dict()["address_txs"][2:3] == result["address_txs"]
    assert result.get("next_page", None) is None

    path_with_order = path + "?order={order}"
    _reversed = list(reversed(address_txs.to_dict()["address_txs"]))
    result = get_json(
        client, path_with_order, currency="btc", address=address2.address, order="asc"
    )
    assert _reversed == result["address_txs"]

    path_with_order_and_page = path_with_order + "&pagesize={pagesize}&page={page}"
    result = get_json(
        client,
        path_with_order_and_page,
        currency="btc",
        address=address2.address,
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
        address=address2.address,
        order="asc",
        pagesize=2,
        page=result["next_page"],
    )

    assert _reversed[2:3] == result["address_txs"]
    assert result.get("next_page", None) is None

    path_with_direction = path + "?direction={direction}"
    result = get_json(
        client,
        path_with_direction,
        currency="btc",
        address=address2.address,
        direction="out",
    )
    address_txs.address_txs = txs[1:3]
    assert address_txs.to_dict()["address_txs"] == result["address_txs"]
    path_with_range = (
        path_with_direction + "&min_height={min_height}&max_height={max_height}"
    )
    result = get_json(
        client,
        path_with_range,
        currency="btc",
        address=address2.address,
        direction="out",
        min_height=1,
        max_height=1,
    )
    address_txs.address_txs = txs[2:3]
    assert address_txs.to_dict()["address_txs"] == result["address_txs"]

    result = get_json(
        client,
        path_with_range,
        currency="btc",
        address=address2.address,
        direction="out",
        min_height=2,
        max_height=2,
    )
    address_txs.address_txs = txs[1:2]
    assert address_txs.to_dict()["address_txs"] == result["address_txs"]

    result = get_json(
        client,
        path_with_direction,
        currency="btc",
        address=address2.address,
        direction="in",
    )
    address_txs.address_txs = txs[0:1]
    assert address_txs.to_dict()["address_txs"] == result["address_txs"]

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
        client, path, currency="eth", address=eth_address.address
    )

    assert txs_eth.to_dict()["address_txs"] == result["address_txs"]
    result = get_json(
        client,
        path_with_direction,
        currency="eth",
        address=eth_address.address,
        direction="out",
    )
    assert txs_eth.to_dict()["address_txs"][1:3] == result["address_txs"]

    path_with_range_and_tc = path_with_range + "&token_currency={token_currency}"
    result = get_json(
        client,
        path_with_range_and_tc,
        currency="eth",
        address=eth_address.address,
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
        address=eth_address.address,
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
        address=eth_address.address,
        direction="",
        min_height="",
        max_height=3,
        token_currency="",
    )
    assert txs_eth.to_dict()["address_txs"][1:4] == result["address_txs"]

    result = get_json(
        client, path, currency="eth", address=eth_address2.address
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

    path_tc = path + "?token_currency={token_currency}"
    result = get_json(
        client, path_tc, currency="eth", address=eth_address2.address, token_currency="weth"
    )

    assert len(result["address_txs"]) == 1
    assert [x["currency"] for x in result["address_txs"]] == ["weth"]

    result = get_json(
        client, path_tc, currency="eth", address=eth_address2.address, token_currency="eth"
    )

    assert len(result["address_txs"]) == 3
    assert [x["currency"] for x in result["address_txs"]] == ["eth", "eth", "eth"]
    assert [x["timestamp"] for x in result["address_txs"]] == [17, 16, 15]

    result = get_json(
        client,
        path_with_range_and_tc,
        currency="eth",
        address=eth_address2.address,
        direction="",
        min_height=2,
        max_height=2,
        token_currency="",
    )

    assert len(result["address_txs"]) == 3
    assert [x["currency"] for x in result["address_txs"]] == ["eth", "weth", "usdt"]
    assert [x["height"] for x in result["address_txs"]] == [2, 2, 2]

    result = get_json(
        client,
        path_with_range_and_tc,
        currency="eth",
        address=eth_address2.address,
        direction="",
        min_height=2,
        max_height=2,
        token_currency="weth",
    )

    assert len(result["address_txs"]) == 1
    assert [x["currency"] for x in result["address_txs"]] == ["weth"]
    assert [x["height"] for x in result["address_txs"]] == [2]

    path_with_pagesize2 = path + "?pagesize={pagesize}"
    result1 = get_json(
        client, path_with_pagesize2, currency="btc", address="addressE", pagesize=1
    )

    assert len(result1["address_txs"]) == 1
    assert result1["next_page"] == "13168304:1"

    path_with_pagesize3 = path + "?pagesize={pagesize}&page={page}"
    result2 = get_json(
        client,
        path_with_pagesize3,
        currency="btc",
        address="addressE",
        pagesize=1,
        page=result1["next_page"],
    )

    assert result1 != result2
    assert result2["next_page"] == "13168303:1"

    result3 = get_json(
        client,
        path_with_pagesize3,
        currency="btc",
        address="addressE",
        pagesize=1,
        page=result2["next_page"],
    )

    assert result2 != result3
    assert result3["next_page"] == "13168302:1"

    result4 = get_json(
        client,
        path_with_pagesize3,
        currency="btc",
        address="addressE",
        pagesize=1,
        page=result3["next_page"],
    )

    assert result3 != result4
    assert result4.get("next_page", None) is None

    addr_txs_total = (
        result1["address_txs"]
        + result2["address_txs"]
        + result3["address_txs"]
        + result4["address_txs"]
    )

    result_total = get_json(
        client, path_with_pagesize2, currency="btc", address="addressE", pagesize=4
    )

    assert result_total["address_txs"] == addr_txs_total

    path_with_pagesize_direction = path + "?pagesize={pagesize}&direction={direction}"
    result = get_json(
        client,
        path_with_pagesize_direction,
        currency="btc",
        address="addressE",
        direction="",
        pagesize=1,
    )

    assert len(result["address_txs"]) == 1
    assert result.get("next_page", None) is not None

    result1 = get_json(
        client,
        path_with_pagesize_direction,
        currency="btc",
        address="addressE",
        direction="in",
        pagesize=1,
    )

    assert len(result1["address_txs"]) == 1
    assert result1.get("next_page", None) is not None

    path_with_pagesize_and_page = path + (
        "?pagesize={pagesize}&page={page}&direction={direction}"
    )
    result2 = get_json(
        client,
        path_with_pagesize_and_page,
        currency="btc",
        address="addressE",
        direction="in",
        pagesize=1,
        page=result1["next_page"],
    )

    assert len(result2["address_txs"]) == 1
    assert result2.get("next_page", None) is not None

    result3 = get_json(
        client,
        path_with_pagesize_and_page,
        currency="btc",
        address="addressE",
        direction="in",
        pagesize=1,
        page=result2["next_page"],
    )

    assert len(result3["address_txs"]) == 0
    assert result3.get("next_page", None) is None

    addr_txs_total = (
        result1["address_txs"] + result2["address_txs"] + result3["address_txs"]
    )

    result4 = get_json(
        client,
        path_with_pagesize_direction,
        currency="btc",
        address="addressE",
        direction="in",
        pagesize=3,
    )

    assert addr_txs_total == result4["address_txs"]


def test_list_tags_by_address(client):
    path = "/{currency}/addresses/{address}/tags"
    result = get_json(
        client, path, currency="btc", address=addressWithTags.address
    )
    tags = [tag.to_dict() for tag in addressWithTags.tags]
    assert tags == result["address_tags"]

    result = get_json(
        client, path, auth="unauthorized", currency="btc", address=addressWithTags.address
    )
    tags_public = [tag for tag in tags if tag["tagpack_is_public"]]
    assert tags_public == result["address_tags"]

    result = get_json(
        client, path, currency="eth", address=eth_addressWithTags.address
    )

    expected = [
        omit(tag.to_dict(), {"inherited_from"}) for tag in eth_addressWithTags.tags
    ]

    assert expected == result["address_tags"]

    # Casing of the address does not matter for ethereum
    result = get_json(
        client, path, currency="eth", address=eth_addressWithTags.address.upper()
    )
    assert expected == result["address_tags"]

    # Adding trailing whitespace is handled gracefully
    result = get_json(
        client, path, currency="eth", address=eth_addressWithTags.address.upper() + "   "
    )
    assert expected == result["address_tags"]

    result = get_json(
        client, path, currency="abcd", address=eth_addressWithTags.address
    )
    assert len(result["address_tags"]) == 2

    # Test that page parameter as string is handled correctly (regression test)
    path_with_page = path + "?pagesize={pagesize}&page={page}"
    result = get_json(
        client,
        path_with_page,
        currency="btc",
        address=addressWithTags.address,
        pagesize=100,
        page=1,
    )
    assert "address_tags" in result


def test_list_address_neighbors(client):
    path = (
        "/{currency}/addresses/{address}/neighbors"
        "?include_labels={include_labels}&direction={direction}"
    )

    result = get_json(
        client,
        path,
        currency="btc",
        address=address.address,
        include_labels=True,
        direction="out",
    )
    awton = addressWithTagsOutNeighbors.to_dict()
    assert awton == result

    result = get_json(
        client,
        path + "&only_ids={only_ids}",
        currency="btc",
        address=address.address,
        include_labels=True,
        only_ids=addressF.address,
        direction="out",
    )
    awton2 = addressWithTagsOutNeighbors.to_dict()
    awton2["neighbors"] = awton2["neighbors"][1:2]
    assert awton2 == result

    result = get_json(
        client,
        path,
        currency="btc",
        auth="unauthorized",
        address=address.address,
        include_labels=True,
        direction="out",
    )
    awton["neighbors"][0]["labels"] = ["labelX"]
    assert awton == result

    result = get_json(
        client,
        path,
        currency="btc",
        address=address.address,
        include_labels=True,
        direction="in",
    )
    assert addressWithTagsInNeighbors.to_dict() == result

    result = get_json(
        client,
        path,
        currency="eth",
        address=eth_address.address,
        include_labels=True,
        direction="out",
    )
    assert ["0xabcdef", "0x123456"] == [
        n.address.address for n in eth_addressWithTagsOutNeighbors.neighbors
    ]

    result = get_json(
        client,
        path,
        currency="eth",
        address=eth_address2.address,
        include_labels=True,
        direction="out",
    )
    assert eth_address2WithTokenFlows.to_dict() == result

    # correct handling pages in wrong format.
    status, body = raw_request(
        client,
        (
            "/{currency}/addresses/{address}/neighbors?"
            "include_labels={include_labels}&direction={direction}&page={page}"
        ),
        currency="eth",
        include_labels=False,
        direction="in",
        page="PAGE2",
        address="0x123456",
    )
    assert status == 400
    assert "is not formatted correctly" in body


def test_get_address_entity(client):
    path = "/{currency}/addresses/{address}/entity"
    result = get_json(
        client, path, currency="btc", address=address.address, include_tags=True
    )
    assert entityWithTags.to_dict() == result

    result = get_json(
        client, path, currency="eth", address=eth_address.address, include_tags=True
    )

    ewt = eth_entityWithTags.to_dict()
    ewt["best_address_tag"].pop("inherited_from")
    assert ewt == result

    non_existent_address = "0x40a197b01CDeF4C77196045EaFFaC80F25Be00FE"
    status, body = raw_request(
        client, path, currency="eth", address=non_existent_address, include_tags=True
    )
    assert status == 404
    assert ("Address 0x40a197b01cdef4c77196045eaffac80f25be00fe not found") in body


# async def test_list_address_links(client):
#     path = "/{currency}/addresses/{address}/links?neighbor={neighbor}"
#     result = await get_json(
#         client, path, currency="btc", address=address.address, neighbor="addressE"
#     )
#
#     link = Links(
#         links=[
#             LinkUtxo(
#                 tx_hash="123456",
#                 currency="btc",
#                 input_value=make_values(eur=-0.1, usd=-0.2, value=-10000000),
#                 output_value=make_values(eur=0.1, usd=0.2, value=10000000),
#                 timestamp=1361497172,
#                 height=2,
#             )
#         ]
#     )
#
#     assert_equal_sorted(link.to_dict(), result, "links", "tx_hash")
#
#     txs_links = Links(links=[tx2_eth, tx22_eth])
#     result = await get_json(
#         client, path, currency="eth", address=eth_address.address, neighbor="0x123456"
#     )
#
#     first = result["links"][0]
#     second = result["links"][1]
#
#     assert_equal_sorted(txs_links.to_dict(), result, "links", "tx_hash")
#
#     path_paged = path + "&pagesize={pagesize}"
#     result = await get_json(
#         client,
#         path_paged,
#         currency="eth",
#         address=eth_address.address,
#         neighbor="0x123456",
#         pagesize=1,
#     )
#     txs_first = Links(links=[tx2_eth if first["tx_hash"] == tx2_eth.tx_hash else tx22_eth])
#     assert [li.to_dict() for li in txs_first.links] == result["links"]
#     assert result.get("next_page", None) is not None
#
#     path_paged2 = path + "&pagesize={pagesize}&page={page}"
#     result = await get_json(
#         client,
#         path_paged2,
#         currency="eth",
#         address=eth_address.address,
#         neighbor="0x123456",
#         page=result["next_page"],
#         pagesize=1,
#     )
#     txs_second = Links(links=[tx22_eth if second["tx_hash"] == tx22_eth.tx_hash else tx2_eth])
#     assert [li.to_dict() for li in txs_second.links] == result["links"]
#     assert result.get("next_page", None) is not None
#
#     result = await get_json(
#         client,
#         path_paged2,
#         currency="eth",
#         address=eth_address.address,
#         neighbor="0x123456",
#         page=result["next_page"],
#         pagesize=1,
#     )
#     assert Links(links=[]).to_dict() == result
#
#     path_order = "/{currency}/addresses/{address}/links?neighbor={neighbor}&order={order}"
#     result = await get_json(
#         client,
#         path_order,
#         currency="eth",
#         address=eth_address.address,
#         neighbor="0x123456",
#         order="desc",
#     )
#
#     assert ["af6e0004", "af6e0003"] == [x["tx_hash"] for x in result["links"]]
#     assert result.get("next_page", None) is None
#
#     result = await get_json(
#         client,
#         path_order,
#         currency="eth",
#         address=eth_address.address,
#         neighbor="0x123456",
#         order="asc",
#     )
#
#     assert list(reversed(["af6e0004", "af6e0003"])) == [
#         x["tx_hash"] for x in result["links"]
#     ]
#     assert result.get("next_page", None) is None
#
#     for mh, ex, exv in [
#         (2, 2, ["af6e0004", "af6e0003"]),
#         (3, 1, ["af6e0004"]),
#         (4, 0, []),
#     ]:
#         result = await get_json(
#             client,
#             path_order + "&min_height={min_height}",
#             currency="eth",
#             address=eth_address.address,
#             neighbor="0x123456",
#             min_height=mh,
#             order="desc",
#         )
#
#         assert ex == len(result["links"])
#         assert exv == [x["tx_hash"] for x in result["links"]]
#
#     rel = itertools.permutations(["A", "B", "C", "D", "E"], r=2)
#     er = {("A", "E"): 1}
#     queries = [(x, y, er.get((x, y), 0)) for x, y in rel]
#
#     for o in ["desc", "asc"]:
#         for a, b, n in queries:
#             result = await get_json(
#                 client,
#                 path_order,
#                 currency="btc",
#                 address=f"address{a}",
#                 neighbor=f"address{b}",
#                 order=o,
#             )
#             assert n == len(result["links"])
#
#     for o in ["desc", "asc"]:
#         for a, b, n in queries:
#             result = await get_json(
#                 client,
#                 path_order + "&min_height=2",
#                 currency="btc",
#                 address=f"address{a}",
#                 neighbor=f"address{b}",
#                 order=o,
#             )
#             assert n == len(result["links"])
#
#     for o in ["desc", "asc"]:
#         for a, b, n in queries:
#             result = await get_json(
#                 client,
#                 path_order + "&max_height=3",
#                 currency="btc",
#                 address=f"address{a}",
#                 neighbor=f"address{b}",
#                 order=o,
#             )
#             assert n == len(result["links"])
#
#     er = {}
#     queries = [(x, y, er.get((x, y), 0)) for x, y in rel]
#     for o in ["desc", "asc"]:
#         for a, b, n in queries:
#             result = await get_json(
#                 client,
#                 path_order + "&min_height=3",
#                 currency="btc",
#                 address=f"address{a}",
#                 neighbor=f"address{b}",
#                 order=o,
#             )
#             assert n == len(result["links"])
#
#     for o in ["desc", "asc"]:
#         for a, b, n in queries:
#             result = await get_json(
#                 client,
#                 path_order + "&max_height=2",
#                 currency="btc",
#                 address=f"address{a}",
#                 neighbor=f"address{b}",
#                 order=o,
#             )
#             assert n == len(result["links"])
