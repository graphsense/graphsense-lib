from graphsenselib.web.models import (
    LabeledItemRef,
    SearchResultByCurrency,
)
from tests.web.helpers import get_json
from tests.web.testdata.general import base_search_results, stats


async def test_get_statistics(client):
    result = await get_json(client, "/stats")
    result["currencies"] = sorted(result["currencies"], key=lambda c: c["name"])
    cs = [c.to_dict() for c in stats.currencies]
    assert cs == result["currencies"]

    result = await get_json(client, "/stats", auth="unauthenticated")
    result["currencies"] = sorted(result["currencies"], key=lambda c: c["name"])
    assert cs == result["currencies"]


async def test_search(client):
    expected = base_search_results()
    expected.currencies[0] = SearchResultByCurrency(
        currency="btc", addresses=["xyz1234", "xyz1278"], txs=[]
    )

    path = "/search?q={q}"
    result = await get_json(client, path, q="xyz12")
    assert expected.to_dict() == result

    expected.currencies[0] = SearchResultByCurrency(
        currency="btc", addresses=["xyz1278"], txs=[]
    )

    result = await get_json(client, path, q="xyz127")
    assert expected.to_dict() == result

    expected.currencies[0] = SearchResultByCurrency(
        currency="btc",
        txs=["ab1880".rjust(64, "0"), "ab188013".rjust(64, "0")],
        addresses=[],
    )

    result = await get_json(client, path, q="ab188")
    assert expected.to_dict() == result

    expected.currencies[0] = SearchResultByCurrency(
        currency="btc", txs=["ab188013".rjust(64, "0")], addresses=[]
    )

    result = await get_json(client, path, q="ab18801")
    assert expected.to_dict() == result

    expected.currencies[0] = SearchResultByCurrency(
        currency="btc", txs=["00ab188013".rjust(64, "0")], addresses=[]
    )

    result = await get_json(client, path, q="00ab1")
    assert expected.to_dict() == result

    expected = base_search_results()
    expected.labels = sorted(["Internet Archive 2", "Internet, Archive"])

    result = await get_json(client, path, q="internet")
    result["labels"] = sorted(result["labels"])
    assert expected.to_dict() == result

    result = await get_json(client, path, auth="y", q="internet")
    expected.labels = ["Internet, Archive"]
    assert expected.to_dict() == result

    expected = base_search_results()
    expected.actors = [
        LabeledItemRef(id="actorX", label="Actor X"),
        LabeledItemRef(id="actorY", label="Actor Y"),
        LabeledItemRef(id="anotherActor", label="Another Actor Y"),
    ]

    result = await get_json(client, path, q="actor")
    result["labels"] = sorted(result["labels"])
    assert expected.to_dict() == result

    result = await get_json(client, path, auth="y", q="actor")
    expected.actors = [
        LabeledItemRef(id="actorX", label="Actor X"),
        LabeledItemRef(id="actorY", label="Actor Y"),
        LabeledItemRef(id="anotherActor", label="Another Actor Y"),
    ]
    assert expected.to_dict() == result

    expected = base_search_results()
    expected.currencies[2] = SearchResultByCurrency(
        currency="eth",
        txs=["af6e0000".rjust(64, "0"), "af6e0003".rjust(64, "0")],
        addresses=[],
    )

    expected.currencies[3] = SearchResultByCurrency(
        currency="trx",
        txs=["af6e0000".rjust(64, "0"), "af6e0003".rjust(64, "0")],
        addresses=[],
    )

    result = await get_json(client, path, q="af6e")
    expected.to_dict() == result

    expected = base_search_results()
    expected.currencies[2] = SearchResultByCurrency(
        currency="eth", txs=[], addresses=["0xabcdef"]
    )

    result = await get_json(client, path, q="0xabcde")
    assert expected.to_dict() == result
