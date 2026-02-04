from graphsenselib.web.models import (
    CurrencyStats,
    LabeledItemRef,
    SearchResult,
    SearchResultByCurrency,
    Stats,
)

stats = Stats(
    currencies=[
        CurrencyStats(
            name="btc",
            no_entities=7890,
            no_addresses=4560,
            no_blocks=3,
            timestamp=420,
            no_txs=110,
            no_labels=13,
            no_tagged_addresses=79,
            no_address_relations=1230,
        ),
        CurrencyStats(
            name="eth",
            no_entities=0,
            no_addresses=1,
            no_blocks=2300002,
            timestamp=16,
            no_txs=10,
            no_labels=4,
            no_tagged_addresses=90,
            no_address_relations=2,
        ),
        CurrencyStats(
            name="ltc",
            no_entities=789,
            no_addresses=456,
            no_blocks=3,
            timestamp=42,
            no_txs=11,
            no_labels=2,
            no_tagged_addresses=20,
            no_address_relations=123,
        ),
        CurrencyStats(
            name="trx",
            no_entities=0,
            no_addresses=1,
            no_blocks=3,
            timestamp=16,
            no_txs=10,
            no_labels=0,
            no_tagged_addresses=0,
            no_address_relations=2,
        ),
    ]
)


async def get_statistics(test_case):
    result = await test_case.request("/stats")
    result["currencies"] = sorted(result["currencies"], key=lambda c: c["name"])
    cs = [c.to_dict() for c in stats.currencies]
    assert cs == result["currencies"]

    result = await test_case.request("/stats", auth="unauthenticated")
    result["currencies"] = sorted(result["currencies"], key=lambda c: c["name"])

    assert cs == result["currencies"]


async def search(test_case):
    def base_search_results():
        return SearchResult(
            currencies=[
                SearchResultByCurrency(currency="btc", addresses=[], txs=[]),
                SearchResultByCurrency(currency="ltc", addresses=[], txs=[]),
                SearchResultByCurrency(currency="eth", addresses=[], txs=[]),
                SearchResultByCurrency(currency="trx", addresses=[], txs=[]),
            ],
            labels=[],
            actors=[],
        )

    expected = base_search_results()
    expected.currencies[0] = SearchResultByCurrency(
        currency="btc", addresses=["xyz1234", "xyz1278"], txs=[]
    )

    path = "/search?q={q}"
    result = await test_case.request(path, q="xyz12")
    test_case.assertEqual(expected.to_dict(), result)

    expected.currencies[0] = SearchResultByCurrency(
        currency="btc", addresses=["xyz1278"], txs=[]
    )

    result = await test_case.request(path, q="xyz127")
    test_case.assertEqual(expected.to_dict(), result)

    expected.currencies[0] = SearchResultByCurrency(
        currency="btc",
        txs=["ab1880".rjust(64, "0"), "ab188013".rjust(64, "0")],
        addresses=[],
    )

    result = await test_case.request(path, q="ab188")
    test_case.assertEqual(expected.to_dict(), result)

    expected.currencies[0] = SearchResultByCurrency(
        currency="btc", txs=["ab188013".rjust(64, "0")], addresses=[]
    )

    result = await test_case.request(path, q="ab18801")
    test_case.assertEqual(expected.to_dict(), result)

    expected.currencies[0] = SearchResultByCurrency(
        currency="btc", txs=["00ab188013".rjust(64, "0")], addresses=[]
    )

    result = await test_case.request(path, q="00ab1")
    test_case.assertEqual(expected.to_dict(), result)

    expected = base_search_results()
    expected.labels = sorted(["Internet Archive 2", "Internet, Archive"])

    result = await test_case.request(path, q="internet")
    result["labels"] = sorted(result["labels"])
    assert expected.to_dict() == result

    result = await test_case.request(path, auth="y", q="internet")
    expected.labels = ["Internet, Archive"]
    assert expected.to_dict() == result

    expected = base_search_results()
    expected.actors = [
        LabeledItemRef(id="actorX", label="Actor X"),
        LabeledItemRef(id="actorY", label="Actor Y"),
        LabeledItemRef(id="anotherActor", label="Another Actor Y"),
    ]

    result = await test_case.request(path, q="actor")
    result["labels"] = sorted(result["labels"])
    assert expected.to_dict() == result

    result = await test_case.request(path, auth="y", q="actor")
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

    result = await test_case.request(path, q="af6e")
    expected.to_dict() == result

    expected = base_search_results()
    expected.currencies[2] = SearchResultByCurrency(
        currency="eth", txs=[], addresses=["0xabcdef"]
    )

    result = await test_case.request(path, q="0xabcde")
    assert expected.to_dict() == result
