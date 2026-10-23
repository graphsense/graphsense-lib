from graphsenselib.web.models import (
    LabeledItemRef,
    SearchResultByCurrency,
)
from tests.web.helpers import get_json
from tests.web.testdata.general import base_search_results, stats


def test_get_statistics(client):
    result = get_json(client, "/stats")
    result["currencies"] = sorted(result["currencies"], key=lambda c: c["name"])
    cs = [c.to_dict() for c in stats.currencies]
    assert cs == result["currencies"]

    result = get_json(client, "/stats", auth="unauthenticated")
    result["currencies"] = sorted(result["currencies"], key=lambda c: c["name"])
    assert cs == result["currencies"]


def test_get_capabilities(client):
    result = get_json(client, "/capabilities")
    # local serving is always full core: every supported network, nothing
    # disabled (external-backend networks are merged in by the middleware)
    entries = sorted(result["networks"], key=lambda entry: entry["network"])
    assert entries == [
        {"network": currency.name, "disabled": []}
        for currency in sorted(stats.currencies, key=lambda c: c.name)
    ]
    # no optional service is enabled in the test deployment
    assert result["services"] == []


def test_search(client):
    expected = base_search_results()
    expected.currencies[0] = SearchResultByCurrency(
        currency="btc", addresses=["xyz1234", "xyz1278"], txs=[]
    )

    path = "/search?q={q}"
    result = get_json(client, path, q="xyz12")
    assert expected.to_dict() == result

    expected.currencies[0] = SearchResultByCurrency(
        currency="btc", addresses=["xyz1278"], txs=[]
    )

    result = get_json(client, path, q="xyz127")
    assert expected.to_dict() == result

    expected.currencies[0] = SearchResultByCurrency(
        currency="btc",
        txs=["ab1880".rjust(64, "0"), "ab188013".rjust(64, "0")],
        addresses=[],
    )

    result = get_json(client, path, q="ab188")
    assert expected.to_dict() == result

    expected.currencies[0] = SearchResultByCurrency(
        currency="btc", txs=["ab188013".rjust(64, "0")], addresses=[]
    )

    result = get_json(client, path, q="ab18801")
    assert expected.to_dict() == result

    expected.currencies[0] = SearchResultByCurrency(
        currency="btc", txs=["00ab188013".rjust(64, "0")], addresses=[]
    )

    result = get_json(client, path, q="00ab1")
    assert expected.to_dict() == result

    expected = base_search_results()
    expected.labels = sorted(["Internet Archive 2", "Internet, Archive"])

    result = get_json(client, path, q="internet")
    result["labels"] = sorted(result["labels"])
    assert expected.to_dict() == result

    result = get_json(client, path, auth="y", q="internet")
    expected.labels = ["Internet, Archive"]
    assert expected.to_dict() == result

    expected = base_search_results()
    expected.actors = [
        LabeledItemRef(id="actorX", label="Actor X"),
        LabeledItemRef(id="actorY", label="Actor Y"),
        LabeledItemRef(id="anotherActor", label="Another Actor Y"),
    ]

    result = get_json(client, path, q="actor")
    result["labels"] = sorted(result["labels"])
    assert expected.to_dict() == result

    result = get_json(client, path, auth="y", q="actor")
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

    result = get_json(client, path, q="af6e")
    expected.to_dict() == result

    expected = base_search_results()
    expected.currencies[2] = SearchResultByCurrency(
        currency="eth", txs=[], addresses=["0xabcdef"]
    )

    result = get_json(client, path, q="0xabcde")
    assert expected.to_dict() == result


def test_search_uppercase_bech32(client):
    # BIP173 allows a bech32 address in all uppercase too (QR codes use it);
    # search must find the lowercase form the database stores
    expected = base_search_results()
    expected.currencies[0] = SearchResultByCurrency(
        currency="btc", addresses=["bc1xyz123456789"], txs=[]
    )

    path = "/search?q={q}"
    for q in ["bc1xyz12", "BC1XYZ12", "BC1XYZ123456789"]:
        assert get_json(client, path, q=q) == expected.to_dict()


def test_search_uppercase_ltc1_also_finds_base58(client):
    # "LTC1" also starts base58 LTC addresses by chance (always "L", then
    # random characters), so an all-uppercase query is searched as typed and
    # in lowercase
    path = "/search?q={q}"
    for q, addresses in [
        ("LTC1QXYZ9", ["LTC1QXYZ9abc", "ltc1qxyz9def"]),
        ("ltc1qxyz9", ["ltc1qxyz9def"]),
        ("LTC1QXYZ9a", ["LTC1QXYZ9abc"]),  # mixed case: only base58
    ]:
        expected = base_search_results()
        expected.currencies[1] = SearchResultByCurrency(
            currency="ltc", addresses=addresses, txs=[]
        )
        assert get_json(client, path, q=q) == expected.to_dict(), q


def test_get_capabilities_lists_enabled_services(client):
    import httpx

    from graphsenselib.web.config import NotificationsConfig
    from graphsenselib.web.routes.notifications import NotificationsClient

    def service(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/meta"
        return httpx.Response(
            200,
            json={
                "version": "26.10.0rc1",
                "api": 1,
                "limits": {"instant_mail_interval_minutes": 5},
            },
        )

    url = "http://gs-notifications:8000"
    client.app.state.notifications_client = NotificationsClient(
        NotificationsConfig(enabled=True, url=url),
        httpx.AsyncClient(base_url=url, transport=httpx.MockTransport(service)),
    )
    try:
        result = get_json(client, "/capabilities")
    finally:
        del client.app.state.notifications_client
    assert result["services"] == [
        {
            "name": "notifications",
            "available": True,
            "version": "26.10.0rc1",
            "api": 1,
            "beta": True,
            "limits": {"instant_mail_interval_minutes": 5},
        }
    ]
