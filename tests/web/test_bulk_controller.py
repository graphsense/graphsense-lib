import asyncio
import json
from types import SimpleNamespace

from starlette.datastructures import Headers

from graphsenselib.db.asynchronous.services.models import RatesResponse
from graphsenselib.web.builtin.plugins.obfuscate_tags.obfuscate_tags import (
    ObfuscateTags,
)
from graphsenselib.web.models import (
    AddressTag,
    AddressTags,
    Cluster,
    NeighborCluster,
    NeighborClusters,
    Rate,
    TxSummary,
    Values,
)
from graphsenselib.web.routes.bulk import (
    bounded_as_completed,
    to_csv_generator,
    wrap,
)
from graphsenselib.web.service.rates_service import get_rates
from tests.web.helpers import request_with_status
from tests.web.testdata.blocks import block, block2
from tests.web.testdata.bulk import block_path, error_bodies, headers


def _make_addr_tag(is_public, label):
    return AddressTag(
        label=label,
        category="exchange",
        concepts=[],
        actor="actorX",
        abuse=None,
        tagpack_uri="uriX",
        source="sourceX",
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


def _make_values():
    return Values(fiat_values=[Rate(code="eur", value=0.0)], value=0)


def _make_cluster(tag=None):
    return Cluster(
        currency="btc",
        entity=109578,
        root_address="addr",
        balance=_make_values(),
        first_tx=TxSummary(timestamp=0, height=1, tx_hash="tx"),
        last_tx=TxSummary(timestamp=0, height=1, tx_hash="tx"),
        in_degree=1,
        out_degree=1,
        no_addresses=1,
        no_incoming_txs=1,
        no_outgoing_txs=1,
        total_received=_make_values(),
        total_spent=_make_values(),
        actors=None,
        best_address_tag=tag,
        no_address_tags=1,
    )


def _obfuscating_request():
    """Fake request with the ObfuscateTags plugin registered and no caller
    X-Consumer-Groups header, so the default-obfuscate path applies."""
    module = ObfuscateTags.__module__
    app = SimpleNamespace(
        state=SimpleNamespace(plugins=[ObfuscateTags], plugin_contexts={module: {}})
    )
    # request.state deliberately has no plugin_state attribute (defaults to {})
    return SimpleNamespace(app=app, state=SimpleNamespace(), headers=Headers({}))


def test_bulk_wrap_obfuscates_private_tags():
    """Regression: bulk streaming responses bypass PluginRoute, so wrap() must
    apply the obfuscation hooks itself. Private tag fields must be blanked while
    the row set (and thus counts) stays identical to the un-obfuscated result."""
    public = _make_addr_tag(True, "PublicLabel")
    private = _make_addr_tag(False, "PrivateLabel")

    async def op(ctx, currency, **params):
        return AddressTags(address_tags=[public, private], next_page=None)

    flat = asyncio.run(
        wrap(
            _obfuscating_request(),
            None,
            op,
            "btc",
            {},
            {"address": "addr"},
            1,
            "json",
            asyncio.Semaphore(1),
        )
    )

    # Both tags are still present (count preserved), only content is redacted.
    assert len(flat) == 2
    by_public = {row["tagpack_is_public"]: row for row in flat}

    assert by_public[True]["label"] == "PublicLabel"
    assert by_public[True]["source"] == "sourceX"
    assert by_public[True]["tagpack_uri"] == "uriX"

    assert by_public[False]["label"] == ""
    assert by_public[False]["source"] == ""
    assert by_public[False]["tagpack_uri"] == ""
    assert by_public[False]["actor"] == ""


def test_bulk_wrap_relations_only_neighbors_are_not_obfuscated_as_entities():
    """Regression: with relations_only=true a neighbor's `entity` is the bare
    cluster id, not an expanded cluster. The obfuscation hook assumed the
    expanded shape and raised AttributeError: 'int' object has no attribute
    'best_address_tag' inside wrap(), aborting bulk.json/list_cluster_neighbors
    after the 200 header had been sent (prod, btc/bulk.json/
    list_cluster_neighbors?num_pages=1). Id-only neighbors carry no tags or
    actors, so there is nothing to obfuscate; expanded ones still are."""
    private = _make_addr_tag(False, "PrivateLabel")

    async def op(ctx, currency, **params):
        return NeighborClusters(
            neighbors=[
                NeighborCluster(
                    entity=109578, value=_make_values(), no_txs=1, labels=None
                ),
                NeighborCluster(
                    entity=_make_cluster(tag=private),
                    value=_make_values(),
                    no_txs=2,
                    labels=None,
                ),
            ],
            next_page=None,
        )

    flat = asyncio.run(
        wrap(
            _obfuscating_request(),
            None,
            op,
            "btc",
            {"relations_only": True},
            {"cluster": 2647118},
            1,
            "json",
            asyncio.Semaphore(1),
        )
    )

    assert len(flat) == 2
    id_only, expanded = flat
    # the id-only reference survives untouched under both keys
    assert id_only["entity"] == 109578
    assert id_only["cluster"] == 109578
    # ... and the expanded neighbor in the same page is still obfuscated
    assert expanded["entity"]["best_address_tag"]["label"] == ""
    assert expanded["entity"]["best_address_tag"]["actor"] == ""


def _plain_request():
    """Fake request with no plugins registered."""
    app = SimpleNamespace(state=SimpleNamespace(plugins=[], plugin_contexts={}))
    return SimpleNamespace(app=app, state=SimpleNamespace(), headers=Headers({}))


def test_bulk_get_rates_rows_are_json_serializable():
    """Regression: get_rates wrapped internal pydantic Rate models in a plain
    dict; flatten(format="json") passes dicts through untouched, so the models
    reached json.dumps un-serialized and bulk.json/get_rates 500ed mid-stream."""

    async def fake_get_rates(currency, height=None):
        return RatesResponse(
            height=1,
            rates=[{"code": "eur", "value": 0.5}, {"code": "usd", "value": 1.0}],
        )

    ctx = SimpleNamespace(
        services=SimpleNamespace(
            rates_service=SimpleNamespace(get_rates=fake_get_rates)
        )
    )

    flat = asyncio.run(
        wrap(
            _plain_request(),
            ctx,
            get_rates,
            "btc",
            {},
            {"height": 1},
            1,
            "json",
            asyncio.Semaphore(1),
        )
    )

    assert len(flat) == 1
    row = json.loads(json.dumps(flat[0]))
    assert row["_request_height"] == 1
    assert row["rates"] == [
        {"code": "eur", "value": 0.5},
        {"code": "usd", "value": 1.0},
    ]


def test_bulk_json_get_rates(client):
    body = {"height": [1]}
    result = request_with_status(
        client,
        "/{currency}/bulk.{form}/get_rates?num_pages=1".format(
            currency="btc", form="json"
        ),
        200,
        body,
    )
    assert result == [
        {
            "rates": [
                {"code": "eur", "value": 0.0},
                {"code": "usd", "value": 0.0},
            ],
            "_request_height": 1,
        }
    ]


def test_bulk_csv(client):
    body = {"height": [1, 2]}
    response = client.request(
        "POST",
        block_path.format(form="csv", currency="btc"),
        json=body,
        headers=headers,
    )
    result = response.text

    expected = (
        "_error,_info,_request_height,block_hash,currency,height,no_txs,timestamp\r\n"
        ",,1,00000000839a8e6886ab5951d76f411475428afc90947ee320161bbf18eb6048,btc,1,1,1231469665\r\n"
        ",,2,000000006a625f06636b8bb6ac7b960a8d03705d1ace08b1a19da3fdcc99ddbd,btc,2,1,1231469744\r\n"
    )
    assert sorted(expected.split("\r\n")) == sorted(result.split("\r\n"))

    # get_address
    path = "/{currency}/bulk.{form}/get_address?num_pages=1"
    body = {"address": ["a123456", "2"]}
    response = client.request(
        "POST",
        path.format(form="csv", currency="btc"),
        json=body,
        headers=headers,
    )
    result = response.text
    expected = (
        "_error,_info,_request_address,actors,address,aggregates_truncated,balance_eur,balance_usd,balance_value,cluster,currency,cutoff,entity,first_tx_height,first_tx_timestamp,first_tx_tx_hash,fresh_cluster_id,in_degree,is_contract,last_tx_height,last_tx_timestamp,last_tx_tx_hash,no_incoming_txs,no_outgoing_txs,out_degree,status,token_balances,total_received_eur,total_received_usd,total_received_value,total_spent_eur,total_spent_usd,total_spent_value,total_tokens_received,total_tokens_spent\r\n"  # noqa
        "not found,,2,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,\r\n"
        ",,a123456,,a123456,,1.15,2.31,115422577,123,btc,,123,1,1361497172,04d92601677d62a985310b61a301e74870fa942c8be0648e16b1db23b996a8cd,,5013,,1,1361497172,bd01b57a50bdee0fb34ce77f5c62a664cea5b94b304d438a8225850f05b45ae5,3981,267,284,clean,,2130676.5,2543214.5,40412296129,2118309.0,2541183.0,40296873552,,\r\n"
    )  # noqa
    assert sorted(expected.split("\r\n")) == sorted(result.split("\r\n"))

    # no data
    body = {"height": [100, 200]}
    response = client.request(
        "POST",
        block_path.format(form="csv", currency="btc"),
        json=body,
        headers=headers,
    )
    result = response.text
    expected = "_error,_info,_request_height\r\nnot found,,100\r\nnot found,,200\r\n"
    assert sorted(expected.split("\r\n")) == sorted(result.split("\r\n"))

    # error bodies:
    for body in error_bodies:
        response = client.request(
            "POST",
            block_path.format(form="csv", currency="btc"),
            json=body,
            headers=headers,
        )
        assert 400 == response.status_code, "response is " + response.text


def test_bulk_json(client):
    body = {"height": [1, 2]}
    result = request_with_status(
        client,
        block_path.format(form="json", currency="btc"),
        200,
        body,
        currency="btc",
        form="json",
    )

    def s(b):
        return b["block_hash"]

    result = sorted(result, key=s)
    expected = [block.to_dict(), block2.to_dict()]
    for b in expected:
        b["_request_height"] = b["height"]
    blocks = sorted(expected, key=s)
    assert blocks == result


def test_bulk_rejects_oversized_key_list(client):
    """GHSA-372j-2wgf-23ch: stack() creates one coroutine per list item, so the
    list length must be bounded. Anything above the configured cap is refused
    with a 400 naming the offending key, not silently truncated."""
    limit = client.app_state.config.max_bulk_items
    body = {"height": list(range(limit + 1))}
    response = client.request(
        "POST",
        block_path.format(form="json", currency="btc"),
        json=body,
        headers=headers,
    )
    assert response.status_code == 400, response.text
    detail = response.json()["detail"]
    assert "height" in detail
    assert str(limit) in detail


def test_bulk_accepts_list_at_the_limit(client):
    """The cap is inclusive — a request exactly at the limit still runs."""
    limit = client.app_state.config.max_bulk_items
    body = {"height": [1] * limit}
    response = client.request(
        "POST",
        block_path.format(form="json", currency="btc"),
        json=body,
        headers=headers,
    )
    assert response.status_code == 200, response.text


def test_bounded_as_completed_bounds_in_flight_tasks():
    """The scheduler must create coroutines lazily: at no point may more than
    max_in_flight of them exist, however long the requested run is."""
    total = 500
    max_in_flight = 7
    created = 0
    live = 0
    peak_live = 0

    async def item():
        nonlocal live, peak_live
        live += 1
        peak_live = max(peak_live, live)
        await asyncio.sleep(0)
        live -= 1
        return "row"

    def make_task(i):
        nonlocal created
        created += 1
        return item()

    async def run():
        seen = 0
        peak_created_ahead = 0
        async for task in bounded_as_completed(make_task, total, max_in_flight):
            assert await task == "row"
            seen += 1
            peak_created_ahead = max(peak_created_ahead, created - seen)
        return seen, peak_created_ahead

    seen, peak_created_ahead = asyncio.run(run())
    assert seen == total
    assert created == total
    assert peak_live <= max_in_flight
    assert peak_created_ahead <= max_in_flight


def test_bounded_as_completed_cancels_pending_on_early_exit():
    """A consumer that stops early (client disconnect, writer error) must not
    leave tasks running behind the closed response."""
    started = []
    cancelled = []

    async def item(i):
        started.append(i)
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            cancelled.append(i)
            raise
        return i

    async def run():
        gen = bounded_as_completed(lambda i: item(i), 100, 5)
        async for _ in gen:  # pragma: no cover - never reached, all items hang
            break
        await gen.aclose()
        await asyncio.sleep(0)

    async def driver():
        task = asyncio.create_task(run())
        await asyncio.sleep(0.05)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    asyncio.run(driver())
    assert len(started) == 5, "more tasks were scheduled than the window allows"
    assert sorted(cancelled) == sorted(started)


def test_to_csv_generator_streams_past_the_header_inference_window():
    """The CSV path infers its header from the first rows and then streams the
    rest off the same iterator. Regression guard for the rewrite that dropped
    the 'collect every remaining operation first' buffering step."""
    total = 250

    async def rows_for(i):
        return [{"height": i, "block_hash": f"h{i}"}]

    async def fake_stack():
        for i in range(total):
            yield asyncio.ensure_future(rows_for(i))

    async def run():
        return [chunk async for chunk in to_csv_generator(fake_stack())]

    chunks = asyncio.run(run())
    header, *rows = [c.strip() for c in chunks if c.strip()]
    assert header == "_error,_info,block_hash,height"
    assert len(rows) == total
    assert rows[0] == ",,h0,0"
    assert rows[-1] == f",,h{total - 1},{total - 1}"
