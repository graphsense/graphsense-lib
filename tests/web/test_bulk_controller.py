import asyncio
from types import SimpleNamespace

from starlette.datastructures import Headers

from graphsenselib.web.builtin.plugins.obfuscate_tags.obfuscate_tags import (
    ObfuscateTags,
)
from graphsenselib.web.models import AddressTag, AddressTags
from graphsenselib.web.routes.bulk import wrap
from tests.web.helpers import get_json, request_with_status
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
        "_error,_info,_request_address,actors,address,balance_eur,balance_usd,balance_value,cluster,currency,entity,first_tx_height,first_tx_timestamp,first_tx_tx_hash,fresh_cluster_id,in_degree,is_contract,last_tx_height,last_tx_timestamp,last_tx_tx_hash,no_incoming_txs,no_outgoing_txs,out_degree,status,token_balances,total_received_eur,total_received_usd,total_received_value,total_spent_eur,total_spent_usd,total_spent_value,total_tokens_received,total_tokens_spent\r\n"  # noqa
        "not found,,2,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,\r\n"
        ",,a123456,,a123456,1.15,2.31,115422577,123,btc,123,1,1361497172,04d92601677d62a985310b61a301e74870fa942c8be0648e16b1db23b996a8cd,,5013,,1,1361497172,bd01b57a50bdee0fb34ce77f5c62a664cea5b94b304d438a8225850f05b45ae5,3981,267,284,clean,,2130676.5,2543214.5,40412296129,2118309.0,2541183.0,40296873552,,\r\n"
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
    expected = (
        "_error,_info,_request_height\r\n"
        "not found,,100\r\n"
        "not found,,200\r\n"
    )
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
