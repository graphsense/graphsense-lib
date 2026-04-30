"""Regression tests for the `graphsense.ext.GraphSense` facade."""

from __future__ import annotations


import graphsense
from graphsense.api_client import ApiClient
from graphsense.ext import GraphSense, Raw


def _clear_key_env(monkeypatch):
    for name in (
        "GS_API_KEY",
        "GRAPHSENSE_API_KEY",
        "IKNAIO_API_KEY",
        "API_KEY",
    ):
        monkeypatch.delenv(name, raising=False)


def _clear_host_env(monkeypatch):
    for name in ("GS_HOST", "GRAPHSENSE_HOST", "IKNAIO_HOST"):
        monkeypatch.delenv(name, raising=False)


def test_api_key_resolution_precedence(monkeypatch):
    # Lowest priority: legacy API_KEY
    _clear_key_env(monkeypatch)
    monkeypatch.setenv("API_KEY", "from-legacy-env")
    assert GraphSense().api_client.configuration.api_key["api_key"] == "from-legacy-env"

    # GS_API_KEY beats API_KEY
    monkeypatch.setenv("GS_API_KEY", "from-gs")
    assert GraphSense().api_client.configuration.api_key["api_key"] == "from-gs"

    # IKNAIO_API_KEY beats GS_API_KEY
    monkeypatch.setenv("IKNAIO_API_KEY", "from-iknaio")
    assert GraphSense().api_client.configuration.api_key["api_key"] == "from-iknaio"

    # GRAPHSENSE_API_KEY is the highest-priority env var
    monkeypatch.setenv("GRAPHSENSE_API_KEY", "from-graphsense")
    assert GraphSense().api_client.configuration.api_key["api_key"] == "from-graphsense"

    # Explicit arg wins over everything
    assert (
        GraphSense(api_key="explicit").api_client.configuration.api_key["api_key"]
        == "explicit"
    )


def test_host_resolution_precedence(monkeypatch):
    _clear_host_env(monkeypatch)
    _clear_key_env(monkeypatch)

    # GS_HOST is the lowest-priority env var
    monkeypatch.setenv("GS_HOST", "http://from-gs")
    assert GraphSense().api_client.configuration.host == "http://from-gs"

    # IKNAIO_HOST beats GS_HOST
    monkeypatch.setenv("IKNAIO_HOST", "http://from-iknaio")
    assert GraphSense().api_client.configuration.host == "http://from-iknaio"

    # GRAPHSENSE_HOST is the highest-priority env var
    monkeypatch.setenv("GRAPHSENSE_HOST", "http://from-graphsense")
    assert GraphSense().api_client.configuration.host == "http://from-graphsense"

    # Explicit arg wins over everything
    assert (
        GraphSense(host="http://explicit").api_client.configuration.host
        == "http://explicit"
    )


def test_raw_exposes_non_deprecated_groups_only(gs: GraphSense):
    assert "addresses" in gs.raw.groups()
    assert "clusters" in gs.raw.groups()
    assert "entities" not in gs.raw.groups()  # deprecated by default


def test_raw_can_show_deprecated_when_opted_in(api_client: ApiClient):
    gs_dep = GraphSense(api_client=api_client, show_deprecated=True)
    assert "entities" in gs_dep.raw.groups()


def test_raw_picks_up_new_api_classes(api_client: ApiClient, monkeypatch):
    """If the generator adds a new `FooApi` class, Raw should pick it up."""

    class FooApi:
        def __init__(self, api_client=None):
            self.api_client = api_client

        def do_thing(self):
            return "ok"

    monkeypatch.setattr(graphsense, "FooApi", FooApi, raising=False)
    raw = Raw(api_client)
    assert "foo" in raw.groups()
    assert raw.foo.do_thing() == "ok"


def test_lookup_address_bundles_in_parallel(
    gs: GraphSense,
    http_mock,
    sample_address,
    sample_tags,
    sample_tag_summary,
    sample_cluster,
):
    # Register most-specific matchers first.
    http_mock.add("GET", r"/tag_summary(\?|$)", json_body=sample_tag_summary)
    http_mock.add("GET", r"/tags(\?|$)", json_body=sample_tags)
    http_mock.add("GET", r"/btc/clusters/123(\?|$)", json_body=sample_cluster)
    http_mock.add("GET", r"/btc/addresses/1A1z(\?|$)", json_body=sample_address)

    bundle = gs.lookup_address(
        "1A1z",
        with_tags=True,
        with_tag_summary=True,
        with_cluster=True,
    )

    assert bundle.data.address == "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa"
    assert bundle.tags is not None
    assert bundle.cluster is not None
    assert bundle.tag_summary is not None
    # 1 base + 3 auxiliary = 4 total HTTP calls
    assert len(http_mock.calls) == 4


def test_lookup_address_without_bundling_does_only_one_call(
    gs: GraphSense, http_mock, sample_address
):
    http_mock.add("GET", "/btc/addresses/1A1z", json_body=sample_address)
    bundle = gs.lookup_address("1A1z")
    assert bundle.tags is None and bundle.cluster is None
    assert len(http_mock.calls) == 1


def test_lookup_tx_with_io_fetches_inputs_and_outputs(
    gs: GraphSense, http_mock, sample_tx_utxo
):
    """`with_io=True` fans out to both /inputs and /outputs for UTXO txs."""
    http_mock.add("GET", r"/btc/txs/0xabc/inputs(\?|$)", json_body=[])
    http_mock.add("GET", r"/btc/txs/0xabc/outputs(\?|$)", json_body=[])
    http_mock.add("GET", r"/btc/txs/0xabc(\?|$)", json_body=sample_tx_utxo)

    bundle = gs.lookup_tx("0xabc", with_io=True)
    assert bundle.io == {"inputs": [], "outputs": []}
    urls = [c.url for c in http_mock.calls]
    assert any("/inputs" in u for u in urls)
    assert any("/outputs" in u for u in urls)


def test_lookup_tx_with_io_skipped_for_account_chain(
    gs: GraphSense, http_mock, sample_tx_account
):
    """Account-model chains (eth, ...) don't implement /io; we must skip it."""
    http_mock.add("GET", r"/btc/txs/0xabc(\?|$)", json_body=sample_tx_account)
    bundle = gs.lookup_tx("0xabc", with_io=True)
    assert bundle.io is None
    # No /inputs or /outputs requests went out.
    urls = [c.url for c in http_mock.calls]
    assert not any(u.endswith("/inputs") or "/inputs?" in u for u in urls)
    assert not any(u.endswith("/outputs") or "/outputs?" in u for u in urls)


def test_lookup_tx_with_flows_skipped_for_utxo_chain(
    gs: GraphSense, http_mock, sample_tx_utxo
):
    """`/flows` is account-only; UTXO chains must not hit it."""
    http_mock.add("GET", r"/btc/txs/0xabc(\?|$)", json_body=sample_tx_utxo)
    bundle = gs.lookup_tx("0xabc", with_flows=True)
    assert bundle.flows is None
    assert not any("/flows" in c.url for c in http_mock.calls)


def test_lookup_tx_with_flows_used_for_account_chain(
    gs: GraphSense, http_mock, sample_tx_account
):
    http_mock.add("GET", r"/btc/txs/0xabc/flows(\?|$)", json_body={"flows": []})
    http_mock.add("GET", r"/btc/txs/0xabc(\?|$)", json_body=sample_tx_account)
    bundle = gs.lookup_tx("0xabc", with_flows=True)
    assert bundle.flows is not None
    assert any("/flows" in c.url for c in http_mock.calls)


def test_lookup_tx_with_heuristics_passes_include_all(
    gs: GraphSense, http_mock, sample_tx_utxo
):
    """`with_heuristics=True` must hit get_tx with `include_heuristics=all`."""
    http_mock.add("GET", r"/btc/txs/0xabc(\?|$)", json_body=sample_tx_utxo)
    gs.lookup_tx("0xabc", with_heuristics=True)
    base_calls = [c for c in http_mock.calls if c.url.split("?")[0].endswith("/0xabc")]
    assert base_calls, "base get_tx call missing"
    assert "include_heuristics=all" in base_calls[0].url


def test_search_passthrough(gs: GraphSense, http_mock):
    http_mock.add(
        "GET",
        "/search",
        json_body={"currencies": [], "labels": [], "actors": []},
    )
    result = gs.search("satoshi")
    assert result is not None
    assert any("search" in c.url for c in http_mock.calls)


def test_bundle_to_dict_merges_model_and_extras(
    gs: GraphSense, http_mock, sample_address, sample_tags
):
    http_mock.add("GET", r"/tags(\?|$)", json_body=sample_tags)
    http_mock.add("GET", r"/btc/addresses/1A1z(\?|$)", json_body=sample_address)
    b = gs.lookup_address("1A1z", with_tags=True)
    out = b.to_dict()
    assert out["address"] == sample_address["address"]
    assert "tags" in out
