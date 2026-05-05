"""Bulk-vs-per-item dispatch heuristic and GraphSense.bulk()."""

from __future__ import annotations

from graphsense.ext import bulk as bulk_mod
from graphsense.ext import GraphSense


def test_should_bulk_default_threshold():
    assert not bulk_mod.should_bulk(5)
    assert bulk_mod.should_bulk(10)
    assert bulk_mod.should_bulk(100)


def test_should_bulk_override_true():
    assert bulk_mod.should_bulk(1, override=True)


def test_should_bulk_override_false():
    assert not bulk_mod.should_bulk(1000, override=False)


def test_run_parallel_preserves_order():
    def f(x):
        return x * 2

    assert bulk_mod.run_parallel(f, [1, 2, 3, 4]) == [2, 4, 6, 8]


def test_gs_bulk_posts_to_json_endpoint(gs: GraphSense, http_mock):
    http_mock.add(
        "POST",
        "/btc/bulk.json/get_address",
        json_body=[{"address": "1A", "balance": 1}],
    )
    gs.bulk("get_address", ["1A", "1B"])
    assert any("/btc/bulk.json/get_address" in c.url for c in http_mock.calls)
    # body should contain the key list
    call = [c for c in http_mock.calls if "/bulk.json" in c.url][0]
    body = call.body
    assert "1A" in str(body)
    assert "1B" in str(body)


def test_gs_bulk_csv_hits_csv_endpoint(gs: GraphSense, http_mock):
    http_mock.add(
        "POST",
        "/btc/bulk.csv/get_address",
        body="address,balance\n1A,1\n",
        headers={"content-type": "text/csv"},
    )
    gs.bulk("get_address", ["1A"], format="csv")
    assert any("/btc/bulk.csv/get_address" in c.url for c in http_mock.calls)
