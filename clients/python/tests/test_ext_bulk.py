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


def test_chunked_splits_evenly_and_keeps_remainder():
    assert list(bulk_mod.chunked([1, 2, 3, 4, 5], 2)) == [[1, 2], [3, 4], [5]]
    assert list(bulk_mod.chunked([], 2)) == []


def test_merge_json_chunks_concatenates_rows():
    merged = bulk_mod.merge_json_chunks([[{"a": 1}], [{"a": 2}, {"a": 3}], None])
    assert merged == [{"a": 1}, {"a": 2}, {"a": 3}]


def test_merge_csv_chunks_unions_headers():
    """Each chunk gets its own server-side header inference, so a later chunk
    may carry a column the first one never saw. Nothing may be dropped."""
    first = "address,balance\n1A,1\n"
    second = "address,balance,token\n1B,2,usdt\n"
    merged = bulk_mod.merge_csv_chunks([first, second])
    lines = merged.strip().splitlines()
    assert lines[0] == "address,balance,token"
    assert lines[1] == "1A,1,"
    assert lines[2] == "1B,2,usdt"


def test_gs_bulk_splits_long_key_lists(gs: GraphSense, http_mock):
    """A key list longer than the server's per-request cap is sent as several
    requests instead of one oversized (and rejected) request."""
    http_mock.add(
        "POST",
        "/btc/bulk.json/get_address",
        json_body=[{"address": "1A", "balance": 1}],
    )
    keys = [f"addr{i}" for i in range(7)]
    result = gs.bulk("get_address", keys, chunk_size=3)

    calls = [c for c in http_mock.calls if "/bulk.json" in c.url]
    assert len(calls) == 3
    sent = [str(c.body) for c in calls]
    assert "addr0" in sent[0] and "addr2" in sent[0] and "addr3" not in sent[0]
    assert "addr6" in sent[2]
    # every chunk's rows come back concatenated
    assert len(result) == 3


def test_gs_bulk_sends_one_request_below_the_chunk_size(gs: GraphSense, http_mock):
    http_mock.add(
        "POST",
        "/btc/bulk.json/get_address",
        json_body=[{"address": "1A", "balance": 1}],
    )
    gs.bulk("get_address", ["1A", "1B"], chunk_size=100)
    assert len([c for c in http_mock.calls if "/bulk.json" in c.url]) == 1
