"""Tests for `tags-for` pagination/options and date support on `block` /
`exchange-rates`."""

from __future__ import annotations

import json
from typing import Any
from urllib.parse import parse_qs, urlparse

from click.testing import CliRunner

from graphsense.cli.main import cli
from graphsense.ext import GraphSense


def _query(url: str) -> dict[str, list[str]]:
    return parse_qs(urlparse(url).query)


# ---------------------------------------------------------------- tags-for ---


def _tags_payload(n: int, next_page: str | None) -> dict[str, Any]:
    return {
        "address_tags": [
            {
                "label": f"tag-{i}",
                "category": "exchange",
                "actor": "x",
                "confidence": "heuristic",
                "confidence_level": 50,
                "lastmod": 1,
                "currency": "btc",
            }
            for i in range(n)
        ],
        "next_page": next_page,
    }


def test_tags_for_default_includes_best_cluster_tag(http_mock):
    http_mock.add("GET", r"/btc/addresses/1A/tags", json_body=_tags_payload(1, None))
    runner = CliRunner()
    res = runner.invoke(
        cli,
        ["--host", "http://testserver", "--api-key", "t", "tags-for", "btc", "1A"],
    )
    assert res.exit_code == 0, res.output
    q = _query(http_mock.calls[0].url)
    assert q.get("include_best_cluster_tag") == ["True"]


def test_tags_for_no_include_best_cluster_tag(http_mock):
    http_mock.add("GET", r"/btc/addresses/1A/tags", json_body=_tags_payload(1, None))
    runner = CliRunner()
    res = runner.invoke(
        cli,
        [
            "--host",
            "http://testserver",
            "--api-key",
            "t",
            "tags-for",
            "btc",
            "1A",
            "--no-include-best-cluster-tag",
        ],
    )
    assert res.exit_code == 0, res.output
    q = _query(http_mock.calls[0].url)
    assert q.get("include_best_cluster_tag") == ["False"]


def test_tags_for_walks_pages_until_exhausted(http_mock):
    # Simulate the server: page 1 returns next_page="p2"; page 2 returns null.
    page_seq = [
        _tags_payload(2, "p2"),
        _tags_payload(1, None),
    ]
    state = {"i": 0}

    def matcher(method: str, url: str) -> bool:
        return method == "GET" and "/btc/addresses/1A/tags" in url

    def responder(_call):
        body = json.dumps(page_seq[state["i"]]).encode("utf-8")
        state["i"] += 1
        return 200, body, {"content-type": "application/json"}

    http_mock.rules.append((matcher, responder))

    runner = CliRunner()
    res = runner.invoke(
        cli,
        [
            "--host",
            "http://testserver",
            "--api-key",
            "t",
            "tags-for",
            "btc",
            "1A",
            "--page-size",
            "2",
        ],
    )
    assert res.exit_code == 0, res.output
    parsed = json.loads(res.output)
    assert len(parsed["address_tags"]) == 3
    assert parsed["next_page"] is None
    # Two HTTP calls: first without `page=`, second with `page=p2`.
    assert len(http_mock.calls) == 2
    q1 = _query(http_mock.calls[0].url)
    q2 = _query(http_mock.calls[1].url)
    assert "page" not in q1
    assert q1.get("pagesize") == ["2"]
    assert q2.get("page") == ["p2"]


def test_tags_for_limit_truncates_and_surfaces_next_page(http_mock):
    page_seq = [
        _tags_payload(3, "p2"),
        _tags_payload(3, "p3"),
    ]
    state = {"i": 0}

    def matcher(method: str, url: str) -> bool:
        return method == "GET" and "/btc/addresses/1A/tags" in url

    def responder(_call):
        body = json.dumps(page_seq[state["i"]]).encode("utf-8")
        state["i"] += 1
        return 200, body, {"content-type": "application/json"}

    http_mock.rules.append((matcher, responder))

    runner = CliRunner()
    res = runner.invoke(
        cli,
        [
            "--host",
            "http://testserver",
            "--api-key",
            "t",
            "tags-for",
            "btc",
            "1A",
            "--limit",
            "5",
            "--page-size",
            "3",
        ],
    )
    assert res.exit_code == 0, res.output
    parsed = json.loads(res.output)
    assert len(parsed["address_tags"]) == 5
    # We stopped after page 2 (which returned next_page="p3"), so callers
    # can resume from there.
    assert parsed["next_page"] == "p3"
    assert len(http_mock.calls) == 2


def test_tags_for_facade_returns_aggregated_dict(gs: GraphSense, http_mock):
    page_seq = [
        _tags_payload(1, "next"),
        _tags_payload(1, None),
    ]
    state = {"i": 0}

    def matcher(method: str, url: str) -> bool:
        return method == "GET" and "/btc/addresses/1A/tags" in url

    def responder(_call):
        body = json.dumps(page_seq[state["i"]]).encode("utf-8")
        state["i"] += 1
        return 200, body, {"content-type": "application/json"}

    http_mock.rules.append((matcher, responder))

    out = gs.tags_for("1A", page_size=1)
    assert len(out["address_tags"]) == 2
    assert out["next_page"] is None


# ----------------------------------------------------- block / exchange-rates


_BLOCK = {
    "block_hash": "0x" + "00" * 32,
    "currency": "btc",
    "height": 100,
    "no_txs": 1,
    "timestamp": 1700000000,
}


def test_block_by_height(http_mock):
    http_mock.add("GET", r"/btc/blocks/100", json_body=_BLOCK)
    runner = CliRunner()
    res = runner.invoke(
        cli,
        ["--host", "http://testserver", "--api-key", "t", "block", "btc", "100"],
    )
    assert res.exit_code == 0, res.output
    assert any("/btc/blocks/100" in c.url for c in http_mock.calls)


def test_block_by_date(http_mock):
    http_mock.add(
        "GET",
        r"/btc/block_by_date/2024-01-15",
        json_body={
            "before_block": 825000,
            "before_timestamp": 1705276800,
            "after_block": 825001,
            "after_timestamp": 1705276900,
        },
    )
    runner = CliRunner()
    res = runner.invoke(
        cli,
        [
            "--host",
            "http://testserver",
            "--api-key",
            "t",
            "block",
            "btc",
            "2024-01-15",
        ],
    )
    assert res.exit_code == 0, res.output
    urls = [c.url for c in http_mock.calls]
    assert any("/btc/block_by_date/2024-01-15" in u for u in urls)
    assert not any("/btc/blocks/" in u for u in urls)


def test_exchange_rates_by_height(http_mock):
    http_mock.add(
        "GET",
        r"/btc/rates/100",
        json_body={"height": 100, "rates": [{"code": "usd", "value": 1.0}]},
    )
    runner = CliRunner()
    res = runner.invoke(
        cli,
        [
            "--host",
            "http://testserver",
            "--api-key",
            "t",
            "exchange-rates",
            "btc",
            "100",
        ],
    )
    assert res.exit_code == 0, res.output
    assert any("/btc/rates/100" in c.url for c in http_mock.calls)


def test_block_by_iso_datetime(http_mock):
    """ISO 8601 datetime forms (with time/timezone) reach get_block_by_date."""
    http_mock.add(
        "GET",
        # Path-encoded `:` becomes %3A; match either form.
        r"/btc/block_by_date/2024-01-15T12(:|%3A)34(:|%3A)56Z",
        json_body={"before_block": 825000, "before_timestamp": 1705320896},
    )
    runner = CliRunner()
    res = runner.invoke(
        cli,
        [
            "--host",
            "http://testserver",
            "--api-key",
            "t",
            "block",
            "btc",
            "2024-01-15T12:34:56Z",
        ],
    )
    assert res.exit_code == 0, res.output
    urls = [c.url for c in http_mock.calls]
    assert any("/btc/block_by_date/" in u for u in urls)


def test_block_invalid_height_or_date_returns_friendly_error(http_mock):
    runner = CliRunner()
    res = runner.invoke(
        cli,
        [
            "--host",
            "http://testserver",
            "--api-key",
            "t",
            "block",
            "btc",
            "2020-10-15:9:00",
        ],
    )
    assert res.exit_code != 0
    assert "traceback" not in res.output.lower()
    assert "2020-10-15:9:00" in res.output
    assert "iso 8601" in res.output.lower()


def test_exchange_rates_invalid_height_or_date_returns_friendly_error(http_mock):
    runner = CliRunner()
    res = runner.invoke(
        cli,
        [
            "--host",
            "http://testserver",
            "--api-key",
            "t",
            "exchange-rates",
            "btc",
            "not-a-date",
        ],
    )
    assert res.exit_code != 0
    assert "traceback" not in res.output.lower()
    assert "iso 8601" in res.output.lower()


def test_exchange_rates_by_date_resolves_height_first(http_mock):
    # Order matters: register the more specific URL first.
    http_mock.add(
        "GET",
        r"/btc/block_by_date/2024-01-15",
        json_body={
            "before_block": 825000,
            "before_timestamp": 1705276800,
            "after_block": 825001,
            "after_timestamp": 1705276900,
        },
    )
    http_mock.add(
        "GET",
        r"/btc/rates/825000",
        json_body={"height": 825000, "rates": [{"code": "usd", "value": 1.0}]},
    )

    runner = CliRunner()
    res = runner.invoke(
        cli,
        [
            "--host",
            "http://testserver",
            "--api-key",
            "t",
            "exchange-rates",
            "btc",
            "2024-01-15",
        ],
    )
    assert res.exit_code == 0, res.output
    urls = [c.url for c in http_mock.calls]
    assert any("/btc/block_by_date/2024-01-15" in u for u in urls)
    assert any("/btc/rates/825000" in u for u in urls)
