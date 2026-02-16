#!/usr/bin/env python3
"""
Generate regression test cases from Loki API gateway logs.

Usage:
    LOKI_URL=http://loki.example.com:3100 uv run scripts/generate_loki_tests.py

    # With custom options
    uv run scripts/generate_loki_tests.py --hours 48 --limit 10000
"""

import argparse
import json
import logging
import os
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import requests

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

LOKI_URL = os.environ.get("LOKI_URL", "")

EXCLUDE_PATTERNS = [
    r"^/health", r"^/metrics", r"^/openapi", r"^/docs", r"^/ui", r"^/favicon", r"^/$",
    r"/\.env", r"wp-includes", r"wp-content", r"wp-admin", r"wordpress",
    r"/actuator", r"wlwmanifest", r"\.php", r"\.asp", r"\.aspx", r"\.cgi",
    r"\.xml$", r"^/cgi-bin", r"^/shell", r"^/eval", r"^/\.", r"^/sitemap",
    r"^/swagger\.json", r"^/robots", r"^/admin", r"^/backend", r"^/api/\.", r"^/app/\.",
    r"swagger-ui", r"\.js$", r"\.css$", r"\.map$", r"\.ico$", r"\.png$",
    r"\.jpg$", r"\.gif$", r"\.woff", r"\.ttf$", r"\.eot$", r"\.svg$",
    r'"', r"'", r"<", r">", r"\|", r"\\\\",
]

MAX_EXAMPLES_PER_PATTERN = 25


def fetch_loki_logs(hours: int = 24, limit: int = 5000, loki_url: str = LOKI_URL) -> dict:
    """Fetch logs from Loki with pagination."""
    query = '{service="apisix-gateway"}'
    url = f"{loki_url}/loki/api/v1/query_range"
    batch_size = 5000
    all_results = []

    end_time = datetime.now()
    start_time = end_time - timedelta(hours=hours)
    current_end = int(end_time.timestamp() * 1e9)
    start_ns = int(start_time.timestamp() * 1e9)

    log.info(f"Fetching logs from {loki_url}")
    log.info(f"  Time range: last {hours} hours")

    total_fetched = 0
    batch_num = 0

    while total_fetched < limit:
        batch_num += 1
        batch_limit = min(batch_size, limit - total_fetched)

        params = {
            "query": query,
            "start": str(start_ns),
            "end": str(current_end),
            "limit": str(batch_limit),
            "direction": "backward",
        }

        log.info(f"  Batch {batch_num}: fetching up to {batch_limit}...")
        response = requests.get(url, params=params, timeout=300)
        response.raise_for_status()

        results = response.json().get("data", {}).get("result", [])
        if not results:
            break

        batch_entries = sum(len(s.get("values", [])) for s in results)
        if batch_entries == 0:
            break

        all_results.extend(results)
        total_fetched += batch_entries
        log.info(f"  Batch {batch_num}: got {batch_entries} (total: {total_fetched})")

        oldest_ts = min(
            int(e[0]) for s in results for e in s.get("values", [])
        )
        if oldest_ts <= start_ns:
            break
        current_end = oldest_ts - 1

        if batch_entries < batch_limit:
            break

    log.info(f"Total fetched: {total_fetched}")
    return {"data": {"result": all_results}}


def parse_log_entry(entry: list) -> dict | None:
    """Parse a log entry to extract API call info."""
    timestamp, log_line = entry

    try:
        parsed = json.loads(log_line)
    except json.JSONDecodeError:
        return None

    if "gs-rest" not in parsed.get("route_name", ""):
        return None
    if not parsed.get("server_host", "").startswith("api."):
        return None

    uri = parsed.get("uri") or parsed.get("request_uri") or parsed.get("upstream_uri")
    method = parsed.get("method", "GET")
    status = parsed.get("status")

    if isinstance(status, str):
        status = int(status)

    if not uri or method != "GET":
        return None

    for pattern in EXCLUDE_PATTERNS:
        if re.search(pattern, uri):
            return None

    return {
        "uri": uri,
        "method": method,
        "status": status or 200,
        "timestamp": timestamp,
    }


def normalize_uri(uri: str) -> str:
    """Normalize URI to pattern."""
    path = urlparse(uri).path
    replacements = [
        (r"/addresses/[13][a-km-zA-HJ-NP-Z1-9]{25,34}(?=/|$)", "/addresses/{btc_address}"),
        (r"/addresses/bc1[a-z0-9]{39,59}(?=/|$)", "/addresses/{btc_bech32}"),
        (r"/addresses/0x[a-fA-F0-9]{40}(?=/|$)", "/addresses/{eth_address}"),
        (r"/addresses/T[a-zA-Z0-9]{33}(?=/|$)", "/addresses/{trx_address}"),
        (r"/entities/\d+(?=/|$)", "/entities/{entity_id}"),
        (r"/txs/0x[a-fA-F0-9]{64}(?=/|$)", "/txs/{eth_tx}"),
        (r"/txs/[a-fA-F0-9]{64}(?=/|$)", "/txs/{tx_hash}"),
        (r"/blocks/\d+(?=/|$)", "/blocks/{height}"),
        (r"/rates/\d+(?=/|$)", "/rates/{height}"),
        (r"/block_by_date/[^/]+(?=/|$)", "/block_by_date/{date}"),
        (r"/actors/[^/]+(?=/|$)", "/actors/{actor}"),
        (r"/taxonomies/[^/]+/concepts", "/taxonomies/{taxonomy}/concepts"),
    ]
    for regex, replacement in replacements:
        path = re.sub(regex, replacement, path)
    return path


def extract_calls(loki_data: dict) -> list[dict]:
    """Extract unique API calls from Loki response."""
    calls = []
    seen = set()

    for stream in loki_data.get("data", {}).get("result", []):
        for entry in stream.get("values", []):
            parsed = parse_log_entry(entry)
            if parsed and parsed["uri"] not in seen:
                seen.add(parsed["uri"])
                calls.append(parsed)

    return calls


def group_calls(calls: list[dict]) -> dict[str, list[dict]]:
    """Group calls by pattern + status category."""
    grouped = defaultdict(list)
    for call in calls:
        pattern = normalize_uri(call["uri"])
        status_cat = f"{call.get('status', 200) // 100}xx"
        grouped[f"{pattern} [{status_cat}]"].append(call)
    return grouped


def select_calls(grouped: dict[str, list[dict]], max_per: int) -> list[dict]:
    """Select representative calls."""
    selected = []
    for pattern_key, calls in sorted(grouped.items()):
        sorted_calls = sorted(calls, key=lambda x: x["timestamp"], reverse=True)
        seen_params = set()

        for call in sorted_calls:
            param_keys = frozenset(parse_qs(urlparse(call["uri"]).query).keys())
            if param_keys not in seen_params or len(seen_params) < max_per:
                seen_params.add(param_keys)
                selected.append({
                    "pattern": pattern_key,
                    "uri": call["uri"],
                    "status": call.get("status", 200),
                })
                if len([s for s in selected if s["pattern"] == pattern_key]) >= max_per:
                    break

    return selected


def generate_pytest(calls: list[dict], output: Path):
    """Generate pytest file."""
    by_pattern = defaultdict(list)
    for call in calls:
        by_pattern[call["pattern"]].append((call["uri"], call.get("status", 200)))

    lines = [
        '"""',
        "Auto-generated regression tests from Loki API logs.",
        "",
        f"Generated at: {datetime.now().isoformat()}",
        f"Total endpoints: {len(calls)}",
        f"Unique patterns: {len(by_pattern)}",
        '"""',
        "",
        "import pytest",
        "from tests.rest.test_baseline_regression import BaselineRegressionTestBase",
        "",
        "",
        "class TestLokiGeneratedCalls(BaselineRegressionTestBase):",
        '    """Tests generated from production API call logs."""',
        "",
    ]

    for pattern, uri_status_pairs in sorted(by_pattern.items()):
        test_name = re.sub(r"[^a-zA-Z0-9_]", "_", pattern.replace("/", "_"))
        test_name = re.sub(r"_+", "_", test_name).strip("_")

        if len(uri_status_pairs) == 1:
            uri, status = uri_status_pairs[0]
            lines.extend([
                "    @pytest.mark.regression",
                "    @pytest.mark.loki_generated",
                f"    def test_{test_name}(self):",
                f'        """Test {pattern} (expected status: {status})"""',
                f'        self.assert_endpoint_equal("{uri}")',
                "",
            ])
        else:
            lines.extend([
                "    @pytest.mark.regression",
                "    @pytest.mark.loki_generated",
                "    @pytest.mark.parametrize(",
                '        "uri,expected_status",',
                "        [",
            ])
            for uri, status in uri_status_pairs:
                lines.append(f'            ("{uri}", {status}),')
            lines.extend([
                "        ],",
                "    )",
                f"    def test_{test_name}(self, uri, expected_status):",
                f'        """Test {pattern}"""',
                "        self.assert_endpoint_equal(uri)",
                "",
            ])

    output.write_text("\n".join(lines))
    log.info(f"Generated: {output}")


def main():
    parser = argparse.ArgumentParser(description="Generate Loki regression tests")
    parser.add_argument("--hours", type=int, default=672)  # 4 weeks
    parser.add_argument("--limit", type=int, default=200000)
    parser.add_argument("--max-per-pattern", type=int, default=MAX_EXAMPLES_PER_PATTERN)
    parser.add_argument("--output", type=Path, default=Path("tests/rest/test_loki_generated.py"))
    parser.add_argument("--loki-url", default=LOKI_URL)
    args = parser.parse_args()

    if not args.loki_url:
        log.error("Error: Set LOKI_URL env var or use --loki-url")
        sys.exit(1)

    loki_data = fetch_loki_logs(args.hours, args.limit, args.loki_url)
    calls = extract_calls(loki_data)
    log.info(f"Found {len(calls)} unique calls")

    if not calls:
        log.error("No calls found")
        sys.exit(1)

    grouped = group_calls(calls)
    selected = select_calls(grouped, args.max_per_pattern)
    log.info(f"Selected {len(selected)} representative calls")

    args.output.parent.mkdir(parents=True, exist_ok=True)
    generate_pytest(selected, args.output)


if __name__ == "__main__":
    main()
