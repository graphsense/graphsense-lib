"""
Regression tests: current code vs baseline container.

This module compares the current FastAPI implementation against a baseline
Docker container (previous stable release) to ensure API compatibility.

Both servers query the same test data containers, ensuring consistent results.

Usage:
    # Run with pytest (containers started automatically)
    uv run pytest tests/rest/test_baseline_regression.py -v -m regression

    # With specific baseline version
    BASELINE_VERSION=v25.11.16 uv run pytest tests/rest/test_baseline_regression.py -v

    # Skip baseline container for quick iteration
    SKIP_BASELINE_CONTAINER=1 uv run pytest tests/rest/test_baseline_regression.py -v
"""

import json
import logging
import os
import re
import time
from typing import Any
from urllib.parse import urljoin

import pytest
import requests

from tests.rest.conftest import record_regression_timing

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Server endpoints from environment
CURRENT_SERVER = os.environ.get("CURRENT_SERVER", "http://localhost:9000")
BASELINE_SERVER = os.environ.get("BASELINE_SERVER", "http://localhost:9001")

HEADERS = {"Content-Type": "application/json", "Accept": "application/json"}

AUTHENTICATED_HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json",
}

# Test data constants - these should exist in the test database
BTC_ADDRESS = "1Archive1n2C579dMsAu3iC6tWzuQJz8dN"
BTC_ADDRESS_PRIVATE_TAGS = "3D4gm7eGSXiEkWS5V3hN9kDVo2eDGBK4eA"
ETH_ADDRESS = "0xdac17f958d2ee523a2206206994597c13d831ec7"
BTC_ENTITY = 109578
BTC_TX = "4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b"
BTC_HEIGHT = 100000


def normalize_endpoint_to_pattern(uri: str) -> str:
    """Normalize URI to a pattern for grouping timing data."""
    path = uri.split("?")[0]

    replacements = [
        (
            r"/addresses/[13][a-km-zA-HJ-NP-Z1-9]{25,34}(?=/|$)",
            "/addresses/{btc_address}",
        ),
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

    pattern = path
    for regex, replacement in replacements:
        pattern = re.sub(regex, replacement, pattern)

    return pattern


def get_response(
    base_url: str, endpoint: str, auth: str = "test", authenticated: bool = True
) -> tuple[dict, int, float]:
    """Get response from an endpoint, returning (data, status_code, elapsed_time)."""
    url = urljoin(base_url + "/", endpoint.lstrip("/"))
    base_headers = AUTHENTICATED_HEADERS if authenticated else HEADERS
    headers = {**base_headers, "Authorization": auth}

    start = time.time()
    response = requests.get(url, headers=headers, timeout=30)
    elapsed = time.time() - start

    try:
        data = response.json()
    except json.JSONDecodeError:
        data = {"_raw": response.text}

    return data, response.status_code, elapsed


def post_response(
    base_url: str,
    endpoint: str,
    body: dict,
    auth: str = "test",
    authenticated: bool = True,
) -> tuple[dict, int, float]:
    """POST request to an endpoint, returning (data, status_code, elapsed_time)."""
    url = urljoin(base_url + "/", endpoint.lstrip("/"))
    base_headers = AUTHENTICATED_HEADERS if authenticated else HEADERS
    headers = {**base_headers, "Authorization": auth}

    start = time.time()
    response = requests.post(url, headers=headers, json=body, timeout=60)
    elapsed = time.time() - start

    try:
        data = response.json()
    except json.JSONDecodeError:
        data = {"_raw": response.text}

    return data, response.status_code, elapsed


def normalize_response(data: Any) -> Any:
    """Normalize response data for comparison."""
    if isinstance(data, dict):
        return {k: normalize_response(v) for k, v in sorted(data.items())}
    elif isinstance(data, list):
        return [normalize_response(item) for item in data]
    elif isinstance(data, float):
        return round(data, 8)
    return data


# Keys for which list order doesn't matter
UNORDERED_LIST_KEYS = {
    "address_tags",
    "addresses",
    "tags",
    "concepts",
    "sources",
    "creators",
}

# Keys to ignore in comparison
IGNORED_KEYS = {"version"}


def get_sort_key(item: Any) -> Any:
    """Get a sort key for list items."""
    if isinstance(item, dict):
        if "_request_address" in item:
            return (item.get("_request_address", ""), str(item))
        if "address" in item:
            return (item.get("address", ""), item.get("label", ""), str(item))
        return str(sorted(item.items()))
    return str(item)


def should_sort_list(data: list, path: str) -> bool:
    """Determine if a list should be sorted before comparison."""
    path_key = path.split(".")[-1] if path else ""
    if path_key in UNORDERED_LIST_KEYS:
        return True
    if path == "" and data and isinstance(data[0], dict):
        if "_request_address" in data[0] or "address" in data[0]:
            return True
    return False


def compare_responses(baseline_data: Any, current_data: Any, path: str = "") -> list[str]:
    """Compare two responses and return list of differences."""
    differences = []

    if type(baseline_data) != type(current_data):
        differences.append(
            f"{path}: type mismatch: {type(baseline_data).__name__} vs {type(current_data).__name__}"
        )
        return differences

    if isinstance(baseline_data, dict):
        baseline_keys = set(baseline_data.keys()) - IGNORED_KEYS
        current_keys = set(current_data.keys()) - IGNORED_KEYS

        missing_in_current = baseline_keys - current_keys

        if missing_in_current:
            differences.append(f"{path}: keys missing in current: {missing_in_current}")
        # Extra keys in current are acceptable (new fields are features, not regressions)

        for key in baseline_keys & current_keys:
            sub_path = f"{path}.{key}" if path else key
            differences.extend(
                compare_responses(baseline_data[key], current_data[key], sub_path)
            )

    elif isinstance(baseline_data, list):
        if len(baseline_data) != len(current_data):
            differences.append(
                f"{path}: list length mismatch: {len(baseline_data)} vs {len(current_data)}"
            )
        else:
            if should_sort_list(baseline_data, path):
                baseline_sorted = sorted(baseline_data, key=get_sort_key)
                current_sorted = sorted(current_data, key=get_sort_key)
                for i, (baseline_item, current_item) in enumerate(
                    zip(baseline_sorted, current_sorted)
                ):
                    differences.extend(
                        compare_responses(baseline_item, current_item, f"{path}[{i}]")
                    )
            else:
                for i, (baseline_item, current_item) in enumerate(
                    zip(baseline_data, current_data)
                ):
                    differences.extend(
                        compare_responses(baseline_item, current_item, f"{path}[{i}]")
                    )

    elif baseline_data != current_data:
        baseline_str = str(baseline_data)[:100]
        current_str = str(current_data)[:100]
        differences.append(f"{path}: value mismatch: {baseline_str} vs {current_str}")

    return differences


class BaselineRegressionTestBase:
    """Base class for baseline regression tests."""

    baseline_url: str = BASELINE_SERVER

    def compare_endpoint(self, endpoint: str, auth: str = "test") -> dict:
        """Compare an endpoint between baseline and current servers."""
        baseline_data, baseline_status, baseline_time = get_response(
            self.baseline_url, endpoint, auth
        )
        current_data, current_status, current_time = get_response(
            CURRENT_SERVER, endpoint, auth
        )

        result = {
            "endpoint": endpoint,
            "baseline_status": baseline_status,
            "current_status": current_status,
            "baseline_time": baseline_time,
            "current_time": current_time,
            "speedup": baseline_time / current_time if current_time > 0 else 0,
            "differences": [],
        }

        if baseline_status != current_status:
            result["differences"].append(
                f"Status code mismatch: {baseline_status} vs {current_status}"
            )
        elif baseline_status == 200:
            baseline_normalized = normalize_response(baseline_data)
            current_normalized = normalize_response(current_data)
            result["differences"] = compare_responses(
                baseline_normalized, current_normalized
            )

        return result

    def compare_post_endpoint(
        self, endpoint: str, body: dict, auth: str = "test"
    ) -> dict:
        """Compare a POST endpoint between baseline and current servers."""
        baseline_data, baseline_status, baseline_time = post_response(
            self.baseline_url, endpoint, body, auth
        )
        current_data, current_status, current_time = post_response(
            CURRENT_SERVER, endpoint, body, auth
        )

        result = {
            "endpoint": endpoint,
            "body": body,
            "baseline_status": baseline_status,
            "current_status": current_status,
            "baseline_time": baseline_time,
            "current_time": current_time,
            "differences": [],
        }

        if baseline_status != current_status:
            result["differences"].append(
                f"Status code mismatch: {baseline_status} vs {current_status}"
            )
        elif baseline_status == 200:
            baseline_normalized = normalize_response(baseline_data)
            current_normalized = normalize_response(current_data)
            result["differences"] = compare_responses(
                baseline_normalized, current_normalized
            )

        return result

    def assert_endpoint_equal(self, endpoint: str, auth: str = "test"):
        """Assert that an endpoint returns identical results from both servers."""
        result = self.compare_endpoint(endpoint, auth)

        pattern = normalize_endpoint_to_pattern(endpoint)
        record_regression_timing(
            endpoint=endpoint,
            baseline_time=result["baseline_time"],
            current_time=result["current_time"],
            pattern=pattern,
        )

        logger.info(
            f"  {endpoint}: baseline={result['baseline_time']:.3f}s, "
            f"current={result['current_time']:.3f}s, speedup={result['speedup']:.1f}x"
        )

        if result["differences"]:
            diff_str = "\n  ".join(result["differences"][:10])
            if len(result["differences"]) > 10:
                diff_str += (
                    f"\n  ... and {len(result['differences']) - 10} more differences"
                )
            pytest.fail(f"Endpoint {endpoint} has differences:\n  {diff_str}")

    def assert_post_endpoint_equal(self, endpoint: str, body: dict, auth: str = "test"):
        """Assert that a POST endpoint returns identical results from both servers."""
        result = self.compare_post_endpoint(endpoint, body, auth)

        logger.info(
            f"  {endpoint}: baseline={result['baseline_time']:.3f}s, "
            f"current={result['current_time']:.3f}s"
        )

        if result["differences"]:
            diff_str = "\n  ".join(result["differences"][:10])
            if len(result["differences"]) > 10:
                diff_str += (
                    f"\n  ... and {len(result['differences']) - 10} more differences"
                )
            pytest.fail(f"Endpoint {endpoint} has differences:\n  {diff_str}")


class TestBaselineRegressionBasic(BaselineRegressionTestBase):
    """Basic endpoint tests comparing baseline vs current."""

    @pytest.mark.regression
    def test_stats(self):
        """Test /stats endpoint."""
        self.assert_endpoint_equal("stats")

    @pytest.mark.regression
    def test_search_basic(self):
        """Test /search endpoint."""
        self.assert_endpoint_equal("search?q=binance&limit=5")

    @pytest.mark.regression
    def test_get_address_btc(self):
        """Test BTC address endpoint."""
        self.assert_endpoint_equal(f"btc/addresses/{BTC_ADDRESS}")

    @pytest.mark.regression
    def test_get_address_eth(self):
        """Test ETH address endpoint."""
        self.assert_endpoint_equal(f"eth/addresses/{ETH_ADDRESS}")

    @pytest.mark.regression
    def test_get_entity_btc(self):
        """Test BTC entity endpoint."""
        self.assert_endpoint_equal(f"btc/entities/{BTC_ENTITY}")

    @pytest.mark.regression
    def test_get_block(self):
        """Test block endpoint."""
        self.assert_endpoint_equal(f"btc/blocks/{BTC_HEIGHT}")

    @pytest.mark.regression
    def test_get_tx(self):
        """Test transaction endpoint."""
        self.assert_endpoint_equal(f"btc/txs/{BTC_TX}")

    @pytest.mark.regression
    def test_get_exchange_rates(self):
        """Test exchange rates endpoint."""
        self.assert_endpoint_equal(f"btc/rates/{BTC_HEIGHT}")

    @pytest.mark.regression
    def test_list_address_txs(self):
        """Test address transactions endpoint."""
        self.assert_endpoint_equal(f"btc/addresses/{BTC_ADDRESS}/txs?pagesize=5")

    @pytest.mark.regression
    def test_list_address_neighbors(self):
        """Test address neighbors endpoint."""
        self.assert_endpoint_equal(
            f"btc/addresses/{BTC_ADDRESS}/neighbors?direction=out&pagesize=5"
        )

    @pytest.mark.regression
    def test_list_entity_addresses(self):
        """Test entity addresses endpoint."""
        self.assert_endpoint_equal(f"btc/entities/{BTC_ENTITY}/addresses?pagesize=5")

    @pytest.mark.regression
    def test_list_entity_neighbors(self):
        """Test entity neighbors endpoint."""
        self.assert_endpoint_equal(
            f"btc/entities/{BTC_ENTITY}/neighbors?direction=out&pagesize=5"
        )

    @pytest.mark.regression
    def test_list_tags_by_address(self):
        """Test address tags endpoint."""
        self.assert_endpoint_equal(f"btc/addresses/{BTC_ADDRESS}/tags")

    @pytest.mark.regression
    def test_list_taxonomies(self):
        """Test taxonomies endpoint."""
        self.assert_endpoint_equal("tags/taxonomies")

    @pytest.mark.regression
    def test_supported_tokens(self):
        """Test supported tokens endpoint."""
        self.assert_endpoint_equal("eth/supported_tokens")


class TestBaselineRegressionAddressParameters(BaselineRegressionTestBase):
    """Test address endpoints with various parameter configurations."""

    @pytest.mark.regression
    @pytest.mark.parametrize("include_actors", [True, False])
    def test_get_address_include_actors(self, include_actors):
        """Test get_address with include_actors parameter."""
        self.assert_endpoint_equal(
            f"btc/addresses/{BTC_ADDRESS}?include_actors={str(include_actors).lower()}"
        )

    @pytest.mark.regression
    @pytest.mark.parametrize("direction", ["in", "out"])
    def test_list_address_txs_direction(self, direction):
        """Test list_address_txs with direction parameter."""
        self.assert_endpoint_equal(
            f"btc/addresses/{BTC_ADDRESS}/txs?direction={direction}&pagesize=5"
        )

    @pytest.mark.regression
    @pytest.mark.parametrize("order", ["asc", "desc"])
    def test_list_address_txs_order(self, order):
        """Test list_address_txs with order parameter."""
        self.assert_endpoint_equal(
            f"btc/addresses/{BTC_ADDRESS}/txs?order={order}&pagesize=5"
        )

    @pytest.mark.regression
    @pytest.mark.parametrize("direction", ["in", "out"])
    def test_list_address_neighbors_direction(self, direction):
        """Test list_address_neighbors with direction parameter."""
        self.assert_endpoint_equal(
            f"btc/addresses/{BTC_ADDRESS}/neighbors?direction={direction}&pagesize=5"
        )


class TestBaselineRegressionEntityParameters(BaselineRegressionTestBase):
    """Test entity endpoints with various parameter configurations."""

    @pytest.mark.regression
    @pytest.mark.parametrize("include_actors", [True, False])
    def test_get_entity_include_actors(self, include_actors):
        """Test get_entity with include_actors parameter."""
        self.assert_endpoint_equal(
            f"btc/entities/{BTC_ENTITY}?include_actors={str(include_actors).lower()}"
        )

    @pytest.mark.regression
    @pytest.mark.parametrize("direction", ["in", "out"])
    def test_list_entity_neighbors_direction(self, direction):
        """Test list_entity_neighbors with direction parameter."""
        self.assert_endpoint_equal(
            f"btc/entities/{BTC_ENTITY}/neighbors?direction={direction}&pagesize=5"
        )

    @pytest.mark.regression
    @pytest.mark.parametrize("direction", ["in", "out"])
    def test_list_entity_txs_direction(self, direction):
        """Test list_entity_txs with direction parameter."""
        self.assert_endpoint_equal(
            f"btc/entities/{BTC_ENTITY}/txs?direction={direction}&pagesize=5"
        )


class TestBaselineRegressionTransactionParameters(BaselineRegressionTestBase):
    """Test transaction endpoints with various parameter configurations."""

    @pytest.mark.regression
    @pytest.mark.parametrize("include_io", [True, False])
    def test_get_tx_include_io(self, include_io):
        """Test get_tx with include_io parameter."""
        self.assert_endpoint_equal(
            f"btc/txs/{BTC_TX}?include_io={str(include_io).lower()}"
        )

    @pytest.mark.regression
    @pytest.mark.parametrize("io", ["inputs", "outputs"])
    def test_get_tx_io(self, io):
        """Test get_tx_io with inputs/outputs parameter."""
        self.assert_endpoint_equal(f"btc/txs/{BTC_TX}/{io}")


class TestBaselineRegressionBulkEndpoints(BaselineRegressionTestBase):
    """Test bulk endpoints for baseline comparison."""

    @pytest.mark.regression
    def test_bulk_json_get_address(self):
        """Test bulk JSON get_address endpoint."""
        body = {"address": [BTC_ADDRESS]}
        self.assert_post_endpoint_equal("btc/bulk.json/get_address?num_pages=1", body)

    @pytest.mark.regression
    def test_bulk_json_get_entity(self):
        """Test bulk JSON get_entity endpoint."""
        body = {"entity": [BTC_ENTITY]}
        self.assert_post_endpoint_equal("btc/bulk.json/get_entity?num_pages=1", body)


class TestBaselineRegressionETHEndpoints(BaselineRegressionTestBase):
    """Test Ethereum-specific endpoints."""

    @pytest.mark.regression
    def test_eth_address_txs(self):
        """Test ETH address transactions."""
        self.assert_endpoint_equal(f"eth/addresses/{ETH_ADDRESS}/txs?pagesize=5")

    @pytest.mark.regression
    def test_eth_address_neighbors_in(self):
        """Test ETH address incoming neighbors."""
        self.assert_endpoint_equal(
            f"eth/addresses/{ETH_ADDRESS}/neighbors?direction=in&pagesize=5"
        )

    @pytest.mark.regression
    def test_eth_address_neighbors_out(self):
        """Test ETH address outgoing neighbors."""
        self.assert_endpoint_equal(
            f"eth/addresses/{ETH_ADDRESS}/neighbors?direction=out&pagesize=5"
        )

    @pytest.mark.regression
    def test_eth_address_entity(self):
        """Test ETH address entity lookup."""
        self.assert_endpoint_equal(f"eth/addresses/{ETH_ADDRESS}/entity")

    @pytest.mark.regression
    def test_eth_address_tags(self):
        """Test ETH address tags."""
        self.assert_endpoint_equal(f"eth/addresses/{ETH_ADDRESS}/tags")


class TestBaselineRegressionConversions(BaselineRegressionTestBase):
    """Test DeFi conversion endpoints."""

    @pytest.mark.regression
    def test_eth_dex_swap_conversion(self):
        """Test ETH DEX swap conversion."""
        tx = "0x76f4263391a7d72f66cb1f254e8643e37ca739ab2859b9e9cd5b5bda3194332b"
        self.assert_endpoint_equal(f"eth/txs/{tx}/conversions")

    @pytest.mark.regression
    def test_eth_bridge_conversion(self):
        """Test ETH bridge conversion (eth to btc)."""
        tx = "0x6D65123E246D752DE3F39E0FDF5B788BAAD35A29B7E95B74C714E6C7C1EA61DD"
        self.assert_endpoint_equal(f"eth/txs/{tx}/conversions")


class TestBaselineRegressionLinks(BaselineRegressionTestBase):
    """Test links endpoints between addresses and entities."""

    @pytest.mark.regression
    def test_address_links(self):
        """Test address links endpoint."""
        neighbor = "1HQ3Go3ggs8pFnXuHVHRytPCq5fGG8Hbhx"
        self.assert_endpoint_equal(
            f"btc/addresses/{BTC_ADDRESS}/links?neighbor={neighbor}"
        )

    @pytest.mark.regression
    def test_entity_links(self):
        """Test entity links endpoint."""
        neighbor_entity = 17642138
        self.assert_endpoint_equal(
            f"btc/entities/{BTC_ENTITY}/links?neighbor={neighbor_entity}"
        )
