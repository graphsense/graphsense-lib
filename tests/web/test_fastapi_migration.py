"""
DEPRECATED: Use test_baseline_regression.py instead.

Test to compare FastAPI migration against original aiohttp/Connexion version.

This test runs both server versions and compares their JSON outputs to ensure
the migration maintains exact functional equivalence.

This module is deprecated in favor of test_baseline_regression.py which uses
a Docker container as baseline instead of requiring manual server setup.

Usage (if you still need to run these tests):
    # Start old server on port 9001
    GS_REST_DEV_PORT=9001 make serve-old

    # Start new FastAPI server on port 9000
    uv run uvicorn graphsenselib.web.app:create_app --factory --port 9000

    # Run comparison tests (need to explicitly include migration tests)
    uv run pytest tests/web/test_fastapi_migration.py -v -m migration
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

from tests.web.conftest import record_migration_timing

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Server endpoints - can be overridden via environment variables
OLD_SERVER = os.environ.get("OLD_SERVER", "http://localhost:9001")
NEW_SERVER = os.environ.get("NEW_SERVER", "http://localhost:9000")

HEADERS = {"Content-Type": "application/json", "Accept": "application/json"}

# Headers that simulate authenticated access with private tags permission
# The X-Consumer-Groups header is typically set by an API gateway
AUTHENTICATED_HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "X-Consumer-Groups": "tags-private",  # Grants access to private tags
}

# Test data constants
BTC_ADDRESS = "1Archive1n2C579dMsAu3iC6tWzuQJz8dN"
BTC_ADDRESS_PRIVATE_TAGS = (
    "3D4gm7eGSXiEkWS5V3hN9kDVo2eDGBK4eA"  # Address with potentially private tags
)
ETH_ADDRESS = "0xdac17f958d2ee523a2206206994597c13d831ec7"
BTC_ENTITY = 109578
BTC_TX = "4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b"
BTC_HEIGHT = 100000


def normalize_endpoint_to_pattern(uri: str) -> str:
    """Normalize URI to a pattern for grouping timing data."""
    # Remove query string for pattern
    path = uri.split("?")[0]

    # Replace specific values with placeholders
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
    """Get response from an endpoint, returning (data, status_code, elapsed_time).

    Args:
        base_url: The server base URL
        endpoint: The API endpoint to call
        auth: Authorization header value
        authenticated: If True, include X-Consumer-Groups for private tags access
    """
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
    """POST request to an endpoint, returning (data, status_code, elapsed_time).

    Args:
        base_url: The server base URL
        endpoint: The API endpoint to call
        body: The JSON body to send
        auth: Authorization header value
        authenticated: If True, include X-Consumer-Groups for private tags access
    """
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
    """Normalize response data for comparison.

    Handles known differences that are acceptable:
    - Floating point precision differences
    - Ordering of certain list items (where order doesn't matter)
    """
    if isinstance(data, dict):
        return {k: normalize_response(v) for k, v in sorted(data.items())}
    elif isinstance(data, list):
        return [normalize_response(item) for item in data]
    elif isinstance(data, float):
        # Round floats to avoid precision issues
        return round(data, 8)
    return data


# Keys for which list order doesn't matter (unordered results from database)
UNORDERED_LIST_KEYS = {
    "address_tags",
    "addresses",
    "tags",
    "concepts",
    "sources",
    "creators",
}

# Keys to ignore in comparison (expected to differ between versions)
IGNORED_KEYS = {"version"}


def get_sort_key(item: Any) -> Any:
    """Get a sort key for list items."""
    if isinstance(item, dict):
        # Use '_request_address' or 'address' as primary sort key, then 'label'
        if "_request_address" in item:
            return (item.get("_request_address", ""), str(item))
        if "address" in item:
            return (item.get("address", ""), item.get("label", ""), str(item))
        return str(sorted(item.items()))
    return str(item)


def should_sort_list(data: list, path: str) -> bool:
    """Determine if a list should be sorted before comparison."""
    # Check if path ends with an unordered key
    path_key = path.split(".")[-1] if path else ""
    if path_key in UNORDERED_LIST_KEYS:
        return True
    # Top-level lists (bulk results) with address items should be sorted
    if path == "" and data and isinstance(data[0], dict):
        if "_request_address" in data[0] or "address" in data[0]:
            return True
    return False


def compare_responses(old_data: Any, new_data: Any, path: str = "") -> list[str]:
    """Compare two responses and return list of differences."""
    differences = []

    if type(old_data) != type(new_data):
        differences.append(
            f"{path}: type mismatch: {type(old_data).__name__} vs {type(new_data).__name__}"
        )
        return differences

    if isinstance(old_data, dict):
        old_keys = set(old_data.keys()) - IGNORED_KEYS
        new_keys = set(new_data.keys()) - IGNORED_KEYS

        missing_in_new = old_keys - new_keys
        missing_in_old = new_keys - old_keys

        if missing_in_new:
            differences.append(f"{path}: keys missing in new: {missing_in_new}")
        if missing_in_old:
            differences.append(f"{path}: extra keys in new: {missing_in_old}")

        for key in old_keys & new_keys:
            sub_path = f"{path}.{key}" if path else key
            differences.extend(
                compare_responses(old_data[key], new_data[key], sub_path)
            )

    elif isinstance(old_data, list):
        if len(old_data) != len(new_data):
            differences.append(
                f"{path}: list length mismatch: {len(old_data)} vs {len(new_data)}"
            )
        else:
            # Check if this is an unordered list
            if should_sort_list(old_data, path):
                # Sort both lists before comparing
                old_sorted = sorted(old_data, key=get_sort_key)
                new_sorted = sorted(new_data, key=get_sort_key)
                for i, (old_item, new_item) in enumerate(zip(old_sorted, new_sorted)):
                    differences.extend(
                        compare_responses(old_item, new_item, f"{path}[{i}]")
                    )
            else:
                for i, (old_item, new_item) in enumerate(zip(old_data, new_data)):
                    differences.extend(
                        compare_responses(old_item, new_item, f"{path}[{i}]")
                    )

    elif old_data != new_data:
        # Truncate long values for readability
        old_str = str(old_data)[:100]
        new_str = str(new_data)[:100]
        differences.append(f"{path}: value mismatch: {old_str} vs {new_str}")

    return differences


def check_servers_available():
    """Check if both servers are running."""
    for name, url in [("OLD", OLD_SERVER), ("NEW", NEW_SERVER)]:
        try:
            response = requests.get(f"{url}/stats", headers=HEADERS, timeout=5)
            if response.status_code not in [200, 401, 403]:
                pytest.skip(
                    f"{name} server at {url} returned status {response.status_code}"
                )
        except requests.exceptions.ConnectionError:
            pytest.skip(f"{name} server not available at {url}")


class MigrationTestBase:
    """Base class for migration tests with shared utilities."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Check servers are available before each test."""
        check_servers_available()

    def compare_endpoint(self, endpoint: str, auth: str = "test") -> dict:
        """Compare an endpoint between old and new servers."""
        old_data, old_status, old_time = get_response(OLD_SERVER, endpoint, auth)
        new_data, new_status, new_time = get_response(NEW_SERVER, endpoint, auth)

        result = {
            "endpoint": endpoint,
            "old_status": old_status,
            "new_status": new_status,
            "old_time": old_time,
            "new_time": new_time,
            "speedup": old_time / new_time if new_time > 0 else 0,
            "differences": [],
        }

        if old_status != new_status:
            result["differences"].append(
                f"Status code mismatch: {old_status} vs {new_status}"
            )
        elif old_status == 200:
            # Only compare content if both succeeded
            old_normalized = normalize_response(old_data)
            new_normalized = normalize_response(new_data)
            result["differences"] = compare_responses(old_normalized, new_normalized)

        return result

    def compare_post_endpoint(
        self, endpoint: str, body: dict, auth: str = "test"
    ) -> dict:
        """Compare a POST endpoint between old and new servers."""
        old_data, old_status, old_time = post_response(OLD_SERVER, endpoint, body, auth)
        new_data, new_status, new_time = post_response(NEW_SERVER, endpoint, body, auth)

        result = {
            "endpoint": endpoint,
            "body": body,
            "old_status": old_status,
            "new_status": new_status,
            "old_time": old_time,
            "new_time": new_time,
            "differences": [],
        }

        if old_status != new_status:
            result["differences"].append(
                f"Status code mismatch: {old_status} vs {new_status}"
            )
        elif old_status == 200:
            old_normalized = normalize_response(old_data)
            new_normalized = normalize_response(new_data)
            result["differences"] = compare_responses(old_normalized, new_normalized)

        return result

    def assert_endpoint_equal(self, endpoint: str, auth: str = "test"):
        """Assert that an endpoint returns identical results from both servers."""
        result = self.compare_endpoint(endpoint, auth)

        # Record timing for report
        pattern = normalize_endpoint_to_pattern(endpoint)
        record_migration_timing(
            endpoint=endpoint,
            old_time=result["old_time"],
            new_time=result["new_time"],
            pattern=pattern,
        )

        logger.info(
            f"  {endpoint}: old={result['old_time']:.3f}s, new={result['new_time']:.3f}s, "
            f"speedup={result['speedup']:.1f}x"
        )

        if result["differences"]:
            diff_str = "\n  ".join(
                result["differences"][:10]
            )  # Show first 10 differences
            if len(result["differences"]) > 10:
                diff_str += (
                    f"\n  ... and {len(result['differences']) - 10} more differences"
                )
            pytest.fail(f"Endpoint {endpoint} has differences:\n  {diff_str}")

    def assert_post_endpoint_equal(self, endpoint: str, body: dict, auth: str = "test"):
        """Assert that a POST endpoint returns identical results from both servers."""
        result = self.compare_post_endpoint(endpoint, body, auth)

        logger.info(
            f"  {endpoint}: old={result['old_time']:.3f}s, new={result['new_time']:.3f}s"
        )

        if result["differences"]:
            diff_str = "\n  ".join(result["differences"][:10])
            if len(result["differences"]) > 10:
                diff_str += (
                    f"\n  ... and {len(result['differences']) - 10} more differences"
                )
            pytest.fail(f"Endpoint {endpoint} has differences:\n  {diff_str}")


class TestFastAPIMigrationBasic(MigrationTestBase):
    """Basic endpoint tests - one test per endpoint with default parameters."""

    @pytest.mark.migration
    def test_openapi_spec(self):
        """Test that OpenAPI specs have matching endpoints and operations."""
        old_resp = requests.get(
            f"{OLD_SERVER}/openapi.json", headers=HEADERS, timeout=30
        )
        new_resp = requests.get(
            f"{NEW_SERVER}/openapi.json", headers=HEADERS, timeout=30
        )

        assert old_resp.status_code == 200, (
            f"Old server OpenAPI returned {old_resp.status_code}"
        )
        assert new_resp.status_code == 200, (
            f"New server OpenAPI returned {new_resp.status_code}"
        )

        old_spec = old_resp.json()
        new_spec = new_resp.json()

        # Normalize paths by stripping trailing slashes
        def normalize_path(p):
            return p.rstrip("/") if p != "/" else p

        # Compare paths (endpoints)
        old_paths = {normalize_path(p) for p in old_spec.get("paths", {}).keys()}
        new_paths = {normalize_path(p) for p in new_spec.get("paths", {}).keys()}

        missing_in_new = old_paths - new_paths
        extra_in_new = new_paths - old_paths

        differences = []
        if missing_in_new:
            differences.append(f"Paths missing in FastAPI: {sorted(missing_in_new)}")
        if extra_in_new:
            differences.append(f"Extra paths in FastAPI: {sorted(extra_in_new)}")

        # Build normalized path to original path mappings
        old_path_map = {normalize_path(p): p for p in old_spec.get("paths", {}).keys()}
        new_path_map = {normalize_path(p): p for p in new_spec.get("paths", {}).keys()}

        # Compare operations for each shared path
        for norm_path in old_paths & new_paths:
            old_orig = old_path_map[norm_path]
            new_orig = new_path_map[norm_path]

            old_methods = set(old_spec["paths"][old_orig].keys())
            new_methods = set(new_spec["paths"][new_orig].keys())

            # Filter out non-HTTP methods like 'parameters'
            http_methods = {"get", "post", "put", "delete", "patch", "head", "options"}
            old_methods = old_methods & http_methods
            new_methods = new_methods & http_methods

            if old_methods != new_methods:
                differences.append(
                    f"Path {norm_path}: methods differ - old={old_methods}, new={new_methods}"
                )

        if differences:
            pytest.fail(f"OpenAPI spec differences:\n  " + "\n  ".join(differences))

    @pytest.mark.migration
    @pytest.mark.xfail(
        reason="Strict parameter type comparison - cosmetic differences don't affect client"
    )
    def test_openapi_functional_equivalence(self):
        """Test that OpenAPI specs are functionally equivalent.

        This test ensures the FastAPI migration maintains API compatibility by checking:
        1. All paths (endpoints) exist in both versions
        2. All operations (methods) exist for each path
        3. All operationIds match
        4. All parameters exist with matching names, types, and required status
        5. Response schemas exist (names may differ due to snake_case vs PascalCase)

        Cosmetic differences that are ignored:
        - Schema naming conventions (snake_case vs PascalCase)
        - Connexion-specific extensions (x-openapi-router-controller, etc.)
        - Parameter metadata (explode, style, example)
        - Info section (version, description, contact)
        - OpenAPI version (3.0 vs 3.1)
        - FastAPI validation responses (422)
        """
        old_resp = requests.get(
            f"{OLD_SERVER}/openapi.json", headers=HEADERS, timeout=30
        )
        new_resp = requests.get(
            f"{NEW_SERVER}/openapi.json", headers=HEADERS, timeout=30
        )

        assert old_resp.status_code == 200, (
            f"Old server OpenAPI returned {old_resp.status_code}"
        )
        assert new_resp.status_code == 200, (
            f"New server OpenAPI returned {new_resp.status_code}"
        )

        old_spec = old_resp.json()
        new_spec = new_resp.json()

        differences = []
        HTTP_METHODS = {"get", "post", "put", "delete", "patch", "head", "options"}

        def normalize_path(p: str) -> str:
            return p.rstrip("/") if p != "/" else p

        # Build normalized path mappings
        old_paths = {normalize_path(p): p for p in old_spec.get("paths", {}).keys()}
        new_paths = {normalize_path(p): p for p in new_spec.get("paths", {}).keys()}

        # 1. Check all paths exist
        missing_paths = set(old_paths.keys()) - set(new_paths.keys())
        extra_paths = set(new_paths.keys()) - set(old_paths.keys())

        if missing_paths:
            differences.append(
                f"CRITICAL: Paths missing in new: {sorted(missing_paths)}"
            )
        if extra_paths:
            differences.append(f"INFO: Extra paths in new: {sorted(extra_paths)}")

        # 2. Check operations for each common path
        for norm_path in sorted(set(old_paths.keys()) & set(new_paths.keys())):
            old_path_item = old_spec["paths"][old_paths[norm_path]]
            new_path_item = new_spec["paths"][new_paths[norm_path]]

            old_methods = set(old_path_item.keys()) & HTTP_METHODS
            new_methods = set(new_path_item.keys()) & HTTP_METHODS

            missing_methods = old_methods - new_methods
            if missing_methods:
                differences.append(
                    f"CRITICAL: {norm_path}: methods missing: {missing_methods}"
                )

            # 3. Check operationId and parameters for each method
            for method in sorted(old_methods & new_methods):
                old_op = old_path_item[method]
                new_op = new_path_item[method]

                # Check operationId
                old_op_id = old_op.get("operationId")
                new_op_id = new_op.get("operationId")
                if old_op_id != new_op_id:
                    differences.append(
                        f"CRITICAL: {norm_path}.{method}: operationId mismatch: "
                        f"{old_op_id} vs {new_op_id}"
                    )

                # Check parameters (by name)
                old_params = {p["name"]: p for p in old_op.get("parameters", [])}
                new_params = {p["name"]: p for p in new_op.get("parameters", [])}

                missing_params = set(old_params.keys()) - set(new_params.keys())
                if missing_params:
                    differences.append(
                        f"CRITICAL: {norm_path}.{method}: parameters missing: {missing_params}"
                    )

                # Check parameter types and required status
                for param_name in sorted(
                    set(old_params.keys()) & set(new_params.keys())
                ):
                    old_p = old_params[param_name]
                    new_p = new_params[param_name]

                    # Check 'in' (path, query, header, cookie)
                    if old_p.get("in") != new_p.get("in"):
                        differences.append(
                            f"CRITICAL: {norm_path}.{method}.{param_name}: "
                            f"'in' mismatch: {old_p.get('in')} vs {new_p.get('in')}"
                        )

                    # Check required status
                    old_required = old_p.get("required", False)
                    new_required = new_p.get("required", False)
                    if old_required != new_required:
                        differences.append(
                            f"WARNING: {norm_path}.{method}.{param_name}: "
                            f"required mismatch: {old_required} vs {new_required}"
                        )

                    # Check type (from schema)
                    old_type = old_p.get("schema", {}).get("type")
                    new_type = new_p.get("schema", {}).get("type")
                    if old_type and new_type and old_type != new_type:
                        differences.append(
                            f"CRITICAL: {norm_path}.{method}.{param_name}: "
                            f"type mismatch: {old_type} vs {new_type}"
                        )

                # Check response codes (ignore 422 which FastAPI adds)
                old_responses = set(old_op.get("responses", {}).keys())
                new_responses = set(new_op.get("responses", {}).keys()) - {"422"}

                missing_responses = old_responses - new_responses
                if missing_responses:
                    differences.append(
                        f"WARNING: {norm_path}.{method}: response codes missing: {missing_responses}"
                    )

        # Filter and report
        critical = [d for d in differences if d.startswith("CRITICAL")]
        warnings = [d for d in differences if d.startswith("WARNING")]
        info = [d for d in differences if d.startswith("INFO")]

        if critical:
            diff_str = "\n  ".join(critical[:20])
            if len(critical) > 20:
                diff_str += f"\n  ... and {len(critical) - 20} more critical issues"
            pytest.fail(f"OpenAPI functional differences:\n  {diff_str}")

        # Log warnings but don't fail
        if warnings:
            logger.warning(f"OpenAPI warnings ({len(warnings)}): {warnings[:5]}")

    @pytest.mark.migration
    def test_openapi_client_compatibility(self):
        """Test OpenAPI spec compatibility with Python client generator.

        This test ensures the FastAPI-generated OpenAPI spec will produce
        a compatible Python client by checking:
        1. Schema names use snake_case (required for backward-compatible model file names)
        2. All key response schemas that the existing client uses are present
        3. All operationIds match (these become method names in the client)
        4. Response $refs use snake_case

        The existing client has models like:
        - graphsense/model/address.py (from schema 'address')
        - graphsense/model/address_tag.py (from schema 'address_tag')
        etc.

        If schema names changed to PascalCase, the generated files would differ.
        """
        old_resp = requests.get(
            f"{OLD_SERVER}/openapi.json", headers=HEADERS, timeout=30
        )
        new_resp = requests.get(
            f"{NEW_SERVER}/openapi.json", headers=HEADERS, timeout=30
        )

        assert old_resp.status_code == 200, (
            f"Old server OpenAPI returned {old_resp.status_code}"
        )
        assert new_resp.status_code == 200, (
            f"New server OpenAPI returned {new_resp.status_code}"
        )

        old_spec = old_resp.json()
        new_spec = new_resp.json()

        differences = []

        # 1. Compare schema names (these become model file names)
        # FastAPI built-ins are OK to differ
        fastapi_builtins = {"HTTPValidationError", "ValidationError"}

        old_schemas = set(old_spec.get("components", {}).get("schemas", {}).keys())
        new_schemas = set(new_spec.get("components", {}).get("schemas", {}).keys())

        # Key schemas that must exist for client compatibility
        # (schemas used in API responses that client code depends on)
        key_schemas = {
            "address",
            "entity",
            "block",
            "tx_utxo",
            "tx_account",
            "address_tags",
            "address_txs",
            "neighbor_addresses",
            "neighbor_entities",
            "entity_addresses",
            "links",
            "search_result",
            "stats",
            "rates",
            "actor",
            "taxonomy",
            "concept",
            "tag_summary",
            "label_summary",
            "token_configs",
            "address_tag",
            "neighbor_address",
            "neighbor_entity",
            "link_utxo",
            "related_address",
            "related_addresses",
            "external_conversion",
            "currency_stats",
            "block_at_date",
            "actor_context",
        }

        missing_key = key_schemas - new_schemas
        if missing_key:
            differences.append(f"Key schemas missing: {sorted(missing_key)}")

        # Check new schemas use snake_case
        for name in new_schemas - fastapi_builtins:
            if any(c.isupper() for c in name):
                differences.append(
                    f"Schema '{name}' uses PascalCase (should be snake_case)"
                )

        # 2. Compare operationIds (these become method names in the API client)
        def get_operation_ids(spec):
            ops = {}
            for path, methods in spec.get("paths", {}).items():
                for method, details in methods.items():
                    if isinstance(details, dict) and "operationId" in details:
                        ops[details["operationId"]] = f"{method.upper()} {path}"
            return ops

        old_ops = get_operation_ids(old_spec)
        new_ops = get_operation_ids(new_spec)

        missing_ops = set(old_ops.keys()) - set(new_ops.keys())
        if missing_ops:
            differences.append(f"operationIds missing: {sorted(missing_ops)}")

        # 3. Verify response refs use snake_case
        def find_pascal_refs(obj, path=""):
            issues = []
            if isinstance(obj, dict):
                if "$ref" in obj:
                    ref = obj["$ref"]
                    if ref.startswith("#/components/schemas/"):
                        schema_name = ref.split("/")[-1]
                        if schema_name not in fastapi_builtins:
                            if any(c.isupper() for c in schema_name):
                                issues.append(f"{path}: {ref}")
                for k, v in obj.items():
                    issues.extend(find_pascal_refs(v, f"{path}.{k}" if path else k))
            elif isinstance(obj, list):
                for i, item in enumerate(obj):
                    issues.extend(find_pascal_refs(item, f"{path}[{i}]"))
            return issues

        pascal_refs = find_pascal_refs(new_spec.get("paths", {}))
        if pascal_refs:
            differences.append(f"PascalCase $refs found: {pascal_refs[:5]}")

        if differences:
            diff_str = "\n  ".join(differences)
            pytest.fail(f"Client compatibility issues:\n  {diff_str}")

    @pytest.mark.migration
    @pytest.mark.xfail(reason="Version differs between master and feature branch")
    def test_stats(self):
        """Test /stats endpoint."""
        self.assert_endpoint_equal("stats")

    @pytest.mark.migration
    def test_search_basic(self):
        """Test /search endpoint."""
        self.assert_endpoint_equal("search?q=binance&limit=5")

    @pytest.mark.migration
    def test_get_address_btc(self):
        """Test BTC address endpoint."""
        self.assert_endpoint_equal(f"btc/addresses/{BTC_ADDRESS}")

    @pytest.mark.migration
    def test_get_address_eth(self):
        """Test ETH address endpoint."""
        self.assert_endpoint_equal(f"eth/addresses/{ETH_ADDRESS}")

    @pytest.mark.migration
    def test_get_entity_btc(self):
        """Test BTC entity endpoint."""
        self.assert_endpoint_equal(f"btc/entities/{BTC_ENTITY}")

    @pytest.mark.migration
    def test_get_block(self):
        """Test block endpoint."""
        self.assert_endpoint_equal(f"btc/blocks/{BTC_HEIGHT}")

    @pytest.mark.migration
    def test_get_tx(self):
        """Test transaction endpoint."""
        self.assert_endpoint_equal(f"btc/txs/{BTC_TX}")

    @pytest.mark.migration
    def test_get_exchange_rates(self):
        """Test exchange rates endpoint."""
        self.assert_endpoint_equal(f"btc/rates/{BTC_HEIGHT}")

    @pytest.mark.migration
    def test_list_address_txs(self):
        """Test address transactions endpoint."""
        self.assert_endpoint_equal(f"btc/addresses/{BTC_ADDRESS}/txs?pagesize=5")

    @pytest.mark.migration
    def test_list_address_neighbors(self):
        """Test address neighbors endpoint."""
        self.assert_endpoint_equal(
            f"btc/addresses/{BTC_ADDRESS}/neighbors?direction=out&pagesize=5"
        )

    @pytest.mark.migration
    def test_list_entity_addresses(self):
        """Test entity addresses endpoint."""
        self.assert_endpoint_equal(f"btc/entities/{BTC_ENTITY}/addresses?pagesize=5")

    @pytest.mark.migration
    def test_list_entity_neighbors(self):
        """Test entity neighbors endpoint."""
        self.assert_endpoint_equal(
            f"btc/entities/{BTC_ENTITY}/neighbors?direction=out&pagesize=5"
        )

    @pytest.mark.migration
    def test_list_tags_by_address(self):
        """Test address tags endpoint."""
        self.assert_endpoint_equal(f"btc/addresses/{BTC_ADDRESS}/tags")

    @pytest.mark.migration
    def test_list_taxonomies(self):
        """Test taxonomies endpoint."""
        self.assert_endpoint_equal("tags/taxonomies")

    @pytest.mark.migration
    def test_supported_tokens(self):
        """Test supported tokens endpoint."""
        self.assert_endpoint_equal("eth/supported_tokens")


class TestAddressObfuscation(MigrationTestBase):
    """Test address/entity endpoints to verify tag obfuscation behavior is consistent.

    These tests verify that tag/actor obfuscation works identically between
    the old aiohttp and new FastAPI implementations for addresses with:
    - Public tags (1Archive1n2C579dMsAu3iC6tWzuQJz8dN - Internet Archive)
    - Potentially private tags (3D4gm7eGSXiEkWS5V3hN9kDVo2eDGBK4eA)

    Tests use endpoints where obfuscation is actually applied:
    - /addresses/{addr}/entity - obfuscates best_address_tag and actors
    - /addresses/{addr}/tag_summary - obfuscates via tag_transformer in service
    - /entities/{entity}/tags - obfuscates private tag labels
    """

    @pytest.mark.migration
    @pytest.mark.parametrize("address", [BTC_ADDRESS, BTC_ADDRESS_PRIVATE_TAGS])
    def test_address_entity_obfuscation(self, address):
        """Test address entity endpoint (obfuscation applies to best_address_tag and actors)."""
        self.assert_endpoint_equal(f"btc/addresses/{address}/entity")

    @pytest.mark.migration
    @pytest.mark.parametrize("address", [BTC_ADDRESS, BTC_ADDRESS_PRIVATE_TAGS])
    def test_tag_summary_obfuscation(self, address):
        """Test tag_summary endpoint with obfuscation.

        This endpoint uses a tag_transformer in the service layer which checks
        should_obfuscate_private_tags(). Without private group headers,
        private tags should be obfuscated.
        """
        self.assert_endpoint_equal(
            f"btc/addresses/{address}/tag_summary?include_best_cluster_tag=true"
        )

    @pytest.mark.migration
    @pytest.mark.parametrize("address", [BTC_ADDRESS, BTC_ADDRESS_PRIVATE_TAGS])
    def test_entity_tags_obfuscation(self, address):
        """Test entity tags endpoint (obfuscation applies to private tags).

        Looks up the entity ID from the address first, then compares entity tags.
        """
        # Get entity ID from address
        data, status, _ = get_response(NEW_SERVER, f"btc/addresses/{address}/entity")
        assert status == 200, f"Failed to get entity for {address}"
        entity_id = data["entity"]

        # Compare entity tags between old and new
        self.assert_endpoint_equal(f"btc/entities/{entity_id}/tags")


class TestSearchParameters(MigrationTestBase):
    """Test search endpoint with various parameter configurations."""

    @pytest.mark.migration
    @pytest.mark.parametrize("currency", [None, "btc", "eth"])
    def test_search_currency_filter(self, currency):
        """Test search with currency filter."""
        if currency:
            self.assert_endpoint_equal(f"search?q=binance&currency={currency}&limit=5")
        else:
            self.assert_endpoint_equal("search?q=binance&limit=5")

    @pytest.mark.migration
    @pytest.mark.parametrize(
        "include_labels,include_actors",
        [
            (True, True),
            (True, False),
            (False, True),
            (False, False),
        ],
    )
    def test_search_include_flags(self, include_labels, include_actors):
        """Test search with include flags."""
        endpoint = f"search?q=binance&limit=5&include_labels={str(include_labels).lower()}&include_actors={str(include_actors).lower()}"
        self.assert_endpoint_equal(endpoint)

    @pytest.mark.migration
    @pytest.mark.parametrize(
        "include_txs,include_addresses",
        [
            (True, True),
            (True, False),
            (False, True),
            (False, False),
        ],
    )
    def test_search_include_results(self, include_txs, include_addresses):
        """Test search with result type flags."""
        endpoint = f"search?q=binance&limit=5&include_txs={str(include_txs).lower()}&include_addresses={str(include_addresses).lower()}"
        self.assert_endpoint_equal(endpoint)


class TestAddressParameters(MigrationTestBase):
    """Test address endpoints with various parameter configurations."""

    @pytest.mark.migration
    @pytest.mark.parametrize("include_actors", [True, False])
    def test_get_address_include_actors(self, include_actors):
        """Test get_address with include_actors parameter."""
        self.assert_endpoint_equal(
            f"btc/addresses/{BTC_ADDRESS}?include_actors={str(include_actors).lower()}"
        )

    @pytest.mark.migration
    @pytest.mark.parametrize("direction", ["in", "out"])
    def test_list_address_txs_direction(self, direction):
        """Test list_address_txs with direction parameter."""
        self.assert_endpoint_equal(
            f"btc/addresses/{BTC_ADDRESS}/txs?direction={direction}&pagesize=5"
        )

    @pytest.mark.migration
    @pytest.mark.parametrize("order", ["asc", "desc"])
    def test_list_address_txs_order(self, order):
        """Test list_address_txs with order parameter."""
        self.assert_endpoint_equal(
            f"btc/addresses/{BTC_ADDRESS}/txs?order={order}&pagesize=5"
        )

    @pytest.mark.migration
    @pytest.mark.parametrize(
        "direction,order",
        [
            ("in", "asc"),
            ("in", "desc"),
            ("out", "asc"),
            ("out", "desc"),
        ],
    )
    def test_list_address_txs_direction_order(self, direction, order):
        """Test list_address_txs with direction and order parameters."""
        self.assert_endpoint_equal(
            f"btc/addresses/{BTC_ADDRESS}/txs?direction={direction}&order={order}&pagesize=5"
        )

    @pytest.mark.migration
    def test_list_address_txs_height_filter(self):
        """Test list_address_txs with height filtering."""
        self.assert_endpoint_equal(
            f"btc/addresses/{BTC_ADDRESS}/txs?min_height=100000&max_height=200000&pagesize=5"
        )

    @pytest.mark.migration
    @pytest.mark.parametrize("direction", ["in", "out"])
    def test_list_address_neighbors_direction(self, direction):
        """Test list_address_neighbors with direction parameter."""
        self.assert_endpoint_equal(
            f"btc/addresses/{BTC_ADDRESS}/neighbors?direction={direction}&pagesize=5"
        )

    @pytest.mark.migration
    @pytest.mark.parametrize("include_labels", [True, False])
    def test_list_address_neighbors_include_labels(self, include_labels):
        """Test list_address_neighbors with include_labels parameter."""
        self.assert_endpoint_equal(
            f"btc/addresses/{BTC_ADDRESS}/neighbors?direction=out&include_labels={str(include_labels).lower()}&pagesize=5"
        )

    @pytest.mark.migration
    @pytest.mark.parametrize("include_actors", [True, False])
    def test_list_address_neighbors_include_actors(self, include_actors):
        """Test list_address_neighbors with include_actors parameter."""
        self.assert_endpoint_equal(
            f"btc/addresses/{BTC_ADDRESS}/neighbors?direction=out&include_actors={str(include_actors).lower()}&pagesize=5"
        )

    @pytest.mark.migration
    @pytest.mark.parametrize("include_best_cluster_tag", [True, False])
    def test_list_tags_by_address_include_cluster_tag(self, include_best_cluster_tag):
        """Test list_tags_by_address with include_best_cluster_tag parameter."""
        self.assert_endpoint_equal(
            f"btc/addresses/{BTC_ADDRESS}/tags?include_best_cluster_tag={str(include_best_cluster_tag).lower()}"
        )

    @pytest.mark.migration
    def test_list_tags_by_address_pagesize(self):
        """Test list_tags_by_address with pagesize parameter."""
        self.assert_endpoint_equal(f"btc/addresses/{BTC_ADDRESS}/tags?pagesize=10")

    @pytest.mark.migration
    @pytest.mark.parametrize("include_best_cluster_tag", [True, False])
    def test_get_tag_summary_by_address(self, include_best_cluster_tag):
        """Test get_tag_summary_by_address with include_best_cluster_tag parameter."""
        self.assert_endpoint_equal(
            f"btc/addresses/{BTC_ADDRESS}/tag_summary?include_best_cluster_tag={str(include_best_cluster_tag).lower()}"
        )

    @pytest.mark.migration
    @pytest.mark.parametrize("include_actors", [True, False])
    def test_get_address_entity_include_actors(self, include_actors):
        """Test get_address_entity with include_actors parameter."""
        self.assert_endpoint_equal(
            f"btc/addresses/{BTC_ADDRESS}/entity?include_actors={str(include_actors).lower()}"
        )


class TestEntityParameters(MigrationTestBase):
    """Test entity endpoints with various parameter configurations."""

    @pytest.mark.migration
    @pytest.mark.parametrize("include_actors", [True, False])
    def test_get_entity_include_actors(self, include_actors):
        """Test get_entity with include_actors parameter."""
        self.assert_endpoint_equal(
            f"btc/entities/{BTC_ENTITY}?include_actors={str(include_actors).lower()}"
        )

    @pytest.mark.migration
    @pytest.mark.parametrize("exclude_best_address_tag", [True, False])
    def test_get_entity_exclude_best_tag(self, exclude_best_address_tag):
        """Test get_entity with exclude_best_address_tag parameter."""
        self.assert_endpoint_equal(
            f"btc/entities/{BTC_ENTITY}?exclude_best_address_tag={str(exclude_best_address_tag).lower()}"
        )

    @pytest.mark.migration
    @pytest.mark.parametrize("direction", ["in", "out"])
    def test_list_entity_neighbors_direction(self, direction):
        """Test list_entity_neighbors with direction parameter."""
        self.assert_endpoint_equal(
            f"btc/entities/{BTC_ENTITY}/neighbors?direction={direction}&pagesize=5"
        )

    @pytest.mark.migration
    @pytest.mark.parametrize("include_labels", [True, False])
    def test_list_entity_neighbors_include_labels(self, include_labels):
        """Test list_entity_neighbors with include_labels parameter."""
        self.assert_endpoint_equal(
            f"btc/entities/{BTC_ENTITY}/neighbors?direction=out&include_labels={str(include_labels).lower()}&pagesize=5"
        )

    @pytest.mark.migration
    @pytest.mark.parametrize("include_actors", [True, False])
    def test_list_entity_neighbors_include_actors(self, include_actors):
        """Test list_entity_neighbors with include_actors parameter."""
        self.assert_endpoint_equal(
            f"btc/entities/{BTC_ENTITY}/neighbors?direction=out&include_actors={str(include_actors).lower()}&pagesize=5"
        )

    @pytest.mark.migration
    @pytest.mark.parametrize("exclude_best_address_tag", [True, False])
    def test_list_entity_neighbors_exclude_best_tag(self, exclude_best_address_tag):
        """Test list_entity_neighbors with exclude_best_address_tag parameter."""
        self.assert_endpoint_equal(
            f"btc/entities/{BTC_ENTITY}/neighbors?direction=out&exclude_best_address_tag={str(exclude_best_address_tag).lower()}&pagesize=5"
        )

    @pytest.mark.migration
    @pytest.mark.parametrize("direction", ["in", "out"])
    def test_list_entity_txs_direction(self, direction):
        """Test list_entity_txs with direction parameter."""
        self.assert_endpoint_equal(
            f"btc/entities/{BTC_ENTITY}/txs?direction={direction}&pagesize=5"
        )

    @pytest.mark.migration
    @pytest.mark.parametrize("order", ["asc", "desc"])
    def test_list_entity_txs_order(self, order):
        """Test list_entity_txs with order parameter."""
        self.assert_endpoint_equal(
            f"btc/entities/{BTC_ENTITY}/txs?order={order}&pagesize=5"
        )

    @pytest.mark.migration
    @pytest.mark.parametrize(
        "direction,order",
        [
            ("in", "asc"),
            ("in", "desc"),
            ("out", "asc"),
            ("out", "desc"),
        ],
    )
    def test_list_entity_txs_direction_order(self, direction, order):
        """Test list_entity_txs with direction and order parameters."""
        self.assert_endpoint_equal(
            f"btc/entities/{BTC_ENTITY}/txs?direction={direction}&order={order}&pagesize=5"
        )

    @pytest.mark.migration
    def test_list_entity_txs_height_filter(self):
        """Test list_entity_txs with height filtering."""
        self.assert_endpoint_equal(
            f"btc/entities/{BTC_ENTITY}/txs?min_height=100000&max_height=200000&pagesize=5"
        )

    @pytest.mark.migration
    def test_list_entity_addresses_pagesize(self):
        """Test list_entity_addresses with pagesize parameter."""
        self.assert_endpoint_equal(f"btc/entities/{BTC_ENTITY}/addresses?pagesize=10")

    @pytest.mark.migration
    @pytest.mark.xfail(
        reason="Pagination without explicit ordering returns different items from database"
    )
    def test_list_address_tags_by_entity_pagesize(self):
        """Test list_address_tags_by_entity with pagesize parameter."""
        self.assert_endpoint_equal(f"btc/entities/{BTC_ENTITY}/tags?pagesize=10")


class TestTransactionParameters(MigrationTestBase):
    """Test transaction endpoints with various parameter configurations."""

    @pytest.mark.migration
    @pytest.mark.parametrize("include_io", [True, False])
    def test_get_tx_include_io(self, include_io):
        """Test get_tx with include_io parameter."""
        self.assert_endpoint_equal(
            f"btc/txs/{BTC_TX}?include_io={str(include_io).lower()}"
        )

    @pytest.mark.migration
    def test_get_tx_include_io_index(self):
        """Test get_tx with include_io and include_io_index parameters."""
        self.assert_endpoint_equal(
            f"btc/txs/{BTC_TX}?include_io=true&include_io_index=true"
        )

    @pytest.mark.migration
    def test_get_tx_include_nonstandard_io(self):
        """Test get_tx with include_io and include_nonstandard_io parameters."""
        self.assert_endpoint_equal(
            f"btc/txs/{BTC_TX}?include_io=true&include_nonstandard_io=true"
        )

    @pytest.mark.migration
    @pytest.mark.parametrize("io", ["inputs", "outputs"])
    def test_get_tx_io(self, io):
        """Test get_tx_io with inputs/outputs parameter."""
        self.assert_endpoint_equal(f"btc/txs/{BTC_TX}/{io}")

    @pytest.mark.migration
    @pytest.mark.parametrize("io", ["inputs", "outputs"])
    def test_get_tx_io_include_io_index(self, io):
        """Test get_tx_io with include_io_index parameter."""
        self.assert_endpoint_equal(f"btc/txs/{BTC_TX}/{io}?include_io_index=true")

    @pytest.mark.migration
    @pytest.mark.parametrize("io", ["inputs", "outputs"])
    def test_get_tx_io_include_nonstandard(self, io):
        """Test get_tx_io with include_nonstandard_io parameter."""
        self.assert_endpoint_equal(f"btc/txs/{BTC_TX}/{io}?include_nonstandard_io=true")


class TestTagsParameters(MigrationTestBase):
    """Test tags endpoints with various parameter configurations."""

    @pytest.mark.migration
    def test_list_taxonomies(self):
        """Test list_taxonomies endpoint."""
        self.assert_endpoint_equal("tags/taxonomies")

    @pytest.mark.migration
    def test_list_concepts(self):
        """Test list_concepts endpoint."""
        self.assert_endpoint_equal("tags/taxonomies/entity/concepts")

    @pytest.mark.migration
    def test_list_address_tags(self):
        """Test list_address_tags endpoint."""
        self.assert_endpoint_equal("tags?label=binance&pagesize=5")

    @pytest.mark.migration
    def test_list_address_tags_pagesize(self):
        """Test list_address_tags with different pagesize."""
        self.assert_endpoint_equal("tags?label=binance&pagesize=10")


class TestBlockParameters(MigrationTestBase):
    """Test block endpoints with various parameter configurations."""

    @pytest.mark.migration
    def test_get_block(self):
        """Test get_block endpoint."""
        self.assert_endpoint_equal(f"btc/blocks/{BTC_HEIGHT}")

    @pytest.mark.migration
    def test_list_block_txs(self):
        """Test list_block_txs endpoint."""
        self.assert_endpoint_equal(f"btc/blocks/{BTC_HEIGHT}/txs")

    @pytest.mark.migration
    def test_get_block_by_date(self):
        """Test get_block_by_date endpoint."""
        # Use ISO 8601 datetime format (required by old Connexion server)
        self.assert_endpoint_equal("btc/block_by_date/2012-01-01T00:00:00Z")


class TestBulkEndpoints(MigrationTestBase):
    """Test bulk endpoints for migration parity."""

    @pytest.mark.migration
    def test_bulk_json_get_address(self):
        """Test bulk JSON get_address endpoint."""
        body = {"address": [BTC_ADDRESS]}
        self.assert_post_endpoint_equal("btc/bulk.json/get_address?num_pages=1", body)

    @pytest.mark.migration
    def test_bulk_json_get_address_multiple(self):
        """Test bulk JSON get_address with multiple addresses."""
        body = {"address": [BTC_ADDRESS, "1BvBMSEYstWetqTFn5Au4m4GFg7xJaNVN2"]}
        self.assert_post_endpoint_equal("btc/bulk.json/get_address?num_pages=1", body)

    @pytest.mark.migration
    def test_bulk_json_get_entity(self):
        """Test bulk JSON get_entity endpoint."""
        body = {"entity": [BTC_ENTITY]}
        self.assert_post_endpoint_equal("btc/bulk.json/get_entity?num_pages=1", body)

    @pytest.mark.migration
    def test_bulk_csv_get_address(self):
        """Test bulk CSV get_address endpoint returns valid CSV."""
        # For CSV, we just check that both servers return the same status
        # since CSV comparison is more complex
        body = {"address": [BTC_ADDRESS]}
        headers = {**HEADERS, "Authorization": "test"}

        old_url = urljoin(OLD_SERVER + "/", "btc/bulk.csv/get_address?num_pages=1")
        new_url = urljoin(NEW_SERVER + "/", "btc/bulk.csv/get_address?num_pages=1")

        old_response = requests.post(old_url, headers=headers, json=body, timeout=60)
        new_response = requests.post(new_url, headers=headers, json=body, timeout=60)

        assert old_response.status_code == new_response.status_code, (
            f"Status mismatch: {old_response.status_code} vs {new_response.status_code}"
        )


class TestMultiCurrency(MigrationTestBase):
    """Test endpoints across multiple currencies."""

    @pytest.mark.migration
    @pytest.mark.parametrize(
        "currency,address",
        [
            ("btc", BTC_ADDRESS),
            ("eth", ETH_ADDRESS),
        ],
    )
    def test_get_address_multi_currency(self, currency, address):
        """Test get_address across different currencies."""
        self.assert_endpoint_equal(f"{currency}/addresses/{address}")

    @pytest.mark.migration
    @pytest.mark.parametrize("currency", ["btc", "eth"])
    def test_supported_tokens_multi_currency(self, currency):
        """Test supported_tokens across different currencies."""
        self.assert_endpoint_equal(f"{currency}/supported_tokens")


class TestPagination(MigrationTestBase):
    """Test pagination parameters."""

    @pytest.mark.migration
    @pytest.mark.parametrize("pagesize", [1, 5, 10, 25])
    def test_list_address_txs_pagesize(self, pagesize):
        """Test list_address_txs with various pagesize values."""
        self.assert_endpoint_equal(
            f"btc/addresses/{BTC_ADDRESS}/txs?pagesize={pagesize}"
        )

    @pytest.mark.migration
    @pytest.mark.parametrize("pagesize", [1, 5, 10])
    def test_list_address_neighbors_pagesize(self, pagesize):
        """Test list_address_neighbors with various pagesize values."""
        self.assert_endpoint_equal(
            f"btc/addresses/{BTC_ADDRESS}/neighbors?direction=out&pagesize={pagesize}"
        )

    @pytest.mark.migration
    @pytest.mark.parametrize("pagesize", [1, 5, 10])
    def test_list_entity_neighbors_pagesize(self, pagesize):
        """Test list_entity_neighbors with various pagesize values."""
        self.assert_endpoint_equal(
            f"btc/entities/{BTC_ENTITY}/neighbors?direction=out&pagesize={pagesize}"
        )


class TestErrorResponses(MigrationTestBase):
    """Test error response handling matches between versions."""

    @pytest.mark.migration
    def test_invalid_currency(self):
        """Test 404 for invalid currency."""
        self.assert_endpoint_equal("invalid_currency/addresses/test")

    @pytest.mark.migration
    def test_invalid_address(self):
        """Test response for non-existent address."""
        self.assert_endpoint_equal("btc/addresses/1InvalidAddressThatDoesNotExist123")

    @pytest.mark.migration
    @pytest.mark.xfail(
        reason="Both return 500 for large entity IDs - Cassandra limitation"
    )
    def test_invalid_entity(self):
        """Test response for non-existent entity."""
        self.assert_endpoint_equal("btc/entities/999999999999")

    @pytest.mark.migration
    def test_invalid_tx_hash(self):
        """Test response for invalid transaction hash."""
        self.assert_endpoint_equal(
            "btc/txs/0000000000000000000000000000000000000000000000000000000000000000"
        )

    @pytest.mark.migration
    def test_invalid_block_height(self):
        """Test response for non-existent block height."""
        self.assert_endpoint_equal("btc/blocks/999999999")

    @pytest.mark.migration
    @pytest.mark.xfail(
        reason="FastAPI returns 422 for validation errors, Connexion returns 400"
    )
    def test_missing_required_param_direction(self):
        """Test response when required 'direction' param is missing."""
        self.assert_endpoint_equal(f"btc/addresses/{BTC_ADDRESS}/neighbors")

    @pytest.mark.migration
    @pytest.mark.xfail(
        reason="FastAPI returns 422 for validation errors, Connexion returns 400"
    )
    def test_invalid_direction_param(self):
        """Test response for invalid direction value."""
        self.assert_endpoint_equal(
            f"btc/addresses/{BTC_ADDRESS}/neighbors?direction=invalid"
        )

    @pytest.mark.migration
    @pytest.mark.xfail(
        reason="FastAPI returns 422 for validation errors, Connexion returns 400"
    )
    def test_invalid_pagesize_negative(self):
        """Test response for negative pagesize."""
        self.assert_endpoint_equal(f"btc/addresses/{BTC_ADDRESS}/txs?pagesize=-1")

    @pytest.mark.migration
    @pytest.mark.xfail(
        reason="FastAPI returns 422 for validation errors, Connexion returns 400"
    )
    def test_invalid_pagesize_zero(self):
        """Test response for zero pagesize."""
        self.assert_endpoint_equal(f"btc/addresses/{BTC_ADDRESS}/txs?pagesize=0")

    @pytest.mark.migration
    def test_invalid_pagesize_too_large(self):
        """Test response for excessively large pagesize."""
        self.assert_endpoint_equal(f"btc/addresses/{BTC_ADDRESS}/txs?pagesize=10000")

    @pytest.mark.migration
    @pytest.mark.xfail(
        reason="FastAPI returns 404 for path validation, Connexion returns 400"
    )
    def test_invalid_height_negative(self):
        """Test response for negative block height."""
        self.assert_endpoint_equal("btc/blocks/-1")

    @pytest.mark.migration
    @pytest.mark.xfail(
        reason="FastAPI returns 422 for validation errors, Connexion returns 400"
    )
    def test_search_empty_query(self):
        """Test search with empty query."""
        self.assert_endpoint_equal("search?q=")

    @pytest.mark.migration
    @pytest.mark.xfail(
        reason="FastAPI returns 422 for validation errors, Connexion returns 400"
    )
    def test_search_single_char(self):
        """Test search with single character (might be invalid)."""
        self.assert_endpoint_equal("search?q=a")


class TestETHSpecificEndpoints(MigrationTestBase):
    """Test Ethereum/account-model specific endpoints."""

    @pytest.mark.migration
    def test_eth_address_txs(self):
        """Test ETH address transactions."""
        self.assert_endpoint_equal(f"eth/addresses/{ETH_ADDRESS}/txs?pagesize=5")

    @pytest.mark.migration
    def test_eth_address_neighbors_in(self):
        """Test ETH address incoming neighbors."""
        self.assert_endpoint_equal(
            f"eth/addresses/{ETH_ADDRESS}/neighbors?direction=in&pagesize=5"
        )

    @pytest.mark.migration
    def test_eth_address_neighbors_out(self):
        """Test ETH address outgoing neighbors."""
        self.assert_endpoint_equal(
            f"eth/addresses/{ETH_ADDRESS}/neighbors?direction=out&pagesize=5"
        )

    @pytest.mark.migration
    def test_eth_address_entity(self):
        """Test ETH address entity lookup."""
        self.assert_endpoint_equal(f"eth/addresses/{ETH_ADDRESS}/entity")

    @pytest.mark.migration
    def test_eth_address_tags(self):
        """Test ETH address tags."""
        self.assert_endpoint_equal(f"eth/addresses/{ETH_ADDRESS}/tags")

    @pytest.mark.migration
    @pytest.mark.xfail(
        reason="List ordering differs for concepts/sources in tag_summary"
    )
    def test_eth_address_tag_summary(self):
        """Test ETH address tag summary."""
        self.assert_endpoint_equal(f"eth/addresses/{ETH_ADDRESS}/tag_summary")

    @pytest.mark.migration
    def test_eth_tx_with_flows(self):
        """Test ETH transaction with flows."""
        # Use a known ETH transaction
        eth_tx = "0xc55e2b90168af6972193c1f86fa4d7d7b31a29c156665d15b9cd48618b5177ef"
        self.assert_endpoint_equal(f"eth/txs/{eth_tx}")

    @pytest.mark.migration
    def test_eth_tx_flows(self):
        """Test ETH transaction flows endpoint."""
        eth_tx = "0xc55e2b90168af6972193c1f86fa4d7d7b31a29c156665d15b9cd48618b5177ef"
        self.assert_endpoint_equal(f"eth/txs/{eth_tx}/flows")

    @pytest.mark.migration
    def test_eth_supported_tokens(self):
        """Test ETH supported tokens with pagination."""
        self.assert_endpoint_equal("eth/supported_tokens?pagesize=10")


class TestLinksEndpoints(MigrationTestBase):
    """Test links endpoints between addresses and entities."""

    @pytest.mark.migration
    def test_address_links(self):
        """Test address links endpoint."""
        # First get a neighbor to use for links query
        neighbor = (
            "1HQ3Go3ggs8pFnXuHVHRytPCq5fGG8Hbhx"  # Known neighbor of archive address
        )
        self.assert_endpoint_equal(
            f"btc/addresses/{BTC_ADDRESS}/links?neighbor={neighbor}"
        )

    @pytest.mark.migration
    def test_entity_links(self):
        """Test entity links endpoint."""
        # Use a known neighbor entity
        neighbor_entity = 17642138
        self.assert_endpoint_equal(
            f"btc/entities/{BTC_ENTITY}/links?neighbor={neighbor_entity}"
        )


class TestActorEndpoints(MigrationTestBase):
    """Test actor/attribution endpoints."""

    @pytest.mark.migration
    def test_get_actor(self):
        """Test get actor by ID."""
        self.assert_endpoint_equal("tags/actors/binance")

    @pytest.mark.migration
    def test_get_actor_tags(self):
        """Test get actor tags."""
        self.assert_endpoint_equal("tags/actors/binance/tags?pagesize=5")

    @pytest.mark.migration
    def test_list_concepts_abuse(self):
        """Test list concepts for abuse taxonomy."""
        self.assert_endpoint_equal("tags/taxonomies/abuse/concepts")

    @pytest.mark.migration
    def test_list_concepts_confidence(self):
        """Test list concepts for confidence taxonomy."""
        self.assert_endpoint_equal("tags/taxonomies/confidence/concepts")


class TestDateFiltering(MigrationTestBase):
    """Test date filtering on various endpoints."""

    @pytest.mark.migration
    def test_address_txs_date_filter(self):
        """Test address transactions with date filtering."""
        self.assert_endpoint_equal(
            f"btc/addresses/{BTC_ADDRESS}/txs?min_date=2015-01-01T00:00:00Z&max_date=2020-01-01T00:00:00Z&pagesize=5"
        )

    @pytest.mark.migration
    def test_entity_txs_date_filter(self):
        """Test entity transactions with date filtering."""
        self.assert_endpoint_equal(
            f"btc/entities/{BTC_ENTITY}/txs?min_date=2015-01-01T00:00:00Z&max_date=2020-01-01T00:00:00Z&pagesize=5"
        )

    @pytest.mark.migration
    def test_address_links_date_filter(self):
        """Test address links with date filtering."""
        neighbor = "1HQ3Go3ggs8pFnXuHVHRytPCq5fGG8Hbhx"
        self.assert_endpoint_equal(
            f"btc/addresses/{BTC_ADDRESS}/links?neighbor={neighbor}&min_date=2015-01-01T00:00:00Z"
        )

    @pytest.mark.migration
    def test_block_by_date_various(self):
        """Test block by date with various dates."""
        self.assert_endpoint_equal("btc/block_by_date/2015-06-15T12:30:00Z")

    @pytest.mark.migration
    def test_block_by_date_early(self):
        """Test block by date for early Bitcoin history."""
        self.assert_endpoint_equal("btc/block_by_date/2010-01-01T00:00:00Z")


class TestEdgeCases(MigrationTestBase):
    """Test edge cases and boundary conditions."""

    @pytest.mark.migration
    def test_address_with_no_txs(self):
        """Test address that might have no transactions."""
        # Use an address with minimal activity
        self.assert_endpoint_equal("btc/addresses/1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa")

    @pytest.mark.migration
    def test_genesis_block(self):
        """Test genesis block (height 0)."""
        self.assert_endpoint_equal("btc/blocks/0")

    @pytest.mark.migration
    def test_genesis_block_txs(self):
        """Test genesis block transactions."""
        self.assert_endpoint_equal("btc/blocks/0/txs")

    @pytest.mark.migration
    def test_coinbase_tx(self):
        """Test coinbase transaction (genesis)."""
        genesis_tx = "4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b"
        self.assert_endpoint_equal(f"btc/txs/{genesis_tx}?include_io=true")

    @pytest.mark.migration
    def test_search_special_characters(self):
        """Test search with special characters."""
        self.assert_endpoint_equal("search?q=test%20space&limit=5")

    @pytest.mark.migration
    def test_search_unicode(self):
        """Test search with unicode characters."""
        self.assert_endpoint_equal("search?q=test%C3%A9&limit=5")

    @pytest.mark.migration
    def test_empty_result_search(self):
        """Test search that returns no results."""
        self.assert_endpoint_equal("search?q=xyznonexistent12345&limit=5")

    @pytest.mark.migration
    def test_large_entity(self):
        """Test a large entity (exchange)."""
        # Binance hot wallet entity
        self.assert_endpoint_equal("btc/entities/109578")

    @pytest.mark.migration
    def test_pagesize_one(self):
        """Test minimum pagesize of 1."""
        self.assert_endpoint_equal(f"btc/addresses/{BTC_ADDRESS}/txs?pagesize=1")

    @pytest.mark.migration
    def test_order_combinations(self):
        """Test order parameter explicitly set."""
        self.assert_endpoint_equal(
            f"btc/addresses/{BTC_ADDRESS}/txs?order=asc&pagesize=3"
        )


class TestTokenTransactions(MigrationTestBase):
    """Test token-specific transaction endpoints."""

    @pytest.mark.migration
    def test_eth_address_txs_token_filter(self):
        """Test ETH address transactions with token filter."""
        self.assert_endpoint_equal(
            f"eth/addresses/{ETH_ADDRESS}/txs?token_currency=usdt&pagesize=5"
        )

    @pytest.mark.migration
    def test_eth_tx_flows_token_filter(self):
        """Test ETH transaction flows with token filter."""
        eth_tx = "0xc55e2b90168af6972193c1f86fa4d7d7b31a29c156665d15b9cd48618b5177ef"
        self.assert_endpoint_equal(
            f"eth/txs/{eth_tx}/flows?token_currency=eth&pagesize=5"
        )


class TestBulkEndpointsExtended(MigrationTestBase):
    """Extended bulk endpoint tests."""

    @pytest.mark.migration
    def test_bulk_get_address_with_tags(self):
        """Test bulk get_address_with_tags."""
        body = {"address": [BTC_ADDRESS]}
        self.assert_post_endpoint_equal(
            "btc/bulk.json/get_address_with_tags?num_pages=1", body
        )

    @pytest.mark.migration
    def test_bulk_list_address_txs(self):
        """Test bulk list_address_txs."""
        body = {"address": [BTC_ADDRESS]}
        self.assert_post_endpoint_equal(
            "btc/bulk.json/list_address_txs?num_pages=1", body
        )

    @pytest.mark.migration
    def test_bulk_list_address_neighbors(self):
        """Test bulk list_address_neighbors."""
        body = {"address": [BTC_ADDRESS], "direction": "out"}
        self.assert_post_endpoint_equal(
            "btc/bulk.json/list_address_neighbors?num_pages=1", body
        )

    @pytest.mark.migration
    def test_bulk_get_entity_with_tags(self):
        """Test bulk get entity with tags."""
        body = {"entity": [BTC_ENTITY]}
        self.assert_post_endpoint_equal("btc/bulk.json/get_entity?num_pages=1", body)


class TestSpentInSpending(MigrationTestBase):
    """Test UTXO-specific spent_in and spending endpoints."""

    @pytest.mark.migration
    def test_tx_spent_in(self):
        """Test transaction spent_in endpoint."""
        # Use a transaction that has spent outputs
        self.assert_endpoint_equal(f"btc/txs/{BTC_TX}/spent_in")

    @pytest.mark.migration
    def test_tx_spending(self):
        """Test transaction spending endpoint."""
        self.assert_endpoint_equal(f"btc/txs/{BTC_TX}/spending")

    @pytest.mark.migration
    def test_tx_spent_in_with_index(self):
        """Test transaction spent_in with specific output index."""
        self.assert_endpoint_equal(f"btc/txs/{BTC_TX}/spent_in?io_index=0")

    @pytest.mark.migration
    def test_tx_spending_with_index(self):
        """Test transaction spending with specific input index."""
        self.assert_endpoint_equal(f"btc/txs/{BTC_TX}/spending?io_index=0")


class TestExchangeRates(MigrationTestBase):
    """Test exchange rate endpoints."""

    @pytest.mark.migration
    def test_rates_early_block(self):
        """Test exchange rates for early block."""
        self.assert_endpoint_equal("btc/rates/100")

    @pytest.mark.migration
    def test_rates_recent_block(self):
        """Test exchange rates for more recent block."""
        self.assert_endpoint_equal("btc/rates/500000")

    @pytest.mark.migration
    def test_rates_eth(self):
        """Test exchange rates for ETH."""
        self.assert_endpoint_equal("eth/rates/10000000")


class TestRelatedAddresses(MigrationTestBase):
    """Test related addresses endpoint."""

    @pytest.mark.migration
    def test_related_addresses(self):
        """Test related addresses (pubkey derived)."""
        self.assert_endpoint_equal(f"btc/addresses/{BTC_ADDRESS}/related_addresses")

    @pytest.mark.migration
    def test_related_addresses_with_pagesize(self):
        """Test related addresses with pagesize."""
        self.assert_endpoint_equal(
            f"btc/addresses/{BTC_ADDRESS}/related_addresses?pagesize=5"
        )


class TestMultipleCurrencies(MigrationTestBase):
    """Test endpoints across all supported currencies."""

    @pytest.mark.migration
    @pytest.mark.parametrize("currency", ["btc", "bch", "ltc", "zec"])
    def test_supported_tokens_utxo(self, currency):
        """Test supported tokens for UTXO chains."""
        self.assert_endpoint_equal(f"{currency}/supported_tokens")

    @pytest.mark.migration
    def test_trx_supported_tokens(self):
        """Test TRX supported tokens."""
        self.assert_endpoint_equal("trx/supported_tokens")


class TestOnlyIdsFilter(MigrationTestBase):
    """Test only_ids filter parameter."""

    @pytest.mark.migration
    def test_address_neighbors_only_ids(self):
        """Test address neighbors with only_ids filter."""
        neighbor = "1HQ3Go3ggs8pFnXuHVHRytPCq5fGG8Hbhx"
        self.assert_endpoint_equal(
            f"btc/addresses/{BTC_ADDRESS}/neighbors?direction=out&only_ids={neighbor}"
        )

    @pytest.mark.migration
    def test_entity_neighbors_only_ids(self):
        """Test entity neighbors with only_ids filter."""
        neighbor_entity = 17642138
        self.assert_endpoint_equal(
            f"btc/entities/{BTC_ENTITY}/neighbors?direction=out&only_ids={neighbor_entity}"
        )


class TestConversions(MigrationTestBase):
    """Test DeFi conversion endpoints (from regression tests)."""

    @pytest.mark.migration
    def test_eth_dex_swap_conversion(self):
        """Test ETH DEX swap conversion."""
        tx = "0x76f4263391a7d72f66cb1f254e8643e37ca739ab2859b9e9cd5b5bda3194332b"
        self.assert_endpoint_equal(f"eth/txs/{tx}/conversions")

    @pytest.mark.migration
    def test_eth_bridge_conversion(self):
        """Test ETH bridge conversion (eth to btc)."""
        tx = "0x6D65123E246D752DE3F39E0FDF5B788BAAD35A29B7E95B74C714E6C7C1EA61DD"
        self.assert_endpoint_equal(f"eth/txs/{tx}/conversions")

    @pytest.mark.migration
    def test_eth_to_token_conversion(self):
        """Test ETH to token conversion."""
        tx = "0x42D529A72CECD6ECE546D5AC0D2A6C2A9407876B66478A33917D8928833433F8"
        self.assert_endpoint_equal(f"eth/txs/{tx}/conversions")

    @pytest.mark.migration
    def test_eth_thorchain_conversion(self):
        """Test ETH to BTC via Thorchain."""
        tx = "0x16ed29f9bf9914ea3b62e4e94829eaef10118d04e82849a285ef8a5700defa1a"
        self.assert_endpoint_equal(f"eth/txs/{tx}/conversions")


class TestLinksExtended(MigrationTestBase):
    """Extended link tests (from regression tests)."""

    @pytest.mark.migration
    def test_eth_entity_links_with_pagination(self):
        """Test ETH entity links with pagination."""
        self.assert_endpoint_equal(
            "eth/entities/316592288/links?neighbor=31455019&pagesize=100"
        )

    @pytest.mark.migration
    def test_eth_address_links_with_pagination(self):
        """Test ETH address links with pagination."""
        self.assert_endpoint_equal(
            "eth/addresses/0x8ccec5bfb049af5dd2916853a14974b0a9f47e4d/links?neighbor=0x453290aaf6dca3cee4325bad3f52b1346b6213a7&pagesize=100"
        )

    @pytest.mark.migration
    def test_btc_entity_links_small_pagesize(self):
        """Test BTC entity links with small pagesize (tests cutoff)."""
        self.assert_endpoint_equal(
            "btc/entities/2647118/links?neighbor=109578&pagesize=1"
        )

    @pytest.mark.migration
    def test_btc_address_links_with_order(self):
        """Test BTC address links with order parameter."""
        self.assert_endpoint_equal(
            "btc/addresses/bc1qm34lsc65zpw79lxes69zkqmk6ee3ewf0j77s3h/links?neighbor=bc1qc82pdh5zy8kk6gc0t0kjpggu9pg80zewsmy4ac&order=desc&pagesize=100"
        )


class TestTRXEndpoints(MigrationTestBase):
    """Test Tron-specific endpoints."""

    # Known TRX address (Tether)
    TRX_ADDRESS = "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"

    @pytest.mark.migration
    def test_trx_address(self):
        """Test TRX address endpoint."""
        self.assert_endpoint_equal(f"trx/addresses/{self.TRX_ADDRESS}")

    @pytest.mark.migration
    def test_trx_address_txs(self):
        """Test TRX address transactions."""
        self.assert_endpoint_equal(f"trx/addresses/{self.TRX_ADDRESS}/txs?pagesize=5")

    @pytest.mark.migration
    def test_trx_address_neighbors(self):
        """Test TRX address neighbors."""
        self.assert_endpoint_equal(
            f"trx/addresses/{self.TRX_ADDRESS}/neighbors?direction=out&pagesize=5"
        )

    @pytest.mark.migration
    def test_trx_address_links_with_pagination(self):
        """Test TRX address links with pagination (from regression tests)."""
        self.assert_endpoint_equal(
            "trx/addresses/TCz47XgC9TjCeF4UzfB6qZbM9LTF9s1tG7/links?neighbor=TT8oWoMeoziArGXsPej6EYF5TN4WSUhvfu&order=desc&pagesize=2"
        )


class TestSearchExtended(MigrationTestBase):
    """Extended search tests (from regression tests)."""

    @pytest.mark.migration
    def test_search_btc_prefix(self):
        """Test search with BTC address prefix."""
        self.assert_endpoint_equal("search?q=bc1qasd&limit=100&currency=btc")

    @pytest.mark.migration
    def test_search_eth_prefix(self):
        """Test search with ETH address prefix."""
        self.assert_endpoint_equal("search?q=0x00000&limit=100")

    @pytest.mark.migration
    def test_search_trx_prefix(self):
        """Test search with TRX address prefix."""
        self.assert_endpoint_equal("search?q=TCxZGE&limit=100")

    @pytest.mark.migration
    def test_search_overflow_check(self):
        """Test search with potentially overflowing hex."""
        self.assert_endpoint_equal("search?q=0xfffff")

    @pytest.mark.migration
    def test_search_no_results(self):
        """Test search with query that returns no results."""
        self.assert_endpoint_equal("search?q=0xfffff0193483022348723")


class TestTxsListExtended(MigrationTestBase):
    """Extended transaction list tests (from regression tests)."""

    @pytest.mark.migration
    def test_eth_address_txs_with_height_filter(self):
        """Test ETH address transactions with height filter."""
        self.assert_endpoint_equal(
            "eth/addresses/0x10c318b1d817396a8a66016438ac9dfb615ffcf1/txs?pagesize=100&min_height=7957441&order=desc"
        )

    @pytest.mark.migration
    def test_eth_large_address_txs(self):
        """Test ETH large address (Tether) transactions with filters."""
        self.assert_endpoint_equal(
            f"eth/addresses/{ETH_ADDRESS}/txs?min_height=20698064&max_height=22567324&order=asc&pagesize=5"
        )


class TestRouteConventions:
    """Tests for route definition conventions (no running servers needed)."""

    def test_no_trailing_slashes_in_routes(self):
        """Ensure no routes have trailing slashes (causes 307 redirects)."""
        from graphsenselib.web.routes import (
            addresses,
            blocks,
            bulk,
            entities,
            general,
            rates,
            tags,
            tokens,
            txs,
        )

        routers = [
            ("addresses", addresses.router),
            ("blocks", blocks.router),
            ("bulk", bulk.router),
            ("entities", entities.router),
            ("general", general.router),
            ("rates", rates.router),
            ("tags", tags.router),
            ("tokens", tokens.router),
            ("txs", txs.router),
        ]

        violations = []
        for module_name, router in routers:
            for route in router.routes:
                path = getattr(route, "path", "")
                # Check for trailing slash (but "/" alone is fine)
                if path != "/" and path.endswith("/"):
                    violations.append(f"{module_name}: {path}")

        if violations:
            msg = "Routes with trailing slashes found (causes 307 redirects):\n"
            msg += "\n".join(f"  - {v}" for v in violations)
            assert False, msg


if __name__ == "__main__":
    # Quick manual test
    check_servers_available()
    test = TestFastAPIMigrationBasic()
    test.setup()

    endpoints = [
        "stats",
        "search?q=binance&limit=5",
        f"btc/addresses/{BTC_ADDRESS}",
        f"btc/entities/{BTC_ENTITY}",
        f"btc/blocks/{BTC_HEIGHT}",
    ]

    for endpoint in endpoints:
        result = test.compare_endpoint(endpoint)
        status = "PASS" if not result["differences"] else "FAIL"
        print(
            f"{status}: {endpoint} (old={result['old_time']:.3f}s, new={result['new_time']:.3f}s)"
        )
        if result["differences"]:
            for diff in result["differences"][:5]:
                print(f"  - {diff}")
