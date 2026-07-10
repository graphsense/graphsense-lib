"""
Manual regression tests: baseline container vs current code.

These are hand-written regression tests for specific edge cases and bug fixes.
They use the baseline Docker container as reference (previous stable release).

Each endpoint gets its own test method for better granularity and easier
debugging when regressions occur.

For auto-generated regression tests, see test_baseline_regression.py.
For Loki log-derived tests, see test_loki_generated.py.

Usage:
    # Run all manual regression tests
    make test-rest-manual

    # Run with specific baseline version
    BASELINE_VERSION=v25.11.16 make test-rest-manual
"""

import json
import logging
import os
import time
from typing import Any, Dict
from urllib.parse import urljoin

import pytest
import requests

logger = logging.getLogger(__name__)

# Server URLs from environment
CURRENT_SERVER = os.environ.get("CURRENT_SERVER", "http://localhost:9000")
BASELINE_SERVER = os.environ.get("BASELINE_SERVER", "http://localhost:9001")

HEADERS = {"Content-Type": "application/json", "Accept": "application/json"}

AUTHENTICATED_HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json",
}

# Per-server API keys, needed once BASELINE_SERVER points at a deployment whose
# gateway validates them (e.g. https://api.iknaio.com). See test_baseline_regression.
CURRENT_AUTH = os.environ.get("CURRENT_AUTH")
BASELINE_AUTH = os.environ.get("BASELINE_AUTH")

# Extra per-server headers (JSON), to replay what an API gateway injects in front
# of the baseline. See test_baseline_regression for the rationale.
CURRENT_HEADERS = json.loads(os.environ.get("CURRENT_HEADERS") or "{}")
BASELINE_HEADERS = json.loads(os.environ.get("BASELINE_HEADERS") or "{}")


def headers_for(base_url: str, authenticated: bool) -> Dict[str, str]:
    """Headers for a request, carrying the API key of the server being addressed.

    Anonymous requests omit the key on purpose — the tag-obfuscation tests depend
    on it — and are only ever sent to the current server, which is not fronted by
    a validating gateway.
    """
    request_headers = dict(AUTHENTICATED_HEADERS if authenticated else HEADERS)
    if authenticated:
        key = BASELINE_AUTH if base_url == BASELINE_SERVER else CURRENT_AUTH
        if key:
            request_headers["Authorization"] = key
        request_headers.update(
            BASELINE_HEADERS if base_url == BASELINE_SERVER else CURRENT_HEADERS
        )
    return request_headers


def get_data_from_endpoint(
    base_url: str, endpoint: str, authenticated: bool = True
) -> tuple[Dict[str, Any], float]:
    """Get data from an endpoint."""
    now = time.time()
    url = urljoin(base_url + "/", endpoint.lstrip("/"))
    response = requests.get(url, headers=headers_for(base_url, authenticated))
    response.raise_for_status()
    elapsed = time.time() - now
    return response.json(), elapsed


def compare_outputs(
    baseline_data: Dict[str, Any], current_data: Dict[str, Any]
) -> Dict[str, Any]:
    """Compare outputs with detailed differences."""
    if isinstance(baseline_data, list):
        baseline_data = {"items": baseline_data}
    if isinstance(current_data, list):
        current_data = {"items": current_data}

    result = {
        "are_equal": False,
        "differences": [],
        "baseline_keys": list(baseline_data.keys()),
        "current_keys": list(current_data.keys()),
    }

    missing_in_current = set(result["baseline_keys"]) - set(result["current_keys"])
    missing_in_baseline = set(result["current_keys"]) - set(result["baseline_keys"])

    if missing_in_current:
        result["differences"].append(
            f"Keys missing in current: {list(missing_in_current)}"
        )
    if missing_in_baseline:
        result["differences"].append(
            f"Keys missing in baseline: {list(missing_in_baseline)}"
        )

    common_keys = set(result["baseline_keys"]) & set(result["current_keys"])
    for key in common_keys:
        if baseline_data[key] != current_data[key]:
            if isinstance(baseline_data[key], list) and isinstance(
                current_data[key], list
            ):
                if len(baseline_data[key]) != len(current_data[key]):
                    result["differences"].append(
                        f"List length difference in '{key}': "
                        f"{len(baseline_data[key])} != {len(current_data[key])}"
                    )
                else:
                    for i, (baseline_item, current_item) in enumerate(
                        zip(baseline_data[key][:5], current_data[key][:5])
                    ):
                        if baseline_item != current_item:
                            result["differences"].append(
                                f"First difference in '{key}' at index {i}: "
                                f"{baseline_item} != {current_item}"
                            )
                            break
            else:
                result["differences"].append(
                    f"Difference in '{key}': {baseline_data[key]} != {current_data[key]}"
                )

    result["are_equal"] = len(result["differences"]) == 0
    return result


class ManualRegressionTestBase:
    """Base class for manual regression tests using baseline container."""

    baseline_url: str = BASELINE_SERVER
    current_url: str = CURRENT_SERVER

    def compare_instances(self, call: str) -> Dict[str, Any]:
        """Compare outputs from baseline and current servers."""
        current_data, current_time = get_data_from_endpoint(self.current_url, call)
        baseline_data, baseline_time = get_data_from_endpoint(self.baseline_url, call)
        comparison = compare_outputs(baseline_data, current_data)

        speedup = baseline_time / current_time if current_time > 0 else float("inf")
        logger.info(
            f"Call: {call} | Speed: baseline={baseline_time:.2f}s, "
            f"current={current_time:.2f}s, speedup={speedup:.1f}x"
        )

        return comparison

    def assert_call_equal(self, call: str):
        """Assert a single call returns equal results."""
        logger.info(f"Testing call: {call}")
        comparison = self.compare_instances(call)
        if not comparison["are_equal"]:
            diff_details = "\n".join(comparison["differences"])
            pytest.fail(
                f"Outputs differ for call: {call}\n\nDetailed differences:\n{diff_details}"
            )


# =============================================================================
# Conversion endpoint tests (DeFi swaps, bridges, atomic swaps)
# =============================================================================


class TestManualRegressionConversions(ManualRegressionTestBase):
    """Manual regression tests for conversion endpoints."""

    @pytest.mark.regression
    def test_conversions_dex_swap(self):
        """DEX swap conversion."""
        self.assert_call_equal(
            "eth/txs/0x76f4263391a7d72f66cb1f254e8643e37ca739ab2859b9e9cd5b5bda3194332b/conversions"
        )

    @pytest.mark.regression
    def test_conversions_bridge_eth_to_btc(self):
        """Bridge ETH to BTC conversion."""
        self.assert_call_equal(
            "eth/txs/0x6D65123E246D752DE3F39E0FDF5B788BAAD35A29B7E95B74C714E6C7C1EA61DD/conversions"
        )

    @pytest.mark.regression
    def test_conversions_atomic_swap_send(self):
        """Atomic swap send conversion."""
        self.assert_call_equal(
            "eth/txs/0xD49764B134172947F5226038CD80C8068B76A1F785C6696A34ECDCF29C4D3C78/conversions"
        )

    @pytest.mark.regression
    def test_conversions_atomic_swap_receive(self):
        """Atomic swap receive conversion."""
        self.assert_call_equal(
            "eth/txs/0x5DA7B9D34173E74F1594726B718D0759D5877E14C3CAA8612838A109C196EEEE/conversions"
        )

    @pytest.mark.regression
    def test_conversions_atomic_swap_refund(self):
        """Atomic swap refund conversion."""
        self.assert_call_equal(
            "eth/txs/0x9BD32A4E5B2BC63E858A04F2B8050A8168FBB6CA57A846769348BB1C1B862836/conversions"
        )

    @pytest.mark.regression
    def test_conversions_eth_to_token(self):
        """ETH to token conversion."""
        self.assert_call_equal(
            "eth/txs/0x42D529A72CECD6ECE546D5AC0D2A6C2A9407876B66478A33917D8928833433F8/conversions"
        )

    @pytest.mark.regression
    def test_conversions_eth_to_btc_thorchain(self):
        """ETH to BTC via Thorchain conversion."""
        self.assert_call_equal(
            "eth/txs/0x16ed29f9bf9914ea3b62e4e94829eaef10118d04e82849a285ef8a5700defa1a/conversions"
        )

    @pytest.mark.regression
    def test_conversions_thorchain_second_endpoint(self):
        """Thorchain second endpoint conversion."""
        self.assert_call_equal(
            "eth/txs/ce09a43f14088aa5ab1e2366609678c03f4b1023b5e372c6a208201601a9270f/conversions"
        )

    @pytest.mark.regression
    def test_conversions_swap_trace0_null_trace_address(self):
        """Swap with trace0 trace_address == None."""
        self.assert_call_equal(
            "eth/txs/250bb9fd01c35f36b96eeac039de3841013c9ac63a74788976900a713ccd2695/conversions"
        )

    @pytest.mark.regression
    def test_conversions_bridge_eth_btc_with_log(self):
        """Bridge ETH -> BTC with log (OP RETURN)."""
        self.assert_call_equal(
            "eth/txs/9ADD0876DC5478BC9658C10033AC59B8C504A5122266DBBBDE289BEEF2DF3D97/conversions"
        )

    @pytest.mark.regression
    def test_conversions_bridge_eth_btc_direct_memo(self):
        """Bridge ETH -> BTC without log (direct memo)."""
        self.assert_call_equal(
            "eth/txs/0xC0915244DC52B5EFC4F602A7C68874D689AB6F8B71D151D39244617030DB89E0/conversions"
        )

    @pytest.mark.regression
    def test_conversions_bridge_btc_eth_subtx(self):
        """Bridge BTC -> ETH (subtx)."""
        self.assert_call_equal(
            "eth/txs/e2948634dce13d0998dbd65a0f56ffa8d4f070088cce57cc09cc366981073f9f_I321/conversions"
        )

    @pytest.mark.regression
    def test_conversions_bridge_btc_eth_general(self):
        """Bridge BTC -> ETH (general)."""
        self.assert_call_equal(
            "eth/txs/e2948634dce13d0998dbd65a0f56ffa8d4f070088cce57cc09cc366981073f9f/conversions"
        )

    @pytest.mark.regression
    def test_conversions_curve_stableswap_usdc_usdt(self):
        """Curve 3pool StableSwap USDC -> USDT (TokenExchange event)."""
        self.assert_call_equal(
            "eth/txs/0xd58399684daca775f09742e76e48395b4c6979fa9d46daaf20d6a97eac7c9272/conversions"
        )


#    @pytest.mark.regression
#    def test_conversions_bridge_btc_eth_general(self):
#        """Bridge BTC -> ETH (general)."""
#        self.assert_call_equal(
#            "btc/txs/0B7B76EF969D20D3015CA92726F4BA0E2070D6920DDCAC2E61ABB07C72FD1878/conversions"
#        )


# =============================================================================
# Links endpoint tests (address/entity links with pagination)
# =============================================================================


class TestManualRegressionLinks(ManualRegressionTestBase):
    """Manual regression tests for links endpoints."""

    @pytest.mark.regression
    def test_links_eth_entity_316592288(self):
        """ETH entity links (was 60-70s, now 2s)."""
        self.assert_call_equal(
            "eth/entities/316592288/links?neighbor=31455019&pagesize=100"
        )

    @pytest.mark.regression
    def test_links_eth_address_8ccec5bfb049(self):
        """ETH address links (was 17s, now 0.6s)."""
        self.assert_call_equal(
            "eth/addresses/0x8ccec5bfb049af5dd2916853a14974b0a9f47e4d/links?neighbor=0x453290aaf6dca3cee4325bad3f52b1346b6213a7&pagesize=100"
        )

    @pytest.mark.regression
    def test_links_eth_entity_225414228(self):
        """ETH entity links neighbor 229413023."""
        self.assert_call_equal(
            "eth/entities/225414228/links?neighbor=229413023&pagesize=100"
        )

    @pytest.mark.regression
    def test_links_eth_entity_276182118(self):
        """ETH entity links (was 45s, now 3.5s)."""
        self.assert_call_equal(
            "eth/entities/276182118/links?neighbor=81071666&pagesize=100"
        )

    @pytest.mark.regression
    def test_links_eth_entity_huge_addresses(self):
        """ETH entity links between 2 huge addresses."""
        self.assert_call_equal(
            "eth/entities/225414228/links?neighbor=31455019&pagesize=100"
        )

    @pytest.mark.regression
    def test_links_btc_entity_2647118(self):
        """BTC entity links."""
        self.assert_call_equal(
            "btc/entities/2647118/links?neighbor=109578&pagesize=100"
        )

    @pytest.mark.regression
    def test_links_btc_entity_cutoff(self):
        """BTC entity links cutoff test (pagesize=1)."""
        self.assert_call_equal("btc/entities/2647118/links?neighbor=109578&pagesize=1")

    @pytest.mark.regression
    def test_links_btc_address_with_order(self):
        """BTC address links with order=desc."""
        self.assert_call_equal(
            "btc/addresses/bc1qm34lsc65zpw79lxes69zkqmk6ee3ewf0j77s3h/links?neighbor=bc1qc82pdh5zy8kk6gc0t0kjpggu9pg80zewsmy4ac&order=desc&pagesize=100"
        )

    @pytest.mark.regression
    def test_links_trx_address_with_pagination(self):
        """TRX address links with pagination token."""
        self.assert_call_equal(
            "trx/addresses/TCz47XgC9TjCeF4UzfB6qZbM9LTF9s1tG7/links?neighbor=TT8oWoMeoziArGXsPej6EYF5TN4WSUhvfu&order=desc&pagesize=2&page=264825815160586363:137:1"
        )

    @pytest.mark.regression
    def test_links_trx_address_cutoff(self):
        """TRX address links cutoff test."""
        self.assert_call_equal(
            "trx/addresses/TCz47XgC9TjCeF4UzfB6qZbM9LTF9s1tG7/links?neighbor=TT8oWoMeoziArGXsPej6EYF5TN4WSUhvfu&order=desc&pagesize=2"
        )

    @pytest.mark.regression
    def test_links_utxo_net_sender_in_outputs_not_linked(self):
        """A net sender appearing in the outputs is not a link.

        btc tx 18fc9830...: bc1q3ng... is an input (2300) AND an output (1951),
        netting to an outflow of 349, so it is a net sender of the tx. UTXO
        relations pair net senders with net receivers, so there is no relation
        edge 1oUWFer... -> bc1q3ng... and /links must be empty — consistent
        with /neighbors, which never contained that edge. The pre-netting
        /links filtered on raw io membership and returned the tx, disagreeing
        with the relations pre-check (which correctly returned empty).

        Current-server invariant, not a baseline comparison: a baseline
        running the pre-netting code returns 1 link here.
        """
        pair = (
            "btc/addresses/1oUWFer56GT7RUkMs26Az6kmnGyySmdAA/links"
            "?neighbor=bc1q3ngmpljwztklmj5n7et4fv8k2kfjph28gwdefm&pagesize=10"
        )
        data, _ = get_data_from_endpoint(self.current_url, pair)
        assert data.get("links") == [], (
            "net sender in outputs must not produce a link (no relation edge)"
        )

        # the premise: the neighbor list agrees there is no edge
        nb, _ = get_data_from_endpoint(
            self.current_url,
            "btc/addresses/1oUWFer56GT7RUkMs26Az6kmnGyySmdAA/neighbors"
            "?direction=out&pagesize=10",
        )
        neighbor_addresses = {
            (n.get("address") or {}).get("address") for n in nb.get("neighbors", [])
        }
        assert "bc1q3ngmpljwztklmj5n7et4fv8k2kfjph28gwdefm" not in neighbor_addresses, (
            "premise broken: relation edge exists, links may not be empty"
        )

    @pytest.mark.regression
    def test_links_utxo_net_receiver_of_same_tx_is_linked(self):
        """Positive control for the netting case: the same tx 18fc9830...

        bc1qfuys... is a pure output (net receiver), so the relation
        1oUWFer... -> bc1qfuys... exists and the tx is a link under both the
        netted and the pre-netting semantics — safe as a baseline comparison.
        """
        self.assert_call_equal(
            "btc/addresses/1oUWFer56GT7RUkMs26Az6kmnGyySmdAA/links"
            "?neighbor=bc1qfuyscyyxqyzuu6pd7fwqtv5kxwalp6ajd3h7xu&pagesize=10"
        )


# =============================================================================
# Transaction list endpoint tests (with filters)
# =============================================================================


class TestManualRegressionTxsList(ManualRegressionTestBase):
    """Manual regression tests for transaction list endpoints."""

    @pytest.mark.regression
    def test_txs_list_eth_address_with_height_filter(self):
        """ETH address txs with height filter (was 6s, now 1s)."""
        self.assert_call_equal(
            "eth/addresses/0x10c318b1d817396a8a66016438ac9dfb615ffcf1/txs?pagesize=100&min_height=7957441&order=desc"
        )

    @pytest.mark.regression
    def test_txs_list_tether_address_height_range(self):
        """Tether address with height range (was 1.3s, now 0.2s)."""
        self.assert_call_equal(
            "eth/addresses/0xdac17f958d2ee523a2206206994597c13d831ec7/txs?min_height=20698064&max_height=22567324&order=asc&pagesize=5"
        )

    @pytest.mark.regression
    def test_txs_list_small_pagesize(self):
        """Small pagesize test (was 0.37s, now 0.12s)."""
        self.assert_call_equal(
            "eth/addresses/0x255c0dc1567739ceb2c8cd0fddcf1706563868d0/txs?pagesize=1"
        )


# =============================================================================
# Tag obfuscation tests (anonymous vs authenticated)
# =============================================================================


class TestManualRegressionObfuscation(ManualRegressionTestBase):
    """Manual regression tests for tag obfuscation."""

    @pytest.mark.regression
    def test_obfuscation_anonymous_gets_obfuscated_tags(self):
        """Test that anonymous users get obfuscated tags (not zero tags).

        This catches the bug where get_show_private_tags() didn't check
        header_modifications from the plugin middleware.
        """
        call = "btc/addresses/3D4gm7eGSXiEkWS5V3hN9kDVo2eDGBK4eA/tag_summary"

        # Get data without auth header (anonymous user)
        data, _ = get_data_from_endpoint(self.current_url, call, authenticated=False)

        # Anonymous users should still get tags (not zero)
        assert data.get("tag_count", 0) > 0, (
            "Anonymous users should see obfuscated tags, not zero tags"
        )

        # But labels should be obfuscated (empty string)
        assert data.get("best_label") == "", (
            f"Labels should be obfuscated for anonymous users, got: {data.get('best_label')}"
        )

        # All label keys in summary should be empty
        for label_key in data.get("label_summary", {}).keys():
            assert label_key == "", f"Label keys should be obfuscated, got: {label_key}"


# =============================================================================
# Search endpoint tests (edge cases and partial matches)
# =============================================================================


class TestManualRegressionSearch(ManualRegressionTestBase):
    """Manual regression tests for search endpoint."""

    @pytest.mark.regression
    def test_search_btc_address_prefix(self):
        """Search BTC address prefix."""
        self.assert_call_equal("search?q=bc1qasd&limit=100&currency=btc")

    @pytest.mark.regression
    def test_search_eth_address_prefix(self):
        """Search ETH address prefix."""
        self.assert_call_equal("search?q=0x00000&limit=100")

    @pytest.mark.regression
    def test_search_trx_address_prefix(self):
        """Search TRX address prefix."""
        self.assert_call_equal("search?q=TCxZGE&limit=100")

    @pytest.mark.regression
    def test_search_trace_token_search(self):
        """Trace/token search."""
        self.assert_call_equal(
            "search?q=dbd6a65731ab62a68d3d89015a7557ae9376c4693b6e90e0e3c23c903aa89858_T198&limit=100"
        )

    @pytest.mark.regression
    def test_search_overflow_check_short(self):
        """Overflow check with short hex."""
        self.assert_call_equal("search?q=0xfffff")

    @pytest.mark.regression
    def test_search_overflow_check_longer(self):
        """Overflow check with longer hex."""
        self.assert_call_equal("search?q=0xfffff01")

    @pytest.mark.regression
    def test_search_no_results(self):
        """Search with no results expected."""
        self.assert_call_equal("search?q=0xfffff0193483022348723")

    @pytest.mark.regression
    def test_search_partial_tx_hash(self):
        """Partial transaction hash search."""
        self.assert_call_equal(
            "search?q=0x42D529A72CECD6ECE546D5AC0D2A6C2A9407876B66478A33917D8928833433F"
        )


# =============================================================================
# Cross-chain related addresses (pubkey symmetry)
# =============================================================================


class TestManualRegressionRelatedAddresses(ManualRegressionTestBase):
    """Tests that cross-chain pubkey clusters are symmetric.

    Every address in a pubkey cluster must see the same full set of
    related addresses (minus itself).  Querying from BTC, BCH, LTC,
    ZEC, ETH, or TRX must all yield the same cluster members.
    """

    # A known cross-chain cluster
    CLUSTER = [
        ("btc", "1G47mSr3oANXMafVrR8UC4pzV7FEAzo3r9"),
        ("bch", "1G47mSr3oANXMafVrR8UC4pzV7FEAzo3r9"),
        ("ltc", "LaH52f9sspcacPMf2Z7mU5tkhKcWJxvAgA"),
        ("zec", "t1YvimnGBmVA7xDiPnqwbKsvujmSJz4X5m2"),
        ("eth", "0x0d0707963952f2fba59dd06f2b425ace40b492fe"),
        ("trx", "TBA6CypYJizwA9XdC7Ubgc5F1bxrQ7SqPt"),
    ]

    def _get_full_cluster(self, base_url, currency, address):
        """Query related_addresses and return the full cluster set (self + related)."""
        call = f"{currency}/addresses/{address}/related_addresses"
        data, _ = get_data_from_endpoint(base_url, call)
        cluster = {(currency, address)}
        for r in data["related_addresses"]:
            cluster.add((r["currency"], r["address"]))
        return cluster

    @pytest.mark.regression
    def test_crosschain_cluster_symmetry(self):
        """All members of a pubkey cluster must yield the same full set."""
        reference = None
        reference_currency = None

        for currency, address in self.CLUSTER:
            cluster = self._get_full_cluster(self.current_url, currency, address)

            if reference is None:
                reference = cluster
                reference_currency = currency
                continue

            if cluster != reference:
                missing = reference - cluster
                extra = cluster - reference
                pytest.fail(
                    f"Cluster mismatch querying from {currency} vs {reference_currency}.\n"
                    f"  Missing: {missing}\n"
                    f"  Extra:   {extra}"
                )

    @pytest.mark.regression
    def test_crosschain_cluster_contains_bch(self):
        """BCH must appear in related addresses when querying from any network."""
        bch_member = ("bch", "1G47mSr3oANXMafVrR8UC4pzV7FEAzo3r9")

        for currency, address in self.CLUSTER:
            if currency == "bch":
                continue
            cluster = self._get_full_cluster(self.current_url, currency, address)
            assert bch_member in cluster, (
                f"BCH missing from related addresses when querying from {currency}"
            )
