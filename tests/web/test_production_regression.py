import requests
from typing import Dict, Any
import logging
from urllib.parse import urljoin
import time
import os
import pytest

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


new_endpoint = os.environ.get("NEW_SERVER", "http://localhost:9000")
current_endpoint = "https://api.test.ikna.io"
current_key = os.environ.get("GS_API_KEY_TEST")

headers = {"Content-Type": "application/json", "Accept": "application/json"}

# Headers to simulate authenticated access (what API gateway provides)
authenticated_headers = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "X-Consumer-Groups": "tags-private",  # Simulate gateway adding this on auth
}


def get_data_from_current_endpoint(
    endpoint: str, key: str
) -> tuple[Dict[str, Any], float]:
    """Get data from the current endpoint with API key authentication."""
    now = time.time()
    url = urljoin(current_endpoint, endpoint)
    auth_headers = {"Authorization": key, **headers}
    try:
        response = requests.get(url, headers=auth_headers)
        response.raise_for_status()
        elapsed = time.time() - now
        return response.json(), elapsed
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching from current endpoint: {e}")
        raise


def get_data_from_new_endpoint(
    endpoint: str, authenticated: bool = True
) -> tuple[Dict[str, Any], float]:
    """Get data from the new endpoint.

    Args:
        endpoint: The API endpoint to call
        authenticated: If True, include headers that simulate API gateway auth.
                      If False, call without auth headers (for anonymous user tests).
    """
    now = time.time()
    url = urljoin(new_endpoint, endpoint)
    try:
        # Use authenticated_headers to simulate what the API gateway provides
        request_headers = authenticated_headers if authenticated else headers
        response = requests.get(url, headers=request_headers)
        response.raise_for_status()
        elapsed = time.time() - now
        return response.json(), elapsed
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching from new endpoint: {e}")
        raise


def compare_outputs(
    current_data: Dict[str, Any], new_data: Dict[str, Any]
) -> Dict[str, Any]:
    """Compare outputs with detailed differences."""

    if isinstance(current_data, list):
        current_data = {"items": current_data}
    if isinstance(new_data, list):
        new_data = {"items": new_data}

    result = {
        "are_equal": False,
        "differences": [],
        "current_keys": list(current_data.keys()),
        "new_keys": list(new_data.keys()),
    }

    # Check for missing keys
    missing_in_new = set(result["current_keys"]) - set(result["new_keys"])
    missing_in_current = set(result["new_keys"]) - set(result["current_keys"])

    if missing_in_new:
        result["differences"].append(
            f"Keys missing in new data: {list(missing_in_new)}"
        )
    if missing_in_current:
        result["differences"].append(
            f"Keys missing in current data: {list(missing_in_current)}"
        )

    # Compare common keys
    common_keys = set(result["current_keys"]) & set(result["new_keys"])
    for key in common_keys:
        if current_data[key] != new_data[key]:
            # For lists, compare lengths and first few items if different
            if isinstance(current_data[key], list) and isinstance(new_data[key], list):
                if len(current_data[key]) != len(new_data[key]):
                    result["differences"].append(
                        f"List length difference in key '{key}': {len(current_data[key])} != {len(new_data[key])}"
                    )
                else:
                    # Compare first few items if lists are different
                    for i, (current_item, new_item) in enumerate(
                        zip(current_data[key][:5], new_data[key][:5])
                    ):
                        if current_item != new_item:
                            result["differences"].append(
                                f"First difference in key '{key}' at index {i}: {current_item} != {new_item}"
                            )
                            break
            else:
                result["differences"].append(
                    f"Difference in key '{key}': {current_data[key]} != {new_data[key]}"
                )

    result["are_equal"] = len(result["differences"]) == 0
    return result


def compare_instances(call) -> Dict[str, Any]:
    """Test and compare outputs from both endpoints."""
    try:
        new_data, new_time = get_data_from_new_endpoint(call)
        current_data, current_time = get_data_from_current_endpoint(call, current_key)
        comparison = compare_outputs(current_data, new_data)

        # Add speed comparison
        speedup = current_time / new_time if new_time > 0 else float("inf")
        print(
            f"Call: {call} | Speed: Current={current_time:.2f}s, New={new_time:.2f}s, Speedup={speedup:.1f}x"
        )

        return comparison
    except Exception as e:
        logger.error(f"Error during testing: {e}")
        raise


@pytest.mark.production
def test_conversions():
    """Run the regression test and return the comparison result."""

    call_1 = "eth/txs/0x76f4263391a7d72f66cb1f254e8643e37ca739ab2859b9e9cd5b5bda3194332b/conversions"  # dex swap
    call_2 = "eth/txs/0x6D65123E246D752DE3F39E0FDF5B788BAAD35A29B7E95B74C714E6C7C1EA61DD/conversions"  # bridge eth to btc

    call_3_send = "eth/txs/0xD49764B134172947F5226038CD80C8068B76A1F785C6696A34ECDCF29C4D3C78/conversions"
    call_3_receive = "eth/txs/0x5DA7B9D34173E74F1594726B718D0759D5877E14C3CAA8612838A109C196EEEE/conversions"
    call_3_refund = "eth/txs/0x9BD32A4E5B2BC63E858A04F2B8050A8168FBB6CA57A846769348BB1C1B862836/conversions"
    call_4_eth_to_token = "eth/txs/0x42D529A72CECD6ECE546D5AC0D2A6C2A9407876B66478A33917D8928833433F8/conversions"

    call_4_eth_to_btc_thorchain = "eth/txs/0x16ed29f9bf9914ea3b62e4e94829eaef10118d04e82849a285ef8a5700defa1a/conversions"
    # https://thorchain.net/address/0x19317e026ef473d44D746d364062539Ba7Cb0fa3

    # thor 2nd endpoint
    call_5 = "eth/txs/ce09a43f14088aa5ab1e2366609678c03f4b1023b5e372c6a208201601a9270f/conversions"
    call_6 = "eth/txs/250bb9fd01c35f36b96eeac039de3841013c9ac63a74788976900a713ccd2695/conversions"  # swap that has trace0 trace_address == None

    # thor BTC OP RETURN
    call_7_2 = "eth/txs/9ADD0876DC5478BC9658C10033AC59B8C504A5122266DBBBDE289BEEF2DF3D97/conversions"  # bridge eth -> btc with log, now supported via OP RETURN
    call_7 = "eth/txs/0xC0915244DC52B5EFC4F602A7C68874D689AB6F8B71D151D39244617030DB89E0/conversions"  # bridge eth -> btc without log, direct memo, now supported
    call_8 = "eth/txs/e2948634dce13d0998dbd65a0f56ffa8d4f070088cce57cc09cc366981073f9f_I321/conversions"  # bridge BTC -> ETH , ETH receiving to_asset_transfer subtx
    call_8_1 = "eth/txs/e2948634dce13d0998dbd65a0f56ffa8d4f070088cce57cc09cc366981073f9f/conversions"  # bridge BTC -> ETH , ETH receiving tx (general, not subtx)
    # call_7_3 = "btc/txs/0B7B76EF969D20D3015CA92726F4BA0E2070D6920DDCAC2E61ABB07C72FD1878/conversions"

    calls = [
        call_1,
        call_2,
        call_3_send,
        call_3_receive,
        call_3_refund,
        call_4_eth_to_token,
        call_4_eth_to_btc_thorchain,
        call_5,
        call_6,
        call_7_2,
        call_7,  # , call_7_2
        call_8,
        call_8_1,
    ]
    for call in calls:
        logger.info(f"Testing call: {call}")
        comparison = compare_instances(call)
        if not comparison["are_equal"]:
            diff_details = "\n".join(comparison["differences"])
            assert False, (
                f"Outputs differ for call: {call}\n\nDetailed differences:\n{diff_details}"
            )


@pytest.mark.production
def test_links():
    """Run the regression test and return the comparison result."""

    call_0 = "eth/entities/316592288/links?neighbor=31455019&pagesize=100"  # old 60-70s, new 2s
    call_2 = "eth/addresses/0x8ccec5bfb049af5dd2916853a14974b0a9f47e4d/links?neighbor=0x453290aaf6dca3cee4325bad3f52b1346b6213a7&pagesize=100"  # old time: 17, new 0.6
    call_5 = "eth/entities/225414228/links?neighbor=229413023&pagesize=100"  # works
    call_6 = "eth/entities/276182118/links?neighbor=81071666&pagesize=100"  # old time 45s 3.5 seconds new
    call_7 = "eth/entities/225414228/links?neighbor=31455019&pagesize=100"  # 2 huge addresses
    call_8 = "btc/entities/2647118/links?neighbor=109578&pagesize=100"  # btc
    call_8_1 = (
        "btc/entities/2647118/links?neighbor=109578&pagesize=1"  # tests cutoff for btc
    )
    call_9 = "btc/addresses/bc1qm34lsc65zpw79lxes69zkqmk6ee3ewf0j77s3h/links?neighbor=bc1qc82pdh5zy8kk6gc0t0kjpggu9pg80zewsmy4ac&order=desc&pagesize=100"
    call_10 = "trx/addresses/TCz47XgC9TjCeF4UzfB6qZbM9LTF9s1tG7/links?neighbor=TT8oWoMeoziArGXsPej6EYF5TN4WSUhvfu&order=desc&pagesize=2&page=264825815160586363:137:1"
    call_11 = "trx/addresses/TCz47XgC9TjCeF4UzfB6qZbM9LTF9s1tG7/links?neighbor=TT8oWoMeoziArGXsPej6EYF5TN4WSUhvfu&order=desc&pagesize=2"  # tests cutoff for trx
    calls = [
        call_0,
        call_2,
        call_5,
        call_6,
        call_7,
        call_8,
        call_8_1,
        call_9,
        call_10,
        call_11,
    ]
    for call in calls:
        logger.info(f"Testing call: {call}")
        comparison = compare_instances(call)
        if not comparison["are_equal"]:
            diff_details = "\n".join(comparison["differences"])
            assert False, (
                f"Outputs differ for call: {call}\n\nDetailed differences:\n{diff_details}"
            )


@pytest.mark.production
def test_txs_list():
    """Run the regression test and return the comparison result."""

    call_1 = "eth/addresses/0x10c318b1d817396a8a66016438ac9dfb615ffcf1/txs?pagesize=100&min_height=7957441&order=desc"  # 6 -> 1
    call_3 = "eth/addresses/0xdac17f958d2ee523a2206206994597c13d831ec7/txs?min_height=20698064&max_height=22567324&order=asc&pagesize=5"  # 1.3 -> 0.2
    call_4 = "eth/addresses/0x255c0dc1567739ceb2c8cd0fddcf1706563868d0/txs?pagesize=1"  # old time: 0.37s, new 0.12s
    calls = [call_1, call_3, call_4]
    for call in calls:
        logger.info(f"Testing call: {call}")
        comparison = compare_instances(call)
        if not comparison["are_equal"]:
            diff_details = "\n".join(comparison["differences"])
            assert False, (
                f"Outputs differ for call: {call}\n\nDetailed differences:\n{diff_details}"
            )


@pytest.mark.production
def test_obfuscation():
    """Test that anonymous users get obfuscated tags (not zero tags).

    This catches the bug where get_show_private_tags() didn't check
    header_modifications from the plugin middleware.
    """
    # Address known to have private tags
    call = "btc/addresses/3D4gm7eGSXiEkWS5V3hN9kDVo2eDGBK4eA/tag_summary"

    # Get data without auth header (anonymous user)
    data, _ = get_data_from_new_endpoint(call, authenticated=False)

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


@pytest.mark.production
def test_search():
    """Run the regression test and return the comparison result."""

    # Note: If this runs depends on the order of the keyspaces in the config.yaml
    call_1 = "search?q=bc1qasd&limit=100&currency=btc"
    call_2 = "search?q=0x00000&limit=100"
    call_3 = "search?q=TCxZGE&limit=100"
    # todo find out why traces and tokens order is not deterministic
    call_4 = "search?q=dbd6a65731ab62a68d3d89015a7557ae9376c4693b6e90e0e3c23c903aa89858_T198&limit=100"
    call_5 = "search?q=0xfffff"  # check possibly wrong "overflow"
    call_6 = "search?q=0xfffff01"
    call_7 = "search?q=0xfffff0193483022348723"  # no results
    call_8 = (
        "search?q=0x42D529A72CECD6ECE546D5AC0D2A6C2A9407876B66478A33917D8928833433F"
    )

    calls = [call_1, call_2, call_3, call_4, call_5, call_6, call_7, call_8]
    for call in calls:
        logger.info(f"Testing call: {call}")
        comparison = compare_instances(call)
        if not comparison["are_equal"]:
            diff_details = "\n".join(comparison["differences"])
            assert False, (
                f"Outputs differ for call: {call}\n\nDetailed differences:\n{diff_details}"
            )
