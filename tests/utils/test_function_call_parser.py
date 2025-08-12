from eth_utils import keccak, to_hex

from graphsenselib.utils.function_call_parser import (
    parse_function_call,
    generate_function_selector,
    decoded_function_to_str,
    get_filtered_function_signatures,
    function_signatures,
)


def test_parse_function_call_empty_input():
    """Test parsing with empty input bytes"""
    result = parse_function_call(b"", function_signatures)
    assert result is None


def test_parse_function_call_short_input():
    """Test parsing with input shorter than 4 bytes"""
    result = parse_function_call(b"abc", function_signatures)
    assert result is None


def test_parse_function_call_unknown_selector():
    """Test parsing with unknown function selector"""
    unknown_input = b"\x12\x34\x56\x78" + b"\x00" * 32
    result = parse_function_call(unknown_input, function_signatures)
    assert result is None


def test_parse_function_call_transfer_success():
    """Test successful parsing of ERC20 transfer function"""
    # transfer(address,uint256) selector is 0xa9059cbb
    selector = bytes.fromhex("a9059cbb")
    # Encode address (32 bytes) and uint256 (32 bytes)
    to_address = b"\x00" * 12 + b"\x12" * 20  # 20-byte address padded to 32 bytes
    amount = (1000).to_bytes(32, "big")
    input_bytes = selector + to_address + amount

    result = parse_function_call(input_bytes, function_signatures)

    assert result is not None
    assert result["name"] == "transfer"
    assert result["selector"] == "0xa9059cbb"
    assert len(result["inputs"]) == 2
    assert result["inputs"][0]["name"] == "to"
    assert result["inputs"][0]["type"] == "address"
    assert result["inputs"][1]["name"] == "amount"
    assert result["inputs"][1]["type"] == "uint256"
    assert result["inputs"][1]["value"] == 1000
    assert result["parameters"]["amount"] == 1000


def test_parse_function_call_transfer_from_success():
    """Test successful parsing of ERC20 transferFrom function"""
    # transferFrom(address,address,uint256) selector is 0x23b872dd
    selector = bytes.fromhex("23b872dd")
    from_address = b"\x00" * 12 + b"\x11" * 20
    to_address = b"\x00" * 12 + b"\x22" * 20
    amount = (2000).to_bytes(32, "big")
    input_bytes = selector + from_address + to_address + amount

    result = parse_function_call(input_bytes, function_signatures)

    assert result is not None
    assert result["name"] == "transferFrom"
    assert result["selector"] == "0x23b872dd"
    assert len(result["inputs"]) == 3
    assert result["parameters"]["amount"] == 2000


def test_parse_function_call_approve_success():
    """Test successful parsing of ERC20 approve function"""
    # approve(address,uint256) selector is 0x095ea7b3
    selector = bytes.fromhex("095ea7b3")
    spender_address = b"\x00" * 12 + b"\x33" * 20
    amount = (5000).to_bytes(32, "big")
    input_bytes = selector + spender_address + amount

    result = parse_function_call(input_bytes, function_signatures)

    assert result is not None
    assert result["name"] == "approve"
    assert result["selector"] == "0x095ea7b3"
    assert len(result["inputs"]) == 2
    assert result["parameters"]["amount"] == 5000


def test_parse_function_call_no_parameters():
    """Test parsing function with no parameters"""
    # Create a test signature for a function with no parameters
    test_signatures = {
        "0x12345678": [{"name": "totalSupply", "inputs": [], "tags": ["erc20", "view"]}]
    }

    selector = bytes.fromhex("12345678")
    input_bytes = selector  # No additional data

    result = parse_function_call(input_bytes, test_signatures)

    assert result is not None
    assert result["name"] == "totalSupply"
    assert result["selector"] == "0x12345678"
    assert result["inputs"] == []
    assert result["parameters"] == {}


def test_parse_function_call_abi_decode_failure():
    """Test handling of ABI decoding failures"""
    selector = bytes.fromhex("a9059cbb")  # transfer selector
    # Provide insufficient data for ABI decoding
    input_bytes = selector + b"\x00" * 10  # Not enough bytes for address + uint256

    result = parse_function_call(input_bytes, function_signatures)
    assert result is None


def test_parse_function_call_multiple_variants():
    """Test function with multiple signature variants"""
    # Create test signatures with multiple variants
    test_signatures = {
        "0xabcdef12": [
            {
                "name": "testFunc",
                "inputs": [{"name": "param1", "type": "uint256"}],
                "tags": ["test"],
            },
            {
                "name": "testFunc",
                "inputs": [{"name": "param1", "type": "address"}],
                "tags": ["test"],
            },
        ]
    }

    selector = bytes.fromhex("abcdef12")
    # Provide data that matches the uint256 variant
    param_data = (12345).to_bytes(32, "big")
    input_bytes = selector + param_data

    result = parse_function_call(input_bytes, test_signatures)

    assert result is not None
    assert result["name"] == "testFunc"
    assert result["inputs"][0]["value"] == 12345


def test_generate_function_selector_transfer():
    """Test generating selector for transfer function"""
    selector = generate_function_selector("transfer(address,uint256)")
    assert selector == "0xa9059cbb"


def test_generate_function_selector_transfer_from():
    """Test generating selector for transferFrom function"""
    selector = generate_function_selector("transferFrom(address,address,uint256)")
    assert selector == "0x23b872dd"


def test_generate_function_selector_approve():
    """Test generating selector for approve function"""
    selector = generate_function_selector("approve(address,uint256)")
    assert selector == "0x095ea7b3"


def test_generate_function_selector_no_params():
    """Test generating selector for function with no parameters"""
    selector = generate_function_selector("totalSupply()")
    expected = to_hex(keccak(text="totalSupply()")[:4])
    assert selector == expected


def test_decoded_function_to_str_with_params():
    """Test string representation of decoded function with parameters"""
    decoded = {
        "name": "transfer",
        "inputs": [
            {"name": "to", "value": "0x1234567890123456789012345678901234567890"},
            {"name": "amount", "value": 1000},
        ],
    }

    result = decoded_function_to_str(decoded)
    assert (
        result == "transfer(to=0x1234567890123456789012345678901234567890,amount=1000)"
    )


def test_decoded_function_to_str_no_params():
    """Test string representation of decoded function without parameters"""
    decoded = {"name": "totalSupply", "inputs": []}

    result = decoded_function_to_str(decoded)
    assert result == "totalSupply()"


def test_decoded_function_to_str_missing_inputs():
    """Test string representation when inputs key is missing"""
    decoded = {"name": "someFunction"}

    result = decoded_function_to_str(decoded)
    assert result == "someFunction()"


def test_get_filtered_function_signatures_erc20():
    """Test filtering by erc20 tag"""
    result = get_filtered_function_signatures("erc20", function_signatures)

    assert len(result) == 3  # transfer, transferFrom, approve
    assert "0xa9059cbb" in result
    assert "0x23b872dd" in result
    assert "0x095ea7b3" in result


def test_get_filtered_function_signatures_transfer():
    """Test filtering by transfer tag"""
    result = get_filtered_function_signatures("transfer", function_signatures)

    assert len(result) == 2  # transfer, transferFrom
    assert "0xa9059cbb" in result
    assert "0x23b872dd" in result
    assert "0x095ea7b3" not in result


def test_get_filtered_function_signatures_approval():
    """Test filtering by approval tag"""
    result = get_filtered_function_signatures("approval", function_signatures)

    assert len(result) == 1  # approve only
    assert "0x095ea7b3" in result
    assert "0xa9059cbb" not in result


def test_get_filtered_function_signatures_no_match():
    """Test filtering with pattern that matches nothing"""
    result = get_filtered_function_signatures("nonexistent", function_signatures)
    assert len(result) == 0


def test_get_filtered_function_signatures_regex_pattern():
    """Test filtering with regex pattern"""
    result = get_filtered_function_signatures("^token$", function_signatures)

    # Should match all functions with "token" tag
    assert len(result) == 3
