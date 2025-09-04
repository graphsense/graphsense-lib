import re
from typing import Any, Dict, List, Optional
from eth_abi import decode as abi_decode
from eth_utils import to_hex, keccak


def parse_function_call(
    input_bytes: Optional[bytes], function_signatures: Dict[str, List[Dict[str, Any]]]
) -> Optional[Dict[str, Any]]:
    """
    Parse an Ethereum function call given input bytes and function signatures database.

    Args:
        input_bytes: The transaction input data as bytes
        function_signatures: Dictionary mapping function selectors to function definitions

    Returns:
        Dictionary containing decoded function call data or None if parsing fails
    """
    if not input_bytes or len(input_bytes) < 4:
        return None

    # Extract function selector (first 4 bytes)
    function_selector = to_hex(input_bytes[:4])
    call_data = input_bytes[4:]

    if function_selector not in function_signatures:
        return None

    function_definitions = function_signatures[function_selector]

    # Try each function definition variant
    for func_def in function_definitions:
        try:
            # Extract parameter types for ABI decoding
            param_types = [param["type"] for param in func_def.get("inputs", [])]

            if not param_types and len(call_data) == 0:
                # Function with no parameters
                return {
                    "name": func_def["name"],
                    "selector": function_selector,
                    "inputs": [],
                    "parameters": {},
                    "function_def": func_def,
                }
            elif param_types:
                # Decode parameters using ABI
                decoded_params = abi_decode(param_types, call_data)

                # Create parameter mapping
                parameters = {}
                inputs = []
                for i, param in enumerate(func_def.get("inputs", [])):
                    param_name = param["name"]
                    param_value = decoded_params[i] if i < len(decoded_params) else None

                    parameters[param_name] = param_value
                    inputs.append(
                        {
                            "name": param_name,
                            "type": param["type"],
                            "value": param_value,
                        }
                    )

                return {
                    "name": func_def["name"],
                    "selector": function_selector,
                    "inputs": inputs,
                    "parameters": parameters,
                    "function_def": func_def,
                }

        except Exception:
            # Try next function definition variant
            continue

    return None


def generate_function_selector(function_signature: str) -> str:
    """
    Generate function selector from function signature string.

    Args:
        function_signature: Function signature like "transfer(address,uint256)"

    Returns:
        4-byte function selector as hex string
    """
    return to_hex(keccak(text=function_signature)[:4])


def decoded_function_to_str(decoded_function: Dict[str, Any]) -> str:
    """
    Convert decoded function call to readable string format.

    Args:
        decoded_function: Decoded function call dictionary

    Returns:
        Formatted string representation
    """
    name = decoded_function["name"]
    params = ",".join(
        [f"{x['name']}={x['value']}" for x in decoded_function.get("inputs", [])]
    )
    return f"{name}({params})"


def get_filtered_function_signatures(
    filter_pattern: str, function_signatures: Dict[str, List[Dict[str, Any]]]
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Filter function signatures by tag pattern.

    Args:
        filter_pattern: Regex pattern to match against function tags
        function_signatures: Function signatures database

    Returns:
        Filtered function signatures dictionary
    """
    pattern = re.compile(filter_pattern)

    result = {}
    for selector, functions in function_signatures.items():
        filtered_functions = [
            func
            for func in functions
            if any(pattern.match(tag) for tag in func.get("tags", []))
        ]

        if filtered_functions:
            result[selector] = filtered_functions

    return result


# Example function signatures database (similar to log_signatures structure)
function_signatures = {
    "0xa9059cbb": [  # transfer(address,uint256)
        {
            "name": "transfer",
            "inputs": [
                {"name": "to", "type": "address"},
                {"name": "amount", "type": "uint256"},
            ],
            "tags": ["erc20", "token", "transfer"],
        }
    ],
    "0x23b872dd": [  # transferFrom(address,address,uint256)
        {
            "name": "transferFrom",
            "inputs": [
                {"name": "from", "type": "address"},
                {"name": "to", "type": "address"},
                {"name": "amount", "type": "uint256"},
            ],
            "tags": ["erc20", "token", "transfer"],
        }
    ],
    "0x095ea7b3": [  # approve(address,uint256)
        {
            "name": "approve",
            "inputs": [
                {"name": "spender", "type": "address"},
                {"name": "amount", "type": "uint256"},
            ],
            "tags": ["erc20", "token", "approval"],
        }
    ],
    "0xff190b9f": [  # swapEthForTokens(address token, uint256 amountOutMin, uint256 deadline)
        {
            "name": "swapEthForTokens",
            "inputs": [
                {"name": "token", "type": "address"},
                {"name": "amountOutMin", "type": "uint256"},
                {"name": "deadline", "type": "uint256"},
            ],
            "tags": ["swap", "weth"],
        }
    ],
}
