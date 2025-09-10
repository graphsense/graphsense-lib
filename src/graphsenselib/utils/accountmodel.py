from typing import Optional, Union

from .generic import remove_prefix

# Constants for native asset placeholders
ETH_PLACEHOLDER_ADDRESS = "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
NULL_ADDRESS = "0x0000000000000000000000000000000000000000"
NATIVE_ASSET = "native"


def ensure_0x_prefix(istr: str) -> str:
    if istr.lower().startswith("0x"):
        return istr
    else:
        return f"0x{istr}"


def eth_address_to_hex(address):
    if not isinstance(address, bytes):
        return address
    return "0x" + bytes_to_hex(address)  # ty: ignore[unsupported-operator]


def hex_str_to_bytes(hex_str: str) -> bytes:
    return bytes.fromhex(hex_str)


def bytes_to_hex(b: bytes) -> Optional[str]:
    r = bytes(b).hex()
    return r if len(r) > 0 else None


def is_hex_string(string: Optional[str]) -> bool:
    return (
        string is not None
        and (string.startswith("0x") or string.startswith("0X"))
        and len(string) >= 2
    )


def strip_0x(string: Optional[str]) -> Optional[str]:
    return (
        remove_prefix(remove_prefix(string, "0x"), "0X")  # ty: ignore[invalid-argument-type]
        if is_hex_string(string)
        else string
    )


def to_int(string: Union[str, int]) -> int:
    if isinstance(string, int):
        return string

    if is_hex_string(string):
        return int(string, 16)
    else:
        return int(string)


def hex_to_bytes(hex_str: Optional[str]) -> Optional[bytes]:
    """Convert hexstring (starting with 0x) to bytearray."""
    return bytes.fromhex(strip_0x(hex_str)) if hex_str is not None else None  # type: ignore[invalid-argument-type]


def is_native_placeholder(asset: str) -> bool:
    """
    Check if an asset address represents a native token placeholder.

    Args:
        asset: Asset address to check

    Returns:
        True if the asset is a native token placeholder (0xeeee... or 0x0000...)
    """
    asset_lower = asset.lower()
    return (
        asset_lower == ETH_PLACEHOLDER_ADDRESS.lower()
        or asset_lower == NULL_ADDRESS.lower()
    )


def normalize_asset(asset: str) -> str:
    if is_native_placeholder(asset):
        return NATIVE_ASSET
    else:
        return asset.lower()
