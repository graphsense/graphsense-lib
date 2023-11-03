from typing import Optional

from .generic import remove_prefix


def hex_str_to_bytes(hex_str: str) -> bytes:
    return bytes.fromhex(hex_str)


def bytes_to_hex(b: bytes) -> str:
    r = bytes(b).hex()
    return r if len(r) > 0 else None


def is_hex_string(string: str):
    return string is not None and string.startswith("0x") and len(string) >= 2


def strip_0x(string: str) -> str:
    return remove_prefix(string, "0x") if is_hex_string(string) else string


def to_int(string: str):
    if type(string) == int:
        return string

    if is_hex_string(string):
        return int(string, 16)
    else:
        return int(string)


def hex_to_bytearray(hex_str: str) -> Optional[bytearray]:
    """Convert hexstring (starting with 0x) to bytearray."""
    return bytearray.fromhex(strip_0x(hex_str)) if hex_str is not None else None
