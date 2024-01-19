from typing import Optional, Union

from typeguard import typechecked

from .generic import remove_prefix


@typechecked
def hex_str_to_bytes(hex_str: str) -> bytes:
    return bytes.fromhex(hex_str)


@typechecked
def bytes_to_hex(b: bytes) -> Optional[str]:
    r = bytes(b).hex()
    return r if len(r) > 0 else None


@typechecked
def is_hex_string(string: Optional[str]) -> bool:
    return string is not None and string.startswith("0x") and len(string) >= 2


@typechecked
def strip_0x(string: Optional[str]) -> Optional[str]:
    return remove_prefix(string, "0x") if is_hex_string(string) else string


@typechecked
def to_int(string: Union[str, int]) -> int:
    if type(string) == int:
        return string

    if is_hex_string(string):
        return int(string, 16)
    else:
        return int(string)


@typechecked
def hex_to_bytes(hex_str: Optional[str]) -> Optional[bytes]:
    """Convert hexstring (starting with 0x) to bytearray."""
    return bytes.fromhex(strip_0x(hex_str)) if hex_str is not None else None
