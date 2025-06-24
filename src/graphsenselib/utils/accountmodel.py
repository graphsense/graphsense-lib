from typing import Optional, Union

from .generic import remove_prefix


def ensure_0x_prefix(istr: str) -> str:
    if istr.lower().startswith("0x"):
        return istr
    else:
        return f"0x{istr}"


def eth_address_to_hex(address):
    if not isinstance(address, bytes):
        return address
    return "0x" + bytes_to_hex(address)


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
        remove_prefix(remove_prefix(string, "0x"), "0X")
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
    return bytes.fromhex(strip_0x(hex_str)) if hex_str is not None else None
