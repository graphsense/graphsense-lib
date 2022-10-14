from typing import Optional


def hex_str_to_bytes(hex_str):
    return bytes.fromhex(hex_str)


def bytes_to_hex(b):
    r = bytes(b).hex()
    return r if len(r) > 0 else None


def is_hex_string(string):
    return string is not None and string.startswith("0x") and len(string) >= 2


def strip_0x(string):
    if is_hex_string(string):
        return string[2:]
    else:
        return string


def to_int(string):
    if type(string) == int:
        return string

    if is_hex_string(string):
        return int(string, 16)
    else:
        return int(string)


def hex_to_bytearray(hex_str: str) -> Optional[bytearray]:
    """Convert hexstring (starting with 0x) to bytearray."""
    return bytearray.fromhex(strip_0x(hex_str)) if hex_str is not None else None
