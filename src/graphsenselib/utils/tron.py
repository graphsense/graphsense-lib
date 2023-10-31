import hashlib

import base58

from .accountmodel import bytes_to_hex, hex_str_to_bytes, strip_0x

TRON_ADDRESS_PREFIX = b"\x41"


def sha256(bts):
    m = hashlib.sha256()
    m.update(bts)
    return m.digest()


def get_tron_address_checksum(addr_bytes_with_prefix: bytes):
    h0 = sha256(addr_bytes_with_prefix)
    h1 = sha256(h0)
    checkSum = h1[0:4]
    return checkSum


def add_tron_prefix(address_bytes, prefix: bytes = TRON_ADDRESS_PREFIX):
    if len(address_bytes) == 20:
        return prefix + address_bytes
    return address_bytes


def strip_tron_prefix(address_bytes, prefix: bytes = TRON_ADDRESS_PREFIX):
    if len(address_bytes) == 21 and address_bytes.startswith(prefix):
        return address_bytes[len(prefix) :]
    return address_bytes


def evm_to_bytes(evm_address_hex: str, prefix: bytes = TRON_ADDRESS_PREFIX) -> bytes:
    return strip_tron_prefix(hex_str_to_bytes(strip_0x(evm_address_hex)), prefix)


def evm_to_tron_address(
    evm_address_hex: str, prefix: bytes = TRON_ADDRESS_PREFIX
) -> bytes:
    # inspired by
    # https://github.com/tronprotocol/tronweb/blob/d8c0d48847c0a2dd1c92f4a93f1e01b31c33dc94/src/utils/crypto.js#L14
    a = add_tron_prefix(hex_str_to_bytes(strip_0x(evm_address_hex)), prefix)
    checkSum = get_tron_address_checksum(a)
    taddress = a + checkSum
    return base58.b58encode(taddress)


def evm_to_tron_address_string(
    evm_address_hex: str, prefix: bytes = TRON_ADDRESS_PREFIX
) -> str:
    return evm_to_tron_address(evm_address_hex, prefix).decode("utf-8")


def tron_address_to_bytes(taddress_str: str, validate: bool = True) -> bytes:
    return tron_address_to_evm(taddress_str, validate)


def tron_address_to_evm(taddress_str: str, validate: bool = True) -> bytes:
    ab = base58.b58decode(taddress_str)
    checkSum = ab[-4:]
    a = ab[:-4]

    # recompute checksum
    checkSumComputed = get_tron_address_checksum(a) if validate else None

    if not validate or all(a == b for a, b in zip(checkSum, checkSumComputed)):
        return strip_tron_prefix(a)
    else:
        raise ValueError(f"Invalid checksum on address {taddress_str}")


def tron_address_to_evm_string(taddress_str: str, validate: bool = True) -> str:
    return "0x" + bytes_to_hex(tron_address_to_evm(taddress_str, validate))


def tron_address_to_legacy(taddress_str: str, validate: bool = True) -> bytes:
    """Converts a tron address to its legacy evm/eth format

    Args:
        taddress_str (str): string of tron base58 encoded address
        validate (bool, optional): if checksum should be checked
    """
    return tron_address_to_evm_string(taddress_str, validate)


def equal_evm_tron(t_address: str, evm_address: str) -> bool:
    try:
        return evm_to_bytes(evm_address) == tron_address_to_bytes(t_address)
    except ValueError:
        return False


def equal_evm_evm(evm_address1: str, evm_address2: str) -> bool:
    try:
        return evm_to_bytes(evm_address1) == evm_to_bytes(evm_address2)
    except ValueError:
        return False


def equal_tron_tron(evm_address1: str, evm_address2: str) -> bool:
    try:
        return tron_address_to_bytes(evm_address1) == tron_address_to_bytes(
            evm_address2
        )
    except ValueError:
        return False


def tron_address_equal(address1: str, address2: str) -> bool:
    return (
        equal_evm_tron(address1, address2)
        or equal_evm_tron(address2, address1)
        or equal_evm_evm(address1, address2)
        or equal_tron_tron(address1, address2)
    )
