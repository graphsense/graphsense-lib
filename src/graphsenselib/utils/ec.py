from eth_hash.auto import keccak
from typing import Tuple

from coincurve.keys import PublicKey as _CoincurvePublicKey

# secp256k1 field prime (SEC2 2.4.1).
_SECP256K1_P = 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEFFFFFC2F


def secp256k1_decompress(pubkey: bytes) -> bytes:
    """Decompress a secp256k1 public key.

    For further information see: https://bitcoin.stackexchange.com/questions/86234/how-to-uncompress-a-public-key
    :param pubkey: The secp256k1 public key in compressed format given as bytes
    :return: The secp256k1 public key in uncompressed format given as bytes
    """
    if not isinstance(pubkey, bytes):
        raise ValueError("Input pubkey must be bytes")
    if len(pubkey) != 33:
        raise ValueError(
            "Input pubkey must be 33 bytes long, if it is 65 bytes long it is probably uncompressed"
        )

    p = _SECP256K1_P
    x = int.from_bytes(pubkey[1:33], byteorder="big")
    y_sq = (pow(x, 3, p) + 7) % p  # y^2 = x^3 + 7 (mod p)
    y = pow(y_sq, (p + 1) // 4, p)  # quadratic residue
    if y % 2 != pubkey[0] % 2:
        # check against the first byte to identify the correct
        # y out of the two possibel values y and -y
        y = p - y
    y = y.to_bytes(32, byteorder="big")
    return b"\x04" + pubkey[1:33] + y


def secp256k1_compress(pubkey: bytes) -> bytes:
    """Compress a secp256k1 public key.

    :param pubkey: The secp256k1 public key in uncompressed format given as bytes
    :return: The secp256k1 public key in compressed format given as bytes
    """
    if not isinstance(pubkey, bytes):
        raise ValueError("Input pubkey must be bytes")
    if len(pubkey) != 65:
        raise ValueError(
            "Input pubkey must be 65 bytes long, if it is 33 bytes long it is probably already compressed"
        )

    x_bytes, y_bytes = secp256k1_extract_coordinates(pubkey)
    if isinstance(x_bytes, bytes) and isinstance(y_bytes, bytes):
        y = int.from_bytes(y_bytes, "big")
        x = int.from_bytes(x_bytes, "big")
    else:
        raise ValueError("x and y must be bytes!")
    prefix = b"\x02" if y % 2 == 0 else b"\x03"
    compressed_public_key = prefix + x.to_bytes(32, "big")
    return compressed_public_key


def secp256k1_extract_coordinates(pubkey: bytes) -> Tuple[bytes, bytes]:
    """Extract the x and y coordinates of a secp256k1 public key.

    :param pubkey: The secp256k1 public key in either compressed or uncompressed format given as bytes
    :return: A tuple of (x,y) given as bytes
    """
    if not isinstance(pubkey, bytes):
        raise ValueError("pubkey given as bytes expected")
    if len(pubkey) == 33:
        decomp_pubkey = secp256k1_decompress(pubkey)
    elif len(pubkey) == 65:
        decomp_pubkey = pubkey
    else:
        raise ValueError(
            "Invalid length, if not 33 or 65 bytes its probably not a compressed or uncompressed key"
        )
    x_bytes = decomp_pubkey[1:33]
    y_bytes = decomp_pubkey[-32:]
    return (x_bytes, y_bytes)


def secp256k1_pubkey_to_eth_addr(pubkey: bytes) -> bytes:
    """Generate std. Ethereum address out of uncompressed public key

    :param pubkey: The secp256k1 public key in uncomressed format given as bytes
                   (note that no check is performed if the key is really uncompressed)
    :return: The Ethereum address as bytes without "0x" prefix and without checksum.
    """
    return keccak(pubkey[1:])[-20:]


def is_valid_secp256k1_pubkey(pk: bytes, fast: bool = True) -> bool:
    """Takes a compressed or uncompressed secp256k1 public key and checks if it is a valid public key for that curve

    For more information see SEC1v2 https://www.secg.org/sec1-v2.pdf#subsubsection.3.2.2

    ``fast=True`` (default) validates via coincurve (C/libsecp256k1) in
    microseconds — the right choice for bulk extraction. ``fast=False`` is a
    pure-Python SEC1 check kept as an independent cross-check oracle. It skips
    SEC1's subgroup test ``n * Q == O``: secp256k1 has cofactor h = 1, so every
    on-curve point with in-range coordinates is already in the prime-order
    group. Both paths reject off-curve, out-of-range and zero-coordinate keys
    identically.
    """
    if fast:
        try:
            _CoincurvePublicKey(bytes(pk))
            return True
        except ValueError:
            # coincurve raises ValueError for any unparseable / off-curve /
            # out-of-range key — the definitive "not a valid pubkey". Narrow on
            # purpose: a non-bytes arg (TypeError) or any other error is a bug we
            # want surfaced, not silently turned into a dropped key.
            return False

    x_bytes, y_bytes = secp256k1_extract_coordinates(pk)
    x = int.from_bytes(x_bytes, byteorder="big")
    y = int.from_bytes(y_bytes, byteorder="big")
    p = _SECP256K1_P
    # The point at infinity has no affine encoding; zero coordinates mark it.
    if x == 0 or y == 0:
        return False
    if x >= p or y >= p:
        return False
    # On the curve y^2 = x^3 + 7 (mod p).
    return (y * y - (pow(x, 3, p) + 7)) % p == 0
