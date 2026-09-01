"""Address encoding, bucketing and search prefixes.

Everything here is used by BOTH the Spark backfill and the DAL. Two producers
disagreeing about a key is the failure mode this whole migration is most exposed
to, so the rule is: one definition, called from both sides, with a test that the
Spark expression and the Python function agree on real addresses.

Encoding delegates to ``graphsenselib.utils.address``. It is a proven exact
bijection (its inverse is graphsense-spark's ``AddressDecoder.scala``) and
reimplementing it would be a second thing to get wrong.
"""

from __future__ import annotations

import zlib

from graphsenselib.utils.address import address_to_bytes, address_to_str

#: Leading run that carries no information for a network's bech32 addresses:
#: human-readable part, separator, and the witness-version character.
#:
#: This is the fix for v2's prefix-index defect. v2 strips only ``"bc"`` from BTC
#: (``config.py:145``), leaving ``1q...``/``1p...`` -- so two of four prefix
#: characters are constants and the entire segwit space lands in 32^2 = 1024
#: partitions of ~390k rows, where LTC (which strips ``"ltc1"``) gets ~1M
#: partitions of ~380. v3 reads whole prefix partitions and filters client-side,
#: because packed bytes are not order-preserving across address types, so small
#: partitions are a PREREQUISITE rather than an improvement.
_BECH32_DEAD_PREFIX: dict[str, str] = {
    "btc": "bc1",
    "ltc": "ltc1",
}

DEFAULT_PREFIX_LENGTH = 4


def encode_address(network: str, address: str) -> bytes:
    """User-format address string -> stored bytes."""
    return address_to_bytes(network.lower(), address)


def decode_address(network: str, address: bytes) -> str:
    """Stored bytes -> user-format address string."""
    return address_to_str(network.lower(), address)


def bucket(address: bytes, buckets: int) -> int:
    """Which bucket an entity's row lives in.

    CRC-32 rather than a cryptographic hash or Spark's own ``hash``: this must be
    computed identically by a Spark expression and by Python, and CRC-32 is the
    one function where that holds by definition -- ``F.crc32`` is
    ``java.util.zip.CRC32``, the same IEEE CRC-32 as :func:`zlib.crc32`. Spark's
    ``hash`` is Murmur3 over its *internal* row representation and is not
    reproducible outside the JVM; ``xxhash64`` would need a new dependency.
    Uniformity is all that is required of a bucket, and CRC-32 has it.
    """
    if buckets <= 0:
        raise ValueError(f"buckets must be positive, got {buckets}")
    return zlib.crc32(address) % buckets


def search_prefix(
    network: str, address: str, length: int = DEFAULT_PREFIX_LENGTH
) -> str:
    """The ``address_by_prefix`` partition key for an address.

    Drops the network's dead leading run first, so the prefix carries ``length``
    varying characters rather than ``length`` minus however many are constant.
    """
    net = network.lower()
    dead = _BECH32_DEAD_PREFIX.get(net)
    body = address
    if dead and address.lower().startswith(dead):
        # +1 for the witness-version character, which is 'q' for v0 and 'p' for
        # taproot -- effectively constant, so it buys no partitions.
        body = address[len(dead) + 1 :]
    return body[:length].lower()
