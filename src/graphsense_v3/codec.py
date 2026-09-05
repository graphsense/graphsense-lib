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


#: The synthetic source of a coinbase transaction's value. graphsense-spark
#: inserts a literal "coinbase" input on such transactions
#: (`utxo/Transformation.scala:111-125`) and the REST surfaces the string as an
#: address (`comparison_service.py:55`), so v3 has to keep it -- but under D1 an
#: address is bytes, and "coinbase" is not an encodable address. Empty bytes is
#: the sentinel: no real address encodes to it, since every codec emits at least
#: one byte for a non-empty string.
COINBASE = "coinbase"
COINBASE_BYTES = b""


#: The marker gslib's converters put in front of an address that is not a
#: standard form for its network. BCH's converter STRIPS it in `to_str` but
#: only re-adds it in `to_bytes` for a `bc1` address, so the two are not
#: inverses for anything else -- see `encode_address`.
NONSTANDARD_PREFIX = "nonstandard"


def encode_address(network: str, address: str) -> bytes:
    """User-format address string -> stored bytes.

    **Repairs gslib's one non-bijective case.** `AddressConverterBchWith
    NonstandardFallback.to_str` removes the ``nonstandard`` prefix, while its
    `to_bytes` re-adds it only for ``bc1`` addresses -- so a nonstandard BCH
    script address decodes to a string that cannot be encoded back. v2 never
    noticed because it stores address STRINGS; v3 keys on bytes and therefore
    round-trips every address it reads.

    The retry is self-validating: the repaired bytes are accepted only if they
    decode back to exactly the input. Without that check this would silently
    manufacture bytes for a genuinely invalid address on every network, which
    is a far worse failure than the one it fixes.
    """
    from graphsenselib.utils.address import InvalidAddress

    if address == COINBASE:
        return COINBASE_BYTES
    net = network.lower()
    try:
        return address_to_bytes(net, address)
    except InvalidAddress:
        repaired = address_to_bytes(net, f"{NONSTANDARD_PREFIX}{address}")
        if address_to_str(net, repaired) != address:
            raise
        return repaired


def decode_address(network: str, address: bytes) -> str:
    """Stored bytes -> user-format address string."""
    if address == COINBASE_BYTES:
        return COINBASE
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


#: Bits reserved for a transaction's position within its block. 32 is what the
#: account families already use, and consistency across families is worth more
#: than the ~20 bits it wastes: a UTXO block holds at most a few thousand
#: transactions, and (3.4M blocks << 32) is still only 2^54.
TX_INDEX_BITS = 32


def tx_id(block_id: int, index: int) -> int:
    """``(block_id << 32) + index`` -- a transaction's id, in every family.

    Derivable from the transaction itself, so nothing has to be counted, looked
    up or carried between batches. Three properties matter:

    * It **orders identically** to the running counter it replaces, since
      block_id is the high part and index the low -- so ``ORDER BY tx_id``,
      ``min(tx_id)`` and ``max(tx_id)`` all keep their meaning.
    * It is **decodable**: ``first_tx_id -> height`` becomes arithmetic instead
      of a point read on `transaction`, and a height range maps onto a tx_id
      range with no lookup at all.
    * It is **local**: a backfill of blocks N..M needs nothing from block N-1,
      so ranged and parallel runs need no coordination.

    It is *not* dense. Nothing may treat a tx_id as a count of transactions.
    """
    if block_id < 0 or index < 0:
        raise ValueError(
            f"block_id and index must be non-negative: {block_id}, {index}"
        )
    if index >= 1 << TX_INDEX_BITS:
        raise ValueError(
            f"transaction index {index} does not fit in {TX_INDEX_BITS} bits"
        )
    return (block_id << TX_INDEX_BITS) + index


def block_of_tx_id(value: int) -> int:
    """The block a transaction id belongs to."""
    return value >> TX_INDEX_BITS


def index_of_tx_id(value: int) -> int:
    """A transaction's position within its block."""
    return value & ((1 << TX_INDEX_BITS) - 1)


def tx_id_range(first_block: int, last_block: int) -> tuple[int, int]:
    """Inclusive ``(lo, hi)`` tx_id bounds covering a block range.

    This is what makes a height filter a pushdown rather than a lookup: v2 had
    to read the previous block's `block_transactions` and take ``max(tx_id)``
    (``db/utxo.py:109``) to answer the same question.
    """
    if last_block < first_block:
        raise ValueError(f"empty block range: {first_block}..{last_block}")
    return tx_id(first_block, 0), tx_id(last_block + 1, 0) - 1
