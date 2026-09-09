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

import hashlib
import zlib

from graphsenselib.utils.address import (
    InvalidAddress,
    address_to_bytes,
    address_to_str,
)

#: A network's P2PKH version byte -- t1 for ZEC -- for :func:`reversion_address`.
#:
#: A COPY of `graphsenselib.ingest.rpc_utxo._PUBKEY_ADDRESS_VERSION`, and the
#: FOURTH of these tables in the repo. Copying one is exactly what
#: `spark.columns._script_types` argues against, so the reason it happens here
#: has to be stated: this is read INSIDE A pandas UDF, on the executors, and
#: every module that holds one of the other three drags in something the baked
#: spark-env archive does not carry. `rpc_utxo` imports `rpc_eth` for orjson;
#: `ingest.utxo` imports `..db` and hence pydantic and goodconf;
#: `utils.pubkey_to_address` imports coincurve and eth_keys. A five-entry dict
#: is not worth an RPC client on every executor, and the import fails there
#: anyway -- which is how this was found, as a ModuleNotFoundError mid-run.
#:
#: `tests/v3/test_v3_codec.py` fails if this drifts from the ingest table. The
#: real fix is the pending refactor that gives these bytes one dep-free home
#: (see `tests/ingest/test_pubkey_address_version_parity.py` for the three).
P2PKH_VERSION = {
    "btc": b"\x00",
    "bch": b"\x00",
    "ltc": b"\x30",
    "doge": b"\x1e",
    "zec": b"\x1c\xb8",
}

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


#: Base58 alphabet, in the order the checksum encoding uses.
_B58 = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"


def _b58decode(text: str) -> bytes:
    number = 0
    for char in text:
        number = number * 58 + _B58.index(char)
    raw = number.to_bytes((number.bit_length() + 7) // 8, "big")
    return b"\x00" * (len(text) - len(text.lstrip("1"))) + raw


def _b58encode(raw: bytes) -> str:
    number = int.from_bytes(raw, "big")
    out = ""
    while number:
        number, remainder = divmod(number, 58)
        out = _B58[remainder] + out
    return "1" * (len(raw) - len(raw.lstrip(b"\x00"))) + out


def _b58check(version: bytes, payload: bytes) -> str:
    body = version + payload
    digest = hashlib.sha256(hashlib.sha256(body).digest()).digest()[:4]
    return _b58encode(body + digest)


def reversion_address(network: str, address: str) -> str:
    """A base58 address re-encoded with ``network``'s own P2PKH version byte.

    **A LAKE REPAIR, applied on the WRITE path only.** The LTC lake predates the
    2026-06-15 network-aware P2PK fix (c103323c): gslib DERIVES a P2PK address
    from the script rather than reading it from the node -- the one address
    class it computes itself -- and the pre-fix parser hardcoded BTC's version
    byte. So the lake holds `1417...` where LTC has `LNE5...`: the same hash160
    under the wrong network's byte. Every other class comes from the node's own
    `addresses` field and is unaffected, which is why this is ~28% of
    EARLY-CHAIN addresses and not everything.

    Not applied when ENCODING a lookup, only when writing: v2 stores `LNE5...`
    and answers "not found" for `1417...`, so re-versioning a caller's address
    would make v3 find something v2 does not.

    Delete this once the lake is re-ingested. It is a workaround, not a rule.

    Leaves alone anything that is not a base58check address with a *different*
    version: bech32 passes through, and so does an address already carrying the
    right byte. Only the hash160 is preserved, which is the part both backends
    agree on -- the version byte is exactly what the stale lake gets wrong.
    """
    want = P2PKH_VERSION.get(network.lower())
    if want is None or not address or not all(c in _B58 for c in address):
        return address
    try:
        raw = _b58decode(address)
    except ValueError:
        return address
    if len(raw) < 5:
        return address
    body, checksum = raw[:-4], raw[-4:]
    if hashlib.sha256(hashlib.sha256(body).digest()).digest()[:4] != checksum:
        return address  # not base58check; leave it exactly as it is
    if body[: len(want)] == want:
        return address

    # Only rewrite a version that is ANOTHER NETWORK'S P2PKH byte. Rewriting on
    # length alone is wrong: LTC's P2SH address is also one version byte plus a
    # 20-byte hash, so a length test turns a valid P2SH address into a valid,
    # different P2PKH one -- a wrong answer rather than a reported mismatch.
    for other in P2PKH_VERSION.values():
        if body[: len(other)] == other and len(body) == len(other) + 20:
            return _b58check(want, body[len(other) :])
    return address


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
