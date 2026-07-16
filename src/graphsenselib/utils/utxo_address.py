"""BTC-form base58 P2PKH address healing, importable on Spark executors.

``normalize_base58_p2pkh`` is referenced by the delta-to-raw normalization
UDF (``transformation/utxo.py``), so Spark executors import this module when
they unpickle the closure. The executors run the baked spark-env archive
(Dockerfile: graphsense-lib wheel ``--no-deps`` + the crypto stack), which
has none of the ingest machinery — keep this module's imports stdlib-only at
import time; the third-party bits (base58, pubkey_to_address) load lazily
inside the function and are part of the spark-env.

Ingest-side callers import these names via ``graphsenselib.ingest.utxo``,
which re-exports them.
"""

from typing import Optional

# The ``p2pkh`` prefix here MUST match rpc_utxo._PUBKEY_ADDRESS_VERSION (the P2PK
# fast path) — the two are separate ingest code paths that derive the same P2PK
# output address, so a mismatch makes them disagree. Both are parity-tested
# against pubkey_to_address.MAINNET_ADDRESS_SPECS in
# tests/ingest/test_pubkey_address_version_parity.py.
#
# NOTE: DOGE is not ingested at the moment; its entry is kept only so the two
# ingest paths stay consistent if/when doge ingest is enabled (and so the parity
# test can guard it now rather than regress silently later).
_NETWORK_SCRIPT_PARAMS = {
    "btc": {"p2pkh": b"\x00", "p2sh": b"\x05", "bech32_hrp": "bc"},
    "bch": {"p2pkh": b"\x00", "p2sh": b"\x05", "bech32_hrp": "bc"},
    "ltc": {"p2pkh": b"\x30", "p2sh": b"\x32", "bech32_hrp": "ltc"},
    "doge": {"p2pkh": b"\x1e", "p2sh": b"\x16", "bech32_hrp": None},
    "zec": {"p2pkh": b"\x1c\xb8", "p2sh": b"\x1c\xbd", "bech32_hrp": None},
}

_BASE58_ALPHABET = frozenset(
    "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
)


def normalize_base58_p2pkh(address: Optional[str], network: str) -> Optional[str]:
    """Re-encode a base58check P2PKH address carrying BTC's version byte
    (0x00, leading ``1``) to ``network``'s own P2PKH version byte.

    Pre-2.14.4 ingests derived fallback addresses with btcpy's hardcoded
    ``mainnet=True`` (BTC version byte) on every network; stored LTC P2PK
    rows — and their spends, via input resolution against those rows — still
    carry that form. Only version byte 0x00 is rewritten: P2SH stays
    untouched because legacy LTC P2SH legitimately uses BTC's 0x05, and on
    the affected networks no legitimate encoding starts with a 0x00 version
    byte. No-op for networks sharing BTC's version byte, unknown networks,
    and strings that are not valid 0x00-prefixed base58check.
    """
    params = _NETWORK_SCRIPT_PARAMS.get(network)
    if params is None or params["p2pkh"] == b"\x00":
        return address
    if not address or address[0] != "1" or not set(address) <= _BASE58_ALPHABET:
        return address

    import base58

    from graphsenselib.utils.pubkey_to_address import base58check_encode, double_sha256

    raw = base58.b58decode(address)
    if len(raw) != 25 or raw[0] != 0x00 or double_sha256(raw[:-4])[:4] != raw[-4:]:
        return address
    return base58check_encode(params["p2pkh"], raw[1:-4])
