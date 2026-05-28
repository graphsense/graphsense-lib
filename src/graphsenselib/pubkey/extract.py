"""Per-tx signing-pubkey extraction for UTXO and account-model chains.

All recovered pubkeys are normalised to compressed secp256k1 form (33 bytes)
so that cross-chain identity matching is unambiguous regardless of which side
provided the key in compressed vs. uncompressed form.

Functions here are intended to be wrapped in Spark UDFs (see ``job.py``);
they are pure Python with no Spark dependency so they can be unit-tested
directly.
"""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional

from graphsenselib.utils.ec import is_valid_secp256k1_pubkey, secp256k1_compress

# Default mainnet chain IDs used to reconstruct the EIP-155 signing message
# for legacy transactions whose chain_id is not stored in the delta lake
# (the JSON-RPC ingest blacklists chainId — see ingest/rpc_eth._TX_BLACKLIST).
_DEFAULT_CHAIN_ID = {
    "eth": 1,
    # TRX msg_hash is just the tx_hash (sha256 of raw_data); chain_id unused.
    "trx": None,
}


# ---------------------------------------------------------------------------
# Compressed-form normalisation
# ---------------------------------------------------------------------------


def _to_compressed(pubkey: bytes) -> Optional[bytes]:
    """Return ``pubkey`` as a 33-byte compressed secp256k1 key, or None.

    Accepts 33-byte compressed (0x02/0x03 prefix) and 65-byte uncompressed
    (0x04 prefix). Any other length, malformed prefix, or off-curve point
    yields ``None`` so callers can drop the row.
    """
    if not pubkey:
        return None
    if len(pubkey) == 33 and pubkey[0] in (0x02, 0x03):
        try:
            if not is_valid_secp256k1_pubkey(pubkey):
                return None
        except Exception:
            return None
        return bytes(pubkey)
    if len(pubkey) == 65 and pubkey[0] == 0x04:
        try:
            if not is_valid_secp256k1_pubkey(pubkey):
                return None
            return secp256k1_compress(bytes(pubkey))
        except Exception:
            return None
    return None


# ---------------------------------------------------------------------------
# UTXO: parse scriptSig pushes; pick pubkey from witness or scriptSig
# ---------------------------------------------------------------------------


def _parse_script_pushes(script: bytes) -> List[bytes]:
    """Return all data pushes in a Bitcoin-style script.

    Recognises the standard push opcodes (0x01..0x4b for inline pushes,
    OP_PUSHDATA1/2/4). Stops at the first malformed opcode; non-push
    opcodes are skipped so that scripts like ``<sig> <pubkey>`` (the
    canonical P2PKH scriptSig) yield exactly two payloads regardless of
    any leading or trailing junk.
    """
    pushes: List[bytes] = []
    i = 0
    n = len(script)
    while i < n:
        op = script[i]
        i += 1
        if 0x01 <= op <= 0x4B:
            length = op
            if i + length > n:
                break
            pushes.append(bytes(script[i : i + length]))
            i += length
        elif op == 0x4C:  # OP_PUSHDATA1
            if i + 1 > n:
                break
            length = script[i]
            i += 1
            if i + length > n:
                break
            pushes.append(bytes(script[i : i + length]))
            i += length
        elif op == 0x4D:  # OP_PUSHDATA2
            if i + 2 > n:
                break
            length = int.from_bytes(script[i : i + 2], "little")
            i += 2
            if i + length > n:
                break
            pushes.append(bytes(script[i : i + length]))
            i += length
        elif op == 0x4E:  # OP_PUSHDATA4
            if i + 4 > n:
                break
            length = int.from_bytes(script[i : i + 4], "little")
            i += 4
            if i + length > n:
                break
            pushes.append(bytes(script[i : i + length]))
            i += length
        # else: non-push opcode — skip the single byte already consumed
    return pushes


def _pubkey_from_input(input_row: Dict[str, Any]) -> Optional[bytes]:
    """Extract a single compressed pubkey from one UTXO input.

    Looks at the segwit witness first (P2WPKH and the nested P2SH-P2WPKH
    case both put ``[sig, pubkey]`` in the witness), then falls back to
    parsing scriptSig pushes for legacy P2PKH. Returns None when no
    plausible secp256k1 pubkey can be recovered (P2PK, P2SH multisig
    without a single signing key, taproot key-path, coinbase, …).
    """
    witness = input_row.get("txinwitness")
    if witness:
        for elem in witness:
            if elem is None:
                continue
            elem_b = bytes(elem)
            compressed = _to_compressed(elem_b)
            if compressed is not None:
                return compressed

    script_hex = input_row.get("script_hex")
    if script_hex:
        if isinstance(script_hex, (bytes, bytearray)):
            script_bytes = bytes(script_hex)
        else:
            # Spark passes binary as bytes; handle hex string defensively
            # for unit-test calls and dict fixtures.
            try:
                script_bytes = bytes.fromhex(
                    script_hex[2:] if script_hex.startswith("0x") else script_hex
                )
            except (ValueError, AttributeError):
                return None
        pushes = _parse_script_pushes(script_bytes)
        # Canonical P2PKH scriptSig is exactly <sig> <pubkey>; the pubkey
        # is the last push. Iterate from the end so we still find it in
        # the unusual case of trailing OP_RETURN-style noise.
        for push in reversed(pushes):
            compressed = _to_compressed(push)
            if compressed is not None:
                return compressed
    return None


def extract_pubkeys_utxo(inputs: Iterable[Dict[str, Any]]) -> List[bytes]:
    """Extract compressed pubkeys from all inputs of a single UTXO tx.

    Drops inputs whose witness/scriptSig does not encode a recoverable
    secp256k1 pubkey (coinbase, P2PK, P2WSH, taproot key-path, …). The
    returned list may contain duplicates if the same pubkey signs several
    inputs in one tx; callers should dedupe.
    """
    if not inputs:
        return []
    out: List[bytes] = []
    for inp in inputs:
        if inp is None:
            continue
        pk = _pubkey_from_input(inp)
        if pk is not None:
            out.append(pk)
    return out


# ---------------------------------------------------------------------------
# Account model: ECDSA recovery from (v, r, s) + reconstructed msg_hash
# ---------------------------------------------------------------------------


def _as_int_be(value: Any) -> int:
    """Coerce a delta-stored varint column into a Python int.

    Delta lake stores ETH varints as big-endian binary blobs, but unit
    tests pass plain ints; accept either.
    """
    if value is None:
        return 0
    if isinstance(value, int):
        return value
    if isinstance(value, (bytes, bytearray)):
        return int.from_bytes(value, "big", signed=False)
    if isinstance(value, str):
        return int(value, 16) if value.startswith("0x") else int(value)
    raise TypeError(f"Unsupported varint value type: {type(value)}")


def _to_hex0x(value: Any) -> str:
    """Coerce bytes / bytearray / hex string into a 0x-prefixed hex string."""
    if value is None:
        return "0x"
    if isinstance(value, (bytes, bytearray)):
        return "0x" + bytes(value).hex()
    if isinstance(value, str):
        return value if value.startswith("0x") else "0x" + value
    raise TypeError(f"Cannot coerce {type(value)!r} to hex string")


def _convert_access_list(access_list: Optional[Iterable[Any]]) -> List[Dict[str, Any]]:
    """Convert the delta ``access_list`` column into eth_account's expected form.

    Delta rows expose entries as Spark structs ``(address, storageKeys)`` (the
    JSON-RPC camelCase is preserved at ingest; see ingest/account.py).
    eth_account's ``serializable_unsigned_transaction_from_dict`` validates
    type-1/2/3 access-list fields as hex strings (not bytes), so coerce here.
    Address/key payloads may arrive as bytes (delta) or 0x-hex (raw JSON-RPC).
    """
    if not access_list:
        return []
    out: List[Dict[str, Any]] = []
    for entry in access_list:
        if entry is None:
            continue
        if hasattr(entry, "asDict"):
            entry = entry.asDict(recursive=True)
        if isinstance(entry, dict):
            address = entry.get("address")
            keys = entry.get("storageKeys") or entry.get("storage_keys") or []
        else:
            address, keys = entry[0], entry[1] or []
        out.append(
            {
                "address": _to_hex0x(address),
                "storageKeys": [_to_hex0x(k) for k in keys],
            }
        )
    return out


def _build_eth_signature_data(tx_row: Dict[str, Any], chain_id: int) -> Dict[str, Any]:
    """Build the dict consumed by ``eth_get_msg_hash_from_signature_data``."""
    tx_type = int(tx_row.get("transaction_type") or 0)
    to = tx_row.get("to_address")
    base = {
        "nonce": _as_int_be(tx_row.get("nonce")),
        "gas": _as_int_be(tx_row.get("gas")),
        "to": bytes(to) if to is not None else b"",
        "value": _as_int_be(tx_row.get("value")),
        "data": bytes(tx_row.get("input") or b""),
    }
    if tx_type == 0:
        base["gasPrice"] = _as_int_be(tx_row.get("gas_price"))
        v_int = _as_int_be(tx_row.get("v"))
        if v_int not in (27, 28):
            base["chainId"] = chain_id
        return base
    if tx_type == 1:
        base.update(
            {
                "type": 1,
                "chainId": chain_id,
                "gasPrice": _as_int_be(tx_row.get("gas_price")),
                "accessList": _convert_access_list(tx_row.get("access_list")),
            }
        )
        return base
    if tx_type == 2:
        base.update(
            {
                "type": 2,
                "chainId": chain_id,
                "maxFeePerGas": _as_int_be(tx_row.get("max_fee_per_gas")),
                "maxPriorityFeePerGas": _as_int_be(
                    tx_row.get("max_priority_fee_per_gas")
                ),
                "accessList": _convert_access_list(tx_row.get("access_list")),
            }
        )
        return base
    if tx_type == 3:
        base.update(
            {
                "type": 3,
                "chainId": chain_id,
                "maxFeePerGas": _as_int_be(tx_row.get("max_fee_per_gas")),
                "maxPriorityFeePerGas": _as_int_be(
                    tx_row.get("max_priority_fee_per_gas")
                ),
                "accessList": _convert_access_list(tx_row.get("access_list")),
                "maxFeePerBlobGas": _as_int_be(tx_row.get("max_fee_per_blob_gas")),
                "blobVersionedHashes": [
                    bytes(h) for h in (tx_row.get("blob_versioned_hashes") or [])
                ],
            }
        )
        return base
    raise ValueError(f"Unsupported ETH transaction_type: {tx_type}")


def extract_pubkey_account(
    tx_row: Dict[str, Any],
    currency: str,
    chain_id: Optional[int] = None,
) -> Optional[bytes]:
    """Recover the compressed signing pubkey from one account-model tx row.

    ETH (and EIP-style L2s): reconstruct the unsigned-tx hash from the
    delta-stored fields using ``signature.py`` and recover the pubkey via
    ECDSA with (v, r, s). TRX: the tx_hash *is* the signed message digest
    (sha256 of the protobuf raw_data) — no reconstruction needed.

    Returns ``None`` if recovery fails (missing signature, type mismatch,
    off-curve key).
    """
    from graphsenselib.utils.signature import (  # deferred — heavy eth-account dep
        eth_get_msg_hash_from_signature_data,
        eth_recover_pubkey,
    )

    v = tx_row.get("v")
    r = tx_row.get("r")
    s = tx_row.get("s")
    if v is None or r is None or s is None:
        return None
    try:
        v_int = _as_int_be(v)
        r_int = _as_int_be(r)
        s_int = _as_int_be(s)
        if r_int == 0 and s_int == 0:
            # TRX rows that come through without a usable signature land here.
            return None

        if currency == "trx":
            tx_hash = tx_row.get("tx_hash")
            if tx_hash is None:
                return None
            msg_hash = bytes(tx_hash)
        else:
            resolved_chain_id = (
                chain_id if chain_id is not None else _DEFAULT_CHAIN_ID.get(currency, 1)
            )
            sig_data = _build_eth_signature_data(tx_row, resolved_chain_id or 1)
            msg_hash = eth_get_msg_hash_from_signature_data(sig_data)

        recovered = eth_recover_pubkey((v_int, r_int, s_int), msg_hash)
        # PublicKey.to_bytes() returns 64-byte uncompressed-without-prefix.
        uncompressed = b"\x04" + recovered.to_bytes()
        return secp256k1_compress(uncompressed)
    except Exception:
        return None
