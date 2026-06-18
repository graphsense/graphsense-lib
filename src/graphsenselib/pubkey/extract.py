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


_OP_CHECKMULTISIG = 0xAE
_OP_0 = 0x00
_OP_1 = 0x51
_OP_16 = 0x60


def _script_to_bytes(script_hex: Any) -> Optional[bytes]:
    """Coerce a Spark binary / hex-string scriptSig into bytes (or None)."""
    if not script_hex:
        return None
    if isinstance(script_hex, (bytes, bytearray)):
        return bytes(script_hex)
    try:
        return bytes.fromhex(
            script_hex[2:] if script_hex.startswith("0x") else script_hex
        )
    except (ValueError, AttributeError):
        return None


def _pubkeys_from_multisig_script(script: bytes) -> List[bytes]:
    """Extract every compressed pubkey from a multisig redeem/witness script.

    Mirrors the legacy ``parse_sh_ms``: a bare/redeem/witness multisig script
    is ``OP_M <pk1> … <pkN> OP_N OP_CHECKMULTISIG`` — it starts with an OP_M
    opcode (OP_0 or OP_1..OP_16) and ends with OP_CHECKMULTISIG (0xAE). The
    OP_M/OP_N numeric opcodes are skipped by ``_parse_script_pushes`` (non-push),
    leaving the 33/65-byte key pushes, which we curve-validate and normalise.
    The start/end opcode gate rejects bare keys (start 0x02/0x03/0x04) and DER
    sigs (start 0x30) without any length heuristic.
    """
    if not script or script[-1] != _OP_CHECKMULTISIG:
        return []
    if not (script[0] == _OP_0 or _OP_1 <= script[0] <= _OP_16):
        return []
    out: List[bytes] = []
    for push in _parse_script_pushes(script):
        if len(push) in (33, 65):
            compressed = _to_compressed(push)
            if compressed is not None:
                out.append(compressed)
    return out


def _pubkeys_from_input(input_row: Dict[str, Any]) -> List[bytes]:
    """Extract all signing pubkeys revealed by one UTXO input.

    Covers the same input-side cases the legacy extractor did:

    * P2PKH       — scriptSig ``<sig> <pubkey>`` (last key push).
    * P2WPKH / P2SH-P2WPKH — ``[sig, pubkey]`` in the witness.
    * P2SH-P2PKH  — scriptSig ``<sig> <pubkey> <redeemScript>`` (key push).
    * P2SH multisig — redeem script (last scriptSig push) holds N keys.
    * P2WSH multisig — witness script (last witness element) holds N keys.

    P2PK and bare P2MS reveal their keys in the *output* script, not the
    spending input, so they are handled separately by the output-side
    extractor. Returns ``[]`` when no key is recoverable (coinbase, taproot
    key-path, …). Deduplicated within the input; callers dedupe across the tx.
    """
    out: List[bytes] = []
    seen: set[bytes] = set()

    def _add(pk: Optional[bytes]) -> None:
        if pk is not None and pk not in seen:
            seen.add(pk)
            out.append(pk)

    witness = input_row.get("txinwitness")
    if witness:
        elems = [bytes(e) for e in witness if e is not None]
        # P2WPKH / nested P2SH-P2WPKH: a standalone pubkey witness element.
        for elem in elems:
            _add(_to_compressed(elem))
        # P2WSH multisig: the last witness element is the witnessScript.
        if elems:
            for pk in _pubkeys_from_multisig_script(elems[-1]):
                _add(pk)

    script_bytes = _script_to_bytes(input_row.get("script_hex"))
    if script_bytes:
        pushes = _parse_script_pushes(script_bytes)
        # P2PKH / P2SH-P2PKH: the signing key is the last key-shaped push.
        # Iterate from the end so trailing redeem-script pushes are skipped.
        for push in reversed(pushes):
            compressed = _to_compressed(push)
            if compressed is not None:
                _add(compressed)
                break
        # P2SH multisig: the redeem script is the last scriptSig push.
        if pushes:
            for pk in _pubkeys_from_multisig_script(pushes[-1]):
                _add(pk)
    return out


def _pubkey_from_input(input_row: Dict[str, Any]) -> Optional[bytes]:
    """Back-compat single-key view of :func:`_pubkeys_from_input`.

    Returns the first recovered signing pubkey (the canonical single signer
    for P2PKH/P2WPKH), or None. Prefer :func:`_pubkeys_from_input`.
    """
    keys = _pubkeys_from_input(input_row)
    return keys[0] if keys else None


def extract_pubkeys_utxo(inputs: Iterable[Dict[str, Any]]) -> List[bytes]:
    """Extract compressed pubkeys from all inputs of a single UTXO tx.

    Covers P2PKH, P2WPKH, nested P2SH-P2WPKH, P2SH-P2PKH and P2SH/P2WSH
    multisig (one input can yield several keys). Drops inputs with no
    recoverable secp256k1 key (coinbase, taproot key-path, …). The returned
    list may contain duplicates (a key signing several inputs); callers dedupe.
    """
    if not inputs:
        return []
    out: List[bytes] = []
    for inp in inputs:
        if inp is None:
            continue
        out.extend(_pubkeys_from_input(inp))
    return out


_OP_CHECKSIG = 0xAC


def _pubkeys_from_output(output_row: Dict[str, Any]) -> List[bytes]:
    """Extract pubkeys revealed directly in one UTXO *output* script.

    These keys never need the output to be spent (the legacy extractor read
    them from outputs):

    * P2PK — ``<pubkey> OP_CHECKSIG`` (script ends 0xAC, one key push).
    * bare P2MS — ``OP_M <pk1>…<pkN> OP_N OP_CHECKMULTISIG`` (ends 0xAE).

    A P2PKH output (``…OP_EQUALVERIFY OP_CHECKSIG``) also ends in 0xAC but its
    only push is the 20-byte hash, which fails the 33/65 key-length filter, so
    no false key is produced. Returns ``[]`` for all hash-style outputs
    (P2PKH/P2SH/P2WPKH/P2WSH/taproot).
    """
    script = _script_to_bytes(output_row.get("script_hex"))
    if not script:
        return []
    out: List[bytes] = []
    seen: set[bytes] = set()

    def _add(pk: Optional[bytes]) -> None:
        if pk is not None and pk not in seen:
            seen.add(pk)
            out.append(pk)

    # P2PK: a single key push followed by OP_CHECKSIG.
    if script[-1] == _OP_CHECKSIG:
        for push in _parse_script_pushes(script):
            if len(push) in (33, 65):
                _add(_to_compressed(push))
    # bare P2MS: OP_CHECKMULTISIG-terminated multi-key script.
    for pk in _pubkeys_from_multisig_script(script):
        _add(pk)
    return out


def extract_pubkeys_utxo_outputs(outputs: Iterable[Dict[str, Any]]) -> List[bytes]:
    """Extract compressed pubkeys from all outputs of a single UTXO tx.

    Covers P2PK and bare P2MS, whose keys live in the output script. All other
    output types yield nothing. May contain duplicates; callers dedupe.
    """
    if not outputs:
        return []
    out: List[bytes] = []
    for outp in outputs:
        if outp is None:
            continue
        out.extend(_pubkeys_from_output(outp))
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


def _delta_row_to_rpc_shape(tx_row: Dict[str, Any], chain_id: int) -> Dict[str, Any]:
    """Map a delta ``transaction`` row to the RPC-JSON shape that
    ``signature.eth_get_signature_data_from_rpc_json`` consumes.

    The delta lake stores snake_case field names and big-endian binary
    varints; the signature helpers expect RPC-JSON camelCase keys with
    int / bytes / 0x-hex values. This adapter only bridges naming and
    encoding — the per-transaction-type field selection and EIP-155
    chainId rules stay defined once in ``signature.type_to_signature_fields_eth``.

    All keys any tx type might need are supplied; the signature helper reads
    only the subset its ``type`` requires, so extra keys are harmless.
    """
    to = tx_row.get("to_address")
    return {
        "type": int(tx_row.get("transaction_type") or 0),
        "nonce": _as_int_be(tx_row.get("nonce")),
        "gas": _as_int_be(tx_row.get("gas")),
        "gasPrice": _as_int_be(tx_row.get("gas_price")),
        "to": bytes(to) if to is not None else b"",
        "input": bytes(tx_row.get("input") or b""),
        "value": _as_int_be(tx_row.get("value")),
        "chainId": chain_id,
        "maxFeePerGas": _as_int_be(tx_row.get("max_fee_per_gas")),
        "maxPriorityFeePerGas": _as_int_be(tx_row.get("max_priority_fee_per_gas")),
        "maxFeePerBlobGas": _as_int_be(tx_row.get("max_fee_per_blob_gas")),
        "blobVersionedHashes": [
            bytes(h) for h in (tx_row.get("blob_versioned_hashes") or [])
        ],
        "accessList": _convert_access_list(tx_row.get("access_list")),
        "v": _as_int_be(tx_row.get("v")),
    }


def _normalize_evm_addr(value: Any) -> Optional[str]:
    """Lower-case 40-hex-char EVM address (no 0x) from bytes / hex string."""
    if value is None:
        return None
    if isinstance(value, (bytes, bytearray)):
        return bytes(value).hex().lower()
    if isinstance(value, str):
        v = value[2:] if value.startswith("0x") else value
        return v.lower()
    return None


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

    Self-check (the legacy extractor had it, the first port dropped it): for
    ETH the signer's address IS ``keccak(pubkey)[12:]`` by definition, so when
    ``from_address`` is present the recovered key's address MUST equal it — a
    mismatch can only mean a bad hash reconstruction or wrong ``v``, so we drop
    the row. TRX is NOT dropped on mismatch: ``from_address`` there is the
    contract ``owner_address``, which legitimately differs from the signer for
    multisig / permission accounts, so the check would discard valid keys.

    Returns ``None`` if recovery fails (missing signature, type mismatch,
    off-curve key) or the ETH from-address self-check fails.
    """
    from graphsenselib.utils.signature import (  # deferred — heavy eth-account dep
        eth_get_msg_hash_from_signature_data,
        eth_get_signature_data_from_rpc_json,
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
            rpc_shape = _delta_row_to_rpc_shape(tx_row, resolved_chain_id or 1)
            sig_data = eth_get_signature_data_from_rpc_json(rpc_shape)
            msg_hash = eth_get_msg_hash_from_signature_data(sig_data)

        recovered = eth_recover_pubkey((v_int, r_int, s_int), msg_hash)

        # ETH-strict from-address self-check (skip for TRX — see docstring).
        if currency != "trx":
            expected = _normalize_evm_addr(tx_row.get("from_address"))
            if expected is not None:
                derived = recovered.to_address()  # '0x' + 40 lowercase hex
                if derived[2:] != expected:
                    return None

        # PublicKey.to_bytes() returns 64-byte uncompressed-without-prefix.
        uncompressed = b"\x04" + recovered.to_bytes()
        return secp256k1_compress(uncompressed)
    except Exception:
        return None
