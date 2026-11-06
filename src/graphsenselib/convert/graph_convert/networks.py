"""Chain-agnostic identifiers for networks, addresses and assets.

Network ids follow CAIP-2 (``<namespace>:<reference>``), address ids CAIP-10
(``<caip2>:<address>``) and native asset ids CAIP-19
(``<caip2>/slip44:<coin type>``). A network without a known CAIP-2 id falls
back to ``graphsense:<code>``, so every id stays unique and parseable, just
not portable.

For ``bip122`` the reference is the first 32 hex characters of the genesis
block hash; Bitcoin Cash uses the hash of its fork block (478559) instead,
since it shares Bitcoin's genesis block.
"""

from __future__ import annotations

from graphsenselib.convert.gs_files import normalize_address_id

FALLBACK_NAMESPACE = "graphsense"

_CAIP2: dict[str, str] = {
    "btc": "bip122:000000000019d6689c085ae165831e93",
    "bch": "bip122:000000000000000000651ef99cb9fcbe",
    "ltc": "bip122:12a765e31ffd4059bada1e25190f6e98",
    "zec": "bip122:00040fe8ec8471911baa1db1266ea15d",
    "eth": "eip155:1",
}

# SLIP-44 coin types of the native assets.
_SLIP44: dict[str, int] = {
    "btc": 0,
    "ltc": 2,
    "zec": 133,
    "bch": 145,
    "eth": 60,
    "trx": 195,
}


def caip2(network: str) -> str:
    """CAIP-2 id of a GraphSense network code, e.g. ``eth`` -> ``eip155:1``."""
    code = network.lower()
    return _CAIP2.get(code, f"{FALLBACK_NAMESPACE}:{code}")


def normalize_address(address: str) -> str:
    """The one spelling an address is exported in.

    EVM hex addresses are lowercased (no EIP-55 checksum casing); everything
    else, including Tron base58, is kept as GraphSense spells it.
    """
    return normalize_address_id(address)


def address_id(network: str, address: str) -> str:
    """CAIP-10 account id, e.g. ``eip155:1:0xabc…``."""
    return f"{caip2(network)}:{normalize_address(address)}"


def tx_id(network: str, identifier: str) -> str:
    """Node id of a transaction: ``<caip2>:tx:<identifier>``.

    Not a CAIP standard. ``identifier`` keeps an account-model sub-payment
    suffix (``_T3``, ``_I12``), so each transfer in one hash is its own node.
    """
    return f"{caip2(network)}:tx:{identifier}"


def asset_id(network: str, asset: str) -> str | None:
    """CAIP-19 id of ``asset`` on ``network``, or None when unknown.

    Only native assets are mapped so far; tokens need their contract
    address, which the tx body does not carry.
    """
    code, asset = network.lower(), asset.lower()
    if asset != code or code not in _SLIP44:
        return None
    return f"{caip2(code)}/slip44:{_SLIP44[code]}"
