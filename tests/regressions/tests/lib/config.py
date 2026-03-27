"""Shared configuration maps for regression test infrastructure."""

# Maps currency codes to their schema types.
SCHEMA_TYPE_MAP = {
    "btc": "utxo",
    "ltc": "utxo",
    "bch": "utxo",
    "zec": "utxo",
    "eth": "account",
    "trx": "account_trx",
}

SCHEMA_TABLES_MAP_KEYS = {
    "utxo",
    "account",
    "account_trx",
}
