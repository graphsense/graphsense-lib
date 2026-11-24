import re

KNOWN_CURRENCIES = {
    # Base currencies
    "ETH": "Ether",
    "BTC": "Bitcoin",
    "XBT": "Bitcoin ISO 4217",
    "BCH": "Bitcoin Cash",
    "ZEC": "Z-Cash",
    "LTC": "Litecoin",
    "WETH": "Wrapped Ether",
    "XMR": "Monero",
    "DASH": "Dash",
    "BTG": "Bitcoin Gold",
    "ETC": "Ethereum Classic",
    "BSV": "Bitcoin Satoshi Vision",
    "XRP": "Ripple",
    "TRX": "Tron",
    "EOS": "EOS",
    "ALGO": "ALGO",
    # Token and second layer
    "ARB": "Arbitrum",
    "USDT": "Tether USD",
    "USDC": "Circle USD Coin",
    "DAI": "Dai",
    "KCS": "KuCoin Token",
    "MATIC": "Polygon Token",
    "POL": "Polygon Ecosystem Token",
    "SOL": "Solana",
    "AVAX": "Avalanche",
    "TON": "Toncoin",
}


KNOWN_NETWORKS = {
    "ETH": "Ethereum",
    "BTC": "Bitcoin",
    "BCH": "Bitcoin Cash",
    "ZEC": "Z-Cash",
    "LTC": "Litecoin",
    "BSC": "Binance Smartchain",
    "XMR": "Monero",
    "DASH": "Dash",
    "BTG": "Bitcoin Gold",
    "ETC": "Ethereum Classic",
    "BSV": "Bitcoin Satoshi Vision",
    "XRP": "Ripple",
    "ARB": "Arbitrum",
    "TRX": "Tron",
    "EOSIO": "EOSIO",
    "MATIC": "Polygon (formerly Matic)",
    "KCC": "Kucoin Community Chain",
    "ALGO": "Algorand",
    "OP": "Optimism",
    "POL": "Polygon PoS",
    "AVAX": "Avalanche C-Chain",
    "BASE": "Base",
    "SOL": "Solana",
    "TON": "TON",
}

# The chains a token lives on, used for (a) the "network is a token, not a
# chain" warning, which suggests these, and (b) address validation of tags
# whose network is the token: the address must be valid on one of them, so
# a chain missing here turns its valid addresses into "invalid" findings.
# Every chain listed needs a validator in utils/address.py, else the token
# cannot be checked at all. Chains a token has left (USDT on ALGO, EOSIO and
# Omni/BTC, USDC on TRX) stay, for older tags. Last reviewed 2026-10-23;
# stablecoins add chains every few months, so review it now and then.
NETWORK_SUGGESTIONS = {
    "USDT": [
        "ETH",
        "TRX",
        "BSC",
        "SOL",
        "TON",
        "ARB",
        "AVAX",
        "POL",
        "OP",
        "BASE",
        "KCC",
        "ALGO",
        "EOSIO",
        # Omni Layer: USDT's first chain (2014-2023), a protocol on top of
        # Bitcoin, so its addresses are Bitcoin addresses. Kept for older
        # tags; it also makes any valid BTC address valid under USDT.
        "BTC",
    ],
    "USDC": [
        "ETH",
        "SOL",
        "BASE",
        "ARB",
        "AVAX",
        "POL",
        "OP",
        "BSC",
        "TRX",
        "ALGO",
        "KCC",
    ],
    "DAI": ["ETH", "ARB", "OP", "POL", "BASE", "BSC", "AVAX"],
    "WETH": ["ETH", "ARB", "OP", "BASE", "POL", "BSC", "AVAX"],
    "KCS": ["ETH", "KCC"],
}


# Full chain names -> network code. The tagstore upper-cases a tag's network
# but resolves no synonyms, and cluster mapping only reads tags whose network
# is a GraphSense keyspace code, so a tag with network "bitcoin" shows on its
# address but never reaches its cluster. Keys are upper case, letters and
# digits only (see network_alias_key). Only unambiguous chain names belong
# here: no token standards (ERC20, TRC20, BEP20, which say nothing certain
# about the chain of a given address) and no key that is itself a code in
# KNOWN_NETWORKS (MATIC).
NETWORK_ALIASES = {
    # GraphSense keyspaces
    "BITCOIN": "BTC",
    "LITECOIN": "LTC",
    "BITCOINCASH": "BCH",
    "ZCASH": "ZEC",
    "ETHEREUM": "ETH",
    "TRON": "TRX",
    # other chains in KNOWN_NETWORKS
    "ARBITRUM": "ARB",
    "ARBITRUMONE": "ARB",
    "OPTIMISM": "OP",
    "POLYGON": "POL",
    "BNBSMARTCHAIN": "BSC",
    "BINANCESMARTCHAIN": "BSC",
    "BNBCHAIN": "BSC",
    "AVALANCHE": "AVAX",
    "AVALANCHECCHAIN": "AVAX",
    "AVAXCCHAIN": "AVAX",
    "SOLANA": "SOL",
    "TONCOIN": "TON",
    "ALGORAND": "ALGO",
    "RIPPLE": "XRP",
    "MONERO": "XMR",
    "ETHEREUMCLASSIC": "ETC",
    "BITCOINGOLD": "BTG",
    "BITCOINSV": "BSV",
    # chains without a KNOWN_NETWORKS entry yet
    "DOGECOIN": "DOGE",
    "CARDANO": "ADA",
    "STELLAR": "XLM",
    "POLKADOT": "DOT",
    "COSMOS": "ATOM",
    "TEZOS": "XTZ",
}


def network_alias_key(network) -> str:
    return re.sub(r"[^A-Z0-9]", "", str(network).strip().upper())


def suggest_network_code(network):
    """The network code a full chain name stands for ("Bitcoin" -> "BTC"),
    or None if the network is already a code or not a known chain name."""
    if str(network).strip().upper() in KNOWN_NETWORKS:
        return None
    return NETWORK_ALIASES.get(network_alias_key(network))


def suggest_networks_from_currency(currency, only_unknown=True):
    if currency in KNOWN_NETWORKS and not only_unknown:
        return [currency]
    return NETWORK_SUGGESTIONS.get(currency, [])


def is_known_network(network):
    return network in KNOWN_NETWORKS


def is_known_currency(currency):
    return currency in KNOWN_CURRENCIES


CONFIG_FILE = "config.yaml"

DEFAULT_CONFIG = {
    "taxonomies": {
        "concept": "src/tagpack/db/concepts.yaml",
        "confidence": "src/tagpack/db/confidence.csv",
        "country": "src/tagpack/db/countries.csv",
        "tag_type": "src/tagpack/db/tag_types.csv",
        "tag_subject": "src/tagpack/db/tag_subject.csv",
        "concept_relation_annotation": "src/tagpack/db/concept_relation_annotation.csv",
    }
}

DEFAULT_SCHEMA = "public"
