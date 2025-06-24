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
    "KCS": "KuCoin Token",
    "MATIC": "Polygon Token",
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
}

NETWORK_SUGGESTIONS = {
    "USDT": ["ETH", "ARB", "ETC", "BSC", "TRX", "ALGO", "EOSIO", "KCC"],
    "USDC": ["ETH", "ARB", "ETC", "BSC", "TRX", "OP", "KCC", "ALGO"],
    "KCS": ["ETH", "KCC"],
}


def suggest_networks_from_currency(currency, only_unknown=True):
    if currency in KNOWN_NETWORKS and not only_unknown:
        return [currency]
    return NETWORK_SUGGESTIONS.get(currency, [])


def is_known_network(network):
    return network in KNOWN_NETWORKS


def is_known_currency(currency):
    return currency in KNOWN_CURRENCIES
