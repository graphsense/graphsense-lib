"""Transaction-related API models."""

from typing import Optional, Union

from graphsenselib.web.models.base import APIModel, api_model_config
from graphsenselib.web.models.values import VALUES_EXAMPLE, Values

TX_SUMMARY_EXAMPLE = {
    "height": 47,
    "timestamp": 123456789,
    "tx_hash": "04d92601677d62a985310b61a301e74870fa942c8be0648e16b1db23b996a8cd",
}

TX_REF_EXAMPLE = {
    "input_index": 0,
    "output_index": 0,
    "tx_hash": "120439699b10bac37c5316834644acfa8eae6f17b370231e79a2d71ce90f0ebf",
}

TX_VALUE_EXAMPLE = {
    "address": ["1GeUZK971B5Umn6JH47PbgrP9qW7RaeW95"],
    "value": {
        "fiat_values": [
            {"code": "eur", "value": 19.48},
            {"code": "usd", "value": 25.55},
        ],
        "value": 21200000,
    },
    "index": 0,
}

TX_UTXO_EXAMPLE = {
    "tx_type": "utxo",
    "currency": "btc",
    "tx_hash": "04d92601677d62a985310b61a301e74870fa942c8be0648e16b1db23b996a8cd",
    "coinbase": False,
    "height": 47,
    "no_inputs": 2,
    "no_outputs": 2,
    "timestamp": 123456789,
    "total_input": VALUES_EXAMPLE,
    "total_output": VALUES_EXAMPLE,
    "inputs": [
        {
            "address": ["1Archive1n2C579dMsAu3iC6tWzuQJz8dN"],
            "value": VALUES_EXAMPLE,
        },
        {
            "address": ["addressB"],
            "value": VALUES_EXAMPLE,
        },
    ],
    "outputs": [
        {
            "address": ["addressC"],
            "value": VALUES_EXAMPLE,
        },
        {
            "address": ["addressD"],
            "value": VALUES_EXAMPLE,
        },
    ],
}

TX_ACCOUNT_EXAMPLE = {
    "tx_type": "account",
    "identifier": "eth",
    "currency": "ETH",
    "network": "eth",
    "tx_hash": "04d92601677d62a985310b61a301e74870fa942c8be0648e16b1db23b996a8cd",
    "height": 47,
    "timestamp": 123456789,
    "value": VALUES_EXAMPLE,
    "from_address": "addressA",
    "to_address": "addressB",
}

ADDRESS_TX_UTXO_EXAMPLE = {
    "tx_type": "utxo",
    "tx_hash": "0fa34dfc851611b747725f315ea156e2ce931674560f9750ed30be0f786e0d05",
    "currency": "btc",
    "coinbase": False,
    "height": 690083,
    "timestamp": 1625703347,
    "value": {
        "fiat_values": [
            {"code": "eur", "value": -18.19},
            {"code": "usd", "value": -21.53},
        ],
        "value": -65485,
    },
}

LINK_UTXO_EXAMPLE = {
    "tx_type": "utxo",
    "tx_hash": "5805ba651d6b72eda3d6e34e43d44c6df20c455f0488acecc75fd6165d6d2590",
    "currency": "btc",
    "height": 334069,
    "timestamp": 1418432879,
    "input_value": {
        "fiat_values": [
            {"code": "eur", "value": -6352.06},
            {"code": "usd", "value": -7916.22},
        ],
        "value": -2278238097,
    },
    "output_value": {
        "fiat_values": [
            {"code": "eur", "value": 6630.22},
            {"code": "usd", "value": 8262.87},
        ],
        "value": 2378000000,
    },
}


class TxSummary(APIModel):
    """Transaction summary model."""

    model_config = api_model_config(TX_SUMMARY_EXAMPLE)

    height: int
    timestamp: int
    tx_hash: str


class TxRef(APIModel):
    """Transaction reference model."""

    model_config = api_model_config(TX_REF_EXAMPLE)

    input_index: int
    output_index: int
    tx_hash: str


class TxValue(APIModel):
    """Transaction value model for UTXO inputs/outputs."""

    model_config = api_model_config(TX_VALUE_EXAMPLE)

    address: list[str]
    value: Values
    index: Optional[int] = None


class TxUtxo(APIModel):
    """UTXO transaction model."""

    model_config = api_model_config(TX_UTXO_EXAMPLE)

    tx_type: str = "utxo"
    currency: str
    tx_hash: str
    coinbase: bool
    height: int
    no_inputs: int
    no_outputs: int
    timestamp: int
    total_input: Values
    total_output: Values
    inputs: Optional[list[TxValue]] = None
    outputs: Optional[list[TxValue]] = None


class TxAccount(APIModel):
    """Account-based transaction model."""

    model_config = api_model_config(TX_ACCOUNT_EXAMPLE)

    tx_type: str = "account"
    identifier: str
    currency: str
    network: str
    tx_hash: str
    height: int
    timestamp: int
    value: Values
    from_address: str
    to_address: str
    token_tx_id: Optional[int] = None
    fee: Optional[Values] = None
    contract_creation: Optional[bool] = None
    is_external: Optional[bool] = None


# Union type for transactions
Tx = Union[TxUtxo, TxAccount]


class Txs(APIModel):
    """Paginated list of transactions."""

    txs: list[Tx]
    next_page: Optional[str] = None


class AddressTxUtxo(APIModel):
    """UTXO transaction for an address."""

    model_config = api_model_config(ADDRESS_TX_UTXO_EXAMPLE)

    tx_type: str = "utxo"
    tx_hash: str
    currency: str
    coinbase: bool
    height: int
    timestamp: int
    value: Values


class AddressTxs(APIModel):
    """Paginated list of address transactions."""

    model_config = api_model_config(
        {
            "address_txs": [ADDRESS_TX_UTXO_EXAMPLE],
            "next_page": "00110004010d329a01010800000000211a20f0f07ffffff5f07ffffff5",
        }
    )

    address_txs: list[Union[AddressTxUtxo, TxAccount]]
    next_page: Optional[str] = None


class LinkUtxo(APIModel):
    """UTXO link model."""

    model_config = api_model_config(LINK_UTXO_EXAMPLE)

    tx_type: str = "utxo"
    tx_hash: str
    currency: str
    height: int
    timestamp: int
    input_value: Values
    output_value: Values


# Links can be of different types
Link = Union[LinkUtxo, AddressTxUtxo, TxAccount]


class Links(APIModel):
    """Paginated list of links."""

    model_config = api_model_config({"links": [LINK_UTXO_EXAMPLE]})

    links: list[Link]
    next_page: Optional[str] = None
