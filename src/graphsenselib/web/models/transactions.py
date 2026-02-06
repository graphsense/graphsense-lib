"""Transaction-related API models."""

from typing import Optional, Union

from pydantic import ConfigDict

from graphsenselib.web.models.base import APIModel
from graphsenselib.web.models.values import Values

TX_SUMMARY_EXAMPLE = {
    "height": 47,
    "timestamp": 123456789,
    "tx_hash": "04d92601677d62a985310b61a301e74870fa942c8be0648e16b1db23b996a8cd",
}


class TxSummary(APIModel):
    """Transaction summary model."""

    model_config = ConfigDict(
        populate_by_name=True,
        from_attributes=True,
        json_schema_extra={"example": TX_SUMMARY_EXAMPLE},
    )

    height: int
    timestamp: int
    tx_hash: str


class TxRef(APIModel):
    """Transaction reference model."""

    model_config = ConfigDict(
        populate_by_name=True,
        from_attributes=True,
        json_schema_extra={
            "example": {
                "input_index": 0,
                "output_index": 0,
                "tx_hash": "120439699b10bac37c5316834644acfa8eae6f17b370231e79a2d71ce90f0ebf",
            }
        },
    )

    input_index: int
    output_index: int
    tx_hash: str


class TxValue(APIModel):
    """Transaction value model for UTXO inputs/outputs."""

    model_config = ConfigDict(
        populate_by_name=True,
        from_attributes=True,
        json_schema_extra={
            "example": {
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
        },
    )

    address: list[str]
    value: Values
    index: Optional[int] = None


class TxUtxo(APIModel):
    """UTXO transaction model."""

    model_config = ConfigDict(
        populate_by_name=True,
        from_attributes=True,
        json_schema_extra={
            "example": {
                "tx_type": "utxo",
                "currency": "btc",
                "tx_hash": "04d92601677d62a985310b61a301e74870fa942c8be0648e16b1db23b996a8cd",
                "coinbase": False,
                "height": 47,
                "no_inputs": 2,
                "no_outputs": 2,
                "timestamp": 123456789,
                "total_input": {
                    "fiat_values": [
                        {"code": "eur", "value": 30},
                        {"code": "usd", "value": 60},
                    ],
                    "value": 15,
                },
                "total_output": {
                    "fiat_values": [
                        {"code": "eur", "value": 30},
                        {"code": "usd", "value": 60},
                    ],
                    "value": 15,
                },
                "inputs": [
                    {
                        "address": ["1Archive1n2C579dMsAu3iC6tWzuQJz8dN"],
                        "value": {
                            "fiat_values": [
                                {"code": "eur", "value": 10},
                                {"code": "usd", "value": 20},
                            ],
                            "value": 5,
                        },
                    },
                    {
                        "address": ["addressB"],
                        "value": {
                            "fiat_values": [
                                {"code": "eur", "value": 20},
                                {"code": "usd", "value": 40},
                            ],
                            "value": 10,
                        },
                    },
                ],
                "outputs": [
                    {
                        "address": ["addressC"],
                        "value": {
                            "fiat_values": [
                                {"code": "eur", "value": 5},
                                {"code": "usd", "value": 10},
                            ],
                            "value": 2,
                        },
                    },
                    {
                        "address": ["addressD"],
                        "value": {
                            "fiat_values": [
                                {"code": "eur", "value": 25},
                                {"code": "usd", "value": 50},
                            ],
                            "value": 12,
                        },
                    },
                ],
            }
        },
    )

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

    model_config = ConfigDict(
        populate_by_name=True,
        from_attributes=True,
        json_schema_extra={
            "example": {
                "tx_type": "account",
                "identifier": "eth",
                "currency": "ETH",
                "network": "eth",
                "tx_hash": "04d92601677d62a985310b61a301e74870fa942c8be0648e16b1db23b996a8cd",
                "height": 47,
                "timestamp": 123456789,
                "value": {
                    "fiat_values": [
                        {"code": "eur", "value": 30},
                        {"code": "usd", "value": 60},
                    ],
                    "value": 15,
                },
                "from_address": "addressA",
                "to_address": "addressB",
            }
        },
    )

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

    model_config = ConfigDict(
        populate_by_name=True,
        from_attributes=True,
        json_schema_extra={
            "example": {
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
        },
    )

    tx_type: str = "utxo"
    tx_hash: str
    currency: str
    coinbase: bool
    height: int
    timestamp: int
    value: Values


class AddressTxs(APIModel):
    """Paginated list of address transactions."""

    model_config = ConfigDict(
        populate_by_name=True,
        from_attributes=True,
        json_schema_extra={
            "example": {
                "address_txs": [
                    {
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
                    },
                    {
                        "tx_type": "utxo",
                        "tx_hash": "9586bd883cf255aa4207a3749cad61de34b6f1001bd25cecfac3a9237d57a84d",
                        "currency": "btc",
                        "coinbase": False,
                        "height": 690076,
                        "timestamp": 1625697035,
                        "value": {
                            "fiat_values": [
                                {"code": "eur", "value": -171698.17},
                                {"code": "usd", "value": -203136.12},
                            ],
                            "value": -600012259,
                        },
                    },
                ],
                "next_page": "00110004010d329a01010800000000211a20f0f07ffffff5f07ffffff5",
            }
        },
    )

    address_txs: list[Union[AddressTxUtxo, TxAccount]]
    next_page: Optional[str] = None


class LinkUtxo(APIModel):
    """UTXO link model."""

    model_config = ConfigDict(
        populate_by_name=True,
        from_attributes=True,
        json_schema_extra={
            "example": {
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
        },
    )

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

    model_config = ConfigDict(
        populate_by_name=True,
        from_attributes=True,
        json_schema_extra={
            "example": {
                "links": [
                    {
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
                    },
                    {
                        "tx_type": "utxo",
                        "tx_hash": "fd7cc9178688e3899eb9e6c1f2c985e66cf2c60a5233b850f28c2b8805c1a056",
                        "currency": "btc",
                        "height": 334056,
                        "timestamp": 1418427638,
                        "input_value": {
                            "fiat_values": [
                                {"code": "eur", "value": -2.84},
                                {"code": "usd", "value": -3.54},
                            ],
                            "value": -1010000,
                        },
                        "output_value": {
                            "fiat_values": [
                                {"code": "eur", "value": 2.82},
                                {"code": "usd", "value": 3.51},
                            ],
                            "value": 1000000,
                        },
                    },
                ],
            }
        },
    )

    links: list[Link]
    next_page: Optional[str] = None
