"""Transaction-related API models."""

from typing import Optional, Union

from graphsenselib.web.models.base import APIModel
from graphsenselib.web.models.values import Values


class TxSummary(APIModel):
    """Transaction summary model."""

    height: int
    timestamp: int
    tx_hash: str


class TxRef(APIModel):
    """Transaction reference model."""

    input_index: int
    output_index: int
    tx_hash: str


class TxValue(APIModel):
    """Transaction value model for UTXO inputs/outputs."""

    address: list[str]
    value: Values
    index: Optional[int] = None


class TxUtxo(APIModel):
    """UTXO transaction model."""

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

    tx_type: str = "utxo"
    tx_hash: str
    currency: str
    coinbase: bool
    height: int
    timestamp: int
    value: Values


class AddressTxs(APIModel):
    """Paginated list of address transactions."""

    address_txs: list[Union[AddressTxUtxo, TxAccount]]
    next_page: Optional[str] = None


class LinkUtxo(APIModel):
    """UTXO link model."""

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

    links: list[Link]
    next_page: Optional[str] = None
