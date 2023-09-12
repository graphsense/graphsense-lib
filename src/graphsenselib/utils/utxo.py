from collections import Counter
from dataclasses import dataclass
from typing import Iterable, List, Set

from ..datatypes import FlowDirection
from .generic import flatten


@dataclass
class SlimTx:
    address: str
    block_id: int
    timestamp: int
    tx_hash: str
    direction: FlowDirection


def filter_inoutputs(inoutputs_list: list) -> List:
    if inoutputs_list is None:
        return []
    return [
        (inout.address[0], inout.value)
        for inout in inoutputs_list
        if inout.address is not None and len(inout.address) == 1
    ]


def regularize_inoutputs(inoutputs_list: list) -> dict:
    """
    Sums the in/outputs for the same address in a inputput list of a tx

    Args:
        inoutputs_list (list): Inout-list like stored in raw keyspace

    Returns:
        dict: keyed by address.
    """
    # Ignore inouts with more than one address (copied form spark transform)
    if inoutputs_list is None:
        return {}
    r = filter_inoutputs(inoutputs_list)
    cnt = Counter()
    for adr, value in r:
        cnt[adr] += value
    return dict(cnt)


def get_total_input_sum(input_list: list) -> int:
    """Simple sum of all input

    Args:
        input_list (list): list of inputs

    Returns:
        int: sum of input
    """
    if input_list is None:
        return 0
    return sum([inp.value for inp in input_list])


def get_regflow(regin: dict, regout: dict, address: str) -> int:
    """Calculates the in/out flow an address.

    Args:
        regin (dict): regularized inputs
        regout (dict): regularized outputs
        address (str): address of interest

    Returns:
        int: Negative if net outflow, positive if net inflow
    """
    return regout.get(address, 0) - regin.get(address, 0)


def get_unique_addresses_from_transaction(transaction) -> Iterable[str]:
    return {adr.address for adr in get_slim_tx_from_transaction(transaction)}


def get_slim_tx_from_transaction(transaction) -> Iterable[SlimTx]:
    # Only take first address from address array
    # this is equivalent to the spark job, but is ignoring multisig

    output_addresses = [
        (address, FlowDirection.OUT)
        for (address, value) in filter_inoutputs(transaction.outputs)
    ]
    input_addresses = [
        (address, FlowDirection.IN)
        for (address, value) in filter_inoutputs(transaction.inputs)
    ]

    addresses = input_addresses + output_addresses
    return [
        SlimTx(
            addr,
            transaction.block_id,
            transaction.timestamp,
            transaction.tx_hash,
            direction,
        )
        for addr, direction in addresses
    ]


def get_slim_tx_from_transactions(transactions) -> Iterable[SlimTx]:
    return flatten([get_slim_tx_from_transaction(tx) for tx in transactions])


def get_unique_addresses_from_transactions(transactions) -> Set[str]:
    return {adr.address for adr in get_slim_tx_from_transactions(transactions)}


def get_unique_ordered_output_addresses_from_transactions(
    transactions,
) -> Iterable[str]:
    """Returns all unique output addresses in the order they appear in the txs.
    This is useful to assign address ids where order should matter.

    Args:
        transactions (TYPE): Iterable of dbtransactions

    Returns:
        Iterable[str]: order preserving Iterable
    """
    """
        Construction see
        https://stackoverflow.com/questions/1653970/does-python-have-an-ordered-set
    """
    return list(
        dict.fromkeys(
            [
                tx.address
                for tx in get_slim_tx_from_transactions(transactions)
                if tx.direction == FlowDirection.OUT
            ]
        )
    )


def get_unique_ordered_input_addresses_from_transactions(
    transactions,
) -> Iterable[str]:
    """Returns all unique input addresses in the order they appear in the txs.
    This is useful to assign address ids where order should matter.

    Args:
        transactions (TYPE): Iterable of dbtransactions

    Returns:
        Iterable[str]: order preserving Iterable
    """
    """
        Construction see
        https://stackoverflow.com/questions/1653970/does-python-have-an-ordered-set
    """
    return list(
        dict.fromkeys(
            [
                tx.address
                for tx in get_slim_tx_from_transactions(transactions)
                if tx.direction == FlowDirection.IN
            ]
        )
    )
