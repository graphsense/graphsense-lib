from dataclasses import dataclass
from math import floor
from typing import Iterable, Set

from ..datatypes import FlowDirection
from .generic import flatten


@dataclass
class SlimTx:
    address: str
    block_id: int
    timestamp: int
    tx_hash: str
    direction: FlowDirection


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


def get_unique_addresses_from_trace(trace) -> Iterable[str]:
    return {adr.address for adr in get_slim_tx_from_trace(trace)}


def get_slim_tx_from_trace(trace) -> Iterable[SlimTx]:
    # Only take first address from address array
    # this is equivalent to the spark job, but is ignoring multisig

    sending_addresses = [(trace.from_address, FlowDirection.OUT)]
    receiving_addresses = [(trace.to_address, FlowDirection.IN)]

    addresses = sending_addresses + receiving_addresses

    return [
        SlimTx(
            addr,
            trace.block_id,
            0,  # todo #trace.timestamp,
            trace.tx_hash,
            direction,
        )
        for addr, direction in addresses
    ]


def get_slim_tx_from_traces(traces) -> Iterable[SlimTx]:
    return flatten([get_slim_tx_from_trace(tx) for tx in traces])


def get_unique_addresses_from_traces(traces) -> Set[str]:
    return {adr.address for adr in get_slim_tx_from_traces(traces)}


def get_unique_ordered_receiver_addresses_from_traces(
    traces,
) -> Iterable[str]:
    """Returns all unique output addresses in the order they appear in the txs.
    This is useful to assign address ids where order should matter.

    Args:
        traces (TYPE): Iterable of dbtraces

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
                for tx in get_slim_tx_from_traces(traces)
                if tx.direction == FlowDirection.OUT
            ]
        )
    )


def get_unique_ordered_addresses(
    address_containing_objects,
    mode: str,
) -> Iterable[str]:
    """Returns all unique input addresses in the order they appear in the txs.
    This is useful to assign address ids where order should matter.

    Args:
        traces (TYPE): Iterable of dbtraces

    Returns:
        Iterable[str]: order preserving Iterable
    """
    """
        Construction see
        https://stackoverflow.com/questions/1653970/does-python-have-an-ordered-set
    """
    if mode == "sender":
        list_to_prepare = [x.to_address for x in address_containing_objects]
    elif mode == "receiver":
        list_to_prepare = [x.from_address for x in address_containing_objects]
    elif mode == "both":
        list_to = [x.to_address for x in address_containing_objects]
        list_from = [x.from_address for x in address_containing_objects]
        list_to_prepare = list_to + list_from
    else:
        raise Exception("Unknown mode")
    return list(dict.fromkeys(list_to_prepare[::-1]))[::-1]


def calculate_id_group_with_overflow(tx_id: int, bucket_size: int):
    blub = int(floor(float(tx_id) / bucket_size))

    if blub.bit_length() >= 31:
        # downcast to 32bit integer
        # blub = ctypes.c_uint32(blub).value
        blub = (blub + 2**31) % 2**32 - 2**31
    return blub


def get_id_group(id_, bucket_size):
    gid = floor(int(id_) / bucket_size)
    if gid.bit_length() > 31:
        # tron tx_id are long and the group is int
        # thus we need to also consider overflows in this case
        # additionally spark does not calculate ids on int basis but
        # based on floats which can lead to rounding errors.
        gid = calculate_id_group_with_overflow(id_, bucket_size)
    return gid


def get_id_group_with_secondary_addresstransactions(
    iid, bucket_size, block_id, block_bucket_size_address_txs
):
    address_id_group = get_id_group(iid, bucket_size)
    address_id_secondary_group = block_id // block_bucket_size_address_txs
    return address_id_group, address_id_secondary_group


def get_id_group_with_secondary_relations(
    iid, id_for_secondary, bucket_size, relations_nbuckets
):
    address_id_group = get_id_group(iid, bucket_size)
    address_id_secondary_group = id_for_secondary % relations_nbuckets
    return address_id_group, address_id_secondary_group
