import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, NamedTuple, Tuple

import pandas as pd
from cassandra.cqlengine.columns import Integer
from cassandra.cqlengine.usertype import UserType

from graphsenselib.deltaupdate.update.tokens import TokenTransfer

from ...db import DbChange
from ...utils import DataObject as MutableNamedTuple
from ...utils.logging import LoggerScope
from .abstractupdater import TABLE_NAME_DELTA_HISTORY
from .generic import DeltaScalar, DeltaValue, Tx
from .modelsaccount import (
    BalanceDelta,
    EntityDeltaAccount,
    RawEntityTxAccount,
    RelationDeltaAccount,
)

logger = logging.getLogger(__name__)

currency_to_decimals = {
    "ETH": 18,
    "TRX": 6,
}


@dataclass
class TxReference(UserType):
    trace_index: Integer(required=False)
    log_index: Integer(required=False)


def only_call_traces(traces: List) -> List:
    return [trace for trace in traces if trace.call_type == "call"]


def get_bookkeeping_changes(
    base_statistics: MutableNamedTuple,
    current_statistics: NamedTuple,
    last_block_processed: int,
    nr_new_address_relations: int,
    nr_new_addresses: int,
    nr_new_tx: int,
    highest_address_id: int,
    runtime_seconds: int,
    bts: Dict[int, datetime],
    new_blocks: int,
    patch_mode: bool,
) -> List[DbChange]:
    """Creates changes for the bookkeeping tables like summary statistics after
    other data has been updated.

    Args:
        base_statistics (MutableNamedTuple): statistics db row, all the other
        parameters are note data is updated in this process
        current_statistics (NamedTuple): Current value of db statistics for comparison
        last_block_processed (int): Last block processed
        nr_new_address_relations (int): Delta new addresses relations in changeset
        nr_new_addresses (int): Delta new addresses in changeset
        nr_new_tx (int): Delta new txs in changeset
        highest_address_id (int): current highest address_id
        runtime_seconds (int): runtime to create the last changes in seconds
        bts (Dict[int, datetime]): mapping from block to its timestamp
        delta values
    """
    changes = []
    with LoggerScope.debug(logger, "Creating summary_statistics updates") as lg:
        lb_date = bts[last_block_processed]
        stats = base_statistics
        stats.no_blocks = current_statistics.no_blocks + new_blocks
        stats.timestamp = int(lb_date.timestamp())
        stats.no_address_relations += nr_new_address_relations
        stats.no_addresses += nr_new_addresses
        stats.no_transactions += nr_new_tx

        statistics = stats.as_dict()

        if current_statistics.no_blocks != stats.no_blocks:
            if not patch_mode:
                assert current_statistics.no_blocks < stats.no_blocks

        changes.append(DbChange.new(table="summary_statistics", data=statistics))

        lg.debug(f"Statistics: {statistics}")

        data_history = {
            "last_synced_block": last_block_processed,
            "last_synced_block_timestamp": lb_date,
            "highest_address_id": highest_address_id,
            "timestamp": datetime.now(),
            "write_new": False,
            "write_dirty": False,
            "runtime_seconds": runtime_seconds,
        }
        changes.append(DbChange.new(table=TABLE_NAME_DELTA_HISTORY, data=data_history))

        lg.debug(f"History: {data_history}")

    return changes


def get_entitytx_from_tokentransfer(
    tokentransfer: TokenTransfer, is_outgoing, rates, hash_to_id, address_hash_to_id
) -> RawEntityTxAccount:
    tx_id = hash_to_id[tokentransfer.tx_hash]

    address_hash = (
        tokentransfer.from_address if is_outgoing else tokentransfer.to_address
    )
    address_id = address_hash_to_id[address_hash]

    dv = DeltaValue(
        tokentransfer.value,
        get_prices(
            tokentransfer.value,
            tokentransfer.decimals,
            rates[tokentransfer.block_id],
            tokentransfer.usd_equivalent,
            tokentransfer.coin_equivalent,
        ),
    )

    token_values = {tokentransfer.asset: dv}

    tx_reference = {
        "trace_index": None,
        "log_index": tokentransfer.log_index,
    }
    tx_reference = TxReference(**tx_reference)

    reta = RawEntityTxAccount(
        identifier=address_id,
        is_outgoing=is_outgoing,
        tx_id=tx_id,
        tx_reference=tx_reference,
        value=0,
        token_values=token_values,
        block_id=tokentransfer.block_id,
    )
    return reta


def get_entitytx_from_transaction(
    tx, is_outgoing, hash_to_id, address_hash_to_id
) -> RawEntityTxAccount:
    tx_id = hash_to_id[tx.tx_hash]
    address_hash = tx.from_address if is_outgoing else tx.to_address

    address_id = address_hash_to_id[address_hash]

    tx_reference = {
        "trace_index": None,
        "log_index": None,
    }
    tx_reference = TxReference(**tx_reference)

    reta = RawEntityTxAccount(
        identifier=address_id,
        is_outgoing=is_outgoing,
        tx_id=tx_id,
        tx_reference=tx_reference,
        value=tx.value,
        token_values={},
        block_id=tx.block_id,
    )
    return reta


def get_prices(
    value, decimals, block_rates, usd_equivalent, coin_equivalent
) -> List[int]:
    euro_per_eth = block_rates[0]
    dollar_per_eth = block_rates[1]
    dollar_per_euro = dollar_per_eth / euro_per_eth

    if usd_equivalent == 1:
        dollar_value = value / 10**decimals
    elif coin_equivalent == 1:
        dollar_value = value / 10**decimals * dollar_per_eth
    else:
        raise Exception(
            "Unknown price type. only native coin and dollar equivalent supported atm"
        )

    euro_value = dollar_value / dollar_per_euro
    return [euro_value, dollar_value]


def get_prices_coin(value, currency, block_rates):
    coin_decimals = currency_to_decimals[currency]
    return get_prices(value, coin_decimals, block_rates, 0, 1)


def balance_updates_traces_txs(
    relation_updates: List[RelationDeltaAccount],
    address_hash_to_id: Dict[bytes, int],
    currency: str,
) -> List[BalanceDelta]:
    excludedCallTypes = [
        "delegatecall",
        "callcode",
        "staticcall",
    ]  # todo combine with trx logic somehow, now its scattered
    filtered_updates = [
        update for update in relation_updates if update.type not in excludedCallTypes
    ]

    return [
        BalanceDelta(
            address_hash_to_id[update.src_identifier],
            {currency: DeltaScalar(-update.value.value)},
        )
        for update in filtered_updates
    ] + [
        BalanceDelta(
            address_hash_to_id[update.dst_identifier],
            {currency: DeltaScalar(update.value.value)},
        )
        for update in filtered_updates
        if update.dst_identifier is not None
    ]


def balance_updates_tokens(
    relation_updates: List[RelationDeltaAccount], address_hash_to_id: Dict[bytes, int]
) -> List[BalanceDelta]:
    updates = []
    for update in relation_updates:
        for token, value in update.token_values.items():
            updates.append(
                BalanceDelta(
                    address_hash_to_id[update.src_identifier],
                    {token: DeltaScalar(-value.value)},
                )
            )
            updates.append(
                BalanceDelta(
                    address_hash_to_id[update.dst_identifier],
                    {token: DeltaScalar(value.value)},
                )
            )

    return updates


def get_entitytx_from_trace(
    trace, is_outgoing, hash_to_id, address_hash_to_id
) -> RawEntityTxAccount:
    tx_id = hash_to_id[trace.tx_hash]
    address_hash = trace.from_address if is_outgoing else trace.to_address
    address_id = address_hash_to_id[address_hash]

    tx_reference = {
        "trace_index": trace.trace_index,
        "log_index": None,
    }
    tx_reference = TxReference(**tx_reference)

    reta = RawEntityTxAccount(
        identifier=address_id,
        is_outgoing=is_outgoing,
        tx_id=tx_id,
        tx_reference=tx_reference,
        value=trace.value,
        token_values={},  # we dont support TRC10 right now
        block_id=trace.block_id,
    )
    return reta


def get_entitydelta_from_trace(
    trace, is_outgoing, rates, hash_to_id, currency
) -> EntityDeltaAccount:
    identifier = trace.from_address if is_outgoing else trace.to_address
    total_received_value = 0 if is_outgoing else trace.value
    total_spent_value = trace.value if is_outgoing else 0
    total_received = DeltaValue(
        total_received_value,
        get_prices_coin(total_received_value, currency, rates[trace.block_id]),
    )
    total_spent = DeltaValue(
        total_spent_value,
        get_prices_coin(total_spent_value, currency, rates[trace.block_id]),
    )
    total_tokens_received = (
        {}
    )  # for now we dont support TRC10, so an empty dict is fine
    total_tokens_spent = {}  # for now we dont support TRC10, so an empty dict is fine
    if trace.tx_hash is None:
        first_tx_id = -1
        last_tx_id = -1
        no_incoming_txs = 0  # spark logic
    else:
        first_tx_id = hash_to_id[trace.tx_hash]
        last_tx_id = hash_to_id[trace.tx_hash]
        no_incoming_txs = int(not is_outgoing)

    no_outgoing_txs = int(is_outgoing)
    no_zerovalue = int((trace.value == 0))  # and trace.call_type == "call")
    no_incoming_txs_zero_value = 0 if is_outgoing else no_zerovalue
    no_outgoing_txs_zero_value = no_zerovalue if is_outgoing else 0

    eda = EntityDeltaAccount(
        identifier=identifier,
        total_received=total_received,
        total_spent=total_spent,
        total_tokens_received=total_tokens_received,
        total_tokens_spent=total_tokens_spent,
        first_tx_id=first_tx_id,
        last_tx_id=last_tx_id,
        no_incoming_txs=no_incoming_txs,
        no_outgoing_txs=no_outgoing_txs,
        no_incoming_txs_zero_value=no_incoming_txs_zero_value,
        no_outgoing_txs_zero_value=no_outgoing_txs_zero_value,
    )
    return eda


def get_entitydelta_from_tokentransfer(
    tokentransfer, is_outgoing, rates, hash_to_id
) -> EntityDeltaAccount:
    identifier = tokentransfer.from_address if is_outgoing else tokentransfer.to_address

    fiat_values = get_prices(
        tokentransfer.value,
        tokentransfer.decimals,
        rates[tokentransfer.block_id],
        tokentransfer.usd_equivalent,
        tokentransfer.coin_equivalent,
    )
    dv = DeltaValue(tokentransfer.value, fiat_values)

    total_received = DeltaValue(0, [0, 0])
    total_spent = DeltaValue(0, [0, 0])
    total_tokens_received = {tokentransfer.asset: dv} if not is_outgoing else {}
    total_tokens_spent = {tokentransfer.asset: dv} if is_outgoing else {}
    first_tx_id = hash_to_id[tokentransfer.tx_hash]
    last_tx_id = hash_to_id[tokentransfer.tx_hash]
    no_incoming_txs = int(not is_outgoing)
    no_outgoing_txs = int(is_outgoing)
    no_incoming_txs_zero_value = 0
    no_outgoing_txs_zero_value = 0

    eda = EntityDeltaAccount(
        identifier=identifier,
        total_received=total_received,
        total_spent=total_spent,
        total_tokens_received=total_tokens_received,
        total_tokens_spent=total_tokens_spent,
        first_tx_id=first_tx_id,
        last_tx_id=last_tx_id,
        no_incoming_txs=no_incoming_txs,
        no_outgoing_txs=no_outgoing_txs,
        no_incoming_txs_zero_value=no_incoming_txs_zero_value,
        no_outgoing_txs_zero_value=no_outgoing_txs_zero_value,
    )
    return eda


def get_entitydelta_from_transaction(
    tx,
    is_outgoing,
    rates: Dict[int, Tuple[float, float]],
    hash_to_id: Dict[str, int],
    currency: str,
) -> EntityDeltaAccount:
    identifier = tx.from_address if is_outgoing else tx.to_address

    total_received_value = 0 if is_outgoing else tx.value
    total_spent_value = tx.value if is_outgoing else 0

    total_received = DeltaValue(
        total_received_value,
        get_prices_coin(total_received_value, currency, rates[tx.block_id]),
    )
    total_spent = DeltaValue(
        total_spent_value,
        get_prices_coin(total_spent_value, currency, rates[tx.block_id]),
    )
    total_tokens_received = {}
    total_tokens_spent = {}
    first_tx_id = hash_to_id[tx.tx_hash]
    last_tx_id = hash_to_id[tx.tx_hash]

    no_incoming_txs = int(not is_outgoing)
    no_outgoing_txs = int(is_outgoing)
    no_incoming_txs_zero_value = 0 if is_outgoing else int(tx.value == 0)
    no_outgoing_txs_zero_value = int(tx.value == 0) if is_outgoing else 0

    eda = EntityDeltaAccount(
        identifier=identifier,
        total_received=total_received,
        total_spent=total_spent,
        total_tokens_received=total_tokens_received,
        total_tokens_spent=total_tokens_spent,
        first_tx_id=first_tx_id,
        last_tx_id=last_tx_id,
        no_incoming_txs=no_incoming_txs,
        no_outgoing_txs=no_outgoing_txs,
        no_incoming_txs_zero_value=no_incoming_txs_zero_value,
        no_outgoing_txs_zero_value=no_outgoing_txs_zero_value,
    )
    return eda


def relationdelta_from_trace(
    trace, rates: Dict[int, Tuple[float, float]], currency: str
) -> RelationDeltaAccount:
    fadr, tadr = trace.from_address, trace.to_address
    value = DeltaValue(
        trace.value, get_prices_coin(trace.value, currency, rates[trace.block_id])
    )
    token_values = {}

    no_transactions = 1
    return RelationDeltaAccount(
        src_identifier=fadr,
        dst_identifier=tadr,
        no_transactions=no_transactions,
        value=value,
        token_values=token_values,
        type=trace.call_type,
    )


def relationdelta_from_transaction(
    tx: Tx, rates: Dict[int, Tuple[float, float]], currency: str
) -> RelationDeltaAccount:
    iadr, oadr = tx.from_address, tx.to_address
    value = DeltaValue(
        tx.value, get_prices_coin(tx.value, currency, rates[tx.block_id])
    )
    token_values = {}
    no_transactions = 1

    return RelationDeltaAccount(
        src_identifier=iadr,
        dst_identifier=oadr,
        no_transactions=no_transactions,
        value=value,
        token_values=token_values,
        type="tx",
    )


def relationdelta_from_tokentransfer(
    tokentransfer: TokenTransfer, rates: Dict[int, Tuple[float, float]]
) -> RelationDeltaAccount:
    iadr, oadr = tokentransfer.from_address, tokentransfer.to_address
    value = tokentransfer.value
    dollar_value, euro_value = get_prices(
        tokentransfer.value,
        tokentransfer.decimals,
        rates[tokentransfer.block_id],
        tokentransfer.usd_equivalent,
        tokentransfer.coin_equivalent,
    )
    value = DeltaValue(value, [dollar_value, euro_value])

    token_values = {tokentransfer.asset: value}
    no_transactions = 1
    return RelationDeltaAccount(
        src_identifier=iadr,
        dst_identifier=oadr,
        no_transactions=no_transactions,
        value=DeltaValue(0, [0, 0]),
        token_values=token_values,
        type="token",
    )


def get_sorted_unique_addresses(
    traces_s: list, reward_traces: list, token_transfers: list, transactions: list
):
    addresses_sorting_df_to_tokens = [
        {
            "address": obj.to_address,
            "block_id": obj.block_id,
            "is_log": True,
            "index": obj.log_index,
            "is_from_address": False,
        }
        for obj in token_transfers
    ]

    addresses_sorting_df_from_tokens = [
        {
            "address": obj.from_address,
            "block_id": obj.block_id,
            "is_log": True,
            "index": obj.log_index,
            "is_from_address": True,
        }
        for obj in token_transfers
    ]

    addresses_sorting_df_to_traces = [
        {
            "address": obj.to_address,
            "block_id": obj.block_id,
            "is_log": False,
            "index": obj.trace_index,
            "is_from_address": False,
        }
        for obj in traces_s + reward_traces
    ]

    addresses_sorting_df_from_traces = [
        {
            "address": obj.from_address,
            "block_id": obj.block_id,
            "is_log": False,
            "index": obj.trace_index,
            "is_from_address": True,
        }
        for obj in traces_s
    ]

    addresses_sorting_df_from_txs = [
        {
            "address": obj.from_address,
            "block_id": obj.block_id,
            "is_log": False,
            # this is a hack to imitate spark; we assume there a max 1M tx per block
            "index": obj.transaction_index - 1_000_000,
            "is_from_address": True,
        }
        for obj in transactions
        if obj.from_address is not None
    ]
    addresses_sorting_df_to_txs = [
        {
            "address": obj.to_address,
            "block_id": obj.block_id,
            "is_log": False,
            # this is a hack to imitate spark; we assume there a max 1M tx per block
            "index": obj.transaction_index - 1_000_000,
            "is_from_address": False,
        }
        for obj in transactions
    ]

    addresses_sorting_df_data = (
        addresses_sorting_df_from_traces
        + addresses_sorting_df_to_traces
        + addresses_sorting_df_from_txs
        + addresses_sorting_df_to_txs
        + addresses_sorting_df_from_tokens
        + addresses_sorting_df_to_tokens
    )

    addresses_sorting_df = pd.DataFrame(addresses_sorting_df_data)

    addresses_sorting_df.sort_values(
        inplace=True, by=["block_id", "is_log", "index", "is_from_address"]
    )  # imitate spark
    addresses_sorting_df.drop_duplicates(keep="first")  # imitate spark
    df_sorted = addresses_sorting_df.sort_values(
        by=["block_id", "is_log", "index", "is_from_address"]
    )
    addresses = df_sorted["address"]
    return addresses
