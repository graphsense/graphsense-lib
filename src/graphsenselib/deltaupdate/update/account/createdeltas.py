import logging
from dataclasses import dataclass
from typing import Dict, List, Tuple

import pandas as pd
from cassandra.cqlengine.columns import Integer
from cassandra.cqlengine.usertype import UserType

from graphsenselib.deltaupdate.update.account.modelsdelta import (
    BalanceDelta,
    EntityDeltaAccount,
    RawEntityTxAccount,
    RelationDeltaAccount,
)
from graphsenselib.deltaupdate.update.account.modelsraw import Block, Trace, Transaction
from graphsenselib.deltaupdate.update.account.tokens import TokenTransfer
from graphsenselib.deltaupdate.update.generic import DeltaScalar, DeltaValue

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


def get_prices(
    value, decimals, block_rates, usd_equivalent, eur_equivalent, coin_equivalent
) -> List[int]:
    euro_per_eth = block_rates[0]
    dollar_per_eth = block_rates[1]
    dollar_per_euro = dollar_per_eth / euro_per_eth

    # enforce mutal exclusion
    assert sum(int(x) for x in [usd_equivalent, eur_equivalent, coin_equivalent]) <= 1

    if usd_equivalent:
        dollar_value = value / 10**decimals
        euro_value = dollar_value / dollar_per_euro
    elif eur_equivalent:
        euro_value = value / 10**decimals
        dollar_value = euro_value / dollar_per_euro
    elif coin_equivalent:
        dollar_value = value / 10**decimals * dollar_per_eth
        euro_value = dollar_value / dollar_per_euro
    else:
        raise Exception(
            "Unknown price type. only native coin and dollar equivalent supported atm"
        )

    return [euro_value, dollar_value]


def get_prices_coin(value, currency, block_rates):
    coin_decimals = currency_to_decimals[currency]
    return get_prices(value, coin_decimals, block_rates, False, False, True)


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
            tokentransfer.eur_equivalent,
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
    tx: Transaction, is_outgoing, hash_to_id, address_hash_to_id
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


def balance_updates_traces_txs(
    relation_updates: List[RelationDeltaAccount],
    address_hash_to_id: Dict[bytes, int],
    currency: str,
) -> List[BalanceDelta]:
    excludedCallTypes = [
        "delegatecall",
        "callcode",
        "staticcall",
    ]
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
    trace: Trace, is_outgoing: bool, hash_to_id: dict, address_hash_to_id: dict
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
    trace: Trace,
    is_outgoing: bool,
    rates: Dict[int, Tuple[float, float]],
    hash_to_id: dict,
    currency: str,
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
    total_tokens_received = {}  # for now we dont support TRC10, so an empty dict is fine
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
    is_contract = is_contract_trace(trace=trace, currency=currency) and not is_outgoing

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
        is_contract=is_contract,
    )
    return eda


def get_entitydelta_from_tokentransfer(
    tokentransfer: TokenTransfer,
    is_outgoing: bool,
    rates: Dict[int, Tuple[float, float]],
    hash_to_id: dict,
) -> EntityDeltaAccount:
    identifier = tokentransfer.from_address if is_outgoing else tokentransfer.to_address

    fiat_values = get_prices(
        tokentransfer.value,
        tokentransfer.decimals,
        rates[tokentransfer.block_id],
        tokentransfer.usd_equivalent,
        tokentransfer.eur_equivalent,
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
        is_contract=False,
    )
    return eda


def is_contract_transaction(tx: Transaction, currency: str) -> bool:
    if (
        currency == "ETH"
    ):  # transactions dont create contracts, only traces do (in data)
        return False
    elif currency == "TRX":  # could improve this with the specific type of transaction
        return tx.receipt_contract_address is not None
    else:
        raise ValueError(f"Unknown currency {currency}")


def is_contract_trace(trace: Trace, currency: str) -> bool:
    if currency == "ETH":  # could improve this with the specific type of transaction
        return trace.trace_type == "create"
    elif currency == "TRX":  # traces dont create contracts transactions do (in data)
        return False
    else:
        raise ValueError(f"Unknown currency {currency}")


def get_entitydelta_from_transaction(
    tx: Transaction,
    is_outgoing: bool,
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
    is_contract = is_contract_transaction(tx=tx, currency=currency) and not is_outgoing

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
        is_contract=is_contract,
    )
    return eda


def relationdelta_from_trace(
    trace: Trace, rates: Dict[int, Tuple[float, float]], currency: str
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
    tx: Transaction, rates: Dict[int, Tuple[float, float]], currency: str
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
        tokentransfer.eur_equivalent,
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
    traces_s: List[Trace],
    reward_traces: List[Trace],
    token_transfers: List[TokenTransfer],
    transactions: List[Transaction],
    blocks: List[Block],
) -> pd.Series:
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

    address_sorting_df_miner = [
        {
            "address": block.miner,
            "block_id": block.block_id,
            "is_log": False,
            # this is a hack to imitate spark; we assume there a max 1M tx per block
            "index": 1_000_000_000,
            "is_from_address": False,
        }
        for block in blocks
    ]

    addresses_sorting_df_data = (
        addresses_sorting_df_from_traces
        + addresses_sorting_df_to_traces
        + addresses_sorting_df_from_txs
        + addresses_sorting_df_to_txs
        + addresses_sorting_df_from_tokens
        + addresses_sorting_df_to_tokens
        + address_sorting_df_miner
    )

    addresses_sorting_df = pd.DataFrame(addresses_sorting_df_data)
    if addresses_sorting_df.empty:
        return pd.Series([])

    addresses_sorting_df.sort_values(
        inplace=True, by=["block_id", "is_log", "index", "is_from_address"]
    )
    df_sorted_unique = addresses_sorting_df.drop_duplicates(
        keep="first", subset=["address"]
    )
    addresses = df_sorted_unique["address"]
    return addresses


def get_entity_transaction_updates_trace_token(
    traces_s_filtered: List[Trace],
    token_transfers: List[TokenTransfer],
    hash_to_id: dict,
    address_hash_to_id: dict,
    rates: dict,
) -> List[RawEntityTxAccount]:
    trace_outgoing = [
        get_entitytx_from_trace(trace, True, hash_to_id, address_hash_to_id)
        for trace in traces_s_filtered
    ]

    trace_incoming = [
        get_entitytx_from_trace(trace, False, hash_to_id, address_hash_to_id)
        for trace in traces_s_filtered
    ]

    token_outgoing = [
        get_entitytx_from_tokentransfer(tt, True, rates, hash_to_id, address_hash_to_id)
        for tt in token_transfers
    ]
    token_incoming = [
        get_entitytx_from_tokentransfer(
            tt, False, rates, hash_to_id, address_hash_to_id
        )
        for tt in token_transfers
    ]

    entity_transactions_traces_tokens = (
        trace_outgoing + trace_incoming + token_outgoing + token_incoming
    )
    return entity_transactions_traces_tokens


def get_entity_updates_trace_token(
    traces_s_filtered: List[Trace],
    token_transfers: List[TokenTransfer],
    reward_traces: List[Trace],
    hash_to_id: dict,
    currency: str,
    rates: dict,
):
    trace_outgoing = [
        get_entitydelta_from_trace(trace, True, rates, hash_to_id, currency)
        for trace in traces_s_filtered
    ]

    trace_incoming = [
        get_entitydelta_from_trace(trace, False, rates, hash_to_id, currency)
        for trace in traces_s_filtered + reward_traces
    ]

    token_outgoing = [
        get_entitydelta_from_tokentransfer(tt, True, rates, hash_to_id)
        for tt in token_transfers
    ]

    token_incoming = [
        get_entitydelta_from_tokentransfer(tt, False, rates, hash_to_id)
        for tt in token_transfers
    ]

    entity_deltas_traces_tokens = (
        trace_outgoing + trace_incoming + token_outgoing + token_incoming
    )
    return entity_deltas_traces_tokens


def get_entity_transactions_updates_tx(
    transactions: List[Transaction],
    hash_to_id: Dict[str, int],
    address_hash_to_id: Dict[bytes, int],
) -> List[RawEntityTxAccount]:
    outgoing = [
        get_entitytx_from_transaction(tx, True, hash_to_id, address_hash_to_id)
        for tx in transactions
        if tx.from_address is not None
    ]
    incoming = [
        get_entitytx_from_transaction(tx, False, hash_to_id, address_hash_to_id)
        for tx in transactions
        if tx.to_address is not None
    ]
    entity_transactions_tx = outgoing + incoming
    return entity_transactions_tx


def get_entity_updates_tx(
    transactions: List[Transaction],
    hash_to_id: Dict[str, int],
    currency: str,
    rates: Dict[int, Tuple[float, float]],
) -> List[EntityDeltaAccount]:
    outgoing = [
        get_entitydelta_from_transaction(tx, True, rates, hash_to_id, currency)
        for tx in transactions
        if tx.from_address is not None
    ]
    incoming = [
        get_entitydelta_from_transaction(tx, False, rates, hash_to_id, currency)
        for tx in transactions
        if tx.to_address is not None
    ]

    entity_deltas_tx = outgoing + incoming

    return entity_deltas_tx


def get_balance_deltas(
    relation_updates_trace: List[RelationDeltaAccount],
    relation_updates_tx: List[RelationDeltaAccount],
    relation_updates_tokens: List[RelationDeltaAccount],
    reward_traces: List[Trace],
    transactions: List[Transaction],
    blocks: List[Block],
    address_hash_to_id: Dict[bytes, int],
    currency: str,
) -> List[BalanceDelta]:
    credits_debits_tokens_eth = []
    credits_debits_tokens_eth += balance_updates_traces_txs(
        relation_updates_trace + relation_updates_tx, address_hash_to_id, currency
    )
    credits_debits_tokens_eth += balance_updates_tokens(
        relation_updates_tokens, address_hash_to_id
    )

    miner_rewards = [
        BalanceDelta(address_hash_to_id[t.to_address], {currency: DeltaScalar(t.value)})
        for t in reward_traces
    ]

    if currency == "TRX":
        txFeeDebits = []
        burntFees = []
        txFeeCredits = [
            BalanceDelta(
                address_hash_to_id[tx.from_address],
                {currency: DeltaScalar(-tx.fee)},
            )
            for tx in transactions
            if tx.from_address in address_hash_to_id
        ]
    elif currency == "ETH":
        block_to_miner_id = {
            block.block_id: address_hash_to_id[block.miner] for block in blocks
        }
        txFeeDebits = [
            BalanceDelta(
                block_to_miner_id[tx.block_id],
                {currency: DeltaScalar(tx.receipt_gas_used * tx.gas_price)},
            )
            for tx in transactions
        ]

        burntFees = [
            BalanceDelta(
                block_to_miner_id[b.block_id],
                {currency: DeltaScalar(-b.base_fee_per_gas * b.gas_used)},
            )
            for b in blocks
        ]
        txFeeCredits = [
            BalanceDelta(
                address_hash_to_id[tx.from_address],
                {currency: DeltaScalar(-tx.receipt_gas_used * tx.gas_price)},
            )
            for tx in transactions
            if tx.from_address in address_hash_to_id
        ]
    else:
        raise ValueError(f"Unknown currency {currency}")

    balance_updates = (
        credits_debits_tokens_eth
        + txFeeDebits
        + txFeeCredits
        + burntFees
        + miner_rewards
    )
    return balance_updates
