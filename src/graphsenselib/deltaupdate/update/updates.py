from typing import Dict, List, Tuple

from .deltahelpers import (
    balance_updates_tokens,
    balance_updates_traces_txs,
    get_entitydelta_from_tokentransfer,
    get_entitydelta_from_trace,
    get_entitydelta_from_transaction,
    get_entitytx_from_tokentransfer,
    get_entitytx_from_trace,
    get_entitytx_from_transaction,
)
from .generic import DeltaScalar
from .modelsaccount import (
    BalanceDelta,
    EntityDeltaAccount,
    RawEntityTxAccount,
    RelationDeltaAccount,
)


def get_entity_transaction_updates_trace_token(
    traces_s_filtered: list,
    token_transfers: list,
    hash_to_id: dict,
    address_hash_to_id: dict,
    rates: dict,
):
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
    traces_s_filtered: list,
    token_transfers: list,
    reward_traces: list,
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
    transactions: List, hash_to_id: Dict[str, int], address_hash_to_id: Dict[bytes, int]
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
    transactions: List,
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


def get_balance_updates(
    relation_updates_trace: List[RelationDeltaAccount],
    relation_updates_tx: List[RelationDeltaAccount],
    relation_updates_tokens: List[RelationDeltaAccount],
    reward_traces: List,
    transactions: List,
    blocks: List,
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
