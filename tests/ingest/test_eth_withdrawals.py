"""Tests for EIP-4895 withdrawal → synthetic reward trace derivation.

Withdrawals are stored only on the (delta lake) block table; every consumer
derives the same tx_hash-less reward traces from them. These tests pin the
row shape and assert the ingest-side and delta-updater-side derivations agree.
"""

import pandas as pd

from graphsenselib.deltaupdate.update.account.modelsraw import (
    eth_withdrawal_traces_from_lake_blocks,
)
from graphsenselib.ingest.account import (
    GWEI_TO_WEI,
    WITHDRAWAL_TRACE_INDEX_OFFSET,
    eth_withdrawals_to_reward_traces,
    prepare_blocks_inplace_eth,
)

# Real withdrawals from ETH block 17100000 (amounts in Gwei).
WITHDRAWALS_BLOCK_17100000 = [
    {
        "index": 1041981,
        "validator_index": 340674,
        "address": "0xb9d7934878b5fb9610b3fe8a5e441e8fad7e293f",
        "amount": 12210183,
    },
    {
        "index": 1041982,
        "validator_index": 340675,
        "address": "0x1f9090aae28b8a3dceadf281b0f12828e676c326",
        "amount": 12122076,
    },
]


def _prepared_block(block_id=17100000, withdrawals=None):
    """Block dict in post-prepare_blocks_inplace_eth shape (subset)."""
    block = {
        "number": block_id,
        "hash": "0x" + "11" * 32,
        "parent_hash": "0x" + "22" * 32,
        "nonce": "0x" + "00" * 8,
        "sha3_uncles": "0x" + "33" * 32,
        "logs_bloom": "0x" + "00" * 256,
        "transactions_root": "0x" + "44" * 32,
        "state_root": "0x" + "55" * 32,
        "receipts_root": "0x" + "66" * 32,
        "miner": "0x" + "77" * 20,
        "difficulty": 0,
        "total_difficulty": 0,
        "size": 1000,
        "extra_data": "0x",
        "gas_limit": 30000000,
        "gas_used": 15000000,
        "base_fee_per_gas": 10,
        "timestamp": 1681000000,
        "transaction_count": 1,
        "type": "block",
        "parent_beacon_block_root": None,
        "requests_hash": None,
        "withdrawals": [dict(w) for w in (withdrawals or [])],
        "uncles": [],
    }
    prepare_blocks_inplace_eth([block], 1000)
    return block


def test_withdrawals_become_reward_traces():
    block = _prepared_block(withdrawals=WITHDRAWALS_BLOCK_17100000)
    rows = eth_withdrawals_to_reward_traces([block])

    assert len(rows) == 2
    first = rows[0]
    assert first["block_id"] == 17100000
    assert first["block_id_group"] == 17100
    assert first["trace_index"] == WITHDRAWAL_TRACE_INDEX_OFFSET
    assert first["trace_id"] == f"reward_17100000_{WITHDRAWAL_TRACE_INDEX_OFFSET}"
    assert first["trace_type"] == "reward"
    assert first["reward_type"] == "withdrawal"
    assert first["tx_hash"] is None
    assert first["from_address"] is None
    assert first["to_address"] == bytes.fromhex(
        "b9d7934878b5fb9610b3fe8a5e441e8fad7e293f"
    )
    # EIP-4895 amounts are Gwei; trace values are wei.
    assert first["value"] == 12210183 * GWEI_TO_WEI
    assert first["status"] == 1

    second = rows[1]
    assert second["trace_index"] == WITHDRAWAL_TRACE_INDEX_OFFSET + 1
    assert second["value"] == 12122076 * GWEI_TO_WEI


def test_no_withdrawals_no_rows():
    assert eth_withdrawals_to_reward_traces([_prepared_block(withdrawals=[])]) == []


def test_lake_blocks_derivation_matches_ingest_derivation():
    """The delta-updater derivation must agree with the Cassandra rows the
    ingest writes, field for field, or incremental updates diverge from a
    full re-transform."""
    block = _prepared_block(withdrawals=WITHDRAWALS_BLOCK_17100000)
    ingest_rows = eth_withdrawals_to_reward_traces([block])

    # Lake block shape: withdrawals carry big-endian Gwei bytes + hex address
    # (exactly what prepare_blocks_inplace_eth produced for the parquet sink).
    lake_df = pd.DataFrame(
        [{"block_id": 17100000, "withdrawals": block["withdrawals"]}]
    )
    lake_traces = eth_withdrawal_traces_from_lake_blocks(lake_df)

    assert len(lake_traces) == len(ingest_rows)
    for row, trace in zip(ingest_rows, lake_traces):
        assert trace.block_id == row["block_id"]
        assert trace.trace_index == row["trace_index"]
        assert trace.tx_hash is None
        assert trace.from_address is None
        assert trace.to_address == row["to_address"]
        assert trace.value == row["value"]
        assert trace.status == 1
        assert trace.trace_type == "reward"


def test_lake_blocks_pre_shanghai_rows_are_skipped():
    # Amounts as bytes, like the lake stores them.
    lake_withdrawals = [
        {
            **w,
            "amount": w["amount"].to_bytes((w["amount"].bit_length() + 7) // 8, "big"),
        }
        for w in WITHDRAWALS_BLOCK_17100000
    ]
    df = pd.DataFrame(
        [
            {"block_id": 15000000, "withdrawals": None},
            {"block_id": 17100000, "withdrawals": lake_withdrawals},
        ]
    )
    traces = eth_withdrawal_traces_from_lake_blocks(df)
    assert len(traces) == 2
    assert all(t.block_id == 17100000 for t in traces)


def test_lake_blocks_without_withdrawals_column():
    df = pd.DataFrame([{"block_id": 15000000}])
    assert eth_withdrawal_traces_from_lake_blocks(df) == []
