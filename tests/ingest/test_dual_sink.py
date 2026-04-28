# -*- coding: utf-8 -*-
"""Test the dual-sink (Cassandra + Delta) transform/sink boundary.

Verifies that _transform_with_cassandra produces a neutral format
(parquet-ready) with Cassandra-specific fields as extras, and that
both sinks consume the data correctly.
"""

from unittest.mock import MagicMock

import pytest

pytest.importorskip("pyarrow")

from graphsenselib.ingest.cassandra.sink import CassandraSink
from graphsenselib.ingest.common import BlockRangeContent
from graphsenselib.ingest.transform import TransformerUTXO
from graphsenselib.ingest.utxo import TX_BUCKET_SIZE, TX_HASH_PREFIX_LENGTH
from graphsenselib.schema.resources.parquet.utxo import UTXO_SCHEMA_RAW
from graphsenselib.utils.account import get_id_group


def _make_raw_blocks_and_txs():
    """Create minimal raw block + tx data matching BtcBlockExporter output."""
    blocks = [
        {
            "type": "block",
            "hash": "000000000000000000012345abcdef0123456789abcdef0123456789abcdef01",
            "size": 1000,
            "stripped_size": 800,
            "weight": 3200,
            "version": 0x20000000,
            "merkle_root": "abcdef0123456789" * 4,
            "nonce": "00000000",
            "bits": "1d00ffff",
            "coinbase_param": "0badcafe",
            "number": 10000,
            "timestamp": 1700000000,
            "transaction_count": 2,
        },
    ]
    txs = [
        {
            "type": "transaction",
            "hash": "aabb000000000000000000000000000000000000000000000000000000000001",
            "size": 250,
            "virtual_size": 200,
            "version": 2,
            "lock_time": 0,
            "block_number": 10000,
            "block_hash": "000000000000000000012345abcdef0123456789abcdef0123456789abcdef01",
            "block_timestamp": 1700000000,
            "is_coinbase": True,
            "index": 0,
            "inputs": [],
            "outputs": [
                {
                    "index": 0,
                    "script_asm": "OP_DUP OP_HASH160 abc123 OP_EQUALVERIFY OP_CHECKSIG",
                    "script_hex": "76a914abc12388ac",
                    "required_signatures": 1,
                    "type": "pubkeyhash",
                    "addresses": ["1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa"],
                    "value": 625000000,
                },
            ],
            "input_count": 0,
            "output_count": 1,
            "input_value": 0,
            "output_value": 625000000,
            "fee": 0,
        },
        {
            "type": "transaction",
            "hash": "ccdd000000000000000000000000000000000000000000000000000000000002",
            "size": 300,
            "virtual_size": 250,
            "version": 2,
            "lock_time": 0,
            "block_number": 10000,
            "block_hash": "000000000000000000012345abcdef0123456789abcdef0123456789abcdef01",
            "block_timestamp": 1700000000,
            "is_coinbase": False,
            "index": 1,
            "inputs": [
                {
                    "index": 0,
                    "spent_transaction_hash": "aabb000000000000000000000000000000000000000000000000000000000001",
                    "spent_output_index": 0,
                    "script_asm": "3045022100...",
                    "script_hex": "483045022100abcd",
                    "sequence": 4294967295,
                    "required_signatures": None,
                    "type": "pubkeyhash",
                    "addresses": ["1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa"],
                    "value": 625000000,
                    "txinwitness": None,
                },
            ],
            "outputs": [
                {
                    "index": 0,
                    "script_asm": "OP_DUP OP_HASH160 def456 OP_EQUALVERIFY OP_CHECKSIG",
                    "script_hex": "76a914def45688ac",
                    "required_signatures": 1,
                    "type": "pubkeyhash",
                    "addresses": ["1BvBMSEYstWetqTFn5Au4m4GFg7xJaNVN2"],
                    "value": 624000000,
                },
            ],
            "input_count": 1,
            "output_count": 1,
            "input_value": 625000000,
            "output_value": 624000000,
            "fee": 1000000,
        },
    ]
    return blocks, txs


def _make_mock_db():
    """Create a mock AnalyticsDb that provides just enough for the transformer."""
    db = MagicMock()
    # get_latest_tx_id_before_block returns -1 (so first tx_id is 0)
    db.raw.get_latest_tx_id_before_block.return_value = -1
    # CassandraOutputResolver needs these but we won't resolve via Cassandra
    db.raw.get_transaction_outputs_in_bucket.return_value = []
    return db


def test_transform_with_cassandra_produces_dual_format():
    """After _transform_with_cassandra, txs should have BOTH parquet-ready
    fields and Cassandra extras (inputs_cassandra, outputs_cassandra, etc.)."""
    db = _make_mock_db()
    transformer = TransformerUTXO(
        partition_batch_size=10000,
        network="btc",
        db=db,
        resolve_inputs_via_cassandra=False,
        fill_unresolved_inputs=True,
    )

    blocks, txs = _make_raw_blocks_and_txs()
    brc = BlockRangeContent(
        table_contents={"blocks": blocks, "txs": txs},
        start_block=10000,
        end_block=10000,
    )

    result = transformer.transform(brc)

    result_txs = result.table_contents["transaction"]
    result_blocks = result.table_contents["block"]

    # Blocks should have partition and block_id
    assert len(result_blocks) == 1
    assert result_blocks[0]["partition"] == 1  # 10000 // 10000

    # Both txs should be present
    assert len(result_txs) == 2

    # Sort by index for deterministic checks
    result_txs.sort(key=lambda t: t.get("tx_id", 0))

    for tx in result_txs:
        # --- Neutral parquet fields (after prepare_transactions_inplace_parquet) ---
        # Field renamed: hash → tx_hash, block_number → block_id
        assert "tx_hash" in tx
        assert isinstance(tx["tx_hash"], bytes), (
            "tx_hash should be bytes after parquet prep"
        )
        assert "block_id" in tx
        assert "partition" in tx

        # --- Cassandra extras ---
        assert "tx_id" in tx
        assert isinstance(tx["tx_id"], int)
        assert "tx_id_group" in tx
        assert tx["tx_id_group"] == get_id_group(tx["tx_id"], TX_BUCKET_SIZE)
        assert "tx_prefix" in tx
        assert isinstance(tx["tx_prefix"], str), "tx_prefix should be a hex string"
        assert len(tx["tx_prefix"]) == TX_HASH_PREFIX_LENGTH
        assert "coinjoin" in tx
        assert isinstance(tx["coinjoin"], bool)

    # Non-coinbase tx should have inputs_cassandra / outputs_cassandra
    non_coinbase = [t for t in result_txs if not t.get("coinbase", True)]
    assert len(non_coinbase) == 1
    tx2 = non_coinbase[0]
    assert "inputs_cassandra" in tx2
    assert "outputs_cassandra" in tx2
    assert isinstance(tx2["inputs_cassandra"], list)
    assert len(tx2["inputs_cassandra"]) == 1
    assert isinstance(tx2["outputs_cassandra"], list)
    assert len(tx2["outputs_cassandra"]) == 1

    # Parquet inputs should be a list of dicts (not tx_io_summary lists)
    assert isinstance(tx2["inputs"], list)
    if tx2["inputs"]:
        assert isinstance(tx2["inputs"][0], dict), (
            "Parquet inputs should be list-of-dicts, not tx_io_summary"
        )

    # Cassandra inputs should be tx_io_summary format:
    # [address, value, type, script_hex, txinwitness, sequence]
    cassandra_inp = tx2["inputs_cassandra"][0]
    assert isinstance(cassandra_inp, list), (
        "Cassandra inputs should be tx_io_summary lists"
    )
    assert len(cassandra_inp) == 6

    # tx_ids should be sequential starting from 0
    assert result_txs[0]["tx_id"] == 0
    assert result_txs[1]["tx_id"] == 1

    # block_transactions and tx_lookups should be present (Cassandra-only tables)
    assert "block_transactions" in result.table_contents
    assert "transaction_by_tx_prefix" in result.table_contents
    assert "transaction_spent_in" in result.table_contents
    assert "transaction_spending" in result.table_contents


def test_cassandra_sink_swaps_inputs():
    """CassandraSink should replace inputs/outputs with inputs_cassandra/outputs_cassandra."""
    db = MagicMock()
    sink = CassandraSink(db)

    parquet_inputs = [{"spent_transaction_hash": b"\x01" * 32, "value": 100}]
    cassandra_inputs = [["1addr", 100, 3, b"\xab", None]]
    parquet_outputs = [{"addresses": [b"\x02" * 20], "value": 90}]
    cassandra_outputs = [["1addr2", 90, 3, b"\xcd", None]]

    rows = [
        {
            "tx_hash": b"\xaa" * 32,
            "block_id": 10000,
            "inputs": parquet_inputs,
            "outputs": parquet_outputs,
            "inputs_cassandra": cassandra_inputs,
            "outputs_cassandra": cassandra_outputs,
            "partition": 1,
            "tx_id": 0,
        },
    ]

    brc = BlockRangeContent(
        table_contents={"transaction": rows},
        start_block=10000,
        end_block=10000,
    )

    sink.write(brc)

    # Verify cassandra_ingest was called with swapped fields
    call_args = db.raw.ingest.call_args
    table_name = call_args[0][0]
    written_rows = call_args[0][1]

    assert table_name == "transaction"
    assert len(written_rows) == 1
    row = written_rows[0]
    # inputs should now be the Cassandra format
    assert row["inputs"] == cassandra_inputs
    assert row["outputs"] == cassandra_outputs
    # The cassandra-prefixed fields should be gone
    assert "inputs_cassandra" not in row
    assert "outputs_cassandra" not in row


def test_delta_sink_ignores_cassandra_extras():
    """DeltaDumpWriter should write only schema columns, ignoring Cassandra extras."""
    import shutil
    import tempfile

    from graphsenselib.ingest.delta.sink import DeltaDumpSinkFactory, read_table

    tmpdir = tempfile.mkdtemp(prefix="dual_sink_test_")

    sink = DeltaDumpSinkFactory.create_writer(
        network="btc",
        s3_credentials=None,
        write_mode="overwrite",
        directory=tmpdir,
    )

    # Run the full transform to get realistic data
    db = _make_mock_db()
    transformer = TransformerUTXO(
        partition_batch_size=10000,
        network="btc",
        db=db,
        resolve_inputs_via_cassandra=False,
        fill_unresolved_inputs=True,
    )

    blocks, txs = _make_raw_blocks_and_txs()
    brc = BlockRangeContent(
        table_contents={"blocks": blocks, "txs": txs},
        start_block=10000,
        end_block=10000,
    )

    result = transformer.transform(brc)

    # Write through the delta sink
    sink.write(result)

    # Read back and verify schema
    df_block = read_table(tmpdir, "block")
    df_tx = read_table(tmpdir, "transaction")

    assert len(df_block) == 1
    assert len(df_tx) == 2

    # Delta schema should NOT contain Cassandra-only columns
    assert "inputs_cassandra" not in df_tx.columns
    assert "outputs_cassandra" not in df_tx.columns
    assert "tx_id" not in df_tx.columns
    assert "tx_id_group" not in df_tx.columns
    assert "tx_prefix" not in df_tx.columns

    # Delta schema SHOULD contain parquet columns
    tx_schema_fields = set(UTXO_SCHEMA_RAW["transaction"].names)
    for col in df_tx.columns:
        assert col in tx_schema_fields, f"Unexpected column {col} in delta output"

    shutil.rmtree(tmpdir)


def test_delta_only_vs_dual_sink_parquet_equivalence():
    """The parquet-relevant fields should be identical whether the transformer
    runs in delta-only mode or dual-sink (with-cassandra) mode."""
    # Delta-only transform
    transformer_delta = TransformerUTXO(
        partition_batch_size=10000,
        network="btc",
        db=None,
        fill_unresolved_inputs=True,
    )
    blocks1, txs1 = _make_raw_blocks_and_txs()
    brc1 = BlockRangeContent(
        table_contents={"blocks": blocks1, "txs": txs1},
        start_block=10000,
        end_block=10000,
    )
    result_delta = transformer_delta.transform(brc1)

    # Dual-sink transform
    db = _make_mock_db()
    transformer_dual = TransformerUTXO(
        partition_batch_size=10000,
        network="btc",
        db=db,
        resolve_inputs_via_cassandra=False,
        fill_unresolved_inputs=True,
    )
    blocks2, txs2 = _make_raw_blocks_and_txs()
    brc2 = BlockRangeContent(
        table_contents={"blocks": blocks2, "txs": txs2},
        start_block=10000,
        end_block=10000,
    )
    result_dual = transformer_dual.transform(brc2)

    # Compare block tables
    delta_blocks = result_delta.table_contents["block"]
    dual_blocks = result_dual.table_contents["block"]
    assert len(delta_blocks) == len(dual_blocks)

    # Compare parquet-relevant fields of transactions
    parquet_fields = set(UTXO_SCHEMA_RAW["transaction"].names)
    delta_txs = sorted(
        result_delta.table_contents["transaction"],
        key=lambda t: (t["block_id"], t.get("index", 0)),
    )
    dual_txs = sorted(
        result_dual.table_contents["transaction"],
        key=lambda t: (t["block_id"], t.get("index", 0)),
    )
    assert len(delta_txs) == len(dual_txs)

    # coinjoin is only computed in the dual-sink (Cassandra) path; the
    # delta-only path leaves it unset (None).  block_hash is dropped by
    # prepare_transactions_inplace_parquet but re-attached as a Cassandra
    # extra.  These are expected differences — compare only the shared fields.
    cassandra_only_extras = {"coinjoin", "block_hash"}
    compare_fields = parquet_fields - cassandra_only_extras

    for dt, du in zip(delta_txs, dual_txs):
        for field in compare_fields:
            if field in dt or field in du:
                assert dt.get(field) == du.get(field), (
                    f"Field '{field}' differs: delta={dt.get(field)!r} vs dual={du.get(field)!r}"
                )
