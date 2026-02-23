"""Unit tests for graphsenselib.ingest.fast_rpc parsers and enrichment."""

import json
import threading
from unittest.mock import MagicMock, patch

import pytest

from graphsenselib.ingest.fast_rpc import (
    BatchRpcClient,
    FastBlockExporter,
    FastBlockReceiptExporter,
    FastReceiptExporter,
    enrich_transactions,
    hex_to_dec,
    parse_block_json,
    parse_log_json,
    parse_receipt_json,
    parse_transaction_json,
    to_float_or_none,
    to_normalized_address,
)


# ---------------------------------------------------------------------------
# hex_to_dec
# ---------------------------------------------------------------------------


class TestHexToDec:
    def test_none(self):
        assert hex_to_dec(None) is None

    def test_int_passthrough(self):
        assert hex_to_dec(42) == 42

    def test_hex_string(self):
        assert hex_to_dec("0xa") == 10

    def test_hex_string_no_prefix(self):
        assert hex_to_dec("ff") == 255

    def test_zero(self):
        assert hex_to_dec("0x0") == 0

    def test_large_value(self):
        assert hex_to_dec("0xde0b6b3a7640000") == 1000000000000000000


# ---------------------------------------------------------------------------
# to_normalized_address
# ---------------------------------------------------------------------------


class TestToNormalizedAddress:
    def test_none(self):
        assert to_normalized_address(None) is None

    def test_lowercase(self):
        assert (
            to_normalized_address("0xABCDEF1234567890ABCDEF1234567890ABCDEF12")
            == "0xabcdef1234567890abcdef1234567890abcdef12"
        )

    def test_already_lower(self):
        assert (
            to_normalized_address("0xabcdef1234567890abcdef1234567890abcdef12")
            == "0xabcdef1234567890abcdef1234567890abcdef12"
        )

    def test_non_string(self):
        assert to_normalized_address(123) == 123


# ---------------------------------------------------------------------------
# to_float_or_none
# ---------------------------------------------------------------------------


class TestToFloatOrNone:
    def test_none(self):
        assert to_float_or_none(None) is None

    def test_string(self):
        assert to_float_or_none("1.5") == 1.5

    def test_int(self):
        assert to_float_or_none(42) == 42.0


# ---------------------------------------------------------------------------
# parse_block_json
# ---------------------------------------------------------------------------


SAMPLE_BLOCK_JSON = {
    "number": "0x10",
    "hash": "0xblockhash",
    "parentHash": "0xparenthash",
    "nonce": "0x0000000000000001",
    "sha3Uncles": "0xsha3uncles",
    "logsBloom": "0xlogsbloom",
    "transactionsRoot": "0xtxroot",
    "stateRoot": "0xstateroot",
    "receiptsRoot": "0xreceiptsroot",
    "miner": "0xAbCdEf0000000000000000000000000000000001",
    "difficulty": "0x100",
    "totalDifficulty": "0x200",
    "size": "0x300",
    "extraData": "0xextra",
    "gasLimit": "0x1000",
    "gasUsed": "0x500",
    "timestamp": "0x60000000",
    "baseFeePerGas": "0x3b9aca00",
    "withdrawalsRoot": "0xwroot",
    "withdrawals": [
        {
            "index": "0x0",
            "validatorIndex": "0x1",
            "address": "0xaddr",
            "amount": "0xa",
        }
    ],
    "blobGasUsed": "0x20000",
    "excessBlobGas": "0x10000",
    "transactions": [
        {"hash": "0xtx1"},
        {"hash": "0xtx2"},
    ],
}


class TestParseBlockJson:
    def test_basic_fields(self):
        block = parse_block_json(SAMPLE_BLOCK_JSON)
        assert block["type"] == "block"
        assert block["number"] == 16
        assert block["hash"] == "0xblockhash"
        assert block["parent_hash"] == "0xparenthash"
        assert block["miner"] == "0xabcdef0000000000000000000000000000000001"
        assert block["difficulty"] == 256
        assert block["total_difficulty"] == 512
        assert block["size"] == 768
        assert block["gas_limit"] == 4096
        assert block["gas_used"] == 1280
        assert block["timestamp"] == 0x60000000
        assert block["transaction_count"] == 2
        assert block["base_fee_per_gas"] == 1000000000
        assert block["blob_gas_used"] == 0x20000
        assert block["excess_blob_gas"] == 0x10000

    def test_withdrawals(self):
        block = parse_block_json(SAMPLE_BLOCK_JSON)
        assert len(block["withdrawals"]) == 1
        w = block["withdrawals"][0]
        assert w["index"] == 0
        assert w["validator_index"] == 1
        assert w["address"] == "0xaddr"
        assert w["amount"] == 10

    def test_pre_london_block(self):
        """Pre-London blocks have no baseFeePerGas."""
        pre_london = {
            "number": "0x1",
            "hash": "0xhash",
            "parentHash": "0xph",
            "nonce": "0x00",
            "sha3Uncles": "0x00",
            "logsBloom": "0x00",
            "transactionsRoot": "0x00",
            "stateRoot": "0x00",
            "receiptsRoot": "0x00",
            "miner": "0x0000000000000000000000000000000000000001",
            "difficulty": "0x1",
            "totalDifficulty": "0x1",
            "size": "0x100",
            "extraData": "0x",
            "gasLimit": "0x1000",
            "gasUsed": "0x0",
            "timestamp": "0x1",
            "transactions": [],
        }
        block = parse_block_json(pre_london)
        assert block["base_fee_per_gas"] is None
        assert block["withdrawals"] == []
        assert block["blob_gas_used"] is None
        assert block["excess_blob_gas"] is None
        assert block["transaction_count"] == 0


# ---------------------------------------------------------------------------
# parse_transaction_json
# ---------------------------------------------------------------------------


SAMPLE_TX_JSON = {
    "hash": "0xtxhash",
    "nonce": "0x5",
    "blockHash": "0xblockhash",
    "blockNumber": "0x10",
    "transactionIndex": "0x0",
    "from": "0xAbCd0000000000000000000000000000000000AA",
    "to": "0xEfGh0000000000000000000000000000000000BB",
    "value": "0xde0b6b3a7640000",
    "gas": "0x5208",
    "gasPrice": "0x3b9aca00",
    "input": "0x",
    "maxFeePerGas": "0x77359400",
    "maxPriorityFeePerGas": "0x3b9aca00",
    "type": "0x2",
    "maxFeePerBlobGas": None,
    "blobVersionedHashes": None,
    "v": "0x1",
    "r": "0xabc",
    "s": "0xdef",
}


class TestParseTransactionJson:
    def test_basic_fields(self):
        tx = parse_transaction_json(SAMPLE_TX_JSON, block_timestamp=1000)
        assert tx["type"] == "transaction"
        assert tx["hash"] == "0xtxhash"
        assert tx["nonce"] == 5
        assert tx["block_number"] == 16
        assert tx["block_timestamp"] == 1000
        assert tx["transaction_index"] == 0
        assert (
            tx["from_address"]
            == "0xabcd0000000000000000000000000000000000aa"
        )
        assert tx["value"] == 1000000000000000000
        assert tx["gas"] == 21000
        assert tx["gas_price"] == 1000000000
        assert tx["transaction_type"] == 2
        assert tx["v"] == 1
        assert tx["r"] == 0xABC
        assert tx["s"] == 0xDEF
        assert tx["blob_versioned_hashes"] == []

    def test_null_to_address(self):
        """Contract creation has null to_address."""
        tx_json = dict(SAMPLE_TX_JSON, to=None)
        tx = parse_transaction_json(tx_json, block_timestamp=1000)
        assert tx["to_address"] is None


# ---------------------------------------------------------------------------
# parse_receipt_json
# ---------------------------------------------------------------------------


SAMPLE_RECEIPT_JSON = {
    "transactionHash": "0xtxhash",
    "transactionIndex": "0x0",
    "blockHash": "0xblockhash",
    "blockNumber": "0x10",
    "cumulativeGasUsed": "0x5208",
    "gasUsed": "0x5208",
    "contractAddress": None,
    "root": None,
    "status": "0x1",
    "effectiveGasPrice": "0x3b9aca00",
    "l1Fee": None,
    "l1GasUsed": None,
    "l1GasPrice": None,
    "l1FeeScalar": None,
    "blobGasPrice": None,
    "blobGasUsed": None,
    "logs": [],
}


class TestParseReceiptJson:
    def test_basic_fields(self):
        receipt = parse_receipt_json(SAMPLE_RECEIPT_JSON)
        assert receipt["type"] == "receipt"
        assert receipt["transaction_hash"] == "0xtxhash"
        assert receipt["transaction_index"] == 0
        assert receipt["block_number"] == 16
        assert receipt["cumulative_gas_used"] == 21000
        assert receipt["gas_used"] == 21000
        assert receipt["contract_address"] is None
        assert receipt["status"] == 1
        assert receipt["effective_gas_price"] == 1000000000
        assert receipt["l1_fee"] is None

    def test_contract_creation(self):
        receipt_json = dict(
            SAMPLE_RECEIPT_JSON,
            contractAddress="0xAbCd0000000000000000000000000000000000FF",
        )
        receipt = parse_receipt_json(receipt_json)
        assert (
            receipt["contract_address"]
            == "0xabcd0000000000000000000000000000000000ff"
        )

    def test_l2_fields(self):
        """Optimism-style L2 receipt fields."""
        receipt_json = dict(
            SAMPLE_RECEIPT_JSON,
            l1Fee="0x100",
            l1GasUsed="0x200",
            l1GasPrice="0x300",
            l1FeeScalar="1.5",
        )
        receipt = parse_receipt_json(receipt_json)
        assert receipt["l1_fee"] == 256
        assert receipt["l1_gas_used"] == 512
        assert receipt["l1_gas_price"] == 768
        assert receipt["l1_fee_scalar"] == 1.5


# ---------------------------------------------------------------------------
# parse_log_json
# ---------------------------------------------------------------------------


SAMPLE_LOG_JSON = {
    "logIndex": "0x0",
    "transactionHash": "0xtxhash",
    "transactionIndex": "0x0",
    "blockHash": "0xblockhash",
    "blockNumber": "0x10",
    "address": "0xcontractaddr",
    "data": "0xdata",
    "topics": ["0xtopic0", "0xtopic1"],
}


class TestParseLogJson:
    def test_basic_fields(self):
        log = parse_log_json(SAMPLE_LOG_JSON)
        assert log["type"] == "log"
        assert log["log_index"] == 0
        assert log["transaction_hash"] == "0xtxhash"
        assert log["transaction_index"] == 0
        assert log["block_number"] == 16
        assert log["address"] == "0xcontractaddr"
        assert log["data"] == "0xdata"
        assert log["topics"] == ["0xtopic0", "0xtopic1"]

    def test_empty_topics(self):
        log_json = dict(SAMPLE_LOG_JSON, topics=None)
        log = parse_log_json(log_json)
        assert log["topics"] == []


# ---------------------------------------------------------------------------
# enrich_transactions
# ---------------------------------------------------------------------------


class TestEnrichTransactions:
    def test_basic_enrichment(self):
        txs = [
            {"hash": "0xtx1", "nonce": 1, "v": 27, "r": 100, "s": 200},
            {"hash": "0xtx2", "nonce": 2, "v": 28, "r": 101, "s": 201},
        ]
        receipts = [
            {
                "transaction_hash": "0xtx1",
                "cumulative_gas_used": 21000,
                "gas_used": 21000,
                "contract_address": None,
                "root": None,
                "status": 1,
                "effective_gas_price": 1000,
                "l1_fee": None,
                "l1_gas_used": None,
                "l1_gas_price": None,
                "l1_fee_scalar": None,
                "blob_gas_price": None,
                "blob_gas_used": None,
            },
            {
                "transaction_hash": "0xtx2",
                "cumulative_gas_used": 42000,
                "gas_used": 21000,
                "contract_address": "0xcontract",
                "root": "0xroot",
                "status": 1,
                "effective_gas_price": 2000,
                "l1_fee": None,
                "l1_gas_used": None,
                "l1_gas_price": None,
                "l1_fee_scalar": None,
                "blob_gas_price": None,
                "blob_gas_used": None,
            },
        ]

        enriched = enrich_transactions(txs, receipts)
        assert len(enriched) == 2

        # Check receipt fields are added
        assert enriched[0]["receipt_gas_used"] == 21000
        assert enriched[0]["receipt_status"] == 1
        assert enriched[0]["receipt_contract_address"] is None
        assert enriched[1]["receipt_contract_address"] == "0xcontract"
        assert enriched[1]["receipt_effective_gas_price"] == 2000

        # Check original tx fields preserved (including v, r, s)
        assert enriched[0]["nonce"] == 1
        assert enriched[0]["v"] == 27
        assert enriched[0]["r"] == 100
        assert enriched[0]["s"] == 200

    def test_missing_receipt_raises(self):
        txs = [{"hash": "0xtx_missing"}]
        receipts = []
        with pytest.raises(ValueError, match="Receipt not found"):
            enrich_transactions(txs, receipts)


# ---------------------------------------------------------------------------
# BatchRpcClient
# ---------------------------------------------------------------------------


class TestBatchRpcClient:
    def test_get_latest_block_number(self):
        client = BatchRpcClient("http://fake:8545")
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "jsonrpc": "2.0",
            "id": 1,
            "result": "0x100",
        }
        mock_response.raise_for_status = MagicMock()

        with patch.object(client, "_get_session") as mock_session:
            mock_session.return_value.post.return_value = mock_response
            result = client.get_latest_block_number()

        assert result == 256

    def test_make_batch_request(self):
        client = BatchRpcClient("http://fake:8545")
        mock_response = MagicMock()
        mock_response.json.return_value = [
            {"jsonrpc": "2.0", "id": 1, "result": "0x1"},
            {"jsonrpc": "2.0", "id": 2, "result": "0x2"},
        ]
        mock_response.raise_for_status = MagicMock()

        with patch.object(client, "_get_session") as mock_session:
            mock_session.return_value.post.return_value = mock_response
            results = client.make_batch_request(
                [
                    {"jsonrpc": "2.0", "method": "test", "params": [], "id": 1},
                    {"jsonrpc": "2.0", "method": "test", "params": [], "id": 2},
                ]
            )

        assert len(results) == 2
        assert results[0]["id"] == 1
        assert results[1]["id"] == 2

    def test_thread_local_session(self):
        """Each thread gets its own session, main thread gets a different one."""
        client = BatchRpcClient("http://fake:8545")

        main_session = client._get_session()
        child_sessions = []
        barrier = threading.Barrier(2)

        def get_session_id():
            s = client._get_session()
            child_sessions.append(id(s))
            barrier.wait()  # keep thread alive so its session isn't recycled

        threads = [threading.Thread(target=get_session_id) for _ in range(2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Child threads should not share the main thread's session
        assert all(sid != id(main_session) for sid in child_sessions)


# ---------------------------------------------------------------------------
# FastBlockExporter
# ---------------------------------------------------------------------------


class TestFastBlockExporter:
    def test_export_single_block(self):
        mock_client = MagicMock()
        mock_client.make_batch_request.return_value = [
            {
                "jsonrpc": "2.0",
                "id": 100,
                "result": {
                    "number": "0x64",
                    "hash": "0xblockhash100",
                    "parentHash": "0xph",
                    "nonce": "0x00",
                    "sha3Uncles": "0x00",
                    "logsBloom": "0x00",
                    "transactionsRoot": "0x00",
                    "stateRoot": "0x00",
                    "receiptsRoot": "0x00",
                    "miner": "0x0000000000000000000000000000000000000001",
                    "difficulty": "0x1",
                    "totalDifficulty": "0x1",
                    "size": "0x100",
                    "extraData": "0x",
                    "gasLimit": "0x1000",
                    "gasUsed": "0x0",
                    "timestamp": "0x60000000",
                    "transactions": [
                        {
                            "hash": "0xtx1",
                            "nonce": "0x0",
                            "blockHash": "0xblockhash100",
                            "blockNumber": "0x64",
                            "transactionIndex": "0x0",
                            "from": "0x0000000000000000000000000000000000000001",
                            "to": "0x0000000000000000000000000000000000000002",
                            "value": "0x0",
                            "gas": "0x5208",
                            "gasPrice": "0x1",
                            "input": "0x",
                            "type": "0x0",
                            "v": "0x1b",
                            "r": "0x1",
                            "s": "0x2",
                        }
                    ],
                },
            }
        ]

        exporter = FastBlockExporter(mock_client, batch_size=10, max_workers=1)
        blocks, txs = exporter.export_blocks_and_transactions(100, 100)

        assert len(blocks) == 1
        assert blocks[0]["number"] == 100
        assert blocks[0]["timestamp"] == 0x60000000

        assert len(txs) == 1
        assert txs[0]["hash"] == "0xtx1"
        assert txs[0]["block_timestamp"] == 0x60000000
        assert txs[0]["block_number"] == 100


# ---------------------------------------------------------------------------
# FastReceiptExporter
# ---------------------------------------------------------------------------


class TestFastReceiptExporter:
    def test_export_single_receipt(self):
        mock_client = MagicMock()
        mock_client.make_batch_request.return_value = [
            {
                "jsonrpc": "2.0",
                "id": 0,
                "result": {
                    "transactionHash": "0xtx1",
                    "transactionIndex": "0x0",
                    "blockHash": "0xblockhash",
                    "blockNumber": "0x64",
                    "cumulativeGasUsed": "0x5208",
                    "gasUsed": "0x5208",
                    "contractAddress": None,
                    "root": None,
                    "status": "0x1",
                    "effectiveGasPrice": "0x1",
                    "logs": [
                        {
                            "logIndex": "0x0",
                            "transactionHash": "0xtx1",
                            "transactionIndex": "0x0",
                            "blockHash": "0xblockhash",
                            "blockNumber": "0x64",
                            "address": "0xcontract",
                            "data": "0xdata",
                            "topics": ["0xtopic0"],
                        }
                    ],
                },
            }
        ]

        exporter = FastReceiptExporter(mock_client, batch_size=10, max_workers=1)
        receipts, logs = exporter.export_receipts_and_logs(["0xtx1"])

        assert len(receipts) == 1
        assert receipts[0]["transaction_hash"] == "0xtx1"
        assert receipts[0]["gas_used"] == 21000
        assert receipts[0]["status"] == 1

        assert len(logs) == 1
        assert logs[0]["log_index"] == 0
        assert logs[0]["address"] == "0xcontract"
        assert logs[0]["topics"] == ["0xtopic0"]


# ---------------------------------------------------------------------------
# FastBlockReceiptExporter
# ---------------------------------------------------------------------------


def _make_block_receipts_response(block_number, receipts_data):
    """Helper to build a mock eth_getBlockReceipts RPC response."""
    return {
        "jsonrpc": "2.0",
        "id": block_number,
        "result": receipts_data,
    }


class TestFastBlockReceiptExporter:
    def test_single_block_with_receipts_and_logs(self):
        mock_client = MagicMock()
        mock_client.make_batch_request.return_value = [
            _make_block_receipts_response(
                100,
                [
                    {
                        "transactionHash": "0xtx1",
                        "transactionIndex": "0x0",
                        "blockHash": "0xblockhash100",
                        "blockNumber": "0x64",
                        "cumulativeGasUsed": "0x5208",
                        "gasUsed": "0x5208",
                        "contractAddress": None,
                        "root": None,
                        "status": "0x1",
                        "effectiveGasPrice": "0x3b9aca00",
                        "logs": [
                            {
                                "logIndex": "0x0",
                                "transactionHash": "0xtx1",
                                "transactionIndex": "0x0",
                                "blockHash": "0xblockhash100",
                                "blockNumber": "0x64",
                                "address": "0xcontract1",
                                "data": "0xdata1",
                                "topics": ["0xtopic0", "0xtopic1"],
                            },
                            {
                                "logIndex": "0x1",
                                "transactionHash": "0xtx1",
                                "transactionIndex": "0x0",
                                "blockHash": "0xblockhash100",
                                "blockNumber": "0x64",
                                "address": "0xcontract2",
                                "data": "0xdata2",
                                "topics": ["0xtopic0"],
                            },
                        ],
                    }
                ],
            )
        ]

        exporter = FastBlockReceiptExporter(mock_client, batch_size=10, max_workers=1)
        receipts, logs = exporter.export_receipts_and_logs(100, 100)

        assert len(receipts) == 1
        assert receipts[0]["transaction_hash"] == "0xtx1"
        assert receipts[0]["block_number"] == 100
        assert receipts[0]["gas_used"] == 21000
        assert receipts[0]["status"] == 1
        assert receipts[0]["effective_gas_price"] == 1000000000

        assert len(logs) == 2
        assert logs[0]["log_index"] == 0
        assert logs[0]["address"] == "0xcontract1"
        assert logs[0]["topics"] == ["0xtopic0", "0xtopic1"]
        assert logs[1]["log_index"] == 1
        assert logs[1]["address"] == "0xcontract2"

    def test_multiple_blocks_ordering(self):
        """Receipts and logs are returned sorted by block_number."""
        mock_client = MagicMock()
        # Return in reverse order to test sorting
        mock_client.make_batch_request.return_value = [
            _make_block_receipts_response(
                101,
                [
                    {
                        "transactionHash": "0xtx_b101",
                        "transactionIndex": "0x0",
                        "blockHash": "0xbh101",
                        "blockNumber": "0x65",
                        "cumulativeGasUsed": "0x5208",
                        "gasUsed": "0x5208",
                        "contractAddress": None,
                        "root": None,
                        "status": "0x1",
                        "effectiveGasPrice": "0x1",
                        "logs": [
                            {
                                "logIndex": "0x0",
                                "transactionHash": "0xtx_b101",
                                "transactionIndex": "0x0",
                                "blockHash": "0xbh101",
                                "blockNumber": "0x65",
                                "address": "0xaddr101",
                                "data": "0x",
                                "topics": [],
                            }
                        ],
                    }
                ],
            ),
            _make_block_receipts_response(
                100,
                [
                    {
                        "transactionHash": "0xtx_b100",
                        "transactionIndex": "0x0",
                        "blockHash": "0xbh100",
                        "blockNumber": "0x64",
                        "cumulativeGasUsed": "0x5208",
                        "gasUsed": "0x5208",
                        "contractAddress": None,
                        "root": None,
                        "status": "0x1",
                        "effectiveGasPrice": "0x1",
                        "logs": [
                            {
                                "logIndex": "0x0",
                                "transactionHash": "0xtx_b100",
                                "transactionIndex": "0x0",
                                "blockHash": "0xbh100",
                                "blockNumber": "0x64",
                                "address": "0xaddr100",
                                "data": "0x",
                                "topics": [],
                            }
                        ],
                    }
                ],
            ),
        ]

        exporter = FastBlockReceiptExporter(mock_client, batch_size=10, max_workers=1)
        receipts, logs = exporter.export_receipts_and_logs(100, 101)

        assert len(receipts) == 2
        # Sorted by block_number
        assert receipts[0]["block_number"] == 100
        assert receipts[0]["transaction_hash"] == "0xtx_b100"
        assert receipts[1]["block_number"] == 101
        assert receipts[1]["transaction_hash"] == "0xtx_b101"

        assert len(logs) == 2
        assert logs[0]["block_number"] == 100
        assert logs[1]["block_number"] == 101

    def test_empty_block(self):
        """Block with no transactions returns empty receipts and logs."""
        mock_client = MagicMock()
        mock_client.make_batch_request.return_value = [
            _make_block_receipts_response(100, []),
        ]

        exporter = FastBlockReceiptExporter(mock_client, batch_size=10, max_workers=1)
        receipts, logs = exporter.export_receipts_and_logs(100, 100)

        assert receipts == []
        assert logs == []

    def test_null_result_treated_as_empty(self):
        """Some nodes return null instead of [] for empty blocks."""
        mock_client = MagicMock()
        mock_client.make_batch_request.return_value = [
            _make_block_receipts_response(100, None),
        ]

        exporter = FastBlockReceiptExporter(mock_client, batch_size=10, max_workers=1)
        receipts, logs = exporter.export_receipts_and_logs(100, 100)

        assert receipts == []
        assert logs == []

    def test_format_compatibility_with_parse_receipt(self):
        """Output format matches parse_receipt_json / parse_log_json output."""
        raw_receipt = {
            "transactionHash": "0xabc",
            "transactionIndex": "0x2",
            "blockHash": "0xbh",
            "blockNumber": "0xa",
            "cumulativeGasUsed": "0x100",
            "gasUsed": "0x80",
            "contractAddress": "0xAbCd0000000000000000000000000000000000FF",
            "root": "0xroot",
            "status": "0x1",
            "effectiveGasPrice": "0x3b9aca00",
            "l1Fee": "0x10",
            "l1GasUsed": "0x20",
            "l1GasPrice": "0x30",
            "l1FeeScalar": "1.5",
            "blobGasPrice": "0x40",
            "blobGasUsed": "0x50",
            "logs": [
                {
                    "logIndex": "0x5",
                    "transactionHash": "0xabc",
                    "transactionIndex": "0x2",
                    "blockHash": "0xbh",
                    "blockNumber": "0xa",
                    "address": "0xlogaddr",
                    "data": "0xlogdata",
                    "topics": ["0xt0", "0xt1"],
                }
            ],
        }

        # Direct parse
        expected_receipt = parse_receipt_json(raw_receipt)
        expected_log = parse_log_json(raw_receipt["logs"][0])

        # Via FastBlockReceiptExporter
        mock_client = MagicMock()
        mock_client.make_batch_request.return_value = [
            _make_block_receipts_response(10, [raw_receipt]),
        ]

        exporter = FastBlockReceiptExporter(mock_client, batch_size=10, max_workers=1)
        receipts, logs = exporter.export_receipts_and_logs(10, 10)

        assert receipts[0] == expected_receipt
        assert logs[0] == expected_log
