"""Unit tests for traces module.

Verifies that the trace processing logic (parse, status calculation,
ID generation, indexing) produces the same output as ethereum-etl.
"""

from graphsenselib.ingest.traces import (
    calculate_trace_ids,
    calculate_trace_indexes,
    calculate_trace_statuses,
    hex_to_dec,
    parse_raw_trace,
    trace_address_to_str,
)


def test_hex_to_dec():
    assert hex_to_dec("0x1") == 1
    assert hex_to_dec("0xff") == 255
    assert hex_to_dec("0x0") == 0
    assert hex_to_dec(None) is None
    assert hex_to_dec(42) == 42


def test_trace_address_to_str():
    assert trace_address_to_str([]) == ""
    assert trace_address_to_str(None) == ""
    assert trace_address_to_str([0]) == "0"
    assert trace_address_to_str([0, 1, 2]) == "0_1_2"


def test_parse_raw_trace_call():
    raw = {
        "action": {
            "from": "0xAaA",
            "to": "0xBbB",
            "value": "0x100",
            "gas": "0x5208",
            "input": "0x",
            "callType": "call",
        },
        "result": {
            "gasUsed": "0x5208",
            "output": "0x",
        },
        "subtraces": 0,
        "traceAddress": [],
        "transactionHash": "0xdeadbeef",
        "transactionPosition": 0,
        "type": "call",
    }

    trace = parse_raw_trace(raw, 12345)

    assert trace["type"] == "trace"
    assert trace["block_number"] == 12345
    assert trace["transaction_hash"] == "0xdeadbeef"
    assert trace["transaction_index"] == 0
    assert trace["from_address"] == "0xaaa"
    assert trace["to_address"] == "0xbbb"
    assert trace["value"] == 256
    assert trace["gas"] == 21000
    assert trace["gas_used"] == 21000
    assert trace["input"] == "0x"
    assert trace["output"] == "0x"
    assert trace["trace_type"] == "call"
    assert trace["call_type"] == "call"
    assert trace["reward_type"] is None
    assert trace["subtraces"] == 0
    assert trace["trace_address"] == []
    assert trace["error"] is None


def test_parse_raw_trace_create():
    raw = {
        "action": {
            "from": "0xaaa",
            "value": "0x0",
            "gas": "0x10000",
            "init": "0x6060",
        },
        "result": {
            "address": "0xccc",
            "code": "0x6060",
            "gasUsed": "0x5000",
        },
        "subtraces": 0,
        "traceAddress": [0],
        "transactionHash": "0xdeadbeef",
        "transactionPosition": 1,
        "type": "create",
    }

    trace = parse_raw_trace(raw, 100)

    assert trace["trace_type"] == "create"
    assert trace["from_address"] == "0xaaa"
    assert trace["to_address"] == "0xccc"
    assert trace["input"] == "0x6060"
    assert trace["output"] == "0x6060"


def test_parse_raw_trace_suicide():
    raw = {
        "action": {
            "address": "0xaaa",
            "balance": "0x100",
            "refundAddress": "0xbbb",
        },
        "subtraces": 0,
        "traceAddress": [1],
        "transactionHash": "0xdeadbeef",
        "transactionPosition": 2,
        "type": "suicide",
    }

    trace = parse_raw_trace(raw, 100)

    assert trace["trace_type"] == "suicide"
    assert trace["from_address"] == "0xaaa"
    assert trace["to_address"] == "0xbbb"
    assert trace["value"] == 256


def test_parse_raw_trace_reward():
    raw = {
        "action": {
            "author": "0xminer",
            "value": "0x1bc16d674ec80000",
            "rewardType": "block",
        },
        "subtraces": 0,
        "traceAddress": [],
        "type": "reward",
    }

    trace = parse_raw_trace(raw, 100)

    assert trace["trace_type"] == "reward"
    assert trace["to_address"] == "0xminer"
    assert trace["value"] == 2000000000000000000
    assert trace["reward_type"] == "block"
    assert trace["transaction_hash"] is None
    assert trace["transaction_index"] is None


def test_calculate_trace_statuses_simple():
    traces = [
        {
            "transaction_hash": "0xabc",
            "trace_address": [],
            "error": None,
        },
        {
            "transaction_hash": "0xabc",
            "trace_address": [0],
            "error": "out of gas",
        },
    ]

    calculate_trace_statuses(traces)

    assert traces[0]["status"] == 1
    assert traces[1]["status"] == 0


def test_calculate_trace_statuses_propagation():
    """Parent failure propagates to children."""
    traces = [
        {
            "transaction_hash": "0xabc",
            "trace_address": [],
            "error": "reverted",
        },
        {
            "transaction_hash": "0xabc",
            "trace_address": [0],
            "error": None,
        },
        {
            "transaction_hash": "0xabc",
            "trace_address": [0, 0],
            "error": None,
        },
    ]

    calculate_trace_statuses(traces)

    assert traces[0]["status"] == 0  # has error
    assert traces[1]["status"] == 0  # parent failed
    assert traces[2]["status"] == 0  # grandparent failed


def test_calculate_trace_statuses_partial_failure():
    """Only the failing subtree gets status=0."""
    traces = [
        {
            "transaction_hash": "0xabc",
            "trace_address": [],
            "error": None,
        },
        {
            "transaction_hash": "0xabc",
            "trace_address": [0],
            "error": "reverted",
        },
        {
            "transaction_hash": "0xabc",
            "trace_address": [0, 0],
            "error": None,
        },
        {
            "transaction_hash": "0xabc",
            "trace_address": [1],
            "error": None,
        },
    ]

    calculate_trace_statuses(traces)

    assert traces[0]["status"] == 1  # root OK
    assert traces[1]["status"] == 0  # has error
    assert traces[2]["status"] == 0  # parent failed
    assert traces[3]["status"] == 1  # sibling OK


def test_calculate_trace_ids_transaction_scoped():
    traces = [
        {
            "block_number": 100,
            "transaction_hash": "0xabc",
            "trace_type": "call",
            "trace_address": [],
        },
        {
            "block_number": 100,
            "transaction_hash": "0xabc",
            "trace_type": "call",
            "trace_address": [0],
        },
    ]

    calculate_trace_ids(traces)

    assert traces[0]["trace_id"] == "call_abc_"
    assert traces[1]["trace_id"] == "call_abc_0"


def test_calculate_trace_ids_transaction_scoped_strips_uppercase_prefix():
    traces = [
        {
            "block_number": 100,
            "transaction_hash": "0XABCDEF",
            "trace_type": "call",
            "trace_address": [],
        }
    ]

    calculate_trace_ids(traces)
    assert traces[0]["trace_id"] == "call_ABCDEF_"


def test_calculate_trace_ids_block_scoped():
    traces = [
        {
            "block_number": 100,
            "transaction_hash": None,
            "trace_type": "reward",
            "trace_address": [],
        },
        {
            "block_number": 100,
            "transaction_hash": None,
            "trace_type": "reward",
            "trace_address": [],
        },
    ]

    calculate_trace_ids(traces)

    assert traces[0]["trace_id"] == "reward_100_0"
    assert traces[1]["trace_id"] == "reward_100_1"


def test_calculate_trace_indexes():
    traces = [
        {"block_number": 100, "a": 1},
        {"block_number": 100, "a": 2},
        {"block_number": 101, "a": 3},
    ]

    calculate_trace_indexes(traces)

    assert traces[0]["trace_index"] == 0
    assert traces[1]["trace_index"] == 1
    assert traces[2]["trace_index"] == 0


def test_calculate_trace_ids_block_scoped_sorted_like_ethereum_etl():
    traces = [
        {
            "block_number": 100,
            "transaction_hash": None,
            "trace_type": "reward",
            "trace_address": [],
            "reward_type": "uncle",
            "from_address": None,
            "to_address": "0xbbb",
            "value": 2,
        },
        {
            "block_number": 100,
            "transaction_hash": None,
            "trace_type": "reward",
            "trace_address": [],
            "reward_type": "block",
            "from_address": None,
            "to_address": "0xaaa",
            "value": 1,
        },
    ]

    calculate_trace_ids(traces)

    # Sorted by (reward_type, from_address, to_address, value)
    assert traces[0]["trace_id"] == "reward_100_1"
    assert traces[1]["trace_id"] == "reward_100_0"
