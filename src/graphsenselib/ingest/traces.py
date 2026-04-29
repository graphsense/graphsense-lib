"""Fast trace exporter using batch JSON-RPC trace_block calls.

Replaces ethereum-etl's ExportTracesJob which uses batch_size=1 (hardcoded),
resulting in one HTTP request per block. This implementation batches multiple
trace_block calls per HTTP request and executes batches concurrently.

The output dict format is identical to ethereum-etl's trace_mapper.trace_to_dict().
"""

import logging
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

from graphsenselib.ingest.rpc_eth import (
    BatchRpcClient,
    hex_to_dec,
    validate_rpc_fields,
)

logger = logging.getLogger(__name__)

# Top-level keys in a trace_block response item
_TRACE_KNOWN_KEYS = frozenset(
    {
        "action",
        "result",
        "type",
        "subtraces",
        "traceAddress",
        "transactionHash",
        "transactionPosition",
        "error",
        "blockHash",
        "blockNumber",
    }
)

_TRACE_BLACKLIST = frozenset()

# -- Trace action/result keys per trace_type --------------------------------

_ACTION_KEYS = {
    "call": frozenset({"callType", "from", "gas", "input", "to", "value"}),
    "create": frozenset({"from", "gas", "init", "value", "creationMethod"}),
    "suicide": frozenset({"address", "refundAddress", "balance"}),
    "reward": frozenset({"author", "value", "rewardType"}),
}

_ACTION_BLACKLIST = {
    "call": frozenset(),
    "create": frozenset(),
    "suicide": frozenset(),
    "reward": frozenset(),
}

_RESULT_KEYS = {
    "call": frozenset({"gasUsed", "output"}),
    "create": frozenset({"address", "code", "gasUsed"}),
    "suicide": frozenset(),
    "reward": frozenset(),
}

_RESULT_BLACKLIST = {
    "call": frozenset(),
    "create": frozenset(),
    "suicide": frozenset(),
    "reward": frozenset(),
}


def trace_address_to_str(trace_address):
    if trace_address is None or len(trace_address) == 0:
        return ""
    return "_".join(str(a) for a in trace_address)


def _to_normalized_address(address):
    if address is None or not isinstance(address, str):
        return address
    return address.lower()


def parse_raw_trace(json_trace, block_number):
    """Convert raw trace_block JSON response item to the dict format
    matching ethereum-etl's trace_mapper.trace_to_dict()."""
    validate_rpc_fields(json_trace.keys(), _TRACE_KNOWN_KEYS, _TRACE_BLACKLIST, "trace")
    action = json_trace.get("action") or {}
    result = json_trace.get("result") or {}
    trace_type = json_trace.get("type")

    trace = {
        "type": "trace",
        "block_number": block_number,
        "transaction_hash": json_trace.get("transactionHash"),
        "transaction_index": json_trace.get("transactionPosition"),
        "subtraces": json_trace.get("subtraces", 0),
        "trace_address": json_trace.get("traceAddress", []),
        "error": json_trace.get("error"),
        "trace_type": trace_type,
        "call_type": None,
        "reward_type": None,
        "from_address": None,
        "to_address": None,
        "value": None,
        "gas": None,
        "gas_used": None,
        "input": None,
        "output": None,
        "creation_method": None,
    }

    if trace_type in _ACTION_KEYS:
        validate_rpc_fields(
            action.keys(),
            _ACTION_KEYS[trace_type],
            _ACTION_BLACKLIST[trace_type],
            f"trace.action ({trace_type})",
        )
        if result:
            validate_rpc_fields(
                result.keys(),
                _RESULT_KEYS[trace_type],
                _RESULT_BLACKLIST[trace_type],
                f"trace.result ({trace_type})",
            )

    if trace_type == "call":
        trace["from_address"] = _to_normalized_address(action.get("from"))
        trace["to_address"] = _to_normalized_address(action.get("to"))
        trace["value"] = hex_to_dec(action.get("value"))
        trace["gas"] = hex_to_dec(action.get("gas"))
        trace["input"] = action.get("input")
        trace["call_type"] = action.get("callType")
        trace["gas_used"] = hex_to_dec(result.get("gasUsed"))
        trace["output"] = result.get("output")
    elif trace_type == "create":
        trace["from_address"] = _to_normalized_address(action.get("from"))
        trace["to_address"] = _to_normalized_address(result.get("address"))
        trace["value"] = hex_to_dec(action.get("value"))
        trace["gas"] = hex_to_dec(action.get("gas"))
        trace["input"] = action.get("init")
        trace["gas_used"] = hex_to_dec(result.get("gasUsed"))
        trace["output"] = result.get("code")
        trace["creation_method"] = action.get("creationMethod")
    elif trace_type == "suicide":
        trace["from_address"] = _to_normalized_address(action.get("address"))
        trace["to_address"] = _to_normalized_address(action.get("refundAddress"))
        trace["value"] = hex_to_dec(action.get("balance"))
    elif trace_type == "reward":
        trace["to_address"] = _to_normalized_address(action.get("author"))
        trace["value"] = hex_to_dec(action.get("value"))
        trace["reward_type"] = action.get("rewardType")

    return trace


def calculate_trace_statuses(traces):
    """Calculate trace statuses with parent-to-child failure propagation.

    Reimplements ethereum-etl's trace_status_calculator:
    1. Set status=0 for traces with errors, status=1 otherwise
    2. Group by transaction hash
    3. Propagate parent failure to children via trace_address tree
    """
    for trace in traces:
        error = trace.get("error")
        if error is not None and len(error) > 0:
            trace["status"] = 0
        else:
            trace["status"] = 1

    grouped = defaultdict(list)
    for trace in traces:
        tx_hash = trace.get("transaction_hash")
        if tx_hash is not None and len(tx_hash) > 0:
            grouped[tx_hash].append(trace)

    for tx_traces in grouped.values():
        _propagate_status_for_transaction(tx_traces)


def _propagate_status_for_transaction(tx_traces):
    sorted_traces = sorted(tx_traces, key=lambda t: len(t.get("trace_address") or []))
    indexed = {trace_address_to_str(t["trace_address"]): t for t in sorted_traces}
    for t in sorted_traces:
        ta = t.get("trace_address") or []
        if len(ta) > 0:
            parent_key = trace_address_to_str(ta[:-1])
            parent = indexed.get(parent_key)
            if parent is not None and parent["status"] == 0:
                t["status"] = 0


def calculate_trace_ids(traces):
    """Calculate trace IDs.

    Reimplements ethereum-etl's trace_id_calculator:
    - Transaction-scoped: {trace_type}_{tx_hash}_{trace_address_str}
    - Block-scoped (rewards): {trace_type}_{block_number}_{index}
    """
    grouped_by_block = defaultdict(list)
    for trace in traces:
        grouped_by_block[trace["block_number"]].append(trace)

    for block_traces in grouped_by_block.values():
        tx_traces = defaultdict(list)
        block_scoped_by_type = defaultdict(list)
        for trace in block_traces:
            tx_hash = trace.get("transaction_hash")
            if tx_hash is not None and len(tx_hash) > 0:
                tx_traces[tx_hash].append(trace)
            else:
                block_scoped_by_type[trace.get("trace_type")].append(trace)

        for tx_hash, t_list in tx_traces.items():
            # Keep compatibility with ethereum-etl trace IDs: tx hash in the
            # trace_id must not include the optional "0x" prefix.
            normalized_tx_hash = (
                tx_hash[2:] if tx_hash.startswith(("0x", "0X")) else tx_hash
            )
            for t in t_list:
                ta_str = trace_address_to_str(t["trace_address"])
                t["trace_id"] = f"{t['trace_type']}_{normalized_tx_hash}_{ta_str}"

        # Match ethereum-etl behavior for block-scoped traces:
        # group by trace_type and sort by reward_type/from/to/value first.
        for type_traces in block_scoped_by_type.values():
            sorted_traces = sorted(
                type_traces,
                key=lambda trace: (
                    trace.get("reward_type"),
                    trace.get("from_address"),
                    trace.get("to_address"),
                    trace.get("value"),
                ),
            )
            for idx, t in enumerate(sorted_traces):
                t["trace_id"] = f"{t['trace_type']}_{t['block_number']}_{idx}"


def calculate_trace_indexes(traces):
    # Match legacy behavior: trace_index resets per block.
    grouped_by_block = defaultdict(list)
    for trace in traces:
        grouped_by_block[trace["block_number"]].append(trace)

    for block_number in sorted(grouped_by_block.keys()):
        for idx, trace in enumerate(grouped_by_block[block_number]):
            trace["trace_index"] = idx


class TraceExporter:
    """Fast trace exporter using batch JSON-RPC trace_block calls.

    Key improvements over ethereum-etl's ExportTracesJob:
    - Configurable trace_batch_size (blocks per JSON-RPC batch) instead of 1
    - Direct JSON -> output dict (no intermediate domain objects)
    - Concurrent batch execution via ThreadPoolExecutor
    """

    def __init__(
        self,
        provider_uri=None,
        timeout=600,
        trace_batch_size=10,
        max_workers=20,
        client=None,
    ):
        if client is not None:
            self.client = client
        else:
            self.client = BatchRpcClient(provider_uri, timeout)
        self.trace_batch_size = trace_batch_size
        self.max_workers = max_workers

    def _fetch_traces_for_blocks(self, block_numbers, max_retries=15):
        """Fetch traces for a batch of blocks via a single batch JSON-RPC call.

        Returns dict mapping block_number -> list of trace dicts,
        preserving the trace_block response order within each block.
        """
        rpc_requests = [
            {
                "jsonrpc": "2.0",
                "method": "trace_block",
                "params": [hex(bn)],
                "id": bn,
            }
            for bn in block_numbers
        ]

        last_error: Exception = Exception("no retries attempted")
        for attempt in range(max_retries):
            try:
                results = self.client.make_batch_request(rpc_requests)
                break
            except Exception as e:
                last_error = e
                if attempt < max_retries - 1:
                    wait = min(2**attempt, 30)
                    logger.warning(
                        f"Batch trace_block retry {attempt + 1}/{max_retries} "
                        f"for blocks {block_numbers[0]}-{block_numbers[-1]}: {e}. "
                        f"Waiting {wait}s."
                    )
                    time.sleep(wait)
        else:
            raise last_error

        if not isinstance(results, list):
            results = [results]

        result_map = {r["id"]: r for r in results}

        traces_by_block = {}
        for bn in block_numbers:
            result = result_map.get(bn)
            if result is None:
                raise ValueError(f"Missing response for trace_block({bn})")
            if result.get("error") is not None:
                raise ValueError(f"RPC error for trace_block({bn}): {result['error']}")
            json_traces = result.get("result") or []
            traces_by_block[bn] = [parse_raw_trace(jt, bn) for jt in json_traces]

        return traces_by_block

    def export_traces(self, start_block, end_block):
        """Export traces for block range using batch JSON-RPC.

        Returns (traces, None) matching the interface of
        AccountStreamerAdapter.export_traces().
        """
        block_numbers = list(range(start_block, end_block + 1))

        batches = [
            block_numbers[i : i + self.trace_batch_size]
            for i in range(0, len(block_numbers), self.trace_batch_size)
        ]

        all_traces_by_block = {}

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(
                    self._fetch_traces_for_blocks, batch_blocks
                ): batch_blocks
                for batch_blocks in batches
            }
            for future in as_completed(futures):
                block_traces = future.result()
                all_traces_by_block.update(block_traces)

        # Concatenate in block order, preserving trace_block order within
        all_traces = []

        # Keep parity with ethereum-etl special traces for legacy windows.
        if start_block <= 0 <= end_block:
            all_traces.extend(self._get_special_traces("genesis"))
        if start_block <= 1_920_000 <= end_block:
            all_traces.extend(self._get_special_traces("daofork"))
        for bn in range(start_block, end_block + 1):
            all_traces.extend(all_traces_by_block.get(bn, []))

        calculate_trace_statuses(all_traces)
        calculate_trace_ids(all_traces)
        calculate_trace_indexes(all_traces)

        return all_traces, None

    @staticmethod
    def _get_special_traces(kind: str) -> list[dict]:
        """Load special traces (genesis allocations or DAO fork state changes)
        from bundled CSV data files.

        These are static, deterministic traces for Ethereum block 0 (genesis)
        and block 1,920,000 (DAO fork). The CSV data was originally sourced
        from ethereum-etl's hardcoded alloc/state-change tables.
        """
        import csv
        import importlib.resources

        from graphsenselib.ingest import resources

        _TRACE_TEMPLATE = {
            "type": "trace",
            "transaction_hash": None,
            "transaction_index": None,
            "input": None,
            "output": None,
            "call_type": None,
            "reward_type": None,
            "gas": None,
            "gas_used": None,
            "subtraces": 0,
            "trace_address": None,
            "error": None,
            "status": 1,
            "trace_id": None,
            "trace_index": None,
        }

        traces = []
        if kind == "genesis":
            with (
                importlib.resources.files(resources)
                .joinpath("eth_genesis_transfers.csv")
                .open() as f
            ):
                for row in csv.DictReader(f):
                    traces.append(
                        {
                            **_TRACE_TEMPLATE,
                            "block_number": 0,
                            "from_address": None,
                            "to_address": row["address"],
                            "value": int(row["value"]),
                            "trace_type": "genesis",
                        }
                    )
        elif kind == "daofork":
            with (
                importlib.resources.files(resources)
                .joinpath("eth_daofork_state_changes.csv")
                .open() as f
            ):
                for row in csv.DictReader(f):
                    traces.append(
                        {
                            **_TRACE_TEMPLATE,
                            "block_number": 1_920_000,
                            "from_address": row["from_address"],
                            "to_address": row["to_address"],
                            "value": int(row["value"]),
                            "trace_type": "daofork",
                        }
                    )
        return traces
