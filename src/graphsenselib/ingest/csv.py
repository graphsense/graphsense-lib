import gzip
from csv import DictWriter
from typing import Iterable

BLOCK_HEADER = [
    "parent_hash",
    "nonce",
    "sha3_uncles",
    "logs_bloom",
    "transactions_root",
    "state_root",
    "receipts_root",
    "miner",
    "difficulty",
    "total_difficulty",
    "size",
    "extra_data",
    "gas_limit",
    "gas_used",
    "timestamp",
    "transaction_count",
    "base_fee_per_gas",
    "block_id",
    "block_id_group",
    "block_hash",
]

TX_HEADER = [
    "nonce",
    "transaction_index",
    "from_address",
    "to_address",
    "value",
    "gas",
    "gas_price",
    "input",
    "block_timestamp",
    "block_hash",
    "max_fee_per_gas",
    "max_priority_fee_per_gas",
    "transaction_type",
    "receipt_cumulative_gas_used",
    "receipt_gas_used",
    "receipt_contract_address",
    "receipt_root",
    "receipt_status",
    "receipt_effective_gas_price",
    "tx_hash",
    "tx_hash_prefix",
    "block_id",
]

TRACE_HEADER = [
    "transaction_index",
    "from_address",
    "to_address",
    "value",
    "input",
    "output",
    "trace_type",
    "call_type",
    "reward_type",
    "gas",
    "gas_used",
    "subtraces",
    "trace_address",
    "error",
    "status",
    "trace_id",
    "trace_index",
    "tx_hash",
    "block_id",
    "block_id_group",
]

LOGS_HEADER = [
    "block_id_group",
    "block_id",
    "block_hash",
    "address",
    "data",
    "topics",
    "topic0",
    "tx_hash",
    "log_index",
    "transaction_index",
]


def format_blocks_csv(
    items: Iterable,
    block_bucket_size: int = 1_000,
) -> None:
    """Format blocks."""

    for item in items:
        # remove column
        item.pop("type")
        # rename/add columns
        item["block_id"] = item.pop("number")
        item["block_id_group"] = item["block_id"] // block_bucket_size
        item["block_hash"] = item.pop("hash")

    return items


def format_transactions_csv(
    items: Iterable,
    tx_hash_prefix_len: int = 4,
) -> None:
    """Format transactions."""

    for item in items:
        # remove column
        item.pop("type")
        # rename/add columns
        item["tx_hash"] = item.pop("hash")
        hash_slice = slice(2, 2 + tx_hash_prefix_len)
        item["tx_hash_prefix"] = item["tx_hash"][hash_slice]
        item["block_id"] = item.pop("block_number")

    return items


def format_traces_csv(
    items: Iterable,
    block_bucket_size: int = 1_000,
) -> None:
    """Format traces."""

    for item in items:
        # remove column
        item.pop("type")
        # rename/add columns
        item["tx_hash"] = item.pop("transaction_hash")
        item["block_id"] = item.pop("block_number")
        item["block_id_group"] = item["block_id"] // block_bucket_size
        item["trace_address"] = (
            "|".join(map(str, item["trace_address"]))
            if item["trace_address"] is not None
            else None
        )

    return items


def format_logs_csv(
    items: Iterable,
    block_bucket_size: int = 1_000,
) -> None:
    """Format logs."""

    for item in items:
        # remove column
        item.pop("type")
        # rename/add columns
        item["tx_hash"] = item.pop("transaction_hash")
        item["block_id"] = item.pop("block_number")
        item["block_id_group"] = item["block_id"] // block_bucket_size

        tpcs = item["topics"]

        if tpcs is None:
            tpcs = []

        if "topic0" not in item:
            item["topic0"] = tpcs[0] if len(tpcs) > 0 else None

        qt = ",".join([f'"{t}"' for t in tpcs])

        item["topics"] = f"[{qt}]"

        if "transaction_hash" in item:
            item.pop("transaction_hash")

    return items


def write_csv(
    filename: str, data: Iterable, header: Iterable, delimiter: str = ",", quoting=None
) -> None:
    """Write list of dicts to compresses CSV file."""

    with gzip.open(filename, "wt") as csv_file:
        if quoting is None:
            csv_writer = DictWriter(csv_file, delimiter=delimiter, fieldnames=header)
        else:
            csv_writer = DictWriter(
                csv_file,
                delimiter=delimiter,
                fieldnames=header,
                quoting=quoting,
                quotechar="",
            )
        csv_writer.writeheader()
        for row in data:
            csv_writer.writerow(row)
