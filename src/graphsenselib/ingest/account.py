import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlparse

# import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from ethereumetl.jobs.export_blocks_job import ExportBlocksJob
from ethereumetl.jobs.export_receipts_job import ExportReceiptsJob
from ethereumetl.jobs.export_traces_job import ExportTracesJob
from ethereumetl.providers.auto import get_provider_from_uri
from ethereumetl.service.eth_service import EthService
from ethereumetl.streaming.enrich import enrich_transactions
from ethereumetl.streaming.eth_item_id_calculator import EthItemIdCalculator
from ethereumetl.streaming.eth_item_timestamp_calculator import (
    EthItemTimestampCalculator,
)
from ethereumetl.thread_local_proxy import ThreadLocalProxy
from pyarrow.fs import HadoopFileSystem
from web3 import Web3

from ..db import AnalyticsDb
from ..utils.accountmodel import hex_to_bytearray

logger = logging.getLogger(__name__)

BLOCK_BUCKET_SIZE = 1_000
TX_HASH_PREFIX_LEN = 5

PARQUET_PARTITION_SIZE = 100_000

PARQUET_SCHEMAS = {
    "log": pa.schema(
        [
            ("partition", pa.int16()),
            ("block_id_group", pa.int32()),
            ("block_id", pa.int32()),
            ("block_hash", pa.binary(32)),
            ("address", pa.binary(20)),
            ("data", pa.large_binary()),
            ("topics", pa.list_(pa.binary(32))),
            ("topic0", pa.binary(32)),
            ("tx_hash", pa.binary(32)),
            ("log_index", pa.int32()),
            ("transaction_index", pa.int32()),
        ]
    ),
    "trace": pa.schema(
        [
            ("partition", pa.int16()),
            ("block_id_group", pa.int32()),
            ("block_id", pa.int32()),
            ("tx_hash", pa.binary(32)),
            ("transaction_index", pa.int32()),
            ("from_address", pa.binary(20)),
            ("to_address", pa.binary(20)),
            ("value", pa.decimal128(38, 0)),
            ("input", pa.large_binary()),
            ("output", pa.large_binary()),
            ("trace_type", pa.string()),
            ("call_type", pa.string()),
            ("reward_type", pa.string()),
            ("gas", pa.int32()),
            ("gas_used", pa.decimal128(38, 0)),
            ("subtraces", pa.int32()),
            ("trace_address", pa.string()),
            ("error", pa.string()),
            ("status", pa.int16()),
            ("trace_id", pa.string()),
            ("trace_index", pa.int32()),
        ]
    ),
    "block": pa.schema(
        [
            ("partition", pa.int16()),
            ("block_id_group", pa.int32()),
            ("block_id", pa.int32()),
            ("block_hash", pa.binary(32)),
            ("parent_hash", pa.binary(32)),
            ("nonce", pa.binary(8)),
            ("sha3_uncles", pa.binary(32)),
            ("logs_bloom", pa.binary(256)),
            ("transactions_root", pa.binary(32)),
            ("state_root", pa.binary(32)),
            ("receipts_root", pa.binary(32)),
            ("miner", pa.binary(20)),
            ("difficulty", pa.decimal128(38, 0)),
            ("total_difficulty", pa.decimal128(38, 0)),
            ("size", pa.int64()),
            ("extra_data", pa.large_binary()),
            ("gas_limit", pa.int32()),
            ("gas_used", pa.int32()),
            ("base_fee_per_gas", pa.decimal128(38, 0)),
            ("timestamp", pa.int32()),
            ("transaction_count", pa.int32()),
        ]
    ),
    "transaction": pa.schema(
        [
            ("partition", pa.int16()),
            ("tx_hash_prefix", pa.string()),
            ("tx_hash", pa.binary(32)),
            ("nonce", pa.int32()),
            ("block_hash", pa.binary(32)),
            ("block_id_group", pa.int32()),
            ("block_id", pa.int32()),
            ("transaction_index", pa.int32()),
            ("from_address", pa.binary(20)),
            ("to_address", pa.binary(20)),
            ("value", pa.decimal128(38, 0)),
            ("gas", pa.int32()),
            ("gas_price", pa.decimal128(38, 0)),
            ("input", pa.large_binary()),
            ("block_timestamp", pa.int32()),
            ("max_fee_per_gas", pa.decimal128(38, 0)),
            ("max_priority_fee_per_gas", pa.decimal128(38, 0)),
            ("transaction_type", pa.decimal128(38, 0)),
            ("receipt_cumulative_gas_used", pa.decimal128(38, 0)),
            ("receipt_gas_used", pa.decimal128(38, 0)),
            ("receipt_contract_address", pa.binary(20)),
            ("receipt_root", pa.binary(32)),
            ("receipt_status", pa.decimal128(38, 0)),
            ("receipt_effective_gas_price", pa.decimal128(38, 0)),
        ]
    ),
}


class InMemoryItemExporter:
    """In-memory item exporter for EthStreamerAdapter export jobs."""

    def __init__(self, item_types: Iterable) -> None:
        self.item_types = item_types
        self.items: Dict[str, List] = {}

    def open(self) -> None:  # noqa
        """Open item exporter."""
        for item_type in self.item_types:
            self.items[item_type] = []

    def export_item(self, item) -> None:
        """Export single item."""
        item_type = item.get("type", None)
        if item_type is None:
            raise ValueError(f"type key is not found in item {item}")
        self.items[item_type].append(item)

    def close(self) -> None:
        """Close item exporter."""

    def get_items(self, item_type) -> Iterable:
        """Get items from exporter."""
        return self.items[item_type]


class EthStreamerAdapter:
    """Ethereum streaming adapter to export blocks, transactions,
    receipts, logs amd traces."""

    def __init__(
        self,
        batch_web3_provider: ThreadLocalProxy,
        batch_size: int = 100,
        max_workers: int = 5,
    ) -> None:
        self.batch_web3_provider = batch_web3_provider
        self.batch_size = batch_size
        self.max_workers = max_workers
        self.item_id_calculator = EthItemIdCalculator()
        self.item_timestamp_calculator = EthItemTimestampCalculator()

    def export_blocks_and_transactions(
        self,
        start_block: int,
        end_block: int,
        export_blocks: bool = True,
        export_transactions: bool = True,
    ) -> Tuple[Iterable, Iterable]:
        """Export blocks and transactions for specified block range."""

        blocks_and_transactions_item_exporter = InMemoryItemExporter(
            item_types=["block", "transaction"]
        )
        blocks_and_transactions_job = ExportBlocksJob(
            start_block=start_block,
            end_block=end_block,
            batch_size=self.batch_size,
            batch_web3_provider=self.batch_web3_provider,
            max_workers=self.max_workers,
            item_exporter=blocks_and_transactions_item_exporter,
            export_blocks=export_blocks,
            export_transactions=export_transactions,
        )

        blocks_and_transactions_job.run()
        blocks = blocks_and_transactions_item_exporter.get_items("block")
        transactions = blocks_and_transactions_item_exporter.get_items("transaction")
        return blocks, transactions

    def export_receipts_and_logs(
        self, transactions: Iterable
    ) -> Tuple[Iterable, Iterable]:
        """Export receipts and logs for specified transaction hashes."""

        exporter = InMemoryItemExporter(item_types=["receipt", "log"])
        job = ExportReceiptsJob(
            transaction_hashes_iterable=(
                transaction["hash"] for transaction in transactions
            ),
            batch_size=self.batch_size,
            batch_web3_provider=self.batch_web3_provider,
            max_workers=self.max_workers,
            item_exporter=exporter,
            export_receipts=True,
            export_logs=True,
        )

        job.run()
        receipts = exporter.get_items("receipt")
        logs = exporter.get_items("log")
        return receipts, logs

    def export_traces(
        self,
        start_block: int,
        end_block: int,
        include_genesis_traces: bool = True,
        include_daofork_traces: bool = False,
    ) -> Iterable[Dict]:
        """Export traces for specified block range."""

        exporter = InMemoryItemExporter(item_types=["trace"])
        job = ExportTracesJob(
            start_block=start_block,
            end_block=end_block,
            batch_size=self.batch_size,
            web3=ThreadLocalProxy(lambda: Web3(self.batch_web3_provider)),
            max_workers=self.max_workers,
            item_exporter=exporter,
            include_genesis_traces=include_genesis_traces,
            include_daofork_traces=include_daofork_traces,
        )
        job.run()
        traces = exporter.get_items("trace")
        return traces


def get_last_block_yesterday(batch_web3_provider: ThreadLocalProxy) -> int:
    """Return last block number of previous day from Ethereum client."""

    web3 = Web3(batch_web3_provider)
    eth_service = EthService(web3)

    date = datetime.utcnow().replace(
        hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc
    )
    print(
        f"Determining latest block before {date.isoformat()}: ",
        end="",
        flush=True,
    )
    prev_date = datetime.date(datetime.today()) - timedelta(days=1)
    _, end_block = eth_service.get_block_range_for_date(prev_date)
    print(f"{end_block:,}")
    return end_block


def get_last_synced_block(batch_web3_provider: ThreadLocalProxy) -> int:
    """Return last synchronized block number from Ethereum client."""

    return int(Web3(batch_web3_provider).eth.getBlock("latest").number)


def write_to_sinks(
    db: AnalyticsDb,
    sink_config: dict,
    table_name: str,
    parameters,
    concurrency: int = 100,
):
    for sink, config in sink_config.items():
        if sink == "cassandra":
            cassandra_ingest(db, table_name, parameters, concurrency=concurrency)
        elif sink == "parquet":
            path = config.get("output_directory", None)
            if path is None:
                raise Exception(
                    "No output_dir is set. "
                    "Please set raw_keyspace_file_sink_directory "
                    "in the keyspace config."
                )
            write_parquet(path, table_name, parameters)
        else:
            logger.warning(f"Encountered unknown sink type {sink}, ignoring.")


def cassandra_ingest(
    db: AnalyticsDb, table_name: str, parameters, concurrency: int = 100
) -> None:
    """Concurrent ingest into Apache Cassandra."""
    db.raw.ingest(
        table_name, parameters, concurrency=concurrency, auto_none_to_unset=True
    )


def write_parquet(path, table_name, parameters):
    if not parameters:
        return
    table = pa.Table.from_pylist(parameters, schema=PARQUET_SCHEMAS[table_name])

    if path.startswith("hdfs://"):
        o = urlparse(path)
        fs = HadoopFileSystem(o.hostname, o.port)

        pq.write_to_dataset(
            table,
            root_path=o.path / table_name,
            partition_cols=["partition"],
            existing_data_behavior="overwrite_or_ignore",
            filesystem=fs,
        )
    else:
        path = Path(path)
        if path.exists():
            pq.write_to_dataset(
                table,
                root_path=path / table_name,
                partition_cols=["partition"],
                existing_data_behavior="overwrite_or_ignore",
            )
        else:
            raise Exception(f"Parquet file path not found: {path}")


def ingest_configuration_cassandra(
    db: AnalyticsDb,
    block_bucket_size: int,
    tx_hash_prefix_len: int,
) -> None:
    """Store configuration details in Cassandra table."""
    cassandra_ingest(
        db,
        "configuration",
        [
            {
                "id": db.raw.keyspace_name(),
                "block_bucket_size": int(block_bucket_size),
                "tx_prefix_length": tx_hash_prefix_len,
            }
        ],
    )


def ingest_logs(
    items: Iterable,
    db: AnalyticsDb,
    sink_config: dict,
    block_bucket_size: int = 1_000,
) -> None:
    """Ingest blocks into Apache Cassandra."""

    blob_colums = [
        "block_hash",
        "address",
        "data",
        "topic0",
        "tx_hash",
    ]
    for item in items:
        # remove column
        item.pop("type")
        # rename/add columns
        item["tx_hash"] = item.pop("transaction_hash")
        item["block_id"] = item.pop("block_number")
        item["block_id_group"] = item["block_id"] // block_bucket_size

        # Used for partitioning in parquet files
        # ignored otherwise
        item["partition"] = item["block_id"] // PARQUET_PARTITION_SIZE

        tpcs = item["topics"]

        if tpcs is None:
            tpcs = []

        if "topic0" not in item:
            # bugfix do not use None for topic0 but 0x, None
            # gets converted to UNSET which is not allowed for
            # key columns in cassandra and can not be filtered
            item["topic0"] = tpcs[0] if len(tpcs) > 0 else "0x"

        item["topics"] = [hex_to_bytearray(t) for t in tpcs]

        if "transaction_hash" in item:
            item.pop("transaction_hash")

        for elem in blob_colums:
            item[elem] = hex_to_bytearray(item[elem])

    write_to_sinks(db, sink_config, "log", items)


def ingest_blocks(
    items: Iterable,
    db: AnalyticsDb,
    sink_config: dict,
    block_bucket_size: int = 1_000,
) -> None:
    """Ingest blocks into Apache Cassandra."""

    blob_colums = [
        "block_hash",
        "parent_hash",
        "nonce",
        "sha3_uncles",
        "logs_bloom",
        "transactions_root",
        "state_root",
        "receipts_root",
        "miner",
        "extra_data",
    ]
    for item in items:
        # remove column
        item.pop("type")
        # rename/add columns
        item["block_id"] = item.pop("number")
        item["block_id_group"] = item["block_id"] // block_bucket_size
        item["block_hash"] = item.pop("hash")

        # Used for partitioning in parquet files
        # ignored otherwise
        item["partition"] = item["block_id"] // PARQUET_PARTITION_SIZE

        # convert hex strings to byte arrays (blob in Cassandra)
        for elem in blob_colums:
            item[elem] = hex_to_bytearray(item[elem])

    write_to_sinks(db, sink_config, "block", items)


def ingest_transactions(
    items: Iterable,
    db: AnalyticsDb,
    sink_config: dict,
    tx_hash_prefix_len: int = 4,
    block_bucket_size: int = 1_000,
) -> None:
    """Ingest transactions into Apache Cassandra."""

    blob_colums = [
        "tx_hash",
        "from_address",
        "to_address",
        "input",
        "block_hash",
        "receipt_contract_address",
        "receipt_root",
    ]
    for item in items:
        # remove column
        item.pop("type")
        # rename/add columns
        item["tx_hash"] = item.pop("hash")
        hash_slice = slice(2, 2 + tx_hash_prefix_len)
        item["tx_hash_prefix"] = item["tx_hash"][hash_slice]
        item["block_id"] = item.pop("block_number")
        item["block_id_group"] = item["block_id"] // block_bucket_size

        # Used for partitioning in parquet files
        # ignored otherwise
        item["partition"] = item["block_id"] // PARQUET_PARTITION_SIZE

        # convert hex strings to byte arrays (blob in Cassandra)
        for elem in blob_colums:
            item[elem] = hex_to_bytearray(item[elem])

    write_to_sinks(db, sink_config, "transaction", items)


def ingest_traces(
    items: Iterable,
    db: AnalyticsDb,
    sink_config: dict,
    block_bucket_size: int = 1_000,
) -> None:
    """Ingest traces into Apache Cassandra."""

    blob_colums = ["tx_hash", "from_address", "to_address", "input", "output"]
    for item in items:
        # remove column
        item.pop("type")
        # rename/add columns
        item["tx_hash"] = item.pop("transaction_hash")
        item["block_id"] = item.pop("block_number")
        item["block_id_group"] = item["block_id"] // block_bucket_size

        # Used for partitioning in parquet files
        # ignored otherwise
        item["partition"] = item["block_id"] // PARQUET_PARTITION_SIZE

        item["trace_address"] = (
            ",".join(map(str, item["trace_address"]))
            if item["trace_address"] is not None
            else None
        )
        # convert hex strings to byte arrays (blob in Cassandra)
        for elem in blob_colums:
            item[elem] = hex_to_bytearray(item[elem])

    write_to_sinks(db, sink_config, "trace", items)


def print_block_info(
    last_synced_block: int, last_ingested_block: Optional[int]
) -> None:
    """Display information about number of synced/ingested blocks."""

    logger.warning(f"Last synced block: {last_synced_block:,}")
    if last_ingested_block is None:
        logger.warning("Last ingested block: None")
    else:
        logger.warning(f"Last ingested block: {last_ingested_block:,}")


def ingest(
    db: AnalyticsDb,
    provider_uri: str,
    sink_config: dict,
    user_start_block: Optional[int],
    user_end_block: Optional[int],
    batch_size: int,
    info: bool,
    previous_day: bool,
    provider_timeout: int,
):
    # make sure that only supported sinks are selected.
    if not all((x in ["cassandra", "parquet"]) for x in sink_config.keys()):
        raise Exception(
            "Unsupported sink selected, supported: cassandra,"
            f" parquet; got {list(sink_config.keys())}"
        )

    logger.info(f"Writing data to {list(sink_config.keys())}")

    thread_proxy = ThreadLocalProxy(
        lambda: get_provider_from_uri(
            provider_uri, timeout=provider_timeout, batch=True
        )
    )

    last_synced_block = get_last_synced_block(thread_proxy)
    last_ingested_block = db.raw.get_highest_block()
    print_block_info(last_synced_block, last_ingested_block)

    # if info then only print block info and exit
    if info:
        return

    adapter = EthStreamerAdapter(thread_proxy, batch_size=50)

    start_block = 0
    if user_start_block is None:
        if last_ingested_block is not None:
            start_block = last_ingested_block + 1
    else:
        start_block = user_start_block

    end_block = last_synced_block
    if user_end_block is not None:
        end_block = user_end_block

    if previous_day:
        end_block = get_last_block_yesterday(thread_proxy)

    if start_block > end_block:
        print("No blocks to ingest")
        return

    time1 = datetime.now()
    count = 0

    logger.info(
        f"[{time1}] Ingesting block range "
        f"{start_block:,}:{end_block:,} "
        f"into {list(sink_config.keys())} "
    )

    for block_id in range(start_block, end_block + 1, batch_size):
        current_end_block = min(end_block, block_id + batch_size - 1)

        logging.disable(logging.INFO)
        blocks, txs = adapter.export_blocks_and_transactions(
            block_id, current_end_block
        )
        receipts, logs = adapter.export_receipts_and_logs(txs)
        traces = adapter.export_traces(block_id, current_end_block, True, True)

        enriched_txs = enrich_transactions(txs, receipts)
        logging.disable(logging.NOTSET)

        # ingest into Cassandra
        ingest_logs(logs, db, sink_config, BLOCK_BUCKET_SIZE)
        ingest_traces(traces, db, sink_config, BLOCK_BUCKET_SIZE)
        ingest_transactions(
            enriched_txs, db, sink_config, TX_HASH_PREFIX_LEN, BLOCK_BUCKET_SIZE
        )
        ingest_blocks(blocks, db, sink_config, BLOCK_BUCKET_SIZE)

        count += batch_size

        if count % 1000 == 0:
            time2 = datetime.now()
            time_delta = (time2 - time1).total_seconds()
            logger.info(
                f"[{time2}] "
                f"Last processed block: {current_end_block:,} "
                f"({count/time_delta:.1f} blocks/s)"
            )
            time1 = time2
            count = 0

    logger.info(
        f"[{datetime.now()}] Processed block range " f"{start_block:,}:{end_block:,}"
    )

    # store configuration details
    if "cassandra" in sink_config.keys():
        ingest_configuration_cassandra(
            db, int(BLOCK_BUCKET_SIZE), int(TX_HASH_PREFIX_LEN)
        )
