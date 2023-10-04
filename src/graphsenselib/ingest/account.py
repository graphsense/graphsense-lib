import logging
import pathlib
import re
import sys
from csv import QUOTE_NONE
from datetime import datetime, timedelta
from typing import Dict, Iterable, List, Optional, Tuple

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
from web3 import Web3

from ..config import GRAPHSENSE_DEFAULT_DATETIME_FORMAT, get_approx_reorg_backoff_blocks
from ..db import AnalyticsDb
from ..utils import hex_to_bytearray, parse_timestamp
from ..utils.logging import suppress_log_level
from ..utils.signals import graceful_ctlc_shutdown
from .common import cassandra_ingest, write_to_sinks
from .csv import (
    BLOCK_HEADER,
    LOGS_HEADER,
    TRACE_HEADER,
    TX_HEADER,
    format_blocks_csv,
    format_logs_csv,
    format_traces_csv,
    format_transactions_csv,
    write_csv,
)

logger = logging.getLogger(__name__)

BLOCK_BUCKET_SIZE = 1_000
TX_HASH_PREFIX_LEN = 5

PARQUET_PARTITION_SIZE = 100_000


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
    receipts, logs and traces."""

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

    prev_date = datetime.date(datetime.today()) - timedelta(days=1)
    _, end_block = eth_service.get_block_range_for_date(prev_date)
    logger.info(
        f"Determining latest block before {prev_date.isoformat()} its {end_block:,}",
    )
    return end_block


def get_last_synced_block(batch_web3_provider: ThreadLocalProxy) -> int:
    """Return last synchronized block number from Ethereum client."""

    return int(Web3(batch_web3_provider).eth.getBlock("latest").number)


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


def prepare_logs_inplace(items: Iterable, block_bucket_size: int = 1_000):
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


def ingest_logs(
    items: Iterable,
    db: AnalyticsDb,
    sink_config: dict,
    block_bucket_size: int = 1_000,
) -> None:
    """Ingest blocks into Apache Cassandra."""

    write_to_sinks(db, sink_config, "log", items)


def prepare_blocks_inplace(
    items: Iterable,
    block_bucket_size: int = 1_000,
):
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


def prepare_blocks_inplace_trx(items: Iterable):
    for b in items:
        b["timestamp"] = b["timestamp"]


def prepare_transactions_inplace(
    items: Iterable, tx_hash_prefix_len: int = 4, block_bucket_size: int = 1_000
):
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


def prepare_transactions_inplace_trx(items: Iterable):
    for tx in items:
        tx["block_timestamp"] = tx["block_timestamp"] // 1000


def prepare_traces_inplace(items: Iterable, block_bucket_size: int = 1_000):
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


def print_block_info(
    last_synced_block: int, last_ingested_block: Optional[int]
) -> None:
    """Display information about number of synced/ingested blocks."""

    logger.warning(f"Last synced block: {last_synced_block:,}")
    if last_ingested_block is None:
        logger.warning("Last ingested block: None")
    else:
        logger.warning(f"Last ingested block: {last_ingested_block:,}")


def get_connection_from_url(provider_uri: str, provider_timeout=600):
    return ThreadLocalProxy(
        lambda: get_provider_from_uri(
            provider_uri, timeout=provider_timeout, batch=True
        )
    )


def ingest(
    db: AnalyticsDb,
    currency: str,
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

    thread_proxy = get_connection_from_url(provider_uri, provider_timeout)

    last_synced_block = get_last_synced_block(thread_proxy)
    last_ingested_block = db.raw.get_highest_block()
    print_block_info(last_synced_block, last_ingested_block)

    adapter = EthStreamerAdapter(thread_proxy, batch_size=50)

    start_block = 0
    if user_start_block is None:
        if last_ingested_block is not None:
            start_block = last_ingested_block + 1
    else:
        start_block = user_start_block

    end_block = last_synced_block - get_approx_reorg_backoff_blocks(currency)
    if user_end_block is not None:
        end_block = user_end_block

    if previous_day:
        end_block = get_last_block_yesterday(thread_proxy)

    if start_block > end_block:
        print("No blocks to ingest")
        return

    time1 = datetime.now()
    count = 0

    # if info then only print block info and exit
    if info:
        logger.info(
            f"Would ingest block range "
            f"{start_block:,} - {end_block:,} ({end_block-start_block:,} blks) "
            f"into {list(sink_config.keys())} "
        )

        return

    logger.info(
        f"Ingesting block range "
        f"{start_block:,} - {end_block:,} ({end_block-start_block:,} blks) "
        f"into {list(sink_config.keys())} "
    )

    with graceful_ctlc_shutdown() as check_shutdown_initialized:
        for block_id in range(start_block, end_block + 1, batch_size):
            current_end_block = min(end_block, block_id + batch_size - 1)

            with suppress_log_level(logging.INFO):
                blocks, txs = adapter.export_blocks_and_transactions(
                    block_id, current_end_block
                )
                receipts, logs = adapter.export_receipts_and_logs(txs)
                traces = (
                    []
                    if currency == "trx"
                    else adapter.export_traces(block_id, current_end_block, True, True)
                )

                enriched_txs = enrich_transactions(txs, receipts)

            # reformat and edit data
            prepare_logs_inplace(logs, BLOCK_BUCKET_SIZE)
            prepare_traces_inplace(traces, BLOCK_BUCKET_SIZE)
            prepare_transactions_inplace(
                enriched_txs, TX_HASH_PREFIX_LEN, BLOCK_BUCKET_SIZE
            )
            prepare_blocks_inplace(blocks, BLOCK_BUCKET_SIZE)

            if currency == "trx":
                prepare_transactions_inplace_trx(enriched_txs)
                prepare_blocks_inplace_trx(blocks)

            # ingest into Cassandra
            write_to_sinks(db, sink_config, "log", logs)
            write_to_sinks(db, sink_config, "trace", traces)
            write_to_sinks(db, sink_config, "transaction", enriched_txs)
            write_to_sinks(db, sink_config, "block", blocks)

            count += batch_size

            last_block = blocks[-1]
            last_block_ts = last_block["timestamp"]

            if count % 1000 == 0:
                last_block_date = parse_timestamp(last_block_ts)
                time2 = datetime.now()
                time_delta = (time2 - time1).total_seconds()
                logger.info(
                    f"Last processed block: {current_end_block:,} "
                    f"[{last_block_date.strftime(GRAPHSENSE_DEFAULT_DATETIME_FORMAT)}] "
                    f"({count/time_delta:.1f} blks/s)"
                )
                time1 = time2
                count = 0

            if check_shutdown_initialized():
                break

    last_block_date = parse_timestamp(last_block_ts)
    logger.info(
        f"[{datetime.now()}] Processed block range "
        f"{start_block:,} - {end_block:,} "
        f" ({last_block_date.strftime(GRAPHSENSE_DEFAULT_DATETIME_FORMAT)})"
    )

    # store configuration details
    if "cassandra" in sink_config.keys():
        ingest_configuration_cassandra(
            db, int(BLOCK_BUCKET_SIZE), int(TX_HASH_PREFIX_LEN)
        )


def export_csv(
    db: AnalyticsDb,
    currency: str,
    provider_uri: str,
    directory: str,
    user_start_block: Optional[int],
    user_end_block: Optional[int],
    continue_export: bool,
    batch_size: int,
    file_batch_size: int,
    partition_batch_size: int,
    info: bool,
    previous_day: bool,
    provider_timeout: int,
):
    logger.info(f"Writing data as csv to {directory}")

    thread_proxy = get_connection_from_url(provider_uri, provider_timeout)

    last_synced_block = get_last_synced_block(thread_proxy)
    last_ingested_block = db.raw.get_highest_block()
    print_block_info(last_synced_block, last_ingested_block)

    adapter = EthStreamerAdapter(thread_proxy, batch_size=50)

    start_block = 0
    if user_start_block is None:
        if continue_export:
            block_files = sorted(pathlib.Path(directory).rglob("block*"))
            if block_files:
                last_file = block_files[-1].name
                logger.info(f"Last exported file: {block_files[-1]}")
                start_block = int(re.match(r".*-(\d+)", last_file).group(1)) + 1
    else:
        start_block = user_start_block

    end_block = get_last_synced_block(thread_proxy)
    logger.info(f"Last synced block: {end_block:,}")
    if user_end_block is not None:
        end_block = user_end_block
    if previous_day:
        end_block = get_last_block_yesterday(thread_proxy)

    time1 = datetime.now()
    count = 0

    block_bucket_size = file_batch_size
    if file_batch_size % batch_size != 0:
        logger.error("Error: file_batch_size is not a multiple of batch_size")
        sys.exit(1)

    if partition_batch_size % file_batch_size != 0:
        logger.error("Error: partition_batch_size is not a multiple of file_batch_size")
        sys.exit(1)

    rounded_start_block = start_block // block_bucket_size * block_bucket_size
    rounded_end_block = (end_block + 1) // block_bucket_size * block_bucket_size - 1

    if rounded_start_block > rounded_end_block:
        print("No blocks to export")
        return

    block_range = (
        rounded_start_block,
        rounded_start_block + block_bucket_size - 1,
    )

    if info:
        logger.info(
            f"Would process block range "
            f"{rounded_start_block:,} - {rounded_end_block:,}"
        )
        return

    path = pathlib.Path(directory)
    try:
        path.mkdir(parents=True, exist_ok=True)
    except (PermissionError, NotADirectoryError) as exception:
        logger.error(f"Could not output directory {directory}: {str(exception)}")
        sys.exit(1)

    block_file = "block_%08d-%08d.csv.gz" % block_range
    tx_file = "tx_%08d-%08d.csv.gz" % block_range
    trace_file = "trace_%08d-%08d.csv.gz" % block_range
    logs_file = "logs_%08d-%08d.csv.gz" % block_range

    logger.info(
        f"Processing block range " f"{rounded_start_block:,} - {rounded_end_block:,}"
    )

    block_list = []
    tx_list = []
    trace_list = []
    logs_list = []

    with graceful_ctlc_shutdown() as check_shutdown_initialized:
        for block_id in range(rounded_start_block, rounded_end_block + 1, batch_size):
            current_end_block = min(end_block, block_id + batch_size - 1)

            with suppress_log_level(logging.INFO):
                blocks, txs = adapter.export_blocks_and_transactions(
                    block_id, current_end_block
                )
                if block_id == 0 and currency == "trx":
                    # genesis tx of block 0 have no receipts
                    # to avoid errors we drop them
                    txs = [tx for tx in txs if tx["block_number"] > 0]
                receipts, logs = adapter.export_receipts_and_logs(txs)
                traces = (
                    []
                    if currency == "trx"
                    else adapter.export_traces(block_id, current_end_block, True, True)
                )
                enriched_txs = enrich_transactions(txs, receipts)

            block_list.extend(format_blocks_csv(blocks))
            tx_list.extend(format_transactions_csv(enriched_txs, TX_HASH_PREFIX_LEN))
            trace_list.extend(format_traces_csv(traces))
            logs_list.extend(format_logs_csv(logs))

            count += batch_size

            last_block = block_list[-1]
            last_block_ts = last_block["timestamp"]
            last_block_date = parse_timestamp(last_block_ts)

            if count >= 1000:
                time2 = datetime.now()
                time_delta = (time2 - time1).total_seconds()
                logger.info(
                    f"Last processed block {current_end_block:,} "
                    f"[{last_block_date.strftime(GRAPHSENSE_DEFAULT_DATETIME_FORMAT)}] "
                    f"({count/time_delta:.1f} blks/s)"
                )
                time1 = time2
                count = 0

            if (block_id + batch_size) % block_bucket_size == 0:
                partition_start = block_id - (block_id % partition_batch_size)
                partition_end = partition_start + partition_batch_size - 1
                sub_dir = f"{partition_start:08d}-{partition_end:08d}"
                full_path = path / sub_dir
                full_path.mkdir(parents=True, exist_ok=True)

                if currency == "trx":
                    prepare_transactions_inplace_trx(tx_list)
                    prepare_blocks_inplace_trx(block_list)

                if currency != "trx":
                    # trx has no tracing api
                    write_csv(full_path / trace_file, trace_list, TRACE_HEADER)

                write_csv(full_path / tx_file, tx_list, TX_HEADER)
                write_csv(full_path / block_file, block_list, BLOCK_HEADER)
                write_csv(
                    full_path / logs_file,
                    logs_list,
                    LOGS_HEADER,
                    delimiter="|",
                    quoting=QUOTE_NONE,
                )

                last_block = block_list[-1]
                last_block_ts = last_block["timestamp"]
                last_block_date = parse_timestamp(last_block_ts)

                logger.info(
                    f"Written blocks: {block_range[0]:,} - {block_range[1]:,} "
                    f"[{last_block_date.strftime(GRAPHSENSE_DEFAULT_DATETIME_FORMAT)}] "
                )

                block_range = (
                    block_id + batch_size,
                    block_id + batch_size + block_bucket_size - 1,
                )
                block_file = "block_%08d-%08d.csv.gz" % block_range
                tx_file = "tx_%08d-%08d.csv.gz" % block_range
                trace_file = "trace_%08d-%08d.csv.gz" % block_range
                logs_file = "logs_%08d-%08d.csv.gz" % block_range

                block_list.clear()
                tx_list.clear()
                trace_list.clear()
                logs_list.clear()

                if check_shutdown_initialized():
                    break

    logger.info(
        f"[{datetime.now()}] Processed block range "
        f"{rounded_start_block:,}:{rounded_end_block:,}"
        f" ({last_block_date.strftime(GRAPHSENSE_DEFAULT_DATETIME_FORMAT)})"
    )
