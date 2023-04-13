import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

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

from ..db import AnalyticsDb
from ..utils.accountmodel import hex_to_bytearray

logger = logging.getLogger(__name__)

BLOCK_BUCKET_SIZE = 1_000
TX_HASH_PREFIX_LEN = 5


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


def build_cql_insert_stmt(columns: Sequence[str], table: str) -> str:
    """Create CQL insert statement for specified columns and table name."""

    return "INSERT INTO %s (%s) VALUES (%s);" % (
        table,
        ", ".join(columns),
        ("?," * len(columns))[:-1],
    )


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


def cassandra_ingest(
    db: AnalyticsDb, table_name: str, parameters, concurrency: int = 100
) -> None:
    """Concurrent ingest into Apache Cassandra."""

    db.raw.ingest(
        table_name, parameters, concurrency=concurrency, auto_none_to_unset=True
    )


def ingest_configuration(
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

    cassandra_ingest(db, "log", items)


def ingest_blocks(
    items: Iterable,
    db: AnalyticsDb,
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
        # convert hex strings to byte arrays (blob in Cassandra)
        for elem in blob_colums:
            item[elem] = hex_to_bytearray(item[elem])

    cassandra_ingest(db, "block", items)


def ingest_transactions(
    items: Iterable,
    db: AnalyticsDb,
    tx_hash_prefix_len: int = 4,
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
        # convert hex strings to byte arrays (blob in Cassandra)
        for elem in blob_colums:
            item[elem] = hex_to_bytearray(item[elem])

    cassandra_ingest(db, "transaction", items)


def ingest_traces(
    items: Iterable,
    db: AnalyticsDb,
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
        item["trace_address"] = (
            ",".join(map(str, item["trace_address"]))
            if item["trace_address"] is not None
            else None
        )
        # convert hex strings to byte arrays (blob in Cassandra)
        for elem in blob_colums:
            item[elem] = hex_to_bytearray(item[elem])

    cassandra_ingest(db, "trace", items)


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
    user_start_block: Optional[int],
    user_end_block: Optional[int],
    batch_size: int,
    info: bool,
    previous_day: bool,
    w3_timeout: int,
):
    thread_proxy = ThreadLocalProxy(
        lambda: get_provider_from_uri(provider_uri, timeout=w3_timeout, batch=True)
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
        f"into Cassandra node"
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
        ingest_logs(logs, db, BLOCK_BUCKET_SIZE)
        ingest_traces(traces, db, BLOCK_BUCKET_SIZE)
        ingest_transactions(enriched_txs, db, TX_HASH_PREFIX_LEN)
        ingest_blocks(blocks, db, BLOCK_BUCKET_SIZE)

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
    ingest_configuration(db, int(BLOCK_BUCKET_SIZE), int(TX_HASH_PREFIX_LEN))
