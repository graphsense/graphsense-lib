import concurrent.futures
import logging
import threading
from datetime import datetime, timedelta
from typing import Dict, Iterable, List, Optional, Tuple

import pandas as pd

from ..config import GRAPHSENSE_DEFAULT_DATETIME_FORMAT, get_reorg_backoff_blocks
from ..datatypes import BadUserInputError
from ..db import AnalyticsDb
from ..utils import (
    batch,
    check_timestamp,
    first_or_default,
    parse_timestamp,
    strip_0x,
)
from ..utils.account import get_id_group
from ..utils.logging import configure_logging, suppress_log_level
from ..utils.signals import graceful_ctlc_shutdown
from ..utils.tron import evm_to_bytes, strip_tron_prefix
from .common import (
    AbstractETLStrategy,
    AbstractTask,
    StoreTask,
    cassandra_ingest,
    write_to_sinks,
)
from .fast_rpc import (
    BatchRpcClient,
    FastBlockExporter,
    FastBlockReceiptExporter,
    FastReceiptExporter,
    enrich_transactions as _enrich_transactions,
    get_block_range_for_date,
)
from .fast_traces import FastTraceExporter

logger = logging.getLogger(__name__)

BLOCK_BUCKET_SIZE = 1_000
TX_HASH_PREFIX_LEN = 5

PARQUET_PARTITION_SIZE = 100_000


def _fast_hex_to_bytes(s):
    """Fast hex-to-bytes for ingest pipeline data.

    All values from JSON-RPC are 0x-prefixed hex strings or None.
    Skips the is_hex_string/strip_0x/remove_prefix chain in hex_to_bytes.
    """
    return bytes.fromhex(s[2:]) if s is not None else None


WEB3_QUERY_BATCH_SIZE = 50
WEB3_QUERY_WORKERS = 40


def enrich_txs_with_vrs(
    txs: Iterable[Dict], receipts: Iterable[Dict]
) -> Iterable[Dict]:
    # Our enrich_transactions preserves all fields including v, r, s,
    # unlike ethereumetl's version which stripped them.
    return _enrich_transactions(txs, receipts)


class AccountStreamerAdapter:
    """Standard Ethereum API style streaming adapter to export blocks, transactions,
    receipts, logs and traces."""

    def __init__(
        self,
        client: BatchRpcClient,
        batch_size: int = None,
        max_workers: int = None,
        batch_size_blockstransactions: int = None,
        max_workers_blockstransactions: int = None,
        batch_size_receiptslogs: int = None,
        max_workers_receiptslogs: int = None,
    ) -> None:
        self.client = client
        self.batch_size = batch_size
        self.max_workers = max_workers

        if batch_size_blockstransactions is None:
            batch_size_blockstransactions = batch_size
        if max_workers_blockstransactions is None:
            max_workers_blockstransactions = max_workers
        if batch_size_receiptslogs is None:
            batch_size_receiptslogs = batch_size
        if max_workers_receiptslogs is None:
            max_workers_receiptslogs = max_workers

        self.batch_size_blockstransactions = batch_size_blockstransactions
        self.max_workers_blockstransactions = max_workers_blockstransactions
        self.batch_size_receiptslogs = batch_size_receiptslogs
        self.max_workers_receiptslogs = max_workers_receiptslogs

        self._block_exporter = FastBlockExporter(
            client,
            batch_size=batch_size_blockstransactions or 50,
            max_workers=max_workers_blockstransactions or 20,
        )
        self._receipt_exporter = FastReceiptExporter(
            client,
            batch_size=batch_size_receiptslogs or 50,
            max_workers=max_workers_receiptslogs or 20,
        )
        self._block_receipt_exporter = FastBlockReceiptExporter(
            client,
            batch_size=batch_size_blockstransactions or 20,
            max_workers=max_workers_blockstransactions or 10,
        )
        self._trace_exporter = FastTraceExporter(
            client=client,
            trace_batch_size=batch_size or 10,
            max_workers=max_workers or 20,
        )

    def export_blocks_and_transactions(
        self,
        start_block: int,
        end_block: int,
        export_blocks: bool = True,
        export_transactions: bool = True,
    ) -> Tuple[Iterable, Iterable]:
        """Export blocks and transactions for specified block range."""
        return self._block_exporter.export_blocks_and_transactions(
            start_block, end_block
        )

    def export_block_headers(self, start_block: int, end_block: int) -> Iterable:
        """Export block headers (without transactions) for a block range.

        Uses detailed=false which is much faster than detailed=true.
        """
        return self._block_exporter.export_block_headers(start_block, end_block)

    def export_receipts_and_logs(
        self, transactions: Iterable
    ) -> Tuple[Iterable, Iterable]:
        """Export receipts and logs for specified transaction hashes."""
        tx_hashes = [transaction["hash"] for transaction in transactions]
        return self._receipt_exporter.export_receipts_and_logs(tx_hashes)

    def export_receipts_and_logs_by_block(
        self, start_block: int, end_block: int
    ) -> Tuple[Iterable, Iterable]:
        """Export receipts and logs for a block range using eth_getBlockReceipts.

        This is faster than per-transaction receipt fetching since it uses
        1 RPC call per block instead of 1 per transaction.
        """
        return self._block_receipt_exporter.export_receipts_and_logs(
            start_block, end_block
        )

    def export_traces(
        self,
        start_block: int,
        end_block: int,
        include_genesis_traces: bool = True,
        include_daofork_traces: bool = False,
    ) -> Iterable[Dict]:
        """Export traces for specified block range."""
        return self._trace_exporter.export_traces(start_block, end_block)


class EthStreamerAdapter(AccountStreamerAdapter):
    """Ethereum API style streaming adapter to export blocks, transactions,
    receipts, logs and traces."""


class TronStreamerAdapter(AccountStreamerAdapter):
    """Tron API style streaming adapter to export blocks, transactions,
    receipts, logs and traces.
    """

    def __init__(
        self,
        client: BatchRpcClient,
        grpc_endpoint: str,
        batch_size: int = None,
        max_workers: int = None,
        batch_size_blockstransactions: int = None,
        max_workers_blockstransactions: int = None,
        batch_size_receiptslogs: int = None,
        max_workers_receiptslogs: int = None,
    ) -> None:
        super().__init__(
            client,
            batch_size,
            max_workers,
            batch_size_blockstransactions,
            max_workers_blockstransactions,
            batch_size_receiptslogs,
            max_workers_receiptslogs,
        )
        self.grpc_endpoint = grpc_endpoint

    def export_traces(
        self,
        start_block: int,
        end_block: int,
        include_genesis_traces: bool = True,
        include_daofork_traces: bool = False,
    ) -> Iterable[Dict]:
        """Export traces for specified block range."""
        from .tron.export_traces_job import TronExportTracesJob

        job = TronExportTracesJob(
            start_block=start_block,
            end_block=end_block,
            batch_size=self.batch_size,
            grpc_endpoint=self.grpc_endpoint,
            max_workers=self.max_workers,
        )

        return job.run()

    def export_traces_parallel(
        self, start_block: int, end_block: int
    ) -> Iterable[Dict]:
        """Export traces for specified block range."""
        from .tron.export_traces_job import TronExportTracesJob

        job = TronExportTracesJob(
            start_block=start_block,
            end_block=end_block,
            batch_size=self.batch_size,
            grpc_endpoint=self.grpc_endpoint,
            max_workers=self.max_workers,
        )

        return job.run_parallel()

    def export_hash_to_type_mappings(
        self, transactions: Iterable, blocks: Iterable, block_id_name="block_id"
    ) -> Dict:
        from .tron.grpc.api.tron_api_pb2 import NumberMessage
        from .tron.grpc.api.tron_api_pb2_grpc import WalletStub
        from graphsenselib.utils.grpc import get_channel

        grpc_endpoint = self.grpc_endpoint
        channel = get_channel(grpc_endpoint)
        wallet_stub = WalletStub(channel)

        def get_type(tx):
            type_container = tx.raw_data.contract
            assert len(type_container) == 1
            return type_container[0].type

        def get_block(i):  # contains type
            msg = NumberMessage(num=i)
            info = wallet_stub.GetBlockByNum(msg)
            return info

        block_ids = [b[block_id_name] for b in blocks]
        blocks_data = [get_block(b) for b in block_ids]
        txs_grpc = [tx for block in blocks_data for tx in block.transactions]
        types = [get_type(tx) for tx in txs_grpc]
        tx_hashes = [tx["hash"] for tx in transactions]
        hash_to_type = dict(zip(tx_hashes, types))

        return hash_to_type

    def export_hash_to_type_mappings_parallel(
        self, blocks: Iterable, block_id_name="block_id"
    ) -> Dict:
        from .tron.grpc.api.tron_api_pb2 import NumberMessage
        from .tron.grpc.api.tron_api_pb2_grpc import WalletStub
        from graphsenselib.utils.grpc import get_channel

        grpc_endpoint = self.grpc_endpoint
        channel = get_channel(grpc_endpoint)
        wallet_stub = WalletStub(channel)

        def get_type(tx):
            type_container = tx.transaction.raw_data.contract
            assert len(type_container) == 1
            return type_container[0].type

        def get_tx_hash(tx):
            tx_hash = tx.txid
            return "0x" + tx_hash.hex()

        def get_block(i):  # contains type
            msg = NumberMessage(num=i)
            info = wallet_stub.GetBlockByNum2(msg)
            return info

        block_ids = [b[block_id_name] for b in blocks]

        # Using ThreadPoolExecutor to fetch blocks concurrently
        with concurrent.futures.ThreadPoolExecutor() as executor:
            blocks_data = list(executor.map(get_block, block_ids))

        txs_grpc = [tx for block in blocks_data for tx in block.transactions]
        types = [get_type(tx) for tx in txs_grpc]
        tx_hashes = [get_tx_hash(tx) for tx in txs_grpc]
        hash_to_type = dict(zip(tx_hashes, types))

        return hash_to_type

    def get_trc10_token_infos(self) -> Iterable[Dict]:
        """Get all trc10 tokens from a grpc endpoint"""
        from .tron.grpc.api.tron_api_pb2 import EmptyMessage
        from .tron.grpc.api.tron_api_pb2_grpc import WalletStub
        from graphsenselib.utils.grpc import get_channel

        grpc_endpoint = self.grpc_endpoint

        channel = get_channel(grpc_endpoint)
        wallet_stub = WalletStub(channel)
        trc10_tokens = wallet_stub.GetAssetIssueList(EmptyMessage())

        list_of_dicts = []
        for token in trc10_tokens.assets:
            token_dict = {}
            for field in token.DESCRIPTOR.fields:
                value = getattr(token, field.name)
                token_dict[field.name] = value

            list_of_dicts.append(token_dict)

        return list_of_dicts


def get_last_block_yesterday(client: BatchRpcClient) -> int:
    """Return last block number of previous day from Ethereum client."""

    prev_date = datetime.date(datetime.today()) - timedelta(days=1)
    _, end_block = get_block_range_for_date(client, prev_date)
    logger.info(
        f"Determining latest block before {prev_date.isoformat()} its {end_block:,}",
    )
    return end_block


def get_last_synced_block(client: BatchRpcClient) -> int:
    """Return last synchronized block number from Ethereum client."""

    return client.get_latest_block_number()


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


def prepare_logs_inplace(
    items: Iterable,
    block_bucket_size: int,
    partition_size: int = PARQUET_PARTITION_SIZE,
) -> None:
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
        item["block_id_group"] = get_id_group(item["block_id"], block_bucket_size)

        # Used for partitioning in parquet files
        # ignored otherwise
        item["partition"] = item["block_id"] // partition_size

        tpcs = item["topics"]

        if tpcs is None:
            tpcs = []

        if "topic0" not in item:
            # bugfix do not use None for topic0 but 0x, None
            # gets converted to UNSET which is not allowed for
            # key columns in cassandra and can not be filtered
            item["topic0"] = tpcs[0] if len(tpcs) > 0 else "0x"

        item["topics"] = [_fast_hex_to_bytes(t) for t in tpcs]

        if "transaction_hash" in item:
            item.pop("transaction_hash")

        for elem in blob_colums:
            item[elem] = _fast_hex_to_bytes(item[elem])


def ingest_logs(
    items: Iterable,
    db: AnalyticsDb,
    sink_config: dict,
    block_bucket_size: int,
) -> None:
    """Ingest blocks into Apache Cassandra."""

    write_to_sinks(db, sink_config, "log", items)


def prepare_blocks_inplace_eth(
    items: Iterable,
    block_bucket_size: int,
    partition_size: int = PARQUET_PARTITION_SIZE,
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
        "parent_beacon_block_root",
    ]
    for item in items:
        # remove column
        item.pop("type")
        # rename/add columns
        item["block_id"] = item.pop("number")
        item["block_id_group"] = get_id_group(item["block_id"], block_bucket_size)
        item["block_hash"] = item.pop("hash")

        # Used for partitioning in parquet files
        # ignored otherwise
        item["partition"] = item["block_id"] // partition_size

        # convert hex strings to byte arrays (blob in Cassandra)
        for elem in blob_colums:
            item[elem] = _fast_hex_to_bytes(item[elem])

        ws = item["withdrawals"]
        for w in ws:
            w["amount"] = single_int_to_bytes(w["amount"])
        item["withdrawals"] = ws

        item["uncles"] = [_fast_hex_to_bytes(u) for u in item.get("uncles", [])]


def prepare_blocks_inplace_trx(
    items, block_bucket_size, partition_size=PARQUET_PARTITION_SIZE
):
    prepare_blocks_inplace_eth(items, block_bucket_size, partition_size)

    for b in items:
        # b["timestamp"] = b["timestamp"]
        check_timestamp(b["timestamp"])


def prepare_transactions_inplace_eth(
    items: Iterable,
    tx_hash_prefix_len: int,
    block_bucket_size: int,
    partition_size: int = PARQUET_PARTITION_SIZE,
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
        item["block_id_group"] = get_id_group(item["block_id"], block_bucket_size)

        # Used for partitioning in parquet files
        # ignored otherwise
        item["partition"] = item["block_id"] // partition_size

        # convert hex strings to byte arrays (blob in Cassandra)
        for elem in blob_colums:
            item[elem] = _fast_hex_to_bytes(item[elem])

        item["blob_versioned_hashes"] = [
            _fast_hex_to_bytes(t) for t in item["blob_versioned_hashes"]
        ]  # todo probably not needed for tron?


def prepare_transactions_inplace_trx(
    items: Iterable,
    tx_hash_prefix_len: int,
    block_bucket_size: int,
    partition_size: int = PARQUET_PARTITION_SIZE,
):
    from .tron.txTypeTransformer import TxTypeTransformer

    prepare_transactions_inplace_eth(
        items, tx_hash_prefix_len, block_bucket_size, partition_size
    )

    type_transformer = TxTypeTransformer()
    for tx in items:
        type_transformer.transform(tx)
        check_timestamp(tx["block_timestamp"])


def prepare_traces_inplace_eth(
    items: Iterable,
    block_bucket_size: int,
    partition_size: int = PARQUET_PARTITION_SIZE,
):
    blob_colums = ["tx_hash", "from_address", "to_address", "input", "output"]
    for item in items:
        # remove column
        item.pop("type")
        # rename/add columns
        item["tx_hash"] = item.pop("transaction_hash")
        item["block_id"] = item.pop("block_number")
        item["block_id_group"] = get_id_group(item["block_id"], block_bucket_size)

        # Used for partitioning in parquet files
        # ignored otherwise
        item["partition"] = item["block_id"] // partition_size

        item["trace_address"] = (
            ",".join(map(str, item["trace_address"]))
            if item["trace_address"] is not None
            else None
        )
        # convert hex strings to byte arrays (blob in Cassandra)
        for elem in blob_colums:
            item[elem] = _fast_hex_to_bytes(item[elem])


def prepare_traces_inplace_trx(
    items: Iterable,
    block_bucket_size: int,
    partition_size: int = PARQUET_PARTITION_SIZE,
):
    blob_colums = ["tx_hash", "caller_address", "transferto_address"]
    for item in items:
        # rename/add columns
        item["tx_hash"] = item.pop("transaction_hash")
        item["block_id"] = item.pop("block_number")
        item["block_id_group"] = get_id_group(item["block_id"], block_bucket_size)
        item["transferto_address"] = item.pop("transferTo_address")

        # Used for partitioning in parquet files
        # ignored otherwise
        item["partition"] = item["block_id"] // partition_size

        # item["trace_address"] = (
        #    ",".join(map(str, item["trace_address"]))
        #    if item["trace_address"] is not None
        #    else None
        # )
        # convert hex strings to byte arrays (blob in Cassandra)
        for elem in blob_colums:
            item[elem] = evm_to_bytes(item[elem])


def prepare_fees_inplace(
    fees: Iterable,
    tx_hash_prefix_len: int,
    partition=None,
    keep_block_ids=False,
    drop_tx_hash_prefix=False,
) -> None:
    blob_colums = ["tx_hash"]
    for item in fees:
        if not drop_tx_hash_prefix:
            prefix = strip_0x(item["tx_hash"])[:tx_hash_prefix_len]
            item["tx_hash_prefix"] = prefix

        if partition is not None:
            item["partition"] = partition

        for elem in blob_colums:
            item[elem] = evm_to_bytes(item[elem])

    if not keep_block_ids:
        for item in fees:
            item.pop("block_id")


def prepare_trc10_tokens_inplace(items: Iterable):
    def decode_bytes(bytes_):
        if bytes_ is None:
            return None
        # \xa0 is not a valid utf-8 character
        bytes_ = bytes_.replace(b"\xa0", b" ")
        try:
            return bytes_.decode()
        except Exception:
            return bytes_.decode(encoding="Latin1")

    fields_to_convert = ["name", "abbr", "description", "url"]
    for item in items:
        for field in fields_to_convert:
            item[field] = decode_bytes(item.get(field, None))

    times = ["start_time", "end_time"]
    for item in items:
        for field in times:
            if field in item:
                item[field] = item[field] // 1000
                # there are some known anomalous timestamps in the data.
                # We dont want to send too many warnings therefore omit check.
                # check_timestamp(item[field])

    # cast id to int
    for item in items:
        item["id"] = int(item["id"])

    # cast frozen_supply to list of tuples
    for item in items:
        if len(item["frozen_supply"]) > 0:
            item["frozen_supply"] = [
                (x.frozen_amount, x.frozen_days) for x in item["frozen_supply"]
            ]

    for item in items:
        # already in bytes, so strip tron prefix should do the trick
        item["owner_address"] = strip_tron_prefix(item["owner_address"])


def print_block_info(
    last_synced_block: int, last_ingested_block: Optional[int]
) -> None:
    """Display information about number of synced/ingested blocks."""

    logger.info(f"Last synced block: {last_synced_block:,}")
    if last_ingested_block is None:
        logger.info("Last ingested block: None")
    else:
        logger.info(f"Last ingested block: {last_ingested_block:,}")


def get_connection_from_url(provider_uri: str, provider_timeout=600):
    return BatchRpcClient(provider_uri, timeout=provider_timeout)


def ingest(
    db: AnalyticsDb,
    currency: str,
    sources: List[str],
    sink_config: dict,
    user_start_block: Optional[int],
    user_end_block: Optional[int],
    batch_size: int,
    info: bool,
    previous_day: bool,
    provider_timeout: int,
    mode: str,
):
    logger.info("Writing data sequentially")
    # make sure that only supported sinks are selected.
    if not all((x in ["cassandra", "parquet"]) for x in sink_config.keys()):
        raise BadUserInputError(
            "Unsupported sink selected, supported: cassandra,"
            f" parquet; got {list(sink_config.keys())}"
        )

    logger.info(f"Writing data to {list(sink_config.keys())}")

    http_provider_uri = first_or_default(sources, lambda x: x.startswith("http"))

    if http_provider_uri is None:
        raise BadUserInputError("No http provider (node url) is configured.")

    client = get_connection_from_url(http_provider_uri, provider_timeout)
    last_synced_block = get_last_synced_block(client)
    last_ingested_block = db.raw.get_highest_block()
    print_block_info(last_synced_block, last_ingested_block)

    if currency == "trx":
        if user_start_block == 0:
            user_start_block = 1
            logger.warning(
                "Start was set to 1 since genesis blocks "
                "don't have logs and cause issues."
            )

        grpc_provider_uri = first_or_default(sources, lambda x: x.startswith("grpc"))
        if grpc_provider_uri is None:
            raise BadUserInputError("No grpc provider (node url) is configured.")

        adapter = TronStreamerAdapter(
            client,
            grpc_endpoint=grpc_provider_uri,
            batch_size=WEB3_QUERY_BATCH_SIZE,
            max_workers=WEB3_QUERY_WORKERS,
        )

        # ingest trc10 table
        token_infos = adapter.get_trc10_token_infos()
        prepare_trc10_tokens_inplace(token_infos)
        write_to_sinks(db, sink_config, "trc10", token_infos)

        prepare_transactions_inplace = prepare_transactions_inplace_trx
        prepare_blocks_inplace = prepare_blocks_inplace_trx
        prepare_traces_inplace = prepare_traces_inplace_trx

    elif currency == "eth":
        adapter = EthStreamerAdapter(
            client,
            batch_size=WEB3_QUERY_BATCH_SIZE,
            max_workers=WEB3_QUERY_WORKERS,
        )
        prepare_transactions_inplace = prepare_transactions_inplace_eth
        prepare_blocks_inplace = prepare_blocks_inplace_eth
        prepare_traces_inplace = prepare_traces_inplace_eth
    else:
        raise NotImplementedError(f"Currency {currency} not implemented")

    start_block = 0
    if user_start_block is None:
        if last_ingested_block is not None:
            start_block = last_ingested_block + 1
    else:
        start_block = user_start_block

    end_block = last_synced_block - get_reorg_backoff_blocks(currency)
    if user_end_block is not None:
        end_block = user_end_block

    if previous_day:
        end_block = get_last_block_yesterday(client)

    if start_block > end_block:
        logger.warning("No blocks to ingest")
        return

    time1 = datetime.now()
    count = 0

    # if info then only print block info and exit
    if info:
        logger.info(
            f"Would ingest block range "
            f"{start_block:,} - {end_block:,} ({end_block - start_block:,} blks) "
            f"into {list(sink_config.keys())} "
        )

        return

    logger.info(
        f"Ingesting block range "
        f"{start_block:,} - {end_block:,} ({end_block - start_block:,} blks) "
        f"into {list(sink_config.keys())} "
    )

    with graceful_ctlc_shutdown() as check_shutdown_initialized:
        for block_id in range(start_block, end_block + 1, batch_size):
            current_end_block = min(end_block, block_id + batch_size - 1)

            with suppress_log_level(logging.INFO):
                blocks, txs = adapter.export_blocks_and_transactions(
                    block_id, current_end_block
                )
                receipts, logs = adapter.export_receipts_and_logs_by_block(
                    block_id, current_end_block
                )
                traces, fees = adapter.export_traces(
                    block_id, current_end_block, True, True
                )
                enriched_txs = enrich_txs_with_vrs(txs, receipts)

            # reformat and edit data
            prepare_logs_inplace(logs, BLOCK_BUCKET_SIZE)
            prepare_traces_inplace(traces, BLOCK_BUCKET_SIZE)
            prepare_transactions_inplace(
                enriched_txs, TX_HASH_PREFIX_LEN, BLOCK_BUCKET_SIZE
            )
            prepare_blocks_inplace(blocks, BLOCK_BUCKET_SIZE)
            if fees is not None:
                prepare_fees_inplace(fees, TX_HASH_PREFIX_LEN)
                write_to_sinks(db, sink_config, "fee", fees)

            # ingest into Cassandra
            write_to_sinks(db, sink_config, "log", logs)
            write_to_sinks(db, sink_config, "trace", traces)
            write_to_sinks(db, sink_config, "transaction", enriched_txs)
            write_to_sinks(db, sink_config, "block", blocks)

            count += batch_size

            last_block = blocks[-1]
            last_block_ts = last_block["timestamp"]

            last_block_date = parse_timestamp(last_block_ts)
            time2 = datetime.now()
            time_delta = (time2 - time1).total_seconds()
            logger.info(
                f"Last processed block: {current_end_block:,} "
                f"[{last_block_date.strftime(GRAPHSENSE_DEFAULT_DATETIME_FORMAT)}] "
                f"({count / time_delta:.1f} blks/s)"
            )
            time1 = time2
            count = 0

            if check_shutdown_initialized():
                break

    last_block_date = parse_timestamp(last_block_ts)
    logger.info(
        f"Processed block range "
        f"{start_block:,} - {end_block:,} "
        f" ({last_block_date.strftime(GRAPHSENSE_DEFAULT_DATETIME_FORMAT)})"
    )

    # store configuration details
    if "cassandra" in sink_config.keys():
        ingest_configuration_cassandra(
            db, int(BLOCK_BUCKET_SIZE), int(TX_HASH_PREFIX_LEN)
        )


class LoadLogsTask(AbstractTask):
    def run(self, ctx, data):
        txs = data
        receipts, logs = ctx.adapter.export_receipts_and_logs(txs)
        enriched_txs = ctx.strategy.enrich_transactions(txs, receipts)
        logst = ctx.strategy.transform_logs(logs, ctx.BLOCK_BUCKET_SIZE)
        txst = ctx.strategy.transform_transactions(
            enriched_txs, ctx.TX_HASH_PREFIX_LEN, ctx.BLOCK_BUCKET_SIZE
        )
        return [(StoreTask(), ("transaction", txst)), (StoreTask(), ("log", logst))]


class LoadLogsAndTypeTask(AbstractTask):
    def __init__(
        self, is_update_transactions_mode: bool = False, blocks: Iterable = None
    ):
        self.is_update_transactions_mode = is_update_transactions_mode
        self.blocks = blocks

    def run(self, ctx, data):
        txs = data
        receipts, logs = ctx.adapter.export_receipts_and_logs(txs)
        hash_to_type = ctx.adapter.export_hash_to_type_mappings(txs, self.blocks)
        enriched_txs = ctx.strategy.enrich_transactions(txs, receipts)
        enriched_txs = enrich_transactions_with_type(enriched_txs, hash_to_type)
        txst = ctx.strategy.transform_transactions(
            enriched_txs, ctx.TX_HASH_PREFIX_LEN, ctx.BLOCK_BUCKET_SIZE
        )
        if self.is_update_transactions_mode:
            return [(StoreTask(), ("transaction", txst))]

        logst = ctx.strategy.transform_logs(logs, ctx.BLOCK_BUCKET_SIZE)
        return [(StoreTask(), ("transaction", txst)), (StoreTask(), ("log", logst))]


class LoadBlockTask(AbstractTask):
    def run(self, ctx, data):
        start, end = data
        blocks, txs = ctx.adapter.export_blocks_and_transactions(start, end)
        blockst = ctx.strategy.transform_blocks(blocks, ctx.BLOCK_BUCKET_SIZE)
        return [
            (StoreTask(), ("block", blockst)),
            (LoadLogsTask(), txs),
        ]


class LoadBlockTaskTrx(AbstractTask):
    def __init__(self, is_update_transactions_mode: bool = False):
        self.is_update_transactions_mode = is_update_transactions_mode

    def run(self, ctx, data):
        start, end = data
        blocks, txs = ctx.adapter.export_blocks_and_transactions(start, end)
        blockst = ctx.strategy.transform_blocks(blocks, ctx.BLOCK_BUCKET_SIZE)
        return [
            (StoreTask(), ("block", blockst)),
            (LoadLogsAndTypeTask(self.is_update_transactions_mode, blocks), txs),
        ]


class LoadTracesTask(AbstractTask):
    def __init__(self, fees_only_mode: bool = False):
        self.fees_only_mode = fees_only_mode

    def run(self, ctx, data):
        start, end = data
        traces, fees = ctx.adapter.export_traces(start, end, True, True)
        tracest = ctx.strategy.transform_traces(traces, ctx.BLOCK_BUCKET_SIZE)
        fees = ctx.strategy.transform_fees(fees, ctx.TX_HASH_PREFIX_LEN)

        tasks = []

        if not self.fees_only_mode:
            tasks.append((StoreTask(), ("trace", tracest)))

        if fees is not None:
            tasks.append((StoreTask(), ("fee", fees)))

        return tasks


class LoadTrc10TokenInfoTask(AbstractTask):
    def run(self, ctx, data):
        token_infos = ctx.adapter.get_trc10_token_infos()
        token_infost = ctx.strategy.transform_trc10_token_infos(token_infos)
        return [(StoreTask(), ("trc10", token_infost))]


class EthETLStrategy(AbstractETLStrategy):
    def __init__(
        self,
        http_provider_uri: str,
        provider_timeout: int,
        is_trace_only_mode: bool,
        fees_only_mode: bool = False,
    ):
        self.http_provider_uri = http_provider_uri
        self.provider_timeout = provider_timeout
        self.is_trace_only_mode = is_trace_only_mode
        self.fees_only_mode = fees_only_mode

    def per_blockrange_tasks(self):
        if self.is_trace_only_mode:
            return [LoadTracesTask(self.fees_only_mode)]
        else:
            return [LoadBlockTask(), LoadTracesTask()]

    def transform_logs(self, logs, block_bucket_size: int):
        prepare_logs_inplace(logs, block_bucket_size)
        return logs

    def transform_fees(self, fees, tx_hash_prefix_len: int):
        if fees is not None:
            prepare_fees_inplace(fees, tx_hash_prefix_len)
        return fees

    def transform_transactions(
        self, enriched_txs, tx_hash_prefix_len: int, block_bucket_size: int
    ):
        prepare_transactions_inplace_eth(
            enriched_txs, tx_hash_prefix_len, block_bucket_size
        )
        return enriched_txs

    def enrich_transactions(self, txs, receipts):
        return enrich_txs_with_vrs(txs, receipts)

    def transform_traces(self, traces, block_bucket_size: int):
        prepare_traces_inplace_eth(traces, block_bucket_size)
        return traces

    def transform_blocks(self, blocks, block_bucket_size: int):
        prepare_blocks_inplace_eth(blocks, block_bucket_size)
        return blocks

    def get_source_adapter(self):
        return EthStreamerAdapter(
            get_connection_from_url(self.http_provider_uri, self.provider_timeout),
            batch_size=WEB3_QUERY_BATCH_SIZE,
            max_workers=WEB3_QUERY_WORKERS,
        )


class TrxETLStrategy(EthETLStrategy):
    def __init__(
        self,
        http_provider_uri: str,
        provider_timeout: int,
        grpc_provider_uri: str,
        is_trace_only_mode: bool,
        is_update_transactions_mode: bool,
        batch_size: int = WEB3_QUERY_BATCH_SIZE,
        fees_only_mode: bool = False,
    ):
        super().__init__(
            http_provider_uri, provider_timeout, is_trace_only_mode, fees_only_mode
        )
        self.batch_size = batch_size
        self.grpc_provider_uri = grpc_provider_uri
        self.is_update_transactions_mode = is_update_transactions_mode

    def pre_processing_tasks(self):
        return [LoadTrc10TokenInfoTask()]

    def transform_transactions(
        self, enriched_txs, tx_hash_prefix_len: int, block_bucket_size: int
    ):
        prepare_transactions_inplace_trx(
            enriched_txs, tx_hash_prefix_len, block_bucket_size
        )
        return enriched_txs

    def enrich_transactions_with_type(self, enriched_txs, hash_to_type):
        for tx in enriched_txs:
            tx["transaction_type"] = hash_to_type[tx["hash"]]

        return enriched_txs

    def transform_blocks(self, blocks, block_bucket_size: int):
        prepare_blocks_inplace_trx(blocks, block_bucket_size)
        return blocks

    def transform_traces(self, traces, block_bucket_size: int):
        prepare_traces_inplace_trx(traces, block_bucket_size)
        return traces

    def transform_trc10_token_infos(self, token_infos):
        prepare_trc10_tokens_inplace(token_infos)
        return token_infos

    def get_source_adapter(self):
        return TronStreamerAdapter(
            get_connection_from_url(self.http_provider_uri, self.provider_timeout),
            grpc_endpoint=self.grpc_provider_uri,
            batch_size=self.batch_size,
            max_workers=self.batch_size,
        )

    def per_blockrange_tasks(self):
        if self.is_trace_only_mode:
            return [LoadTracesTask(self.fees_only_mode)]
        elif self.is_update_transactions_mode:
            return [LoadBlockTaskTrx(self.is_update_transactions_mode)]
        else:
            return [LoadBlockTaskTrx(), LoadTracesTask()]


def ingest_async(
    db: AnalyticsDb,
    currency: str,
    sources: List[str],
    sink_config: dict,
    user_start_block: Optional[int],
    user_end_block: Optional[int],
    batch_size_user: int,
    info: bool,
    previous_day: bool,
    provider_timeout: int,
    mode: str,
):
    logger.info("Writing data in parallel")

    interleave_batches = 2
    batch_size = (
        (batch_size_user // interleave_batches)
        if batch_size_user >= interleave_batches
        else batch_size_user
    )

    # make sure that only supported sinks are selected.
    if not all((x in ["cassandra", "parquet"]) for x in sink_config.keys()):
        raise BadUserInputError(
            "Unsupported sink selected, supported: cassandra,"
            f" parquet; got {list(sink_config.keys())}"
        )

    fees_only_mode = mode == "account_fees_only"
    is_trace_only_mode = mode == "account_traces_only" or fees_only_mode
    is_update_transactions_mode = mode == "trx_update_transactions"

    http_provider_uri = first_or_default(sources, lambda x: x.startswith("http"))
    if http_provider_uri is None:
        raise BadUserInputError("No http provider (node url) is configured.")

    client = get_connection_from_url(http_provider_uri, provider_timeout)
    last_synced_block = get_last_synced_block(client)
    last_ingested_block = db.raw.get_highest_block()
    print_block_info(last_synced_block, last_ingested_block)

    if currency == "trx":
        if user_start_block == 0:
            user_start_block = 1
            logger.warning(
                "Start was set to 1 since genesis blocks "
                "don't have logs and cause issues."
            )

        grpc_provider_uri = first_or_default(sources, lambda x: x.startswith("grpc"))
        if grpc_provider_uri is None:
            raise BadUserInputError("No grpc provider (node url) is configured.")

        transform_strategy = TrxETLStrategy(
            http_provider_uri,
            provider_timeout,
            grpc_provider_uri,
            is_trace_only_mode,
            is_update_transactions_mode,
            batch_size=batch_size,
            fees_only_mode=fees_only_mode,
        )

    elif currency == "eth":
        transform_strategy = EthETLStrategy(
            http_provider_uri, provider_timeout, is_trace_only_mode
        )

    else:
        raise NotImplementedError(f"Currency {currency} not implemented")

    logger.info(f"Writing data to {list(sink_config.keys())}")

    start_block = 0
    if user_start_block is None:
        if last_ingested_block is not None:
            # to be safe if some error occured in the last import we reimport the las
            # two batches.
            start_block = max(last_ingested_block - (batch_size_user * 2) + 1, 0)
    else:
        start_block = user_start_block

    end_block = last_synced_block - get_reorg_backoff_blocks(currency)
    if user_end_block is not None:
        end_block = user_end_block

    if previous_day:
        end_block = get_last_block_yesterday(client)

    if start_block > end_block:
        logger.warning("No blocks to ingest")
        return

    # if info then only print block info and exit
    if info:
        logger.info(
            f"Would ingest block range "
            f"{start_block:,} - {end_block:,} ({end_block - start_block + 1:,} blks) "
            f"into {list(sink_config.keys())} "
        )

        return

    logger.info(
        f"Ingesting block range "
        f"{start_block:,} - {end_block:,} ({end_block - start_block + 1:,} blks) "
        f"into {list(sink_config.keys())} "
    )

    thread_context = threading.local()

    def initializer_worker(thrd_ctx, db, sink_config, strategy, loglevel):
        configure_logging(loglevel)
        new_db_conn = db.clone()
        new_db_conn.open()
        thrd_ctx.db = new_db_conn

        thrd_ctx.adapter = strategy.get_source_adapter()
        thrd_ctx.strategy = strategy
        thrd_ctx.sink_config = sink_config
        thrd_ctx.TX_HASH_PREFIX_LEN = TX_HASH_PREFIX_LEN
        thrd_ctx.BLOCK_BUCKET_SIZE = BLOCK_BUCKET_SIZE

    def process_task(thrd_ctx, task, data):
        return task.run(thrd_ctx, data)

    def submit_tasks(ex, thrd_ctx, tasks, data=None):
        return [
            ex.submit(
                process_task,
                thrd_ctx,
                cmd,
                data,
            )
            for cmd in tasks
        ]

    with graceful_ctlc_shutdown() as check_shutdown_initialized:
        with concurrent.futures.ThreadPoolExecutor(
            initializer=initializer_worker,
            initargs=(
                thread_context,
                db,
                sink_config,
                transform_strategy,
                logger.getEffectiveLevel(),
            ),
            max_workers=4,  # we write at most 4 tables in parallel
        ) as ex:
            time1 = datetime.now()
            count = 0

            # Add preprocessing tasks
            tasks = submit_tasks(
                ex, thread_context, transform_strategy.pre_processing_tasks()
            )

            for block_ids in batch(
                range(start_block, end_block + 1, batch_size), n=interleave_batches
            ):
                # Execute tasks
                ranges = [(s, min(end_block, s + batch_size - 1)) for s in block_ids]
                current_end_block = ranges[-1][1]
                block_id = ranges[-1][0]

                blockrange_tasks = transform_strategy.per_blockrange_tasks()

                # Submit tasks per block range
                for s, e in ranges:
                    tasks += submit_tasks(
                        ex, thread_context, blockrange_tasks, data=(s, e)
                    )

                blocks = None
                while len(tasks) > 0:
                    completed = next(concurrent.futures.as_completed(tasks))
                    tasks.remove(completed)
                    for cont_task, data in completed.result():
                        # Get the data for the progress indicator
                        if isinstance(cont_task, StoreTask) and data[0] == "block":
                            blocks = data[1]

                        tasks += submit_tasks(
                            ex, thread_context, [cont_task], data=data
                        )

                # Update UI
                last_block_date_str = "Unknown"
                if not is_trace_only_mode:
                    last_block = blocks[-1]
                    last_block_ts = last_block["timestamp"]
                    last_blk_date = parse_timestamp(last_block_ts)
                    last_block_date_str = last_blk_date.strftime(
                        GRAPHSENSE_DEFAULT_DATETIME_FORMAT
                    )

                count += (current_end_block - block_id + 1) * interleave_batches

                # if count % 1000 == 0:
                time2 = datetime.now()
                time_delta = (time2 - time1).total_seconds()
                # logging.disable(logging.NOTSET)
                logger.info(
                    f"Last processed block: {current_end_block:,} "
                    f"[{last_block_date_str}] "  # noqa
                    f"({count / time_delta:.1f} blks/s)"
                )
                # logging.disable(logging.INFO)
                time1 = time2
                count = 0

                if check_shutdown_initialized():
                    break

    # logging.disable(logging.NOTSET)
    logger.info(
        f"Processed block range "
        f"{start_block:,} - {end_block:,} "
        f" ({last_block_date_str})"
    )

    # store configuration details
    if "cassandra" in sink_config.keys():
        ingest_configuration_cassandra(
            db, int(BLOCK_BUCKET_SIZE), int(TX_HASH_PREFIX_LEN)
        )


def single_int_to_bytes(integer: int) -> bytes:
    bytes_needed = (integer.bit_length() + 7) // 8
    return integer.to_bytes(bytes_needed, byteorder="big")


def to_bytes(data, cols):
    for d in data:
        for col in cols:
            if d[col] is not None:
                d[col] = single_int_to_bytes(d[col])
    return data


def from_bytes(data, cols):
    for d in data:
        for col in cols:
            if d[col] is not None:
                d[col] = int.from_bytes(d[col], byteorder="big")
    return data


def from_bytes_df(df, cols):
    for col in cols:
        df[col] = df[col].apply(
            lambda x: int.from_bytes(x, byteorder="big") if not pd.isnull(x) else x
        )
    return df


def enrich_transactions_with_type(enriched_txs, hash_to_type):
    for tx in enriched_txs:
        tx["transaction_type"] = hash_to_type[tx["hash"]]

    return enriched_txs
