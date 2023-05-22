import logging
import time
from collections import OrderedDict
from collections.abc import Iterable
from datetime import datetime, timezone
from functools import lru_cache
from operator import itemgetter
from typing import Optional, Sequence

from bitcoinetl.enumeration.chain import Chain
from bitcoinetl.jobs.export_blocks_job import ExportBlocksJob
from bitcoinetl.rpc.bitcoin_rpc import BitcoinRpc
from bitcoinetl.service.btc_service import BtcService
from blockchainetl.jobs.exporters.in_memory_item_exporter import InMemoryItemExporter
from blockchainetl.thread_local_proxy import ThreadLocalProxy
from btcpy.structs.address import P2pkhAddress
from btcpy.structs.script import ScriptBuilder
from cashaddress.convert import to_legacy_address
from cassandra.cluster import Cluster, Session
from cassandra.concurrent import execute_concurrent_with_args
from cassandra.query import PreparedStatement, SimpleStatement

from ..db import AnalyticsDb
from ..utils.accountmodel import hex_to_bytearray

TX_HASH_PREFIX_LENGTH = 5
TX_BUCKET_SIZE = 25_000
BLOCK_BUCKET_SIZE = 100


logger = logging.getLogger(__name__)


class BtcStreamerAdapter:
    def __init__(self, bitcoin_rpc, chain=Chain.BITCOIN, batch_size=2, max_workers=5):
        self.bitcoin_rpc = bitcoin_rpc
        self.chain = chain
        self.btc_service = BtcService(bitcoin_rpc, chain)
        self.batch_size = batch_size
        self.max_workers = max_workers

    def open(self):  # noqa
        self.item_exporter.open()

    def get_current_block_number(self):
        return int(self.btc_service.get_latest_block().number)

    def export_blocks_and_transactions(self, start_block, end_block):
        blocks_and_transactions_item_exporter = InMemoryItemExporter(
            item_types=["block", "transaction"]
        )

        blocks_and_transactions_job = ExportBlocksJob(
            start_block=start_block,
            end_block=end_block,
            batch_size=self.batch_size,
            bitcoin_rpc=self.bitcoin_rpc,
            max_workers=self.max_workers,
            item_exporter=blocks_and_transactions_item_exporter,
            chain=self.chain,
            export_blocks=True,
            export_transactions=True,
        )
        blocks_and_transactions_job.run()

        blocks = blocks_and_transactions_item_exporter.get_items("block")
        transactions = blocks_and_transactions_item_exporter.get_items("transaction")

        return blocks, transactions

    def close(self):
        self.item_exporter.close()


class LRUCache(OrderedDict):
    def __init__(self, *args, **kwds):
        self.size_limit = kwds.pop("size_limit", None)
        OrderedDict.__init__(self, *args, **kwds)
        self._check_size_limit()

    def __setitem__(self, key, value):
        OrderedDict.__setitem__(self, key, value)
        self._check_size_limit()

    def _check_size_limit(self):
        if self.size_limit is not None:
            while len(self) > self.size_limit:
                self.popitem(last=False)


class OutputResolver(object):
    def __init__(self, session, keyspace, tx_bucket_size):
        self.session = session
        self.keyspace = keyspace
        self.tx_bucket_size = tx_bucket_size
        self.cache = LRUCache(size_limit=10_000_000)

    @lru_cache(maxsize=10_000_000)
    def get_output(self, tx_hash) -> {}:
        if tx_hash in self.cache.keys():
            return self.cache.get(tx_hash)
        return self._get_from_db(tx_hash)

    def _get_from_db(self, tx_hash):
        q = f"""SELECT tx_id from {self.keyspace}.transaction_by_tx_prefix
                WHERE tx_prefix = '{tx_hash[:5]}'
                AND tx_hash = 0x{tx_hash}"""
        tx_id = self.session.execute(q).one()[0]

        q = f"""SELECT outputs FROM {self.keyspace}.transaction
                WHERE tx_id_group = {tx_id // self.tx_bucket_size}
                AND tx_id = {tx_id}"""
        result = self.session.execute(q).one()[0]
        res = {}
        for i, item in enumerate(result):
            res[i] = {
                "addresses": item.address,
                "value": item.value,
                "type": item.address_type,
            }
        return res

    def add_output(self, tx_hash, output) -> None:
        r = {k: output[k] for k in ["type", "addresses", "value"]}
        self.cache.setdefault(tx_hash, {}).setdefault(output["index"], r)

    def get_cache_stats(self):
        return (
            f"functools.lru_cache: {self.get_output.cache_info()}, "
            f"LRUCache: {len(self.cache)} items"
        )


def get_last_block_yesterday(
    btc_adapter: BtcStreamerAdapter, last_synced_block: int
) -> int:
    until_date = datetime.utcnow().replace(
        hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc
    )
    until_timestamp = until_date.timestamp()

    print(
        f"Determining latest block before {until_date.isoformat()}: ",
        end="",
        flush=True,
    )

    block_id = last_synced_block
    block = btc_adapter.btc_service.get_block(block_id)
    while block.timestamp >= until_timestamp:
        block_id -= 1
        block = btc_adapter.btc_service.get_block(block_id)

    block_number = block.number
    print(f"{block_number:,}")
    return block_number


def get_last_ingested_block(session: Session) -> Optional[int]:
    """Return last ingested block ID from block_transactions table."""

    cql_str = "SELECT block_id_group FROM block_transactions PER PARTITION LIMIT 1"
    simple_stmt = SimpleStatement(cql_str, fetch_size=1000)
    groups = [res[0] for res in session.execute(simple_stmt)]

    if len(groups) == 0:
        return None

    max_group = max(groups)

    result = session.execute(
        f"""SELECT block_id
            FROM block_transactions
            WHERE block_id_group={max_group} PER PARTITION LIMIT 1"""
    )
    max_block = result.current_rows[0].block_id

    return max_block


def get_latest_tx_id_before_block(session: Session, block_id: int) -> int:
    last_block = block_id - 1
    result = session.execute(
        f"""SELECT block_bucket_size FROM configuration
            WHERE id='{session.keyspace}'"""
    )
    bucket_size = result.current_rows[0].block_bucket_size

    block_group = last_block // bucket_size

    result = session.execute(
        f"""SELECT txs FROM block_transactions
            WHERE block_id_group={block_group}
            AND block_id={last_block}"""
    )
    latest_tx_id = -1

    if not result.current_rows:
        return latest_tx_id

    for tx in result.current_rows[0].txs:
        if tx.tx_id > latest_tx_id:
            latest_tx_id = tx.tx_id

    return latest_tx_id


def cassandra_ingest(
    session: Session,
    prepared_stmt: PreparedStatement,
    parameters,
    concurrency: int = 100,
) -> None:
    """Concurrent ingest into Apache Cassandra."""

    while True:
        try:
            results = execute_concurrent_with_args(
                session=session,
                statement=prepared_stmt,
                parameters=parameters,
                concurrency=concurrency,
            )

            for i, (success, _) in enumerate(results):
                if not success:
                    while True:
                        try:
                            session.execute(prepared_stmt, parameters[i])
                        except Exception as exception:
                            print("Exception: ", exception)
                            continue
                        break
            break

        except Exception as exception:
            print(exception)
            time.sleep(1)
            continue


def build_cql_insert_stmt(columns: Sequence[str], table: str) -> str:
    """Create CQL insert statement for specified columns and table name."""

    return "INSERT INTO %s (%s) VALUES (%s);" % (
        table,
        ", ".join(columns),
        ("?," * len(columns))[:-1],
    )


def get_prepared_statement(
    session: Session, keyspace: str, table: str
) -> PreparedStatement:
    """Build prepared CQL INSERT statement for specified table."""

    cql_str = f"""SELECT column_name FROM system_schema.columns
                  WHERE keyspace_name = '{keyspace}'
                  AND table_name = '{table}';"""
    result_set = session.execute(cql_str)
    columns = [elem.column_name for elem in result_set.current_rows]
    cql_str = build_cql_insert_stmt(columns, table)
    prepared_stmt = session.prepare(cql_str)
    return prepared_stmt


def ingest_block_transactions(
    txs, session, prepared_stmt: PreparedStatement, block_bucket_size
) -> None:
    mapping = {}
    for tx in txs:
        mapping.setdefault(tx["block_id"], []).append(tx)

    items = []
    for block, block_txs in mapping.items():
        items.append(
            {
                "block_id_group": block // block_bucket_size,
                "block_id": block,
                "txs": [tx_stats(x) for x in block_txs],
            }
        )

    cassandra_ingest(session, prepared_stmt, items)


def ingest_blocks(
    items: Iterable, session: Session, prepared_stmt: PreparedStatement
) -> None:
    cassandra_ingest(session, prepared_stmt, items)


def prepare_blocks_inplace(blocks: Iterable, block_bucket_size) -> None:
    blob_columns = ["hash"]
    pop_columns = [
        "type",
        "size",
        "stripped_size",
        "weight",
        "version",
        "merkle_root",
        "nonce",
        "bits",
        "coinbase_param",
    ]
    for block in blocks:
        for i in pop_columns:
            block.pop(i)

        for elem in blob_columns:
            block[elem] = hex_to_bytearray(
                block[elem]
            )  # convert hex strings to byte arrays (blob in Cassandra)

        block["block_id_group"] = block["number"] // block_bucket_size

        block["block_id"] = block.pop("number")
        block["block_hash"] = block.pop("hash")
        block["no_transactions"] = block.pop("transaction_count")


_address_types = {  # based on BlockSci values (type 0 .. 10)
    "nonstandard": 1,
    "pubkey": 2,
    "p2pk": 2,
    "pubkeyhash": 3,
    "p2pkh": 3,
    "multisig_pubkey": 4,
    "scripthash": 5,
    "p2sh": 5,
    "multisig": 6,
    "null": 7,
    "nulldata": 7,
    "witness_v0_keyhash": 8,
    "p2wpkhv0": 8,
    "witness_v0_scripthash": 9,
    "p2wshv0": 9,
    "witness_unknown": 10,
    "witness_v1_taproot": 11,
}


def addresstype_to_int(addr_type):
    if isinstance(addr_type, int):  # address_type has already been resolved
        return addr_type

    if addr_type in _address_types:
        return _address_types.get(addr_type)

    print(f"unknown address type {addr_type}")
    raise SystemExit(0)


def address_as_string(x):
    if x["type"] in ["null", "nulldata", "nonstandard", "witness_unknown"]:
        return None
    return x["addresses"]


def tx_io_summary(x):
    return [address_as_string(x), x["value"], addresstype_to_int(x["type"])]


def tx_short_summary(tx_hash, t_id, prefix_length):
    return str(tx_hash)[:prefix_length], bytearray.fromhex(str(tx_hash)), t_id


def tx_stats(tx):
    return (
        tx["tx_id"],
        len(tx["inputs"]),
        len(tx["outputs"]),
        tx["total_input"],
        tx["total_output"],
    )


def parse_script(s: str):
    script = ScriptBuilder.identify(s)

    if script.type == "p2pk":
        try:
            return [str(P2pkhAddress(script.pubkey.hash(), mainnet=True))], script.type
        except Exception as e:
            raise ValueError(
                f"ScriptParseError: cannot parse pubkey from {s}"
                f" (of type {script.type}) --- {e}"
            )

    if script.type == "p2pkh":
        return [str(P2pkhAddress(script.pubkeyhash, mainnet=True))], script.type

    if script.type == "multisig":
        return [
            str(P2pkhAddress(k.hash(), mainnet=True)) for k in script.pubkeys
        ], script.type

    if script.type in ["p2sh", "p2wpkhv0", "p2wshv0"]:
        return [str(script.address(mainnet=True))], script.type

    if script.type == "nulldata":
        return None, script.type

    raise ValueError(
        f"ScriptParseError: not handling script type {script.type} at the moment."
    )


def enrich_txs(txs: Iterable, resolver: OutputResolver) -> None:
    for tx in txs:
        if not tx["is_coinbase"]:
            # process inputs
            for i in tx["inputs"]:
                if i["spent_transaction_hash"]:
                    ref, ind = (
                        i["spent_transaction_hash"],
                        i["spent_output_index"],
                    )
                    try:
                        resolved = resolver.get_output(ref).get(ind)
                        i["addresses"] = resolved["addresses"]
                        i["type"] = resolved["type"]
                        i["value"] = resolved["value"]
                    except ValueError as exception:
                        print(f"tx input cannot be resolved for {i['addresses']}")
                        print(exception)
        tx["input_value"] = sum(
            [i["value"] for i in tx["inputs"] if i["value"] is not None]
        )

        # process outputs
        for o in tx["outputs"]:
            if o["addresses"]:
                if o["addresses"][0] and o["addresses"][0].startswith("bitcoincash:"):
                    o["addresses"] = [to_legacy_address(a) for a in o["addresses"]]

                if o["addresses"][0] and o["addresses"][0].startswith("nonstandard"):
                    try:
                        address_list, scripttype = parse_script(o["script_hex"])
                        o["addresses"] = (
                            address_list if address_list else o["addresses"]
                        )
                        o["type"] = scripttype
                    except ValueError as exception:
                        print(
                            f"{exception}: cannot parse output script {o}"
                            f" from tx {tx.get('hash')}"
                        )

                resolver.add_output(tx["hash"], o)


def prepare_transactions_inplace(
    txs: Iterable,
    next_tx_id: int,
    tx_hash_prefix_len: int,
    tx_bucket_size: int,
) -> None:
    pop_columns = [
        "type",
        "size",
        "virtual_size",
        "version",
        "lock_time",
        "index",
        "fee",
    ]
    blob_columns = ["hash"]

    txs = sorted(
        txs, key=itemgetter("block_number", "index")
    )  # because bitcoin-etl does not guarantee a sort order

    for tx in txs:
        for i in pop_columns:
            tx.pop(i)

        tx["tx_prefix"] = tx["hash"][:tx_hash_prefix_len]

        for elem in blob_columns:
            tx[elem] = hex_to_bytearray(
                tx[elem]
            )  # convert hex strings to byte arrays (blob in Cassandra)

        tx["block_id"] = tx.pop("block_number")
        tx["coinbase"] = tx.pop("is_coinbase")
        tx["coinjoin"] = is_coinjoin(tx)

        tx["inputs"] = [tx_io_summary(x) for x in tx["inputs"]]
        tx["outputs"] = [tx_io_summary(x) for x in tx["outputs"]]
        tx["timestamp"] = tx.pop("block_timestamp")
        tx["total_input"] = tx.pop("input_value")
        tx["total_output"] = tx.pop("output_value")
        tx["tx_hash"] = tx.pop("hash")

        tx["tx_id_group"] = next_tx_id // tx_bucket_size
        tx["tx_id"] = next_tx_id
        next_tx_id += 1


def ingest_transactions(
    txs: Iterable, session: Session, prepared_stmt: PreparedStatement
) -> None:
    cassandra_ingest(session, prepared_stmt, txs)


def ingest_transaction_lookups(
    txs: Iterable, session: Session, prepared_stmt: PreparedStatement
) -> None:
    res = [{k: tx[k] for k in ("tx_prefix", "tx_hash", "tx_id")} for tx in txs]
    cassandra_ingest(session, prepared_stmt, res)


def is_coinjoin(tx) -> bool:
    # https://github.com/citp/BlockSci/blob/14ccc9358443b2eb5730bb2902c4b11ab7928abf/src/heuristics/tx_identification.cpp#L48
    if tx["input_count"] < 2 or tx["output_count"] < 3:
        return False

    # Each participant contributes a spend and a change output
    participant_count = int((tx["output_count"] + 1) / 2)
    if participant_count > tx["input_count"]:
        return False

    input_addresses = {
        str(x["addresses"]) for x in tx["inputs"] if "addresses" in x.keys()
    }

    if participant_count > len(input_addresses):
        return False

    # The most common output value should appear 'participantCount' times;
    # if multiple values are tied for 'most common', the lowest value is used
    output_values = {}
    for o in tx["outputs"]:
        output_values[o["value"]] = output_values.get(o["value"], 0) + 1

    highest_count = max(output_values.values())
    val, frequency = min(
        [(key, c) for key, c in output_values.items() if c == highest_count]
    )

    if frequency != participant_count:
        return False

    # Exclude transactions sending dust outputs (unlikely to be CoinJoin)
    if val in (546, 2730):
        return False

    return True


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
    keyspace = db._raw_keyspace

    cluster = Cluster(db.db().db_nodes)
    session = cluster.connect(keyspace)
    thread_proxy = ThreadLocalProxy(lambda: BitcoinRpc(provider_uri))
    btc_adapter = BtcStreamerAdapter(thread_proxy, batch_size=batch_size)

    resolver = OutputResolver(session, keyspace, TX_BUCKET_SIZE)

    last_synced_block = btc_adapter.get_current_block_number()
    last_ingested_block = db.raw.get_highest_block()
    assert last_ingested_block == get_last_ingested_block(session)
    print_block_info(last_synced_block, last_ingested_block)

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

    # if info then only print block info and exit
    if info:
        cluster.shutdown()
        return

    time1 = datetime.now()
    count = 0

    logger.info(
        f"[{time1}] Ingesting block range "
        f"{start_block:,}:{end_block:,} "
        f"into {list(sink_config.keys())} "
    )

    prep_stmt = {
        elem: get_prepared_statement(session, keyspace, elem)
        for elem in [
            "block",
            "transaction",
            "transaction_by_tx_prefix",
            "block_transactions",
        ]
    }

    cql_str = """INSERT INTO configuration
                  (id, block_bucket_size, tx_prefix_length, tx_bucket_size)
                  VALUES (%s, %s, %s, %s)"""
    session.execute(
        cql_str,
        (
            keyspace,
            int(BLOCK_BUCKET_SIZE),
            int(TX_HASH_PREFIX_LENGTH),
            TX_BUCKET_SIZE,
        ),
    )

    for block_id in range(start_block, end_block + 1, batch_size):
        current_end_block = min(end_block, block_id + batch_size - 1)

        blocks, txs = btc_adapter.export_blocks_and_transactions(
            block_id, current_end_block
        )

        # until bitcoin-etl progresses
        # with https://github.com/blockchain-etl/bitcoin-etl/issues/43
        enrich_txs(txs, resolver)

        latest_tx_id = get_latest_tx_id_before_block(session, block_id)
        prepare_transactions_inplace(
            txs, latest_tx_id + 1, TX_HASH_PREFIX_LENGTH, TX_BUCKET_SIZE
        )
        prepare_blocks_inplace(blocks, BLOCK_BUCKET_SIZE)

        ingest_blocks(blocks, session, prep_stmt["block"])
        ingest_transaction_lookups(txs, session, prep_stmt["transaction_by_tx_prefix"])
        ingest_transactions(txs, session, prep_stmt["transaction"])
        ingest_block_transactions(
            txs, session, prep_stmt["block_transactions"], BLOCK_BUCKET_SIZE
        )

        count += batch_size

        if count % 100 == 0:
            time2 = datetime.now()
            time_delta = (time2 - time1).total_seconds() / 60
            print(
                f"[{time2}] "
                f"Last processed block: {current_end_block:,} "
                f"({count/time_delta:.1f} blocks/m)"
            )
            time1 = time2
            count = 0

    print(f"[{datetime.now()}] Processed block range {start_block:,}:{end_block:,}")

    last_block = blocks[-1]
    last_tx = txs[-1]
    total_blocks = last_block["block_id"] + 1
    total_txs = last_tx["tx_id"] + 1
    timestamp = last_block["timestamp"]
    cql_str = """INSERT INTO summary_statistics
                 (id, timestamp, no_blocks, no_txs)
                 VALUES (%s, %s, %s, %s)"""
    session.execute(cql_str, (keyspace, timestamp, total_blocks, total_txs))

    cluster.shutdown()
