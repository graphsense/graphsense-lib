import logging
from abc import ABC, abstractmethod
from collections import OrderedDict
from collections.abc import Iterable
from datetime import datetime, timezone
from json import JSONDecodeError
from operator import itemgetter
from typing import Dict, List, Optional, Tuple

try:
    from bitcoinetl.enumeration.chain import Chain
    from bitcoinetl.jobs.export_blocks_job import ExportBlocksJob
    from bitcoinetl.rpc.bitcoin_rpc import BitcoinRpc
    from bitcoinetl.service.btc_service import BtcService
    from blockchainetl.jobs.exporters.in_memory_item_exporter import (
        InMemoryItemExporter,
    )
    from blockchainetl.thread_local_proxy import ThreadLocalProxy
    from btcpy.structs.address import P2pkhAddress
    from btcpy.structs.script import ScriptBuilder

    CHAIN_MAPPING = {
        "btc": Chain.BITCOIN,
        "ltc": Chain.LITECOIN,
        # Use the new api for btc (in btc etl jargon), this is
        # required for bitcoin cash, otherwise the index field in the transactions
        # is not filled correctly.
        "bch": Chain.BITCOIN,
        "zec": Chain.ZCASH,
    }


except ImportError:
    _has_ingest_dependencies = False
    CHAIN_MAPPING = {}
else:
    _has_ingest_dependencies = True

from methodtools import lru_cache as mlru_cache
from requests.exceptions import ConnectionError as RequestsConnectionError
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_fixed

from ..config import GRAPHSENSE_DEFAULT_DATETIME_FORMAT, get_reorg_backoff_blocks
from ..db import AnalyticsDb
from ..utils import bytes_to_hex, flatten, hex_to_bytes, parse_timestamp, strip_0x
from ..utils.account import get_id_group
from ..utils.address import address_to_bytes
from ..utils.bch import bch_address_to_legacy
from ..utils.logging import suppress_log_level
from ..utils.signals import graceful_ctlc_shutdown
from .common import cassandra_ingest, write_to_sinks

TX_HASH_PREFIX_LENGTH = 5
TX_BUCKET_SIZE = 25_000
BLOCK_BUCKET_SIZE = 100

OUTPUTS_CACHE_ITEMS = 2**24  # approx 16 mio.

logger = logging.getLogger(__name__)


POP_COLUMNS_TX = []

FIELDNAME_MAP_TX = {
    "block_number": "block_id",
    "hash": "tx_hash",
    "is_coinbase": "coinbase",
    "block_timestamp": "timestamp",
    "input_value": "total_input",
    "output_value": "total_output",
}

BLOB_COLUMNS_TX = ["tx_hash"]

POP_COLUMNS_BLOCK = [
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

FIELDNAME_MAP_BLOCK = {
    "number": "block_id",
    "hash": "block_hash",
    "transaction_count": "no_transactions",
}

BLOB_COLUMNS_BLOCK = ["block_hash"]


class UnknownScriptType(Exception):
    pass


class UnknownAddressType(Exception):
    pass


class P2pkParserException(Exception):
    pass


class InputNotFoundException(Exception):
    pass


def drop_columns_from_list(data, cols):
    for elem in data:
        drop_columns(elem, cols)
    return data


def drop_columns(elem, cols):
    for col in cols:
        elem.pop(col, None)
    return elem


class BtcStreamerAdapter:
    def __init__(self, bitcoin_rpc, chain=None, batch_size=2, max_workers=5):
        """Summary

        Args:
            bitcoin_rpc (TYPE): Description
            chain (TYPE, optional): Description
            batch_size (int, optional): Description
            max_workers (int, optional): Description
        """
        if not _has_ingest_dependencies:
            raise ImportError(
                "The ingest.utxo needs bitcoinetl installed. Please install gslib with ingest dependencies."
            )

        if chain is None:
            chain = Chain.BITCOIN

        self.bitcoin_rpc = bitcoin_rpc
        self.chain = chain
        self.btc_service = BtcService(bitcoin_rpc, chain)
        self.batch_size = batch_size
        self.max_workers = max_workers

    def open(self):  # noqa
        self.item_exporter.open()

    def get_current_block_number(self):
        return int(self.btc_service.get_latest_block().number)

    @retry(
        retry=retry_if_exception_type(RequestsConnectionError)
        | retry_if_exception_type(JSONDecodeError),
        stop=stop_after_attempt(10),
        wait=wait_fixed(20),
    )
    def export_transactions(self, start_block, end_block):
        transactions_item_exporter = InMemoryItemExporter(item_types=["transaction"])

        transactions_job = ExportBlocksJob(
            start_block=start_block,
            end_block=end_block,
            batch_size=self.batch_size,
            bitcoin_rpc=self.bitcoin_rpc,
            max_workers=self.max_workers,
            item_exporter=transactions_item_exporter,
            chain=self.chain,
            export_blocks=False,
            export_transactions=True,
        )
        transactions_job.run()

        transactions = transactions_item_exporter.get_items("transaction")

        return transactions

    @retry(
        retry=retry_if_exception_type(RequestsConnectionError)
        | retry_if_exception_type(JSONDecodeError),
        stop=stop_after_attempt(10),
        wait=wait_fixed(20),
    )
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


class OutputResolverBase(ABC):
    @abstractmethod
    def get_output(self, tx_hash) -> Dict:
        pass

    @abstractmethod
    def add_output(self, tx_hash, output) -> None:
        pass

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


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


class CassandraOutputResolver(OutputResolverBase):
    """Output resolver that uses the gs-cassandra database to resolve
    spent inputs.

    Attributes:
        cache (TYPE): Description
        db (TYPE): Database connection
        tx_bucket_size (TYPE): Description
        tx_prefix_length (TYPE): Description
    """

    def __init__(
        self,
        db: AnalyticsDb,
        tx_bucket_size: int = None,
        tx_prefix_length: int = None,
    ):
        self.db = db
        self.tx_bucket_size = tx_bucket_size
        self.tx_prefix_length = tx_prefix_length
        self.cache = LRUCache(size_limit=OUTPUTS_CACHE_ITEMS)

    def __enter__(self):
        self.db.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.db.__exit__(exc_type, exc_val, exc_tb)

    @mlru_cache(maxsize=OUTPUTS_CACHE_ITEMS)
    def get_output(self, tx_hash) -> Dict:
        if tx_hash in self.cache.keys():
            return self.cache.get(tx_hash)
        return self._get_from_db(tx_hash)

    def _get_from_db(self, tx_hash):
        outputs = self.db.raw.get_tx_outputs(
            tx_hash,
            tx_bucket_size=self.tx_bucket_size,
            tx_prefix_length=self.tx_prefix_length,
        )
        return outputs

    def add_output(self, tx_hash, output) -> None:
        r = {k: output[k] for k in ["type", "addresses", "value"]}
        self.cache.setdefault(tx_hash, {}).setdefault(output["index"], r)

    def get_cache_stats(self) -> str:
        """Gets a string summary of the state of the cache

        Returns:
            str: String summary of cache
        """
        return (
            f"Frontend and DB cache: {self.get_output.cache_info()}, "
            f"Run Local Cache: {len(self.cache)} items"
        )


def get_last_block_yesterday(
    btc_adapter: BtcStreamerAdapter, last_synced_block: int
) -> int:
    until_date = datetime.utcnow().replace(
        hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc
    )
    until_timestamp = until_date.timestamp()

    block_id = last_synced_block
    block = btc_adapter.btc_service.get_block(block_id)
    while block.timestamp >= until_timestamp:
        block_id -= 1
        block = btc_adapter.btc_service.get_block(block_id)

    block_number = block.number
    timestamp = parse_timestamp(block.timestamp)
    logger.info(
        f"Determining latest block before {until_date.isoformat()} "
        f"its {block_number:,} at {timestamp.isoformat()}",
    )
    return block_number


def preprocess_block_transactions(items: Iterable, block_bucket_size: int) -> List:
    mapping = {}
    for tx in items:
        mapping.setdefault(tx["block_id"], []).append(tx)

    items = []
    for block, block_txs in mapping.items():
        items.append(
            {
                "block_id_group": get_id_group(block, block_bucket_size),
                "block_id": block,
                "txs": [tx_stats(x) for x in block_txs],
            }
        )
    return items


def ingest_block_transactions(
    items: Iterable,
    db: AnalyticsDb,
    sink_config: dict,
    block_bucket_size: int,
) -> None:
    items = preprocess_block_transactions(items, block_bucket_size)
    write_to_sinks(db, sink_config, "block_transactions", items)


def ingest_blocks(
    items: Iterable,
    db: AnalyticsDb,
    sink_config: dict,
) -> None:
    write_to_sinks(db, sink_config, "block", items)


def ingest_tx_refs(
    items: Iterable,
    db: AnalyticsDb,
    sink_config: dict,
) -> None:
    write_to_sinks(db, sink_config, "transaction_spent_in", items)
    write_to_sinks(db, sink_config, "transaction_spending", items)


def prepare_blocks_inplace(
    blocks: Iterable,
    block_bucket_size: int,
    drop_fields=True,
    rename_fields=True,
    process_fields=True,
    cast_blob_fields=True,
) -> None:
    for block in blocks:
        if drop_fields:
            drop_columns(block, POP_COLUMNS_BLOCK)

        if rename_fields:
            for oldname, newname in FIELDNAME_MAP_BLOCK.items():
                block[newname] = block.pop(oldname)

        if process_fields:
            block["block_id_group"] = get_id_group(block["block_id"], block_bucket_size)

        if cast_blob_fields:
            for elem in BLOB_COLUMNS_BLOCK:
                block[elem] = hex_to_bytes(
                    block[elem]
                )  # convert hex strings to byte arrays (blob in Cassandra)


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
    "shielded": 12,
    "anchor": 13,
}


def addresstype_to_int(addr_type: str) -> int:
    """Convert btcpy address types to integers

    Args:
        addr_type (str): Description

    Returns:
        str: address type as integer

    Raises:
        UnknownAddressType: Description
    """
    if isinstance(addr_type, int):  # address_type has already been resolved
        return addr_type

    if addr_type in _address_types:
        return _address_types.get(addr_type)

    raise UnknownAddressType(f"unknown address type {addr_type}")


def address_as_string(x):
    if x["type"] in ["null", "nulldata", "nonstandard", "witness_unknown", "shielded"]:
        return None
    return x["addresses"]


def tx_io_summary(x):
    """Creates a short summary of in and outputs

    Args:
        x (TYPE): Description

    Returns:
        TYPE: list of form address, value, type
    """
    return [address_as_string(x), x["value"], addresstype_to_int(x["type"])]


def tx_stats(tx):
    return (
        tx["tx_id"],
        len(tx["inputs"]),
        len(tx["outputs"]),
        tx["total_input"],
        tx["total_output"],
    )


def parse_script(s: str) -> Tuple[List[str], str]:
    """Parses the output addresses from a bitcoin-like locking script

    Args:
        s (str): script in binary hex format

    Returns:
        Tuple[List[str], str]: address list and script type

    Raises:
        P2pkParserException: if P2PK script can't be parsed
        UnknownScriptType: On unknown script type
    """
    if not _has_ingest_dependencies:
        raise ImportError(
            "The ingest.utxo needs bitcoinetl installed. Please install gslib with ingest dependencies."
        )

    script = ScriptBuilder.identify(s)

    if script.type == "p2pk":
        try:
            return [str(P2pkhAddress(script.pubkey.hash(), mainnet=True))], script.type
        except Exception as e:
            raise P2pkParserException(
                f"ScriptParseError: cannot parse pubkey from {s}"
                f" (of type {script.type}) --- {e}"
            )

    if script.type == "p2pkh":
        return [str(P2pkhAddress(script.pubkeyhash, mainnet=True))], script.type

    if script.type == "multisig":
        return [
            str(P2pkhAddress(k.hash(), mainnet=True))
            for k in script.pubkeys
            if hasattr(k, "hash")
        ], script.type

    if script.type in ["p2sh", "p2wpkhv0", "p2wshv0"]:
        return [str(script.address(mainnet=True))], script.type

    if script.type == "nulldata":
        return None, script.type

    raise UnknownScriptType(
        f"ScriptParseError: not handling script type {script.type} at the moment."
    )


def enrich_txs(
    txs: Iterable,
    resolver: Optional[OutputResolverBase],
    ignore_missing_outputs: bool,
    input_reference_only=False,
) -> None:
    """Resolves transaction input links to the spent transactions.

    Args:
        txs (Iterable): transactions as produced by btc etl
        resolver (OutputResolverBase): instance of a
                                        resolver that finds the spent outputs
        ignore_missing_outputs (bool): if True skips outputs that can not be resolved

    Raises:
        InputNotFoundException: If inputs can not be
                                resolved and ignore_missing_outputs is false
    """

    # add outputs to cache before processing the inputs
    # help to avoid ordering related errors while resolving inputs
    # one instance where this happens is bitcoin cash;
    # btc-etl does not respect the new CTOR ordering for bch transactions.
    # given the returned tx order, tx inputs do not always resolve correctly.
    # an example is block (801379, tx at index 2 spends tx at index 12)
    # we circumvent this issue by pre-populating the cache.

    for tx in txs:
        # process outputs
        for o in tx["outputs"]:
            if o["addresses"]:
                if o["addresses"][0] and o["addresses"][0].startswith("bitcoincash:"):
                    o["addresses"] = [bch_address_to_legacy(a) for a in o["addresses"]]

                if o["addresses"][0] and o["addresses"][0].startswith("nonstandard"):
                    try:
                        address_list, scripttype = parse_script(o["script_hex"])
                        o["addresses"] = (
                            address_list if address_list else o["addresses"]
                        )
                        o["type"] = scripttype
                    except (
                        UnknownScriptType,
                        UnknownAddressType,
                        P2pkParserException,
                    ) as exception:
                        logger.warning(
                            f"{exception}: cannot parse output script {o}"
                            f" from tx {tx.get('hash')}"
                        )
                if not input_reference_only:
                    resolver.add_output(tx["hash"], o)

    if input_reference_only:
        pass
    else:
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
                            resolved_outputs = resolver.get_output(ref)
                            resolved = (
                                resolved_outputs.get(ind) if resolved_outputs else None
                            )

                            if resolved is None and ignore_missing_outputs:
                                logger.warning("Could not resolve spent_txs outputs")
                                continue
                            elif resolved is None:
                                raise InputNotFoundException(
                                    f"Spent Tx ({ref}) outputs  not found"
                                )

                            i["addresses"] = resolved["addresses"]
                            i["type"] = resolved["type"]
                            i["value"] = resolved["value"]
                        except (
                            UnknownScriptType,
                            UnknownAddressType,
                            P2pkParserException,
                        ) as exception:
                            logger.warning(
                                f"tx input cannot be resolved for {i['addresses']}"
                            )
                            logger.warning(exception)

            tx["input_value"] = sum(
                [i["value"] for i in tx["inputs"] if i["value"] is not None]
            )


def prepare_transactions_inplace(
    txs: Iterable,
    next_tx_id: Optional[int],
    tx_hash_prefix_len: Optional[int],
    tx_bucket_size: Optional[int],
    drop_fields=True,
    rename_fields=True,
    process_fields=True,
    cast_blob_fields=True,
) -> None:
    assert all(tx["index"] is not None for tx in txs)

    txs = sorted(
        txs, key=itemgetter("block_number", "index")
    )  # because bitcoin-etl does not guarantee a sort order

    for tx in txs:
        if drop_fields:
            drop_columns(tx, POP_COLUMNS_TX)

        if rename_fields:
            for oldname, newname in FIELDNAME_MAP_TX.items():
                tx[newname] = tx.pop(oldname)

        if process_fields:
            tx["inputs_raw"] = tx["inputs"]
            tx["outputs_raw"] = tx["outputs"]
            tx["coinjoin"] = is_coinjoin(tx)
            tx["inputs"] = [tx_io_summary(x) for x in tx["inputs"]]
            tx["outputs"] = [tx_io_summary(x) for x in tx["outputs"]]
            tx["tx_id_group"] = get_id_group(next_tx_id, tx_bucket_size)
            tx["tx_id"] = next_tx_id
            tx["tx_prefix"] = tx["tx_hash"][:tx_hash_prefix_len]
            next_tx_id += 1

        if cast_blob_fields:
            for elem in BLOB_COLUMNS_TX:
                tx[elem] = hex_to_bytes(
                    tx[elem]
                )  # convert hex strings to byte arrays (blob in Cassandra)


def get_tx_refs(spending_tx_hash: str, raw_inputs: Iterable, tx_hash_prefix_len: int):
    tx_refs = []
    spending_tx_hash = hex_to_bytes(spending_tx_hash)
    for inp in raw_inputs:
        spending_input_index = inp["index"]
        spent_tx_hash = hex_to_bytes(inp["spent_transaction_hash"])
        spent_output_index = inp["spent_output_index"]
        if spending_tx_hash is not None and spent_tx_hash is not None:
            # in zcash refs can be None in case of shielded txs.
            tx_refs.append(
                {
                    "spending_tx_hash": spending_tx_hash,
                    "spending_input_index": spending_input_index,
                    "spent_tx_hash": spent_tx_hash,
                    "spent_output_index": spent_output_index,
                    "spending_tx_prefix": strip_0x(bytes_to_hex(spending_tx_hash))[
                        :tx_hash_prefix_len
                    ],
                    "spent_tx_prefix": strip_0x(bytes_to_hex(spent_tx_hash))[
                        :tx_hash_prefix_len
                    ],
                }
            )

    return tx_refs


def ingest_transactions(
    items: Iterable,
    db: AnalyticsDb,
    sink_config: dict,
) -> None:
    write_to_sinks(db, sink_config, "transaction", items)


def preprocess_transaction_lookups(items: Iterable) -> List[Dict]:
    res = [{k: tx[k] for k in ("tx_prefix", "tx_hash", "tx_id")} for tx in items]
    return res


def ingest_transaction_lookups(
    items: Iterable,
    db: AnalyticsDb,
    sink_config: dict,
) -> None:
    res = preprocess_transaction_lookups(items)
    write_to_sinks(db, sink_config, "transaction_by_tx_prefix", res)


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
    """Display information about number of synced/ingested blocks.

    Args:
        last_synced_block (int): Description
        last_ingested_block (Optional[int]): Description
    """

    logger.warning(f"Last synced block: {last_synced_block:,}")
    if last_ingested_block is None:
        logger.warning("Last ingested block: None")
    else:
        logger.warning(f"Last ingested block: {last_ingested_block:,}")


def ingest_summary_statistics_cassandra(
    db: AnalyticsDb,
    timestamp: int,
    total_blocks: int,
    total_txs: int,
) -> None:
    """Summary

    Args:
        db (AnalyticsDb): Description
        timestamp (int): Description
        total_blocks (int): Description
        total_txs (int): Description
    """
    cassandra_ingest(
        db,
        "summary_statistics",
        [
            {
                "id": db.raw.keyspace_name(),
                "timestamp": timestamp,
                "no_blocks": total_blocks,
                "no_txs": total_txs,
            }
        ],
    )


def ingest_configuration_cassandra(
    db: AnalyticsDb,
    block_bucket_size: int,
    tx_hash_prefix_len: int,
    tx_bucket_size: int,
) -> None:
    """Store configuration details in Cassandra table.

    Args:
        db (AnalyticsDb): Description
        block_bucket_size (int): Description
        tx_hash_prefix_len (int): Description
        tx_bucket_size (int): Description
    """
    cassandra_ingest(
        db,
        "configuration",
        [
            {
                "id": db.raw.keyspace_name(),
                "block_bucket_size": int(block_bucket_size),
                "tx_prefix_length": tx_hash_prefix_len,
                "tx_bucket_size": tx_bucket_size,
            }
        ],
    )


def get_connection_from_url(
    provider_uri: str, provider_timeout: int
) -> "ThreadLocalProxy":
    return ThreadLocalProxy(lambda: BitcoinRpc(provider_uri, timeout=provider_timeout))


def get_stream_adapter(
    currency: str, provider_uri: str, batch_size: int, provider_timeout: int
) -> BtcStreamerAdapter:
    proxy = get_connection_from_url(provider_uri, provider_timeout)
    return BtcStreamerAdapter(
        proxy, chain=CHAIN_MAPPING.get(currency, None), batch_size=batch_size
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
    mode: str,
):
    if currency not in CHAIN_MAPPING:
        raise ValueError(
            f"{currency} not supported by ingest module,"
            f" supported {list(CHAIN_MAPPING.keys())}"
        )

    import_refs = mode == "utxo_only_tx_graph" or mode == "utxo_with_tx_graph"
    import_base_data = mode != "utxo_only_tx_graph"

    del mode

    logger.info(
        f"Importing base data: {import_base_data}, import tx refs {import_refs}"
    )

    btc_adapter = get_stream_adapter(
        currency, provider_uri, batch_size=batch_size, provider_timeout=provider_timeout
    )

    resolver = CassandraOutputResolver(
        db,
        tx_bucket_size=TX_BUCKET_SIZE,
        tx_prefix_length=TX_HASH_PREFIX_LENGTH,
    )

    last_synced_block = btc_adapter.get_current_block_number()
    last_ingested_block = db.raw.get_highest_block()
    print_block_info(last_synced_block, last_ingested_block)

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
        end_block = get_last_block_yesterday(btc_adapter, last_synced_block)

    if start_block > end_block:
        logger.warning("No blocks to ingest")
        return

    # if info then only print block info and exit
    if info:
        logger.info(
            f"Would ingest block range "
            f"{start_block:,} - {end_block:,} ({end_block - start_block:,} blks) "
            f"into {list(sink_config.keys())} "
        )
        return

    time1 = datetime.now()
    count = 0

    logger.info(
        f"Ingesting block range "
        f"{start_block:,} - {end_block:,} ({end_block - start_block:,} blks) "
        f"into {list(sink_config.keys())} "
    )

    if import_base_data:
        ingest_configuration_cassandra(
            db,
            block_bucket_size=BLOCK_BUCKET_SIZE,
            tx_hash_prefix_len=TX_HASH_PREFIX_LENGTH,
            tx_bucket_size=TX_BUCKET_SIZE,
        )

    with graceful_ctlc_shutdown() as check_shutdown_initialized:
        for block_id in range(start_block, end_block + 1, batch_size):
            current_end_block = min(end_block, block_id + batch_size - 1)

            with suppress_log_level(logging.INFO):
                blocks, txs = btc_adapter.export_blocks_and_transactions(
                    block_id, current_end_block
                )

            tx_refs = flatten(
                [
                    get_tx_refs(tx["hash"], tx["inputs"], TX_HASH_PREFIX_LENGTH)
                    for tx in txs
                ]
            )

            prepare_blocks_inplace(blocks, BLOCK_BUCKET_SIZE)

            if import_base_data:
                # until bitcoin-etl progresses
                # with https://github.com/blockchain-etl/bitcoin-etl/issues/43
                enrich_txs(txs, resolver, ignore_missing_outputs=False)

                latest_tx_id = db.raw.get_latest_tx_id_before_block(block_id)

                prepare_transactions_inplace(
                    txs, latest_tx_id + 1, TX_HASH_PREFIX_LENGTH, TX_BUCKET_SIZE
                )

                ingest_blocks(blocks, db, sink_config)
                ingest_transaction_lookups(
                    txs,
                    db,
                    sink_config,
                )
                ingest_transactions(txs, db, sink_config)
                ingest_block_transactions(txs, db, sink_config, BLOCK_BUCKET_SIZE)

            if import_refs:
                ingest_tx_refs(tx_refs, db, sink_config)

            last_block = blocks[-1]
            last_block_ts = last_block["timestamp"]
            last_block_id = last_block["block_id"]
            count += batch_size

            if count % 100 == 0:
                last_block_date = parse_timestamp(last_block_ts)
                time2 = datetime.now()
                time_delta = (time2 - time1).total_seconds()
                logger.info(
                    f"Last processed block: {current_end_block:,} "
                    f"[{last_block_date.strftime(GRAPHSENSE_DEFAULT_DATETIME_FORMAT)}] "
                    f"({count / time_delta:.1f} blocks/s)"
                )
                logger.debug(resolver.get_cache_stats())
                time1 = time2
                count = 0

            if check_shutdown_initialized():
                break

    last_block_date = parse_timestamp(last_block_ts)
    logger.info(
        f"Processed block range {start_block:,} - {last_block_id:,} "
        f" ({last_block_date.strftime(GRAPHSENSE_DEFAULT_DATETIME_FORMAT)})"
    )

    # store configuration details
    if "cassandra" in sink_config.keys() and import_base_data:
        last_block = blocks[-1]
        last_tx = txs[-1]
        ingest_summary_statistics_cassandra(
            db,
            timestamp=last_block_ts,
            total_blocks=last_block_id + 1,
            total_txs=last_tx["tx_id"] + 1,
        )


def prepare_transactions_inplace_parquet(txs, currency):
    # like in the cassandra ingestion
    prepare_transactions_inplace(
        txs,
        next_tx_id=None,
        tx_hash_prefix_len=None,
        tx_bucket_size=None,
        process_fields=False,
        drop_fields=False,
    )

    for tx in txs:
        drop_columns(tx, ["block_hash"])

        for input in tx["inputs"]:  # noqa
            input["spent_transaction_hash"] = hex_to_bytes(
                input["spent_transaction_hash"]
            )

        for output in tx["outputs"]:
            output["addresses"] = [
                address_to_bytes(currency, address) for address in output["addresses"]
            ]
            output.pop("script_asm", None)


def prepare_refs_inplace_parquet(tx_refs):
    for tx_ref in tx_refs:
        drop_columns(tx_ref, ["spent_tx_prefix"])
