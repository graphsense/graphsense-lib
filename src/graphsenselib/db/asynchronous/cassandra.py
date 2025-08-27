import asyncio
import hashlib
import logging
import re
import time
from collections import UserDict, namedtuple
from datetime import datetime
from functools import partial
from itertools import product
from math import floor
from pprint import PrettyPrinter
from typing import Callable, List, Optional, Sequence, Tuple, Union, Dict, Any

from async_lru import alru_cache
from cassandra import ConsistencyLevel, InvalidRequest
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import (
    EXEC_PROFILE_DEFAULT,
    Cluster,
    ExecutionProfile,
    NoHostAvailable,
)
from cassandra.policies import DCAwareRoundRobinPolicy, TokenAwarePolicy
from cassandra.protocol import ProtocolException
from cassandra.query import SimpleStatement, ValueSequence, dict_factory
from graphsenselib.utils.accountmodel import hex_to_bytes
from graphsenselib.config.cassandra_async_config import CassandraConfig
from graphsenselib.datatypes.abi import decode_logs_db
from graphsenselib.utils.account import calculate_id_group_with_overflow
from graphsenselib.utils.accountmodel import bytes_to_hex, hex_str_to_bytes, strip_0x
from graphsenselib.utils.address import address_to_user_format, cannonicalize_address
from graphsenselib.utils.function_call_parser import (
    parse_function_call,
    function_signatures as function_call_signatures,
)
from graphsenselib.utils.pubkey_to_address import convert_pubkey_to_addresses
from graphsenselib.utils.transactions import (
    SubTransactionIdentifier,
    SubTransactionType,
)
from graphsenselib.utils.tron import partial_tron_to_partial_evm
from graphsenselib.datatypes import NodeType
from graphsenselib.errors import (
    AddressNotFoundException,
    BadConfigError,
    BadUserInputException,
    BlockNotFoundException,
    ClusterNotFoundException,
    DBInconsistencyException,
    NetworkNotFoundException,
    NotFoundException,
    TransactionNotFoundException,
    nodeNotFoundException,
)
from graphsenselib.utils.rest_utils import is_eth_like


SMALL_PAGE_SIZE = 1000
BIG_PAGE_SIZE = 5000
SEARCH_PAGE_SIZE = 100

MAX_HEX_STRING_LENGTH = 64
FETCH_SIZE_MIN = 100


def try_partial_tron_to_partial_evm(addr_prefix):
    try:
        return partial_tron_to_partial_evm(addr_prefix)
    except ValueError:
        return ""


def create_upper_bound(s, is_string_not_hex=False):
    """Create upper bound for range query by incrementing last character"""
    if not s:
        return s

    if is_string_not_hex:
        chars = list(s)
        chars[-1] = chr(ord(chars[-1]) + 1)
        return "".join(chars)

    if all([x == "f" for x in strip_0x(s)]):
        return s + MAX_HEX_STRING_LENGTH * "f"
    else:
        has_0x = s != strip_0x(s)

        stripped_len_before = len(strip_0x(s))
        num = int(s, 16) + 1  # drops leading 0s
        hex_ = hex(num)[2:]  # drop 0x
        len_after = len(hex_)
        chars_to_pad = stripped_len_before - len_after
        padded = chars_to_pad * "0" + hex_
        if has_0x:
            return "0x" + padded
        else:
            return padded


def is_hexadecimal(s):
    """Check if a string is a valid hexadecimal number."""
    s = strip_0x(s)
    return all(c in "0123456789abcdefABCDEF" for c in s)


def getDateFromKeyspaceName(x):
    datePart = x.split("_")[-1]
    try:
        return datetime.strptime(datePart, "%Y%m%d")
    except ValueError:
        return None


class NetworkParameters(UserDict):
    def __getitem__(self, network):
        if network not in self:
            raise NetworkNotFoundException(network)
        return super().__getitem__(network)


def to_hex(paging_state):
    return paging_state.hex() if paging_state else paging_state


def bytes_from_hex(h_str: str) -> bytes:
    return hex_to_bytes(h_str)
    # if isinstance(h_str, str) and h_str.startswith("0x"):
    #     h_str = h_str[2:]
    # return bytes.fromhex(h_str) if h_str else None


def tx_hash_from_hex(tx_hash_str: str) -> bytes:
    try:
        return bytes_from_hex(tx_hash_str)
    except ValueError:
        raise BadUserInputException(
            f"{tx_hash_str} does not look like a valid transaction hash."
        )


def page_from_hex(page):
    try:
        return bytes_from_hex(page)
    except ValueError:
        # bytes.fromHex throws value error if non hex chars are found
        raise BadUserInputException(
            f"The requested next page token ({page}) "
            "is not formatted correctly. "
            "Only use the next_page token found in the response."
        )


def transaction_ordering_key(tx_id_key, tx):
    ref = tx.get("tx_reference", [None, None])
    trace_index = ref[0] or 0
    log_index = ref[1] or 0
    return (tx[tx_id_key], -trace_index, -log_index)


def identity1(x):
    return x


def identity2(y, x):
    return x


def fmt(v):
    if isinstance(v, str):
        return "'" + v + "'"
    if isinstance(v, bytes):
        return "0x" + v.hex()
    return str(v)


def replaceFrom(keyspace, query):
    r = re.compile(r"\s+FROM\s+", re.IGNORECASE)
    return r.sub(f" FROM {keyspace}.", query)


def replacePerc(query, named=False):
    if not named:
        r = re.compile(r"%s", re.IGNORECASE)
        return r.sub("?", query)
    else:
        r = re.compile(r"%\((\S+?)\)s", re.IGNORECASE)
        return r.sub(r":\1", query)


def one(result):
    if result is not None:
        return result.one()
    return None


def build_token_tx(token_currency, tx, token_tx, log):
    token_from = token_tx["parameters"].get("from", "decoding error")
    token_to = token_tx["parameters"].get("to", "decoding error")
    value = token_tx["parameters"].get("value", "decoding error")
    return {
        "currency": token_currency,
        "block_id": tx["block_id"],
        "block_timestamp": tx["block_timestamp"],
        "tx_hash": tx["tx_hash"],
        "from_address": hex_str_to_bytes(strip_0x(token_from)),
        "to_address": hex_str_to_bytes(strip_0x(token_to)),
        "token_tx_id": log["log_index"],
        "value": value,
        "type": "erc20",
    }


class BytesPrettyPrinter(PrettyPrinter):
    def format(self, object, context, maxlevels, level):
        if isinstance(object, bytes):
            x = object.hex()
            return (x[0:24] + "...", True, False)
        return PrettyPrinter.format(self, object, context, maxlevels, level)


class Result:
    def __init__(self, current_rows, params, paging_state):
        self.current_rows = current_rows
        self.params = params
        self.paging_state = paging_state

    def __len__(self):
        return len(self.current_rows)

    def __getitem__(self, key):
        return self.current_rows[key]

    def is_empty(self):
        return self.current_rows is None or not self.current_rows

    def one(self):
        if self.is_empty():
            return None
        return self.current_rows[0]


TxSummary = namedtuple("TxSummary", ["height", "timestamp", "tx_hash"])


def get_tx_id_column_name(network: str) -> str:
    return "transaction_id" if is_eth_like(network) else "tx_id"


# helper functions
def wc(cl, cond):
    return f"AND {cl} " if cond else ""


def get_tx_identifier(row, currency, type_overwrite=None) -> str:
    # trace_tx = row.get("is_tx_trace", False)
    h = row["tx_hash"].hex()

    if type_overwrite == "internal" or row["type"] == "internal":  # and not trace_tx:
        t_type = SubTransactionType.InternalTx
        sub_tx = row["trace_index"]
    elif type_overwrite == "erc20" or row["type"] == "erc20":
        t_type = SubTransactionType.ERC20
        sub_tx = row["token_tx_id"]
    elif type_overwrite == "external" or row["type"] == "external":
        if currency == "eth" and row.get("trace_index", None) is not None:
            t_type = SubTransactionType.InternalTx
            sub_tx = row["trace_index"]
        else:
            t_type = SubTransactionType.ExternalTx
            sub_tx = None
    else:
        raise Exception(f"Unknown transaction type {row}")

    return SubTransactionIdentifier(
        tx_hash=h, tx_type=t_type, sub_index=sub_tx
    ).to_string()


def merge_address_txs_subquery_results(
    logger,
    result_sets: Sequence[Result],
    ascending: bool,
    fetch_size: Optional[int],
    tx_id_keys: str = "tx_id",
    fetched_limit: Optional[int] = None,
    page: Optional[int] = None,
    offset: int = 0,
) -> Tuple[Sequence[dict], Optional[int], Optional[int]]:
    """Merges sub results of the address txs queries per asset and direction

    Args:
        result_sets (Sequence[Result]): List of result sets,
            one per parameter tuple
        fetch_size (int): number of items return at most
        tx_id_keys (str): name of the tx_id column
        fetched_limit (int): The limit that was used to fetch the result sets

    Returns:
        Tuple[Sequence[dict], Optional[int, int]]: A merged list of ordered
            transactions, highest id and the number of rows with this same id
            included in this results set,
            the last two form the page id for subsequent queries.
    """
    # find the least common tx_id where we then cut the result sets.
    # We need this because we order by tx_id, but we make queries for specific
    # (direction, asset) combinations. Without this logic (wlog ascending case),
    # we could progress within one query to tx_id n while another result set is
    # limited and has not yet loaded all txs for tx_ids < n. In this case these rows
    # would be skipped if we simply took the next page's tx_id as the maximum of
    # the tx_ids of the combined result sets. We cap all the result sets
    # at the minimum over all the within-result-set maximums of the tx_ids.
    border_tx_id = None
    for results in result_sets:
        if not results:
            continue
        if fetched_limit and len(results) < fetched_limit:
            continue
        if border_tx_id is None:
            border_tx_id = results[-1][tx_id_keys]
            continue
        order = min if ascending else max
        border_tx_id = order(border_tx_id, results[-1][tx_id_keys])

    # cut result_sets so that we only have the overlapping rows below/above
    # the border_tx_id
    # filtered out rows could be overlapping with yet not retrieved result sets
    candidates = [
        v
        for results in result_sets
        for v in results
        if border_tx_id is None
        or ascending
        and v[tx_id_keys] <= border_tx_id
        or not ascending
        and v[tx_id_keys] >= border_tx_id
    ]

    # Merge overlapping result sets by given sort order
    results = sorted(
        candidates,
        reverse=not ascending,
        key=partial(transaction_ordering_key, tx_id_keys),
    )

    # pp = BytesPrettyPrinter()
    # logger.debug(f'results {pp.pformat(results)}')

    if results and offset:
        results = results[offset:]

    if fetch_size is not None:
        results = results[:fetch_size]

    def get_offset(results, border_tx_id, page, offset):
        if border_tx_id is None:
            return None
        same_key_offset = 0
        while (
            len(results) > same_key_offset
            and results[-same_key_offset - 1][tx_id_keys] == border_tx_id
        ):  # noqa
            same_key_offset += 1

        if page == border_tx_id:
            same_key_offset += offset

        return same_key_offset

    current_offset = offset
    current_page = page
    border_tx_ids_reversed = []
    same_key_offsets_reversed = []
    # simulate all scenarios so we can get all the pages and offsets
    for i in range(len(results)):
        border_tx_id = results[len(results) - i - 1][tx_id_keys]
        current_results = results[:-i] if i > 0 else results
        offset_to_use = current_offset if border_tx_id == current_page else 0

        same_key_offsets_reversed.append(
            get_offset(current_results, border_tx_id, current_page, offset_to_use)
        )
        current_offset = same_key_offsets_reversed[-1]
        current_page = border_tx_id
        border_tx_ids_reversed.append(border_tx_id)

    border_tx_ids = border_tx_ids_reversed[::-1]
    same_key_offsets = same_key_offsets_reversed[::-1]

    return results, border_tx_ids, same_key_offsets


def build_select_address_txs_statement(
    network: str,
    node_type: NodeType,
    cols: Optional[Sequence[str]],
    with_lower_bound: bool,
    with_upper_bound: bool,
    with_tx_id: bool,
    ascending: bool,
    limit: Optional[int],
) -> str:
    # prebuild useful helpers and conditions
    eth_like = is_eth_like(network)
    tx_id_col = get_tx_id_column_name(network)

    # Select and shared where clause
    # Build select statement
    columns = ",".join(cols) if cols is not None else "*"

    query = (
        f"SELECT {columns} from {node_type}_transactions "
        f"WHERE {node_type}_id = %(id)s "
        f"AND {node_type}_id_group = %(g_id)s "
        "AND is_outgoing = %(is_outgoing)s "
    )

    # conditional where clause, loop independent
    query += wc(f"{node_type}_id_secondary_group = %(s_d_group)s", eth_like)

    query += wc("currency = %(currency)s", eth_like)

    # ascending
    query += wc(
        f"{tx_id_col} >= %(tx_id_lower_bound)s",
        not with_tx_id and ascending and with_lower_bound,
    )
    query += wc(
        f"{tx_id_col} <= %(tx_id_upper_bound)s",
        not with_tx_id and ascending and with_upper_bound,
    )

    # descending
    query += wc(
        f"{tx_id_col} >= %(tx_id_lower_bound)s",
        not with_tx_id and not ascending and with_lower_bound,
    )
    query += wc(
        f"{tx_id_col} <= %(tx_id_upper_bound)s",
        not with_tx_id and not ascending and with_upper_bound,
    )

    query += wc(f"{tx_id_col} = %(tx_id)s", with_tx_id)

    ordering = "ASC" if ascending else "DESC"
    # Ordering statement
    ordering_statement = (
        "ORDER BY "
        + (f" currency {ordering}," if eth_like else "")
        + f" {tx_id_col} {ordering}"
    )

    limit = "" if limit is None else f"LIMIT {limit}"

    return f"{query} {ordering_statement} {limit}"


class Cassandra:
    def eth(func):
        def check(*args, **kwargs):
            self = args[0]
            currency = args[1]
            if is_eth_like(currency):
                # first look for currency specific
                # then fallback to eth.
                do = func.__name__ + "_" + currency
                if not hasattr(self, do):
                    do = func.__name__ + "_eth"
                if hasattr(self, do) and callable(getattr(self, do)):
                    f = getattr(self, do)
                    args = args[1:]
                    return f(*args, **kwargs)
            return func(*args, **kwargs)

        return check

    def new(func):
        def check(*args, **kwargs):
            self = args[0]
            currency = args[1]
            if is_eth_like(currency):
                do = func.__name__ + "_new"
                if hasattr(self, do) and callable(getattr(self, do)):
                    f = getattr(self, do)
                    args = args[1:]
                    return f(*args, **kwargs)
            return func(*args, **kwargs)

        return check

    def __init__(self, config: Union[Dict[str, Any], CassandraConfig], logger):
        self.logger = logger

        tconfig = None
        # Convert dict config to CassandraConfig if needed
        if isinstance(config, dict):
            try:
                tconfig = CassandraConfig(**config)
            except Exception as e:
                raise BadConfigError(f"Invalid configuration: {e}")
        elif isinstance(config, CassandraConfig):
            tconfig = config
        else:
            raise BadConfigError(
                "Config must be a dictionary or CassandraConfig instance"
            )

        # Convert back to dict for backward compatibility with existing code
        config = tconfig.model_dump()

        if "currencies" not in config or config["currencies"] is None:
            raise BadConfigError("Missing config property: currencies")
        if "nodes" not in config or config["nodes"] is None:
            raise BadConfigError("Missing config property: nodes")
        self.config = config
        self.tconfig = tconfig
        self.prepared_statements = {}
        self.connect()
        self.parameters = NetworkParameters()
        self.get_cross_chain_pubkey_related_addresses_available = False

        for currency in config["currencies"]:
            if config["currencies"][currency] is None:
                config["currencies"][currency] = {}

            # automatically find latest active keyspace if not configured
            if (
                "raw" not in config["currencies"][currency]
                or config["currencies"][currency]["raw"] is None
            ):
                config["currencies"][currency]["raw"] = f"{currency}_raw"

            if (
                "transformed" not in config["currencies"][currency]
                or config["currencies"][currency]["transformed"] is None
            ):
                config["currencies"][currency]["transformed"] = (
                    self.find_latest_transformed_keyspace(currency)
                )

            self.check_keyspace(config["currencies"][currency]["raw"])
            self.check_keyspace(config["currencies"][currency]["transformed"])
            self.load_parameters(currency)

    def connect(self):
        try:
            cl = ConsistencyLevel.name_to_value.get(
                self.config.get("consistency_level", "LOCAL_ONE"),
                ConsistencyLevel.LOCAL_ONE,
            )

            exec_prof = ExecutionProfile(
                request_timeout=60,
                row_factory=dict_factory,
                load_balancing_policy=TokenAwarePolicy(DCAwareRoundRobinPolicy()),
                consistency_level=cl,
            )

            auth_provider = None
            if "username" in self.config and self.config["username"] is not None:
                auth_provider = PlainTextAuthProvider(
                    username=self.config["username"],
                    password=self.config.get("password", ""),
                )

            self.cluster = Cluster(
                self.config["nodes"],
                port=int(self.config.get("port", 9042)),
                protocol_version=5,
                connect_timeout=60,
                execution_profiles={EXEC_PROFILE_DEFAULT: exec_prof},
                auth_provider=auth_provider,
            )
            self.session = self.cluster.connect()
            # self.session.row_factory = dict_factory
            if self.logger:
                self.logger.info(f"Connection ready. Using CL: {cl}")
        except NoHostAvailable:
            retry = self.config.get("retry_interval", None)
            retry = 5 if retry is None else retry
            if self.logger:
                self.logger.error(f"Could not connect. Retrying in {retry} secs.")
            time.sleep(retry)
            self.connect()

    def check_keyspace(self, keyspace):
        query = "SELECT * FROM system_schema.keyspaces where keyspace_name = %s"
        result = self.session.execute(query, [keyspace])
        if one(result) is None:
            raise BadConfigError("Keyspace {} does not exist".format(keyspace))

        if self.logger:
            self.logger.info(f"Using keyspace: {keyspace}")

    def find_latest_transformed_keyspace(self, network):
        query = "SELECT keyspace_name FROM system_schema.keyspaces"
        result = self.session.execute(query)
        prefix = f"{network.lower()}_transformed"
        candidates = [
            getDateFromKeyspaceName(x["keyspace_name"])
            for x in result
            if x["keyspace_name"].startswith(prefix)
        ]

        res = []
        for c in reversed(sorted(filter(lambda x: x is not None, candidates))):
            ks = f"{prefix}_{c.strftime('%Y%m%d')}"
            q = f"SELECT * FROM {ks}.summary_statistics limit 1"
            result = self.session.execute(q)

            if one(result) is None:
                if self.logger:
                    self.logger.error(
                        f"{ks} is not online (missing summary_statistics row). skipping"
                    )
                continue
            else:
                # return ks
                res.append((ks, result[0]["no_blocks"]))

        res = list(reversed(sorted(res, key=lambda item: item[1])))

        if len(res) > 0:
            return res[0][0]
        else:
            raise Exception(
                f"Automatic detection for transformed keyspace failed for network {network}."
            )

    @eth
    def load_token_configuration(self, currency):
        return None

    def load_token_configuration_eth(self, currency):
        query = "SELECT * FROM token_configuration"
        return {
            row["currency_ticker"]: row
            for row in self.execute(currency, "transformed", query)
        }

    async def fix_timestamp(
        self, currency, item, timestamp_col="block_timestamp", block_id_col="block_id"
    ):
        if currency == "trx" and item[timestamp_col] < 500000000:
            # TODO block timestamp in tx is currently
            # wrong / we divide by 1000 to get from
            # mili to seconds. But it appears to be
            # wrong in the txs.
            block = await self.get_block_timestamp(currency, item[block_id_col])
            if block is not None:
                item[timestamp_col] = block["timestamp"]
            else:
                self.logger.warning(
                    f"Could not load block {item[block_id_col]}, to fix timestamp."
                )

    def load_parameters(self, keyspace):
        currency = keyspace
        self.parameters[keyspace] = {}
        for kind in ["raw", "transformed"]:
            query = "SELECT * FROM configuration"
            result = self.execute(keyspace, kind, query)
            if one(result) is None:
                raise BadConfigError(
                    f"No configuration table found for {kind} keyspace {{}}".format(
                        keyspace
                    )
                )
            for key, value in result.one().items():
                self.parameters[keyspace][key] = value

        self.parameters[keyspace]["token_config"] = self.load_token_configuration(
            keyspace
        )

        # check schema for compatibility and set parameter flags
        keyspace_name = self.get_keyspace_mapping(keyspace, "transformed")

        if keyspace == "eth":
            query = (
                "SELECT column_name FROM system_schema.columns "
                "WHERE keyspace_name = %s AND "
                "table_name = 'block_transactions';"
            )
            result = self.session.execute(query, (keyspace_name,))
            self.parameters[currency]["use_flat_block_txs"] = (
                "tx_id" in [x["column_name"] for x in result]
            ) and keyspace == "eth"
        else:
            self.parameters[currency]["use_flat_block_txs"] = False

        query = (
            "SELECT column_name FROM system_schema.columns "
            "WHERE keyspace_name = %s AND table_name = 'address_transactions';"
        )
        result = self.session.execute(query, (keyspace_name,))
        self.parameters[currency]["use_legacy_log_index"] = (
            "log_index" in [x["column_name"] for x in result]
        ) and keyspace == "eth"

        query = "SELECT table_name FROM system_schema.tables WHERE keyspace_name = %s ;"
        result = self.session.execute(query, (keyspace_name,))
        self.parameters[currency]["use_delta_updater_v1"] = "new_addresses" in [
            x["table_name"] for x in result
        ]

        keyspace_name = self.get_keyspace_mapping(keyspace, "raw")
        result = self.session.execute(query, (keyspace_name,))

        self.parameters[currency]["tx_graph_available"] = "transaction_spending" in [
            x["table_name"] for x in result
        ]

        pubkeyks = self.tconfig.cross_chain_pubkey_mapping_keyspace

        if pubkeyks is None:
            self.get_cross_chain_pubkey_related_addresses_available = False
        else:
            query = (
                "SELECT table_name FROM system_schema.tables WHERE keyspace_name = %s ;"
            )
            result = self.session.execute(query, (pubkeyks,))
            tblnames = [x["table_name"] for x in result]
            self.get_cross_chain_pubkey_related_addresses_available = (
                "pubkey_by_address" in tblnames
            )

    def get_prefix_lengths(self, currency):
        if currency not in self.parameters:
            raise NetworkNotFoundException(currency)
        p = self.parameters[currency]
        return {"address": p["address_prefix_length"], "tx": p["tx_prefix_length"]}

    def get_supported_currencies(self):
        return self.config["currencies"].keys()

    def get_token_configuration(self, currency):
        eth_config = self.parameters.get(currency, None)
        return eth_config["token_config"] if eth_config is not None else {}

    def get_balance_provider(self, currency):
        provider = self.config["currencies"][currency].get("balance_provider", None)
        if provider is not None and provider == "node":
            from graphsenselib.utils.node_balances import get_balances

            return partial(get_balances, provider)
        else:
            return None

    def get_keyspace_mapping(self, currency, keyspace_type):
        if currency is None:
            raise BadUserInputException("Missing currency")
        if keyspace_type not in ("raw", "transformed"):
            raise BadUserInputException(f"Unknown keyspace type {keyspace_type}")
        if currency not in self.config["currencies"]:
            raise BadUserInputException(f"Unknown currency in config: {currency}")
        return self.config["currencies"][currency][keyspace_type]

    def close(self):
        self.cluster.shutdown()

    def execute(
        self,
        currency,
        keyspace_type,
        query,
        params=None,
        paging_state=None,
        fetch_size=None,
    ):
        keyspace = self.get_keyspace_mapping(currency, keyspace_type)
        q = replaceFrom(keyspace, query)
        # self.logger.debug(f'{query} {params}')
        q = SimpleStatement(q, fetch_size=fetch_size)

        try:
            result = self.session.execute(q, params, paging_state=paging_state)
        except NoHostAvailable:
            self.connect()
            result = self.execute(
                currency,
                keyspace_type,
                query,
                params=params,
                paging_state=paging_state,
                fetch_size=fetch_size,
            )

        return result

    async def execute_async(
        self,
        currency,
        keyspace_type,
        query,
        params=[],
        paging_state=None,
        fetch_size=None,
        autopaging=False,
    ):
        try:
            result = await self.execute_async_lowlevel(
                currency,
                keyspace_type,
                query,
                params=params,
                paging_state=paging_state,
                fetch_size=fetch_size,
            )
        except ProtocolException as e:
            if "Invalid value for the paging state" not in str(e):
                raise e
            raise BadUserInputException(
                "Invalid value for page. Please use handle from previous requests."
            )
        if not autopaging:
            return result

        if result.paging_state is None:
            return result

        more = await self.execute_async(
            currency,
            keyspace_type,
            query,
            params=params,
            paging_state=result.paging_state,
            fetch_size=fetch_size,
            autopaging=True,
        )

        for row in more.current_rows:
            result.current_rows.append(row)
        return result

    def get_prepared_statement(self, query):
        hash_object = hashlib.sha256(query.encode("utf-8"))
        hex_dig = hash_object.hexdigest()
        if hex_dig not in self.prepared_statements:
            self.prepared_statements[hex_dig] = self.session.prepare(query)
        return self.prepared_statements[hex_dig]

    def execute_async_core(
        self,
        query: str,
        params: Dict[str, Any] = None,
        paging_state=None,
        fetch_size: Optional[int] = None,
        success_callback: Optional[Callable[Result, None]] = None,
    ):
        prep = self.get_prepared_statement(query)

        prep.fetch_size = int(fetch_size) if fetch_size else None
        # self.session.default_timeout = 60

        response_future = self.session.execute_async(
            prep, params, timeout=60, paging_state=paging_state
        )
        loop = asyncio.get_event_loop()
        future = loop.create_future()

        def safe_set_result():
            try:
                future.set_result(None)
            except asyncio.exceptions.InvalidStateError:
                pass

        def on_done(result):
            if future.cancelled():
                loop.call_soon_threadsafe(safe_set_result)
                return
            result = Result(
                current_rows=result,
                params=params,
                paging_state=response_future._paging_state,
            )
            if success_callback is not None:
                success_callback(result)
            loop.call_soon_threadsafe(future.set_result, result)

        def on_err(result):
            loop.call_soon_threadsafe(future.set_exception, result)

        response_future.add_callbacks(on_done, on_err)
        return future

    def execute_async_lowlevel(
        self,
        currency,
        keyspace_type,
        query,
        params=[],
        paging_state=None,
        fetch_size=None,
    ):
        named_params = isinstance(params, dict)
        keyspace = self.get_keyspace_mapping(currency, keyspace_type)
        query = replaceFrom(keyspace, query)
        q = replacePerc(query, named=named_params)

        def callback(result):
            if self.logger.level == logging.DEBUG:
                if named_params:
                    formatted = query
                    for k, v in params.items():
                        formatted = formatted.replace("%(" + k + ")s", fmt(v))
                else:
                    formatted = query % tuple([fmt(v) for v in params])
                if len(result.current_rows) > 0:
                    self.logger.debug(formatted)
                    # pp = BytesPrettyPrinter()
                    # self.logger.debug(pp.pformat(result.current_rows))
                    self.logger.debug(f"result size {len(result.current_rows)}")

        try:
            return self.execute_async_core(
                q,
                params=params,
                fetch_size=fetch_size,
                paging_state=paging_state,
                success_callback=callback,
            )
        except NoHostAvailable:
            self.connect()
            return self.execute_async_lowlevel(
                currency,
                keyspace_type,
                query,
                params=params,
                paging_state=paging_state,
                fetch_size=fetch_size,
            )

    async def concurrent_with_args(
        self,
        currency,
        keyspace_type,
        query,
        params,
        filter_empty=True,
        return_one=True,
        keep_meta=False,
    ):
        aws = [
            self.execute_async(currency, keyspace_type, query, param, autopaging=True)
            for param in params
        ]
        results = []
        for result in await asyncio.gather(*aws):
            if filter_empty and result is None:
                continue
            if return_one:
                o = one(result)
                if not keep_meta:
                    result = o
                else:
                    result.current_rows = [o]
                if o:
                    results.append(result)
                else:
                    if not filter_empty:
                        results.append(result)
                continue
            if not keep_meta:
                results.extend(result.current_rows)
            else:
                results.append(result)
        return results

    @alru_cache(ttl=5)
    async def get_currency_statistics(self, currency):
        query = "SELECT * FROM summary_statistics LIMIT 1"
        result = await self.execute_async(currency, "transformed", query)
        stats = one(result)
        if self.parameters[currency]["use_delta_updater_v1"]:
            try:
                query = "SELECT * FROM delta_updater_status LIMIT 1"
                result = await self.execute_async(currency, "transformed", query)
                result = one(result)
                if result:
                    stats["no_blocks"] = result["last_synced_block"] + 1
                    stats["timestamp"] = (
                        int(result["last_synced_block_timestamp"].timestamp())
                        if "last_synced_block_timestamp" in result
                        else stats["timestamp"]
                    )

            except InvalidRequest as e:
                if "delta_updater_status" not in str(e):
                    raise e

        if is_eth_like(currency):
            stats["no_clusters"] = 0

        return stats

    @eth
    async def get_block(self, currency, height):
        query = "SELECT * FROM block WHERE block_id_group = %s AND block_id = %s"
        return (
            await self.execute_async(
                currency,
                "raw",
                query,
                [self.get_block_id_group(currency, height), height],
            )
        ).one()

    @alru_cache(maxsize=10000)
    async def get_block_timestamp(self, currency, height):
        query = (
            "SELECT timestamp FROM block WHERE block_id_group = %s AND block_id = %s"
        )
        return (
            await self.execute_async(
                currency,
                "raw",
                query,
                [self.get_block_id_group(currency, height), height],
            )
        ).one()

    async def get_block_by_date_allow_filtering(self, currency, timestamp: int) -> int:
        query = "SELECT min(block_id) as block_id, timestamp FROM block WHERE timestamp >= %s allow filtering"
        return (
            await self.execute_async(
                currency,
                "raw",
                query,
                [timestamp],
            )
        ).one()

    async def get_block_below_block_allow_filtering(
        self, currency, block_id: int
    ) -> int:
        query = "SELECT max(block_id) as block_id, timestamp FROM block WHERE block_id < %s allow filtering"
        return (
            await self.execute_async(
                currency,
                "raw",
                query,
                [block_id],
            )
        ).one()

    async def list_block_txs(self, currency, height):
        tx_ids = await self.list_block_txs_ids(currency, height)
        if tx_ids is None:
            raise BlockNotFoundException(currency, height)
        tx_ids = sorted(list(set(tx_ids)))
        return await self.list_txs_by_ids(currency, tx_ids, include_token_txs=True)

    @alru_cache(maxsize=1000)
    async def get_rates(self, currency, height):
        query = "SELECT * FROM exchange_rates WHERE block_id = %s"
        result = await self.execute_async(currency, "transformed", query, [height])
        result = one(result)
        if result is None:
            return None
        return self.markup_rates(currency, result)

    # async def list_rates(self, currency, heights):
    #     result = await self.concurrent_with_args(
    #         currency, 'transformed',
    #         "SELECT * FROM exchange_rates WHERE block_id = %s",
    #         [[h] for h in heights])
    #     for row in result:
    #         self.markup_rates(currency, row)
    #     return result

    async def list_rates(self, currency, heights):
        aws = [self.get_rates(currency, h) for h in heights]
        return await asyncio.gather(*aws)

    async def list_address_txs(
        self,
        currency,
        address,
        direction,
        min_height=None,
        max_height=None,
        order=None,
        token_currency=None,
        page=None,
        pagesize=None,
    ):
        return await self.list_txs_by_node_type(
            currency,
            NodeType.ADDRESS,
            address,
            direction,
            min_height=min_height,
            max_height=max_height,
            order=order,
            token_currency=token_currency,
            page=page,
            pagesize=pagesize,
        )

    async def list_entity_txs(
        self,
        currency,
        entity,
        direction,
        min_height=None,
        max_height=None,
        order=None,
        token_currency=None,
        page=None,
        pagesize=None,
    ):
        return await self.list_txs_by_node_type(
            currency,
            NodeType.CLUSTER,
            entity,
            direction,
            min_height=min_height,
            max_height=max_height,
            order=order,
            token_currency=token_currency,
            page=page,
            pagesize=pagesize,
        )

    @eth
    async def list_txs_by_node_type(
        self,
        currency,
        node_type: NodeType,
        id,
        direction,
        min_height=None,
        max_height=None,
        order=None,
        token_currency=None,
        page=None,
        pagesize=None,
    ):
        ascending = order == "asc"

        self.logger.debug(f"min_height {min_height}")
        self.logger.debug(f"max_height {max_height}")
        first_tx_id, upper_bound = await self.resolve_tx_id_range_by_block(
            currency, min_height, max_height
        )
        self.logger.debug(f"first_tx_id {first_tx_id}")
        self.logger.debug(f"upper_bound {upper_bound}")

        fetch_size = min(pagesize or BIG_PAGE_SIZE, BIG_PAGE_SIZE)
        include_assets = [currency.upper()]

        results, paging_states, finished = await self.list_address_txs_ordered(
            network=currency,
            node_type=node_type,
            id=id,
            tx_id_lower_bound=first_tx_id,
            tx_id_upper_bound=upper_bound,
            is_outgoing=(direction == "out" if direction is not None else None),
            include_assets=include_assets,
            ascending=ascending,
            page=page,
            fetch_size=fetch_size,
        )
        paging_state = paging_states[-1] if paging_states and not finished else None

        txs = await self.list_txs_by_ids(
            currency, [row["tx_id"] for row in results], filter_empty=False
        )

        rows = []
        for row, tx in zip(results, txs):
            if tx is None:
                continue
            row["tx_hash"] = tx["tx_hash"]
            row["height"] = tx["block_id"]
            row["timestamp"] = tx["timestamp"]
            row["coinbase"] = tx["coinbase"]
            rows.append(row)

        return rows, str(paging_state) if paging_state is not None else None

    @eth
    async def get_next_block_with_transactions(
        self, network: str, find_max_block: bool, height: int, highest_block: int
    ):
        # BTC and similar have txs in every block, since there is at least the coinbase
        # so no search needed.
        return height

    async def get_next_block_with_transactions_eth(
        self, network: str, find_max_block: bool, height: int, highest_block: int
    ) -> Optional[int]:
        height_group = self.get_block_id_group(network, height)
        max_group = self.get_block_id_group(network, highest_block)
        min_group = 0
        if find_max_block:
            query = (
                "SELECT max(block_id) as block FROM block "
                "WHERE block_id_group=%s and transaction_count > 0 and block_id <= %s allow filtering "
            )
        else:
            query = (
                "SELECT min(block_id) as block FROM block "
                "WHERE block_id_group=%s and transaction_count > 0 and block_id >= %s allow filtering "
            )
        result_block = None
        while (
            result_block is None
            and height_group >= min_group
            and height_group <= max_group
        ):
            result = await self.execute_async(
                network, "raw", query, [height_group, int(height)]
            )
            row = one(result)
            if row is not None:
                result_block = row["block"]

            height_group = height_group + (-1 if find_max_block else +1)

        return result_block

    async def resolve_tx_id_range_by_block(
        self, network: str, min_height: Optional[int], max_height: Optional[int]
    ) -> Tuple[Optional[int], Optional[int]]:
        stats = await self.get_currency_statistics(network)
        if not stats:
            return None, None
        last_height = stats["no_blocks"] - 1
        first_tx_id = last_tx_id = None
        if min_height is not None:
            min_block_with_txs = await self.get_next_block_with_transactions(
                network, False, min_height, last_height
            )

            former_txs = (
                await self.list_block_txs_ids(network, min_block_with_txs)
                if min_block_with_txs is not None
                else None
            )
            first_tx_id = min(former_txs) if former_txs else None
        if max_height is not None:
            max_block_with_txs = await self.get_next_block_with_transactions(
                network, True, max_height, last_height
            )
            latter_txs = (
                await self.list_block_txs_ids(network, max_block_with_txs)
                if max_block_with_txs is not None
                else None
            )
            last_tx_id = max(latter_txs) if latter_txs else None

        return first_tx_id, last_tx_id

    @eth
    async def list_block_txs_ids(self, currency, height):
        height_group = self.get_block_id_group(currency, height)
        query = (
            "SELECT txs FROM block_transactions WHERE block_id_group=%s and block_id=%s"
        )
        result = await self.execute_async(
            currency, "raw", query, [height_group, int(height)]
        )

        if one(result) is None:
            return None
        return [tx.tx_id for tx in result.one()["txs"]]

    async def list_block_txs_ids_eth(self, currency, height):
        if not self.parameters[currency]["use_flat_block_txs"]:
            if height is None:
                return None
            height_group = self.get_id_group(currency, height)
            query = (
                "SELECT txs FROM block_transactions "
                "WHERE block_id_group=%s and block_id=%s"
            )
            result = await self.execute_async(
                currency, "transformed", query, [height_group, int(height)]
            )
            if one(result) is None:
                return None
            return result.one()["txs"]
        else:
            return await self.list_block_txs_ids_trx(currency, height)

    async def list_block_txs_ids_trx(self, currency, height):
        if height is None:
            return None
        height_group = self.get_id_group(currency, height)
        query = (
            "SELECT tx_id FROM block_transactions "
            "WHERE block_id_group=%s and block_id=%s"
        )
        result = await self.execute_async(
            currency, "transformed", query, [height_group, int(height)], autopaging=True
        )
        res = [r["tx_id"] for r in result.current_rows]
        return res

    async def get_addresses_by_ids(self, currency, address_ids, address_only=False):
        params = [
            (self.get_id_group(currency, address_id), address_id)
            for address_id in address_ids
        ]
        fields = "address, address_id, address_id_group" if address_only else "*"
        query = (
            f"SELECT {fields} FROM address WHERE "
            "address_id_group = %s and address_id = %s"
        )
        result = await self.concurrent_with_args(currency, "transformed", query, params)

        return result

    async def get_new_address(self, currency, address):
        if not self.parameters[currency]["use_delta_updater_v1"]:
            return None
        prefix = self.scrub_prefix(currency, address)
        if is_eth_like(currency):
            prefix = prefix.upper()
        prefix_length = self.get_prefix_lengths(currency)["address"]
        query = "SELECT * FROM new_addresses WHERE address_prefix = %s AND address = %s"
        try:
            result = await self.execute_async(
                currency, "transformed", query, [prefix[:prefix_length], address]
            )
            return one(result)
        except InvalidRequest as e:
            if "new_addresses" not in str(e):
                raise e

        return None

    async def get_address_id(self, currency, address):
        prefix = self.scrub_prefix(currency, address)
        if is_eth_like(currency):
            prefix = prefix.upper()
        query = (
            "SELECT address_id FROM address_ids_by_address_prefix "
            "WHERE address_prefix = %s AND address = %s"
        )
        prefix_length = self.get_prefix_lengths(currency)["address"]
        result = await self.execute_async(
            currency, "transformed", query, [prefix[:prefix_length], address]
        )
        result = one(result)
        if not result:
            return None

        return result["address_id"]

    async def get_address_id_id_group(self, currency, address):
        address_id = await self.get_address_id(currency, address)
        if address_id is None:
            raise AddressNotFoundException(currency, address)
        id_group = self.get_id_group(currency, address_id)
        return address_id, id_group

    def get_id_group(self, keyspace, id_):
        if keyspace not in self.parameters:
            raise NetworkNotFoundException(keyspace)
        bucket_size = self.parameters[keyspace]["bucket_size"]
        gid = floor(int(id_) / bucket_size)
        if gid.bit_length() > 31:
            # tron tx_id are long and the group is int
            # thus we need to also consider overflows in this case
            # additionally spark does not calculate ids on int basis but
            # based on floats which can lead to rounding errors.
            gid = calculate_id_group_with_overflow(id_, bucket_size)
        return gid

    def get_block_id_group(self, keyspace, id_):
        if keyspace not in self.parameters:
            raise NetworkNotFoundException(keyspace)
        return floor(int(id_) / self.parameters[keyspace]["block_bucket_size"])

    @eth
    def get_tx_id_group(self, keyspace, id_):
        if keyspace not in self.parameters:
            raise NetworkNotFoundException(keyspace)
        return floor(int(id_) / self.parameters[keyspace]["tx_bucket_size"])

    def get_tx_id_group_eth(self, keyspace, id_):
        return self.get_id_group(keyspace, id_)

    async def new_address(self, currency, address):
        data = await self.get_new_address(currency, address)
        if data is None:
            raise AddressNotFoundException(currency, address)
        Values = namedtuple("Values", ["value", "fiat_values"])
        zero_values = Values(
            value=0,
            fiat_values=[
                {"code": c.lower(), "value": 0.0}
                for c in self.parameters[currency]["fiat_currencies"]
            ],
        )
        return {
            "address": address,
            "cluster_id": data["address_id"],
            "first_tx": TxSummary(data["block_id"], data["timestamp"], data["tx_hash"]),
            "last_tx": TxSummary(data["block_id"], data["timestamp"], data["tx_hash"]),
            "no_incoming_txs": 0,
            "no_outgoing_txs": 0,
            "total_received": zero_values,
            "total_spent": zero_values,
            "in_degree": 0,
            "out_degree": 0,
            "balance": 0,
            "status": "new",
        }

    async def new_entity(self, currency, address):
        data = await self.new_address(currency, address)
        data["no_addresses"] = 1
        data["root_address"] = address
        return data

    async def get_address_by_address_id(self, currency, address_id):
        address_id_group = self.get_id_group(currency, address_id)
        query = (
            "SELECT address FROM address WHERE address_id = %s"
            " AND address_id_group = %s"
        )
        result = await self.execute_async(
            currency, "transformed", query, [address_id, address_id_group]
        )
        result = one(result)
        if not result:
            if not is_eth_like(currency):
                return None
            raise AddressNotFoundException(currency, address_id, no_external_txs=True)
        return result["address"]

    async def get_address(self, currency, address):
        address_id, address_id_group = await self.get_address_id_id_group(
            currency, address
        )
        query = "SELECT * FROM address WHERE address_id = %s AND address_id_group = %s"
        result = await self.execute_async(
            currency, "transformed", query, [address_id, address_id_group]
        )
        result = one(result)
        if not result:
            if not is_eth_like(currency):
                return None
            raise AddressNotFoundException(currency, address, no_external_txs=True)

        return await self.finish_address(currency, result)

    @eth
    async def get_address_entity_id(self, currency, address):
        address_id, address_id_group = await self.get_address_id_id_group(
            currency, address
        )

        query = (
            "SELECT cluster_id FROM address WHERE "
            "address_id_group = %s AND address_id = %s "
        )
        result = await self.execute_async(
            currency, "transformed", query, [address_id_group, address_id]
        )
        result = one(result)
        if not result:
            raise DBInconsistencyException(
                f"address {address} in {currency} with id "
                f"{address_id} and id_group {address_id_group} "
                f"not found in address table"
            )
        return result["cluster_id"]

    async def list_address_links(
        self,
        currency,
        address,
        neighbor,
        min_height=None,
        max_height=None,
        order=None,
        token_currency=None,
        page=None,
        pagesize=None,
    ):
        return await self.list_links(
            currency,
            NodeType.ADDRESS,
            address,
            neighbor,
            min_height=min_height,
            max_height=max_height,
            order=order,
            token_currency=token_currency,
            page=page,
            pagesize=pagesize,
        )

    async def list_entity_links(
        self,
        currency,
        address,
        neighbor,
        min_height=None,
        max_height=None,
        order=None,
        token_currency=None,
        page=None,
        pagesize=None,
    ):
        return await self.list_links(
            currency,
            NodeType.CLUSTER,
            address,
            neighbor,
            min_height=min_height,
            max_height=max_height,
            order=order,
            token_currency=token_currency,
            page=page,
            pagesize=pagesize,
        )

    async def list_links(
        self,
        currency,
        node_type: NodeType,
        id,
        neighbor,
        min_height=None,
        max_height=None,
        order=None,
        token_currency=None,
        page=None,
        pagesize=None,
    ):
        ascending = order == "asc"

        if node_type == NodeType.ADDRESS:
            src_node = await self.get_address(currency, id)
            dst_node = await self.get_address(currency, neighbor)
        else:
            src_node = await self.get_entity(currency, id)
            dst_node = await self.get_entity(currency, neighbor)

        if src_node is None:
            raise nodeNotFoundException(currency, node_type, id)

        if dst_node is None:
            raise nodeNotFoundException(currency, node_type, neighbor)

        is_outgoing = src_node["no_outgoing_txs"] < dst_node["no_incoming_txs"]

        if is_outgoing:
            first = id
            second = neighbor
            is_outgoing = True
            first_value = "input_value"
            second_value = "output_value"
        else:
            first = neighbor
            second = id
            is_outgoing = False
            first_value = "output_value"
            second_value = "input_value"

        if token_currency is not None:
            include_assets = [token_currency.upper()]
        else:
            if is_eth_like(currency):
                token_config = self.get_token_configuration(currency)
                include_assets = list(token_config.keys())
                include_assets.append(currency.upper())
            else:
                include_assets = [currency.upper()]

        (
            tx_id_lower_bound,
            tx_id_upper_bound,
        ) = await self.resolve_tx_id_range_by_block(currency, min_height, max_height)

        # Get the transaction ID range overlap for both nodes
        src_first_tx_id = src_node["first_tx_id"]
        src_last_tx_id = src_node["last_tx_id"]
        dst_first_tx_id = dst_node["first_tx_id"]
        dst_last_tx_id = dst_node["last_tx_id"]
        node_tx_id_lower_bound = max(src_first_tx_id, dst_first_tx_id)
        node_tx_id_upper_bound = min(src_last_tx_id, dst_last_tx_id)

        tx_id_lower_bound = max(
            tx_id_lower_bound or node_tx_id_lower_bound, node_tx_id_lower_bound
        )
        tx_id_upper_bound = min(
            tx_id_upper_bound or node_tx_id_upper_bound, node_tx_id_upper_bound
        )

        final_results = []
        requested_pagesize = pagesize or SMALL_PAGE_SIZE
        fetch_size = max(FETCH_SIZE_MIN, requested_pagesize)

        while len(final_results) < requested_pagesize:
            # Use the efficient fetch_size for database calls
            fs_it = fetch_size
            results1_raw, new_pages, finished = await self.list_address_txs_ordered(
                network=currency,
                node_type=node_type,
                id=first,
                tx_id_lower_bound=tx_id_lower_bound,
                tx_id_upper_bound=tx_id_upper_bound,
                is_outgoing=is_outgoing,
                include_assets=include_assets,
                ascending=ascending,
                page=page,
                fetch_size=max(FETCH_SIZE_MIN, fs_it),
            )
            final_page1 = new_pages[-1] if new_pages and not finished else None

            self.logger.debug(f"results1 {len(results1_raw)} {final_page1}")

            tx_id = "transaction_id" if is_eth_like(currency) else "tx_id"

            first_tx_ids = (
                [(row[tx_id], row["tx_reference"]) for row in results1_raw]
                if is_eth_like(currency)
                else [(row[tx_id], None) for row in results1_raw]
            )

            assets = set([currency.upper()])
            if is_eth_like(currency):
                for row in results1_raw:
                    assets.add(row["currency"])

            assets = list(assets)

            results2, _, _ = await self.list_address_txs_ordered(
                network=currency,
                node_type=node_type,
                id=second,
                tx_id_lower_bound=None,
                tx_id_upper_bound=None,
                # is_outgoing=not is_outgoing,
                # fetch both directions regardless, this is needed since for
                # some transactions the total flow is an inflow but it is an
                # outflowat the same time. If we would not fetch both
                # directions we would miss these transactions.
                is_outgoing=None,
                include_assets=assets,
                ascending=ascending,
                tx_ids=first_tx_ids,  # limit second set by tx ids of first set
                page=None,
                fetch_size=len(first_tx_ids),
            )
            self.logger.debug(f"results2 {len(results2)}")

            if is_eth_like(currency):
                results2 = await self.normalize_address_transactions(currency, results2)
            else:
                results1 = {row[tx_id]: row for row in results1_raw}
                tx_ids = [row[tx_id] for row in results2]
                txs = await self.list_txs_by_ids(currency, tx_ids)

                if len(results2) != len(txs):
                    raise DBInconsistencyException(
                        "result sets for txs intersection not equal"
                    )

                # merge address_transactions and raw transaction sets
                for row, tx in zip(results2, txs):
                    row[second_value] = row["value"]
                    row[first_value] = results1[row[tx_id]]["value"]

                    for k, v in tx.items():
                        row[k] = v

            if is_eth_like(currency):
                # TODO probably this check is no longer necessary
                # since we filtered on tx_ref level already
                # in list_address_txs_ordered
                if node_type == NodeType.CLUSTER:
                    neighbor = dst_node["root_address"]
                    id = src_node["root_address"]
                # Token/Trace transactions might not be between the requested
                # nodes so only keep the relevant ones
                before = len(results2)
                results2 = [
                    tx
                    for tx in results2
                    if tx["to_address"] == neighbor and tx["from_address"] == id
                ]
                self.logger.info(f"pruned {before - len(results2)}")
            final_results.extend(results2)

            if not is_eth_like(currency):
                # make sure id is in inputs and neighbor in outputs
                if node_type == NodeType.ADDRESS:

                    async def has_address_in_ios(address, ios):
                        if ios is None and address == "coinbase":
                            return True
                        if ios is None:
                            return False

                        addresses_io = sum([x.address or [] for x in ios], [])

                        return address in addresses_io

                    final_results = [
                        fr
                        for fr in final_results
                        if await has_address_in_ios(id, fr["inputs"])
                        and await has_address_in_ios(neighbor, fr["outputs"])
                    ]
                else:

                    def get_address_from_io(x):
                        address_wrapped = x.address
                        if address_wrapped is None:
                            return None

                        assert len(address_wrapped) == 1
                        return address_wrapped[0]

                    async def has_cluster_id_in_ios(cluster_id, ios):
                        addresses_io = [get_address_from_io(x) for x in ios]
                        addresses_io = [x for x in addresses_io if x is not None]
                        # get cluster ids for all the addresses
                        cluster_ids_io = [
                            await self.get_address_entity_id(currency, address)
                            for address in addresses_io
                        ]

                        return cluster_id in cluster_ids_io

                    final_results = [
                        fr
                        for fr in final_results
                        if await has_cluster_id_in_ios(id, fr["inputs"])
                        and await has_cluster_id_in_ios(neighbor, fr["outputs"])
                    ]

            # Check if we have enough results after all filtering
            if len(final_results) >= requested_pagesize:
                # Truncate to exact requested size and get page token from last result
                final_results = final_results[:requested_pagesize]
                # now we know which is the last item from results1 that made it into final_results
                # so we can use that to get the page token from the last result
                last_row = final_results[-1]
                result1_to_page_mapping = {
                    (
                        row[tx_id],
                        row["tx_reference"] if is_eth_like(currency) else None,
                    ): page
                    for row, page in zip(results1_raw, new_pages)
                }
                page = result1_to_page_mapping[
                    (
                        last_row[tx_id],
                        last_row["tx_reference"] if is_eth_like(currency) else None,
                    )
                ]
                self.logger.debug(
                    f"early termination with {len(final_results)} results, page: {page}"
                )
                break

            page = new_pages[-1] if new_pages else None
            self.logger.debug(f"next page {page}")
            if page is None:
                break

        self.logger.debug(f"final_results len {len(final_results)}")

        return final_results, str(page) if page is not None else None

    async def list_matching_addresses(self, currency, expression, limit=10):
        prefix_lengths = self.get_prefix_lengths(currency)
        expression_orginal = expression

        postprocess_address = identity1
        currency_for_prefixscrub = currency
        if currency == "trx":
            postprocess_address = try_partial_tron_to_partial_evm
            currency_for_prefixscrub = "eth"

        expression = postprocess_address(expression)

        if len(expression) < prefix_lengths["address"]:
            return []
        norm = identity2

        prefix = self.scrub_prefix(currency_for_prefixscrub, expression)
        prefix = prefix[: prefix_lengths["address"]]

        if is_eth_like(currency):
            if not is_hexadecimal(expression):
                return []

            # eth addresses are case insensitive
            expression = expression.lower()
            norm = address_to_user_format
            prefix = prefix.upper()

        else:
            expression = expression_orginal
        rows = []

        if len(prefix) < 1:
            return []

        async def collect(query, params):
            result = await self.execute_async(currency, "transformed", query, params)
            rows.extend([norm(currency, row["address"]) for row in result.current_rows])

        # Use the expression that would actually be stored in the database
        query_expression = (
            expression_orginal if not is_eth_like(currency) else expression
        )

        upper_bound = create_upper_bound(
            query_expression, is_string_not_hex=not is_eth_like(currency)
        )

        if is_eth_like(currency) and len(query_expression) % 2 != 0:
            query_expression = query_expression + "0"

        if is_eth_like(currency) and len(upper_bound) % 2 != 0:
            upper_bound = upper_bound + "0"

        query = (
            "SELECT address FROM address_ids_by_address_prefix "
            "WHERE address_prefix = %s AND address >= %s AND address < %s"
            "LIMIT %s"
        )
        if is_eth_like(currency):
            params = [
                prefix,
                hex_str_to_bytes(strip_0x(query_expression)),
                hex_str_to_bytes(strip_0x(upper_bound)),
                limit,
            ]
        else:
            params = [prefix, query_expression, upper_bound, limit]

        await collect(query, params)

        if len(rows) < limit:
            remaining_limit = limit - len(rows)
            query = (
                "SELECT address FROM new_addresses "
                "WHERE address_prefix = %s AND address >= %s AND address < %s LIMIT %s"
            )
            if is_eth_like(currency):
                params = [
                    prefix,
                    hex_str_to_bytes(strip_0x(query_expression)),
                    hex_str_to_bytes(strip_0x(upper_bound)),
                    remaining_limit,
                ]
            else:
                params = [prefix, query_expression, upper_bound, remaining_limit]
            if self.parameters[currency]["use_delta_updater_v1"]:
                try:
                    await collect(query, params)
                except InvalidRequest as e:
                    if "new_addresses" not in str(e):
                        raise e
            rows = sorted(rows)
        return rows

    @eth
    async def get_entity(self, currency, entity):
        entity_id_group = self.get_id_group(currency, entity)
        entity = int(entity)
        query = "SELECT * FROM cluster WHERE cluster_id_group = %s AND cluster_id = %s "
        result = await self.execute_async(
            currency, "transformed", query, [entity_id_group, entity]
        )
        result = one(result)
        if not result:
            raise ClusterNotFoundException(currency, entity)
        return (await self.finish_entities(currency, [result]))[0]

    @eth
    async def list_entities(
        self, currency, ids, page=None, pagesize=None, fields=["*"]
    ):
        fetch_size = min(pagesize or SMALL_PAGE_SIZE, SMALL_PAGE_SIZE)
        paging_state = page_from_hex(page)
        flds = ",".join(fields)
        query = f"SELECT {flds} FROM cluster"
        has_ids = isinstance(ids, list)
        if has_ids:
            query += " WHERE cluster_id_group = %s AND cluster_id = %s"
            params = [[self.get_id_group(currency, id), id] for id in ids]
            result = await self.concurrent_with_args(
                currency, "transformed", query, params
            )
            paging_state = None
        else:
            result = await self.execute_async(
                currency,
                "transformed",
                query,
                paging_state=paging_state,
                fetch_size=fetch_size,
            )
            paging_state = result.paging_state
            result = result.current_rows

        with_txs = "*" in fields or "first_tx_id" in fields or "last_tx_id" in fields
        return await self.finish_entities(currency, result, with_txs), to_hex(
            paging_state
        )

    @eth
    async def list_entity_addresses(self, currency, entity, page=None, pagesize=None):
        paging_state = page_from_hex(page)
        entity_id_group = self.get_id_group(currency, entity)
        entity = int(entity)
        query = (
            "SELECT address_id FROM cluster_addresses "
            "WHERE cluster_id_group = %s AND cluster_id = %s"
        )
        fetch_size = min(pagesize or SMALL_PAGE_SIZE, SMALL_PAGE_SIZE)
        results = await self.execute_async(
            currency,
            "transformed",
            query,
            [entity_id_group, entity],
            paging_state=paging_state,
            fetch_size=fetch_size,
        )
        if results is None:
            return []

        params = [
            (self.get_id_group(currency, row["address_id"]), row["address_id"])
            for row in results.current_rows
        ]
        query = "SELECT * FROM address WHERE address_id_group = %s and address_id = %s"
        result = await self.concurrent_with_args(currency, "transformed", query, params)

        return await self.finish_addresses(currency, result), to_hex(
            results.paging_state
        )

    async def list_neighbors(
        self, currency, id, is_outgoing, node_type: NodeType, targets, page, pagesize
    ):
        pagesize = min(pagesize or SMALL_PAGE_SIZE, SMALL_PAGE_SIZE)

        orig_node_type = node_type
        orig_id = id
        if node_type == NodeType.ADDRESS:
            id = await self.get_address_id(currency, id)
            if id is None:
                # check if new address exists
                if await self.get_new_address(currency, orig_id):
                    return [], None
                raise AddressNotFoundException(currency, orig_id)

        elif node_type == NodeType.CLUSTER:
            id = int(id)
            if is_eth_like(currency):
                node_type = NodeType.ADDRESS

        if is_outgoing:
            direction, this, that = ("outgoing", "src", "dst")
        else:
            direction, this, that = ("incoming", "dst", "src")

        id_group = self.get_id_group(currency, id)
        base_parameters = [id_group, id]
        has_targets = isinstance(targets, list)
        sec_condition = ""
        if is_eth_like(currency):
            secondary_id_group = await self.get_id_secondary_group_eth(
                currency, f"address_{direction}_relations", id_group
            )
            sec_in = self.sec_in(secondary_id_group)
            sec_condition = f" AND {this}_address_id_secondary_group in %s"
            base_parameters.append(sec_in)

        basequery = (
            f"SELECT * FROM {node_type}_{direction}_relations WHERE "
            f"{this}_{node_type}_id_group = %s AND "
            f"{this}_{node_type}_id = %s {sec_condition}"
        )
        params = base_parameters.copy()
        if has_targets:
            if len(targets) == 0:
                return None, None

            query = basequery.replace("*", f"{that}_{node_type}_id")
            targets = ValueSequence(targets)
            query += f" AND {that}_{node_type}_id in %s"
            params.append(targets)
        else:
            query = basequery
        fetch_size = min(pagesize or BIG_PAGE_SIZE, BIG_PAGE_SIZE)
        paging_state = page_from_hex(page)
        results = await self.execute_async(
            currency,
            "transformed",
            query,
            params,
            paging_state=paging_state,
            fetch_size=fetch_size,
        )
        paging_state = results.paging_state
        results = results.current_rows
        if has_targets:
            params = []
            query = basequery + f" AND {that}_{node_type}_id = %s"
            for row in results:
                p = base_parameters.copy()
                p.append(row[f"{that}_{node_type}_id"])
                params.append(p)
            results = await self.concurrent_with_args(
                currency, "transformed", query, params
            )

        if orig_node_type == NodeType.CLUSTER and is_eth_like(currency):
            for neighbor in results:
                neighbor["address_id"] = neighbor[that + "_cluster_id"] = neighbor[
                    that + "_address_id"
                ]

        if orig_node_type == NodeType.ADDRESS:
            ids = [row[that + "_address_id"] for row in results]
            addresses = await self.get_addresses_by_ids(
                currency, ids, address_only=True
            )

            if len(addresses) != len(ids):
                address_ids = [address["address"] for address in addresses]
                self.log_missing(address_ids, ids, node_type, query, params)

            for row, address in zip(results, addresses):
                row[that + "_address"] = address["address"]

        field = "value" if is_eth_like(currency) else "estimated_value"
        for neighbor in results:
            neighbor["value"] = self.markup_currency(currency, neighbor[field])
            if "token_values" in neighbor and neighbor["token_values"] is not None:
                neighbor["token_values"] = {
                    k: self.markup_currency(currency, v)
                    for k, v in neighbor["token_values"].items()
                }

        if is_eth_like(currency):
            for row in results:
                row["address_id"] = row[that + "_address_id"]

        return results, to_hex(paging_state)

    @eth
    async def get_spending_txs(self, currency, tx_hash, io_index):
        if not self.parameters[currency]["tx_graph_available"]:
            # for value err msg is visible to the user.
            raise BadUserInputException(
                f"{currency} does not yet support transaction linking."
            )
        prefix = self.get_prefix_lengths(currency)
        if isinstance(io_index, int):
            query = (
                "SELECT * from transaction_spending where "
                "spending_tx_prefix=%s and spending_tx_hash=%s "
                "and spending_input_index=%s"
            )
            params = [tx_hash[: prefix["tx"]], bytearray.fromhex(tx_hash), io_index]
        else:
            query = (
                "SELECT * from transaction_spending where "
                "spending_tx_prefix=%s and spending_tx_hash=%s"
            )
            params = [tx_hash[: prefix["tx"]], bytearray.fromhex(tx_hash)]

        result = await self.execute_async(currency, "raw", query, params)
        return result

    async def get_spending_txs_eth(self, currency, tx_hash, io_index):
        # we raise a value error here,
        # that makes the error msg visible to the user.
        raise BadUserInputException(
            f"Currency {currency} does not support transaction level linking"
        )

    @eth
    async def get_spent_in_txs(self, currency, tx_hash, io_index):
        if not self.parameters[currency]["tx_graph_available"]:
            # for value err msg is visible to the user.
            raise BadUserInputException(
                f"{currency} does not yet support transaction linking."
            )
        prefix = self.get_prefix_lengths(currency)
        if isinstance(io_index, int):
            query = (
                "SELECT * from transaction_spent_in where "
                "spent_tx_prefix=%s and spent_tx_hash=%s "
                "and spent_output_index=%s"
            )
            params = [tx_hash[: prefix["tx"]], bytearray.fromhex(tx_hash), io_index]
        else:
            query = (
                "SELECT * from transaction_spent_in where "
                "spent_tx_prefix=%s and spent_tx_hash=%s"
            )
            params = [tx_hash[: prefix["tx"]], bytearray.fromhex(tx_hash)]

        result = await self.execute_async(currency, "raw", query, params)
        return result

    async def get_spent_in_txs_eth(self, currency, tx_hash, io_index):
        # we raise a value error here,
        # that makes the error msg visible to the user.
        raise BadUserInputException(
            f"Currency {currency} does not support transaction level linking"
        )

    @eth
    def get_tx(self, currency, tx_hash):
        return self.get_tx_by_hash(currency, tx_hash)

    @eth
    async def list_token_txs(self, currency, tx_hash, log_index=None):
        return []

    async def list_token_txs_eth(self, currency, tx_hash, log_index=None):
        tx = await self.get_tx(currency, tx_hash)
        return await self.fetch_token_transactions(currency, tx, log_index)

    async def fetch_token_transaction(self, currency, tx, log_index):
        r = await self.fetch_token_transactions(currency, tx, log_index)
        if len(r) != 1:
            raise NotFoundException(
                f"Found {len(r)} logs for token tx {tx['tx_hash']}:{log_index}"
            )
        return r[0]

    async def fetch_token_transactions(self, currency, tx, log_index=None):
        transfer_topic = bytes_from_hex(
            "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
        )
        token_tx_logs = await self.get_logs_in_block_eth(
            currency,
            tx["block_id"],
            topic=transfer_topic,
            log_index=log_index,
            tx_hash=tx["tx_hash"],
        )
        supported_tokens = {
            v["token_address"]: v
            for v in self.get_token_configuration(currency).values()
        }

        logs_to_decode = [
            tt
            for tt in token_tx_logs.current_rows
            if tt["address"] in supported_tokens and tt["tx_hash"] == tx["tx_hash"]
        ]
        decoded_token_txs = decode_logs_db(logs_to_decode)

        return [
            build_token_tx(
                supported_tokens[log["address"]]["currency_ticker"], tx, token_tx, log
            )
            for (token_tx, log) in decoded_token_txs
        ]

    async def fetch_transaction_trace(
        self, currency, tx, trace_index, get_first_trace=False
    ):
        r = await self.get_traces_in_block(
            currency, tx["block_id"], trace_index, tx_hash=tx["tx_hash"]
        )
        result = r.current_rows

        if len(result) == 0:
            raise NotFoundException(
                f"Found {len(result)} trace in tx {tx['tx_hash']}:{trace_index}"
            )

        if get_first_trace:
            return result[0]

        if len(result) > 1:
            raise NotFoundException(
                f"Found {len(result)} trace in tx {tx['tx_hash']}:{trace_index}"
            )

        return result[0]

    async def fetch_transaction_traces(self, currency, tx):
        result = await self.get_traces_in_block(
            currency, tx["block_id"], tx_hash=tx["tx_hash"]
        )
        return result.current_rows

    async def fetch_transaction_logs(self, currency, tx):
        result = await self.get_logs_in_block_eth(
            currency, tx["block_id"], tx_hash=tx["tx_hash"]
        )
        return result.current_rows

    async def get_logs_in_block_eth(
        self, currency, block_id, topic=None, log_index=None, tx_hash=None
    ):
        block_group = self.get_block_id_group(currency, block_id)
        query = "SELECT * from log where block_id_group=%s and block_id=%s"
        params = [block_group, block_id]

        if topic is not None:
            query += " and topic0=%s"
            params += [topic]

        if log_index is not None:
            query += " and log_index=%s"
            params += [log_index]

        if tx_hash is not None:
            query += " and tx_hash=%s ALLOW FILTERING"
            params += [tx_hash]

        return await self.execute_async(currency, "raw", query, params)

    async def get_traces_in_block(
        self, currency, block_id, trace_index=None, tx_hash=None
    ):
        block_group = self.get_block_id_group(currency, block_id)
        query = "SELECT * from trace where block_id_group=%s and block_id=%s"

        params = [block_group, block_id]
        if trace_index is not None:
            query += " and trace_index=%s"
            params += [trace_index]

        if tx_hash is not None:
            query += " and tx_hash=%s ALLOW FILTERING"
            params += [tx_hash]

        return await self.execute_async(currency, "raw", query, params)

    async def list_matching_txs(
        self, currency, expression, limit, include_sub_tx_identifiers: bool = True
    ):
        expression_original = expression
        expression = expression.split("_")[0]

        prefix_lengths = self.get_prefix_lengths(currency)

        # should be safe for btc txs too. They are hex encoded.
        # so 0x should never be content. For base58 encoded btc adresses
        # there also no 0 character used.
        expression = strip_0x(expression)

        if not is_hexadecimal(expression):
            return []

        if len(expression) < prefix_lengths["tx"]:
            return []

        expression_normalized = expression.lower()

        lower_bound = expression_normalized
        upper_bound = create_upper_bound(expression_normalized)

        if len(lower_bound) % 2 != 0:
            lower_bound = lower_bound + "0"

        if len(upper_bound) % 2 != 0:
            upper_bound = upper_bound + "0"

        if is_eth_like(currency):
            prefix = expression[: prefix_lengths["tx"]].upper()
            kind = "transformed"
            key = "transaction"

            query = (
                "SELECT transaction from transaction_ids_by_transaction_"
                "prefix WHERE transaction_prefix = %s AND transaction >= %s AND transaction < %s LIMIT %s"
            )
            params = [
                prefix,
                hex_str_to_bytes(strip_0x(lower_bound)),
                hex_str_to_bytes(strip_0x(upper_bound)),
                limit,
            ]
        else:
            prefix = expression[: prefix_lengths["tx"]]
            kind = "raw"
            key = "tx_hash"

            query = (
                "SELECT tx_hash from transaction_by_tx_prefix WHERE "
                "tx_prefix = %s AND tx_hash >= %s AND tx_hash < %s LIMIT %s"
            )
            params = [
                prefix,
                hex_str_to_bytes(strip_0x(lower_bound)),
                hex_str_to_bytes(strip_0x(upper_bound)),
                limit,
            ]

        result = await self.execute_async(currency, kind, query, params)

        if result.is_empty():
            return []

        # fix leading zero handling, assumption all tx hashes are 32 bytes
        # which is true for btc-like currencies and ethereum
        rows = [row[key].rjust(32, b"\x00").hex() for row in result.current_rows]

        # also show internal txs and token txs when only on result is left
        if is_eth_like(currency) and len(rows) == 1 and include_sub_tx_identifiers:
            tx_hash = bytearray.fromhex(rows[0])
            tx = await self.get_tx_by_hash_eth(currency, tx_hash)
            token_txs = await self.fetch_token_transactions(currency, tx)
            # smallest trace is equivalent to tx itself so filter
            traces = (await self.fetch_transaction_traces(currency, tx))[1:]
            ids = list(
                {get_tx_identifier(x, currency) for x in token_txs}.union(
                    {
                        get_tx_identifier(x, currency, type_overwrite="internal")
                        for x in traces
                    }
                )
            )
            rows.extend(ids)

        if "_" in expression_original:
            postfix = expression_original.split("_")[1]
            expression_normalized_postfix = (
                expression_normalized + "_" + postfix.lower()
            )
            len_expression = len(expression_normalized_postfix)
            rows = [
                row
                for row in rows
                if row[:len_expression].lower() == expression_normalized_postfix
            ]

        return rows

    def scrub_prefix(self, currency, expression):
        if isinstance(expression, bytes):
            expression = bytes_to_hex(expression)

        if currency == "eth":
            expression = strip_0x(expression)

        if currency not in self.parameters:
            raise NetworkNotFoundException(currency)

        bech32_prefix = self.parameters[currency].get("bech_32_prefix", "")
        return (
            expression[len(bech32_prefix) :]
            if expression.startswith(bech32_prefix)
            else expression
        )

    @eth
    async def list_txs_by_hashes(self, currency, hashes, include_token_txs=False):
        prefix = self.get_prefix_lengths(currency)
        params = [[hash[: prefix["tx"]], bytearray.fromhex(hash)] for hash in hashes]
        statement = (
            "SELECT tx_id from transaction_by_tx_prefix where "
            "tx_prefix=%s and tx_hash=%s"
        )
        result = await self.concurrent_with_args(currency, "raw", statement, params)
        ids = (tx["tx_id"] for tx in result)
        return await self.list_txs_by_ids(
            currency, ids, include_token_txs=include_token_txs
        )

    @eth
    async def get_tx_by_hash(self, currency, tx_hash: bytes):
        prefix = self.get_prefix_lengths(currency)
        try:
            params = [tx_hash[: prefix["tx"]], bytearray.fromhex(tx_hash)]
        except ValueError:
            raise BadUserInputException(
                f"{tx_hash} does not look like a valid transaction hash."
            )
        statement = (
            "SELECT tx_id from transaction_by_tx_prefix where "
            "tx_prefix=%s and tx_hash=%s"
        )
        result = await self.execute_async(currency, "raw", statement, params)
        result = one(result)
        if not result:
            raise TransactionNotFoundException(currency, tx_hash)
        result = await self.get_tx_by_id(currency, result["tx_id"])
        if not result:
            raise DBInconsistencyException(
                f"transaction {tx_hash} with id {result['tx_id']} in network "
                f"{currency} not found"
            )
        return result

    @eth
    async def list_txs_by_ids(
        self, currency, ids, filter_empty=True, include_token_txs=False
    ):
        params = ([self.get_tx_id_group(currency, id), id] for id in ids)
        statement = "SELECT * FROM transaction WHERE tx_id_group = %s and tx_id = %s"
        return await self.concurrent_with_args(
            currency, "raw", statement, params, filter_empty=filter_empty
        )

    @eth
    async def get_tx_by_id(self, currency, id):
        params = [self.get_tx_id_group(currency, id), id]
        statement = "SELECT * FROM transaction WHERE tx_id_group = %s and tx_id = %s"
        return (await self.execute_async(currency, "raw", statement, params)).one()

    async def finish_entities(self, currency, rows, with_txs=True):
        aws = [self.finish_entity(currency, row, with_txs=with_txs) for row in rows]
        return await asyncio.gather(*aws)

    @eth
    async def finish_entity(self, currency, row, with_txs=True):
        a = await self.get_addresses_by_ids(
            currency, [row["cluster_id"]], address_only=True
        )
        row["root_address"] = a[0]["address"]
        return await self.finish_address(currency, row, with_txs)

    async def finish_entity_eth(self, currency, row, with_txs=True):
        row["root_address"] = row["address"]
        return await self.finish_address(currency, row, with_txs)

    async def finish_addresses(self, currency, rows, with_txs=True):
        aws = [self.finish_address(currency, row, with_txs=with_txs) for row in rows]
        return await asyncio.gather(*aws)

    @eth
    async def finish_address(self, currency, row, with_txs=True):
        row["total_received"] = self.markup_currency(currency, row["total_received"])
        row["total_spent"] = self.markup_currency(currency, row["total_spent"])

        await self.add_balance(currency, row)

        if not with_txs:
            return row

        aws = [
            self.get_tx_by_id(currency, id)
            for id in [row["first_tx_id"], row["last_tx_id"]]
        ]
        [tx1, tx2] = await asyncio.gather(*aws)

        if not tx1 or not tx2:
            raise DBInconsistencyException(
                f"first or last transaction not found as referenced by {row}"
            )

        row["first_tx"] = TxSummary(
            tx_hash=tx1["tx_hash"], timestamp=tx1["timestamp"], height=tx1["block_id"]
        )

        row["last_tx"] = TxSummary(
            tx_hash=tx2["tx_hash"], timestamp=tx2["timestamp"], height=tx2["block_id"]
        )

        if "address" in row:
            is_dirty = await self.is_address_dirty(currency, row["address"])
            row["status"] = "dirty" if is_dirty else "clean"

        return row

    async def finish_address_eth(self, currency, row, with_txs=True):
        row["cluster_id"] = row["address_id"]
        row["total_received"] = self.markup_currency(currency, row["total_received"])
        row["total_spent"] = self.markup_currency(currency, row["total_spent"])

        tr = row["total_tokens_received"]
        if tr is not None:
            row["total_tokens_received"] = {
                k: self.markup_currency(currency, v) for k, v in tr.items()
            }

        ts = row["total_tokens_spent"]
        if ts is not None:
            row["total_tokens_spent"] = {
                k: self.markup_currency(currency, v) for k, v in ts.items()
            }

        await self.add_balance(currency, row)

        if not with_txs:
            return row

        aws = [
            self.get_tx_by_id(currency, id)
            for id in [row["first_tx_id"], row["last_tx_id"]]
        ]

        [tx1, tx2] = await asyncio.gather(*aws)

        if not tx1 or not tx2:
            raise DBInconsistencyException(
                f"first or last transaction not found as referenced by {row}"
            )

        row["first_tx"] = TxSummary(
            tx_hash=tx1["tx_hash"],
            timestamp=tx1["block_timestamp"],
            height=tx1["block_id"],
        )

        row["last_tx"] = TxSummary(
            tx_hash=tx2["tx_hash"],
            timestamp=tx2["block_timestamp"],
            height=tx2["block_id"],
        )

        is_dirty = await self.is_address_dirty(currency, row["address"])
        row["status"] = "dirty" if is_dirty else "clean"

        return row

    async def is_address_dirty(self, currency, address):
        if not self.parameters[currency]["use_delta_updater_v1"]:
            return False

        prefix = self.scrub_prefix(currency, address)
        if is_eth_like(currency):
            prefix = prefix.upper()
        prefix_length = self.get_prefix_lengths(currency)["address"]
        query = (
            "SELECT address FROM dirty_addresses "
            "WHERE address_prefix = %s AND address = %s"
        )
        try:
            result = await self.execute_async(
                currency, "transformed", query, [prefix[:prefix_length], address]
            )

            result = one(result)
            if result:
                return True
        except InvalidRequest as e:
            if "dirty_addresses" not in str(e):
                raise e

        return False

    @eth
    async def add_balance(self, currency, row):
        row["balance"] = row["total_received"].value - row["total_spent"].value

    async def add_balance_eth(self, currency, row):
        token_config = self.get_token_configuration(currency)
        token_currencies = list(token_config.keys())
        balance_currencies = [currency.upper()] + token_currencies

        balance_provider = self.get_balance_provider(currency)

        results = None
        if balance_provider is not None:
            # load balance from alternative provider.
            try:
                results = await balance_provider(currency, row["address"], token_config)
            except Exception as e:
                self.logger.warning(
                    f"Could not fetch balances over alternative provider: {e}"
                )

        if results is None:
            # load results from gs database
            if "address_id_group" not in row:
                row["address_id_group"] = self.get_id_group(currency, row["address_id"])
            query = (
                "SELECT balance from balance where address_id=%s "
                "and address_id_group=%s "
                "and currency=%s"
            )

            results = {
                c: one(
                    await self.execute_async(
                        currency,
                        "transformed",
                        query,
                        [row["address_id"], row["address_id_group"], c],
                    )
                )
                for c in balance_currencies
            }

        if results[currency.upper()] is None:
            results[currency.upper()] = {
                "balance": row["total_received"].value - row["total_spent"].value
            }
        row["balance"] = results[currency.upper()]["balance"]

        if currency == "trx":
            # our self compute balances can get negative for now
            # since we have not implemented all special tx types
            # in our logic yet
            row["balance"] = max(row["balance"], 0)

        # TODO: Some accounts have negative balances, this does not make sense.
        # for now we cap with 0 in case of negative
        # Exp. for now is that we either lost some token txs somewhere or
        # the events do not represent all transfers
        # (e.g. the initial distribution) was hardcoded in the token contract
        # so no events exist. Future solution to have accurate balances
        # would be to query the node directly
        token_balances = {
            c: max(b["balance"], 0)
            for c, b in results.items()
            if c in token_config and b is not None
        }
        row["token_balances"] = None if len(token_balances) == 0 else token_balances

    # ETH Variants

    def scrub_prefix_eth(self, currency, expression):
        # remove 0x prefix
        expression = bytes_to_hex(expression)
        if expression.startswith("0x"):
            return expression[2:]
        else:
            return expression

    async def get_block_eth(self, currency, height):
        block_group = self.get_block_id_group(currency, height)
        query = "SELECT * FROM block WHERE block_id_group = %s and block_id = %s"
        return (
            await self.execute_async(currency, "raw", query, [block_group, height])
        ).one()

    # entity = address_id

    async def get_entity_eth(self, currency, entity):
        # mockup entity by address
        id_group = self.get_id_group(currency, entity)
        query = "SELECT * FROM address WHERE address_id_group = %s AND address_id = %s"
        result = await self.execute_async(
            currency, "transformed", query, [id_group, entity]
        )
        result = one(result)
        if not result:
            return None

        entity = (await self.finish_entities(currency, [result]))[0]
        entity["cluster_id"] = entity["address_id"]
        entity["no_addresses"] = 1
        entity.pop("address", None)
        return entity

    async def list_entities_eth(
        self, currency, ids, page=None, pagesize=None, fields=["*"]
    ):
        fields = ["address_id" if i == "cluster_id" else i for i in fields]
        if "*" not in fields:
            fields += ["address_id", "address_id_group"]
        flds = ",".join(fields)
        query = f"SELECT {flds} FROM address"
        has_ids = isinstance(ids, list)
        if has_ids:
            query += " WHERE address_id_group = %s AND address_id = %s"
            params = [[self.get_id_group(currency, id), id] for id in ids]
            result = await self.concurrent_with_args(
                currency, "transformed", query, params
            )
            paging_state = None
        else:
            fetch_size = min(pagesize or SMALL_PAGE_SIZE, SMALL_PAGE_SIZE)
            result = await self.execute_async(
                currency,
                "transformed",
                query,
                paging_state=page_from_hex(page),
                fetch_size=fetch_size,
            )
            paging_state = result.paging_state
            result = result.current_rows

        with_txs = "*" in fields or "first_tx_id" in fields or "last_tx_id" in fields
        result = await self.finish_addresses(currency, result, with_txs)

        for address in result:
            address["cluster_id"] = address["address_id"]
            address["no_addresses"] = 1
        return result, to_hex(paging_state)

    async def get_address_entity_id_eth(self, currency, address):
        address_id = await self.get_address_id(currency, address)
        if address_id is None:
            raise AddressNotFoundException(currency, address, no_external_txs=True)
        return address_id

    async def get_id_secondary_group_eth(self, currency, table, id_group):
        column_prefix = ""
        if table == "address_incoming_relations":
            column_prefix = "dst_"
        elif table == "address_outgoing_relations":
            column_prefix = "src_"

        query = (
            f"SELECT max_secondary_id FROM {table}_"
            f"secondary_ids WHERE {column_prefix}address_id_group = %s"
        )
        result = (
            await self.execute_async(currency, "transformed", query, [id_group])
        ).one()
        return 0 if result is None else result["max_secondary_id"]

    async def list_address_txs_ordered(
        self,
        network: str,
        node_type: NodeType,
        id,
        tx_id_lower_bound: Optional[int],
        tx_id_upper_bound: Optional[int],
        is_outgoing: Optional[bool],
        include_assets: Sequence[Tuple[str, bool]],
        page: Optional[str],
        fetch_size: int,
        cols: Optional[Sequence[str]] = None,
        tx_ids: Optional[Sequence[Tuple[int, Optional[dict]]]] = None,
        ascending: bool = False,
    ) -> Tuple[Sequence[dict], List[Optional[str]]]:
        """Loads a address transactions in execution order
        it allows to only get out- or incoming transaction or only
        transactions of a certain asset (token), for a given address id

        Args:
            network (str): base currency / network
            node_type (str): NodeType (ADDRESS/CLUSTER)
            item_id (int): address/cluster id
            item_id_group (int): address/cluster id group
            tx_id_lower_bound (Optional[int]): tx id lower bound
            tx_id_upper_bound (Optional[int]): tx id upper bound
            is_outgoing (Optional[bool]): if True fetch only outgoing, if False
                fetch only incoming, if None fetch both directions
            include_assets (Sequence[Tuple[str, bool]]): a list of tuples with
                assets to include
            page (Optional[int]): a page id (tx id bound)
            fetch_size (int): how much to fetch per page
            cols (Optional[Sequence[str]], optional): which columns to select
                None means *
            tx_ids (Optional[Sequence[int]]): limit result to given tx_ids
            ascending (Optional[bool]): sort list ascending if True or
                descending if False
        """

        # get first and last tx id for the node
        if node_type == NodeType.ADDRESS:
            data = await self.get_address(network, id)
        elif node_type == NodeType.CLUSTER:
            data = await self.get_entity(network, id)
        else:
            raise BadUserInputException(
                f"Node type {node_type} not supported for address transactions"
            )

        first_tx = data.get("first_tx_id")
        last_tx = data.get("last_tx_id")

        def get_block_from_tx_id(id_):
            return id_ >> 32

        def get_secondary_id_group_from_block(block):
            return block // self.parameters[network]["block_bucket_size_address_txs"]

        def secondary_id_from_tx_id(tx_id):
            block = get_block_from_tx_id(tx_id)
            return get_secondary_id_group_from_block(block)

        try:
            if is_eth_like(network) and tx_ids is None:
                (page, secondary_id, offset) = (
                    page.split(":") if page is not None else (None, None, None)
                )
                page = int(page) if page is not None else None
                secondary_id = int(secondary_id) if secondary_id is not None else None
                offset = int(offset) if offset is not None else None
            else:
                (page, offset) = page.split(":") if page is not None else (None, None)
                page = int(page) if page is not None else None
                offset = int(offset) if offset is not None else None
                secondary_id = None
        except ValueError:
            raise BadUserInputException(f"Page {page} is not in the correct format")

        if node_type == NodeType.ADDRESS:
            item_id, item_id_group = await self.get_address_id_id_group(network, id)
        else:
            if is_eth_like(network):
                node_type = NodeType.ADDRESS
            item_id = id
            item_id_group = self.get_id_group(network, id)

        item_id_secondary_group = [0]
        if is_eth_like(network):
            sec_max = await self.get_id_secondary_group_eth(
                network, "address_transactions", item_id_group
            )
            sec_min = 0

            # restrict to first and last tx
            sec_min = max(sec_min, secondary_id_from_tx_id(first_tx))
            sec_max = min(sec_max, secondary_id_from_tx_id(last_tx))

            # restrict to border tx_id parameters given
            if tx_id_lower_bound:
                sec_min = max(sec_min, secondary_id_from_tx_id(tx_id_lower_bound))
            if tx_id_upper_bound:
                sec_max = min(sec_max, secondary_id_from_tx_id(tx_id_upper_bound))

            if tx_ids is None:
                # For pagination, we only need the current secondary_id
                if secondary_id is not None:
                    item_id_secondary_group = [secondary_id]
                else:
                    # Start with the appropriate secondary_id based on sort order
                    item_id_secondary_group = [sec_max if not ascending else sec_min]
            else:
                item_id_secondary_group = list(range(sec_min, sec_max + 1))

        directions = [is_outgoing] if is_outgoing is not None else [False, True]
        results = []
        pages_results = []  # each result gets a page so we can truncate and still know the page
        finished = False

        # Track the current secondary_id for eth-like networks
        current_secondary_id = (
            item_id_secondary_group[0]
            if is_eth_like(network) and tx_ids is None
            else None
        )

        """
            Keep retrieving pages until the demanded fetch_size is fulfilled
            or there are no more pages
        """
        while len(results) < fetch_size:
            fs_it = fetch_size - len(results)

            if not ascending:
                this_tx_id_lower_bound = tx_id_lower_bound
                if page is not None and tx_id_upper_bound is not None:
                    this_tx_id_upper_bound = min(page, tx_id_upper_bound)
                elif tx_id_upper_bound is not None:
                    this_tx_id_upper_bound = tx_id_upper_bound
                else:
                    this_tx_id_upper_bound = page
            else:
                this_tx_id_upper_bound = tx_id_upper_bound
                if page is not None and tx_id_lower_bound is not None:
                    this_tx_id_lower_bound = max(page, tx_id_lower_bound)
                elif tx_id_lower_bound is not None:
                    this_tx_id_lower_bound = tx_id_lower_bound
                else:
                    this_tx_id_lower_bound = page

            # prebuild useful conditions, dependent on loop
            has_upper_bound = this_tx_id_upper_bound is not None
            has_lower_bound = this_tx_id_lower_bound is not None

            fs_chunk = max(FETCH_SIZE_MIN, (offset or 0) + fetch_size)

            cql_stmt = build_select_address_txs_statement(
                network,
                node_type,
                cols,
                with_lower_bound=has_lower_bound,
                with_upper_bound=has_upper_bound,
                limit=fs_chunk if tx_ids is None else None,
                ascending=ascending,
                with_tx_id=(tx_ids is not None),
            )

            # Use current_secondary_id for eth-like networks without tx_ids
            if is_eth_like(network) and tx_ids is None:
                current_item_id_secondary_group = [current_secondary_id]
            else:
                current_item_id_secondary_group = item_id_secondary_group

            # We have create separate queries for the directions and assets
            # because in Cassandra the first cluster keys are direction and asset
            # If we did this in one query, we would potentially only progress in
            # one asset and direction wrt tx_id (our pagination key), but we want
            # results orderd by tx_id, so we need to query all directions and assets
            if self.parameters[network].get("block_bucket_size_address_txs") is None:
                params_chunks = [
                    {
                        "id": item_id,
                        "g_id": item_id_group,
                        "tx_id_lower_bound": this_tx_id_lower_bound,
                        "tx_id_upper_bound": this_tx_id_upper_bound,
                        "s_d_group": s_d_group,
                        "currency": asset,
                        "is_outgoing": is_outgoing,
                        "tx_id": tx_id,
                        "tx_ref": tx_ref,
                    }
                    for is_outgoing, asset, s_d_group, (tx_id, tx_ref) in product(
                        directions,
                        include_assets,
                        current_item_id_secondary_group,
                        [(0, None)] if tx_ids is None else tx_ids,
                    )
                ]
            else:
                if tx_ids:
                    secondary_ids_with_tx_ids = [
                        (
                            secondary_id_from_tx_id(tx_id),
                            tx_id,
                            tx_ref,
                        )
                        for (tx_id, tx_ref) in tx_ids
                    ]
                else:
                    secondary_ids_with_tx_ids = [
                        (
                            secondary_id,
                            0,
                            None,
                        )
                        for secondary_id in current_item_id_secondary_group
                    ]
                params_chunks = [
                    {
                        "id": item_id,
                        "g_id": item_id_group,
                        "tx_id_lower_bound": this_tx_id_lower_bound,
                        "tx_id_upper_bound": this_tx_id_upper_bound,
                        "s_d_group": s_d_group,
                        "currency": asset,
                        "is_outgoing": is_outgoing,
                        "tx_id": tx_id,
                        "tx_ref": tx_ref,
                    }
                    for is_outgoing, asset, (s_d_group, tx_id, tx_ref) in product(
                        directions,
                        include_assets,
                        secondary_ids_with_tx_ids,
                    )
                ]

            def tx_ref_match(a, b):
                return a[0] == b[0] and a[1] == b[1]

            async def fetch(stmt, params):
                res = await self.execute_async(network, "transformed", cql_stmt, params)
                if params["tx_ref"] is None:
                    return res

                # Filtering on tx_ids in the cql query was very slow once and killed
                # cassandra nodes the second time, so we filter here
                res.current_rows = [
                    r for r in res if tx_ref_match(r["tx_reference"], params["tx_ref"])
                ]
                return res

            aws = [fetch(cql_stmt, p) for p in params_chunks]

            tx_id_keys = "transaction_id" if is_eth_like(network) else "tx_id"
            # collect and merge results
            more_results, more_pages, more_offsets = merge_address_txs_subquery_results(
                self.logger,
                [r.current_rows for r in await asyncio.gather(*aws)],
                ascending,
                fs_it,
                tx_id_keys,
                fetched_limit=fs_chunk,
                page=page,
                offset=offset,
            )

            page = more_pages[-1] if more_pages else None
            offset = more_offsets[-1] if more_offsets else None

            self.logger.debug(f"tx_id_lower_bound {tx_id_lower_bound}")
            self.logger.debug(f"tx_id_upper_bound {tx_id_upper_bound}")

            more_results_mask = [  # True or False to filter out results that are not in the range
                (tx_id_lower_bound is None or r[tx_id_keys] >= tx_id_lower_bound)
                and (tx_id_upper_bound is None or r[tx_id_keys] <= tx_id_upper_bound)
                for r in more_results
            ]
            more_results = [
                r for r, mask in zip(more_results, more_results_mask) if mask
            ]
            more_pages = [p for p, mask in zip(more_pages, more_results_mask) if mask]
            more_offsets = [
                o for o, mask in zip(more_offsets, more_results_mask) if mask
            ]

            self.logger.debug(f"list tx ordered page {page}:{offset}")
            self.logger.debug(f"more_results len {len(more_results)}")

            results.extend(more_results)

            if tx_ids:
                # tx_ids case - no pagination needed
                break

            def get_next_secondary_id(
                current_secondary_id, ascending, sec_min, sec_max
            ):
                if not ascending:
                    next_secondary_id = current_secondary_id - 1
                    if next_secondary_id < sec_min:
                        return None
                    return next_secondary_id
                else:
                    next_secondary_id = current_secondary_id + 1
                    if next_secondary_id > sec_max:
                        return None
                    return next_secondary_id

            if is_eth_like(network):
                more_pages_results = [
                    f"{page}:{current_secondary_id}:{offset}"
                    if page is not None
                    else None
                    for page, offset in zip(more_pages, more_offsets)
                ]
            else:
                more_pages_results = [
                    f"{page}:{offset}" if page is not None else None
                    for page, offset in zip(more_pages, more_offsets)
                ]

            pages_results.extend(more_pages_results)

            if page is None:
                # Current secondary_id_group is exhausted
                if is_eth_like(network):
                    current_secondary_id = get_next_secondary_id(
                        current_secondary_id, ascending, sec_min, sec_max
                    )
                    if current_secondary_id is None:
                        finished = True
                        break

                    offset = None
                else:
                    # No more data expected for non-eth networks
                    finished = True
                    break

        return results, pages_results, finished

    async def list_txs_by_node_type_eth(
        self,
        currency,
        node_type,
        address,
        direction,
        min_height=None,
        max_height=None,
        order=None,
        token_currency=None,
        page=None,
        pagesize=None,
    ):
        ascending = order == "asc"

        if node_type == NodeType.CLUSTER:
            node_type = NodeType.ADDRESS
            address = await self.get_address_by_address_id(currency, address)

        if not token_currency:
            token_config = self.get_token_configuration(currency)
            include_assets = list(token_config.keys())
            include_assets.append(currency.upper())
        else:
            include_assets = [token_currency.upper()]

        first_tx_id, upper_bound = await self.resolve_tx_id_range_by_block(
            currency, min_height, max_height
        )

        fetch_size = min(pagesize or BIG_PAGE_SIZE, BIG_PAGE_SIZE)

        results, paging_states, finished = await self.list_address_txs_ordered(
            network=currency,
            node_type=node_type,
            id=address,
            tx_id_lower_bound=first_tx_id,
            tx_id_upper_bound=upper_bound,
            is_outgoing=(direction == "out" if direction is not None else None),
            include_assets=include_assets,
            ascending=ascending,
            page=page,
            fetch_size=fetch_size,
        )
        paging_state = paging_states[-1] if paging_states and not finished else None

        results = await self.normalize_address_transactions(currency, results)

        for row in results:
            row["value"] *= -1 if row["is_outgoing"] else 1

        return results, str(paging_state) if paging_state is not None else None

    async def normalize_address_transactions(self, currency, txs):
        use_legacy_log_index = self.parameters[currency]["use_legacy_log_index"]

        tx_ids = [tx["transaction_id"] for tx in txs]

        full_txs = {
            tx_id: tx_row
            for tx_id, tx_row in zip(
                tx_ids, await self.list_txs_by_ids(currency, tx_ids)
            )
        }
        for addr_tx in txs:
            # fix log index field with new tx_refstruct
            if not use_legacy_log_index:
                addr_tx["log_index"] = addr_tx["tx_reference"].log_index
                addr_tx["trace_index"] = addr_tx["tx_reference"].trace_index

            full_tx = full_txs[addr_tx["transaction_id"]]
            contract_creation = full_tx.get("contract_creation", None)

            addr_tx["contract_creation"] = contract_creation

            if addr_tx["log_index"] is not None:
                token_tx = await self.fetch_token_transaction(
                    currency, full_tx, addr_tx["log_index"]
                )

                addr_tx["to_address"] = token_tx["to_address"]
                addr_tx["from_address"] = token_tx["from_address"]
                addr_tx["currency"] = token_tx["currency"]
                addr_tx["token_tx_id"] = addr_tx["log_index"]
                addr_tx["type"] = "erc20"
                addr_tx["contract_creation"] = False
                value = token_tx["value"]

            elif currency == "trx" and addr_tx["trace_index"] is not None:
                trace = await self.fetch_transaction_trace(
                    currency, full_tx, addr_tx["trace_index"]
                )

                addr_tx["from_address"] = trace["caller_address"]
                addr_tx["to_address"] = trace["transferto_address"]
                addr_tx["type"] = "internal"
                value = trace["call_value"]
            elif currency == "eth" and addr_tx["trace_index"] is not None:
                trace = await self.fetch_transaction_trace(
                    currency, full_tx, addr_tx["trace_index"]
                )

                addr_tx["from_address"] = trace["from_address"]
                addr_tx["to_address"] = trace["to_address"]
                is_external = (
                    trace["trace_address"] is None
                    or trace["trace_address"].strip() == ""
                )

                # if they are the same, then we know it is external
                if is_external:
                    addr_tx["type"] = "external"
                else:
                    addr_tx["type"] = "internal"

                addr_tx["contract_creation"] = trace["trace_type"] == "create"
                addr_tx["input"] = trace["input"]
                addr_tx["input_parsed"] = parse_function_call(
                    trace["input"], function_call_signatures
                )
                value = trace["value"]
            else:
                addr_tx["to_address"] = full_tx["to_address"]
                addr_tx["from_address"] = full_tx["from_address"]
                addr_tx["currency"] = currency
                addr_tx["type"] = "external"
                addr_tx["input"] = full_tx["input"]
                addr_tx["input_parsed"] = parse_function_call(
                    full_tx["input"], function_call_signatures
                )
                value = full_tx["value"]

            addr_tx["tx_hash"] = full_tx["tx_hash"]
            addr_tx["height"] = full_tx["block_id"]

            addr_tx["timestamp"] = full_tx["block_timestamp"]
            addr_tx["value"] = value
            addr_tx.pop("log_index")

            await self.fix_timestamp(
                currency, addr_tx, timestamp_col="timestamp", block_id_col="height"
            )
        return txs

    async def list_txs_by_ids_eth(self, currency, ids, include_token_txs=False):
        params = [[self.get_tx_id_group(currency, id), id] for id in ids]
        statement = (
            "SELECT transaction from transaction_ids_by_transaction_id_group"
            " where transaction_id_group = %s and transaction_id = %s"
        )
        result = await self.concurrent_with_args(
            currency, "transformed", statement, params, filter_empty=False
        )

        return await self.list_txs_by_hashes(
            currency,
            [row["transaction"] for row in result],
            include_token_txs=include_token_txs,
        )

    async def get_tx_by_id_eth(self, currency, id):
        params = [self.get_tx_id_group(currency, id), id]
        statement = (
            "SELECT transaction from transaction_ids_by_transaction_id_group"
            " where transaction_id_group = %s and transaction_id = %s"
        )
        result = await self.execute_async(currency, "transformed", statement, params)
        result = one(result)
        if not result:
            return None
        try:
            return await self.get_tx_by_hash_eth(currency, result["transaction"])
        except TransactionNotFoundException:
            raise DBInconsistencyException(
                f"transaction {bytes_to_hex(result['transaction'])} "
                f"with id {id} in network {currency} not found')"
            )

    async def list_txs_by_hashes_eth(self, currency, hashes, include_token_txs=False):
        prefix = self.get_prefix_lengths(currency)
        params = [[hash.hex()[: prefix["tx"]], hash] for hash in hashes]
        statement = (
            "SELECT tx_hash, block_id, block_timestamp, value, "
            "from_address, to_address, receipt_contract_address, input from "
            "transaction where tx_hash_prefix=%s and tx_hash=%s"
        )
        result = await self.concurrent_with_args(currency, "raw", statement, params)

        result_with_tokens = []
        for row in result:
            to_address = row["to_address"]
            row["type"] = "external"
            if to_address is None:
                # this is a contract creation transaction
                # set recipient to newly created contract
                # and mark tx as creation
                row["to_address"] = row["receipt_contract_address"]
                row["contract_creation"] = True
            else:
                # normal transaction
                row["to_address"] = to_address
                # result['contract_creation'] = False

            row["input_parsed"] = parse_function_call(
                row["input"], function_call_signatures
            )

            result_with_tokens.append(row)
            if include_token_txs:
                token_txs = await self.fetch_token_transactions(currency, row)
                for ttx in token_txs:
                    ttx["type"] = "erc20"
                    result_with_tokens.append(ttx)

        return result_with_tokens

    async def get_tx_by_hash_eth(self, currency, tx_hash):
        prefix = self.get_prefix_lengths(currency)
        try:
            params = [tx_hash.hex()[: prefix["tx"]], tx_hash]
        except ValueError:
            raise BadUserInputException(
                f"{tx_hash} does not look like a valid transaction hash."
            )
        statement = (
            "SELECT tx_hash, block_id, block_timestamp, value, "
            "from_address, to_address, receipt_contract_address, input from "
            "transaction where tx_hash_prefix=%s and tx_hash=%s"
        )

        result = await self.execute_async(currency, "raw", statement, params)
        result = one(result)

        if not result:
            raise TransactionNotFoundException(currency, tx_hash)

        await self.fix_timestamp(currency, result)

        to_address = result["to_address"]
        if to_address is None:
            # this is a contract creation transaction
            # set recipient to newly created contract and mark tx as creation
            result["to_address"] = result["receipt_contract_address"]
            result["contract_creation"] = True
        else:
            # normal transaction
            result["to_address"] = to_address
            # result['contract_creation'] = False

        result["input_parsed"] = parse_function_call(
            result["input"], function_call_signatures
        )

        return result

    def get_tx_eth(self, currency, tx_hash: str):
        return self.get_tx_by_hash(currency, tx_hash_from_hex(tx_hash))

    async def list_entity_addresses_eth(
        self, currency, entity, page=None, pagesize=None
    ):
        addresses = await self.get_addresses_by_ids(currency, [entity])
        return await self.finish_addresses(currency, addresses), None

    async def get_cross_chain_pubkey_related_addresses(
        self, address: str, currency: Optional[str] = None, only_existing: bool = True
    ) -> List[Any]:
        keyspace = self.tconfig.cross_chain_pubkey_mapping_keyspace

        if currency is not None and currency not in self.parameters:
            raise NetworkNotFoundException(currency)

        active_networks = self.parameters.keys()

        if (
            keyspace is not None
            and self.get_cross_chain_pubkey_related_addresses_available
        ):
            pubkey = one(
                await self.execute_async_core(
                    f"SELECT pubkey FROM {keyspace}.pubkey_by_address WHERE address = ?",
                    {"address": address},
                )
            )

            if pubkey is not None:
                key = pubkey["pubkey"]

                addresses = convert_pubkey_to_addresses(
                    key.hex(), currencies=active_networks
                )

                result = []
                for currency, addressesInC in addresses.items():
                    for type, address in addressesInC.items():
                        result.append(
                            {
                                "address": address,
                                "type": type,
                                "currency": currency,
                                "pubkey": key,
                            }
                        )

                async def try_get_address(currency, address):
                    try:
                        return await self.get_address(currency, address)
                    except AddressNotFoundException:
                        return None

                if only_existing:
                    requests = [
                        try_get_address(
                            x["currency"],
                            cannonicalize_address(x["currency"], x["address"]),
                        )
                        for x in result
                    ]
                    addresses = await asyncio.gather(*requests)

                    return [
                        address
                        for address, req in zip(result, addresses)
                        if req is not None
                    ]
                else:
                    return result

        return []

    def markup_values(self, currency, fiat_values):
        values = []
        fcurs = self.parameters[currency]["fiat_currencies"]

        if fiat_values is None:
            fiat_values = [0.0 for _ in fcurs]

        for fiat, curr in zip(fiat_values, fcurs):
            values.append({"code": curr.lower(), "value": fiat})
        return values

    def markup_currency(self, currency, values):
        Values = namedtuple("Values", values._fields)
        values = values._asdict()
        values["fiat_values"] = self.markup_values(currency, values["fiat_values"])
        return Values(**values)

    def markup_rates(self, currency, row):
        row["rates"] = self.markup_values(currency, row["fiat_values"])
        return row

    def sec_in(self, id):
        return ValueSequence(range(0, id + 1))

    def log_missing(self, ids1, ids2, node_type, query, params):
        missing = []
        for id in ids2:
            if id not in ids1:
                missing.append(id)
        self.logger.critical(
            f"nodes existing in `{query}` {params} but not "
            f"in {node_type} table: {missing}"
        )
