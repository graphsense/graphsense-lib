import hashlib
import logging
import random
import time
from functools import wraps
from typing import Callable, Iterable, List, Optional, Sequence, TypeVar, Union

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import (
    EXEC_PROFILE_DEFAULT,
    Cluster,
    ConsistencyLevel,
    ExecutionProfile,
)

# Session,
from cassandra.concurrent import execute_concurrent, execute_concurrent_with_args
from cassandra.policies import (
    DCAwareRoundRobinPolicy,
    ExponentialReconnectionPolicy,
    RetryPolicy,
    TokenAwarePolicy,
)
from cassandra.cluster import NoHostAvailable
from cassandra import OperationTimedOut, ReadTimeout, WriteTimeout
from cassandra.segment import CrcException

from cassandra.query import (
    UNSET_VALUE,
    BatchStatement,
    BoundStatement,
    PreparedStatement,
    SimpleStatement,
)

from ..utils import remove_multi_whitespace

DEFAULT_TIMEOUT = 120
DEFAULT_CONSISTENCY_LEVEL = "LOCAL_QUORUM"
DEFAULT_SERIAL_CONSISTENCY_LEVEL = "LOCAL_SERIAL"

# Transient driver errors that indicate a temporary cluster-wide stall (e.g.
# nodes starved of CPU by a concurrent Spark run) rather than a permanent fault.
# Safe to ride out with application-level backoff on the calling thread.
# CrcException is the client-side protocol-v5 segment checksum failure
# (CASSANDRA-19971): the driver defuncts the affected connection, so the retry
# runs on a fresh one and typically succeeds immediately. Retrying it here is
# no more ambiguous than the existing WriteTimeout retry.
TRANSIENT_DB_ERRORS = (
    NoHostAvailable,
    OperationTimedOut,
    ReadTimeout,
    WriteTimeout,
    CrcException,
)

# Application-level ride-out backoff for synchronous reads. The backoff sleeps on
# the calling thread (never the driver reactor) so connection heartbeats keep
# flowing. ~20 retries with delay capped at 30s ≈ multi-minute ride-out window.
RIDE_OUT_MAX_RETRIES = 20
RIDE_OUT_BASE_DELAY = 0.5
RIDE_OUT_MAX_DELAY = 30

_T = TypeVar("_T")

# create logger
logger = logging.getLogger(__name__)


# taken from https://github.com/apache/cassandra-dtest/blob/0085d21bc687995478e338302e619e82ad4a4644/dtest.py#L88C5-L88C5 # noqa
class GraphsenseRetryPolicy(RetryPolicy):
    """Non-blocking driver-side retry policy.

    The driver invokes these callbacks on its I/O reactor thread, so they MUST
    NOT block. An earlier version called ``time.sleep()`` here for backoff; during
    a long sleep the reactor could not service connection heartbeats, so healthy
    connections were defuncted (heartbeat failure / ``ConnectionShutdown``) and the
    in-flight query failed.

    Here we only do a few *immediate* retries (on the next coordinator) to absorb
    a single-host blip, then rethrow. Riding out long, cluster-wide stalls (e.g.
    nodes starved of CPU by a concurrent Spark run) is done at the application
    layer with backoff that sleeps on the calling thread — see
    ``CassandraDb._execute_with_backoff``.
    """

    def __init__(self, max_retries=2):
        self.max_retries = max_retries

    def on_read_timeout(self, *args, **kwargs):
        # Reads are idempotent: an immediate retry on a different coordinator is
        # always safe. No sleep here — backoff happens on the application thread.
        if kwargs["retry_num"] < self.max_retries:
            return (self.RETRY_NEXT_HOST, None)
        return (self.RETHROW, None)

    def on_write_timeout(
        self,
        query,
        consistency,
        write_type,
        required_responses,
        received_responses,
        retry_num,
    ):
        # Only retry writes the application has marked idempotent — non-idempotent
        # retries can silently double-apply on timeout.
        if not getattr(query, "is_idempotent", False):
            return (self.RETHROW, None)
        if retry_num < self.max_retries:
            return (self.RETRY, None)
        return (self.RETHROW, None)

    def on_request_error(self, query, consistency, error, retry_num):
        # Retrying on another coordinator is only safe for reads or queries the
        # application marked idempotent; a non-idempotent write could double-apply.
        query_string = (
            query.prepared_statement.query_string
            if isinstance(query, BoundStatement)
            else query.query_string
        )
        is_read = query_string.upper().lstrip().startswith("SELECT")
        if (
            is_read or getattr(query, "is_idempotent", False)
        ) and retry_num < self.max_retries:
            return (self.RETRY_NEXT_HOST, None)
        return (self.RETHROW, None)

    def on_unavailable(
        self, query, consistency, required_replicas, alive_replicas, retry_num
    ):
        # Unavailable is often transient (node restart, GC pause, gossip flap).
        # Try a different coordinator — its view of replica liveness may be stale.
        if retry_num < self.max_retries:
            return (self.RETRY_NEXT_HOST, None)
        return (self.RETHROW, None)


def normalize_cql_statement(stmt: str) -> str:
    return remove_multi_whitespace(stmt.lower().strip()).rstrip(";")


def none_to_unset(items: Union[dict, tuple, list]):
    """Sets all None value to UNSET to avoid tombstone creation see

    https://stackoverflow.com/questions/34637680/
    how-insert-in-cassandra-without-null-value-in-column

    Args:
        items (Union[dict, tuple, list]): items to insert

    Returns:
        None: -

    Raises:
        Exception: If datatype of items is not supported (list,tuple,dict)
    """
    if isinstance(items, dict):
        return {k: (UNSET_VALUE if v is None else v) for k, v in items.items()}
    elif isinstance(items, tuple):
        return tuple([UNSET_VALUE if v is None else v for v in list(items)])
    elif isinstance(items, list):
        return [(UNSET_VALUE if v is None else v) for v in items]
    else:
        raise Exception(
            f"Can't auto unset for type {type(items)} please assign "
            "cassandra.query.UNSET_VALUE manually."
        )


def get_select_rep(data):
    if isinstance(data, str) or isinstance(data, int) or isinstance(data, float):
        return str(data)
    else:
        raise Exception(f"No conversion for type {type(data)} to cql select known.")


def get_table_name(table: str, keyspace: Optional[str] = None) -> str:
    return table if keyspace is None else f"{keyspace}.{table}"


def build_select_stmt(
    table: str,
    columns: Sequence[str] = ["*"],
    keyspace: Optional[str] = None,
    where: Optional[dict] = None,
    limit: Optional[int] = None,
    per_partition_limit: Optional[int] = None,
) -> str:
    """Create CQL select statement for specified columns and table name.

    Args:
        table (str): Description
        columns (Sequence[str], optional): Description
        keyspace (Optional[str], optional): Description
        where (Optional[dict], optional): Description
        limit (Optional[int], optional): Description
        per_partition_limit (Optional[int], optional): Description


    Returns:
        str: Description
    """
    cols = ",".join(columns)
    whr = (
        ""
        if where is None
        else " WHERE "
        + " AND ".join([f"{k}={get_select_rep(v)}" for k, v in where.items()])
    )
    lmt = "" if limit is None else f" LIMIT {limit}"
    pplmt = (
        ""
        if per_partition_limit is None
        else f" PER PARTITION LIMIT {per_partition_limit}"
    )
    if per_partition_limit is not None:
        lmt = pplmt
    return f"SELECT {cols} FROM {get_table_name(table, keyspace)}{whr}{lmt};"


def build_insert_stmt(
    columns: Sequence[str], table: str, upsert=True, keyspace: Optional[str] = None
) -> str:
    """Create CQL insert statement for specified columns and table name.

    Args:
        columns (Sequence[str]): column names without type (eg. address)
        table (str): name of the table.

    Returns:
        str: the insert statement
    """

    return (
        f"INSERT INTO {get_table_name(table, keyspace)} ({', '.join(columns)}) "
        f"VALUES ({('?,' * len(columns))[:-1]}) "
        f"{'' if upsert else 'IF NOT EXISTS'}"
        ";"
    )


def build_delete_stmt(
    key_columns: Sequence[str], table: str, keyspace: Optional[str] = None
) -> str:
    """Create CQL insert statement for specified columns and table name.

    Args:
        columns (Sequence[str]): column names without type (eg. address)
        table (str): name of the table.

    Returns:
        str: the insert statement
    """

    return (
        f"DELETE FROM {get_table_name(table, keyspace)} "
        f"WHERE {' AND '.join([f'{x}=?' for x in key_columns])}"
        ";"
    )


def build_fail_if_exists_stmt(fail_if_exists: bool) -> str:
    """Summary

    Args:
        fail_if_exists (bool): if statement should fail if item already exists

    Returns:
        str: part of the cql that controls the creation behavior
    """
    return "" if fail_if_exists else "IF NOT EXISTS "


def build_create_stmt(
    columns: Sequence[str],
    pk_columns: Sequence[str],
    table: str,
    fail_if_exists: bool,
    keyspace: Optional[str] = None,
    with_stmt: str = None,
) -> str:
    """Create CQL Create statement

    Args:
        columns (Sequence[str]): Columns with their type (eg. address_prefix text)
        pk_columns (Sequence[str]): Columns used as pk
        table (str): name of the table to create
        fail_if_exists (bool): if an error should be thrown if table already exists

    Returns:
        str: the create statement
    """
    columns_stmt = ", ".join(columns)
    pk_stmt = ",".join(pk_columns)
    w = f" WITH {with_stmt}" if with_stmt is not None else ""

    return (
        f"CREATE TABLE {build_fail_if_exists_stmt(fail_if_exists)}"
        f"{get_table_name(table, keyspace)} "
        f"({columns_stmt}, PRIMARY KEY ({pk_stmt})){w};"
    )


def build_truncate_stmt(table: str, keyspace: Optional[str] = None) -> str:
    if keyspace is None:
        raise Exception("Please provide an explicit keyspace for truncate operations.")
    return f"TRUNCATE {get_table_name(table, keyspace)};"


class StorageError(Exception):
    """Class for Cassandra-related errors"""

    def __init__(self, message: str):
        super().__init__("Cassandra Error: " + message)


class CassandraScope:
    def __init__(
        self,
        db_nodes,
        default_timeout=DEFAULT_TIMEOUT,
        username: Optional[str] = None,
        password: Optional[str] = None,
    ):
        self._db = CassandraDb(db_nodes, default_timeout, username, password)

    def __enter__(self):
        self._db.connect()
        return self._db

    def __exit__(self, exc_type, exc_value, tb):
        self._db.close()


class CassandraKeyspaceScope:
    def _set_ks(self, keyspace_name):
        if keyspace_name is not None:
            self._db.set_keyspace(keyspace_name)

    def __init__(self, db, keyspace_name):
        self._db = db
        self._old_keyspace = None
        self._new_keyspace = keyspace_name

    def __enter__(self):
        self._old_keyspace = self._db.get_keyspace()
        self._set_ks(self._new_keyspace)
        return self._db

    def __exit__(self, exc_type, exc_value, tb):
        self._set_ks(self._old_keyspace)


class CassandraDb:
    """Cassandra connector"""

    def needs_session(func):
        @wraps(func)
        def x(*args, **kwargs):
            self = args[0]
            if self.session is None:
                raise StorageError("Session not available. Call connect() first")
            return func(*args, **kwargs)

        return x

    def __init__(
        self,
        db_nodes: Iterable,
        default_timeout=DEFAULT_TIMEOUT,
        username: Optional[str] = None,
        password: Optional[str] = None,
        consistency_level: str = DEFAULT_CONSISTENCY_LEVEL,
        serial_consistency_level: str = DEFAULT_SERIAL_CONSISTENCY_LEVEL,
    ) -> None:
        ports_in_order = [int(x.split(":")[1]) for x in db_nodes if ":" in x]
        nodes = [x.split(":")[0] for x in db_nodes]
        self.db_nodes = nodes
        unique_ports = set(ports_in_order)
        if len(unique_ports) > 1:
            logger.warning(
                "cassandra_nodes specify conflicting ports %s; using the "
                "first one (%d). Use the same port for all nodes to silence "
                "this warning.",
                sorted(unique_ports),
                ports_in_order[0],
            )
        self.db_port = ports_in_order[0] if ports_in_order else 9042
        self.db_username = username
        self.db_password = password
        self.cluster = None
        self.session = None
        self.prep_stmts = {}
        self._default_timeout = default_timeout
        self._consistency_level = consistency_level
        self._serial_consistency_level = serial_consistency_level
        self._columns_cache = {}
        self._columns_cache_ttl = 600  # 10 minutes in seconds

    @property
    def nodes_with_port(self) -> List[str]:
        if self.db_port != 9042:
            return [f"{node}:{self.db_port}" for node in self.db_nodes]
        return self.db_nodes

    def clone(self) -> "CassandraDb":
        return CassandraDb(
            self.nodes_with_port,
            self._default_timeout,
            self.db_username,
            self.db_password,
            self._consistency_level,
            self._serial_consistency_level,
        )

    def _is_cache_valid(self, cache_key: str) -> bool:
        """Check if cached entry is still valid based on TTL."""
        if cache_key not in self._columns_cache:
            return False
        cached_time = self._columns_cache[cache_key]["timestamp"]
        return (time.time() - cached_time) < self._columns_cache_ttl

    def _get_cached_columns(self, cache_key: str):
        """Get cached columns if valid, otherwise return None."""
        if self._is_cache_valid(cache_key):
            return self._columns_cache[cache_key]["data"]
        return None

    def _cache_columns(self, cache_key: str, data):
        """Cache column data with timestamp."""
        self._columns_cache[cache_key] = {"data": data, "timestamp": time.time()}

    def invalidate_columns_cache(
        self, keyspace: Optional[str] = None, table: Optional[str] = None
    ):
        """Invalidate the columns cache.

        Args:
            keyspace: If provided, only invalidate cache entries for this keyspace
            table: If provided (along with keyspace), only invalidate cache for this specific table
        """
        if keyspace is None and table is None:
            # Clear entire cache
            self._columns_cache.clear()
        elif keyspace is not None and table is not None:
            # Clear specific keyspace.table entry
            cache_key = f"{keyspace}.{table}"
            self._columns_cache.pop(cache_key, None)
        elif keyspace is not None:
            # Clear all entries for the keyspace
            keys_to_remove = [
                key
                for key in self._columns_cache.keys()
                if key.startswith(f"{keyspace}.")
            ]
            for key in keys_to_remove:
                self._columns_cache.pop(key, None)
        else:
            raise ValueError("Cannot specify table without keyspace")

    def __repr__(self):
        return f"{', '.join(self.db_nodes)}"

    def connect(self):
        """Connect to given Cassandra cluster nodes."""
        exec_prof = ExecutionProfile(
            retry_policy=GraphsenseRetryPolicy(),
            consistency_level=ConsistencyLevel.name_to_value[self._consistency_level],
            serial_consistency_level=ConsistencyLevel.name_to_value[
                self._serial_consistency_level
            ],
            request_timeout=self._default_timeout,
            load_balancing_policy=TokenAwarePolicy(DCAwareRoundRobinPolicy()),
        )
        auth_provider = None
        if self.db_username is not None:
            auth_provider = PlainTextAuthProvider(
                username=self.db_username, password=self.db_password
            )

        self.cluster = Cluster(
            self.db_nodes,
            port=self.db_port,
            execution_profiles={EXEC_PROFILE_DEFAULT: exec_prof},
            connect_timeout=15,
            # Tolerate longer server-side stalls (e.g. a node starved of CPU by a
            # concurrent Spark run) before declaring a connection defunct.
            idle_heartbeat_timeout=60,
            # Pin the keepalive cadence explicitly (driver default is 30s) so a
            # defuncted connection is re-probed promptly after a stall.
            idle_heartbeat_interval=30,
            # Background reconnection backoff for hosts marked down (e.g. after a
            # heartbeat-induced defunct). This is host reconnection, NOT query
            # retry — query ride-out happens in _execute_with_backoff. Start at 1s,
            # cap at 60s so a recovered node is re-added within ~a minute.
            reconnection_policy=ExponentialReconnectionPolicy(1.0, 60.0),
            # protocol_version=6,
            compression="lz4",
            auth_provider=auth_provider,
        )
        try:
            self.session = self.cluster.connect()
        except Exception as e:
            raise StorageError(f"Cannot connect to {self.db_nodes}") from e

    def get_keyspace(self):
        return self.session.keyspace

    def on_keyspace(self, keyspace_name: str):
        return CassandraKeyspaceScope(self, keyspace_name)

    def set_keyspace(self, keyspace_name: str):
        return self.session.set_keyspace(keyspace_name)

    @needs_session
    def set_row_factory(self, factory_function):
        self.session.row_factory = factory_function

    @needs_session
    def has_keyspace(self, keyspace: str) -> bool:
        """Check whether a given keyspace is present in the cluster."""
        try:
            query = "SELECT keyspace_name FROM system_schema.keyspaces"
            result = self.session.execute(query)
            keyspaces = [row.keyspace_name for row in result]
            return keyspace in keyspaces
        except Exception as e:
            raise StorageError(f"Error when executing query: \n{query}") from e

    @needs_session
    def has_table(self, keyspace: str, table: str) -> bool:
        """Check whether a given table is present in the cluster."""
        try:
            query = "SELECT keyspace_name, table_name FROM system_schema.tables"
            result = self.session.execute(query)
            tables = [row.table_name for row in result if row.keyspace_name == keyspace]
            return table in tables
        except Exception as e:
            raise StorageError(f"Error when executing query: \n{query}") from e

    def _retry_transient(self, produce: Callable[[], _T]) -> _T:
        """Run ``produce()`` on the calling thread, riding out transient
        cluster-wide stalls with exponential backoff + jitter.

        Backoff sleeps here (the application thread), never inside the driver
        retry policy which runs on the I/O reactor thread; sleeping there would
        stall connection heartbeats and defunct otherwise-healthy connections.

        ``produce`` must FULLY MATERIALIZE its result (consume any generator /
        resolve any future) so a transient error is raised here where it can be
        retried, not later during lazy iteration by the caller. Only use for
        idempotent work (reads, or writes with no counters / LWT) — a retry
        re-runs the whole operation.
        """
        attempt = 0
        while True:
            try:
                return produce()
            except TRANSIENT_DB_ERRORS as exc:
                if attempt >= RIDE_OUT_MAX_RETRIES:
                    logger.error(
                        f"DB op gave up after {attempt + 1} attempts "
                        f"({type(exc).__name__}): {exc}"
                    )
                    raise
                cap = min(
                    RIDE_OUT_BASE_DELAY * (2 ** (attempt + 1)), RIDE_OUT_MAX_DELAY
                )
                delay = random.uniform(RIDE_OUT_BASE_DELAY, cap)
                logger.warning(
                    f"Transient DB error ({type(exc).__name__}); retry "
                    f"#{attempt + 1} after {delay:.2f}s backoff"
                )
                time.sleep(delay)
                attempt += 1

    def _execute_with_backoff(self, statement, parameters=None) -> Iterable:
        """Run a single-statement read on the calling thread, riding out
        transient cluster-wide stalls (see ``_retry_transient``)."""
        if parameters is None:
            return self._retry_transient(lambda: self.session.execute(statement))
        return self._retry_transient(
            lambda: self.session.execute(statement, parameters)
        )

    @needs_session
    def execute_safe(
        self, cql_query_str: str, params: dict, fetch_size=None
    ) -> Iterable:
        # flat_stmt = cql_query_str.replace("\n", " ")
        # logger.debug(f"{flat_stmt} in keyspace {self.session.keyspace}")
        stmt = SimpleStatement(cql_query_str, fetch_size=fetch_size)
        return self._execute_with_backoff(stmt, params)

    @needs_session
    def execute(
        self, cql_query_str: str, fetch_size=None, keyspace: Optional[str] = None
    ) -> Iterable:
        # flat_stmt = cql_query_str.replace("\n", " ")
        # logger.debug(f"{flat_stmt} in keyspace {self.session.keyspace}")

        stmt = SimpleStatement(cql_query_str, fetch_size=None, keyspace=keyspace)
        if fetch_size is not None:
            stmt.fetch_size = fetch_size

        # The statement-level keyspace above is only sent on protocol v5+; on
        # older versions the driver drops it silently and bare table names
        # would resolve against the session keyspace. Switch the session
        # keyspace as well so the query targets the right keyspace regardless
        # of the negotiated protocol version.
        if keyspace is not None and self.get_keyspace() != keyspace:
            with self.on_keyspace(keyspace):
                return self._execute_with_backoff(stmt)

        return self._execute_with_backoff(stmt)

    @needs_session
    def read_partitions_concurrent(
        self,
        keyspace: str,
        table: str,
        key_column: str,
        select_columns: str,
        keys: Sequence,
        fetch_size: int = 5000,
        concurrency: int = 32,
        group_column: Optional[str] = None,
        bucket_size: Optional[int] = None,
    ) -> List:
        """Read full partitions for many partition-key values, one query per key.

        A single large multi-partition ``... WHERE pk IN (many)`` forces the
        coordinator to gather every partition within one server read timeout,
        which fails with ReadTimeout once any partition is large (e.g. an
        exchange cluster). Issuing one query per partition lets each be paged
        independently via ``fetch_size`` so no single request exceeds the read
        timeout, and ``execute_concurrent_with_args`` keeps throughput up with
        bounded concurrency. Returns the flattened list of rows across all keys.

        The table is bucketed on a composite key ``(group_column, key_column)``;
        the query restricts both and the bucket ``floor(key / bucket_size)`` is
        derived per key. ``group_column`` and ``bucket_size`` are required.
        """
        if not keys:
            return []
        if group_column is None or bucket_size is None:
            raise ValueError(
                "read_partitions_concurrent requires group_column and bucket_size "
                "for the bucketed composite key"
            )
        stmt = self.session.prepare(
            f"SELECT {select_columns} FROM {keyspace}.{table} "
            f"WHERE {group_column} = ? AND {key_column} = ?"
        )
        args = [(k // bucket_size, k) for k in keys]
        stmt.fetch_size = fetch_size
        results = execute_concurrent_with_args(
            self.session,
            stmt,
            args,
            concurrency=concurrency,
            raise_on_first_error=True,
        )
        rows = []
        for _success, result in results:
            rows.extend(result)
        return rows

    @needs_session
    def execute_statement(self, stmt: BoundStatement, fetch_size=None) -> Iterable:
        return self._execute_with_backoff(stmt)

    @needs_session
    def execute_async(self, cql_query_str: str, fetch_size=None):
        # flat_stmt = cql_query_str.replace("\n", " ")
        # logger.debug(f"{flat_stmt} in keyspace {self.session.keyspace}")
        stmt = SimpleStatement(cql_query_str, fetch_size=None)
        if fetch_size is not None:
            stmt.fetch_size = fetch_size
        return self.session.execute_async(stmt)

    @needs_session
    def execute_async_safe(self, cql_query_str: str, params: dict, fetch_size=None):
        # flat_stmt = cql_query_str.replace("\n", " ")
        # logger.debug(f"{flat_stmt} in keyspace {self.session.keyspace}")
        stmt = SimpleStatement(cql_query_str, fetch_size=fetch_size)
        return self.session.execute_async(stmt, params)

    def execute_statements_atomic(self, statements: List[BoundStatement]):
        batch = BatchStatement()
        for stmt in statements:
            batch.add(stmt)

        self.session.execute(batch)

    @needs_session
    def execute_statements(
        self, statements: List[BoundStatement], concurrency: int = 100
    ):
        return execute_concurrent(
            self.session,
            [(stmt, None) for stmt in statements],
            raise_on_first_error=True,
            concurrency=concurrency,
        )

    @needs_session
    def execute_statements_async(
        self, statements: List[BoundStatement], concurrency=100
    ):
        # Read/lookup helper (the delta-updater hot loop leans on it). Ride out
        # transient cluster-wide stalls like the single-statement reads do — the
        # write apply-path has its own retry, but this read path had none, so a
        # single OperationTimedOut during a Spark stall aborted the whole run.
        # The results generator is materialized inside the retry so the transient
        # error raises here (where it can be retried) instead of during the
        # caller's iteration; the returned list is order-preserving and iterates
        # identically to the old generator.
        return self._retry_transient(
            lambda: list(
                execute_concurrent(
                    self.session,
                    [(stmt, None) for stmt in statements],
                    raise_on_first_error=True,
                    concurrency=concurrency,
                    results_generator=True,
                )
            )
        )

    @needs_session
    def execute_statement_async(self, stmt, params):
        return self.session.execute_async(stmt, params)

    @needs_session
    def execute_batch_async(self, stmt, params):
        if len(params) > 10000:
            logger.warning(
                "CAUTION: Running many (10k+) parallel queries against db "
                "without concurrency control. "
                "Might lead to timeouts. Consider using execute_statements_async."
            )
        prp = self.get_prepared_statement(stmt)
        futures = [
            (identifier, self.execute_statement_async(prp, param_list))
            for identifier, param_list in params
        ]
        return futures

    @needs_session
    def execute_batch(self, stmt, params):
        # Read helper: materialize (resolve the futures) inside the ride-out
        # retry so a transient stall re-issues and re-resolves the whole batch
        # rather than aborting the delta-updater run. Idempotent (reads / upserts).
        return self._retry_transient(
            lambda: [
                (i, future.result())
                for (i, future) in self.execute_batch_async(stmt, params)
            ]
        )

    def await_batch(self, futures):
        return [(i, future.result()) for (i, future) in futures]

    @needs_session
    def setup_keyspace_using_schema(self, schema: str) -> None:
        """Setup keyspace and tables."""

        statements = schema.split(";")
        for stmt in statements:
            stmt = stmt.strip()
            if len(stmt) > 0:
                self.session.execute(stmt + ";")

    @needs_session
    def get_columns_for_table(self, keyspace: str, table: str):
        # Check cache first
        cache_key = f"{keyspace}.{table}"
        cached_result = self._get_cached_columns(cache_key)
        if cached_result is not None:
            return cached_result

        # Query database if not in cache or expired
        cql_str = (
            f"SELECT * FROM system_schema.columns "
            f"WHERE keyspace_name = '{keyspace}' "
            f"AND table_name = '{table}';"
        )
        result = self.session.execute(cql_str)

        # Cache the result
        self._cache_columns(cache_key, result)
        return result

    @needs_session
    def get_prepared_insert_statement(
        self,
        keyspace: str,
        table: str,
        upsert=True,
        cl=None,
        max_retries=3,
        base_delay=0.5,
        max_delay=10,
    ) -> PreparedStatement:
        """Build prepared CQL statement for specified table."""

        retry_attempt = 0
        while retry_attempt <= max_retries:
            try:
                result_set = self.get_columns_for_table(keyspace, table)
                columns = [elem.column_name for elem in result_set._current_rows]
                ps = self.get_prepared_statement(
                    build_insert_stmt(columns, table, upsert=upsert, keyspace=keyspace)
                )
                if cl is not None:
                    ps.consistency_level = cl
                # Upserts (INSERT without IF NOT EXISTS) are idempotent by PK;
                # LWTs (upsert=False -> IF NOT EXISTS) are not.
                ps.is_idempotent = upsert
                return ps
            except (
                NoHostAvailable,
                OperationTimedOut,
                ReadTimeout,
                WriteTimeout,
                Exception,
            ) as exception:
                if retry_attempt == max_retries:
                    if isinstance(exception, NoHostAvailable):
                        logger.error(
                            f"Failed to prepare insert statement after {max_retries + 1} attempts due to NoHostAvailable: {exception}"
                        )
                    else:
                        logger.error(
                            f"Failed to prepare insert statement after {max_retries + 1} attempts: {exception}"
                        )
                    raise exception

                retry_attempt += 1
                delay = min(base_delay * (2**retry_attempt), max_delay)

                if isinstance(exception, NoHostAvailable):
                    logger.warning(
                        f"NoHostAvailable error while preparing insert statement (attempt {retry_attempt}/{max_retries + 1}): {exception}. "
                        f"Retrying in {delay} seconds..."
                    )
                else:
                    logger.warning(
                        f"Error while preparing insert statement (attempt {retry_attempt}/{max_retries + 1}): {exception}. "
                        f"Retrying in {delay} seconds..."
                    )

                time.sleep(delay)

    @needs_session
    def get_prepared_statement(self, stmt):
        hash_object = hashlib.sha256(stmt.encode("utf-8"))
        hex_dig = hash_object.hexdigest()
        if hex_dig not in self.prep_stmts:
            self.prep_stmts[hex_dig] = self.session.prepare(stmt)
        return self.prep_stmts[hex_dig]

    @needs_session
    def ingest(
        self,
        table: str,
        keyspace: str,
        items: Iterable,
        upsert=True,
        cl=None,
        concurrency: int = 100,
        auto_none_to_unset=False,
    ):
        stmt = self.get_prepared_insert_statement(keyspace, table, upsert=upsert, cl=cl)

        if auto_none_to_unset:
            items = [none_to_unset(row) for row in items]

        self._exe_with_retries(stmt, items, concurrency=concurrency)

    @needs_session
    def _exe_with_retries(
        self,
        prepared_stmt: PreparedStatement,
        parameters,
        concurrency: int = 100,
        retries=4,
        base_delay=0.5,
        max_delay=10,
    ) -> None:
        """Concurrent ingest into Apache Cassandra."""
        from cassandra.cluster import NoHostAvailable

        nr_statements = len(parameters)
        retries = (retries - 1) * nr_statements
        retry_attempt = 0

        while True:
            try:
                results = execute_concurrent_with_args(
                    session=self.session,
                    statement=prepared_stmt,
                    parameters=parameters,
                    concurrency=concurrency,
                )

                for i, (success, _) in enumerate(results):
                    if not success:
                        individual_retry_attempt = 0
                        while True:
                            try:
                                logger.warning(
                                    "Retrying failed statement:",
                                    prepared_stmt,
                                    parameters[i],
                                )
                                self.session.execute(prepared_stmt, parameters[i])
                            except (NoHostAvailable, Exception) as exception:
                                # Individual statement retry backoff
                                individual_retry_attempt += 1
                                delay = min(
                                    base_delay * (2**individual_retry_attempt),
                                    max_delay,
                                )

                                if isinstance(exception, NoHostAvailable):
                                    logger.warning(
                                        f"NoHostAvailable error: {str(exception)} "
                                        f"retrying another {retries} times/statements. "
                                        f"Backing off for {delay} seconds"
                                    )
                                else:
                                    logger.warning(
                                        f"Retry after exception: {str(exception)} "
                                        f"retrying another {retries} times/statements. "
                                        f"Backing off for {delay} seconds"
                                    )

                                time.sleep(delay)
                                retries -= 1
                                if retries < 0:
                                    logger.error("Giving up retries for ingest.")
                                    raise exception
                                else:
                                    continue
                            break
                break

            except (NoHostAvailable, Exception) as exception:
                # Batch retry backoff
                retry_attempt += 1
                delay = min(base_delay * (2**retry_attempt), max_delay)

                if isinstance(exception, NoHostAvailable):
                    logger.warning(
                        f"NoHostAvailable error in batch execution: {str(exception)} "
                        f"retrying another {retries} times/statements. "
                        f"Backing off for {delay} seconds"
                    )
                else:
                    logger.warning(
                        f"Retry after exception: {str(exception)} "
                        f"retrying another {retries} times/statements. "
                        f"Backing off for {delay} seconds"
                    )

                time.sleep(delay)
                retries -= nr_statements
                if retries < 0:
                    if isinstance(exception, NoHostAvailable):
                        logger.error(
                            "Giving up retries for ingest due to NoHostAvailable."
                        )
                    else:
                        logger.error("Giving up retries for ingest.")
                    raise exception
                else:
                    continue

    @needs_session
    def setup_keyspace_using_schema_file(self, schema_file: str) -> None:
        with open(schema_file, "r", encoding="utf-8") as file_handle:
            self.setup_keyspace_using_schema(file_handle.read())

    @needs_session
    def close(self):
        """Closes the cassandra cluster connection."""
        self.cluster.shutdown()
