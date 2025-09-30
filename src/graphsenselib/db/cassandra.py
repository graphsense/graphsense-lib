import hashlib
import logging
import time
from functools import wraps
from typing import Iterable, List, Optional, Sequence, Union

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import (
    EXEC_PROFILE_DEFAULT,
    Cluster,
    ConsistencyLevel,
    ExecutionProfile,
)

# Session,
from cassandra.concurrent import execute_concurrent, execute_concurrent_with_args
from cassandra.policies import DCAwareRoundRobinPolicy, RetryPolicy, TokenAwarePolicy
from cassandra.query import (
    UNSET_VALUE,
    BatchStatement,
    BoundStatement,
    PreparedStatement,
    SimpleStatement,
)

from ..utils import remove_multi_whitespace

DEFAULT_TIMEOUT = 120

# create logger
logger = logging.getLogger(__name__)


# taken from https://github.com/apache/cassandra-dtest/blob/0085d21bc687995478e338302e619e82ad4a4644/dtest.py#L88C5-L88C5 # noqa
class GraphsenseRetryPolicy(RetryPolicy):
    """
    A retry policy that retries 10 times by default, but can be configured to
    retry more times.
    """

    def __init__(self, max_retries=10, base_delay=0.5, max_delay=10):
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay

    def on_read_timeout(self, *args, **kwargs):
        if kwargs["retry_num"] < self.max_retries:
            logger.warning(
                "Retrying read after timeout. Attempt #" + str(kwargs["retry_num"])
            )
            self.__backoff(kwargs["retry_num"])
            return (self.RETRY, None)
        else:
            return (self.RETHROW, None)

    def __backoff(self, retry_num):
        # exponential backoff delay
        delay = min(self.base_delay * (2 ** (retry_num + 1)), self.max_delay)
        logger.warning(f"Backing off for {delay} seconds (retry number {retry_num})")
        time.sleep(delay)

    def on_write_timeout(self, *args, **kwargs):
        logger.warning("write timeout; propagate error to application")
        return (self.RETHROW, None)

    def on_request_error(self, query, consistency, error, retry_num):
        query_string = (
            query.prepared_statement.query_string
            if isinstance(query, BoundStatement)
            else query.query_string
        )
        if query_string.upper().startswith("SELECT ") and retry_num < self.max_retries:
            logger.debug(
                f"Error while executing request; was read; retry on next host: {error}"
            )
            return (self.RETRY_NEXT_HOST, None)
        else:
            logger.debug(
                "Error while executing request; "
                f"propagate error to application: {error}"
            )

        return (self.RETHROW, None)

    # def on_unavailable(self, *args, **kwargs):
    #     if kwargs["retry_num"] < self.max_retries:
    #         logger.debug(
    #             "Retrying request after UE. Attempt #" + str(kwargs["retry_num"])
    #         )
    #         return (self.RETRY, None)
    #     else:
    #         return (self.RETHROW, None)


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
    ) -> None:
        ports = {int(x.split(":")[1]) for x in db_nodes if ":" in x}
        nodes = [x.split(":")[0] for x in db_nodes]
        self.db_nodes = nodes
        self.db_port = list(ports)[0] if len(ports) == 1 else 9042
        self.db_username = username
        self.db_password = password
        self.cluster = None
        self.session = None
        self.prep_stmts = {}
        self._default_timeout = default_timeout

    def clone(self) -> "CassandraDb":
        return CassandraDb(
            self.db_nodes, self._default_timeout, self.db_username, self.db_password
        )

    def __repr__(self):
        return f"{', '.join(self.db_nodes)}"

    def connect(self):
        """Connect to given Cassandra cluster nodes."""
        exec_prof = ExecutionProfile(
            retry_policy=GraphsenseRetryPolicy(),
            consistency_level=ConsistencyLevel.LOCAL_QUORUM,
            serial_consistency_level=ConsistencyLevel.LOCAL_SERIAL,
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
            connect_timeout=self._default_timeout,
            idle_heartbeat_timeout=self._default_timeout,
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

    @needs_session
    def execute_safe(
        self, cql_query_str: str, params: dict, fetch_size=None
    ) -> Iterable:
        # flat_stmt = cql_query_str.replace("\n", " ")
        # logger.debug(f"{flat_stmt} in keyspace {self.session.keyspace}")
        stmt = SimpleStatement(cql_query_str, fetch_size=fetch_size)
        return self.session.execute(stmt, params)

    @needs_session
    def execute(self, cql_query_str: str, fetch_size=None) -> Iterable:
        # flat_stmt = cql_query_str.replace("\n", " ")
        # logger.debug(f"{flat_stmt} in keyspace {self.session.keyspace}")

        stmt = SimpleStatement(cql_query_str, fetch_size=None)
        if fetch_size is not None:
            stmt.fetch_size = fetch_size

        return self.session.execute(stmt)

    @needs_session
    def execute_statement(self, stmt: BoundStatement, fetch_size=None) -> Iterable:
        return self.session.execute(stmt)

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
    def execute_statements(self, statements: List[BoundStatement]):
        return execute_concurrent(
            self.session,
            [(stmt, None) for stmt in statements],
            raise_on_first_error=True,
        )

    @needs_session
    def execute_statements_async(
        self, statements: List[BoundStatement], concurrency=100
    ):
        return execute_concurrent(
            self.session,
            [(stmt, None) for stmt in statements],
            raise_on_first_error=True,
            concurrency=concurrency,
            results_generator=True,
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
        return [
            (i, future.result())
            for (i, future) in self.execute_batch_async(stmt, params)
        ]

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
        cql_str = (
            f"SELECT * FROM system_schema.columns "
            f"WHERE keyspace_name = '{keyspace}' "
            f"AND table_name = '{table}';"
        )
        return self.session.execute(cql_str)

    @needs_session
    def get_prepared_insert_statement(
        self, keyspace: str, table: str, upsert=True, cl=None
    ) -> PreparedStatement:
        """Build prepared CQL statement for specified table."""

        result_set = self.get_columns_for_table(keyspace, table)
        columns = [elem.column_name for elem in result_set._current_rows]
        ps = self.get_prepared_statement(
            build_insert_stmt(columns, table, upsert=upsert, keyspace=keyspace)
        )
        if cl is not None:
            ps.consistency_level = cl
        return ps

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
                            except Exception as exception:
                                # Individual statement retry backoff
                                individual_retry_attempt += 1
                                delay = min(
                                    base_delay * (2**individual_retry_attempt),
                                    max_delay,
                                )
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

            except Exception as exception:
                # Batch retry backoff
                retry_attempt += 1
                delay = min(base_delay * (2**retry_attempt), max_delay)
                logger.warning(
                    f"Retry after exception: {str(exception)} "
                    f"retrying another {retries} times/statements. "
                    f"Backing off for {delay} seconds"
                )
                time.sleep(delay)
                retries -= nr_statements
                if retries < 0:
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
