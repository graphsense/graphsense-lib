from os import environ
from pathlib import Path

import pytest
import pytest_asyncio
from docker.errors import ImageNotFound, NotFound
from goodconf import GoodConfConfigDict
from testcontainers.cassandra import CassandraContainer
from testcontainers.postgres import PostgresContainer

from click.testing import CliRunner
from graphsenselib.config import AppConfig, Environment, KeyspaceConfig, IngestConfig
from graphsenselib.config.config import KeyspaceSetupConfig, set_config


# Import tagstore dependencies for test setup
try:
    from graphsenselib.tagstore.db.database import get_db_engine, init_database
    from graphsenselib.tagpack.cli import cli as tagpacktool_cli
    from graphsenselib.tagstore.db import TagstoreDbAsync

    TAGSTORE_AVAILABLE = True
except ImportError:
    TAGSTORE_AVAILABLE = False


# =============================================================================
# Container Configuration
# =============================================================================

# Cassandra image selection:
# - Default: vanilla cassandra:4.1.4 (slow startup, creates schemas at runtime)
# - Fast mode: set USE_FAST_CASSANDRA=1 to use pre-baked image with schemas
#   Build fast image with: make build-fast-cassandra
VANILLA_CASSANDRA_IMAGE = "cassandra:4.1.4"
FAST_CASSANDRA_IMAGE = environ.get(
    "CASSANDRA_TEST_IMAGE", "graphsense/cassandra-test:4.1.4"
)
USE_FAST_CASSANDRA = environ.get("USE_FAST_CASSANDRA", "").lower() in (
    "1",
    "true",
    "yes",
)

cassandra_image = (
    FAST_CASSANDRA_IMAGE if USE_FAST_CASSANDRA else VANILLA_CASSANDRA_IMAGE
)

# Cassandra is shared with web tests (imported by tests/web/conftest.py)
# Postgres is NOT shared - web tests use different tagstore data
cassandra = CassandraContainer(cassandra_image)
postgres = PostgresContainer("postgres:16-alpine")

# Test data directories for tagpack tests
DATA_DIR_TP = Path(__file__).parent.resolve() / "testfiles" / "simple"
DATA_DIR_A = Path(__file__).parent.resolve() / "testfiles" / "actors"


# =============================================================================
# Schema Creation (for slow/vanilla Cassandra mode)
# =============================================================================

SCHEMA_DIR = (
    Path(__file__).parent.parent / "src" / "graphsenselib" / "schema" / "resources"
)
SCHEMA_MAPPING = {"btc": "utxo", "ltc": "utxo", "eth": "account", "trx": "account_trx"}
SCHEMA_MAPPING_OVERRIDE = {("trx", "transformed"): "account"}
MAGIC_REPLACE_CONSTANT = "0x8BADF00D"
MAGIC_REPLACE_CONSTANT2 = f"{MAGIC_REPLACE_CONSTANT}_REPLICATION_CONFIG"
SIMPLE_REPLICATION_CONFIG = "{'class': 'SimpleStrategy', 'replication_factor': 1}"


def create_web_schemas(host, port):
    """Create web test schemas (resttest_*) in vanilla Cassandra."""
    from cassandra.cluster import Cluster

    cluster = Cluster([host], port=port)
    session = cluster.connect()

    for currency, schema_base in SCHEMA_MAPPING.items():
        for schema_type in ["raw", "transformed"]:
            schema_name = SCHEMA_MAPPING_OVERRIDE.get(
                (currency, schema_type), schema_base
            )
            filename = f"{schema_type}_{schema_name}_schema.sql"
            keyspace = f"resttest_{currency}_{schema_type}"

            schema_file = SCHEMA_DIR / filename
            if not schema_file.exists():
                raise FileNotFoundError(f"Schema file not found: {schema_file}")

            schema_str = (
                schema_file.read_text()
                .replace(MAGIC_REPLACE_CONSTANT2, SIMPLE_REPLICATION_CONFIG)
                .replace(MAGIC_REPLACE_CONSTANT, keyspace)
            )

            for stmt in schema_str.split(";"):
                stmt = stmt.strip()
                if stmt:
                    session.execute(stmt)

    cluster.shutdown()


# =============================================================================
# Shared Fixtures
# =============================================================================


def insert_test_data(db_setup):
    """Insert test data for tagstore tests"""
    if not TAGSTORE_AVAILABLE:
        return

    db_url = db_setup["db_connection_string"]
    engine = None
    try:
        engine = get_db_engine(db_url)
        init_database(engine)

        runner = CliRunner()
        result = runner.invoke(
            tagpacktool_cli,
            ["actorpack", "insert", str(DATA_DIR_A), "-u", db_url, "--no-strict-check"],
        )
        assert result.exit_code == 0

        tps = [
            (True, "config.yaml"),
            (False, "duplicate_tag.yaml"),
            (False, "empty_tag_list.yaml"),
            (True, "ex_addr_tagpack.yaml"),
            (True, "multiple_tags_for_address.yaml"),
            (True, "with_concepts.yaml"),
        ]

        for public, tpf in tps:
            result = runner.invoke(
                tagpacktool_cli,
                [
                    "tagpack",
                    "insert",
                    str(DATA_DIR_TP / tpf),
                    "-u",
                    db_url,
                    "--no-strict-check",
                    "--no-git",
                ]
                + (["--public"] if public else []),
                catch_exceptions=False,
            )

            assert result.exit_code == 0, f"Failed to insert {tpf}: {result.output}"

        result = runner.invoke(
            tagpacktool_cli, ["tagstore", "refresh-views", "-u", db_url]
        )

        assert result.exit_code == 0, f"Failed to refresh views: {result.output}"

    finally:
        # Properly dispose of the SQLAlchemy engine to close all connections
        if engine is not None:
            engine.dispose()


@pytest.fixture(scope="session", autouse=True)
def gs_db_setup(request):
    """Start Cassandra container (shared across all tests)."""
    try:
        cassandra.start()
    except ImageNotFound as e:
        if USE_FAST_CASSANDRA and "graphsense/cassandra-test" in str(e):
            raise RuntimeError(
                f"Fast Cassandra image not found: {FAST_CASSANDRA_IMAGE}\n"
                "You need to build it first with: make build-fast-cassandra\n"
                "Or run tests without USE_FAST_CASSANDRA=1 (slower)"
            ) from e
        raise

    def remove_container():
        try:
            cassandra.stop()
        except NotFound:
            pass  # Already stopped by another fixture

    request.addfinalizer(remove_container)

    cas_host = cassandra.get_container_host_ip()
    cas_port = cassandra.get_exposed_port(9042)

    return (cas_host, cas_port)


@pytest.fixture(scope="session")
def db_setup(request):
    """PostgreSQL database setup for tagstore tests"""
    if not TAGSTORE_AVAILABLE:
        pytest.skip("Tagstore dependencies not available")

    postgres.start()

    def remove_container():
        try:
            postgres.stop()
        except Exception:
            # Ignore errors during container cleanup
            pass

    request.addfinalizer(remove_container)

    postgres_sync_url = postgres.get_connection_url()
    portgres_async_url = postgres_sync_url.replace("psycopg2", "asyncpg")

    setup = {
        "db_connection_string": postgres_sync_url.replace("+psycopg2", ""),
        "db_connection_string_psycopg2": postgres_sync_url,
        "db_connection_string_async": portgres_async_url,
    }

    insert_test_data(setup)

    return setup


@pytest_asyncio.fixture
async def async_tagstore_db(db_setup):
    """Async database fixture that properly manages connection lifecycle"""
    if not TAGSTORE_AVAILABLE:
        pytest.skip("Tagstore dependencies not available")

    db = TagstoreDbAsync.from_url(db_setup["db_connection_string_async"])
    try:
        yield db
    finally:
        # Properly dispose of the async engine to close all connections
        await db.engine.dispose()


@pytest.fixture(autouse=True)
def patch_config(gs_db_setup, monkeypatch):
    cas_host, cas_port = gs_db_setup

    # to load more data for testing replace with a real node URL
    # afterwards replace the real node url in the casset files.
    node_url_btc = "http://test-data-btc"

    pytest_ks_btc = KeyspaceConfig(
        raw_keyspace_name="pytest_btc_raw",
        transformed_keyspace_name="pytest_btc_transformed",
        schema_type="utxo",
        ingest_config=IngestConfig(
            node_reference=node_url_btc,
            secondary_node_references=[],
            raw_keyspace_file_sinks={},
        ),
        keyspace_setup_config={
            "raw": KeyspaceSetupConfig(
                data_configuration={
                    "id": "pytest_btc_raw",
                    "block_bucket_size": 100,
                    "tx_bucket_size": 25000,
                    "tx_prefix_length": 5,
                }
            ),
            "transformed": KeyspaceSetupConfig(
                data_configuration={
                    "keyspace_name": "pytest_btc_transformed",
                    "address_prefix_length": 3,
                    "bech_32_prefix": "bc1",
                    "bucket_size": 5000,
                    "coinjoin_filtering": True,
                    "fiat_currencies": ["EUR", "USD"],
                }
            ),
        },
    )

    pytest_keyspaces = {"btc": pytest_ks_btc}

    pytest_env = Environment(
        cassandra_nodes=[f"{cas_host}:{cas_port}"],
        keyspaces=pytest_keyspaces,
        username="test_user",
        password="test_password",
    )

    # make sure none of the configured files are loaded.
    monkeypatch.setattr(
        AppConfig,
        "model_config",
        GoodConfConfigDict(default_files=[], env_prefix="GRAPHSENSE_PYTEST_"),
    )

    data = AppConfig(environments={"pytest": pytest_env}, slack_topics={})

    assert data.underlying_file is None

    set_config(data)

    from graphsenselib.config import get_config

    app_config = get_config()

    assert list(app_config.environments.keys()) == ["pytest"]
    assert app_config.underlying_file is None
