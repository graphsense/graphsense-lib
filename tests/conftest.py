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

VANILLA_CASSANDRA_IMAGE = "cassandra:4.1.4"
FAST_CASSANDRA_IMAGE = environ.get(
    "CASSANDRA_TEST_IMAGE", "graphsense/cassandra-test:4.1.4"
)
DANGEROUSLY_ACCELERATE_TESTS = environ.get(
    "DANGEROUSLY_ACCELERATE_TESTS", ""
).lower() in (
    "1",
    "true",
    "yes",
)

cassandra_image = (
    FAST_CASSANDRA_IMAGE if DANGEROUSLY_ACCELERATE_TESTS else VANILLA_CASSANDRA_IMAGE
)

# Shared with web tests (imported by tests/web/conftest.py)
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
        if engine is not None:
            engine.dispose()


# Updated by gs_db_setup when Cassandra starts; used by patch_config automatically.
_cassandra_coords = ("localhost", "9042")


@pytest.fixture(scope="session")
def gs_db_setup(request):
    """Start Cassandra container. Only tests requesting this fixture pay the cost."""
    global _cassandra_coords
    try:
        cassandra.start()
    except ImageNotFound as e:
        if DANGEROUSLY_ACCELERATE_TESTS and "graphsense/cassandra-test" in str(e):
            raise RuntimeError(
                f"Fast Cassandra image not found: {FAST_CASSANDRA_IMAGE}\n"
                "Build it with: make build-fast-cassandra\n"
                "Or run without DANGEROUSLY_ACCELERATE_TESTS=1 (slower)"
            ) from e
        raise

    def cleanup():
        try:
            cassandra.stop()
        except (NotFound, Exception):
            pass

    request.addfinalizer(cleanup)

    cas_host = cassandra.get_container_host_ip()
    cas_port = cassandra.get_exposed_port(9042)
    _cassandra_coords = (cas_host, cas_port)
    return (cas_host, cas_port)


@pytest.fixture(scope="session")
def db_setup(request):
    """PostgreSQL database setup for tagstore tests."""
    if not TAGSTORE_AVAILABLE:
        pytest.skip("Tagstore dependencies not available")

    postgres.start()

    def cleanup():
        try:
            postgres.stop()
        except Exception:
            pass

    request.addfinalizer(cleanup)

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
        await db.engine.dispose()


def _build_test_config(cas_host="localhost", cas_port="9042"):
    """Build a test AppConfig with the given Cassandra coordinates."""
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

    return AppConfig(environments={"pytest": pytest_env}, slack_topics={})


@pytest.fixture(autouse=True)
def patch_config(monkeypatch):
    """Patch AppConfig for all tests.

    Uses dummy Cassandra coordinates by default. Once gs_db_setup has run
    (because some test requested it), automatically uses real coordinates.
    """
    monkeypatch.setattr(
        AppConfig,
        "model_config",
        GoodConfConfigDict(default_files=[], env_prefix="GRAPHSENSE_PYTEST_"),
    )
    set_config(_build_test_config(*_cassandra_coords))
