import pytest
import pytest_asyncio
from goodconf import GoodConfConfigDict
from testcontainers.cassandra import CassandraContainer
from testcontainers.postgres import PostgresContainer
from pathlib import Path
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

cassandra = CassandraContainer("cassandra:4.1.4")
postgres = PostgresContainer("postgres:16-alpine")

# Test data directories for tagpack tests
DATA_DIR_TP = Path(__file__).parent.resolve() / "testfiles" / "simple"
DATA_DIR_A = Path(__file__).parent.resolve() / "testfiles" / "actors"


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
        # assert result.exit_code == 0

    finally:
        # Properly dispose of the SQLAlchemy engine to close all connections
        if engine is not None:
            engine.dispose()


@pytest.fixture(scope="session", autouse=True)
def gs_db_setup(request):
    cassandra.start()

    def remove_container():
        cassandra.stop()

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
    monkeypatch.setattr(AppConfig, "model_config", GoodConfConfigDict(default_files=[]))

    data = AppConfig(environments={"pytest": pytest_env}, slack_topics={})

    assert data.underlying_file is None

    set_config(data)

    from graphsenselib.config import get_config

    app_config = get_config()

    assert list(app_config.environments.keys()) == ["pytest"]
    assert app_config.underlying_file is None
