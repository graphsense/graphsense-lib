import pytest
from goodconf import GoodConfConfigDict
from testcontainers.cassandra import CassandraContainer

from graphsenselib.config import AppConfig, Environment, KeyspaceConfig
from graphsenselib.config.config import KeyspaceSetupConfig, set_config

cassandra = CassandraContainer("cassandra:4.1.4")


@pytest.fixture(scope="session", autouse=True)
def gs_db_setup(request):
    cassandra.start()

    def remove_container():
        cassandra.stop()

    request.addfinalizer(remove_container)

    cas_host = cassandra.get_container_host_ip()
    cas_port = cassandra.get_exposed_port(9042)

    return (cas_host, cas_port)


@pytest.fixture(autouse=True)
def patch_config(gs_db_setup, monkeypatch):
    cas_host, cas_port = gs_db_setup
    pytest_ks_btc = KeyspaceConfig(
        raw_keyspace_name="pytest_btc_raw",
        transformed_keyspace_name="pytest_btc_transformed",
        schema_type="utxo",
        ingest_config=None,
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
        cassandra_nodes=[f"{cas_host}:{cas_port}"], keyspaces=pytest_keyspaces
    )

    # make sure none of the configured files are loaded.
    monkeypatch.setattr(AppConfig, "model_config", GoodConfConfigDict(default_files=[]))

    data = AppConfig(environments={"pytest": pytest_env}, slack_topics={})

    assert data.underlying_file is None

    set_config(data)

    from graphsenselib.config import get_config

    app_config = get_config()

    assert list(app_config.environments.keys()) == ["pytest"]
