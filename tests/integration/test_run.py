import asyncio
import pytest

from graphsenselib.config.cassandra_async_config import CassandraConfig, CurrencyConfig
from graphsenselib.db.asynchronous.cassandra import Cassandra
from graphsenselib.db.asynchronous.services.blocks_service import BlocksService
from graphsenselib.db.factory import DbFactory
from graphsenselib.schema.schema import GraphsenseSchemas
from ..helpers import vcr_default_params
from click.testing import CliRunner

from graphsenselib.ingest.cli import ingest


pytest.importorskip("bitcoinetl")


class MockLogger:
    level = 0

    def info(self, msg):
        pass

    def warning(self, msg):
        pass

    def error(self, msg):
        pass


@pytest.mark.vcr(**vcr_default_params)
def test_pipeline():
    runner = CliRunner()
    result = runner.invoke(
        ingest,
        [
            "-c",
            "btc",
            "-e",
            "pytest",
            "--mode",
            "utxo_with_tx_graph",
            "--start-block",
            "0",
            "--end-block",
            "100",
            "--create-schema",
        ],
    )

    assert result.exit_code == 0

    assert "" in result.output

    with DbFactory().from_config("pytest", "btc") as db:
        # Check if the keyspace was created
        assert db.raw.exists()

        # assert not db.transformed.exists()

        GraphsenseSchemas().create_keyspaces_if_not_exist("pytest", "btc")

        assert db.transformed.exists()

    from graphsenselib.config import get_config

    app_config = get_config()

    envConfig = app_config.get_environment("pytest")
    ks_config = app_config.get_keyspace_config("pytest", "btc")

    cas_host, cas_port = envConfig.cassandra_nodes[0].split(":")

    # Now verify the ingested data using async services
    asyncio.run(
        _verify_ingested_data(
            cas_host,
            int(cas_port),
            ks_config.raw_keyspace_name,
            ks_config.transformed_keyspace_name,
        )
    )


async def _verify_ingested_data(cas_host: str, cas_port: int, raw, transformed):
    """Verify the ingested data using async data access services."""

    logger = MockLogger()

    currencyConf = {"btc": CurrencyConfig(raw=raw, transformed=transformed)}

    db = Cassandra(
        CassandraConfig(nodes=[cas_host], port=cas_port, currencies=currencyConf),
        logger=logger,
    )

    # Mock dependencies for services
    class MockRatesService:
        async def get_rates(self, currency: str, height: int = None):
            return type("Rates", (), {"rates": {"USD": 50000.0}})()

    class MockConfig:
        block_by_date_use_linear_search = False

    # Initialize services
    rates_service = MockRatesService()
    config = MockConfig()

    blocks_service = BlocksService(db, rates_service, config, logger)

    block = await blocks_service.get_block(currency="btc", height=1)

    assert block is not None
    assert block.currency == "btc"
    assert block.height == 1
    assert block.timestamp == 1231469665
    assert (
        block.block_hash
        == "00000000839a8e6886ab5951d76f411475428afc90947ee320161bbf18eb6048"
    )
    assert block.no_txs == 1
