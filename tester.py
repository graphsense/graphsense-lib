import logging
import sys

from src.graphsenselib.config import config, currency_to_schema_type
from src.graphsenselib.db import DbFactory
from src.graphsenselib.ingest.factory import IngestFactory
from src.graphsenselib.ingest.parquet import SCHEMA_MAPPING
from src.graphsenselib.schema import GraphsenseSchemas

logger = logging.getLogger(__name__)


def ingest(
    env,
    currency,
    sinks,
    start_block,
    end_block,
    batch_size,
    timeout,
    info,
    previous_day,
    create_schema,
    mode,
):
    """Ingests cryptocurrency data form the client/node to the graphsense db
    \f
    Args:
        env (str): Environment to work on
        currency (str): currency to work on
    """
    ks_config = config.get_keyspace_config(env, currency)
    provider = ks_config.ingest_config.get_first_node_reference()
    parquet_file_sink_config = ks_config.ingest_config.raw_keyspace_file_sinks.get(
        "parquet", None
    )

    if ks_config.schema_type == "account" and mode != "legacy":
        logger.error(
            "Only legacy mode is available for account type currencies. Exiting."
        )
        sys.exit(11)

    parquet_file_sink = (
        parquet_file_sink_config.directory
        if parquet_file_sink_config is not None
        else None
    )

    if create_schema:
        GraphsenseSchemas().create_keyspace_if_not_exist(
            env, currency, keyspace_type="raw"
        )

    def create_sink_config(sink, currency):
        schema_type = currency_to_schema_type[currency]
        return (
            {
                "output_directory": parquet_file_sink,
                "schema": SCHEMA_MAPPING[schema_type],
            }
            if sink == "parquet" and schema_type == "account"
            else {}
        )

    with DbFactory().from_config(env, currency) as db:
        IngestFactory().from_config(env, currency).ingest(
            db=db,
            currency=currency,
            source=provider,
            sink_config={k: create_sink_config(k, currency) for k in sinks},
            user_start_block=start_block,
            user_end_block=end_block,
            batch_size=batch_size,
            info=info,
            previous_day=previous_day,
            provider_timeout=timeout,
            mode=mode,
        )


if __name__ == "__main__":
    env = "dev"
    currency = "trx"
    sinks = ["cassandra"]
    start_block = 50_000_500  # 20_000
    end_block = 50_000_600  # 20_100
    batch_size = 50
    timeout = 3600
    info = False
    previous_day = False
    create_schema = True
    mode = "legacy"
    ingest(
        env,
        currency,
        sinks,
        start_block,
        end_block,
        batch_size,
        timeout,
        info,
        previous_day,
        create_schema,
        mode,
    )
