#!/usr/bin/env python3
"""Initialize Cassandra with GraphSense test schemas at Docker build time."""

import sys

import requests
from cassandra.cluster import Cluster

TAG = "master"
SCHEMA_BASE = f"https://raw.githubusercontent.com/graphsense/graphsense-lib/{TAG}/src/graphsenselib/schema/resources/"

# Web test schemas (resttest_*)
SCHEMA_MAPPING = {"btc": "utxo", "ltc": "utxo", "eth": "account", "trx": "account_trx"}
SCHEMA_MAPPING_OVERRIDE = {("trx", "transformed"): "account"}

MAGIC_REPLACE_CONSTANT = "0x8BADF00D"
MAGIC_REPLACE_CONSTANT2 = f"{MAGIC_REPLACE_CONSTANT}_REPLICATION_CONFIG"
SIMPLE_REPLICATION_CONFIG = "{'class': 'SimpleStrategy', 'replication_factor': 1}"


def get_schema_file(filename):
    res = requests.get(SCHEMA_BASE + filename)
    res.raise_for_status()
    return res.text


def create_schema(session, keyspace, schema_name, schema_type):
    """Create a single keyspace with its schema."""
    filename = f"{schema_type}_{schema_name}_schema.sql"

    sys.stdout.write(f"Creating schema: {keyspace} from {filename}\n")
    sys.stdout.flush()

    schema_str = (
        get_schema_file(filename)
        .replace(MAGIC_REPLACE_CONSTANT2, SIMPLE_REPLICATION_CONFIG)
        .replace(MAGIC_REPLACE_CONSTANT, keyspace)
    )

    for stmt in schema_str.split(";"):
        stmt = stmt.strip()
        if stmt:
            session.execute(stmt)


def main():
    cluster = Cluster(["127.0.0.1"])
    session = cluster.connect()

    # Create web test schemas (resttest_*)
    for currency, schema_base in SCHEMA_MAPPING.items():
        for schema_type in ["raw", "transformed"]:
            schema_name = SCHEMA_MAPPING_OVERRIDE.get(
                (currency, schema_type), schema_base
            )
            keyspace = f"resttest_{currency}_{schema_type}"
            create_schema(session, keyspace, schema_name, schema_type)

    # Create root test schemas (pytest_btc_* only - used by tests/db/test_cassandra.py)
    create_schema(session, "pytest_btc_raw", "utxo", "raw")
    create_schema(session, "pytest_btc_transformed", "utxo", "transformed")

    sys.stdout.write("All schemas created successfully!\n")
    cluster.shutdown()


if __name__ == "__main__":
    main()
