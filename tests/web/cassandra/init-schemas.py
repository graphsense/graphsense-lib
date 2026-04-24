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

# Mirror of DISCOVERY_KEYSPACES in tests/conftest.py. Keep both in sync.
DISCOVERY_KEYSPACES = [
    {"name": "disctest_btc_transformed_20260101", "kind": "transformed",
     "no_blocks": 100, "has_state": True, "bootstrapped": True},
    {"name": "disctest_btc_transformed_20260201", "kind": "transformed",
     "no_blocks": 500, "has_state": True, "bootstrapped": True},
    {"name": "disctest_btc_transformed_20260301", "kind": "transformed",
     "no_blocks": None, "has_state": True, "bootstrapped": False},
    {"name": "disctest_btc_transformed_20260401", "kind": "transformed",
     "no_blocks": 200, "has_state": True, "bootstrapped": True},
    {"name": "disctest_btc_transformed_20260501", "kind": "transformed",
     "no_blocks": 999, "has_state": True, "bootstrapped": False},
    {"name": "disctest_btc_raw", "kind": "raw",
     "has_configuration": True, "has_state": True, "bootstrapped": True},
    {"name": "disctest_btc_raw_20260101", "kind": "raw",
     "has_configuration": True, "has_state": True, "bootstrapped": True},
    {"name": "disctest_btc_raw_20260201_prod", "kind": "raw",
     "has_configuration": True, "has_state": True, "bootstrapped": True},
    {"name": "disctest_btc_raw_20260301", "kind": "raw",
     "has_configuration": True, "has_state": True, "bootstrapped": True},
    {"name": "disctest_btc_raw_20260401", "kind": "raw",
     "has_configuration": True, "has_state": True, "bootstrapped": False},
    {"name": "discbc_btc_transformed_20260301", "kind": "transformed",
     "no_blocks": 700, "has_state": False, "bootstrapped": False},
    {"name": "discbc_btc_raw_20260301", "kind": "raw",
     "has_configuration": True, "has_state": False, "bootstrapped": False},
    {"name": "discother_btc_raw_20260301", "kind": "raw",
     "has_configuration": True, "has_state": True, "bootstrapped": False,
     "other_state_keys": ["in_progress"]},
]


def create_discovery_keyspaces(session):
    for ks in DISCOVERY_KEYSPACES:
        name = ks["name"]
        session.execute(
            f"CREATE KEYSPACE IF NOT EXISTS {name} "
            f"WITH replication = {SIMPLE_REPLICATION_CONFIG}"
        )
        if ks["kind"] == "transformed":
            session.execute(
                f"CREATE TABLE IF NOT EXISTS {name}.summary_statistics ("
                "id int PRIMARY KEY, no_blocks bigint)"
            )
            if ks["no_blocks"] is not None:
                session.execute(
                    f"INSERT INTO {name}.summary_statistics (id, no_blocks) "
                    f"VALUES (0, {ks['no_blocks']})"
                )
        elif ks["kind"] == "raw":
            if ks.get("has_configuration"):
                session.execute(
                    f"CREATE TABLE IF NOT EXISTS {name}.configuration ("
                    "keyspace_name text PRIMARY KEY)"
                )
                session.execute(
                    f"INSERT INTO {name}.configuration (keyspace_name) VALUES ('{name}')"
                )

        if ks.get("has_state"):
            session.execute(
                f"CREATE TABLE IF NOT EXISTS {name}.state ("
                "key text PRIMARY KEY, value text, updated_at timestamp)"
            )
            if ks.get("bootstrapped"):
                session.execute(
                    f"INSERT INTO {name}.state (key, value, updated_at) "
                    f"VALUES ('bootstrapped', '2026-04-24T00:00:00+00:00', "
                    f"toTimestamp(now()))"
                )
            for other_key in ks.get("other_state_keys", []):
                session.execute(
                    f"INSERT INTO {name}.state (key, value, updated_at) "
                    f"VALUES ('{other_key}', 'sentinel', toTimestamp(now()))"
                )


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

    create_discovery_keyspaces(session)

    sys.stdout.write("All schemas created successfully!\n")
    cluster.shutdown()


if __name__ == "__main__":
    main()
