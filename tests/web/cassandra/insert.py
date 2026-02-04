import os
from pathlib import Path

from cassandra.cluster import Cluster

DATA_DIR = Path(__file__).parent.resolve() / "data"
SCHEMA_DIR = Path(__file__).parent.parent.parent.parent / "src" / "graphsenselib" / "schema" / "resources"

SCHEMA_MAPPING = {"btc": "utxo", "ltc": "utxo", "eth": "account", "trx": "account_trx"}
SCHEMA_MAPPING_OVERRIDE = {("trx", "transformed"): "account"}

MAGIC_REPLACE_CONSTANT = "0x8BADF00D"
MAGIC_REPLACE_CONSTANT2 = f"{MAGIC_REPLACE_CONSTANT}_REPLICATION_CONFIG"
SIMPLE_REPLICATION_CONFIG = "{'class': 'SimpleStrategy', 'replication_factor': 1}"


def create_schemas(host, port):
    """Create test schemas in vanilla Cassandra (slow mode)."""
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


def load_test_data(host, port):
    """Load test data into Cassandra (schemas must already exist)."""
    cluster = Cluster([host], port=port)
    session = cluster.connect()

    # Collect all insert statements
    inserts = []
    for file_path in DATA_DIR.iterdir():
        if not file_path.is_file():
            continue
        table_name = os.path.basename(file_path)
        content = file_path.read_text()
        for line in content.split("\n"):
            line = line.strip()
            if line:
                inserts.append(f"INSERT INTO {table_name} JSON '{line}'")

    # Execute inserts concurrently using async Cassandra driver
    futures = [session.execute_async(stmt) for stmt in inserts]

    # Wait for all inserts to complete
    for future in futures:
        future.result()

    cluster.shutdown()
