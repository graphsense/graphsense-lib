import os
from pathlib import Path

from cassandra.cluster import Cluster

DATA_DIR = Path(__file__).parent.resolve() / "data"


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
