"""Shared helpers for interpreting Cassandra connection settings.

Kept dependency-free (no cassandra driver import) so the low-level ingest paths
and the Spark session builder can both use it without pulling in the driver.
"""

import logging
from typing import Iterable, List, Tuple

logger = logging.getLogger(__name__)

DEFAULT_CASSANDRA_PORT = 9042


def split_nodes_and_port(db_nodes: Iterable[str]) -> Tuple[List[str], int]:
    """Split ``cassandra_nodes`` entries into bare hosts plus a single port.

    Accepts a mix of ``"host"`` and ``"host:port"`` entries — the form used
    throughout the graphsense config. The driver takes one port for all contact
    points, so the first port found wins; conflicting ports are warned about
    rather than silently dropped. Falls back to
    ``DEFAULT_CASSANDRA_PORT`` when no entry carries one.
    """
    db_nodes = list(db_nodes)
    ports_in_order = [int(x.split(":")[1]) for x in db_nodes if ":" in x]
    hosts = [x.split(":")[0] for x in db_nodes]

    unique_ports = set(ports_in_order)
    if len(unique_ports) > 1:
        logger.warning(
            "cassandra_nodes specify conflicting ports %s; using the "
            "first one (%d). Use the same port for all nodes to silence "
            "this warning.",
            sorted(unique_ports),
            ports_in_order[0],
        )

    port = ports_in_order[0] if ports_in_order else DEFAULT_CASSANDRA_PORT
    return hosts, port
