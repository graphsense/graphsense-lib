"""Applying v3 DDL to a cluster.

The only place this package talks to Cassandra outside Spark. Every statement it
can execute comes from the rendered schema, and the keyspace name it targets is
checked against the v3 pattern first, so it cannot alter a live keyspace.
"""

from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from graphsense_v3.settings import RunSettings

logger = logging.getLogger(__name__)

_COMMENT = re.compile(r"^\s*--.*$", re.MULTILINE)


def statements(cql: str) -> list[str]:
    """Split rendered CQL into executable statements, comments removed.

    Safe to split on ``;`` only because the renderer never emits one inside a
    literal -- option maps and collection types contain commas, not semicolons.
    """
    stripped = _COMMENT.sub("", cql)
    return [s.strip() for s in stripped.split(";") if s.strip()]


def keyspace_of(cql: str) -> str | None:
    """The keyspace a rendered schema creates."""
    match = re.search(r"CREATE KEYSPACE IF NOT EXISTS\s+(\w+)", cql)
    return match.group(1) if match else None


def apply_cql(settings: "RunSettings", cql: str) -> int:
    """Execute rendered DDL against the configured cluster. Returns the count.

    ``USE`` is honoured by the driver session, so the statements run in the
    order the renderer emitted them.
    """
    from cassandra.auth import PlainTextAuthProvider
    from cassandra.cluster import Cluster

    from graphsense_v3.settings import assert_v3_keyspace
    from graphsenselib.utils.cassandra import split_nodes_and_port

    target = keyspace_of(cql)
    if target is None:
        raise ValueError("rendered CQL creates no keyspace; refusing to execute it")
    assert_v3_keyspace(target)

    hosts, port = split_nodes_and_port(settings.cassandra_nodes)
    auth = (
        PlainTextAuthProvider(username=settings.username, password=settings.password)
        if settings.username
        else None
    )
    cluster = Cluster(hosts, port=port, auth_provider=auth)
    session = cluster.connect()
    try:
        applied = 0
        for statement in statements(cql):
            logger.debug("CQL: %s", statement.split("\n", 1)[0])
            session.execute(statement)
            applied += 1
        return applied
    finally:
        cluster.shutdown()
