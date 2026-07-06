"""Address->entity resolution serves the legacy cluster id.

``get_address_entity_id`` resolves an address to the legacy ``address.cluster_id``
unconditionally: entity ids are self-describing (fresh ids live above
``FRESH_CLUSTER_ID_OFFSET``), and an address's fresh cluster is discoverable via
the shifted ``fresh_cluster_id`` field on address responses, so this legacy
resolution needs no switch and never mixes the two id spaces.

DB-free: the real ``Cassandra.get_address_entity_id`` is bound to a fake self
that stands in for its db dependencies.
"""

import asyncio
from types import SimpleNamespace

from graphsenselib.db.asynchronous.cassandra import Cassandra
from graphsenselib.utils.constants import FRESH_CLUSTER_ID_OFFSET


class _Result:
    def __init__(self, row):
        self._row = row

    def one(self):
        return self._row


def _make_self(legacy_cluster_id, address_id=42):
    async def get_address_id_id_group(currency, address):
        return address_id, 0

    async def get_fresh_cluster_id(currency, aid):
        raise AssertionError("legacy resolution must not consult fresh tables")

    async def execute_async(currency, keyspace, query, params):
        return _Result({"cluster_id": legacy_cluster_id})

    return SimpleNamespace(
        get_address_id_id_group=get_address_id_id_group,
        get_fresh_cluster_id=get_fresh_cluster_id,
        execute_async=execute_async,
        get_id_group=lambda keyspace, id_: 0,
    )


def test_returns_legacy_cluster_id():
    s = _make_self(legacy_cluster_id=1396178)
    result = asyncio.run(Cassandra.get_address_entity_id(s, "ltc", "Laddr"))
    assert result == 1396178


def test_never_returns_a_fresh_space_id():
    s = _make_self(legacy_cluster_id=1396178)
    result = asyncio.run(Cassandra.get_address_entity_id(s, "ltc", "Laddr"))
    assert result < FRESH_CLUSTER_ID_OFFSET
