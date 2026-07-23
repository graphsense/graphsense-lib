"""Out-of-range entity ids must behave like absent ids, never 500.

Public entity ids can only exist in [0, 2**31) (legacy, int32-bound in
Cassandra and the tagstore) or [2**33, 2**33 + 2**31) (fresh, offset +
int32-bound raw id). Ids in the gap or beyond used to reach the databases
unchecked and blow up on the int32/int4 binds (asyncpg DataError on
/clusters/{id}/tags, cassandra struct.error on /entities/{id}).
"""

from graphsenselib.utils.constants import FRESH_CLUSTER_ID_OFFSET

from tests.web.helpers import raw_request, request_with_status

DEAD_ZONE_ID = 2280857679
BEYOND_FRESH_ID = FRESH_CLUSTER_ID_OFFSET + 2**31


def test_out_of_range_entity_404s(client):
    for bad_id in (DEAD_ZONE_ID, BEYOND_FRESH_ID):
        for currency in ("eth", "btc"):
            request_with_status(client, f"/{currency}/entities/{bad_id}", 404)
            request_with_status(client, f"/{currency}/clusters/{bad_id}", 404)


def test_out_of_range_cluster_tags_empty(client):
    result = request_with_status(
        client, f"/eth/clusters/{DEAD_ZONE_ID}/tags?pagesize=10", 200
    )
    assert result == {"address_tags": []}
    result = request_with_status(
        client, f"/btc/clusters/{BEYOND_FRESH_ID}/tags?pagesize=10", 200
    )
    assert result == {"address_tags": []}


def test_out_of_range_entity_subresources_match_absent_behavior(client):
    result = request_with_status(
        client, f"/eth/entities/{DEAD_ZONE_ID}/neighbors?direction=out", 200
    )
    assert result == {"neighbors": []}

    result = request_with_status(client, f"/eth/entities/{DEAD_ZONE_ID}/addresses", 200)
    assert result == {"addresses": []}

    result = request_with_status(client, f"/eth/entities/{DEAD_ZONE_ID}/txs", 200)
    assert result == {"address_txs": []}

    status, _ = raw_request(client, f"/eth/entities/{DEAD_ZONE_ID}/links?neighbor=1")
    assert status == 404
