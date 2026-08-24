"""list_links must scan the smaller side, also when a count is int32-wrapped.

The scan side is picked by comparing src.no_outgoing_txs against
dst.no_incoming_txs. Those columns are 32-bit and an older Spark jar stored a
count past 2**31 as a NEGATIVE number, so the raw comparison inverted the
choice for exactly the busiest nodes: the TRON USDT contract holds
-580,448,962, which made every counterparty look larger and sent the scan into
its ~3.7e9-row history instead of the other side's few thousand rows. Every
/links request touching such a node then ran until the endpoint timeout.

DB-free: the real list_links is bound to a fake self, and the candidate fetch
raises a sentinel as soon as the side has been chosen — the side is the whole
assertion.
"""

import asyncio
from types import SimpleNamespace

import pytest

from graphsenselib.datatypes.common import NodeType
from graphsenselib.db.asynchronous.cassandra import Cassandra

CURRENCY = "trx"

# TSMnejGCoV8dX4GfCPcLS77g99SLZG8Kne -> TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t
SMALL = "0x1111111111111111111111111111111111111111"
USDT = "0xa614f803b6fd780986a42c78ec9c7f77e6ded13c"

SMALL_OUTGOING_TXS = 3168
# what trx_transformed holds for the USDT contract; true value ~3.7e9
USDT_INCOMING_TXS_WRAPPED = -580448962


class _StopAfterSideChoice(Exception):
    def __init__(self, scanned_id):
        self.scanned_id = scanned_id


def _node(no_outgoing_txs, no_incoming_txs):
    return {
        "no_outgoing_txs": no_outgoing_txs,
        "no_incoming_txs": no_incoming_txs,
        "first_tx_id": 1,
        "last_tx_id": 10**9,
        "total_tokens_spent": {"USDT": 1},
        "total_tokens_received": {"USDT": 1},
    }


def _make_self(nodes):
    async def get_address(currency, address):
        return nodes[address]

    async def get_address_id(currency, address):
        return abs(hash(address)) % 10**6

    async def list_neighbors(currency, id, is_outgoing, node_type, **kwargs):
        # a real, sparse edge: the pre-check must not short-circuit
        return [{"no_transactions": 5}], None

    async def resolve_tx_id_range_by_block(currency, min_height, max_height):
        return None, None

    async def list_address_txs_ordered(**kwargs):
        raise _StopAfterSideChoice(kwargs["id"])

    return SimpleNamespace(
        tconfig=SimpleNamespace(
            fanout_bounding_and_links_precheck_enabled=True,
            links_adaptive_fetch_size_cap=1000,
            links_directed_probe_enabled=True,
            # the race spawns recursive scans; side selection is what is
            # under test here, so keep a single scan
            links_sparse_direction_race_enabled=False,
            links_direction_race_min_candidates=100,
            links_slim_candidate_columns_enabled=True,
            links_candidate_prefetch_enabled=True,
            links_per_tx_asset_probe_enabled=True,
        ),
        logger=SimpleNamespace(
            debug=lambda *a, **k: None, warning=lambda *a, **k: None
        ),
        get_address=get_address,
        resolve_tx_id_range_by_block=resolve_tx_id_range_by_block,
        get_address_id=get_address_id,
        list_neighbors=list_neighbors,
        list_address_txs_ordered=list_address_txs_ordered,
        get_token_configuration=lambda currency: {"USDT": {}},
        _warn_unconfigured_tokens=lambda *a, **k: None,
    )


def _scanned_side(src, dst, nodes):
    s = _make_self(nodes)
    with pytest.raises(_StopAfterSideChoice) as e:
        asyncio.run(
            Cassandra.list_links(
                s,
                CURRENCY,
                NodeType.ADDRESS,
                src,
                dst,
                order="asc",
                pagesize=25,
            )
        )
    return e.value.scanned_id


def test_wrapped_neighbor_count_does_not_invert_the_scan_side():
    """A negative (wrapped) neighbor count must not make it look like the
    smaller side. Pre-fix this scanned USDT and timed out."""
    nodes = {
        SMALL: _node(SMALL_OUTGOING_TXS, 5359),
        USDT: _node(0, USDT_INCOMING_TXS_WRAPPED),
    }
    assert _scanned_side(SMALL, USDT, nodes) == SMALL


def test_unwrapped_counts_still_pick_the_smaller_side():
    """The ordinary case is unchanged: the larger neighbor is not scanned."""
    nodes = {
        SMALL: _node(SMALL_OUTGOING_TXS, 5359),
        USDT: _node(0, 97_956_203),
    }
    assert _scanned_side(SMALL, USDT, nodes) == SMALL


def test_smaller_neighbor_side_is_still_chosen_over_a_large_source():
    """When the neighbor's incoming side really is the smaller one, it stays
    the scanned side — the fix must not just always pick the source."""
    big_src = "0x2222222222222222222222222222222222222222"
    nodes = {
        big_src: _node(31_287_540, 608_197),
        SMALL: _node(0, 5359),
    }
    assert _scanned_side(big_src, SMALL, nodes) == SMALL
