"""Bootstrap mapping semantics of ``gs_clustering.get_mapping_min`` (DB-free).

The crate owns what ``_mapping_to_write_arrays`` used to do in Python: the
union-find links by minimum so ``cluster_id == min(address_id)`` with no
relabel pass — the survivor rule shared with the incremental delta path — and
the returned Arrow batch carries only the rows the bootstrap writes: (a) the
coinbase placeholder (address 0) is dropped, (b) with ``skip_singletons``
size-1 clusters are dropped too, (c) rows come in ascending address_id order
(the fresh_address_cluster write relies on partition-key contiguity).
"""

import numpy as np
import pytest

gs_clustering = pytest.importorskip(
    "gs_clustering", reason="gs_clustering is required (make build-rust)"
)


def _mapping(max_address_id, unions, skip_singletons):
    c = gs_clustering.Clustering(max_address_id=max_address_id)
    c.process_transactions([list(u) for u in unions])
    batch = c.get_mapping_min(skip_singletons)
    return (
        batch.column("address_id").to_pylist(),
        batch.column("cluster_id").to_pylist(),
    )


def test_relabels_to_min_member_and_drops_placeholder():
    # components: {1,2,4} and {3,5}; the placeholder (0) is its own component.
    aid, cid = _mapping(5, [[1, 2, 4], [3, 5]], skip_singletons=True)

    assert aid == [1, 2, 3, 4, 5]
    assert cid == [1, 1, 3, 1, 3]


def test_skip_singletons_drops_size_one_clusters_only():
    # {1,2} clustered; address 3 is a genuine singleton.
    aid, cid = _mapping(3, [[1, 2]], skip_singletons=True)

    assert aid == [1, 2]
    assert cid == [1, 1]


def test_keep_singletons_when_not_skipping():
    aid, cid = _mapping(3, [[1, 2]], skip_singletons=False)

    assert aid == [1, 2, 3]
    assert cid == [1, 1, 3]


def test_placeholder_can_be_min_label():
    # If address 0 was ever unioned, members keep cluster_id 0 while the row
    # for address 0 itself is dropped.
    aid, cid = _mapping(2, [[0, 2]], skip_singletons=True)

    assert aid == [2]
    assert cid == [0]


def test_batch_is_uint32_and_zero_copy_viewable():
    c = gs_clustering.Clustering(max_address_id=5)
    c.process_transactions([[1, 2, 4], [3, 5]])
    batch = c.get_mapping_min(True)

    aid = batch.column("address_id").to_numpy()
    cid = batch.column("cluster_id").to_numpy()
    assert aid.dtype == np.uint32 and cid.dtype == np.uint32


def test_dense_get_mapping_still_full_range_min_labelled():
    # The delta path drives small densified id spaces through get_mapping();
    # it must stay one row per id, 0..=max, now min-labelled.
    c = gs_clustering.Clustering(max_address_id=5)
    c.process_transactions([[1, 2, 4], [3, 5]])
    batch = c.get_mapping()

    assert batch.column("address_id").to_pylist() == [0, 1, 2, 3, 4, 5]
    assert batch.column("cluster_id").to_pylist() == [0, 1, 1, 3, 1, 3]
