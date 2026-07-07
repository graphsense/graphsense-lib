"""Min-relabel + filter of the Rust cluster mapping (PHASE 2), DB-free.

``_mapping_to_write_arrays`` consumes ``get_mapping()``'s Arrow batch (one row
per address id, dense ascending) where each component carries an arbitrary
member as its label, and must (a) relabel every cluster to min(address_id) —
the survivor rule shared with the incremental delta path, (b) drop the
coinbase placeholder (address 0), (c) with ``skip_singletons`` drop size-1
clusters, and (d) reject batches violating the dense-ascending contract the
numpy scatter relabel relies on.
"""

import numpy as np
import pytest

pa = pytest.importorskip("pyarrow", reason="pyarrow is required (spark extra)")

from graphsenselib.transformation.clustering import (  # noqa: E402
    _mapping_to_write_arrays,
)


def _batch(cluster_ids):
    n = len(cluster_ids)
    return pa.RecordBatch.from_arrays(
        [
            pa.array(np.arange(n, dtype=np.uint32)),
            pa.array(np.asarray(cluster_ids, dtype=np.uint32)),
        ],
        names=["address_id", "cluster_id"],
    )


def test_relabels_to_min_member_and_drops_placeholder():
    # components: {1,2,4} labelled by member 4, {3,5} by member 5; the
    # placeholder (0) is its own component.
    batch = _batch([0, 4, 4, 5, 4, 5])

    aid_w, cid_w, write_rows, skipped = _mapping_to_write_arrays(
        batch, max_address_id=5, skip_singletons=True
    )

    assert list(aid_w) == [1, 2, 3, 4, 5]
    assert list(cid_w) == [1, 1, 3, 1, 3]
    assert write_rows == 5
    assert skipped == 1  # the placeholder
    assert aid_w.dtype == np.uint32 and cid_w.dtype == np.uint32


def test_skip_singletons_drops_size_one_clusters_only():
    # {1,2} labelled by member 2; address 3 is a genuine singleton.
    batch = _batch([0, 2, 2, 3])

    aid_w, cid_w, _write_rows, skipped = _mapping_to_write_arrays(
        batch, max_address_id=3, skip_singletons=True
    )

    assert list(aid_w) == [1, 2]
    assert list(cid_w) == [1, 1]
    assert skipped == 2  # placeholder + singleton


def test_keep_singletons_when_not_skipping():
    batch = _batch([0, 2, 2, 3])

    aid_w, cid_w, _write_rows, skipped = _mapping_to_write_arrays(
        batch, max_address_id=3, skip_singletons=False
    )

    assert list(aid_w) == [1, 2, 3]
    assert list(cid_w) == [1, 1, 3]
    assert skipped == 1  # only the placeholder


def test_rejects_non_dense_mapping():
    batch = _batch([0, 2, 2])  # 3 rows, but max_address_id implies 4

    with pytest.raises(ValueError):
        _mapping_to_write_arrays(batch, max_address_id=3, skip_singletons=True)
