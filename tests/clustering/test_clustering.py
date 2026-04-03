"""Integration tests for the gs_clustering Rust module."""

import pytest

gs_clustering = pytest.importorskip(
    "gs_clustering", reason="gs_clustering not installed (build with: make build-rust)"
)
from gs_clustering import Clustering  # noqa: E402


def test_one_off_clustering():
    """Full one-off clustering: process transactions, get mapping."""
    c = Clustering(max_address_id=100)

    # Simulate Bitcoin transactions (multi-input heuristic)
    # tx1: addresses 1,2,3 co-spend → same cluster
    # tx2: addresses 4,5 co-spend → same cluster
    # tx3: addresses 3,6 co-spend → merges with tx1's cluster
    # tx4: address 7 alone → singleton
    c.process_transactions(
        [
            [1, 2, 3],
            [4, 5],
            [3, 6],
            [7],
        ]
    )

    batch = c.get_mapping()
    clus = dict(
        zip(
            batch.column("address_id").to_pylist(),
            batch.column("cluster_id").to_pylist(),
        )
    )

    # Cluster A: {1, 2, 3, 6}
    assert clus[1] == clus[2] == clus[3] == clus[6]
    # Cluster B: {4, 5}
    assert clus[4] == clus[5]
    # Cluster C: {7} (singleton)
    assert clus[7] == 7
    # Distinct clusters
    assert clus[1] != clus[4]
    assert clus[1] != clus[7]
    assert clus[4] != clus[7]


def test_incremental_clustering():
    """Rebuild from existing mapping, process new tx, get diff."""
    c = Clustering(max_address_id=100)

    # Existing state: cluster 10 = {1,2,3}, cluster 20 = {4,5}, cluster 30 = {6}
    c.rebuild_from_mapping(
        address_ids=[1, 2, 3, 4, 5, 6],
        cluster_ids=[10, 10, 10, 20, 20, 30],
    )

    # New transaction: inputs [3, 4] → merges cluster 10 and cluster 20
    c.process_transactions([[3, 4]])

    # Check diff
    diff = c.get_diff()
    changed_addrs = set(diff.column("address_id").to_pylist())

    # Some addresses must have changed (at least the merged ones)
    assert len(changed_addrs) > 0

    # Verify all are now in the same cluster
    full = c.get_mapping()
    clus = dict(
        zip(
            full.column("address_id").to_pylist(),
            full.column("cluster_id").to_pylist(),
        )
    )
    assert clus[1] == clus[2] == clus[3] == clus[4] == clus[5]


def test_incremental_no_change():
    """If new transactions don't cause merges, diff should be empty."""
    c = Clustering(max_address_id=100)
    c.rebuild_from_mapping(
        address_ids=[1, 2, 3],
        cluster_ids=[10, 10, 10],
    )

    # Transaction with addresses already in the same cluster → no change
    c.process_transactions([[1, 2]])

    diff = c.get_diff()
    assert len(diff.column("address_id").to_pylist()) == 0


def test_incremental_new_addresses():
    """New addresses not in existing mapping get their own clusters."""
    c = Clustering(max_address_id=100)
    c.rebuild_from_mapping(
        address_ids=[1, 2],
        cluster_ids=[10, 10],
    )

    # New tx with a brand new address 50
    c.process_transactions([[1, 50]])

    diff = c.get_diff()
    changed_addrs = set(diff.column("address_id").to_pylist())

    # Address 50 was not in the snapshot (find(50)==50 before and after rebuild,
    # but now find(50)==find(1) which changed), so it should appear in diff
    assert 50 in changed_addrs

    # Verify address 50 is now in the same cluster as 1,2
    full = c.get_mapping()
    clus = dict(
        zip(
            full.column("address_id").to_pylist(),
            full.column("cluster_id").to_pylist(),
        )
    )
    assert clus[1] == clus[2] == clus[50]


def test_chunked_processing():
    """Multiple process_transactions calls produce same result as one."""
    # Single call
    c1 = Clustering(max_address_id=20)
    c1.process_transactions([[1, 2, 3], [4, 5], [3, 6]])

    # Chunked calls
    c2 = Clustering(max_address_id=20)
    c2.process_transactions([[1, 2, 3]])
    c2.process_transactions([[4, 5]])
    c2.process_transactions([[3, 6]])

    m1 = c1.get_mapping()
    m2 = c2.get_mapping()

    clus1 = m1.column("cluster_id").to_pylist()
    clus2 = m2.column("cluster_id").to_pylist()

    # Same cluster groupings (IDs may differ)
    for i in range(21):
        for j in range(21):
            assert (clus1[i] == clus1[j]) == (clus2[i] == clus2[j]), (
                f"Grouping mismatch at ({i},{j})"
            )


def test_rebuild_mismatched_lengths():
    """rebuild_from_mapping should reject mismatched arrays."""
    c = Clustering(max_address_id=10)
    with pytest.raises(ValueError):
        c.rebuild_from_mapping(
            address_ids=[1, 2, 3],
            cluster_ids=[10, 10],
        )


def test_get_diff_without_rebuild():
    """get_diff should raise if rebuild was not called."""
    c = Clustering(max_address_id=10)
    c.process_transactions([[1, 2]])
    with pytest.raises(RuntimeError):
        c.get_diff()
