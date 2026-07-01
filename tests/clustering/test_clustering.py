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
