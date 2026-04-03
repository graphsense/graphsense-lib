use rayon::prelude::*;
use uf_rush::UFRush;

/// Execute union operations for a batch of transactions.
/// Each inner Vec contains the input address IDs of one transaction.
/// Addresses sharing a transaction input are united (multi-input heuristic).
pub fn execute_union_operations(uf: &UFRush, transactions: &[Vec<u32>]) {
    transactions.par_iter().for_each(|address_ids| {
        if address_ids.len() > 1 {
            let first = address_ids[0] as usize;
            for &addr in &address_ids[1..] {
                uf.unite(first, addr as usize);
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_multi_input_clustering() {
        let uf = UFRush::new(10);
        let txs = vec![vec![1u32, 2, 3]];
        execute_union_operations(&uf, &txs);
        assert_eq!(uf.find(1), uf.find(2));
        assert_eq!(uf.find(2), uf.find(3));
    }

    #[test]
    fn test_singleton_no_union() {
        let uf = UFRush::new(10);
        let txs = vec![vec![5u32]];
        execute_union_operations(&uf, &txs);
        assert_eq!(uf.find(5), 5);
    }

    #[test]
    fn test_transitive_merge() {
        let uf = UFRush::new(10);
        let txs = vec![vec![1u32, 2], vec![2u32, 3]];
        execute_union_operations(&uf, &txs);
        assert_eq!(uf.find(1), uf.find(3));
    }

    #[test]
    fn test_separate_clusters() {
        let uf = UFRush::new(10);
        let txs = vec![vec![1u32, 2], vec![4u32, 5]];
        execute_union_operations(&uf, &txs);
        assert_eq!(uf.find(1), uf.find(2));
        assert_eq!(uf.find(4), uf.find(5));
        assert_ne!(uf.find(1), uf.find(4));
    }

    #[test]
    fn test_empty_transaction() {
        let uf = UFRush::new(10);
        let txs: Vec<Vec<u32>> = vec![vec![]];
        execute_union_operations(&uf, &txs);
        // No panics, no changes
        assert_eq!(uf.find(0), 0);
    }

    #[test]
    fn test_rebuild_from_mapping() {
        // Simulate existing mapping: addresses 1,2,3 in cluster 1; 4,5 in cluster 4
        let uf = UFRush::new(10);

        // Group by cluster_id, unite within groups
        // Cluster 1: addresses 1, 2, 3
        uf.unite(1, 2);
        uf.unite(1, 3);
        // Cluster 4: addresses 4, 5
        uf.unite(4, 5);

        assert_eq!(uf.find(1), uf.find(2));
        assert_eq!(uf.find(1), uf.find(3));
        assert_eq!(uf.find(4), uf.find(5));
        assert_ne!(uf.find(1), uf.find(4));
    }

    #[test]
    fn test_rebuild_then_merge() {
        let uf = UFRush::new(10);
        // Rebuild: cluster A = {1,2}, cluster B = {3,4}
        uf.unite(1, 2);
        uf.unite(3, 4);
        assert_ne!(uf.find(1), uf.find(3));

        // New transaction merges the clusters: tx with inputs [2, 3]
        let txs = vec![vec![2u32, 3]];
        execute_union_operations(&uf, &txs);

        // All 4 should now be in the same cluster
        assert_eq!(uf.find(1), uf.find(3));
        assert_eq!(uf.find(1), uf.find(4));
    }
}
