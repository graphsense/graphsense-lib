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
}
