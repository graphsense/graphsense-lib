use crate::unionfind::MinUnionFind;
use arrow::array::{Array, ListArray, UInt32Array};
use rayon::prelude::*;

/// Execute union operations for a batch of transactions.
/// Each inner Vec contains the input address IDs of one transaction.
/// Addresses sharing a transaction input are united (multi-input heuristic).
pub fn execute_union_operations(uf: &MinUnionFind, transactions: &[Vec<u32>]) {
    transactions.par_iter().for_each(|address_ids| {
        if address_ids.len() > 1 {
            let first = address_ids[0] as usize;
            for &addr in &address_ids[1..] {
                uf.unite(first, addr as usize);
            }
        }
    });
}

/// Execute union operations reading transactions directly from an Arrow
/// `ListArray` whose inner values are uint32 address ids (the offsets delimit
/// each transaction's input set). Equivalent to [`execute_union_operations`]
/// but without materializing nested Python/Rust vectors — the offsets and the
/// contiguous values buffer are read in place. Returns `Err` if the inner
/// value type is not uint32. Inner values are assumed non-null (upstream
/// resolves and filters addresses); null *lists* are skipped defensively.
pub fn execute_union_operations_arrow(uf: &MinUnionFind, lists: &ListArray) -> Result<(), String> {
    let values = lists
        .values()
        .as_any()
        .downcast_ref::<UInt32Array>()
        .ok_or_else(|| {
            format!(
                "expected list<uint32> values, got {:?}",
                lists.values().data_type()
            )
        })?;
    let offsets = lists.value_offsets();
    (0..lists.len()).into_par_iter().for_each(|i| {
        if !lists.is_valid(i) {
            return;
        }
        let start = offsets[i] as usize;
        let end = offsets[i + 1] as usize;
        if end - start > 1 {
            let first = values.value(start) as usize;
            for j in (start + 1)..end {
                uf.unite(first, values.value(j) as usize);
            }
        }
    });
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_multi_input_clustering() {
        let uf = MinUnionFind::new(10);
        let txs = vec![vec![1u32, 2, 3]];
        execute_union_operations(&uf, &txs);
        assert_eq!(uf.find(1), uf.find(2));
        assert_eq!(uf.find(2), uf.find(3));
    }

    #[test]
    fn test_singleton_no_union() {
        let uf = MinUnionFind::new(10);
        let txs = vec![vec![5u32]];
        execute_union_operations(&uf, &txs);
        assert_eq!(uf.find(5), 5);
    }

    #[test]
    fn test_transitive_merge() {
        let uf = MinUnionFind::new(10);
        let txs = vec![vec![1u32, 2], vec![2u32, 3]];
        execute_union_operations(&uf, &txs);
        assert_eq!(uf.find(1), uf.find(3));
    }

    #[test]
    fn test_separate_clusters() {
        let uf = MinUnionFind::new(10);
        let txs = vec![vec![1u32, 2], vec![4u32, 5]];
        execute_union_operations(&uf, &txs);
        assert_eq!(uf.find(1), uf.find(2));
        assert_eq!(uf.find(4), uf.find(5));
        assert_ne!(uf.find(1), uf.find(4));
    }

    #[test]
    fn test_empty_transaction() {
        let uf = MinUnionFind::new(10);
        let txs: Vec<Vec<u32>> = vec![vec![]];
        execute_union_operations(&uf, &txs);
        // No panics, no changes
        assert_eq!(uf.find(0), 0);
    }

    #[test]
    fn test_rebuild_from_mapping() {
        // Simulate existing mapping: addresses 1,2,3 in cluster 1; 4,5 in cluster 4
        let uf = MinUnionFind::new(10);

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
        let uf = MinUnionFind::new(10);
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

    #[test]
    fn test_arrow_multi_input_clustering() {
        use arrow::datatypes::UInt32Type;
        let uf = MinUnionFind::new(10);
        let list = ListArray::from_iter_primitive::<UInt32Type, _, _>(vec![Some(vec![
            Some(1u32),
            Some(2),
            Some(3),
        ])]);
        execute_union_operations_arrow(&uf, &list).unwrap();
        assert_eq!(uf.find(1), uf.find(2));
        assert_eq!(uf.find(2), uf.find(3));
    }

    #[test]
    fn test_arrow_singleton_no_union() {
        use arrow::datatypes::UInt32Type;
        let uf = MinUnionFind::new(10);
        let list = ListArray::from_iter_primitive::<UInt32Type, _, _>(vec![Some(vec![Some(5u32)])]);
        execute_union_operations_arrow(&uf, &list).unwrap();
        assert_eq!(uf.find(5), 5);
    }

    #[test]
    fn test_arrow_equivalence_and_singleton() {
        use arrow::datatypes::UInt32Type;
        let uf = MinUnionFind::new(10);
        let list = ListArray::from_iter_primitive::<UInt32Type, _, _>(vec![
            Some(vec![Some(1u32), Some(2), Some(3)]),
            Some(vec![Some(4u32), Some(5)]),
            Some(vec![Some(5u32), Some(6)]),
            Some(vec![Some(8u32)]),
        ]);
        execute_union_operations_arrow(&uf, &list).unwrap();
        // {1,2,3}, {4,5,6} (transitive via shared 5), 8 singleton
        assert_eq!(uf.find(1), uf.find(3));
        assert_eq!(uf.find(4), uf.find(6));
        assert_ne!(uf.find(1), uf.find(4));
        assert_eq!(uf.find(8), 8);
    }

    #[test]
    fn test_arrow_wrong_value_type_errors() {
        use arrow::datatypes::Int64Type;
        let uf = MinUnionFind::new(10);
        let list = ListArray::from_iter_primitive::<Int64Type, _, _>(vec![Some(vec![
            Some(1i64),
            Some(2),
        ])]);
        assert!(execute_union_operations_arrow(&uf, &list).is_err());
    }

    #[test]
    fn test_arrow_null_list_skipped() {
        use arrow::datatypes::UInt32Type;
        let uf = MinUnionFind::new(10);
        // a null list between two real ones must be skipped, not united
        let list = ListArray::from_iter_primitive::<UInt32Type, _, _>(vec![
            Some(vec![Some(1u32), Some(2)]),
            None,
            Some(vec![Some(4u32), Some(5)]),
        ]);
        execute_union_operations_arrow(&uf, &list).unwrap();
        assert_eq!(uf.find(1), uf.find(2));
        assert_eq!(uf.find(4), uf.find(5));
        assert_ne!(uf.find(1), uf.find(4));
    }

    #[test]
    fn test_arrow_empty_inner_list() {
        use arrow::datatypes::UInt32Type;
        let uf = MinUnionFind::new(10);
        // a zero-length input set is a no-op (does not panic on offsets)
        let list = ListArray::from_iter_primitive::<UInt32Type, _, _>(vec![
            Some(Vec::<Option<u32>>::new()),
            Some(vec![Some(1u32), Some(2)]),
        ]);
        execute_union_operations_arrow(&uf, &list).unwrap();
        assert_eq!(uf.find(1), uf.find(2));
        assert_eq!(uf.find(3), 3);
    }

    #[test]
    fn test_arrow_accumulates_across_calls() {
        use arrow::datatypes::UInt32Type;
        let uf = MinUnionFind::new(10);
        let a = ListArray::from_iter_primitive::<UInt32Type, _, _>(vec![Some(vec![
            Some(1u32),
            Some(2),
        ])]);
        let b = ListArray::from_iter_primitive::<UInt32Type, _, _>(vec![Some(vec![
            Some(2u32),
            Some(3),
        ])]);
        execute_union_operations_arrow(&uf, &a).unwrap();
        execute_union_operations_arrow(&uf, &b).unwrap();
        // 1-2 then 2-3 across separate arrays => {1,2,3}
        assert_eq!(uf.find(1), uf.find(3));
    }
}
