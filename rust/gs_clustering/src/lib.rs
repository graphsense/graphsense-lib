mod clustering;
mod unionfind;

use std::sync::atomic::{AtomicU64, Ordering};

use arrow::array::{make_array, Array, ArrayData, ArrayRef, ListArray, UInt32Array};
use arrow::pyarrow::{FromPyArrow, ToPyArrow};
use arrow::record_batch::RecordBatch;
use pyo3::prelude::*;
use rayon::prelude::*;

use crate::clustering::{execute_union_operations, execute_union_operations_arrow};
use crate::unionfind::MinUnionFind;

#[pyclass]
struct Clustering {
    uf: MinUnionFind,
    max_id: u32,
}

#[pymethods]
impl Clustering {
    #[new]
    fn new(max_address_id: u32) -> Self {
        Self {
            uf: MinUnionFind::new((max_address_id + 1) as usize),
            max_id: max_address_id,
        }
    }

    /// Process a batch of transactions. Each inner list contains the input
    /// address IDs of one transaction. Can be called multiple times (accumulates).
    fn process_transactions(&self, tx_inputs: Vec<Vec<u32>>) {
        execute_union_operations(&self.uf, &tx_inputs);
    }

    /// Process a batch of transactions supplied as a pyarrow `ListArray` whose
    /// inner values are uint32 input address ids (one inner list per
    /// transaction). Unlike `process_transactions`, the Arrow offsets+values
    /// buffers are read directly with zero Python-object materialization, so
    /// the driver-side Arrow→Python (`to_pylist`) conversion that dominates the
    /// feed at scale disappears. Accumulates like `process_transactions`.
    fn process_transactions_arrow(&self, array: &Bound<'_, PyAny>) -> PyResult<()> {
        let data = ArrayData::from_pyarrow_bound(array)?;
        let array_ref = make_array(data);
        let list = array_ref
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| {
                pyo3::exceptions::PyValueError::new_err(
                    "process_transactions_arrow expects a pyarrow ListArray",
                )
            })?;
        array
            .py()
            .allow_threads(|| execute_union_operations_arrow(&self.uf, list))
            .map_err(pyo3::exceptions::PyValueError::new_err)
    }

    /// Return the full (address_id, cluster_id) mapping as an Arrow RecordBatch:
    /// one row per id in `0..=max_id`, dense and ascending. Kept for the
    /// incremental delta path, which drives small densified id spaces through it.
    fn get_mapping(&self, py: Python<'_>) -> PyResult<PyObject> {
        let (address_ids, cluster_ids) = self.parallel_find_all();
        let batch = self
            .make_record_batch(address_ids, cluster_ids)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        batch.to_pyarrow(py)
    }

    /// Return only the rows the bootstrap writes, as an Arrow RecordBatch in
    /// ascending address_id order: the placeholder (id 0) is dropped and, with
    /// `skip_singletons`, every size-1 cluster too (an address absent from the
    /// fresh tables is implicitly its own cluster). `cluster_id` is the
    /// component minimum by construction — the union-find links by minimum —
    /// so no relabel pass exists and the peak allocation is just the kept rows
    /// instead of one row per address id.
    fn get_mapping_min(&self, py: Python<'_>, skip_singletons: bool) -> PyResult<PyObject> {
        let (address_ids, cluster_ids) =
            py.allow_threads(|| self.build_min_mapping(skip_singletons));
        let batch = self
            .make_record_batch(address_ids, cluster_ids)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        batch.to_pyarrow(py)
    }
}

impl Clustering {
    fn parallel_find_all(&self) -> (Vec<u32>, Vec<u32>) {
        // Fill cluster_ids in place by index and generate address_ids from the
        // dense 0..=max_id range. Avoids the ~8 GB intermediate Vec<(u32,u32)>
        // and the single-threaded unzip a `.map(|id| (id, find)).collect().unzip()`
        // would incur — both matter on a full BTC keyspace (~1e9 ids) inside the
        // memory-constrained Spark driver.
        let n = (self.max_id as usize) + 1;
        let mut cluster_ids = vec![0u32; n];
        cluster_ids
            .par_iter_mut()
            .enumerate()
            .for_each(|(id, c)| *c = self.uf.find(id) as u32);
        let address_ids: Vec<u32> = (0..=self.max_id).collect();
        (address_ids, cluster_ids)
    }

    /// The filtered, min-labelled mapping behind `get_mapping_min`.
    ///
    /// Three parallel passes over the id space, no per-address output:
    /// 1. with `skip_singletons`, mark roots owning a non-root member in a
    ///    bitset (1 bit/id) — exactly "cluster size >= 2" (a singleton's root
    ///    has no non-root member);
    /// 2. count kept rows per chunk to size the outputs exactly;
    /// 3. fill disjoint per-chunk output windows in parallel.
    /// A kept row is `id != 0` (placeholder) and, when skipping singletons,
    /// `nontrivial(root)`. Note the placeholder can legitimately BE a min
    /// label: if id 0 was ever unioned, its members are kept with
    /// cluster_id 0 while the row for id 0 itself is dropped.
    fn build_min_mapping(&self, skip_singletons: bool) -> (Vec<u32>, Vec<u32>) {
        const CHUNK: usize = 1 << 22;
        let n = (self.max_id as usize) + 1;

        let nontrivial: Vec<AtomicU64> = if skip_singletons {
            (0..n.div_ceil(64)).map(|_| AtomicU64::new(0)).collect()
        } else {
            Vec::new()
        };
        if skip_singletons {
            (0..n).into_par_iter().for_each(|id| {
                let root = self.uf.find(id);
                if root != id {
                    nontrivial[root / 64].fetch_or(1 << (root % 64), Ordering::Relaxed);
                }
            });
        }
        let kept = |id: usize, root: usize| {
            id != 0
                && (!skip_singletons
                    || nontrivial[root / 64].load(Ordering::Relaxed) & (1 << (root % 64)) != 0)
        };

        let nchunks = n.div_ceil(CHUNK);
        let counts: Vec<usize> = (0..nchunks)
            .into_par_iter()
            .map(|ci| {
                let start = ci * CHUNK;
                let end = (start + CHUNK).min(n);
                (start..end)
                    .filter(|&id| kept(id, self.uf.find(id)))
                    .count()
            })
            .collect();
        let total: usize = counts.iter().sum();

        let mut address_ids = vec![0u32; total];
        let mut cluster_ids = vec![0u32; total];
        {
            let mut aid_rest: &mut [u32] = &mut address_ids;
            let mut cid_rest: &mut [u32] = &mut cluster_ids;
            let mut windows = Vec::with_capacity(nchunks);
            for &count in &counts {
                let (aid_win, aid_tail) = aid_rest.split_at_mut(count);
                let (cid_win, cid_tail) = cid_rest.split_at_mut(count);
                windows.push((aid_win, cid_win));
                aid_rest = aid_tail;
                cid_rest = cid_tail;
            }
            windows
                .into_par_iter()
                .enumerate()
                .for_each(|(ci, (aid_win, cid_win))| {
                    let start = ci * CHUNK;
                    let end = (start + CHUNK).min(n);
                    let mut k = 0;
                    for id in start..end {
                        let root = self.uf.find(id);
                        if kept(id, root) {
                            aid_win[k] = id as u32;
                            cid_win[k] = root as u32;
                            k += 1;
                        }
                    }
                });
        }
        (address_ids, cluster_ids)
    }

    fn make_record_batch(
        &self,
        address_ids: Vec<u32>,
        cluster_ids: Vec<u32>,
    ) -> Result<RecordBatch, arrow::error::ArrowError> {
        RecordBatch::try_from_iter(vec![
            (
                "address_id",
                std::sync::Arc::new(UInt32Array::from(address_ids)) as ArrayRef,
            ),
            (
                "cluster_id",
                std::sync::Arc::new(UInt32Array::from(cluster_ids)) as ArrayRef,
            ),
        ])
    }
}

#[pyfunction]
fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

#[pymodule]
fn gs_clustering(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Clustering>()?;
    m.add_function(wrap_pyfunction!(version, m)?)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    // The v2 delta path (`_components_via_rust`) drives the Clustering struct
    // via process_transactions + get_mapping; this pins that path's behaviour
    // at the struct level (get_mapping itself returns pyarrow, so we read the
    // same data through parallel_find_all).
    #[test]
    fn test_clustering_struct_accumulates_and_maps() {
        let c = Clustering::new(5); // ids 0..=5
        c.process_transactions(vec![vec![1, 2, 3]]);
        c.process_transactions(vec![vec![3, 4]]); // accumulates: 4 joins {1,2,3}
        let (ids, clusters) = c.parallel_find_all();

        // the mapping covers every id in 0..=max_id exactly once
        assert_eq!(ids, vec![0, 1, 2, 3, 4, 5]);
        let root: HashMap<u32, u32> = ids.into_iter().zip(clusters).collect();

        // {1,2,3,4} share a root across the two process_transactions calls,
        // and by-min linking makes that root the minimum member
        assert_eq!(root[&1], 1);
        assert_eq!(root[&2], 1);
        assert_eq!(root[&3], 1);
        assert_eq!(root[&4], 1);
        // untouched ids are their own singleton cluster
        assert_eq!(root[&0], 0);
        assert_eq!(root[&5], 5);
        assert_ne!(root[&1], root[&0]);
    }

    // build_min_mapping pins the exact bootstrap write semantics previously
    // implemented in Python (_mapping_to_write_arrays): min labels, placeholder
    // drop, singleton handling.

    #[test]
    fn test_min_mapping_relabels_and_drops_placeholder() {
        // components: {1,2,4} and {3,5}; the placeholder (0) untouched.
        let c = Clustering::new(5);
        c.process_transactions(vec![vec![1, 2, 4], vec![3, 5]]);
        let (aid, cid) = c.build_min_mapping(true);
        assert_eq!(aid, vec![1, 2, 3, 4, 5]);
        assert_eq!(cid, vec![1, 1, 3, 1, 3]);
    }

    #[test]
    fn test_min_mapping_skips_size_one_clusters_only() {
        // {1,2} clustered; 3 a genuine singleton — dropped with the placeholder.
        let c = Clustering::new(3);
        c.process_transactions(vec![vec![1, 2]]);
        let (aid, cid) = c.build_min_mapping(true);
        assert_eq!(aid, vec![1, 2]);
        assert_eq!(cid, vec![1, 1]);
    }

    #[test]
    fn test_min_mapping_keeps_singletons_when_not_skipping() {
        let c = Clustering::new(3);
        c.process_transactions(vec![vec![1, 2]]);
        let (aid, cid) = c.build_min_mapping(false);
        assert_eq!(aid, vec![1, 2, 3]);
        assert_eq!(cid, vec![1, 1, 3]);
    }

    #[test]
    fn test_min_mapping_placeholder_can_be_min_label() {
        // If id 0 was ever unioned, members keep cluster_id 0 while the row
        // for id 0 itself is dropped (same as the Python filter it replaces).
        let c = Clustering::new(2);
        c.process_transactions(vec![vec![0, 2]]);
        let (aid, cid) = c.build_min_mapping(true);
        assert_eq!(aid, vec![2]);
        assert_eq!(cid, vec![0]);
    }
}
