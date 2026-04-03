mod clustering;

use std::sync::Mutex;

use arrow::array::{ArrayRef, UInt32Array};
use arrow::pyarrow::ToPyArrow;
use arrow::record_batch::RecordBatch;
use pyo3::prelude::*;
use rayon::prelude::*;
use uf_rush::UFRush;

use crate::clustering::execute_union_operations;

#[pyclass]
struct Clustering {
    uf: UFRush,
    max_id: u32,
    /// Snapshot of find() values taken after rebuild, used by get_diff().
    snapshot: Mutex<Option<Vec<u32>>>,
}

#[pymethods]
impl Clustering {
    #[new]
    fn new(max_address_id: u32) -> Self {
        Self {
            uf: UFRush::new((max_address_id + 1) as usize),
            max_id: max_address_id,
            snapshot: Mutex::new(None),
        }
    }

    /// Process a batch of transactions. Each inner list contains the input
    /// address IDs of one transaction. Can be called multiple times (accumulates).
    fn process_transactions(&self, tx_inputs: Vec<Vec<u32>>) {
        execute_union_operations(&self.uf, &tx_inputs);
    }

    /// Return the full (address_id, cluster_id) mapping as an Arrow RecordBatch.
    fn get_mapping(&self, py: Python<'_>) -> PyResult<PyObject> {
        let (address_ids, cluster_ids) = self.parallel_find_all();
        let batch = self
            .make_record_batch(address_ids, cluster_ids)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        batch.to_pyarrow(py)
    }

    /// Rebuild union-find state from existing (address_id, cluster_id) mapping.
    /// Groups addresses by cluster_id and unites all addresses within each group.
    /// Takes a snapshot of find() values for later diff computation.
    fn rebuild_from_mapping(&self, address_ids: Vec<u32>, cluster_ids: Vec<u32>) -> PyResult<()> {
        if address_ids.len() != cluster_ids.len() {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "address_ids and cluster_ids must have the same length",
            ));
        }

        // Group addresses by cluster_id
        let mut groups: std::collections::HashMap<u32, Vec<u32>> =
            std::collections::HashMap::new();
        for (&addr, &cluster) in address_ids.iter().zip(cluster_ids.iter()) {
            groups.entry(cluster).or_default().push(addr);
        }

        // Unite all addresses within each group
        for (_cluster_id, addrs) in &groups {
            if addrs.len() > 1 {
                let first = addrs[0] as usize;
                for &addr in &addrs[1..] {
                    self.uf.unite(first, addr as usize);
                }
            }
        }

        // Take snapshot for diff computation
        let snap: Vec<u32> = (0..=self.max_id)
            .into_par_iter()
            .map(|id| self.uf.find(id as usize) as u32)
            .collect();
        *self.snapshot.lock().unwrap() = Some(snap);

        Ok(())
    }

    /// Return only mappings that changed since rebuild_from_mapping was called.
    /// Returns an Arrow RecordBatch with (address_id, cluster_id) for changed addresses.
    fn get_diff(&self, py: Python<'_>) -> PyResult<PyObject> {
        let snap = self.snapshot.lock().unwrap();
        let snap = snap.as_ref().ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err(
                "get_diff() requires rebuild_from_mapping() to be called first",
            )
        })?;

        let changed: Vec<(u32, u32)> = (0..=self.max_id)
            .into_par_iter()
            .filter_map(|id| {
                let new_root = self.uf.find(id as usize) as u32;
                let old_root = snap[id as usize];
                if new_root != old_root {
                    Some((id, new_root))
                } else {
                    None
                }
            })
            .collect();

        let (address_ids, cluster_ids): (Vec<u32>, Vec<u32>) =
            changed.into_iter().unzip();
        let batch = self.make_record_batch(address_ids, cluster_ids)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        batch.to_pyarrow(py)
    }
}

impl Clustering {
    fn parallel_find_all(&self) -> (Vec<u32>, Vec<u32>) {
        let results: Vec<(u32, u32)> = (0..=self.max_id)
            .into_par_iter()
            .map(|id| (id, self.uf.find(id as usize) as u32))
            .collect();
        results.into_iter().unzip()
    }

    fn make_record_batch(
        &self,
        address_ids: Vec<u32>,
        cluster_ids: Vec<u32>,
    ) -> Result<RecordBatch, arrow::error::ArrowError> {
        RecordBatch::try_from_iter(vec![
            ("address_id", std::sync::Arc::new(UInt32Array::from(address_ids)) as ArrayRef),
            ("cluster_id", std::sync::Arc::new(UInt32Array::from(cluster_ids)) as ArrayRef),
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
