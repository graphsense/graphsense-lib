mod clustering;

use arrow::array::{make_array, Array, ArrayData, ArrayRef, ListArray, UInt32Array};
use arrow::pyarrow::{FromPyArrow, ToPyArrow};
use arrow::record_batch::RecordBatch;
use pyo3::prelude::*;
use rayon::prelude::*;
use uf_rush::UFRush;

use crate::clustering::{execute_union_operations, execute_union_operations_arrow};

#[pyclass]
struct Clustering {
    uf: UFRush,
    max_id: u32,
}

#[pymethods]
impl Clustering {
    #[new]
    fn new(max_address_id: u32) -> Self {
        Self {
            uf: UFRush::new((max_address_id + 1) as usize),
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

    /// Return the full (address_id, cluster_id) mapping as an Arrow RecordBatch.
    fn get_mapping(&self, py: Python<'_>) -> PyResult<PyObject> {
        let (address_ids, cluster_ids) = self.parallel_find_all();
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

        // {1,2,3,4} share a root across the two process_transactions calls
        assert_eq!(root[&1], root[&2]);
        assert_eq!(root[&2], root[&3]);
        assert_eq!(root[&3], root[&4]);
        // untouched ids are their own singleton cluster
        assert_eq!(root[&0], 0);
        assert_eq!(root[&5], 5);
        assert_ne!(root[&1], root[&0]);
    }
}
