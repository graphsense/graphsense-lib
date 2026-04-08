# graphsense-clustering

UTXO address clustering via a parallel Union-Find, implemented in Rust and
exposed to Python through PyO3. Used by
[graphsense-lib](https://github.com/graphsense/graphsense-lib) to run the
multi-input common-ownership heuristic over Bitcoin and other UTXO chains.

## Installation

```bash
pip install graphsense-clustering
```

Pre-built wheels are published for Linux (glibc and musl, x86_64 and aarch64)
and macOS (x86_64 and arm64) against CPython 3.10+ via the stable ABI
(`abi3`), so a single wheel covers all supported Python versions on a given
platform.

Most users do not need this package directly — install it via the
`graphsense-lib` clustering extra:

```bash
pip install 'graphsense-lib[clustering]'
```

## Usage

```python
from gs_clustering import Clustering

# Size the union-find from the maximum known address id.
c = Clustering(max_address_id=1_000_000)

# Feed batches of multi-input transactions. Each inner list is the set of
# input address ids for a single transaction.
c.process_transactions([[1, 2, 3], [4, 5], [2, 7]])

# Materialize the final (address_id, cluster_id) mapping as a pyarrow
# RecordBatch.
batch = c.get_mapping()
```

See the `graphsense-lib` source tree for the integration code that streams
transactions out of Cassandra and writes the cluster mapping back.

## License

MIT
