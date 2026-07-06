"""Manufactured-dataset clustering regression: PySpark one-off vs incremental.

Unlike ``test_clustering.py`` (which ingests real blocks and uses the Scala
transformation as the reference), this test seeds a small, hand-crafted raw
dataset directly into Cassandra and compares our *own* two production clustering
paths for partition equivalence:

  * the PySpark one-off ``run_clustering_spark`` (bulk read + Arrow feed +
    ``process_transactions_arrow`` + bulk write + ``recompute_fresh_cluster_stats``),
    run over the whole range at once; and
  * the incremental ``UpdateStrategyUtxo.run_fresh_clustering`` (point/range reads
    + ``run_incremental_clustering`` per batch), run from an empty state across
    several block batches so cross-batch new/join/merge is exercised.

Both must produce the same address partition (cluster_id labels may differ — the
comparison is membership-based, see ``compare_cluster_partitions``).

Requires (cannot run in the unit suite):
  * Docker (Cassandra testcontainer via the ``cassandra_coords`` fixture);
  * ``current_venv`` with ``graphsenselib`` + ``pyspark`` installed — all
    production code runs there via subprocess (the regressions venv has neither).

Design notes / assumptions (validate on first live run):
  * The seeder runs in ``current_venv`` and computes ``address_prefix`` /
    ``address`` via the production ``to_db_address`` so the point-read path
    resolves correctly.  The PySpark path instead joins the raw input address
    string against ``address_ids_by_address_prefix.address``; for both to agree
    the manufactured addresses must be identity-encoded
    (``to_db_address(a).db_encoding == a``).  The seeder asserts this and fails
    loudly otherwise — pick different addresses if it trips.
  * ``run_fresh_clustering`` is gated on the ``fresh_clustering_active`` state marker;
    the incremental subprocess inherits the test-process env, same as the
    existing ``test_clustering.py`` incremental step.
"""

import json
import os
import subprocess
from pathlib import Path

import pytest

from tests.clustering.ingest_runner import (
    _create_transformed_keyspace,
    _qualify_create,
    _read_fresh_address_cluster,
    run_incremental_clustering_via_production,
)
from tests.clustering.test_clustering import compare_cluster_partitions

pytestmark = pytest.mark.clustering

CURRENCY = "btc"
BLOCK_BUCKET_SIZE = 100
TX_BUCKET_SIZE = 100
ADDRESS_PREFIX_LENGTH = 5
# Deliberately tiny so the dense address_ids [1, 8] land in several distinct
# partition buckets (id // 3 -> 0,0,1,1,1,2,2,2), making the one-off vs
# incremental parity check actually exercise the fresh-table id_group columns.
ADDRESS_BUCKET_SIZE = 3

# Identity-encoded (legacy base58) BTC addresses -> dense address_ids [1, N].
A = "1BvBMSEYstWetqTFn5Au4m4GFg7xJaNVN2"
B = "12c6DSiU4Rq3P4ZxziKxzrL5LmMBrzjrJX"
C = "1HLoD9E4SDFFPDiYfNYnkBLQ85Y51J3Zb1"
D = "1FvzCLoTPGANNjWoUo6jUGuAG3wg1w4YjR"
E = "15ubicBBWFnvoZLT7GiU2qxjRaKJPdkDMG"
F = "1JfbZRwdDHKZmuiZgYArJZhcuuzuw2HuMu"
G = "1GkQmKAmHtNfnD3LHhTkewJxKHVSta4m2a"
H = "16cou7Ht6WjTzuFyDBnht9hmvXytg6XdVT"
ADDR_TO_ID = {A: 1, B: 2, C: 3, D: 4, E: 5, F: 6, G: 7, H: 8}

# Manufactured transactions in tx_id order (tx_id assigned in block order).
# Exercises: chain (A-B then B-C), a separate cluster (D-E), a cross-block merge
# of two already-committed clusters (C-D merges {A,B,C} and {D,E} — see the batch
# split below), a coinbase (skipped), a singleton (skipped), and a fresh
# multi-input cluster (G-H) late in the range.
MANUFACTURED_TXS = [
    {"block_id": 0, "tx_id": 0, "coinbase": True, "inputs": None},
    {"block_id": 0, "tx_id": 1, "coinbase": False, "inputs": [[A], [B]]},
    {"block_id": 1, "tx_id": 2, "coinbase": False, "inputs": [[B], [C]]},
    {"block_id": 2, "tx_id": 3, "coinbase": False, "inputs": [[D], [E]]},
    {"block_id": 3, "tx_id": 4, "coinbase": False, "inputs": [[C], [D]]},
    {"block_id": 4, "tx_id": 5, "coinbase": False, "inputs": [[F]]},
    {"block_id": 5, "tx_id": 6, "coinbase": False, "inputs": [[G], [H]]},
]
LAST_BLOCK = 5
# Final expected partition: {A,B,C,D,E} merged, {G,H} separate, F a singleton
# (skipped by both paths).  Asserted indirectly via one-off == incremental.

# --------------------------------------------------------------------------- #
# In-venv subprocess helpers (the regressions venv has no graphsenselib/pyspark)
# --------------------------------------------------------------------------- #
_SEED_HELPER = """
import json
import sys

from cassandra.cluster import Cluster
from graphsenselib.db.factory import DbFactory

a = json.load(sys.stdin)
host, port = a["cassandra_host"], a["cassandra_port"]
raw_ks, tks = a["raw_keyspace"], a["transformed_keyspace"]
bbs, tbs, apl = a["block_bucket_size"], a["tx_bucket_size"], a["address_prefix_length"]
abks = a["address_bucket_size"]
addr_to_id = a["addr_to_id"]
txs = a["txs"]

with Cluster([host], port=port) as cluster:
    s = cluster.connect()
    # configuration first: to_db_address and the raw readers need it
    s.execute(
        f"INSERT INTO {raw_ks}.configuration "
        f"(id, block_bucket_size, tx_prefix_length, tx_bucket_size) "
        f"VALUES ('{raw_ks}', {bbs}, 5, {tbs})"
    )
    s.execute(
        f"INSERT INTO {tks}.configuration "
        f"(keyspace_name, bucket_size, address_prefix_length, bech_32_prefix, "
        f"coinjoin_filtering, fiat_currencies) "
        f"VALUES ('{tks}', {abks}, {apl}, '', false, ['USD','EUR'])"
    )

db = DbFactory().from_name(
    raw_keyspace_name=raw_ks,
    transformed_keyspace_name=tks,
    schema_type="utxo",
    cassandra_nodes=[f"{host}:{port}"],
    currency=a["currency"],
)
db.open()
try:
    with Cluster([host], port=port) as cluster:
        s = cluster.connect()
        prep_aid = s.prepare(
            f"INSERT INTO {tks}.address_ids_by_address_prefix "
            f"(address_prefix, address, address_id) VALUES (?, ?, ?)"
        )
        for address, aid in addr_to_id.items():
            adr = db.transformed.to_db_address(address)
            assert adr.db_encoding == address, (
                f"address {address!r} is not identity-encoded "
                f"(db_encoding={adr.db_encoding!r}); the PySpark raw-string join "
                f"would not match the point-read path. Pick a different address."
            )
            s.execute(prep_aid, (str(adr.prefix), adr.db_encoding, aid))

        blocks = {}
        for tx in txs:
            tid, bid, cb = tx["tx_id"], tx["block_id"], tx["coinbase"]
            if cb:
                inputs_lit = "null"
            else:
                inputs_lit = "[" + ",".join(
                    "{address: [" + ",".join(f"'{x}'" for x in inp) + "]}"
                    for inp in tx["inputs"]
                ) + "]"
            s.execute(
                f"INSERT INTO {raw_ks}.transaction "
                f"(tx_id_group, tx_id, block_id, coinbase, inputs) "
                f"VALUES ({tid // tbs}, {tid}, {bid}, {str(cb).lower()}, {inputs_lit})"
            )
            blocks.setdefault(bid, []).append(tid)

        for bid, tids in blocks.items():
            txs_lit = "[" + ",".join("{tx_id: " + str(t) + "}" for t in tids) + "]"
            s.execute(
                f"INSERT INTO {raw_ks}.block_transactions "
                f"(block_id_group, block_id, txs) "
                f"VALUES ({bid // bbs}, {bid}, {txs_lit})"
            )
finally:
    db.close()

print("OK")
"""

_SPARK_ONEOFF_HELPER = """
import json
import sys

from graphsenselib.transformation.spark import create_spark_session
from graphsenselib.transformation.clustering import run_clustering_spark

a = json.load(sys.stdin)
spark = create_spark_session(
    app_name="manufactured-clustering-oneoff",
    local=True,
    cassandra_nodes=[f"{a['cassandra_host']}:{a['cassandra_port']}"],
)
try:
    run_clustering_spark(
        spark,
        raw_keyspace=a["raw_keyspace"],
        transformed_keyspace=a["transformed_keyspace"],
        max_address_id=a["max_address_id"],
        bucket_size=a["bucket_size"],
        end_block=a.get("end_block"),
    )
finally:
    spark.stop()

print("OK")
"""


def _run_in_venv(current_venv: Path, helper: str, args: dict) -> None:
    python_bin = str(current_venv / "bin" / "python")
    # The dev shell exports SPARK_SUBMIT_OPTS with a wait-for-attach JDWP agent
    # (server=y,suspend=y) and points SPARK_HOME at an older system Spark. Both
    # would make the helper's Spark JVM hang on the debugger socket / mismatch
    # the venv's bundled pyspark, so strip them for the subprocess.
    env = os.environ.copy()
    for var in ("SPARK_SUBMIT_OPTS", "SPARK_HOME", "PYSPARK_SUBMIT_ARGS"):
        env.pop(var, None)
    # Pin both driver and worker Python to the venv interpreter; otherwise Spark
    # workers default to the system python3 (a different minor version) and fail
    # with PYTHON_VERSION_MISMATCH against the 3.11 driver.
    env["PYSPARK_PYTHON"] = python_bin
    env["PYSPARK_DRIVER_PYTHON"] = python_bin
    result = subprocess.run(
        [python_bin, "-c", helper],
        input=json.dumps(args),
        capture_output=True,
        text=True,
        timeout=600,
        env=env,
    )
    if result.returncode != 0 or "OK" not in result.stdout:
        raise RuntimeError(
            f"venv helper failed (exit {result.returncode}):\n"
            f"stdout: {result.stdout[-3000:]}\nstderr: {result.stderr[-3000:]}"
        )


def _create_raw_keyspace(cassandra_host: str, cassandra_port: int, keyspace: str):
    """Create the raw UTXO keyspace + schema from the repo .sql (driver-side)."""
    from cassandra.cluster import Cluster

    schema_path = (
        Path(__file__).resolve().parents[4]
        / "src"
        / "graphsenselib"
        / "schema"
        / "resources"
        / "raw_utxo_schema.sql"
    )
    schema_sql = schema_path.read_text()
    with Cluster([cassandra_host], port=cassandra_port) as cluster:
        session = cluster.connect()
        session.execute(
            f"CREATE KEYSPACE IF NOT EXISTS {keyspace} "  # noqa: S608
            "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}"
        )
        for stmt in schema_sql.split(";"):
            stmt = stmt.strip()
            if (
                not stmt
                or stmt.upper().startswith("CREATE KEYSPACE")
                or stmt.upper().startswith("USE ")
            ):
                continue
            stmt = _qualify_create(stmt, keyspace)
            session.execute(stmt)


def _mark_fresh_active(cassandra_host: str, cassandra_port: int, keyspace: str):
    """Simulate a completed bootstrap: the incremental production path only
    maintains the fresh_* tables when the marker row is present."""
    from cassandra.cluster import Cluster

    with Cluster([cassandra_host], port=cassandra_port) as cluster:
        session = cluster.connect()
        session.execute(
            f"INSERT INTO {keyspace}.state (key, value, updated_at) "  # noqa: S608
            "VALUES ('fresh_clustering_active', 'test', toTimestamp(now()))"
        )


def _truncate_fresh(cassandra_host: str, cassandra_port: int, keyspace: str):
    from cassandra.cluster import Cluster

    with Cluster([cassandra_host], port=cassandra_port) as cluster:
        session = cluster.connect()
        for table in (
            "fresh_address_cluster",
            "fresh_cluster_addresses",
            "fresh_cluster_stats",
        ):
            session.execute(f"TRUNCATE {keyspace}.{table}")  # noqa: S608


class TestClusteringManufactured:
    """PySpark one-off and incremental clustering must agree on a crafted set."""

    def test_oneoff_vs_incremental(self, cassandra_coords, current_venv):
        host, port = cassandra_coords
        raw_ks = "clust_manu_raw"
        tks = "clust_manu_transformed"

        _create_raw_keyspace(host, port, raw_ks)
        _create_transformed_keyspace(host, port, tks)
        # run_fresh_clustering (the incremental subprocess) is gated on the
        # bootstrap marker; the spark one-off ignores it.
        _mark_fresh_active(host, port, tks)

        seed_args = dict(
            cassandra_host=host,
            cassandra_port=port,
            raw_keyspace=raw_ks,
            transformed_keyspace=tks,
            currency=CURRENCY,
            block_bucket_size=BLOCK_BUCKET_SIZE,
            tx_bucket_size=TX_BUCKET_SIZE,
            address_prefix_length=ADDRESS_PREFIX_LENGTH,
            address_bucket_size=ADDRESS_BUCKET_SIZE,
            addr_to_id=ADDR_TO_ID,
            txs=MANUFACTURED_TXS,
        )
        _run_in_venv(current_venv, _SEED_HELPER, seed_args)

        # --- PySpark one-off over the whole range ---
        _run_in_venv(
            current_venv,
            _SPARK_ONEOFF_HELPER,
            dict(
                cassandra_host=host,
                cassandra_port=port,
                raw_keyspace=raw_ks,
                transformed_keyspace=tks,
                max_address_id=max(ADDR_TO_ID.values()),
                bucket_size=ADDRESS_BUCKET_SIZE,
            ),
        )
        oneoff_mapping = _read_fresh_address_cluster(host, port, tks)
        assert oneoff_mapping, "PySpark one-off wrote no fresh_address_cluster rows"

        # --- incremental from empty, in batches, exercising a real two-cluster
        # merge: {D,E} is committed alone in batch (2,2) before the C-D edge in
        # batch (3,5) merges it with the existing {A,B,C}. (If C-D arrived in the
        # same batch as D-E it would be only a JOIN and never hit the merge
        # branch — survivor election, member_deletes, stats_deletes.)
        _truncate_fresh(host, port, tks)
        batches = [(0, 1), (2, 2), (3, LAST_BLOCK)]
        for b_start, b_end in batches:
            run_incremental_clustering_via_production(
                host,
                port,
                raw_ks,
                tks,
                CURRENCY,
                initial_mapping={},
                min_block_id=b_start,
                max_block_id=b_end,
                current_venv=current_venv,
            )
        incr_mapping = _read_fresh_address_cluster(host, port, tks)
        assert incr_mapping, (
            "incremental clustering wrote no fresh_address_cluster rows"
        )

        # --- partition equivalence + canonical-label invariant ---
        oneoff_clusters: dict[int, set[int]] = {}
        for addr_id, cluster_id in oneoff_mapping.items():
            oneoff_clusters.setdefault(cluster_id, set()).add(addr_id)
        incr_clusters: dict[int, set[int]] = {}
        for addr_id, cluster_id in incr_mapping.items():
            incr_clusters.setdefault(cluster_id, set()).add(addr_id)

        # cluster_id must be the canonical root == min(member address_id) on BOTH
        # paths (the one-off relabels its rank-roots, the incremental elects the
        # min survivor). A wrong-survivor / wrong-label regression fails here even
        # though the partition comparison below intentionally discards labels.
        for source, clusters in (
            ("one-off", oneoff_clusters),
            ("incremental", incr_clusters),
        ):
            for cid, members in clusters.items():
                assert cid == min(members), (
                    f"{source} cluster_id {cid} != min(member address_id) "
                    f"{min(members)} (members={sorted(members)})"
                )

        is_equivalent, mismatches = compare_cluster_partitions(
            oneoff_clusters, incr_mapping
        )
        assert is_equivalent, (
            "PySpark one-off vs incremental partition mismatch:\n"
            + "\n".join(f"  - {m}" for m in mismatches[:50])
        )

    def test_oneoff_endblock_vs_incremental(self, cassandra_coords, current_venv):
        """A one-off capped at --end-block N must equal incremental over [0, N].

        Caps at block 2: ``{A,B,C}`` and ``{D,E}`` exist but the block-3 merge
        (C-D) and the block-5 ``{G,H}`` cluster are excluded — so this fails if
        ``end_block`` does not actually restrict the one-off's read.
        """
        host, port = cassandra_coords
        raw_ks = "clust_manu_eb_raw"
        tks = "clust_manu_eb_transformed"
        end_block = 2

        _create_raw_keyspace(host, port, raw_ks)
        _create_transformed_keyspace(host, port, tks)
        _mark_fresh_active(host, port, tks)

        _run_in_venv(
            current_venv,
            _SEED_HELPER,
            dict(
                cassandra_host=host,
                cassandra_port=port,
                raw_keyspace=raw_ks,
                transformed_keyspace=tks,
                currency=CURRENCY,
                block_bucket_size=BLOCK_BUCKET_SIZE,
                tx_bucket_size=TX_BUCKET_SIZE,
                address_prefix_length=ADDRESS_PREFIX_LENGTH,
                address_bucket_size=ADDRESS_BUCKET_SIZE,
                addr_to_id=ADDR_TO_ID,
                txs=MANUFACTURED_TXS,
            ),
        )

        # --- PySpark one-off capped at end_block ---
        _run_in_venv(
            current_venv,
            _SPARK_ONEOFF_HELPER,
            dict(
                cassandra_host=host,
                cassandra_port=port,
                raw_keyspace=raw_ks,
                transformed_keyspace=tks,
                max_address_id=max(ADDR_TO_ID.values()),
                bucket_size=ADDRESS_BUCKET_SIZE,
                end_block=end_block,
            ),
        )
        oneoff_mapping = _read_fresh_address_cluster(host, port, tks)
        assert oneoff_mapping, (
            "capped PySpark one-off wrote no fresh_address_cluster rows"
        )

        # --- incremental from empty, only over [0, end_block] ---
        _truncate_fresh(host, port, tks)
        for b_start, b_end in [(0, 1), (2, end_block)]:
            run_incremental_clustering_via_production(
                host,
                port,
                raw_ks,
                tks,
                CURRENCY,
                initial_mapping={},
                min_block_id=b_start,
                max_block_id=b_end,
                current_venv=current_venv,
            )
        incr_mapping = _read_fresh_address_cluster(host, port, tks)
        assert incr_mapping, (
            "incremental clustering wrote no fresh_address_cluster rows"
        )

        oneoff_clusters: dict[int, set[int]] = {}
        for addr_id, cluster_id in oneoff_mapping.items():
            oneoff_clusters.setdefault(cluster_id, set()).add(addr_id)

        is_equivalent, mismatches = compare_cluster_partitions(
            oneoff_clusters, incr_mapping
        )
        assert is_equivalent, (
            f"capped one-off (end_block={end_block}) vs incremental mismatch:\n"
            + "\n".join(f"  - {m}" for m in mismatches[:50])
        )
