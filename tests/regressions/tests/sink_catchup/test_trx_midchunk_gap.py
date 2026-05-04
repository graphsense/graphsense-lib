"""TRX raw mid-chunk gap regression test.

Replicates the historical incident where ``trx_raw`` was missing blocks
``82,245,705 .. 82,245,999`` (a 295-block hole inside one 1000-block file
chunk) while later blocks (``82,246,000+``) were ingested cleanly.

How that gap was created
========================

In the dual-sink (delta + cassandra) ``--write-mode append`` path, the
per-chunk write is **not** atomic across sinks::

    IngestRunner._transform_and_write(chunk):
        for sink in self.sinks:        # delta first, cassandra second
            sink.write(data)

When a chunk crosses the cassandra cluster mid-write, the ordering of
side-effects for chunk ``[K*1000, K*1000 + 999]`` is:

* delta:     ``DeltaDumpWriter.write`` also loops tables non-atomically,
             but each per-table ``write_deltalake`` call is its own
             atomic Delta commit. In a typical timeout pattern delta
             finishes its tables for the chunk before cassandra does.
* cassandra: ``cassandra_ingest`` calls ``execute_concurrent_with_args``
             with ``concurrency=100`` per table. After retries are
             exhausted, a portion of the rows have already landed and a
             portion have not (``_exe_with_retries`` re-raises with no
             rollback). The exception aborts the runner.

After the crash the on-disk state read by the resume logic is::

    delta.highest_block()      = K*1000 + 999       # max(block_id) on delta block table
    cassandra.highest_block()  = K*1000 + 704       # arbitrary partial prefix on cass block table

(Both ``highest_block`` queries look only at the ``block`` table, so
``transaction`` / ``log`` / ``trace`` / ``fee`` data missing for some
blocks below ``+704`` would still go undetected by either height.)

The pre-fix ``export_delta`` (commit ``8c2d4e3^``) auto-resumed from the
**delta** sink only:

    if write_mode == "append" and delta_sink is not None:
        highest_block = delta_sink.highest_block()
        ...
        start_block = highest_block + 1

So the very next dual-sink run started at ``K*1000 + 1000``. Cassandra
silently stayed at ``K*1000 + 704`` and the next write went to
``K*1000 + 1000`` — leaving a permanent 295-block hole in ``trx_raw``.
``get_highest_block()`` then reported the *new* max (e.g.
``K*1000 + 1009``) so all subsequent runs happily continued past the
hole. Nothing alerted on it because:

1. The ``IngestRunner`` log line prints ``Written blocks: K*1000 -
   K*1000 + 999`` from the **requested** chunk, not the actual data.
2. ``get_highest_block`` does ``MAX(block_id) WHERE block_id_group =
   max(groups)``, which is the *absolute* max — gaps below it are
   invisible to that query.

Two follow-up commits closed the hole:

* ``8c2d4e3 feat(ingest): refuse to ingest when sinks have diverged``
  added ``Sink.highest_block()`` for every sink and aborts the
  append-mode run if heights disagree.
* ``84aefa1 feat(ingest): auto-catch-up of diverged sinks before
  forward run`` runs single-sink ``IngestRunner`` over the missing
  tail before the dual-sink forward step proceeds.

What this test does
===================

The test recreates the **post-crash divergence** state via two
single-sink overwrites (the only way to reach it through the public CLI
once the ``8c2d4e3`` safeguard exists), then triggers the dual-sink
``append`` and asserts the gap has been filled.

1. **A — sync baseline**: dual-sink overwrite ``[start, end]`` →
   ``cass_A`` and ``delta_A`` both populated end-to-end.

2. **B1 — cassandra-only overwrite** ``[start, mid_low]`` →
   ``cass_B`` at ``mid_low``. ``mid_low`` is intentionally **not**
   aligned to ``file_batch_size`` (1000 for TRX) — it sits at
   ``+704`` inside the 1000-block chunk to mirror the prod geometry.

3. **B2 — delta-only overwrite** ``[start, mid_high]`` →
   ``delta_B`` at ``mid_high``. ``mid_high`` lies at ``+999``, the last
   block of the same 1000-block chunk. After this step the per-sink
   ``highest_block()`` values match what a mid-chunk cassandra crash
   would leave (cass=+704, delta=+999); the cleanly-overwritten side
   tables under cassandra differ from a real crash's partial
   sub-block-table state, but the resume logic exercised here only
   keys off ``block.highest_block`` so the divergence-and-heal path is
   the same.

4. **B3 — dual-sink append** from ``mid_high + 1`` to ``end``.
   ``_catch_up_diverged_sinks`` detects that cassandra lags by
   ``mid_high - mid_low`` blocks (here: 295) and runs a single-sink
   cassandra catch-up over ``[mid_low+1, mid_high]`` before the forward
   step writes ``[mid_high+1, end]`` to both sinks.

5. **No-gap invariant**: query every ``block_id`` in ``[start, end]``
   from ``cass_B`` and assert that none are missing. This is the
   property that was historically violated in ``trx_raw``.

6. **Equivalence**: per-table content hashes for ``cass_A`` vs ``cass_B``
   must match (same as the existing ``test_sink_catchup`` shape).

Failure mode without the fix
============================

If ``_catch_up_diverged_sinks`` were a no-op (the pre-``84aefa1``
world) and ``_abort_on_sink_divergence`` were also a no-op (the
pre-``8c2d4e3`` world), step B3 would write directly from
``mid_high + 1`` and the no-gap assertion would fail with exactly 295
missing block_ids, identical in shape to the production incident.
"""

import hashlib

import pytest

from tests.lib.config import (
    SCHEMA_TYPE_MAP,
    load_ingest_configs,
    resolve_gslib_path,
)
from tests.sink_catchup.config import SinkCatchupConfig
from tests.sink_catchup.ingest_runner import run_ingest

pytestmark = pytest.mark.sink_catchup

# Geometry of the production gap, projected onto a mid-chain TRX range
# the test node covers. file_batch_size for TRX is 1000, so a single
# file chunk spans [N*1000, N*1000 + 999]. The historical gap was
# 82,245,705..82,245,999 (cassandra at +704, delta at +999, 295-block
# hole), and we mirror that geometry exactly with the +704 / +999 split.
CHUNK_BASE = 50_245_000
CASS_LOW = CHUNK_BASE + 704      # cassandra crashed mid-chunk here
DELTA_HIGH = CHUNK_BASE + 999    # delta committed the full chunk
END_BLOCK = CHUNK_BASE + 1_010   # forward step past the chunk boundary
EXPECTED_GAP_SIZE = DELTA_HIGH - CASS_LOW  # 295 blocks

METADATA_TABLES = {"configuration", "summary_statistics", "state"}


def _table_content_hash(session, keyspace: str, table: str) -> tuple[int, str]:
    rows = list(session.execute(f"SELECT * FROM {keyspace}.{table}"))  # noqa: S608
    sorted_rows = sorted(str(sorted(row._asdict().items())) for row in rows)
    h = hashlib.sha256()
    for r in sorted_rows:
        h.update(r.encode())
    return len(rows), h.hexdigest()


def _block_ids_present(session, keyspace: str, block_bucket_size: int) -> set[int]:
    """Return the set of block_ids present in <keyspace>.block.

    Reads every (block_id_group, block_id) pair so no block can hide
    inside a partition the simple ``MAX()`` query would skip.
    """
    rows = session.execute(
        f"SELECT block_id_group, block_id FROM {keyspace}.block"  # noqa: S608
    )
    return {row.block_id for row in rows}


def _build_trx_catchup_config() -> SinkCatchupConfig | None:
    """Build a one-off SinkCatchupConfig pinned to the gap-replay range."""
    ingest_configs = load_ingest_configs()
    ic = ingest_configs.get("trx")
    if not ic:
        return None
    return SinkCatchupConfig(
        currency="trx",
        range_id="midchunk_gap_82245",
        node_url=ic["node_url"],
        secondary_node_references=ic.get("secondary_node_references", []),
        start_block=CHUNK_BASE,
        end_block=END_BLOCK,
        mid_block=CASS_LOW,
        schema_type=SCHEMA_TYPE_MAP.get("trx", "account"),
        gslib_path=resolve_gslib_path(),
        range_note=(
            f"replays the trx_raw 82,245,705-82,245,999 mid-chunk gap "
            f"({EXPECTED_GAP_SIZE} blocks) at the analogous mid-chain offset"
        ),
    )


@pytest.fixture
def trx_midchunk_config() -> SinkCatchupConfig:
    cfg = _build_trx_catchup_config()
    if cfg is None:
        pytest.skip("TRX node not configured in .graphsense.yaml")
    return cfg


class TestTrxMidchunkGap:
    """The 82,245,705 gap must not re-appear: dual-sink append must heal it."""

    def test_midchunk_gap_is_filled_by_auto_catchup(
        self,
        trx_midchunk_config: SinkCatchupConfig,
        minio_config: dict[str, str],
        storage_options: dict[str, str],
        cassandra_coords: tuple[str, int],
        current_venv,
    ):
        cfg = trx_midchunk_config
        cass_host, cass_port = cassandra_coords
        bucket = minio_config["bucket"]
        start = cfg.start_block
        end = cfg.end_block
        mid_low = cfg.mid_block
        mid_high = DELTA_HIGH

        # Sanity on the geometry — guards against accidental re-tuning
        # that would silently weaken the test.
        assert start <= mid_low < mid_high < end, (
            f"bad geometry: start={start}, mid_low={mid_low}, "
            f"mid_high={mid_high}, end={end}"
        )
        assert mid_high - start < 1_000 or (mid_high // 1_000) == (start // 1_000), (
            "mid_low and mid_high must lie inside the same 1000-block file chunk "
            "for the geometry to mirror the production gap"
        )
        assert mid_high - mid_low == EXPECTED_GAP_SIZE, (
            f"replay gap must be {EXPECTED_GAP_SIZE} blocks (matches prod), "
            f"got {mid_high - mid_low}"
        )

        ks_a = f"trxgap_{cfg.range_id}_a"   # sync baseline
        ks_b = f"trxgap_{cfg.range_id}_b"   # post-crash + auto-catchup
        delta_a = f"s3://{bucket}/baseline/trx/{cfg.range_id}"
        delta_b = f"s3://{bucket}/midchunk/trx/{cfg.range_id}"

        minio_kw = dict(
            minio_endpoint=minio_config["endpoint"],
            minio_access_key=minio_config["access_key"],
            minio_secret_key=minio_config["secret_key"],
        )

        print(f"\n{'=' * 72}")
        print(f"TRX MID-CHUNK GAP REPLAY [{cfg.range_id}]")
        print(f"  range:           {start:,}-{end:,} ({end - start + 1} blocks)")
        print(f"  cass-only mid:   {mid_low:,}  (= chunk_base + 704)")
        print(f"  delta-only mid:  {mid_high:,}  (= chunk_base + 999)")
        print(f"  replay gap:      {mid_low + 1:,}..{mid_high:,}  "
              f"({EXPECTED_GAP_SIZE} blocks — matches prod 82,245,705..82,245,999)")
        print(f"  note:            {cfg.range_note}")

        # ------------------------------------------------------------------
        # Phase A: sync baseline — dual-sink overwrite [start, end]
        # ------------------------------------------------------------------
        print("  [A]  sync baseline (dual-sink overwrite) ...", end=" ", flush=True)
        run_ingest(
            venv_dir=current_venv,
            config=cfg,
            sinks=["delta", "cassandra"],
            start_block=start,
            end_block=end,
            write_mode="overwrite",
            cassandra_host=cass_host,
            cassandra_port=cass_port,
            keyspace_name=ks_a,
            delta_directory=delta_a,
            label="sync baseline",
            **minio_kw,
        )
        print("done")

        # ------------------------------------------------------------------
        # Phase B1: cassandra-only overwrite [start, mid_low]
        #     This is what a mid-chunk cassandra crash effectively leaves
        #     behind: cassandra at +704, delta at +999 (next step).
        # ------------------------------------------------------------------
        print(f"  [B1] cass-only overwrite [start, mid_low={mid_low:,}] ...",
              end=" ", flush=True)
        run_ingest(
            venv_dir=current_venv,
            config=cfg,
            sinks=["cassandra"],
            start_block=start,
            end_block=mid_low,
            write_mode="overwrite",
            cassandra_host=cass_host,
            cassandra_port=cass_port,
            keyspace_name=ks_b,
            label="cass-only overwrite (post-crash cass state)",
        )
        print("done")

        # ------------------------------------------------------------------
        # Phase B2: delta-only overwrite [start, mid_high]
        #     Delta committed the whole chunk; this is the second half of
        #     the post-crash on-disk state.
        # ------------------------------------------------------------------
        print(f"  [B2] delta-only overwrite [start, mid_high={mid_high:,}] ...",
              end=" ", flush=True)
        run_ingest(
            venv_dir=current_venv,
            config=cfg,
            sinks=["delta"],
            start_block=start,
            end_block=mid_high,
            write_mode="overwrite",
            delta_directory=delta_b,
            label="delta-only overwrite (post-crash delta state)",
            **minio_kw,
        )
        print("done")

        # ------------------------------------------------------------------
        # Phase B3: dual-sink append [mid_high+1, end]
        #     Pre-fix code path would have started cassandra here too,
        #     leaving the 295-block gap forever. With the fix,
        #     _catch_up_diverged_sinks runs a cassandra-only catch-up
        #     over [mid_low+1, mid_high] before the forward step.
        # ------------------------------------------------------------------
        forward_start = mid_high + 1
        print(f"  [B3] dual-sink append [{forward_start:,}, {end:,}] "
              f"(must auto-catch-up {EXPECTED_GAP_SIZE} cassandra blocks) ...",
              end=" ", flush=True)
        run_ingest(
            venv_dir=current_venv,
            config=cfg,
            sinks=["delta", "cassandra"],
            start_block=forward_start,
            end_block=end,
            write_mode="append",
            cassandra_host=cass_host,
            cassandra_port=cass_port,
            keyspace_name=ks_b,
            delta_directory=delta_b,
            label="dual-sink append (catch-up + forward)",
            **minio_kw,
        )
        print("done")

        # ------------------------------------------------------------------
        # No-gap invariant: this is the property the prod incident violated.
        # ------------------------------------------------------------------
        from cassandra.cluster import Cluster

        cluster = Cluster([cass_host], port=cass_port)
        session = cluster.connect()
        try:
            present = _block_ids_present(session, ks_b, block_bucket_size=100)
            expected = set(range(start, end + 1))
            missing = sorted(expected - present)

            print(f"\n  no-gap check on {ks_b}.block:")
            print(f"    expected blocks: {len(expected):,}  "
                  f"({start:,}..{end:,})")
            print(f"    present blocks:  {len(present):,}")
            print(f"    missing blocks:  {len(missing):,}")
            if missing:
                head = missing[: min(10, len(missing))]
                tail = missing[-min(10, len(missing)):] if len(missing) > 10 else []
                print(f"    first missing:   {head}")
                if tail and tail != head:
                    print(f"    last missing:    {tail}")

            # Hard assertion: this is the regression guard.
            assert not missing, (
                f"trx mid-chunk gap reproduced: {len(missing)} blocks missing "
                f"in cassandra after auto-catch-up. First 10 missing: "
                f"{missing[:10]}. Auto-catch-up failed to heal the divergence."
            )

            # ------------------------------------------------------------------
            # Equivalence: cass_A (sync) vs cass_B (post-catchup) must match.
            # ------------------------------------------------------------------
            print("\n  cassandra equivalence (ks_a vs ks_b):")
            a_tables = sorted(
                row.table_name for row in session.execute(
                    "SELECT table_name FROM system_schema.tables "
                    "WHERE keyspace_name = %s",
                    (ks_a,),
                )
            )
            b_tables = sorted(
                row.table_name for row in session.execute(
                    "SELECT table_name FROM system_schema.tables "
                    "WHERE keyspace_name = %s",
                    (ks_b,),
                )
            )

            mismatches: list[str] = []
            for table_name in sorted(set(a_tables) & set(b_tables)):
                if table_name in METADATA_TABLES:
                    continue
                a_count, a_hash = _table_content_hash(session, ks_a, table_name)
                b_count, b_hash = _table_content_hash(session, ks_b, table_name)
                match = a_hash == b_hash
                status = "MATCH" if match else "MISMATCH"
                print(f"    {table_name:30s} a={a_count:>6,}  b={b_count:>6,}  {status}")
                if not match:
                    mismatches.append(
                        f"{table_name}: a={a_count} {a_hash[:12]}... "
                        f"b={b_count} {b_hash[:12]}..."
                    )

            assert not mismatches, (
                "cassandra content diverged between sync baseline and "
                "auto-catchup run:\n  - " + "\n  - ".join(mismatches)
            )

        finally:
            cluster.shutdown()

        print(f"  result:          PASS  (gap of {EXPECTED_GAP_SIZE} blocks healed)")
        print(f"{'=' * 72}")
