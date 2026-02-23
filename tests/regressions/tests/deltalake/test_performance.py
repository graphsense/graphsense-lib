"""Delta Lake ingest performance tests.

Measures ingest speed for:
1. Full ingest comparison: reference vs current using the same subprocess
   execution path. Runs are repeated and compared by median wall-clock time.
2. Table-level ingest: single-table ingest speed with table_filter.

Parametrized over currencies configured in .graphsense.yaml (filterable via
``DELTA_CURRENCIES`` env var) and currency x table pairs.
"""

import os
from statistics import median

import pytest

from tests.deltalake.ingest_runner import timed_run_ingest
from tests.deltalake.perf_report import compare_timing
from tests.deltalake.perf_runner import run_timed_ingest
from tests.deltalake.timing import IngestTimingResult


@pytest.mark.deltalake_perf
class TestIngestPerformance:
    """Compare ingest speed between reference and current version."""

    def test_ingest_speed_comparison(
        self,
        minio_config,
        storage_options,
        delta_config,
        reference_venv,
        current_venv,
        perf_report_collector,
    ):
        bucket = minio_config["bucket"]
        currency = delta_config.currency
        range_id = delta_config.range_id
        start_block = delta_config.start_block
        end_block = delta_config.perf_end_block

        minio_kw = dict(
            minio_endpoint=minio_config["endpoint"],
            minio_access_key=minio_config["access_key"],
            minio_secret_key=minio_config["secret_key"],
        )

        repeats = int(os.environ.get("DELTA_PERF_REPEATS", "3"))
        assert repeats >= 1, "DELTA_PERF_REPEATS must be >= 1"

        # Use identical subprocess mode for reference and current, then compare
        # medians to reduce noise.
        ref_runs = []
        cur_runs = []
        print(
            f"\nPERF RUN START: {currency}.{range_id} "
            f"blocks={start_block:,}-{end_block:,} repeats={repeats}"
        )
        for i in range(repeats):
            path_ref = f"s3://{bucket}/perf_ref/{currency}/{range_id}/run_{i}"
            path_cur = f"s3://{bucket}/perf_cur/{currency}/{range_id}/run_{i}"
            print(
                f"  [{i + 1}/{repeats}] reference ingest "
                f"({start_block:,}-{end_block:,}) ..."
            )
            ref_runs.append(
                timed_run_ingest(
                    reference_venv,
                    delta_config,
                    path_ref,
                    start_block=start_block,
                    end_block=end_block,
                    write_mode="overwrite",
                    **minio_kw,
                )
            )
            print(
                f"  [{i + 1}/{repeats}] reference done: {ref_runs[-1]:.1f}s"
            )
            print(
                f"  [{i + 1}/{repeats}] current ingest "
                f"({start_block:,}-{end_block:,}) ..."
            )
            cur_runs.append(
                timed_run_ingest(
                    current_venv,
                    delta_config,
                    path_cur,
                    start_block=start_block,
                    end_block=end_block,
                    write_mode="overwrite",
                    **minio_kw,
                )
            )
            print(
                f"  [{i + 1}/{repeats}] current done: {cur_runs[-1]:.1f}s"
            )

        ref_wall_clock_s = median(ref_runs)
        cur_wall_clock_s = median(cur_runs)
        current_result = IngestTimingResult(
            wall_clock_s=cur_wall_clock_s,
            currency=currency,
            start_block=start_block,
            end_block=end_block,
        )

        # Build comparison and collect for report
        comparison = compare_timing(ref_wall_clock_s, current_result)
        perf_report_collector.append(comparison)

        # Print per-currency summary
        print(f"\n{'=' * 60}")
        print(f"PERF: {currency.upper()} [{range_id}]")
        print(f"  Blocks:       {start_block:,} - {end_block:,} ({comparison.num_blocks} blocks)")
        print(f"  Repeats:      {repeats}")
        print(f"  Ref runs:     {', '.join(f'{t:.1f}s' for t in ref_runs)}")
        print(f"  Cur runs:     {', '.join(f'{t:.1f}s' for t in cur_runs)}")
        print(f"  Ref:          {ref_wall_clock_s:.1f}s")
        print(f"  Current:      {cur_wall_clock_s:.1f}s")
        print(f"  Speedup:      {comparison.speedup:.2f}x")
        print(f"  Blocks/s:     {current_result.blocks_per_second:.1f}")
        print(f"{'=' * 60}")

        # Assert current is not more than 2x slower than reference
        assert comparison.speedup > 0.5, (
            f"{currency}: current version is {1/comparison.speedup:.1f}x slower than "
            f"reference (threshold: 2.0x). ref={ref_wall_clock_s:.1f}s, "
            f"current={current_result.wall_clock_s:.1f}s"
        )


@pytest.mark.deltalake_perf
class TestTableLevelPerformance:
    """Measure per-table ingest speed using table_filter."""

    def test_single_table_ingest_speed(
        self,
        minio_config,
        storage_options,
        delta_config,
        table_name,
    ):
        bucket = minio_config["bucket"]
        currency = delta_config.currency
        range_id = delta_config.range_id
        start_block = delta_config.start_block
        end_block = delta_config.perf_end_block

        s3_credentials = {
            "AWS_ENDPOINT_URL": minio_config["endpoint"],
            "AWS_ACCESS_KEY_ID": minio_config["access_key"],
            "AWS_SECRET_ACCESS_KEY": minio_config["secret_key"],
            "AWS_REGION": "us-east-1",
            "AWS_ALLOW_HTTP": "true",
            "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
        }

        path = f"s3://{bucket}/perf_table/{currency}/{range_id}/{table_name}"
        print(
            f"\n  PERF TABLE START: {currency}.{range_id}.{table_name} "
            f"blocks={start_block:,}-{end_block:,}"
        )
        result = run_timed_ingest(
            config=delta_config,
            delta_directory=path,
            start_block=start_block,
            end_block=end_block,
            write_mode="overwrite",
            s3_credentials=s3_credentials,
            table_filter=[table_name],
        )

        # Print per-table summary
        table_rps = result.rows_per_second_by_table.get(table_name, 0.0)
        print(f"\n  PERF TABLE: {currency}.{range_id}.{table_name}")
        print(f"    Wall clock:  {result.wall_clock_s:.1f}s")
        print(f"    Blocks/s:    {result.blocks_per_second:.1f}")
        print(f"    Rows/s:      {table_rps:.1f}")
        print(f"    Source:      {result.total_source_s:.1f}s")
        print(f"    Transform:   {result.total_transform_s:.1f}s")
        print(f"    Sink:        {result.total_sink_s:.1f}s")

        # Sanity check
        assert result.wall_clock_s > 0, (
            f"{currency}.{table_name}: ingest completed in 0s — something is wrong"
        )
