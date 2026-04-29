"""Delta Lake ingest performance tests.

Single test per currency×range: runs the full ingest pipeline once and
reports timing.  No repeated runs, no per-table decomposition — that
level of detail belongs in dedicated benchmarking scripts, not CI.

Parametrized over currencies configured in .graphsense.yaml (filterable via
``DELTA_CURRENCIES`` env var and ``DELTA_RANGE_CATEGORIES``).
"""

import pytest

from tests.deltalake.perf_runner import run_timed_ingest
from tests.deltalake.timing import IngestTimingResult


@pytest.mark.deltalake_perf
class TestIngestPerformance:
    """Full-pipeline ingest speed per currency×range (single run)."""

    def test_ingest_speed(
        self,
        minio_config,
        storage_options,
        delta_config,
        perf_report_collector,
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

        path = f"s3://{bucket}/perf/{currency}/{range_id}"
        print(
            f"\nPERF: {currency}.{range_id} "
            f"blocks={start_block:,}-{end_block:,}"
        )

        result = run_timed_ingest(
            config=delta_config,
            delta_directory=path,
            start_block=start_block,
            end_block=end_block,
            write_mode="overwrite",
            s3_credentials=s3_credentials,
        )

        print(f"  Wall clock:  {result.wall_clock_s:.1f}s")
        print(f"  Blocks/s:    {result.blocks_per_second:.1f}")
        print(f"  Source:      {result.total_source_s:.1f}s")
        print(f"  Transform:   {result.total_transform_s:.1f}s")
        print(f"  Sink:        {result.total_sink_s:.1f}s")

        assert result.wall_clock_s > 0, (
            f"{currency}: ingest completed in 0s — something is wrong"
        )
