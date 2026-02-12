"""Cross-version Delta Lake compatibility test.

Verifies that the *current* graphsense-lib version can append to Delta Lake
tables created by a *reference* version without introducing schema drift,
data corruption, or silent incompatibilities.

Test flow
---------
1. **Reference-only** (baseline truth):
   ref ingests base blocks, ref appends more blocks → snapshot A
2. **Mixed** (cross-version):
   ref ingests same base blocks, *current* appends same blocks → snapshot B
3. **Compare**: A vs B — schema, row counts, content hashes, file metadata

The test is parametrized over all currencies configured in .graphsense.yaml
(filterable via ``DELTA_CURRENCIES`` env var).
"""

import pytest

from tests.deltalake.comparison import compare_snapshots, format_report
from tests.deltalake.ingest_runner import run_ingest
from tests.deltalake.snapshot import EnvironmentInfo, capture_snapshot


@pytest.mark.deltalake
class TestCrossVersionCompatibility:
    """Current graphsense-lib appending to reference-created tables must
    produce identical data as reference-only."""

    def test_current_produces_same_output_as_reference(
        self,
        minio_config,
        storage_options,
        delta_config,
        reference_venv,
        current_venv,
        ref_package_versions,
        current_package_versions,
    ):
        bucket = minio_config["bucket"]
        currency = delta_config.currency
        tables = delta_config.tables

        minio_kw = dict(
            minio_endpoint=minio_config["endpoint"],
            minio_access_key=minio_config["access_key"],
            minio_secret_key=minio_config["secret_key"],
        )

        ref_env = EnvironmentInfo(
            version_label=f"reference ({delta_config.ref_version})",
            package_versions=ref_package_versions,
            currency=delta_config.currency,
            node_url=delta_config.node_url,
        )
        cur_env = EnvironmentInfo(
            version_label="current (dev)",
            package_versions=current_package_versions,
            currency=delta_config.currency,
            node_url=delta_config.node_url,
        )

        # ------------------------------------------------------------------
        # Scenario A: reference-only (baseline truth)
        # ------------------------------------------------------------------
        path_ref = f"s3://{bucket}/ref_only/{currency}"

        # Base ingestion (overwrite)
        run_ingest(
            reference_venv, delta_config, path_ref,
            start_block=delta_config.start_block,
            end_block=delta_config.base_end_block,
            write_mode="overwrite",
            **minio_kw,
        )
        # Append
        run_ingest(
            reference_venv, delta_config, path_ref,
            start_block=delta_config.append_start_block,
            end_block=delta_config.append_end_block,
            write_mode="append",
            **minio_kw,
        )

        snapshot_ref = capture_snapshot(
            storage_options, path_ref, tables, f"reference ({delta_config.ref_version})",
            block_range=(delta_config.start_block, delta_config.append_end_block),
        )
        snapshot_ref.environment = ref_env

        # ------------------------------------------------------------------
        # Scenario B: reference base + current append (cross-version)
        # ------------------------------------------------------------------
        path_mixed = f"s3://{bucket}/mixed/{currency}"

        # Base ingestion with reference version
        run_ingest(
            reference_venv, delta_config, path_mixed,
            start_block=delta_config.start_block,
            end_block=delta_config.base_end_block,
            write_mode="overwrite",
            **minio_kw,
        )
        # Append with CURRENT version
        run_ingest(
            current_venv, delta_config, path_mixed,
            start_block=delta_config.append_start_block,
            end_block=delta_config.append_end_block,
            write_mode="append",
            **minio_kw,
        )

        snapshot_mixed = capture_snapshot(
            storage_options, path_mixed, tables, "current (dev)",
            block_range=(delta_config.start_block, delta_config.append_end_block),
        )
        snapshot_mixed.environment = cur_env

        # ------------------------------------------------------------------
        # Compare
        # ------------------------------------------------------------------
        report = compare_snapshots(snapshot_ref, snapshot_mixed)

        # Print report for CI visibility
        print("\n" + format_report(report, snapshot_ref, snapshot_mixed))

        # Assertions with clear messages per table
        for name, diff in report.table_diffs.items():
            assert not diff.schema_added_columns, (
                f"{name}: columns added: {diff.schema_added_columns}"
            )
            assert not diff.schema_removed_columns, (
                f"{name}: columns removed: {diff.schema_removed_columns}"
            )
            assert not diff.schema_type_changes, (
                f"{name}: type changes detected: {diff.schema_type_changes}"
            )
            assert diff.row_count_diff == 0, (
                f"{name}: row count differs by {diff.row_count_diff} "
                f"(ref={diff.row_count_ref}, cur={diff.row_count_current})"
            )
            assert diff.content_hash_match, (
                f"{name}: data content hash mismatch between reference-only "
                f"and mixed (cross-version) runs"
            )
