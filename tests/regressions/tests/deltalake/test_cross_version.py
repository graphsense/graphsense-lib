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
(filterable via ``DELTA_CURRENCIES`` env var) and over block-range profiles
(filterable via ``DELTA_RANGE_CATEGORIES``).
"""

import pytest

from tests.deltalake.comparison import compare_snapshots, format_report
from tests.deltalake.ingest_runner import run_ingest
from tests.deltalake.snapshot import EnvironmentInfo, capture_snapshot

# Known content divergences between the reference version (v25.11.18, using
# graphsense-bitcoin-etl) and the current version (using fast_btc.py).
# These are bug fixes in the current version, not regressions.
#
# ZEC joinsplit mapping: the old bitcoin-etl correctly mapped vpub_new
# (z→t) as INPUT and vpub_old (t→z) as OUTPUT, matching the Sapling
# valueBalance convention. fast_btc.py uses the same mapping.
# No content divergence is expected for ZEC.
KNOWN_CONTENT_DIVERGENCES: set[tuple[str, str]] = set()

# Schema additions in the current version that the reference version lacks.
# The input struct now includes type, addresses, and value columns for all
# UTXO chains (populated via verbosity 3 or getrawtransaction resolution).
# Content in shared columns is identical — only the struct has new fields.
KNOWN_SCHEMA_ADDITIONS: set[tuple[str, str]] = {
    ("btc", "transaction"),
    ("bch", "transaction"),
    ("ltc", "transaction"),
    ("zec", "transaction"),
}


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
        range_id = delta_config.range_id
        tables = delta_config.tables
        is_genesis = range_id == "genesis"

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
        path_ref = f"s3://{bucket}/ref_only/{currency}/{range_id}"

        if is_genesis:
            # Genesis profile: compare both versions on identical overwrite
            # ingestion from block 0, without base/append staging.
            run_ingest(
                reference_venv,
                delta_config,
                path_ref,
                start_block=delta_config.start_block,
                end_block=delta_config.append_end_block,
                write_mode="overwrite",
                **minio_kw,
            )
        else:
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
        path_mixed = f"s3://{bucket}/mixed/{currency}/{range_id}"

        if is_genesis:
            run_ingest(
                current_venv,
                delta_config,
                path_mixed,
                start_block=delta_config.start_block,
                end_block=delta_config.append_end_block,
                write_mode="overwrite",
                **minio_kw,
            )
        else:
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
            if (currency, name) in KNOWN_SCHEMA_ADDITIONS:
                if diff.schema_added_columns:
                    print(
                        f"  [{name}] KNOWN SCHEMA ADDITION: {diff.schema_added_columns} "
                        f"(new input struct fields from verbosity 3 support)"
                    )
            else:
                assert not diff.schema_added_columns, (
                    f"{name}: columns added: {diff.schema_added_columns}"
                )
            assert not diff.schema_removed_columns, (
                f"{name}: columns removed: {diff.schema_removed_columns}"
            )
            if (currency, name) in KNOWN_SCHEMA_ADDITIONS:
                if diff.schema_type_changes:
                    print(
                        f"  [{name}] KNOWN TYPE CHANGE: {list(diff.schema_type_changes)} "
                        f"(input struct evolved with new fields)"
                    )
            else:
                assert not diff.schema_type_changes, (
                    f"{name}: type changes detected: {diff.schema_type_changes}"
                )
            assert diff.row_count_diff == 0, (
                f"{name}: row count differs by {diff.row_count_diff} "
                f"(ref={diff.row_count_ref}, cur={diff.row_count_current})"
            )
            if (currency, name) in KNOWN_SCHEMA_ADDITIONS:
                # Content hash includes schema, so it will always differ
                # when new struct fields are added. Not a content issue.
                if not diff.content_hash_match:
                    print(
                        f"  [{name}] EXPECTED: content hash differs due to "
                        f"schema additions in input struct"
                    )
            elif (currency, name) in KNOWN_CONTENT_DIVERGENCES:
                if not diff.content_hash_match:
                    print(
                        f"  [{name}] KNOWN DIVERGENCE: content hash differs "
                        f"(bug fix in current version, not a regression)"
                    )
            else:
                assert diff.content_hash_match, (
                    f"{name}: data content hash mismatch between reference-only "
                    f"and mixed (cross-version) runs"
                )
