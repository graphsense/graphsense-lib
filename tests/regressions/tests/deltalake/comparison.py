"""Compare two IngestionSnapshots and produce a structured diff report."""

from __future__ import annotations

from dataclasses import dataclass, field

from tests.deltalake.snapshot import IngestionSnapshot, _compute_content_hash, _sortable_type


def _format_bytes(num_bytes: int) -> str:
    """Format byte count in IEC units for readability."""
    units = ["B", "KiB", "MiB", "GiB", "TiB"]
    value = float(num_bytes)
    for unit in units:
        if value < 1024.0 or unit == units[-1]:
            if unit == "B":
                return f"{int(value)} {unit}"
            return f"{value:.2f} {unit}"
        value /= 1024.0
    return f"{int(num_bytes)} B"


@dataclass
class TableDiff:
    """Differences between two snapshots of the same table."""

    table_name: str
    schema_added_columns: list[str] = field(default_factory=list)
    schema_removed_columns: list[str] = field(default_factory=list)
    schema_type_changes: dict[str, tuple[str, str]] = field(default_factory=dict)
    row_count_ref: int = 0
    row_count_current: int = 0
    row_count_diff: int = 0
    content_hash_match: bool = False
    common_columns_content_hash_match: bool | None = None  # None when schemas identical
    differing_common_columns: list[str] = field(default_factory=list)  # columns that differ
    file_count_ref: int = 0
    file_count_current: int = 0
    file_count_diff: int = 0
    file_names_changed: bool = False
    file_size_ref_bytes: int = 0
    file_size_current_bytes: int = 0
    file_size_diff_bytes: int = 0

    @property
    def is_identical(self) -> bool:
        return (
            not self.schema_added_columns
            and not self.schema_removed_columns
            and not self.schema_type_changes
            and self.row_count_diff == 0
            and self.content_hash_match
        )


@dataclass
class ComparisonReport:
    """Full comparison between reference and current snapshots."""

    reference_label: str
    current_label: str
    table_diffs: dict[str, TableDiff] = field(default_factory=dict)

    @property
    def all_identical(self) -> bool:
        return all(d.is_identical for d in self.table_diffs.values())


def _compare_schemas(
    ref_schema: dict[str, str],
    cur_schema: dict[str, str],
) -> tuple[list[str], list[str], dict[str, tuple[str, str]]]:
    """Compare two column→type dicts and return (added, removed, type_changes)."""
    ref_cols = set(ref_schema.keys())
    cur_cols = set(cur_schema.keys())

    added = sorted(cur_cols - ref_cols)
    removed = sorted(ref_cols - cur_cols)

    type_changes = {}
    for col in sorted(ref_cols & cur_cols):
        if ref_schema[col] != cur_schema[col]:
            type_changes[col] = (ref_schema[col], cur_schema[col])

    return added, removed, type_changes


def compare_snapshots(ref: IngestionSnapshot, current: IngestionSnapshot) -> ComparisonReport:
    """Compare *ref* and *current* snapshots, returning a structured report."""
    report = ComparisonReport(
        reference_label=ref.version_label,
        current_label=current.version_label,
    )

    all_tables = set(ref.tables.keys()) | set(current.tables.keys())

    for name in sorted(all_tables):
        ref_table = ref.tables.get(name)
        cur_table = current.tables.get(name)

        if ref_table is None or cur_table is None:
            # Table missing from one side — record as maximally different
            diff = TableDiff(
                table_name=name,
                schema_removed_columns=list(ref_table.schema.keys()) if ref_table else [],
                schema_added_columns=list(cur_table.schema.keys()) if cur_table else [],
                row_count_ref=ref_table.row_count if ref_table else 0,
                row_count_current=cur_table.row_count if cur_table else 0,
                row_count_diff=(cur_table.row_count if cur_table else 0) - (ref_table.row_count if ref_table else 0),
            )
            report.table_diffs[name] = diff
            continue

        added, removed, type_changes = _compare_schemas(
            ref_table.schema, cur_table.schema
        )

        # When schemas differ, compute hash on common columns to detect
        # data regressions even when new columns have been added.
        common_hash_match = None
        differing_cols = []
        has_schema_diff = added or removed or type_changes
        if (
            has_schema_diff
            and ref_table._arrow_table is not None
            and cur_table._arrow_table is not None
        ):
            common_cols = sorted(
                set(ref_table.column_names) & set(cur_table.column_names)
            )
            # Only compare columns whose types also match
            common_cols = [
                c for c in common_cols
                if ref_table.schema.get(c) == cur_table.schema.get(c)
            ]
            if common_cols:
                ref_projected = ref_table._arrow_table.select(common_cols)
                cur_projected = cur_table._arrow_table.select(common_cols)
                common_hash_match = (
                    _compute_content_hash(ref_projected)
                    == _compute_content_hash(cur_projected)
                )
                if not common_hash_match:
                    # Two-phase comparison:
                    # 1. Sortable columns: compare as independently sorted
                    #    multisets (immune to row-order and cross-column
                    #    contamination from differing values).
                    # 2. Non-sortable columns (lists, structs): align rows
                    #    by sorting on ALL sortable columns that matched in
                    #    phase 1, then compare element-wise.
                    import pyarrow.compute as pc

                    sortable = [
                        c for c in common_cols
                        if _sortable_type(ref_projected.schema.field(c).type)
                    ]
                    non_sortable = [c for c in common_cols if c not in sortable]

                    # Phase 1: per-column multiset comparison for sortable cols
                    matching_sortable = []
                    for c in sortable:
                        rc = ref_projected.column(c).take(pc.sort_indices(ref_projected.column(c)))
                        cc = cur_projected.column(c).take(pc.sort_indices(cur_projected.column(c)))
                        if rc != cc:
                            differing_cols.append(c)
                        else:
                            matching_sortable.append(c)

                    # Phase 2: align rows by stable sort keys, then compare
                    # non-sortable columns element-wise.
                    if non_sortable and matching_sortable:
                        sort_keys = [(c, "ascending") for c in matching_sortable]
                        ref_s = ref_projected.sort_by(sort_keys)
                        cur_s = cur_projected.sort_by(sort_keys)
                        for c in non_sortable:
                            if ref_s.column(c) != cur_s.column(c):
                                differing_cols.append(c)
                    elif non_sortable:
                        # No stable sort keys — can't align, mark as differing
                        differing_cols.extend(non_sortable)

        diff = TableDiff(
            table_name=name,
            schema_added_columns=added,
            schema_removed_columns=removed,
            schema_type_changes=type_changes,
            row_count_ref=ref_table.row_count,
            row_count_current=cur_table.row_count,
            row_count_diff=cur_table.row_count - ref_table.row_count,
            content_hash_match=(ref_table.content_hash == cur_table.content_hash),
            common_columns_content_hash_match=common_hash_match,
            differing_common_columns=differing_cols,
            file_count_ref=len(ref_table.file_names),
            file_count_current=len(cur_table.file_names),
            file_count_diff=len(cur_table.file_names) - len(ref_table.file_names),
            file_names_changed=(ref_table.file_names != cur_table.file_names),
            file_size_ref_bytes=ref_table.total_file_size_bytes,
            file_size_current_bytes=cur_table.total_file_size_bytes,
            file_size_diff_bytes=cur_table.total_file_size_bytes - ref_table.total_file_size_bytes,
        )
        report.table_diffs[name] = diff

    return report


def _format_environment_section(ref: IngestionSnapshot, current: IngestionSnapshot) -> list[str]:
    """Format environment context as header lines."""
    lines = []
    size_diff = current.total_file_size_bytes - ref.total_file_size_bytes
    size_diff_human = _format_bytes(abs(size_diff))

    # Block range (same for both)
    lines.append(f"  Currency:     {ref.environment.currency or 'n/a'}")
    lines.append(f"  Node URL:     {ref.environment.node_url or 'n/a'}")
    lines.append(f"  Block range:  {ref.block_range[0]} - {ref.block_range[1]}")
    lines.append(
        f"  Output size:  ref={_format_bytes(ref.total_file_size_bytes)} "
        f"cur={_format_bytes(current.total_file_size_bytes)} "
        f"(diff={size_diff:+,} B / {size_diff_human})"
    )
    lines.append("")

    # Package versions side by side
    all_pkgs = sorted(
        set(ref.environment.package_versions) | set(current.environment.package_versions)
    )
    if all_pkgs:
        lines.append(f"  {'Package':<20s} {'Reference':<25s} {'Current':<25s}")
        lines.append(f"  {'-' * 20} {'-' * 25} {'-' * 25}")
        for pkg in all_pkgs:
            ref_ver = ref.environment.package_versions.get(pkg, "n/a")
            cur_ver = current.environment.package_versions.get(pkg, "n/a")
            marker = " *" if ref_ver != cur_ver else ""
            lines.append(f"  {pkg:<20s} {ref_ver:<25s} {cur_ver:<25s}{marker}")

    return lines


def format_report(
    report: ComparisonReport,
    ref_snapshot: IngestionSnapshot | None = None,
    current_snapshot: IngestionSnapshot | None = None,
) -> str:
    """Human-readable summary of a ComparisonReport."""
    lines = [
        f"Delta Lake Cross-Version Comparison: {report.reference_label} vs {report.current_label}",
        "=" * 80,
    ]

    # Environment context
    if (
        ref_snapshot
        and current_snapshot
        and ref_snapshot.environment
        and current_snapshot.environment
    ):
        lines.append("")
        lines.extend(_format_environment_section(ref_snapshot, current_snapshot))

    lines.append("")
    lines.append("Table comparison:")
    lines.append("-" * 80)

    for name, diff in sorted(report.table_diffs.items()):
        status = "IDENTICAL" if diff.is_identical else "DIFFERS"
        lines.append(f"\n  [{status}] {name}")
        lines.append(f"    Rows: ref={diff.row_count_ref} cur={diff.row_count_current} (diff={diff.row_count_diff:+d})")
        lines.append(f"    Content hash match: {diff.content_hash_match}")
        if diff.common_columns_content_hash_match is not None:
            lines.append(f"    Common-columns hash match: {diff.common_columns_content_hash_match}")
        if diff.differing_common_columns:
            lines.append(f"    Differing columns: {', '.join(diff.differing_common_columns)}")
        lines.append(f"    Files: ref={diff.file_count_ref} cur={diff.file_count_current}")
        lines.append(
            "    Size: "
            f"ref={_format_bytes(diff.file_size_ref_bytes)} "
            f"cur={_format_bytes(diff.file_size_current_bytes)} "
            f"(diff={diff.file_size_diff_bytes:+,} B)"
        )

        if diff.schema_added_columns:
            lines.append(f"    + Added columns: {', '.join(diff.schema_added_columns)}")
        if diff.schema_removed_columns:
            lines.append(f"    - Removed columns: {', '.join(diff.schema_removed_columns)}")
        if diff.schema_type_changes:
            for col, (old, new) in diff.schema_type_changes.items():
                lines.append(f"    ~ Type change: {col}: {old} -> {new}")

    lines.append("")
    overall = "ALL IDENTICAL" if report.all_identical else "DIFFERENCES FOUND"
    lines.append(f"Overall: {overall}")
    return "\n".join(lines)
