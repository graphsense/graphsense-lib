"""Compare two IngestionSnapshots and produce a structured diff report."""

from __future__ import annotations

from dataclasses import dataclass, field

from tests.deltalake.snapshot import IngestionSnapshot


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
    file_count_ref: int = 0
    file_count_current: int = 0
    file_count_diff: int = 0
    file_names_changed: bool = False
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

        diff = TableDiff(
            table_name=name,
            schema_added_columns=added,
            schema_removed_columns=removed,
            schema_type_changes=type_changes,
            row_count_ref=ref_table.row_count,
            row_count_current=cur_table.row_count,
            row_count_diff=cur_table.row_count - ref_table.row_count,
            content_hash_match=(ref_table.content_hash == cur_table.content_hash),
            file_count_ref=len(ref_table.file_names),
            file_count_current=len(cur_table.file_names),
            file_count_diff=len(cur_table.file_names) - len(ref_table.file_names),
            file_names_changed=(ref_table.file_names != cur_table.file_names),
            file_size_diff_bytes=cur_table.total_file_size_bytes - ref_table.total_file_size_bytes,
        )
        report.table_diffs[name] = diff

    return report


def _format_environment_section(ref: IngestionSnapshot, current: IngestionSnapshot) -> list[str]:
    """Format environment context as header lines."""
    lines = []

    # Block range (same for both)
    lines.append(f"  Currency:     {ref.environment.currency or 'n/a'}")
    lines.append(f"  Node URL:     {ref.environment.node_url or 'n/a'}")
    lines.append(f"  Block range:  {ref.block_range[0]} - {ref.block_range[1]}")
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
        lines.append(f"    Files: ref={diff.file_count_ref} cur={diff.file_count_current}")

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
