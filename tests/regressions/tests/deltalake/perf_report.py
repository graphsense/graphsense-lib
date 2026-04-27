"""Performance comparison and report generation for Delta Lake ingest tests."""

import json
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path


@dataclass
class PerfComparison:
    """Comparison between reference and current ingest run."""
    currency: str
    block_range: tuple[int, int]
    num_blocks: int
    ref_wall_clock_s: float
    current_wall_clock_s: float
    current_blocks_per_s: float
    current_rows_per_s_by_table: dict[str, float] = field(default_factory=dict)
    current_source_s: float = 0.0
    current_transform_s: float = 0.0
    current_sink_s: float = 0.0

    @property
    def speedup(self) -> float:
        if self.current_wall_clock_s <= 0:
            return 0.0
        return self.ref_wall_clock_s / self.current_wall_clock_s


def compare_timing(ref_wall_clock_s: float, current) -> PerfComparison:
    """Build a PerfComparison from a reference wall-clock time and current IngestTimingResult."""

    return PerfComparison(
        currency=current.currency,
        block_range=(current.start_block, current.end_block),
        num_blocks=current.num_blocks,
        ref_wall_clock_s=ref_wall_clock_s,
        current_wall_clock_s=current.wall_clock_s,
        current_blocks_per_s=current.blocks_per_second,
        current_rows_per_s_by_table=current.rows_per_second_by_table,
        current_source_s=current.total_source_s,
        current_transform_s=current.total_transform_s,
        current_sink_s=current.total_sink_s,
    )


def format_perf_report(comparisons: list[PerfComparison]) -> str:
    """Format comparisons as a terminal-printable report."""
    lines = []
    lines.append("=" * 80)
    lines.append("DELTA LAKE INGEST PERFORMANCE REPORT")
    lines.append("=" * 80)

    if not comparisons:
        lines.append("No performance data collected.")
        return "\n".join(lines)

    overall_ref = sum(c.ref_wall_clock_s for c in comparisons)
    overall_cur = sum(c.current_wall_clock_s for c in comparisons)
    overall_speedup = overall_ref / overall_cur if overall_cur > 0 else 0.0

    lines.append(f"Currencies tested: {len(comparisons)}")
    lines.append(f"Overall speedup:   {overall_speedup:.2f}x")
    lines.append("")

    for comp in comparisons:
        lines.append(f"--- {comp.currency.upper()} ---")
        lines.append(f"  Block range:     {comp.block_range[0]:,} - {comp.block_range[1]:,} ({comp.num_blocks} blocks)")
        lines.append(f"  Ref wall clock:  {comp.ref_wall_clock_s:.1f}s")
        lines.append(f"  Cur wall clock:  {comp.current_wall_clock_s:.1f}s")
        lines.append(f"  Speedup:         {comp.speedup:.2f}x")
        lines.append(f"  Blocks/s:        {comp.current_blocks_per_s:.1f}")
        lines.append(f"  Phase breakdown: source={comp.current_source_s:.1f}s  transform={comp.current_transform_s:.1f}s  sink={comp.current_sink_s:.1f}s")
        if comp.current_rows_per_s_by_table:
            lines.append("  Rows/s by table:")
            for table, rps in sorted(comp.current_rows_per_s_by_table.items()):
                lines.append(f"    {table:<20s} {rps:.1f} rows/s")
        lines.append("")

    lines.append("=" * 80)
    return "\n".join(lines)


def save_perf_report(comparisons: list[PerfComparison], report_dir: Path | None = None) -> Path:
    """Save performance report as JSON."""
    if report_dir is None:
        report_dir = Path("reports")
    report_dir.mkdir(parents=True, exist_ok=True)

    overall_ref = sum(c.ref_wall_clock_s for c in comparisons)
    overall_cur = sum(c.current_wall_clock_s for c in comparisons)

    report = {
        "timestamp": datetime.now().isoformat(),
        "summary": {
            "currencies_tested": len(comparisons),
            "overall_speedup": overall_ref / overall_cur if overall_cur > 0 else 0.0,
        },
        "comparisons": [
            {
                "currency": c.currency,
                "block_range": list(c.block_range),
                "num_blocks": c.num_blocks,
                "ref_wall_clock_s": round(c.ref_wall_clock_s, 2),
                "current_wall_clock_s": round(c.current_wall_clock_s, 2),
                "speedup": round(c.speedup, 3),
                "current_blocks_per_s": round(c.current_blocks_per_s, 1),
                "current_rows_per_s_by_table": {
                    k: round(v, 1) for k, v in c.current_rows_per_s_by_table.items()
                },
                "current_source_s": round(c.current_source_s, 1),
                "current_transform_s": round(c.current_transform_s, 1),
                "current_sink_s": round(c.current_sink_s, 1),
            }
            for c in comparisons
        ],
    }

    report_path = report_dir / "deltalake_perf_report.json"
    report_path.write_text(json.dumps(report, indent=2))
    return report_path
