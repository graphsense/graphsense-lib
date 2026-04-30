"""Timing data structures for Delta Lake ingest performance measurement."""

from dataclasses import dataclass, field


@dataclass
class ChunkTiming:
    """Timing for a single file-batch chunk of blocks."""
    start_block: int
    end_block: int
    num_blocks: int
    source_s: float
    transform_s: float
    sink_s: float

    @property
    def wall_clock_s(self) -> float:
        return self.source_s + self.transform_s + self.sink_s


@dataclass
class TableWriteTiming:
    """Timing for writing a single table within a chunk."""
    table_name: str
    rows_written: int
    duration_s: float


@dataclass
class IngestTimingResult:
    """Full result from a timed ingest run."""
    wall_clock_s: float
    currency: str
    start_block: int
    end_block: int
    chunk_timings: list[ChunkTiming] = field(default_factory=list)
    table_write_timings: list[TableWriteTiming] = field(default_factory=list)

    @property
    def num_blocks(self) -> int:
        return self.end_block - self.start_block + 1

    @property
    def blocks_per_second(self) -> float:
        if self.wall_clock_s <= 0:
            return 0.0
        return self.num_blocks / self.wall_clock_s

    @property
    def total_source_s(self) -> float:
        return sum(c.source_s for c in self.chunk_timings)

    @property
    def total_transform_s(self) -> float:
        return sum(c.transform_s for c in self.chunk_timings)

    @property
    def total_sink_s(self) -> float:
        return sum(c.sink_s for c in self.chunk_timings)

    @property
    def rows_per_second_by_table(self) -> dict[str, float]:
        """Aggregate rows/s per table across all chunks."""
        table_rows: dict[str, int] = {}
        table_time: dict[str, float] = {}
        for tw in self.table_write_timings:
            table_rows[tw.table_name] = table_rows.get(tw.table_name, 0) + tw.rows_written
            table_time[tw.table_name] = table_time.get(tw.table_name, 0.0) + tw.duration_s
        return {
            name: table_rows[name] / table_time[name] if table_time[name] > 0 else 0.0
            for name in table_rows
        }
