from datetime import datetime, timezone
from typing import List


def as_utc_datetime(value: str | datetime) -> datetime:
    dt = value if isinstance(value, datetime) else datetime.fromisoformat(value)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def normalize_date_bounds(
    start_date: str | datetime,
    end_date: str | datetime,
    min_start: str | datetime,
    most_recent_date: str | datetime | None = None,
) -> tuple[datetime, datetime]:
    start_dt = as_utc_datetime(start_date)
    end_dt = as_utc_datetime(end_date)
    min_start_dt = as_utc_datetime(min_start)

    if start_dt < min_start_dt:
        start_dt = min_start_dt

    if most_recent_date is not None:
        start_dt = as_utc_datetime(most_recent_date)

    return start_dt, end_dt


def convert_to_fiat(value: int, rates: List[int]) -> List[int]:
    # col(valueColumn) * x / 1e6 + 0.5).cast(LongType) / 100.0
    return [int(value * r / 1e6 + 0.5) / 100 for r in rates]
