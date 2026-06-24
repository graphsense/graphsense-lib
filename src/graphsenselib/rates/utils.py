from datetime import datetime, timezone
from typing import List

import pandas as pd


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


def forward_filled_fx_rate(
    ecb_rates: pd.DataFrame, fiat_currency: str, end_date: str | datetime
) -> pd.DataFrame:
    """Return a gap-free daily USD->`fiat_currency` FX series up to `end_date`.

    The ECB does not publish FX rates on weekends / holidays (and not before
    ~16:00 CET on the current day). Re-index the published rate over a
    continuous daily range and forward-fill, so a day without a fresh rate
    inherits the most recent known one -- even when that anchoring rate lies
    *before* the current import window (e.g. a Monday update whose window is
    only Sat+Sun, where a within-window ffill has nothing to fill from).

    Returns columns ``["date", "fx_rate"]`` with ``date`` as ``"%Y-%m-%d"``
    strings.
    """
    end_dt = as_utc_datetime(end_date)
    fx = ecb_rates[["date", fiat_currency]].rename(columns={fiat_currency: "fx_rate"})
    fx["date"] = pd.to_datetime(fx["date"])
    fx = fx.sort_values("date").set_index("date")
    full_index = pd.date_range(fx.index.min(), end_dt.date())
    fx = fx.reindex(full_index).ffill()
    fx.index = fx.index.strftime("%Y-%m-%d")
    return fx.rename_axis("date").reset_index()


def convert_to_fiat(value: int, rates: List[int]) -> List[int]:
    # col(valueColumn) * x / 1e6 + 0.5).cast(LongType) / 100.0
    return [int(value * r / 1e6 + 0.5) / 100 for r in rates]
