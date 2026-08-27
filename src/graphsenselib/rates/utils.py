import io
from datetime import datetime, timezone
from typing import List

import pandas as pd
import requests


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


# (connect, read) timeout for every exchange-rates HTTP call.
#
# Without a read timeout `requests` blocks in recv() forever when a connection
# is established and then silently dropped (NAT/firewall/LB reaping an idle
# flow) -- there is no OS-level timer that ever wakes it, since requests does
# not enable SO_KEEPALIVE. That stranded an ingest process indefinitely: this
# step runs *before* the ingest lock is acquired, so a wedged run holds nothing,
# blocks nothing and is invisible to the lock's stale-holder alerting -- the
# next cron tick just starts another one and the corpses pile up.
#
# The read timeout is per socket read, not per request, so it bounds "the peer
# went away", not "the response is legitimately slow". That is exactly the
# failure being fixed; combined with the retrying adapter below a transient
# stall is retried and a persistent one fails in bounded time.
HTTP_TIMEOUT = (5, 60)

# Retries connect *and* read timeouts (GET is idempotent), so HTTP_TIMEOUT
# ending a stalled read means one more attempt, not an immediate failure.
_HTTP_MAX_RETRIES = 5


def rates_session() -> requests.Session:
    """A `requests.Session` with the retry policy the rates fetchers share.

    Always pair its requests with `timeout=HTTP_TIMEOUT`: the retry count alone
    does not bound a read that never returns.
    """
    session = requests.Session()
    session.mount(
        "https://", requests.adapters.HTTPAdapter(max_retries=_HTTP_MAX_RETRIES)
    )
    return session


def read_csv_url(url: str, **kwargs) -> pd.DataFrame:
    """`pd.read_csv(url)` with a timeout.

    pandas downloads via `urllib.request.urlopen` without a timeout, so the
    socket default (None) applies and a dead connection hangs forever. Fetch the
    bytes ourselves, then hand pandas a buffer.

    Compression must be passed explicitly by the caller: pandas infers it from a
    URL's suffix but cannot infer it from an unnamed buffer.
    """
    response = rates_session().get(url, timeout=HTTP_TIMEOUT)
    response.raise_for_status()
    return pd.read_csv(io.BytesIO(response.content), **kwargs)
