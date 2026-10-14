"""Address notifications: the contract of the /notifications/* routes.

The routes forward to the gs-notifications service (routes/notifications.py),
which owns the data and the rules; these models are its API contract.
gs-notifications keeps an identical copy (its ``api/models.py``); change
both together. Only the standard library and pydantic, and Python 3.10
compatible, hence ``(str, Enum)`` rather than ``StrEnum``.
"""

from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Literal
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class Frequency(str, Enum):
    instant = "instant"
    hourly = "hourly"
    daily = "daily"


class Channel(str, Enum):
    dashboard = "dashboard"
    email = "email"


class Direction(str, Enum):
    incoming = "in"
    outgoing = "out"


class PausedReason(str, Enum):
    no_access = "no_access"
    user = "user"


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")


def _known_timezone(v: str | None) -> str | None:
    if v is not None:
        try:
            ZoneInfo(v)
        except (ZoneInfoNotFoundError, ValueError) as e:
            raise ValueError(f"unknown timezone {v!r}") from e
    return v


# --- filters ---------------------------------------------------------------


class ValueUnit(str, Enum):
    eur = "eur"
    usd = "usd"
    native = "native"


class MinValue(_Strict):
    amount: Decimal = Field(ge=0, allow_inf_nan=False)
    unit: ValueUnit


class Filters(_Strict):
    """Which transactions notify. Versioned so new options don't need a core
    release (docs/DESIGN.md §6)."""

    v: Literal[1] = 1
    direction: Literal["in", "out", "both"] = "both"
    min_value: MinValue | None = None
    # Token tickers on account networks; None = everything.
    tokens: list[str] | None = Field(default=None, max_length=50)

    @field_validator("tokens")
    @classmethod
    def _normalize_tokens(cls, v: list[str] | None) -> list[str] | None:
        return (
            None if v is None else sorted({t.strip().lower() for t in v if t.strip()})
        )


# --- subscriptions ---------------------------------------------------------


class SubscriptionCreate(_Strict):
    network: str = Field(min_length=1, max_length=32)
    address: str = Field(min_length=1, max_length=256)
    # Defaults to the user's default_frequency.
    frequency: Frequency | None = None
    filters: Filters = Filters()
    label: str | None = Field(default=None, max_length=200)
    # The browser's IANA zone (Intl.DateTimeFormat().resolvedOptions().timeZone).
    # When given it becomes the user's timezone: times in mails, the hour
    # daily digests go out. Unset keeps the current one (initially UTC).
    timezone: str | None = Field(default=None, max_length=64)

    _check_timezone = field_validator("timezone")(_known_timezone)


class SubscriptionUpdate(_Strict):
    """Only the fields sent are changed; ``label: null`` clears the label."""

    frequency: Frequency | None = None
    filters: Filters | None = None
    label: str | None = Field(default=None, max_length=200)
    # true pauses; false resumes (stays paused with no_access while the
    # network is not allowed).
    paused: bool | None = None


class Subscription(BaseModel):
    id: int
    network: str
    # Normalized form, as the GraphSense API uses it.
    address: str
    frequency: Frequency
    filters: Filters
    label: str | None
    # None = running.
    paused_reason: PausedReason | None
    created_at: datetime
    # Polling of the address (shared by everyone watching it). None until the
    # first check; check_errors counts failed checks in a row, retried with
    # backoff until next_check_at. Defaults keep older services valid.
    last_checked_at: datetime | None = None
    next_check_at: datetime | None = None
    check_errors: int = 0


class SubscriptionPage(BaseModel):
    items: list[Subscription]
    # Pass as ?cursor= for the next page; None on the last page.
    next_cursor: str | None


# --- feed ------------------------------------------------------------------


class FeedItem(BaseModel):
    """One transaction on a watched address, as shown in the dashboard."""

    id: int
    subscription_id: int
    # The subscription's label, if the user set one.
    label: str | None
    network: str
    address: str
    tx_hash: str
    height: int
    timestamp: datetime
    direction: Direction
    # Lowercase ticker; differs from network for token transfers.
    currency: str
    token: bool
    # Base units, as a string (may exceed JSON number precision).
    value: str
    # Whole coins; None when the token's decimals are unknown.
    amount: str | None
    # Fiat values at the transaction's height, e.g. {"eur": 1.5, "usd": 1.7}.
    fiat: dict[str, float]
    # Account networks: the other side of the transfer.
    counterparty: str | None
    read: bool


class FeedPage(BaseModel):
    items: list[FeedItem]
    # Pass as ?cursor= for the next (older) page; None on the last page.
    next_cursor: str | None


class FeedReadRequest(_Strict):
    """Mark feed items read: the listed ``ids``, or everything up to and
    including ``up_to`` (an item id). Exactly one of the two."""

    ids: list[int] | None = Field(default=None, min_length=1, max_length=500)
    up_to: int | None = None

    @model_validator(mode="after")
    def _exactly_one(self) -> "FeedReadRequest":
        if (self.ids is None) == (self.up_to is None):
            raise ValueError("give either ids or up_to")
        return self


class FeedReadResult(BaseModel):
    # Items that were unread and are now read.
    marked: int


class UnreadCount(BaseModel):
    # Counting stops at 1000; capped=true means "at least that many".
    count: int
    capped: bool


# --- settings --------------------------------------------------------------


class UserSettings(BaseModel):
    email_enabled: bool
    default_frequency: Frequency
    # IANA name; daily digests go out at local time.
    timezone: str


class UserSettingsUpdate(_Strict):
    email_enabled: bool | None = None
    default_frequency: Frequency | None = None
    timezone: str | None = None

    _check_timezone = field_validator("timezone")(_known_timezone)
