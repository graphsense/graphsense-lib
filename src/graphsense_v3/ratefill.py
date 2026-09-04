"""Carrying the last exchange rate forward over a keyspace's unrated tail.

The same thing :func:`graphsense_v3.spark.job.forward_fill_rates` does during a
backfill, applied to a keyspace that has already been written -- so an existing
keyspace can be made servable without a re-run.

Why it is needed at all: exchange rates land a day at a time, so a backfill that
reaches the chain tip always ends with a few hundred blocks that have no rate
row. The REST rates service raises ``BlockNotFoundException`` for such a block
rather than degrading, and every call that asks for CURRENT rates resolves the
height to ``no_blocks - 1`` -- the tip. One missing rate row therefore takes out
``get_address`` and every neighbour listing for the whole keyspace.

**Writes only to a v3 derived keyspace**, and only to ``exchange_rates``. The
keyspace name is checked against the v3 pattern first, which no v2 name matches.
"""

from __future__ import annotations

import logging
from typing import Any, Optional

from graphsense_v3.settings import RATE_FORWARD_FILL_SECONDS, assert_v3_keyspace

logger = logging.getLogger(__name__)


def last_rated(
    session: Any, derived: str, asset: str, tip: int, size: int
) -> Optional[tuple]:
    """``(block_id, fiat_values)`` for the highest rated block at or below
    ``tip``, walking down one rate partition at a time."""
    block_id = tip
    group = block_id // size
    # A day of blocks is well under this on every chain we run.
    for _ in range(500):
        if group < 0:
            return None
        rows = list(
            session.execute(
                f"SELECT block_id, fiat_values FROM {derived}.exchange_rates "
                f"WHERE asset = %s AND block_id_group = %s AND block_id <= %s "
                f"ORDER BY block_id DESC LIMIT 1",
                (asset, group, block_id),
            )
        )
        if rows:
            return int(rows[0].block_id), dict(rows[0].fiat_values or {})
        group -= 1
        block_id = (group + 1) * size - 1
    return None


def block_timestamps(session: Any, raw: str, low: int, high: int, size: int) -> dict:
    """``{block_id: timestamp}`` for a block range, one partition at a time."""
    found: dict = {}
    for group in range(low // size, high // size + 1):
        for row in session.execute(
            f"SELECT block_id, timestamp FROM {raw}.block "
            f"WHERE block_id_group = %s AND block_id >= %s AND block_id <= %s",
            (group, low, high),
        ):
            found[int(row.block_id)] = int(row.timestamp)
    return found


def zero_fill(
    session: Any,
    raw: str,
    derived: str,
    asset: str,
    *,
    size: int,
    dry_run: bool = False,
) -> dict:
    """A zero rate for every block that has none, matching v2.

    v2 materialises a row for EVERY block, carrying ``[0, 0]`` where the feed
    has nothing -- its rate source starts years after genesis, so the early
    chain is zero-rated there and simply absent here. An absent row is a failed
    request, not a missing number.

    Partition by partition, inserting only what is MISSING. Blanket-inserting
    over the range would overwrite real rates with zeros, which is a far worse
    outcome than the gap being repaired.
    """
    from cassandra.concurrent import execute_concurrent_with_args

    assert_v3_keyspace(derived)
    assert_v3_keyspace(raw)

    rows = list(session.execute(f"SELECT highest_block FROM {raw}.summary_statistics"))
    if not rows or rows[0].highest_block is None:
        raise LookupError(f"{raw} has no summary_statistics row to take a tip from")
    tip = int(rows[0].highest_block)

    found = last_rated(session, derived, asset, tip, size)
    if found is None:
        raise LookupError(
            f"{derived} has no exchange rate for {asset} anywhere, so there are "
            "no fiat currencies to write a zero for"
        )
    zero = {code: 0.0 for code in found[1]}

    insert = session.prepare(
        f"INSERT INTO {derived}.exchange_rates "
        f"(asset, block_id_group, block_id, fiat_values) VALUES (?, ?, ?, ?)"
    )
    written = 0
    for group in range(0, tip // size + 1):
        present = {
            int(row.block_id)
            for row in session.execute(
                f"SELECT block_id FROM {derived}.exchange_rates "
                f"WHERE asset = %s AND block_id_group = %s",
                (asset, group),
            )
        }
        missing = [
            block_id
            for block_id in range(group * size, min((group + 1) * size, tip + 1))
            if block_id not in present
        ]
        if not missing:
            continue
        if not dry_run:
            execute_concurrent_with_args(
                session,
                insert,
                [(asset, group, block_id, zero) for block_id in missing],
                concurrency=64,
            )
        written += len(missing)
    return {"asset": asset, "tip": tip, "written": written, "zero": zero}


def fill(
    session: Any,
    raw: str,
    derived: str,
    asset: str,
    *,
    size: int,
    within_seconds: int = RATE_FORWARD_FILL_SECONDS,
    dry_run: bool = False,
) -> dict:
    """Fill ``derived.exchange_rates`` up to the keyspace's block tip.

    Returns a summary of what was (or would be) written. Blocks further than
    ``within_seconds`` of block time past the last real rate are left alone --
    beyond that the rate feed is broken rather than lagging, and a stale rate
    carried indefinitely would hide that behind plausible numbers.
    """
    assert_v3_keyspace(derived)
    assert_v3_keyspace(raw)

    rows = list(session.execute(f"SELECT highest_block FROM {raw}.summary_statistics"))
    if not rows or rows[0].highest_block is None:
        raise LookupError(f"{raw} has no summary_statistics row to take a tip from")
    tip = int(rows[0].highest_block)

    found = last_rated(session, derived, asset, tip, size)
    if found is None:
        raise LookupError(
            f"{derived} has no exchange rate for {asset} at or below block {tip}; "
            "there is nothing to carry forward"
        )
    rated, fiat_values = found
    if rated >= tip:
        return {"asset": asset, "tip": tip, "rated": rated, "written": 0, "skipped": 0}

    timestamps = block_timestamps(session, raw, rated, tip, size)
    cutoff = timestamps.get(rated)
    written, skipped = 0, 0
    for block_id in range(rated + 1, tip + 1):
        stamp = timestamps.get(block_id)
        if stamp is None:
            # A block the range names but `block` does not hold. Filling a rate
            # for it would invent coverage for a block that is not there.
            skipped += 1
            continue
        if cutoff is not None and stamp > cutoff + within_seconds:
            skipped += 1
            continue
        if not dry_run:
            session.execute(
                f"INSERT INTO {derived}.exchange_rates "
                f"(asset, block_id_group, block_id, fiat_values) VALUES (%s, %s, %s, %s)",
                (asset, block_id // size, block_id, fiat_values),
            )
        written += 1
    return {
        "asset": asset,
        "tip": tip,
        "rated": rated,
        "written": written,
        "skipped": skipped,
        "fiat_values": fiat_values,
    }
