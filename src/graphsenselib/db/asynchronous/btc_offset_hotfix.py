"""BTC tx_id offset hotfix for the REST data layer.

Background:
  btc_raw_20260429 was built with corrupt tx_ids: blocks 468,000-469,999
  contain duplicate batch entries, and every block from 470,000 to chain
  tip has tx_ids shifted by exactly +3,804,158 vs the canonical (old)
  btc_raw keyspace. The transformed keyspace btc_transformed_20251002 was
  bulk-built against canonical btc_raw and then delta-updated against
  btc_raw_20260429, so it now contains a mix of canonical refs (older
  entries) and shifted refs (post-switchover entries).

Empirical fact (verified by tx_hash classification on 200 samples):
  - All transformed-KS refs with tx_id < 233,295,705 are canonical and
    resolve directly in btc_raw.
  - All transformed-KS refs with tx_id >= 233,295,705 are shifted; the
    canonical tx_id is (tx_id - 3,804,158).
  - There are no canonical-frozen-large refs and no transformed refs
    pointing into the dupe-stretch phantom range.

This hotfix:
  - Activates when raw_keyspace == "btc_raw" (i.e. the canonical raw is in
    use) and currency == "btc". Set GS_BTC_TX_ID_HOTFIX_DISABLE=1 to
    force-disable.
  - Translates tx_id-bearing fields read from the transformed KS from the
    shifted space into the canonical space.
  - Translates named tx_id parameters going INTO transformed-KS queries
    (e.g. %(tx_id_lower_bound)s) from canonical back into shifted space so
    range filters and pagination tokens still match the underlying storage.

Drop this module and the call sites in cassandra.py once the Spark reingest
finishes and a fresh transformed keyspace is in place.
"""

from __future__ import annotations

import os
from typing import Any, Iterable, Optional

BTC_TX_ID_OFFSET = 3_804_158
# Floor = canonical_max(btc_raw) + 1.2M slack so canonical chain growth
# (~700k tx/day) has ~1.5 days of headroom before any real canonical tx_id
# could cross the floor and be misclassified as shifted. Ratchet up (or
# remove this hotfix) once the raw KS is reingested.
# canonical_max(btc_raw) measured 2026-05-06 = 1,351,656,364
BTC_SHIFTED_FLOOR = 1_352_856_364
BTC_CANONICAL_FLOOR = BTC_SHIFTED_FLOOR - BTC_TX_ID_OFFSET  # 1,349,052,206

# Per-row tx_id is no longer translated blindly outbound — for
# `address_transactions.tx_id` (and similar) the storage is a mix of canonical
# and shifted values, so a blanket subtract corrupts every canonical-written
# entry. Disambiguation happens at the call site that has the queried address
# context (see list_txs_by_node_type and finish_address). first_tx_id /
# last_tx_id keep the blanket translation because they are reliably shifted
# whenever ≥ SHIFTED_FLOOR (verified 200/200), with the rare canonical-frozen
# cases handled by the same call-site disambiguation.
_TX_ID_OUTBOUND_FIELDS = ("first_tx_id", "last_tx_id")
_TX_ID_INBOUND_KEYS = ("tx_id_lower_bound", "tx_id_upper_bound", "tx_id")


def is_disabled() -> bool:
    return os.environ.get("GS_BTC_TX_ID_HOTFIX_DISABLE", "0") == "1"


def is_active(currency: Optional[str], raw_keyspace: Optional[str]) -> bool:
    if is_disabled():
        return False
    return currency == "btc" and raw_keyspace == "btc_raw"


def shifted_to_canonical(tx_id):
    if tx_id is None:
        return None
    if tx_id >= BTC_SHIFTED_FLOOR:
        return tx_id - BTC_TX_ID_OFFSET
    return tx_id


def canonical_to_shifted(tx_id):
    if tx_id is None:
        return None
    if tx_id >= BTC_CANONICAL_FLOOR:
        return tx_id + BTC_TX_ID_OFFSET
    return tx_id


def translate_row_outbound(row) -> None:
    """Mutate a dict-like row in place: shifted -> canonical for tx_id fields."""
    if not isinstance(row, dict):
        return
    for f in _TX_ID_OUTBOUND_FIELDS:
        v = row.get(f)
        if v is not None:
            row[f] = shifted_to_canonical(v)


def translate_rows_outbound(rows: Iterable) -> None:
    if rows is None:
        return
    for row in rows:
        translate_row_outbound(row)


def translate_params_inbound(params) -> Any:
    """Return a copy of params with canonical -> shifted translation applied
    to known tx_id-bearing keys.

    Operates on dicts (named params). Positional params are left alone —
    site-specific patches handle those if needed. Returns a new dict so the
    caller's original params object is never mutated (canonical_to_shifted is
    non-idempotent, so reusing a dict could double-shift).
    """
    if not isinstance(params, dict):
        return params
    out = dict(params)
    for k in _TX_ID_INBOUND_KEYS:
        if k in out and out[k] is not None:
            out[k] = canonical_to_shifted(out[k])
    return out
