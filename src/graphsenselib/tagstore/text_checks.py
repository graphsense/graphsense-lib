"""Find tagstore text that tagpack validation would now reject.

Read-only. Mis-encoded text (UTF-8 read as Latin-1/cp1252) and non-printable
control characters in tag, actor and pack fields, grouped by the pack that
holds them, so they can be fixed at the source rather than in the database.
The rules are the validator's (``tagpack.tagpack.text_problem``).
"""

import unicodedata
from typing import Dict, List, Optional

from sqlalchemy import bindparam
from sqlmodel import text

from graphsenselib.tagpack.tagpack import repair_mojibake, text_problem

from .db.queries import TagstoreDbAsync
from .ranking_conflicts import _rows

# Anything outside printable ASCII, tab and line breaks: the only strings
# that can fail the check, so the database narrows the scan to those.
_SUSPECT = r"[^\x09\x0a\x0d\x20-\x7e]"

_TAG_SQL = f"""
    SELECT tp.uri, f.field, f.value, count(*), min(t.network), min(t.identifier)
    FROM tag t
    JOIN tagpack tp ON tp.id = t.tagpack
    CROSS JOIN LATERAL (
        VALUES ('label', t.label), ('source', t.source), ('context', t.context)
    ) AS f(field, value)
    WHERE f.value ~ '{_SUSPECT}'
      AND (CAST(:network AS text) IS NULL OR t.network = :network)
    GROUP BY tp.uri, f.field, f.value
"""

_TAGPACK_SQL = f"""
    SELECT tp.uri, f.field, f.value, 1, NULL, NULL
    FROM tagpack tp
    CROSS JOIN LATERAL (
        VALUES ('title', tp.title), ('description', tp.description),
               ('creator', tp.creator)
    ) AS f(field, value)
    WHERE f.value ~ '{_SUSPECT}'
"""

_ACTOR_SQL = f"""
    SELECT ap.uri, f.field, f.value, 1, NULL, a.id
    FROM actor a
    JOIN actorpack ap ON ap.id = a.actorpack
    CROSS JOIN LATERAL (
        VALUES ('label', a.label), ('context', a.context)
    ) AS f(field, value)
    WHERE f.value ~ '{_SUSPECT}'
"""

_ACTORPACK_SQL = f"""
    SELECT ap.uri, f.field, f.value, 1, NULL, NULL
    FROM actorpack ap
    CROSS JOIN LATERAL (
        VALUES ('title', ap.title), ('description', ap.description),
               ('creator', ap.creator)
    ) AS f(field, value)
    WHERE f.value ~ '{_SUSPECT}'
"""


def _visible(value: str) -> str:
    """``value`` with control characters written as \\xNN, so the CSV shows
    them instead of carrying them."""
    return "".join(
        f"\\x{ord(c):02x}"
        if unicodedata.category(c) == "Cc" and c not in "\t\n\r"
        else c
        for c in value
    )


BAD_TEXT_COLUMNS = [
    "kind",
    "pack_uri",
    "field",
    "problem",
    "value",
    "suggested_fix",
    "n_tags",
    "example_network",
    "example_subject",
]


async def find_bad_text(
    db: TagstoreDbAsync, network: Optional[str] = None
) -> List[Dict]:
    """Rows for every distinct (pack, field, value) that fails the check.

    ``network`` limits the tag scan; pack headers and actors are always
    scanned. ``example_subject`` is an address for tags, the actor id for
    actors.
    """
    out = []
    for kind, sql in (
        ("tag", _TAG_SQL),
        ("tagpack", _TAGPACK_SQL),
        ("actor", _ACTOR_SQL),
        ("actorpack", _ACTORPACK_SQL),
    ):
        stmt = text(sql)
        if ":network" in sql:
            stmt = stmt.bindparams(
                bindparam("network", value=network.upper() if network else None),
            )
        for uri, field, value, n, ex_net, ex_subject in await _rows(db, stmt):
            problem = text_problem(value)
            if problem is None:
                continue
            out.append(
                {
                    "kind": kind,
                    "pack_uri": uri,
                    "field": field,
                    "problem": "mis-encoded"
                    if repair_mojibake(value) is not None
                    else "control characters",
                    "value": _visible(value),
                    "suggested_fix": repair_mojibake(value),
                    "n_tags": n if kind == "tag" else None,
                    "example_network": ex_net,
                    "example_subject": ex_subject,
                }
            )
    out.sort(key=lambda r: (r["pack_uri"] or "", r["kind"], r["field"], r["value"]))
    return out
