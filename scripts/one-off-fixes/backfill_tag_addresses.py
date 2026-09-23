#!/usr/bin/env python
# /// script
# requires-python = ">=3.9"
# dependencies = ["psycopg2-binary>=2.9.3"]
# ///
# ruff: noqa: T201
"""Add missing ``address`` rows for address tags, so they get cluster mappings.

Why this exists
---------------
The cluster-mapping job (``graphsense-cli tagstore insert-cluster-mappings``)
maps only the (network, address) pairs listed in the ``address`` table. The
tagpack importer writes that row alongside every tag, but user-reported tags
(``add_user_reported_tag``, the dashboard's "report a tag") only wrote the
``tag`` row until that was fixed, so those addresses never got a cluster
mapping. This script adds the missing rows for every address tag, whatever
wrote it; new rows start with ``is_mapped = false``, which is exactly what the
next (non ``--update``) cluster-mapping run picks up.

It only inserts (``ON CONFLICT DO NOTHING``) and is safe to run repeatedly.
``apply`` records the rows it inserted in ``addrfix_address_added`` so
``rollback`` can remove exactly those.

After ``apply``, run the cluster mapping and refresh the views:

    graphsense-cli tagstore insert-cluster-mappings -u <conn> \\
        --use-gs-lib-config-env <env>
    graphsense-cli tagstore refresh-views -u <conn>

Usage
-----
The connection string is read from ``TAGSTORE_DB_URL`` (override with
``--env-var``), taken from the environment or from ``--env-file`` (default
``.env``), e.g. ``TAGSTORE_DB_URL=postgresql://user:pass@host:5432/tagstore``.
A SQLAlchemy-style ``postgresql+psycopg2://`` prefix is accepted too.

    uv run scripts/one-off-fixes/backfill_tag_addresses.py check
    uv run scripts/one-off-fixes/backfill_tag_addresses.py dry-run
    uv run scripts/one-off-fixes/backfill_tag_addresses.py apply
    uv run scripts/one-off-fixes/backfill_tag_addresses.py rollback [--dry-run]
    uv run scripts/one-off-fixes/backfill_tag_addresses.py cleanup
"""

import argparse
import os
import re
import sys
from pathlib import Path

import psycopg2

ADDED = "addrfix_address_added"

MISSING = """
    FROM tag t
    WHERE t.tag_subject = 'address'
      AND NOT EXISTS (SELECT 1 FROM address a
                      WHERE a.network = t.network AND a.address = t.identifier)
"""


class Abort(Exception):
    pass


def load_env_file(path):
    """Minimal .env reader: KEY=VALUE lines, optional quotes and `export`."""
    values = {}
    for line in Path(path).read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip().removeprefix("export ").strip()
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "'\"":
            value = value[1:-1]
        values[key] = value
    return values


def get_dsn(args):
    dsn = os.environ.get(args.env_var)
    if not dsn and Path(args.env_file).is_file():
        dsn = load_env_file(args.env_file).get(args.env_var)
    if not dsn:
        raise Abort(
            f"{args.env_var} is neither set in the environment nor in {args.env_file}"
        )
    # postgresql+psycopg2:// / postgresql+asyncpg:// -> postgresql://
    return re.sub(r"^(postgres(?:ql)?)\+\w+://", r"\1://", dsn)


def connect(args):
    conn = psycopg2.connect(get_dsn(args))
    info = conn.info
    print(f"connected to {info.dbname} on {info.host}:{info.port} as {info.user}")
    return conn


def scalar(cur, query, params=None):
    cur.execute(query, params)
    return cur.fetchone()[0]


def table_exists(cur, name):
    return scalar(cur, "SELECT to_regclass(%s)", (name,)) is not None


def confirm(prompt, assume_yes):
    if assume_yes:
        return True
    return input(f"{prompt} Type COMMIT to proceed: ").strip() == "COMMIT"


def report(cur):
    tags = scalar(cur, f"SELECT count(*) {MISSING}")
    pairs = scalar(
        cur,
        f"SELECT count(*) FROM (SELECT DISTINCT t.network, t.identifier {MISSING}) x",
    )
    print(f"  tags_without_address       {tags}")
    print(f"  address_rows_to_add        {pairs}")
    cur.execute(
        f"""SELECT t.tagpack, t.network, count(*) {MISSING}
            GROUP BY t.tagpack, t.network ORDER BY 3 DESC, 1, 2 LIMIT 15"""
    )
    rows = cur.fetchall()
    if rows:
        print("  by tagpack / network (top 15):")
        for tagpack, network, n in rows:
            print(f"    {n:>7}  {network:<6} {tagpack}")
    return tags, pairs


def cmd_check(conn, args):
    conn.set_session(readonly=True)
    with conn.cursor() as cur:
        print("current state:")
        _, pairs = report(cur)
        if table_exists(cur, ADDED):
            print(f"{ADDED} present from an earlier apply")
    print("nothing to do" if not pairs else "run dry-run, then apply")
    return 0


def run_backfill(conn, args, commit):
    with conn.cursor() as cur:
        if table_exists(cur, ADDED):
            raise Abort(
                f"{ADDED} exists from an earlier apply; `rollback` or `cleanup`"
            )

        print("before:")
        _, pairs = report(cur)
        if not pairs:
            print("nothing to do")
            return 0

        cur.execute(f"CREATE TABLE {ADDED} (network text, address text)")
        cur.execute(
            f"""WITH ins AS (
                    INSERT INTO address (network, address, is_mapped)
                    SELECT DISTINCT t.network, t.identifier, false {MISSING}
                    ON CONFLICT (network, address) DO NOTHING
                    RETURNING network, address)
                INSERT INTO {ADDED} SELECT network, address FROM ins"""
        )
        added = cur.rowcount
        print(f"changes: {added} address rows added")

        print("after:")
        tags_after, _ = report(cur)
        # A concurrent writer may have added some of the rows first
        # (ON CONFLICT), so fewer is fine; none may be left missing.
        if tags_after or added > pairs:
            conn.rollback()
            raise Abort(
                f"verification failed ({tags_after} tags still without an "
                f"address row, {added} added for {pairs} expected); rolled back"
            )
        print("verification passed")

        if not commit:
            conn.rollback()
            print("dry run: rolled back, nothing changed")
            return 0
        if not confirm("Commit these changes?", args.yes):
            conn.rollback()
            print("rolled back, nothing changed")
            return 1
        conn.commit()
        print(f"committed; inserted rows recorded in {ADDED}")
    print(
        "next: graphsense-cli tagstore insert-cluster-mappings "
        "(without --update), then graphsense-cli tagstore refresh-views"
    )
    return 0


def cmd_dry_run(conn, args):
    return run_backfill(conn, args, commit=False)


def cmd_apply(conn, args):
    return run_backfill(conn, args, commit=True)


def cmd_rollback(conn, args):
    with conn.cursor() as cur:
        if not table_exists(cur, ADDED):
            raise Abort(f"{ADDED} missing; nothing to undo")
        mapped = scalar(
            cur,
            f"""SELECT count(*) FROM address a JOIN {ADDED} x
                ON a.network = x.network AND a.address = x.address
                WHERE a.is_mapped""",
        )
        cur.execute(
            f"""DELETE FROM address a USING {ADDED} x
                WHERE a.network = x.network AND a.address = x.address"""
        )
        print(f"changes: {cur.rowcount} added address rows removed")
        if mapped:
            print(
                f"  note: {mapped} of them were mapped already; their "
                "address_cluster_mapping rows stay (harmless, unused)"
            )

        if args.dry_run:
            conn.rollback()
            print("dry run: rolled back, nothing changed")
            return 0
        if not confirm("Remove these rows?", args.yes):
            conn.rollback()
            print("rolled back, nothing changed")
            return 1
        conn.commit()
        print(f"removed; {ADDED} kept (run `cleanup` to drop it)")
    return 0


def cmd_cleanup(conn, args):
    with conn.cursor() as cur:
        if not table_exists(cur, ADDED):
            print("nothing to drop")
            return 0
        if not confirm(f"Drop {ADDED}? `rollback` is impossible afterwards.", args.yes):
            conn.rollback()
            return 1
        cur.execute(f"DROP TABLE {ADDED}")
    conn.commit()
    print(f"dropped {ADDED}")
    return 0


def parse_args(argv):
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--env-file", default=".env")
    parser.add_argument("--env-var", default="TAGSTORE_DB_URL")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("check", help="read-only report")
    sub.add_parser("dry-run", help="insert and verify, then roll back")
    p = sub.add_parser("apply", help="insert, commit after confirmation")
    p.add_argument("--yes", action="store_true", help="do not ask before COMMIT")
    p = sub.add_parser("rollback", help="remove the rows apply inserted")
    p.add_argument("--yes", action="store_true")
    p.add_argument("--dry-run", action="store_true")
    p = sub.add_parser("cleanup", help=f"drop {ADDED}")
    p.add_argument("--yes", action="store_true")
    return parser.parse_args(argv)


COMMANDS = {
    "check": cmd_check,
    "dry-run": cmd_dry_run,
    "apply": cmd_apply,
    "rollback": cmd_rollback,
    "cleanup": cmd_cleanup,
}


def main(argv=None):
    args = parse_args(argv)
    try:
        conn = connect(args)
    except Abort as e:
        print(f"error: {e}", file=sys.stderr)
        return 2
    try:
        return COMMANDS[args.command](conn, args)
    except Abort as e:
        conn.rollback()
        print(f"error: {e}", file=sys.stderr)
        return 1
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
