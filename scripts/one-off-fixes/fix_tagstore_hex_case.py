#!/usr/bin/env python
# /// script
# requires-python = ">=3.9"
# dependencies = ["psycopg2-binary>=2.9.3"]
# ///
# ruff: noqa: T201
"""Lowercase mixed-case 0x hex addresses already stored in a tagstore.

Why this exists
---------------
Before the fix in ``tagpack.utils.normalize_tag_address`` only tags with
``network == ETH`` had their address lowercased on insert. A token tag without
an explicit network (``currency: USDT``) got the currency as its network, so
its checksummed address was stored verbatim and never matched the exact,
lowercase lookups of the REST API. The importer is fixed; this script repairs
the rows written before the fix. Deploy the fixed importer first, otherwise
new imports write mixed-case rows again.

What it touches
---------------
* ``tag``: address tags (``tag_subject = 'address'``) whose identifier matches
  ``^0x[0-9a-fA-F]+$`` and is not lowercase get ``identifier = lower(...)``.
  Transaction hashes are left alone, as the importer leaves them alone.
* ``address``: the (network, address) set the cluster mapping works from. A
  lowercase row is added for every mixed-case spelling (``is_mapped = false``,
  so the next cluster-mapping run picks it up) unless one already exists, then
  the mixed-case rows are deleted.
* Afterwards the materialized views are refreshed and the quality tables are
  recomputed (skip with ``--skip-refresh``).

Everything in ``apply`` runs in one transaction, which also writes the backup
tables used by ``rollback``:

* ``hexfix_tag_backup``      -- the tag rows as they were
* ``hexfix_address_backup``  -- the mixed-case address rows as they were
* ``hexfix_address_added``   -- the lowercase address rows this run inserted

During a run, writes to ``tag`` and ``address`` are blocked (readers are not),
so a concurrent tagpack import waits instead of racing the fix. If an import
is already holding the tables, the script gives up after ``--lock-timeout``.

Usage
-----
The connection string is read from ``TAGSTORE_DB_URL`` (override with
``--env-var``), taken from the environment or from ``--env-file`` (default
``.env``), e.g. ``TAGSTORE_DB_URL=postgresql://user:pass@host:5432/tagstore``.
A SQLAlchemy-style ``postgresql+psycopg2://`` prefix is accepted too.

    uv run scripts/one-off-fixes/fix_tagstore_hex_case.py check      # read-only report
    uv run scripts/one-off-fixes/fix_tagstore_hex_case.py dry-run    # full run, then ROLLBACK
    uv run scripts/one-off-fixes/fix_tagstore_hex_case.py apply      # asks before COMMIT
    uv run scripts/one-off-fixes/fix_tagstore_hex_case.py rollback [--dry-run]
    uv run scripts/one-off-fixes/fix_tagstore_hex_case.py cleanup    # drop backup tables
"""

import argparse
import os
import re
import sys
from pathlib import Path

import psycopg2

HEX = r"'^0x[0-9a-fA-F]+$'"

TAG_BACKUP = "hexfix_tag_backup"
ADDRESS_BACKUP = "hexfix_address_backup"
ADDRESS_ADDED = "hexfix_address_added"
BACKUP_TABLES = (TAG_BACKUP, ADDRESS_BACKUP, ADDRESS_ADDED)

TAG_AFFECTED = f"""
    tag_subject = 'address'
    AND identifier ~ {HEX}
    AND identifier <> lower(identifier)
"""
ADDRESS_AFFECTED = f"address ~ {HEX} AND address <> lower(address)"

# Mirrors TagStore.refresh_db; views missing on this store are skipped.
MATERIALIZED_VIEWS = (
    "statistics",
    "tag_count_by_cluster",
    "best_cluster_tag",
    "tag_count_by_cluster_v2",
    "best_cluster_tag_v2",
    "label_search",
)

# Unique key of tag is (identifier, network, label, tagpack, source). NULL
# sources never collide in Postgres, hence the plain `=` / IS NOT NULL.
TAG_CONFLICTS = f"""
WITH tag_fix AS (
    SELECT lower(identifier) AS new_id, network, label, tagpack, source
    FROM tag WHERE {TAG_AFFECTED}
)
SELECT count(*) FROM (
    SELECT 1 FROM tag_fix f
    JOIN tag t ON t.identifier = f.new_id AND t.network = f.network
              AND t.label = f.label AND t.tagpack = f.tagpack
              AND t.source = f.source
    UNION ALL
    SELECT 1 FROM tag_fix WHERE source IS NOT NULL
    GROUP BY new_id, network, label, tagpack, source HAVING count(*) > 1
) c
"""

TAGS_WITHOUT_ADDRESS = """
SELECT count(*) FROM tag t
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


def lock_for_writes(cur, lock_timeout):
    # SHARE ROW EXCLUSIVE blocks INSERT/UPDATE/DELETE from other sessions but
    # not SELECT, so the API keeps reading while imports wait.
    cur.execute("SET LOCAL lock_timeout = %s", (lock_timeout,))
    try:
        cur.execute("LOCK TABLE tag, address IN SHARE ROW EXCLUSIVE MODE")
    except psycopg2.errors.LockNotAvailable:
        raise Abort(
            f"could not lock tag/address within {lock_timeout}; is a tagpack "
            "import running? Pause it and retry."
        )


def report(cur):
    counts = {
        "tag_rows_to_fix": scalar(
            cur, f"SELECT count(*) FROM tag WHERE {TAG_AFFECTED}"
        ),
        "tag_conflicts": scalar(cur, TAG_CONFLICTS),
        "address_rows_to_fix": scalar(
            cur, f"SELECT count(*) FROM address WHERE {ADDRESS_AFFECTED}"
        ),
        "address_lowercase_twins": scalar(
            cur,
            f"""SELECT count(*) FROM address m
                WHERE {ADDRESS_AFFECTED.replace("address", "m.address")}
                  AND EXISTS (SELECT 1 FROM address a WHERE a.network = m.network
                              AND a.address = lower(m.address))""",
        ),
        "tags_without_address": scalar(cur, TAGS_WITHOUT_ADDRESS),
    }
    for mapping in ("address_cluster_mapping", "address_cluster_mapping_v2"):
        if table_exists(cur, mapping):
            counts[f"{mapping}_rows"] = scalar(
                cur, f"SELECT count(*) FROM {mapping} WHERE {ADDRESS_AFFECTED}"
            )
    width = max(len(k) for k in counts)
    for key, value in counts.items():
        print(f"  {key:<{width}}  {value}")
    return counts


def confirm(prompt, assume_yes):
    if assume_yes:
        return True
    return input(f"{prompt} Type COMMIT to proceed: ").strip() == "COMMIT"


def refresh(conn):
    conn.autocommit = True
    with conn.cursor() as cur:
        for mv in MATERIALIZED_VIEWS:
            if not table_exists(cur, mv):
                print(f"  skip {mv} (does not exist)")
                continue
            print(f"  refreshing {mv}")
            cur.execute(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {mv}")
        procs = ("calculate_quality(boolean)", "insert_address_quality()")
        if all(scalar(cur, "SELECT to_regprocedure(%s)", (p,)) for p in procs):
            print("  recomputing quality measures")
            cur.execute("CALL calculate_quality(FALSE)")
            cur.execute("CALL insert_address_quality()")
        else:
            print("  skip quality measures (procedures do not exist)")


def refresh_after_commit(conn, args):
    if args.skip_refresh:
        print("skipped refresh; run `graphsense-cli tagstore refresh-views` later")
        return 0
    try:
        refresh(conn)
    except psycopg2.Error as e:
        print(
            f"error: the fix is committed, but the refresh failed: {e}\n"
            "run `graphsense-cli tagstore refresh-views` and "
            "`CALL insert_address_quality();` by hand",
            file=sys.stderr,
        )
        return 1
    return 0


def cmd_check(conn, args):
    conn.set_session(readonly=True)
    with conn.cursor() as cur:
        print("current state:")
        counts = report(cur)
        existing = [t for t in BACKUP_TABLES if table_exists(cur, t)]
    if existing:
        print(f"backup tables present from an earlier apply: {', '.join(existing)}")
    if counts["tag_conflicts"]:
        print(
            "NOT SAFE: lowercasing would violate unique_tag; resolve those rows first"
        )
        return 1
    print("safe to apply" if counts["tag_rows_to_fix"] else "nothing to fix")
    return 0


def run_fix(conn, args, commit):
    with conn.cursor() as cur:
        lock_for_writes(cur, args.lock_timeout)

        existing = [t for t in BACKUP_TABLES if table_exists(cur, t)]
        if existing:
            raise Abort(
                f"backup tables from an earlier apply exist ({', '.join(existing)}); "
                "run `rollback` or `cleanup` first"
            )

        print("before:")
        before = report(cur)
        if before["tag_conflicts"]:
            raise Abort("lowercasing would violate unique_tag; see `check`")
        if not before["tag_rows_to_fix"] and not before["address_rows_to_fix"]:
            print("nothing to fix")
            return 0

        cur.execute(
            f"CREATE TABLE {TAG_BACKUP} AS SELECT * FROM tag WHERE {TAG_AFFECTED}"
        )
        cur.execute(
            f"CREATE TABLE {ADDRESS_BACKUP} AS "
            f"SELECT * FROM address WHERE {ADDRESS_AFFECTED}"
        )
        cur.execute(f"CREATE TABLE {ADDRESS_ADDED} (network text, address text)")

        cur.execute(
            f"UPDATE tag SET identifier = lower(identifier) WHERE {TAG_AFFECTED}"
        )
        tags_updated = cur.rowcount

        cur.execute(
            f"""WITH ins AS (
                    INSERT INTO address (network, address, created, is_mapped)
                    SELECT network, lower(address), min(created), false
                    FROM address WHERE {ADDRESS_AFFECTED}
                    GROUP BY network, lower(address)
                    ON CONFLICT (network, address) DO NOTHING
                    RETURNING network, address)
                INSERT INTO {ADDRESS_ADDED} SELECT network, address FROM ins"""
        )
        addresses_added = cur.rowcount

        cur.execute(f"DELETE FROM address WHERE {ADDRESS_AFFECTED}")
        addresses_deleted = cur.rowcount

        print(
            f"changes: {tags_updated} tags lowercased, "
            f"{addresses_added} lowercase addresses added, "
            f"{addresses_deleted} mixed-case addresses removed"
        )
        print("after:")
        after = report(cur)

        problems = []
        if tags_updated != before["tag_rows_to_fix"]:
            problems.append(
                f"updated {tags_updated} tags, expected {before['tag_rows_to_fix']}"
            )
        if addresses_deleted != before["address_rows_to_fix"]:
            problems.append(
                f"removed {addresses_deleted} addresses, "
                f"expected {before['address_rows_to_fix']}"
            )
        if after["tag_rows_to_fix"] or after["address_rows_to_fix"]:
            problems.append("mixed-case rows remain")
        if after["tags_without_address"] > before["tags_without_address"]:
            problems.append(
                "tags without an address row went from "
                f"{before['tags_without_address']} to {after['tags_without_address']}"
            )
        if problems:
            conn.rollback()
            raise Abort("verification failed, rolled back: " + "; ".join(problems))
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
        print(f"committed; backups kept in {', '.join(BACKUP_TABLES)}")

    return refresh_after_commit(conn, args)


def cmd_dry_run(conn, args):
    return run_fix(conn, args, commit=False)


def cmd_apply(conn, args):
    return run_fix(conn, args, commit=True)


def cmd_rollback(conn, args):
    with conn.cursor() as cur:
        lock_for_writes(cur, args.lock_timeout)
        missing = [t for t in BACKUP_TABLES if not table_exists(cur, t)]
        if missing:
            raise Abort(
                f"backup tables missing ({', '.join(missing)}); nothing to undo"
            )

        orphans_before = scalar(cur, TAGS_WITHOUT_ADDRESS)
        backed_up = scalar(cur, f"SELECT count(*) FROM {TAG_BACKUP}")

        # Only rows still carrying the value apply wrote; tags deleted or
        # reimported since are left as they are now.
        cur.execute(
            f"""UPDATE tag t SET identifier = b.identifier
                FROM {TAG_BACKUP} b
                WHERE t.id = b.id AND t.identifier = lower(b.identifier)"""
        )
        tags_restored = cur.rowcount

        cur.execute(
            f"""INSERT INTO address (network, address, created, is_mapped)
                SELECT network, address, created, is_mapped FROM {ADDRESS_BACKUP}
                ON CONFLICT (network, address) DO NOTHING"""
        )
        addresses_restored = cur.rowcount

        # Drop the lowercase rows apply added, unless a tag uses them by now
        # (e.g. imported after apply).
        cur.execute(
            f"""DELETE FROM address a USING {ADDRESS_ADDED} x
                WHERE a.network = x.network AND a.address = x.address
                  AND NOT EXISTS (SELECT 1 FROM tag t WHERE t.network = a.network
                                  AND t.identifier = a.address)"""
        )
        addresses_removed = cur.rowcount

        print(
            f"changes: {tags_restored}/{backed_up} tags restored, "
            f"{addresses_restored} mixed-case addresses restored, "
            f"{addresses_removed} added lowercase addresses removed"
        )
        if tags_restored < backed_up:
            print(
                f"  note: {backed_up - tags_restored} backed-up tags no longer carry "
                "the lowercased value (deleted or reimported since) and were skipped"
            )

        orphans_after = scalar(cur, TAGS_WITHOUT_ADDRESS)
        if orphans_after > orphans_before:
            conn.rollback()
            raise Abort(
                f"tags without an address row went from {orphans_before} to "
                f"{orphans_after}; rolled back"
            )
        print("verification passed")

        if args.dry_run:
            conn.rollback()
            print("dry run: rolled back, nothing changed")
            return 0
        if not confirm("Restore the pre-fix state?", args.yes):
            conn.rollback()
            print("rolled back, nothing changed")
            return 1
        conn.commit()
        print("restored; backup tables kept (run `cleanup` to drop them)")

    return refresh_after_commit(conn, args)


def cmd_cleanup(conn, args):
    with conn.cursor() as cur:
        existing = [t for t in BACKUP_TABLES if table_exists(cur, t)]
        if not existing:
            print("no backup tables to drop")
            return 0
        if not confirm(
            f"Drop {', '.join(existing)}? `rollback` is impossible afterwards.",
            args.yes,
        ):
            conn.rollback()
            return 1
        cur.execute(f"DROP TABLE {', '.join(existing)}")
    conn.commit()
    print(f"dropped {', '.join(existing)}")
    return 0


def parse_args(argv):
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--env-file", default=".env")
    parser.add_argument("--env-var", default="TAGSTORE_DB_URL")
    parser.add_argument(
        "--lock-timeout",
        default="10s",
        help="how long to wait for running writers before giving up",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("check", help="read-only report and safety check")
    sub.add_parser("dry-run", help="run the full fix and verification, then roll back")
    for name, helptext in (
        ("apply", "run the fix, keep backups, commit after confirmation"),
        ("rollback", "restore the pre-fix state from the backup tables"),
    ):
        p = sub.add_parser(name, help=helptext)
        p.add_argument("--yes", action="store_true", help="do not ask before COMMIT")
        p.add_argument(
            "--skip-refresh",
            action="store_true",
            help="do not refresh materialized views / quality tables afterwards",
        )
        if name == "rollback":
            p.add_argument("--dry-run", action="store_true")
    p = sub.add_parser("cleanup", help="drop the backup tables")
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
