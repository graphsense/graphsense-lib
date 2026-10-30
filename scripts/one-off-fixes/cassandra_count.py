#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.10"
# dependencies = ["cassandra-driver>=3.29"]
# ///
# ruff: noqa: T201
"""Exact row/partition count of a Cassandra table without a full-scan timeout.

Splits the Murmur3 token ring into N slices and runs, in parallel,

    SELECT count(*) FROM <ks>.<table> WHERE token(<pk>) > ? AND token(<pk>) <= ?

Each slice is small enough to finish within the request timeout. A slice that
times out anyway is split in two and retried, down to a minimum size.

Counts rows; for a table whose primary key is just the partition key (e.g.
pubkey_v2.pubkey_by_address) that equals the number of partitions.

Example:

    ./cassandra_count.py --hosts cass1,cass2 --keyspace pubkey_v2 \
        --table pubkey_by_address --pk address

Credentials, if the cluster needs them: CASSANDRA_USERNAME / CASSANDRA_PASSWORD
in the environment. Without them the tool first connects without
authentication; if the cluster requires it, it asks for username and password
on the terminal (the password is not echoed). Never pass a password on the
command line.
"""

from __future__ import annotations

import argparse
import getpass
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from cassandra import (
    AuthenticationFailed,
    ConsistencyLevel,
    OperationTimedOut,
    ReadTimeout,
)
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster, NoHostAvailable

MIN_TOKEN = -(2**63)
MAX_TOKEN = 2**63 - 1
MIN_SLICE = 2**40  # don't split below this many tokens


def slices(n: int) -> list[tuple[int, int]]:
    """n contiguous (lo, hi] slices covering the whole ring. The first slice
    starts at MIN_TOKEN and is queried with >= so MIN_TOKEN is included."""
    step = (MAX_TOKEN - MIN_TOKEN) // n
    bounds = [MIN_TOKEN + i * step for i in range(n)] + [MAX_TOKEN]
    return list(zip(bounds[:-1], bounds[1:]))


def _auth_error(e: Exception) -> bool:
    """True if connecting failed because the cluster wants credentials (or
    rejected the given ones)."""
    if isinstance(e, AuthenticationFailed):
        return True
    if isinstance(e, NoHostAvailable):
        return any(isinstance(err, AuthenticationFailed) for err in e.errors.values())
    return False


def connect(hosts: list[str], port: int):
    """Connect; credentials from the environment, else from the terminal if
    the cluster asks for them."""

    def _cluster(auth):
        return Cluster(
            hosts,
            port=port,
            auth_provider=auth,
            protocol_version=4,  # as graphsense-lib: v5 framing desyncs under load
            executor_threads=4,
        )

    if os.environ.get("CASSANDRA_USERNAME"):
        auth = PlainTextAuthProvider(
            os.environ["CASSANDRA_USERNAME"], os.environ.get("CASSANDRA_PASSWORD", "")
        )
        cluster = _cluster(auth)
        return cluster, cluster.connect()

    cluster = _cluster(None)
    try:
        return cluster, cluster.connect()
    except Exception as e:  # noqa: BLE001 - inspected below
        cluster.shutdown()
        if not _auth_error(e):
            raise
    if not sys.stdin.isatty():
        sys.exit(
            "The cluster requires authentication: set CASSANDRA_USERNAME and "
            "CASSANDRA_PASSWORD, or run interactively."
        )
    for attempt in range(3):
        try:
            user = input("Cassandra username: ").strip()
            password = getpass.getpass("Cassandra password: ")
        except (EOFError, KeyboardInterrupt):
            sys.exit("\nAborted.")
        cluster = _cluster(PlainTextAuthProvider(user, password))
        try:
            return cluster, cluster.connect()
        except Exception as e:  # noqa: BLE001 - inspected below
            cluster.shutdown()
            if not _auth_error(e):
                raise
            print("Authentication failed.", file=sys.stderr)
    sys.exit("Giving up after 3 failed logins.")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--hosts", required=True, help="comma-separated contact points")
    ap.add_argument("--port", type=int, default=9042)
    ap.add_argument("--keyspace", required=True)
    ap.add_argument("--table", required=True)
    ap.add_argument(
        "--pk", required=True, help="partition key column(s), comma-separated"
    )
    ap.add_argument("--splits", type=int, default=4096, help="initial token slices")
    ap.add_argument("--concurrency", type=int, default=16, help="parallel slices")
    ap.add_argument("--timeout", type=float, default=60.0, help="per-query seconds")
    ap.add_argument(
        "--consistency",
        default="LOCAL_ONE",
        choices=["ONE", "LOCAL_ONE", "LOCAL_QUORUM", "QUORUM", "ALL"],
    )
    args = ap.parse_args()

    cluster, session = connect(args.hosts.split(","), args.port)
    session.default_timeout = args.timeout

    tok = f"token({args.pk})"
    cl = getattr(ConsistencyLevel, args.consistency)
    q_first = session.prepare(
        f"SELECT count(*) FROM {args.keyspace}.{args.table} "
        f"WHERE {tok} >= ? AND {tok} <= ?"
    )
    q_rest = session.prepare(
        f"SELECT count(*) FROM {args.keyspace}.{args.table} "
        f"WHERE {tok} > ? AND {tok} <= ?"
    )
    q_first.consistency_level = q_rest.consistency_level = cl

    total = 0
    done = 0
    lock = threading.Lock()
    started = time.time()
    n_slices = args.splits

    def count_range(lo: int, hi: int, first: bool) -> int:
        """Count (lo, hi] (or [lo, hi] for the first slice); split on timeout."""
        stmt = q_first if first else q_rest
        for attempt in range(3):
            try:
                return session.execute(stmt, (lo, hi)).one()[0]
            except (OperationTimedOut, ReadTimeout):
                if hi - lo > MIN_SLICE:
                    mid = lo + (hi - lo) // 2
                    return count_range(lo, mid, first) + count_range(mid, hi, False)
                time.sleep(2**attempt)
            except NoHostAvailable:
                time.sleep(2**attempt)
        raise RuntimeError(f"slice ({lo}, {hi}] failed after retries")

    def work(i: int, lo: int, hi: int) -> int:
        nonlocal total, done
        c = count_range(lo, hi, first=(i == 0))
        with lock:
            total += c
            done += 1
            if done % max(1, n_slices // 50) == 0 or done == n_slices:
                rate = total / max(time.time() - started, 1e-6)
                print(
                    f"\r{done}/{n_slices} slices  {total:,} rows  ({rate:,.0f}/s)",
                    end="",
                    file=sys.stderr,
                    flush=True,
                )
        return c

    with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
        futures = [
            pool.submit(work, i, lo, hi) for i, (lo, hi) in enumerate(slices(n_slices))
        ]
        for f in as_completed(futures):
            f.result()  # re-raise failures

    print(file=sys.stderr)
    print(
        f"{args.keyspace}.{args.table}: {total:,} rows "
        f"({time.time() - started:,.0f}s, {n_slices} slices, {args.consistency})"
    )
    cluster.shutdown()
    return 0


if __name__ == "__main__":
    sys.exit(main())
