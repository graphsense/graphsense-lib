"""Read-only consistency check across the stores of a chain.

For a window of the most recent blocks, compare the three places a chain lives:

- the **raw** Cassandra keyspace and the **Delta Lake**, both written by ingest,
  must hold the same blocks with the same tx/trace/log counts, and nothing may
  sit above their highest block (the trace of an interrupted ingest batch);
- the **transformed** keyspace, written by the delta updater, must satisfy the
  per-block invariants the updater guarantees, and its bookkeeping must be
  intact (no pending WAL record, a history row for the synced height).

For UTXO chains a sample of addresses touched in the newest synced blocks is
recounted exactly: ``no_incoming_txs`` / ``no_outgoing_txs`` must equal the
number of ``address_transactions`` rows with positive / negative value (the
graphsense-spark definition, see ``computeStatistics`` in
``spark/.../utxo/Transformation.scala``). Those counters are read-modify-write,
so this is the check that catches a double-applied batch.

Not covered: account address counters. They are compressed per batch and
exclude zero-value and reward traces, so they do not equal a row count.
"""

import logging
import random
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Tuple

from ..config import currency_to_schema_type
from ..db.analytics import FIRST_BLOCK
from ..deltaupdate.wal import WAL_TABLE_NAME, DeltaWal
from ..utils import flatten
from ..utils.utxo import (
    get_regflow,
    get_unique_addresses_from_transaction,
    regularize_inoutputs,
)

logger = logging.getLogger(__name__)

MAX_LISTED = 10

# How many blocks above the highest block row are probed for leftovers of an
# interrupted ingest batch. Ingest writes a batch table by table with the block
# table last, so a torn batch always has rows at top + 1.
ABOVE_TOP_PROBE_BLOCKS = 10

EXIT_INCONSISTENT = 92  # same code `delta-update validate` uses for gaps

# Which lake transactions the account updater writes to transformed
# block_transactions: the currency filter in UpdateStrategyAccount.get_changes
# (TRX drops txs without a target and failed ones) followed by the `not
# tx.failed` (receipt_status == 0) filter in createchanges.prepare_txs_for_ingest.
# Keep in sync with both.
ACCOUNT_BLOCK_TX_FILTER = {
    "eth": "receipt_status IS DISTINCT FROM 0",
    "trx": (
        "receipt_status = 1 AND "
        "(to_address IS NOT NULL OR receipt_contract_address IS NOT NULL)"
    ),
}

# Lake tables carrying a block_id besides `block`, per schema type.
LAKE_SIDE_TABLES = {
    "utxo": ["transaction"],
    "account": ["transaction", "trace", "log"],
    "account_trx": ["transaction", "trace", "log", "fee"],
}


class Status(str, Enum):
    OK = "OK"
    WARN = "WARN"
    FAIL = "FAIL"
    SKIP = "SKIP"


@dataclass
class Finding:
    check: str
    status: Status
    detail: str


@dataclass
class AddressSample:
    """One sampled address: its stored counters, all its address_transactions
    rows as (is_outgoing, tx_id, value), and the net flow per tx that raw says
    it must have (tx_id -> flow)."""

    address: str
    address_id: Optional[int]
    no_incoming_txs: int = 0
    no_outgoing_txs: int = 0
    rows: List[Tuple[bool, int, int]] = field(default_factory=list)
    expected_flows: Dict[int, int] = field(default_factory=dict)


def _listing(items: List[str]) -> str:
    more = f" (+{len(items) - MAX_LISTED} more)" if len(items) > MAX_LISTED else ""
    return "; ".join(items[:MAX_LISTED]) + more


def compare_per_block(
    check: str, blocks: List[int], sources: Dict[str, Dict[int, int]]
) -> Finding:
    """Every source must report the same value for every block. A block absent
    from a source counts as a mismatch."""
    labels = list(sources)
    bad = []
    for b in blocks:
        values = [sources[label].get(b) for label in labels]
        if any(v is None for v in values) or len(set(values)) > 1:
            bad.append(
                f"{b}: "
                + ", ".join(
                    f"{label}={'missing' if v is None else v}"
                    for label, v in zip(labels, values)
                )
            )
    if not bad:
        return Finding(
            check, Status.OK, f"{len(blocks)} blocks agree ({' = '.join(labels)})"
        )
    return Finding(
        check, Status.FAIL, f"{len(bad)}/{len(blocks)} blocks differ: {_listing(bad)}"
    )


def check_address_samples(samples: List[AddressSample]) -> Finding:
    check = "transformed address counters (sample)"
    if not samples:
        return Finding(check, Status.SKIP, "no addresses sampled")
    problems = []
    for s in samples:
        if s.address_id is None:
            problems.append(f"{s.address}: missing from address_ids_by_address_prefix")
            continue
        n_in = sum(1 for _, _, v in s.rows if v > 0)
        n_out = sum(1 for _, _, v in s.rows if v < 0)
        if (s.no_incoming_txs, s.no_outgoing_txs) != (n_in, n_out):
            problems.append(
                f"{s.address} (id {s.address_id}): counters in/out "
                f"{s.no_incoming_txs}/{s.no_outgoing_txs} but address_transactions "
                f"has {n_in}/{n_out}"
            )
        stored = {tx_id: (out, v) for out, tx_id, v in s.rows}
        for tx_id, flow in s.expected_flows.items():
            if stored.get(tx_id) != (flow < 0, flow):
                problems.append(
                    f"{s.address} (id {s.address_id}) tx {tx_id}: expected flow "
                    f"{flow}, stored {stored.get(tx_id, 'no row')}"
                )
    if problems:
        return Finding(
            check,
            Status.FAIL,
            f"{len(problems)} problems in {len(samples)} addresses: "
            f"{_listing(problems)}",
        )
    return Finding(
        check, Status.OK, f"{len(samples)} addresses match their address_transactions"
    )


def check_above_top(
    store: str, top: int, leftovers: Dict[str, Optional[str]]
) -> Finding:
    """``leftovers`` maps a table to a description of its rows above ``top``,
    or None when it has none."""
    check = f"{store} rows above highest block"
    found = [f"{table}: {what}" for table, what in leftovers.items() if what]
    if not found:
        return Finding(check, Status.OK, f"nothing above block {top}")
    return Finding(
        check,
        Status.WARN,
        f"rows above block {top} ({_listing(found)}): an ingest is running, or a "
        "batch was interrupted before its block rows were written. The next "
        "ingest run rewrites them; if none has succeeded since, investigate.",
    )


class ConsistencyChecker:
    def __init__(
        self,
        db,
        currency: str,
        dtc=None,
        n_blocks: int = 100,
        sample_addresses: int = 50,
        max_address_rows: int = 5000,
        raw_tx_blocks: int = 10,
        seed: Optional[int] = None,
    ):
        self.db = db
        self.currency = currency
        self.schema_type = currency_to_schema_type[currency]
        self.is_account = self.schema_type.startswith("account")
        self.dtc = dtc
        self.n_blocks = n_blocks
        self.sample_addresses = sample_addresses
        self.max_address_rows = max_address_rows
        self.raw_tx_blocks = raw_tx_blocks
        self.rng = random.Random(seed)
        self.findings: List[Finding] = []

    def add(self, check: str, status: Status, detail: str):
        self.findings.append(Finding(check, status, detail))

    def window(self, top: int, n: Optional[int] = None) -> List[int]:
        n = self.n_blocks if n is None else n
        return list(range(max(FIRST_BLOCK[self.currency], top - n + 1), top + 1))

    # -- raw Cassandra -----------------------------------------------------

    def raw_block_counts(self, blocks: List[int]) -> Dict[int, int]:
        raw = self.db.raw
        col = "transaction_count" if self.is_account else "no_transactions"
        bs = raw.get_block_bucket_size()
        counts = {}
        for g in sorted({raw.get_id_group(b, bs) for b in blocks}):
            for row in raw.select(
                "block", ["block_id", col], where={"block_id_group": g}
            ):
                counts[row.block_id] = getattr(row, col)
        return {b: counts[b] for b in blocks if b in counts}

    def raw_per_block(
        self, table: str, blocks: List[int], column: str
    ) -> Dict[int, list]:
        """Rows of ``table`` per block, one single-partition read each;
        ``column`` is a CQL select expression."""
        raw = self.db.raw
        bs = raw.get_block_bucket_size()
        return {
            b: list(
                raw.select(
                    table,
                    [column],
                    where={"block_id_group": raw.get_id_group(b, bs), "block_id": b},
                )
            )
            for b in blocks
        }

    def raw_count(self, table: str, blocks: List[int]) -> Dict[int, int]:
        return {
            b: rows[0].n
            for b, rows in self.raw_per_block(table, blocks, "COUNT(*) AS n").items()
        }

    def raw_utxo_block_tx_ids(self, blocks: List[int]) -> Dict[int, List[int]]:
        """tx_ids per block from raw block_transactions; blocks without a row
        are absent."""
        per_block = self.raw_per_block("block_transactions", blocks, "txs")
        return {
            b: [tx.tx_id for tx in flatten([r.txs for r in rows])]
            for b, rows in per_block.items()
            if rows
        }

    def raw_utxo_tx_rows(self, tx_ids: Dict[int, List[int]]) -> Dict[int, int]:
        """Raw `transaction` rows in each block's tx_id span. A block's tx_ids
        are consecutive, so its rows are exactly the span [min, max]."""
        raw = self.db.raw
        tbs = raw.get_tx_bucket_size()
        out = {}
        for b, ids in tx_ids.items():
            if not ids:
                out[b] = 0
                continue
            lo, hi = min(ids), max(ids)
            out[b] = self._count_tx_span(
                range(raw.get_id_group(lo, tbs), raw.get_id_group(hi, tbs) + 1), lo, hi
            )
        return out

    def _count_tx_span(self, groups, lo: int, hi: Optional[int]) -> int:
        raw = self.db.raw
        cql = (
            f"SELECT COUNT(*) AS n FROM {raw.get_keyspace()}.transaction "
            "WHERE tx_id_group IN :groups AND tx_id >= :lo"
            + (" AND tx_id <= :hi" if hi is not None else "")
        )
        params = {"groups": list(groups), "lo": lo}
        if hi is not None:
            params["hi"] = hi
        stmt = raw._db.get_prepared_statement(cql).bind(params)
        return list(raw._db.execute_statement(stmt))[0].n

    def raw_account_txs_by_hash(self, blocks: List[int]) -> Dict[int, int]:
        """Read every lake tx of ``blocks`` back from raw by hash (raw keeps no
        per-block tx index) and count, per block, those found with that block_id."""
        raw = self.db.raw
        df = self.dtc.select_columns("transaction", blocks, ["block_id", "tx_hash"])
        prefix_len = raw.get_tx_prefix_length()
        stmt = raw.select_stmt(
            "transaction",
            columns=["block_id"],
            where={"tx_hash_prefix": "?", "tx_hash": "?"},
            limit=1,
        )
        params = [
            ((int(b), bytes(h)), [bytes(h).hex()[:prefix_len], bytes(h)])
            for b, h in zip(df["block_id"], df["tx_hash"])
        ]
        found = defaultdict(int)
        for (b, _), res in raw._db.execute_batch(stmt, params):
            row = res.one()
            if row is not None and row.block_id == b:
                found[b] += 1
        return {b: found[b] for b in blocks}

    # -- checks --------------------------------------------------------------

    def check_state(self) -> Tuple[Optional[int], Optional[int], Optional[int], bool]:
        tdb = self.db.transformed
        wal_pending = False
        if tdb._db.has_table(tdb.get_keyspace(), WAL_TABLE_NAME):
            # DeltaWal's constructor does no I/O and pending_header is a single
            # read; never call ensure_schema/recover from here.
            header = DeltaWal(tdb, "consistency-check", "n/a").pending_header()
            if header is not None:
                wal_pending = True
                self.add(
                    "delta updater WAL",
                    Status.FAIL,
                    f"pending record for blocks {header['block_lo']}-"
                    f"{header['block_hi']} (run {header['run_id']}, version "
                    f"{header['code_version']}): a batch was interrupted mid-write. "
                    "The next delta-update run replays it.",
                )
            else:
                self.add("delta updater WAL", Status.OK, "no pending record")
        else:
            self.add(
                "delta updater WAL", Status.SKIP, "no WAL table (WAL never enabled)"
            )

        hb_du = tdb.get_highest_block_delta_updater()
        if hb_du is not None and not tdb.is_first_delta_update_run():
            if tdb.delta_updater_history_has_block(hb_du):
                self.add(
                    "delta updater bookkeeping", Status.OK, f"history row for {hb_du}"
                )
            else:
                self.add(
                    "delta updater bookkeeping",
                    Status.FAIL,
                    f"summary_statistics is at block {hb_du} but delta_updater_history "
                    "has no row for it (torn bookkeeping write)",
                )

        hb_raw = self.db.raw.get_highest_block()
        hb_lake = self.dtc.highest_block() if self.dtc is not None else None
        heights = f"raw {hb_raw}, lake {hb_lake}, transformed {hb_du}"
        status = Status.OK
        notes = []
        if hb_du is not None and hb_raw is not None and hb_du > hb_raw:
            status = Status.FAIL
            notes.append("transformed is ahead of raw")
        if self.is_account and None not in (hb_du, hb_lake) and hb_du > hb_lake:
            status = Status.FAIL
            notes.append("transformed is ahead of the lake")
        if None not in (hb_raw, hb_lake) and hb_raw != hb_lake and status == Status.OK:
            status = Status.WARN
            notes.append(
                f"raw and lake differ by {abs(hb_raw - hb_lake)} blocks "
                "(an ingest may be running, or its sinks diverged)"
            )
        self.add(
            "heights", status, heights + (": " + "; ".join(notes) if notes else "")
        )
        return hb_raw, hb_lake, hb_du, wal_pending

    def check_ingest_window(self, blocks: List[int]):
        """Per-block counts within the ingested window, raw vs lake (or raw
        alone when no lake is configured)."""
        raw_txs = self.raw_block_counts(blocks)
        if not self.is_account:
            tx_ids = self.raw_utxo_block_tx_ids(blocks)
            sources = {
                "raw block.no_transactions": raw_txs,
                "raw block_transactions": {b: len(ids) for b, ids in tx_ids.items()},
                "raw transaction rows": self.raw_utxo_tx_rows(tx_ids),
            }
            if self.dtc is not None:
                lake_blocks = self.dtc.aggregate_per_block(
                    "block", blocks, {"n": "max(no_transactions)"}
                )
                lake_txs = self.dtc.aggregate_per_block(
                    "transaction", blocks, {"n": "count(*)"}
                )
                sources["lake block.no_transactions"] = {
                    b: v["n"] for b, v in lake_blocks.items()
                }
                sources["lake transaction rows"] = {
                    b: lake_txs.get(b, {"n": 0})["n"] for b in lake_blocks
                }
            self.findings.append(compare_per_block("block tx counts", blocks, sources))
            return

        if self.dtc is None:
            self.add("raw vs lake", Status.SKIP, "no delta sink configured")
            return

        def lake_count(table):
            got = self.dtc.aggregate_per_block(table, blocks, {"n": "count(*)"})
            return {b: got.get(b, {"n": 0})["n"] for b in lake_blocks}

        lake_blocks = self.dtc.aggregate_per_block(
            "block",
            blocks,
            {"tx_count": "max(transaction_count)"}
            | (
                {"withdrawals": "sum(coalesce(len(withdrawals), 0))"}
                if self.currency == "eth"
                else {}
            ),
        )
        lake_txs = lake_count("transaction")
        self.findings.append(
            compare_per_block(
                "block tx counts",
                blocks,
                {
                    "raw block.transaction_count": raw_txs,
                    "lake block.transaction_count": {
                        b: v["tx_count"] for b, v in lake_blocks.items()
                    },
                    "lake transaction rows": lake_txs,
                },
            )
        )
        if self.raw_tx_blocks > 0:
            newest = blocks[-self.raw_tx_blocks :]
            self.findings.append(
                compare_per_block(
                    f"raw transaction rows (newest {len(newest)} blocks, by hash)",
                    newest,
                    {
                        "lake transaction rows": {b: lake_txs.get(b) for b in newest},
                        "found in raw": self.raw_account_txs_by_hash(newest),
                    },
                )
            )
        # The raw trace table also holds ETH validator withdrawals (as
        # synthetic traces); the lake keeps them only on block.withdrawals.
        lake_traces = lake_count("trace")
        self.findings.append(
            compare_per_block(
                "trace counts",
                blocks,
                {
                    "raw trace rows": self.raw_count("trace", blocks),
                    "lake trace rows (+withdrawals)": {
                        b: lake_traces[b] + v.get("withdrawals", 0)
                        for b, v in lake_blocks.items()
                    },
                },
            )
        )
        self.findings.append(
            compare_per_block(
                "log counts",
                blocks,
                {
                    "raw log rows": self.raw_count("log", blocks),
                    "lake log rows": lake_count("log"),
                },
            )
        )
        if self.currency == "trx":
            self.findings.append(
                compare_per_block(
                    "trx fee rows",
                    blocks,
                    {
                        "lake transaction rows": lake_txs,
                        "lake fee rows": lake_count("fee"),
                    },
                )
            )

    def check_raw_above_top(self, top: int):
        above = list(range(top + 1, top + 1 + ABOVE_TOP_PROBE_BLOCKS))

        def blocks_desc(blocks):
            return f"blocks {min(blocks)}-{max(blocks)}" if blocks else None

        if self.is_account:
            leftovers = {
                table: blocks_desc(
                    [b for b, n in self.raw_count(table, above).items() if n]
                )
                for table in ("trace", "log")
            }
        else:
            leftovers = {
                "block_transactions": blocks_desc(
                    list(self.raw_utxo_block_tx_ids(above))
                )
            }
            # tx rows past the top block's last tx_id: probe its group and the next.
            top_ids = self.raw_utxo_block_tx_ids([top]).get(top)
            if top_ids:
                last = max(top_ids)
                g = self.db.raw.get_id_group(last, self.db.raw.get_tx_bucket_size())
                n = self._count_tx_span([g, g + 1], last + 1, None)
                if n:
                    leftovers["transaction"] = f"{n} rows past tx_id {last}"
        # Raw account txs are keyed by hash, so leftovers there are not
        # findable; the trace/log probe covers the same batch.
        self.findings.append(check_above_top("raw", top, leftovers))

    def check_lake_above_top(self, top: int):
        leftovers = {}
        for table in LAKE_SIDE_TABLES[self.schema_type]:
            hb = self.dtc.highest_block(table)
            if hb is not None and hb > top:
                leftovers[table] = f"up to block {hb}"
        self.findings.append(check_above_top("lake", top, leftovers))

    def check_exchange_rates(self, blocks: List[int]):
        tdb = self.db.transformed
        missing = [
            str(b)
            for b in blocks
            if (er := tdb.get_exchange_rates_by_block(b)) is None
            or er.fiat_values is None
        ]
        if missing:
            self.add(
                "transformed exchange rates",
                Status.FAIL,
                f"{len(missing)}/{len(blocks)} blocks without rates: {_listing(missing)}",
            )
        else:
            self.add(
                "transformed exchange rates",
                Status.OK,
                f"{len(blocks)} blocks have rates",
            )

    def check_account_block_transactions(self, blocks: List[int]):
        if self.dtc is None:
            self.add(
                "transformed block_transactions",
                Status.SKIP,
                "no delta sink configured",
            )
            return
        tdb = self.db.transformed
        bs = tdb.get_block_id_bucket_size()
        stored = {}
        for b in blocks:
            rows = list(
                tdb.select(
                    "block_transactions",
                    ["COUNT(*) AS n"],
                    where={"block_id_group": tdb.get_id_group(b, bs), "block_id": b},
                )
            )
            stored[b] = rows[0].n
        expected = self.dtc.aggregate_per_block(
            "transaction",
            blocks,
            {"n": f"count(*) FILTER (WHERE {ACCOUNT_BLOCK_TX_FILTER[self.currency]})"},
        )
        self.findings.append(
            compare_per_block(
                "transformed block_transactions",
                blocks,
                {
                    "transformed rows": stored,
                    "expected from lake txs": {
                        b: expected.get(b, {"n": 0})["n"] for b in blocks
                    },
                },
            )
        )

    def sample_utxo_addresses(self, blocks: List[int]) -> List[AddressSample]:
        raw, tdb = self.db.raw, self.db.transformed
        flows: Dict[str, Dict[int, int]] = {}
        # Newest blocks first: they are the ones a torn batch would touch.
        for b in reversed(blocks):
            for tx in raw.get_transactions_in_block(b):
                if tx.coinbase:
                    continue
                reg_in = regularize_inoutputs(tx.inputs)
                reg_out = regularize_inoutputs(tx.outputs)
                for adr in get_unique_addresses_from_transaction(tx):
                    flows.setdefault(adr, {})[tx.tx_id] = get_regflow(
                        reg_in, reg_out, adr
                    )
            if len(flows) >= self.sample_addresses * 20:
                break
        candidates = sorted(flows)
        self.rng.shuffle(candidates)

        bs = tdb.get_address_id_bucket_size()
        samples = []
        skipped_busy = 0
        for adr in candidates:
            if len(samples) >= self.sample_addresses:
                break
            db_adr = tdb.to_db_address(adr)
            row = tdb.select_one_safe(
                "address_ids_by_address_prefix",
                ["address_id"],
                {"address_prefix": db_adr.prefix, "address": db_adr.db_encoding},
            )
            if row is None:
                samples.append(AddressSample(adr, None))
                continue
            aid = row.address_id
            where = {"address_id_group": tdb.get_id_group(aid, bs), "address_id": aid}
            stats = tdb.select_one(
                "address", ["no_incoming_txs", "no_outgoing_txs"], where=where
            )
            n_in = stats.no_incoming_txs if stats is not None else 0
            n_out = stats.no_outgoing_txs if stats is not None else 0
            if n_in + n_out > self.max_address_rows:
                skipped_busy += 1
                continue
            rows = [
                (r.is_outgoing, r.tx_id, r.value)
                for r in tdb.select(
                    "address_transactions",
                    ["is_outgoing", "tx_id", "value"],
                    where=where,
                )
            ]
            samples.append(AddressSample(adr, aid, n_in, n_out, rows, flows[adr]))
        if skipped_busy:
            logger.info(
                f"Skipped {skipped_busy} sampled addresses with more than "
                f"{self.max_address_rows} txs."
            )
        return samples

    def run(self) -> List[Finding]:
        hb_raw, hb_lake, hb_du, wal_pending = self.check_state()

        if hb_raw is not None:
            top = min(hb_raw, hb_lake) if hb_lake is not None else hb_raw
            self.check_ingest_window(self.window(top))
            self.check_raw_above_top(hb_raw)
        if hb_lake is not None:
            self.check_lake_above_top(hb_lake)

        if hb_du is None:
            self.add("transformed", Status.SKIP, "transformed keyspace has no height")
            return self.findings
        delta_window = self.window(hb_du)
        self.check_exchange_rates(delta_window)
        if self.is_account:
            self.check_account_block_transactions(delta_window)
        elif self.sample_addresses <= 0:
            pass
        elif wal_pending:
            self.add(
                "transformed address counters (sample)",
                Status.SKIP,
                "a WAL record is pending; counters are expected to be off until "
                "it is replayed",
            )
        else:
            self.findings.append(
                check_address_samples(self.sample_utxo_addresses(delta_window))
            )
        return self.findings


def run_consistency_check(
    env: str,
    currency: str,
    n_blocks: int,
    sample_addresses: int,
    max_address_rows: int,
    raw_tx_blocks: int,
    use_lock: bool,
    seed: Optional[int] = None,
) -> List[Finding]:
    """Run all checks for one chain and print a report."""
    from contextlib import nullcontext

    from rich.table import Table

    from ..config import get_config
    from ..db import DbFactory
    from ..utils.console import console
    from ..utils.locking import create_lock

    config = get_config()
    du_config = config.get_deltaupdater_config(env, currency)
    dtc = None
    if du_config is not None:
        from ..utils.DeltaTableConnector import DeltaTableConnector

        dtc = DeltaTableConnector(
            du_config.delta_sink.directory, du_config.s3_credentials
        )

    with DbFactory().from_config(env, currency, readonly=True) as db:
        # Hold the transformed lock so a delta update cannot move the counters
        # mid-check. Raw is not locked: ingest only appends above the heights
        # read at the start, and the block row is written last, so the window
        # below them is stable without blocking ingest.
        lock = create_lock(db.transformed.get_keyspace()) if use_lock else nullcontext()
        with lock:
            findings = ConsistencyChecker(
                db,
                currency,
                dtc=dtc,
                n_blocks=n_blocks,
                sample_addresses=sample_addresses,
                max_address_rows=max_address_rows,
                raw_tx_blocks=raw_tx_blocks,
                seed=seed,
            ).run()

    style = {
        Status.OK: "green",
        Status.WARN: "yellow",
        Status.FAIL: "bold red",
        Status.SKIP: "dim",
    }
    table = Table(title=f"Consistency {env}/{currency}, last {n_blocks} blocks")
    table.add_column("check")
    table.add_column("status")
    table.add_column("detail", overflow="fold")
    for f in findings:
        table.add_row(f.check, f"[{style[f.status]}]{f.status.value}[/]", f.detail)
    console.print(table)
    return findings
