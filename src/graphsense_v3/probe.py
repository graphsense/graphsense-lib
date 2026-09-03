"""Every read the v3 DAL has to support, run against a real keyspace.

The point is not that the queries succeed -- it is that they succeed *the way
the schema claims they will*. A v3 table is a bet about an access pattern, and
three of those bets are only settleable against data:

* **the prefix restriction.** Cassandra requires clustering restrictions to
  form a prefix, so anything that has to be pushed down lives in the PARTITION
  key -- which turns one logical read into several partition reads. This
  reports how many, per access pattern, because that number is the design.
* **the bucket function.** ``address_bucket`` and ``rel_bucket`` are
  ``crc32(entity) % n`` (:func:`graphsense_v3.codec.bucket`), computed here the
  way a DAL would. A row that does not come back means the reader and the
  writer disagree, which no unit test can catch: both sides would be this code.
* **the fan-out constants.** ``relation_buckets`` and ``tx_page_size`` are read
  from the keyspace's own ``configuration`` row, not from the defaults, so a
  keyspace written with different constants is probed with its own.

Run it after a backfill, iterate on it before writing the DAL. Every probe
prints the CQL it ran, so a failure is copy-pasteable into cqlsh.

Fixtures are selected with small unbounded scans (``LIMIT`` with no partition
key). That is fine on a benchmark keyspace and is why this is a probe rather
than something the service layer would ever do.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any, Optional

from graphsense_v3.codec import bucket

logger = logging.getLogger(__name__)

#: Access patterns whose absence would block a REST endpoint, versus ones that
#: are an optimisation. A FAIL in the first group is a schema problem.
REQUIRED = "required"
OPTIONAL = "optional"


@dataclass
class Result:
    """One access pattern, run."""

    name: str
    endpoint: str
    cql: str
    rows: int
    millis: float
    reads: int
    kind: str = REQUIRED
    error: Optional[str] = None
    note: str = ""

    @property
    def ok(self) -> bool:
        return self.error is None

    @property
    def verdict(self) -> str:
        if self.error:
            return "FAIL" if self.kind is REQUIRED else "warn"
        return "ok"


@dataclass
class Fixtures:
    """Real keys taken from the keyspace, so the probes address real rows."""

    address: Optional[bytes] = None
    is_outgoing: bool = False
    is_zero_value: bool = False
    tx_page: int = 0
    busiest_address: Optional[bytes] = None
    busiest_pages: int = 0
    tx_hash: Optional[bytes] = None
    tx_prefix: Optional[str] = None
    tx_id: Optional[int] = None
    block_id: Optional[int] = None
    day: Optional[int] = None
    link_src: Optional[bytes] = None
    link_dst: Optional[bytes] = None
    address_prefix: Optional[str] = None
    currency: Optional[str] = None
    missing: list = field(default_factory=list)


class Prober:
    """Runs the catalogue against one raw + derived keyspace pair."""

    def __init__(self, session, raw: str, derived: str, config: dict) -> None:
        self.session = session
        self.raw = raw
        self.derived = derived
        self.config = config
        self.results: list[Result] = []

    # -- plumbing ---------------------------------------------------------

    def _rows(self, cql: str, params: tuple = ()) -> tuple[list, float]:
        started = time.monotonic()
        rows = list(self.session.execute(cql, params))
        return rows, (time.monotonic() - started) * 1000

    def probe(
        self,
        name: str,
        endpoint: str,
        cql: str,
        params: tuple = (),
        *,
        reads: int = 1,
        kind: str = REQUIRED,
        note: str = "",
        expect_rows: bool = True,
    ) -> list:
        """Run one access pattern and record it. Returns the rows."""
        rendered = _render(cql, params)
        try:
            rows, millis = self._rows(cql, params)
        except Exception as exc:  # noqa: BLE001 -- reporting, not handling
            self.results.append(
                Result(name, endpoint, rendered, 0, 0.0, reads, kind, str(exc), note)
            )
            return []
        error = None
        if expect_rows and not rows:
            error = "no rows -- the key is wrong, or the table was not written"
        self.results.append(
            Result(
                name, endpoint, rendered, len(rows), millis, reads, kind, error, note
            )
        )
        return rows

    # -- fixtures ---------------------------------------------------------

    def fixtures(self) -> Fixtures:
        """Pick real keys out of the data. Skips what it cannot find."""
        found = Fixtures()

        rows, _ = self._rows(
            f"SELECT address, is_outgoing, is_zero_value, tx_page, tx_id "
            f"FROM {self.derived}.address_transactions LIMIT 1"
        )
        if rows:
            found.address = bytes(rows[0].address)
            found.is_outgoing = rows[0].is_outgoing
            found.is_zero_value = rows[0].is_zero_value
            found.tx_page = rows[0].tx_page
            found.tx_id = rows[0].tx_id
        else:
            found.missing.append("address_transactions is empty")

        # An address with more than one page is the only way to exercise the
        # paging cursor; a small block range may simply not contain one.
        rows, _ = self._rows(
            f"SELECT address, is_outgoing, is_zero_value, tx_page "
            f"FROM {self.derived}.address_tx_pages LIMIT 2000"
        )
        if rows:
            busiest = max(rows, key=lambda r: r.tx_page)
            found.busiest_address = bytes(busiest.address)
            found.busiest_pages = busiest.tx_page
            found.is_outgoing = busiest.is_outgoing
            found.is_zero_value = busiest.is_zero_value

        rows, _ = self._rows(
            f"SELECT tx_prefix, tx_hash, tx_id "
            f"FROM {self.raw}.transaction_by_tx_prefix LIMIT 1"
        )
        if rows:
            found.tx_prefix = rows[0].tx_prefix
            found.tx_hash = bytes(rows[0].tx_hash)
            found.tx_id = rows[0].tx_id
        else:
            found.missing.append("transaction_by_tx_prefix is empty")

        rows, _ = self._rows(
            f"SELECT highest_block FROM {self.raw}.summary_statistics LIMIT 1"
        )
        if rows:
            found.block_id = rows[0].highest_block

        rows, _ = self._rows(f"SELECT day FROM {self.raw}.block_by_date LIMIT 1")
        if rows:
            found.day = rows[0].day

        rows, _ = self._rows(
            f"SELECT src_address, dst_address "
            f"FROM {self.derived}.address_link_transactions LIMIT 1"
        )
        if rows:
            found.link_src = bytes(rows[0].src_address)
            found.link_dst = bytes(rows[0].dst_address)

        rows, _ = self._rows(
            f"SELECT address_prefix FROM {self.derived}.address_by_prefix LIMIT 1"
        )
        if rows:
            found.address_prefix = rows[0].address_prefix

        rows, _ = self._rows(f"SELECT currency FROM {self.derived}.balance LIMIT 1")
        if rows:
            found.currency = rows[0].currency

        return found

    # -- the catalogue ----------------------------------------------------

    def run(self, found: Fixtures) -> list[Result]:
        self._meta()
        self._address(found)
        self._address_transactions(found)
        self._relations(found)
        self._transaction(found)
        self._block(found)
        self._rates(found)
        return self.results

    def _meta(self) -> None:
        for keyspace in (self.raw, self.derived):
            self.probe(
                f"{keyspace.split('_')[-2]} complete marker",
                "readiness",
                f"SELECT key, value FROM {keyspace}.markers WHERE key = %s",
                ("complete",),
                note="a keyspace without this is missing data and cannot say so",
            )
            self.probe(
                f"{keyspace.split('_')[-2]} configuration",
                "readiness",
                f"SELECT * FROM {keyspace}.configuration WHERE keyspace_name = %s",
                (keyspace,),
                note="the bucketing constants a reader must use",
            )
        self.probe(
            "summary statistics",
            "/{network}/stats",
            f"SELECT * FROM {self.derived}.summary_statistics WHERE id = 0",
        )

    def _address(self, found: Fixtures) -> None:
        if found.address is None:
            return
        buckets = self.config["entity_buckets"]
        addr_bucket = bucket(found.address, buckets)

        self.probe(
            "address stats (epoch slice)",
            "/{network}/addresses/{address}",
            f"SELECT * FROM {self.derived}.address_stats "
            f"WHERE address_bucket = %s AND address = %s",
            (addr_bucket, found.address),
            note=(
                "one partition read; the DAL SUMS the epoch rows. Epoch 0 alone "
                "is the compacted base, so reading only it would silently drop "
                "everything the incremental path has added since."
            ),
        )
        self.probe(
            "address stats, epoch 0 only",
            "/{network}/addresses/{address}",
            f"SELECT * FROM {self.derived}.address_stats "
            f"WHERE address_bucket = %s AND address = %s AND epoch = 0",
            (addr_bucket, found.address),
            kind=OPTIONAL,
            note="the paging cursors and degrees live here and are NOT summable",
        )
        self.probe(
            "balance",
            "/{network}/addresses/{address}",
            f"SELECT * FROM {self.derived}.balance "
            f"WHERE address_bucket = %s AND address = %s",
            (addr_bucket, found.address),
            note="summed over epoch, like the stats",
        )
        if found.currency:
            self.probe(
                "balance, one asset",
                "/{network}/addresses/{address}",
                f"SELECT * FROM {self.derived}.balance "
                f"WHERE address_bucket = %s AND address = %s AND currency = %s",
                (addr_bucket, found.address, found.currency),
                kind=OPTIONAL,
                expect_rows=False,
                note="the account case; a UTXO address holds only the native coin",
            )
        self.probe(
            "balance at a day",
            "balance over time",
            f"SELECT day, balance FROM {self.derived}.balance_history "
            f"WHERE address_bucket = %s AND address = %s AND currency = %s "
            f"AND day <= %s LIMIT 1",
            (addr_bucket, found.address, found.currency or "LTC", 99999999),
            note=(
                "day DESC + LIMIT 1 is the whole point of this table: the "
                "balance ON a day is one row, not a sum over every active day"
            ),
        )
        if found.address_prefix:
            self.probe(
                "address search by prefix",
                "/{network}/search",
                f"SELECT * FROM {self.derived}.address_by_prefix "
                f"WHERE address_prefix = %s",
                (found.address_prefix,),
                note="prefix length comes from `configuration`, not a constant",
            )

    def _address_transactions(self, found: Fixtures) -> None:
        address = found.busiest_address or found.address
        if address is None:
            return
        page_size = self.config["tx_page_size"]

        self.probe(
            "address txs, one direction, first page",
            "/{network}/addresses/{address}/txs",
            f"SELECT tx_id, value, balance FROM {self.derived}.address_transactions "
            f"WHERE address = %s AND is_outgoing = %s AND is_zero_value = false "
            f"AND tx_page = %s",
            (address, found.is_outgoing, 0),
            note=f"one partition, at most tx_page_size={page_size} rows",
        )
        # The cost of "all transactions of an address": the direction and the
        # zero flag are in the PARTITION key, so they cannot be left unbound.
        for outgoing in (False, True):
            self.probe(
                f"address txs, {'outgoing' if outgoing else 'incoming'} page 0",
                "/{network}/addresses/{address}/txs",
                f"SELECT tx_id FROM {self.derived}.address_transactions "
                f"WHERE address = %s AND is_outgoing = %s AND is_zero_value = false "
                f"AND tx_page = 0",
                (address, outgoing),
                kind=OPTIONAL,
                expect_rows=False,
                note="unbounded direction costs TWO partition reads, merged client-side",
            )
        self.probe(
            "address txs including zero-value",
            "/{network}/addresses/{address}/txs",
            f"SELECT tx_id FROM {self.derived}.address_transactions "
            f"WHERE address = %s AND is_outgoing = %s AND is_zero_value = true "
            f"AND tx_page = 0",
            (address, found.is_outgoing),
            reads=2,
            kind=OPTIONAL,
            expect_rows=False,
            note="a FOURTH partition when zero-value rows are not filtered out",
        )
        self.probe(
            "address tx page index",
            "/{network}/addresses/{address}/txs?min_height",
            f"SELECT first_tx_id, tx_page FROM {self.derived}.address_tx_pages "
            f"WHERE address = %s AND is_outgoing = %s AND is_zero_value = false",
            (address, found.is_outgoing),
            note=(
                "ordinal pages are NOT tx_id-aligned, so a height filter cannot "
                "compute its page -- it looks it up here first"
            ),
        )
        if found.tx_id:
            self.probe(
                "address txs below a tx_id",
                "/{network}/addresses/{address}/txs?page",
                f"SELECT tx_id FROM {self.derived}.address_transactions "
                f"WHERE address = %s AND is_outgoing = %s AND is_zero_value = false "
                f"AND tx_page = %s AND tx_id < %s",
                (address, found.is_outgoing, 0, found.tx_id),
                kind=OPTIONAL,
                expect_rows=False,
                note="tx_id is the clustering key, DESC, so this is a range read",
            )

    def _relations(self, found: Fixtures) -> None:
        if found.address is None:
            return
        rel_buckets = self.config["relation_buckets"]

        for direction, table, near in (
            ("outgoing", "address_outgoing_relations", "src_address"),
            ("incoming", "address_incoming_relations", "dst_address"),
        ):
            total = 0
            started = time.monotonic()
            failure = None
            cql = (
                f"SELECT * FROM {self.derived}.{table} "
                f"WHERE {near} = %s AND rel_bucket = %s"
            )
            try:
                for index in range(rel_buckets):
                    total += len(
                        list(self.session.execute(cql, (found.address, index)))
                    )
            except Exception as exc:  # noqa: BLE001
                failure = str(exc)
            self.results.append(
                Result(
                    f"{direction} neighbors (all buckets)",
                    "/{network}/addresses/{address}/neighbors",
                    _render(cql, (found.address, f"0..{rel_buckets - 1}")),
                    total,
                    (time.monotonic() - started) * 1000,
                    rel_buckets,
                    REQUIRED,
                    failure,
                    f"scatters over relation_buckets={rel_buckets} partitions "
                    "unconditionally -- there is no watermark table to stop early",
                )
            )

        if found.link_src is not None and found.link_dst is not None:
            dst_bucket = bucket(found.link_dst, rel_buckets)
            self.probe(
                "neighbor lookup (is X a neighbor of Y)",
                "/{network}/addresses/{address}/neighbors?ids",
                f"SELECT * FROM {self.derived}.address_outgoing_relations "
                f"WHERE src_address = %s AND rel_bucket = %s AND dst_address = %s",
                (found.link_src, dst_bucket, found.link_dst),
                note="a POINT read: the bucket is computed from the counterparty",
            )
            self.probe(
                "link transactions (edge tx list)",
                "/{network}/addresses/{address}/links",
                f"SELECT tx_id, value FROM {self.derived}.address_link_transactions "
                f"WHERE src_address = %s AND dst_bucket = %s AND dst_address = %s",
                (found.link_src, dst_bucket, found.link_dst),
                note="the /links fix: one partition per (source, bucket)",
            )

    def _transaction(self, found: Fixtures) -> None:
        if found.tx_hash is None or found.tx_id is None:
            return
        tx_bucket_size = self.config["tx_block_bucket_size"]
        block_of = found.tx_id >> 32
        group = block_of // tx_bucket_size

        self.probe(
            "tx_id from hash",
            "/{network}/txs/{tx_hash}",
            f"SELECT tx_id FROM {self.raw}.transaction_by_tx_prefix "
            f"WHERE tx_prefix = %s AND tx_hash = %s",
            (found.tx_prefix, found.tx_hash),
            note="hash -> id, the only lookup that needs the prefix index",
        )
        self.probe(
            "transaction by id",
            "/{network}/txs/{tx_hash}",
            f"SELECT * FROM {self.raw}.transaction "
            f"WHERE block_id_group = %s AND tx_id = %s",
            (group, found.tx_id),
            note=(
                "block_id_group = (tx_id >> 32) // tx_block_bucket_size -- "
                "arithmetic, so no second index and no read to find the partition"
            ),
        )
        self.probe(
            "transaction inputs and outputs",
            "/{network}/txs/{tx_hash}?include_io",
            f"SELECT is_output, io_index, address, value "
            f"FROM {self.raw}.transaction_io "
            f"WHERE block_id_group = %s AND tx_id = %s",
            (group, found.tx_id),
            note="same partition key as `transaction`, so it is one extra read",
        )
        self.probe(
            "outputs only",
            "/{network}/txs/{tx_hash}?include_io",
            f"SELECT io_index, address, value FROM {self.raw}.transaction_io "
            f"WHERE block_id_group = %s AND tx_id = %s AND is_output = true",
            (group, found.tx_id),
            kind=OPTIONAL,
            expect_rows=False,
            note="is_output is the second clustering column, so this is a slice",
        )
        self.probe(
            "where an output was spent",
            "/{network}/txs/{tx_hash}/spending",
            f"SELECT * FROM {self.raw}.transaction_spent_in "
            f"WHERE spent_tx_prefix = %s AND spent_tx_hash = %s",
            (found.tx_prefix, found.tx_hash),
            kind=OPTIONAL,
            expect_rows=False,
            note="keyed by the SPENT hash, so it answers 'what spent this'",
        )
        self.probe(
            "what an input spent",
            "/{network}/txs/{tx_hash}/spending",
            f"SELECT * FROM {self.raw}.transaction_spending "
            f"WHERE spending_tx_prefix = %s AND spending_tx_hash = %s",
            (found.tx_prefix, found.tx_hash),
            kind=OPTIONAL,
            expect_rows=False,
        )

    def _block(self, found: Fixtures) -> None:
        if found.block_id is None:
            return
        block_group = found.block_id // self.config["block_bucket_size"]
        tx_group = found.block_id // self.config["tx_block_bucket_size"]

        self.probe(
            "block by height",
            "/{network}/blocks/{height}",
            f"SELECT * FROM {self.raw}.block "
            f"WHERE block_id_group = %s AND block_id = %s",
            (block_group, found.block_id),
        )
        self.probe(
            "transactions in a block",
            "/{network}/blocks/{height}/txs",
            f"SELECT tx_id, tx_hash FROM {self.raw}.transaction "
            f"WHERE block_id_group = %s AND tx_id >= %s AND tx_id < %s",
            (tx_group, found.block_id << 32, (found.block_id + 1) << 32),
            expect_rows=False,
            note=(
                "the reason block_transactions is gone: a block's transactions "
                "are a tx_id RANGE, and the range is arithmetic from the height"
            ),
        )
        if found.day is not None:
            self.probe(
                "blocks on a day",
                "/{network}/blocks/by_date",
                f"SELECT block_id, timestamp FROM {self.raw}.block_by_date "
                f"WHERE day = %s LIMIT 5",
                (found.day,),
                note="day is yyyymmdd as an int, per design rule 5",
            )

    def _rates(self, found: Fixtures) -> None:
        if found.block_id is None:
            return
        group = found.block_id // self.config["block_bucket_size"]
        self.probe(
            "exchange rate for a block",
            "every priced response",
            f"SELECT * FROM {self.derived}.exchange_rates "
            f"WHERE asset = %s AND block_id_group = %s AND block_id = %s",
            (found.currency or "LTC", group, found.block_id),
            kind=OPTIONAL,
            expect_rows=False,
            note="merged table: the native coin and every token share it",
        )


def _render(cql: str, params: tuple) -> str:
    """The CQL with its parameters inlined, so a failure is paste-able."""
    out = cql
    for value in params:
        if isinstance(value, (bytes, bytearray)):
            literal = "0x" + bytes(value).hex()
        elif isinstance(value, bool):
            literal = "true" if value else "false"
        elif isinstance(value, str):
            literal = f"'{value}'"
        else:
            literal = str(value)
        out = out.replace("%s", literal, 1)
    return " ".join(out.split())


def _configuration(session, keyspace: str, fallback: Optional[str] = None) -> dict:
    """The keyspace's own bucketing constants.

    Read rather than assumed: a keyspace written with different constants must
    be probed with its own, or every bucket computed here addresses the wrong
    partition and every probe reports an empty result that looks like missing
    data.

    ``fallback`` is the raw keyspace. Derived keyspaces written before the job
    started emitting their own row have an empty ``configuration`` table, and
    the two are written from the same :class:`NetworkConfig` in one run, so the
    raw row is the same constants under a different name.
    """
    for candidate in (keyspace, fallback):
        if candidate is None:
            continue
        rows = list(
            session.execute(
                f"SELECT * FROM {candidate}.configuration WHERE keyspace_name = %s",
                (candidate,),
            )
        )
        if rows:
            if candidate != keyspace:
                logger.warning(
                    "%s.configuration is empty; using %s's constants. That "
                    "keyspace predates the derived configuration row -- a DAL "
                    "reading it would have nowhere to get entity_buckets.",
                    keyspace,
                    candidate,
                )
            return rows[0]._asdict()
    raise SystemExit(
        f"neither {keyspace} nor {fallback} has a configuration row; the "
        "backfill did not finish, or the keyspace names do not match what was "
        "written"
    )


def run(
    nodes: list,
    raw: str,
    derived: str,
    *,
    username: Optional[str] = None,
    password: Optional[str] = None,
) -> tuple[list[Result], dict]:
    """Probe ``raw`` and ``derived`` on the cluster at ``nodes``.

    Takes the contact points rather than a :class:`RunSettings` because the
    probe needs nothing else: no lake, no Spark profile, no rates keyspace. That
    is what lets it run from a laptop against a cluster, with no
    ``graphsense.yaml`` at all.

    Returns the results and the keyspace's own constants, which the report
    prints -- from one connection, because the constants are what the probes
    were parameterised with and reading them twice could read two different
    things.
    """
    from graphsense_v3.cassandra import connect_to
    from graphsense_v3.settings import assert_v3_keyspace

    # Read-only, but still: this only ever addresses a v3 keyspace.
    for keyspace in (raw, derived):
        assert_v3_keyspace(keyspace)

    cluster = connect_to(nodes, username, password)
    session = cluster.connect()
    try:
        config = _configuration(session, derived, fallback=raw)
        prober = Prober(session, raw, derived, config)
        found = prober.fixtures()
        for gap in found.missing:
            logger.warning("fixture: %s", gap)
        return prober.run(found), config
    finally:
        cluster.shutdown()


def report(results: list[Result], config: dict[str, Any]) -> str:
    """A readable summary. The CQL is included so a failure is actionable."""
    lines = ["", "=" * 78, "v3 DAL access-pattern probe", "=" * 78, ""]
    lines.append(
        "  constants: "
        + ", ".join(
            f"{key}={config[key]}"
            for key in (
                "entity_buckets",
                "tx_page_size",
                "relation_buckets",
                "block_bucket_size",
                "tx_block_bucket_size",
                "address_prefix_length",
                "tx_prefix_length",
            )
            if key in config
        )
    )
    lines.append("")
    lines.append(f"  {'verdict':<8}{'reads':>6}{'rows':>8}{'ms':>9}  access pattern")
    lines.append("  " + "-" * 74)
    for result in results:
        lines.append(
            f"  {result.verdict:<8}{result.reads:>6}{result.rows:>8}"
            f"{result.millis:>9.1f}  {result.name}"
        )
    failures = [r for r in results if not r.ok]
    if failures:
        lines += ["", "-" * 78, "NOT SATISFIED", "-" * 78]
        for result in failures:
            lines += [
                "",
                f"  {result.verdict}: {result.name}   [{result.endpoint}]",
                f"    {result.error}",
                f"    {result.cql}",
            ]
            if result.note:
                lines.append(f"    why it matters: {result.note}")
    lines += ["", "-" * 78, "WHAT EACH PATTERN COSTS", "-" * 78, ""]
    for result in results:
        if result.note:
            lines += [f"  {result.name}  ({result.reads} partition read(s))"]
            lines += [f"      {result.note}", f"      {result.cql}", ""]
    required_failed = sum(1 for r in results if r.kind is REQUIRED and not r.ok)
    lines += [
        "=" * 78,
        f"{len(results)} patterns, {required_failed} required failure(s)",
        "=" * 78,
        "",
    ]
    return "\n".join(lines)
