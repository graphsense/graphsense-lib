"""Spark job: rank the most active addresses that carry no tag yet.

Reads the transformed keyspace ``address`` table from Cassandra, checks the
candidates against the tagstore (Postgres) and emits the top-N addresses that
have no tag of their own, ranked by transaction count, received value (native
or fiat) or degree.

The tagstore is *probed*, not scanned. `tag` holds ~8e7 rows / ~6e7 distinct
identifiers, so pulling the identifier set into Spark to anti-join it would ship
gigabytes and materialise ~6e7 JVM strings, to compare against a candidate pool
three orders of magnitude smaller. Instead the (already small) candidate pool is
collected to the driver and probed with chunked `identifier = ANY(...)` queries,
which ride the `ix_tag_identifier` btree. Same for the cluster ids against
`best_cluster_tag(network, cluster_id)`.

Tag matching is deliberately network-agnostic (``tag.identifier`` only, no
``tag.network`` filter) — an address string is unique to its chain in practice,
and a tag on any network means the address is not "unknown". The cluster-level
``cluster_tagged`` column, in contrast, IS network-scoped, because
``gs_cluster_id`` is only meaningful within one network.

What crosses to the driver
--------------------------
The ``address`` table has up to ~1e9 rows and is never collected. Three points
*do* pull rows into the driver, and every one of them is bounded on purpose —
if you add a fourth, bound it too:

1. ``_distinct_column(ranked, "raw_address")`` — the candidate addresses, which
   are rendered to identifier form on the driver (never in a UDF — see
   ``_user_addresses``) and used both to probe Postgres and to build the
   lookup table joined back on.
2. ``_distinct_column(ranked, "cluster_id")`` — ditto for the cluster flag.
   Both are capped at ``limit * candidate_multiplier``, which ``run()`` checks
   against ``MAX_CANDIDATES`` up front so an over-wide pool fails fast with a
   clear message instead of walking the driver into ``spark.driver.maxResultSize``
   (1 GB by default; the driver aborts the job when collected results exceed it).
3. ``_write`` — the final ranked slice, capped at ``limit``.

Consequently the job runs **no Python on the executors**: no UDF, so no Python
worker. That is a correctness requirement, not a preference — a UDF appended to
the ranked pool made Catalyst re-plan the limit and silently swap in a different
50 000 rows. See ``_user_addresses``.

``.orderBy(...).limit(n)`` is what keeps (1) and (2) cheap: Catalyst compiles it
to ``TakeOrderedAndProjectExec``, so each partition keeps only a bounded
priority queue of its local top-n and the driver merges those. No global sort,
and the driver never sees more than ``numPartitions * n`` rows even mid-rank.

Note that ``collect()`` is not streaming — the driver materialises every row in
the JVM heap, then again as pickled batches in the Python process (PySpark ships
them over a local socket rather than through py4j). A ``--limit`` large enough
to matter would need ``toLocalIterator()`` or a remote ``--out-path``, not a
bigger driver.
"""

import csv
import logging
import os
from dataclasses import dataclass
from decimal import Decimal
from typing import Iterable, List, Optional, Sequence, Set
from urllib.parse import urlparse

from graphsenselib.utils.address import address_to_user_format

logger = logging.getLogger(__name__)

# Schemes every executor can reach. Anything else (including a bare path, which
# Hadoop reads as `file:` on whichever node runs the task) is driver-local.
REMOTE_SCHEMES = frozenset(
    {"s3", "s3a", "s3n", "hdfs", "gs", "abfs", "abfss", "wasb", "wasbs"}
)

# CLI sort key -> output column carrying the metric.
SORT_COLUMNS = {
    "txs": "no_txs",
    "value": "total_received_value",
    "fiat": "total_received_fiat",
    "degree": "degree",
}
OUTPUT_FORMATS = ("csv", "parquet")

# Addresses in the transformed keyspace are text for UTXO chains and a blob for
# account chains; only the latter needs rendering back to user format.
UTXO_SCHEMA_TYPES = ("utxo",)

# Rows per `= ANY(...)` probe. Large enough to amortise the round trip, small
# enough that the planner keeps choosing an index scan over a full scan.
PROBE_CHUNK_SIZE = 10_000

# The candidate pool is collected to the driver (twice: addresses and cluster
# ids). Checked against `limit * candidate_multiplier` before the scan starts,
# so an over-wide pool fails with a clear message rather than as an OOM or a
# `spark.driver.maxResultSize` abort halfway through a long run.
MAX_CANDIDATES = 2_000_000


def psycopg2_dsn(db_url: str) -> str:
    """Render a SQLAlchemy-style Postgres URL as a libpq DSN."""
    from sqlalchemy.engine import make_url

    url = make_url(db_url)
    if not url.database:
        raise ValueError(f"Tagstore URL has no database component: {db_url}")
    # psycopg2 rejects the SQLAlchemy driver suffix (postgresql+asyncpg://).
    return url.set(drivername="postgresql").render_as_string(hide_password=False)


def is_remote_path(path: str) -> bool:
    """Is `path` on a filesystem every Spark executor can reach?

    Decides who performs the write; see `TopUntaggedAddresses._write`. A bare
    path and an explicit `file://` are both driver-local: they name a different
    directory on every node, so only the driver may write them.
    """
    return urlparse(path).scheme in REMOTE_SCHEMES


def local_path(path: str) -> str:
    """Strip a `file://` scheme, leaving a plain filesystem path."""
    parts = urlparse(path)
    return parts.path if parts.scheme == "file" else path


def check_writable(out_path: str) -> None:
    """Fail before the scan if the driver cannot write `out_path` afterwards.

    The write is the last step of a job that reads ~1e9 rows, so an unwritable
    output directory must not be discovered at the end. Probes by actually
    creating a file — `os.access` lies under uid 0 and on some mounts.

    Remote paths are not checked: their credentials live in the Spark/Hadoop
    config and the executors, not this process.
    """
    if is_remote_path(out_path):
        return

    path = local_path(out_path)
    parent = os.path.dirname(os.path.abspath(path)) or "."
    try:
        os.makedirs(parent, exist_ok=True)
        probe = os.path.join(parent, f".{os.path.basename(path)}.writetest")
        with open(probe, "w"):
            pass
        os.unlink(probe)
    except OSError as error:
        raise ValueError(
            f"Cannot write to {parent!r} as uid {os.getuid()}: {error}. "
            f"In Docker the image runs as uid 1000, so a bind-mounted output "
            f"directory must be writable by it (chown 1000:1000 <dir>)."
        ) from error


def _csv_cell(value):
    """Render one value the way Spark's CSV writer would.

    Keeps the driver-written file byte-compatible with a Spark-written one:
    lowercase booleans, empty string for null. Python's default `str()` would
    give `True` and `None`/``.
    """
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    return value


def _percent(part: int, whole: int) -> float:
    return 100.0 * part / whole if whole else 0.0


@dataclass
class TagCoverage:
    """What the run did: pool size, how much of it the tagstore already knows.

    Coverage is measured over the *candidate pool* (the top
    `limit * candidate_multiplier` by the chosen metric), not the whole address
    table — so `tagged_share` reads as "how well tagged are the most active
    addresses", which is the number worth watching. It is not an estimate of
    overall tagging coverage.

    `pool_floor` / `pool_ceiling` are the sort key's range over the pool *as
    ranked*, and `emitted_max` is its maximum over the rows actually written.
    They exist to be compared: see `check_pool_invariant`.
    """

    candidates: int = 0
    tagged: int = 0
    clusters: int = 0  # UTXO only; account keyspaces have no clustering
    tagged_clusters: int = 0
    emitted: int = 0
    sort_by: str = ""
    pool_floor: Optional[Decimal] = None
    pool_ceiling: Optional[Decimal] = None
    emitted_max: Optional[Decimal] = None

    @property
    def untagged(self) -> int:
        return self.candidates - self.tagged

    @property
    def tagged_share(self) -> float:
        return _percent(self.tagged, self.candidates)

    @property
    def tagged_cluster_share(self) -> float:
        return _percent(self.tagged_clusters, self.clusters)

    def check_pool_invariant(self) -> None:
        """Every emitted row came from the pool, so its metric is >= the floor.

        Violating this means the rows written were not drawn from the pool that
        was ranked — the two were measured on different DataFrames, so a
        downstream operator re-planned the `orderBy(...).limit(n)` and silently
        swapped the candidate set. That is not a hypothetical: a Python UDF
        appended to the ranked pool did exactly this on eth/trx, and every other
        signal (row count, tag counts, monotonic output) looked healthy. This is
        the one check that catches it.
        """
        if self.pool_floor is None or self.emitted_max is None:
            return  # nothing emitted, or an all-null metric
        if self.emitted_max < self.pool_floor:
            raise RuntimeError(
                f"Candidate pool was not preserved: the highest emitted "
                f"{self.sort_by} is {self.emitted_max}, below the pool's floor "
                f"of {self.pool_floor}. Every emitted row must come from the "
                f"pool, so the rows written were drawn from a different, "
                f"re-planned candidate set. Refusing to write a wrong ranking."
            )

    def summary_lines(self, is_utxo: bool) -> List[str]:
        """The run's numbers, for a caller that always shows them to the user.

        A job that scans ~1e9 rows and reports nothing cannot be sanity-checked.
        The pool range in particular is what makes a bad ranking obvious at a
        glance: a floor far below what the metric's distribution implies means
        the pool is not the head of the table.
        """
        lines = [
            f"ranked by       : {self.sort_by}",
            f"candidate pool  : {self.candidates} addresses",
            f"  metric range  : {self.pool_floor} .. {self.pool_ceiling}",
            f"already tagged  : {self.tagged} ({self.tagged_share:.1f}%)",
            f"untagged        : {self.untagged}",
        ]
        if is_utxo:
            lines.append(
                f"tagged clusters : {self.tagged_clusters} of {self.clusters} "
                f"({self.tagged_cluster_share:.1f}%)"
            )
        lines += [
            f"rows written    : {self.emitted}",
            f"  highest {self.sort_by:<7}: {self.emitted_max}",
        ]
        return lines

    def log(self, is_utxo: bool) -> None:
        for line in self.summary_lines(is_utxo):
            logger.info(line)


def _chunks(values: Sequence, size: int) -> Iterable[Sequence]:
    for start in range(0, len(values), size):
        yield values[start : start + size]


class TopUntaggedAddresses:
    """Compute the most active addresses without a tag for one currency."""

    def __init__(
        self,
        spark,
        currency: str,
        schema_type: str,
        transformed_keyspace: str,
        tagstore_db_url: str,
        tagstore_schema: str = "public",
    ) -> None:
        self.spark = spark
        self.currency = currency
        self.schema_type = schema_type
        self.transformed_keyspace = transformed_keyspace
        self.tagstore_schema = tagstore_schema
        self.dsn = psycopg2_dsn(tagstore_db_url)

    @property
    def is_utxo(self) -> bool:
        return self.schema_type in UTXO_SCHEMA_TYPES

    def _connect(self):
        from psycopg2 import connect

        return connect(self.dsn, options=f"-c search_path={self.tagstore_schema}")

    def _probe(self, query: str, values: Sequence, params=()) -> Set:
        """Run `query` over `values` in chunks, returning the union of column 0.

        `query` must take the chunk as its last `%s` placeholder.
        """
        if not values:
            return set()
        found: Set = set()
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                for chunk in _chunks(values, PROBE_CHUNK_SIZE):
                    cur.execute(query, (*params, list(chunk)))
                    found.update(row[0] for row in cur)
        finally:
            conn.close()
        return found

    def probe_tagged_identifiers(self, addresses: Sequence[str]) -> Set[str]:
        """Which of `addresses` already carry a tag (on any network)?"""
        tagged = self._probe(
            "SELECT DISTINCT identifier FROM tag WHERE identifier = ANY(%s)",
            addresses,
        )
        logger.info("%d of %d candidates are tagged.", len(tagged), len(addresses))
        return tagged

    def probe_tagged_clusters(self, cluster_ids: Sequence[int]) -> Set[int]:
        """Which of `cluster_ids` have a tagged member, on this network?"""
        tagged = self._probe(
            "SELECT DISTINCT cluster_id FROM best_cluster_tag "
            "WHERE network = %s AND cluster_id = ANY(%s)",
            cluster_ids,
            params=(self.currency.upper(),),
        )
        logger.info(
            "%d of %d candidate clusters are tagged.", len(tagged), len(cluster_ids)
        )
        return tagged

    def _read_addresses(self):
        return (
            self.spark.read.format("org.apache.spark.sql.cassandra")
            .options(table="address", keyspace=self.transformed_keyspace)
            .load()
        )

    def _user_addresses(self, raw_values: Sequence) -> List[str]:
        """Render raw addresses to the tagstore's identifier form, on the driver.

        UTXO keyspaces store `address text` and need no work. Account keyspaces
        store `address blob`, which `address_to_user_format` turns into `0x…`
        (eth) or base58 (trx).

        This MUST NOT become a Spark UDF. Appending a Python UDF projection on
        top of `orderBy(...).limit(n)` made Catalyst re-plan the limit: measured
        against eth_transformed, the identical pool went from
        [1.235e22 .. 7.82e26] wei to [6.84e19 .. 1.001e21] — a different 50 000
        rows, capped three orders of magnitude too low, with the exact
        `Decimal(38,0)` sort key degraded to float64 (1001e18 came back as
        1000999999999999967232). The job then ranked and tag-filtered that wrong
        pool perfectly, so the output looked plausible: monotonic, dense, and
        drawn from the middle of the distribution. Only the account chains were
        affected, because `is_utxo` never attached the UDF.

        Rendering on the driver also costs nothing: these values are the
        candidate pool, already bounded by `MAX_CANDIDATES`, and they must come
        to the driver anyway to probe the tagstore. As a bonus the job now runs
        no Python on the executors at all, so it does not care which interpreter
        the cluster nodes expose as `PYSPARK_PYTHON`.
        """
        if self.is_utxo:
            return list(raw_values)
        return [address_to_user_format(self.currency, bytes(raw)) for raw in raw_values]

    def _with_user_addresses(self, df, raw_values: Sequence, addresses: Sequence[str]):
        """Attach the rendered `address` column, replacing `raw_address`.

        A broadcast join against a pool-sized lookup table. Plain Catalyst
        expressions only — see `_user_addresses` for why no UDF may appear here.
        """
        from pyspark.sql import functions as F

        if self.is_utxo:
            return df.withColumnRenamed("raw_address", "address")

        pairs = [(bytes(raw), address) for raw, address in zip(raw_values, addresses)]
        lookup = self.spark.createDataFrame(pairs, "raw_address binary, address string")
        return df.join(F.broadcast(lookup), on="raw_address").drop("raw_address")

    def _metrics(self, min_txs: int, fiat_index: int):
        """Project the address table down to the ranking metrics."""
        from pyspark.sql import functions as F

        addresses = self._read_addresses()

        columns = [
            F.col("address_id"),
            F.col("address").alias("raw_address"),
            (
                F.coalesce(F.col("no_incoming_txs"), F.lit(0))
                + F.coalesce(F.col("no_outgoing_txs"), F.lit(0))
            ).alias("no_txs"),
            (
                F.coalesce(F.col("in_degree"), F.lit(0))
                + F.coalesce(F.col("out_degree"), F.lit(0))
            ).alias("degree"),
            # Native units of the chain's smallest denomination (satoshi, wei,
            # sun); bigint for UTXO, varint -> Decimal(38,0) for account.
            F.col("total_received").getField("value").alias("total_received_value"),
            F.col("total_received")
            .getField("fiat_values")
            .getItem(fiat_index)
            .alias("total_received_fiat"),
        ]
        if self.is_utxo:
            columns.append(F.col("cluster_id"))

        metrics = addresses.select(*columns)
        if min_txs > 0:
            metrics = metrics.filter(F.col("no_txs") >= min_txs)
        return metrics

    def _distinct_column(self, df, column: str) -> List:
        """Collect the distinct non-null values of `column` to the driver.

        Only ever called on the candidate pool, which `run()` has already capped
        at `MAX_CANDIDATES`. Do not point this at an uncapped DataFrame.
        """
        rows = df.select(column).distinct().collect()
        return [row[0] for row in rows if row[0] is not None]

    def run(
        self,
        out_path: str,
        out_format: str = "csv",
        limit: int = 1000,
        sort_by: str = "txs",
        min_txs: int = 0,
        fiat_index: int = 0,
        candidate_multiplier: int = 50,
    ) -> TagCoverage:
        from pyspark.sql import functions as F

        if out_format not in OUTPUT_FORMATS:
            raise ValueError(f"out_format must be one of {OUTPUT_FORMATS}")
        if sort_by not in SORT_COLUMNS:
            raise ValueError(f"sort_by must be one of {sorted(SORT_COLUMNS)}")
        sort_column = SORT_COLUMNS[sort_by]
        check_writable(out_path)

        candidate_limit = limit * candidate_multiplier
        if candidate_limit > MAX_CANDIDATES:
            raise ValueError(
                f"limit * candidate_multiplier = {candidate_limit} exceeds "
                f"{MAX_CANDIDATES}; the candidate pool is collected to the driver."
            )

        logger.info(
            "Scanning %s.address for the top %d untagged addresses by %s "
            "(candidate pool: %d, min_txs=%d)",
            self.transformed_keyspace,
            limit,
            sort_by,
            candidate_limit,
            min_txs,
        )

        # Rank first, filter tags second. The address table has up to ~1e9 rows;
        # narrowing to a candidate pool keeps it small enough to render and to
        # probe the tagstore by index instead of scanning it.
        #
        # The cost is that a pool made entirely of tagged addresses yields fewer
        # than `limit` rows — the warning below tells the user to widen it. The
        # orderBy+limit compiles to TakeOrderedAndProjectExec (per-partition
        # bounded queues, merged on the driver), not a global sort.
        #
        # Nothing may be appended to this chain that forces Catalyst to re-plan
        # the limit — a Python UDF here silently returned a different 50 000
        # rows. See `_user_addresses`.
        ranked = (
            self._metrics(min_txs, fiat_index)
            .orderBy(F.desc_nulls_last(sort_column))
            .limit(candidate_limit)
            .cache()
        )

        # Measure the sort key's range on the pool *as ranked*, before anything
        # downstream can re-plan it. Compared against the emitted maximum below.
        pool_range = ranked.agg(
            F.min(sort_column).alias("floor"), F.max(sort_column).alias("ceiling")
        ).first()

        # One collect serves three purposes: rendering, the tagstore probe, and
        # the lookup table joined back on. Account addresses arrive as bytearray.
        raw_values = self._distinct_column(ranked, "raw_address")
        candidate_addresses = self._user_addresses(raw_values)
        candidates = self._with_user_addresses(ranked, raw_values, candidate_addresses)

        tagged = self.probe_tagged_identifiers(candidate_addresses)
        untagged = self._exclude(candidates, "address", tagged)

        stats = TagCoverage(
            candidates=len(candidate_addresses),
            tagged=len(tagged),
            sort_by=sort_by,
            pool_floor=pool_range["floor"],
            pool_ceiling=pool_range["ceiling"],
        )

        if self.is_utxo:
            candidate_clusters = self._distinct_column(ranked, "cluster_id")
            tagged_clusters = self.probe_tagged_clusters(candidate_clusters)
            untagged = self._flag_tagged_clusters(untagged, tagged_clusters)
            stats.clusters = len(candidate_clusters)
            stats.tagged_clusters = len(tagged_clusters)
        else:
            # Account-model keyspaces have no clustering, so nothing to report.
            untagged = untagged.withColumn(
                "cluster_tagged", F.lit(None).cast("boolean")
            )

        untagged = untagged.cache()
        found = untagged.count()
        if found < limit:
            logger.warning(
                "Found %d untagged addresses, fewer than the requested limit "
                "of %d. Either the table holds no more matching addresses, or "
                "the candidate pool (top %d before tag removal) was consumed by "
                "tagged addresses — raise --candidate-multiplier to widen it.",
                found,
                limit,
                candidate_limit,
            )

        select_columns = ["address", "address_id", "no_txs", "degree"]
        if self.is_utxo:
            select_columns.append("cluster_id")
        select_columns += [
            "total_received_value",
            "total_received_fiat",
            "cluster_tagged",
        ]

        result = (
            untagged.select(*select_columns)
            .orderBy(F.desc_nulls_last(sort_column))
            .limit(limit)
            .cache()
        )

        stats.emitted = min(found, limit)
        stats.emitted_max = result.agg(F.max(sort_column)).first()[0]
        # Before writing: did the rows we are about to emit come from the pool
        # we ranked? Raises if not.
        stats.check_pool_invariant()

        self._write(result, out_path, out_format)
        result.unpersist()
        untagged.unpersist()
        ranked.unpersist()
        logger.info(
            "Wrote %d rows to %s (%s).", min(found, limit), out_path, out_format
        )
        stats.log(self.is_utxo)
        return stats

    def _write(self, df, out_path: str, out_format: str) -> None:
        """Write the result: one file on the driver, or via Spark for remote paths.

        `df.write.save(path)` is a *distributed* action: the write becomes a
        task, the scheduler assigns it to an executor, and that executor creates
        the file. Only sound when every executor can reach `path`.

        A scheme-less path cannot be. Hadoop resolves it against `fs.defaultFS`,
        which on our cluster is the local filesystem, so `/out/x` becomes
        `file:/out/x` — "local to whichever machine evaluates this", i.e. the
        executor, not the driver. The driver's `-v ./out:/out` bind mount does
        not exist there, and the write dies with

            java.io.IOException: Mkdirs failed to create
            file:/out/.../_temporary/0/_temporary/attempt_...
            (exists=false, cwd=file:/var/data/nvme4/spark/work/app-.../7)

        `.coalesce(1)` does not rescue this: one write task is still one task on
        one executor. It only narrows the failure from every node to one.

        So local paths bypass Spark's writer entirely — `collect()` brings the
        (at most `limit`) rows to the driver and we write them with plain file
        IO, inside whatever container the driver runs in. Remote paths keep the
        distributed writer, because there the executors *can* all reach the
        destination and parallel writes are the point.

        Both branches emit the same bytes: `_csv_cell` reproduces the Spark CSV
        writer's rendering of booleans and nulls, so a local file and an s3 part
        file parse identically.

        `toPandas()` would be the obvious way to do the local write, but PySpark
        drives it through a pandas API that warns twice on pandas 3 (`distutils`
        `LooseVersion`, and a removed `copy=` kwarg). Those land in production
        logs and fail the suite under `-W error`. `collect()` skips that bridge.
        """
        if is_remote_path(out_path):
            writer = df.coalesce(1).write.mode("overwrite").format(out_format)
            if out_format == "csv":
                writer = writer.option("header", "true")
            writer.save(out_path)
            return

        # The parent dir exists and is writable: `run()` called check_writable().
        path = local_path(out_path)
        columns = df.columns
        rows = df.collect()
        if out_format == "csv":
            with open(path, "w", newline="") as fh:
                out = csv.writer(fh)
                out.writerow(columns)
                out.writerows([_csv_cell(value) for value in row] for row in rows)
        else:
            import pyarrow as pa
            import pyarrow.parquet as pq

            table = (
                pa.Table.from_pylist([row.asDict() for row in rows])
                if rows
                # from_pylist([]) yields a table with no columns at all; keep
                # the schema so an empty result is still a readable parquet file.
                else pa.table({column: [] for column in columns})
            )
            pq.write_table(table, path)

    def _exclude(self, df, column: str, excluded: Set[str]):
        """Drop rows whose `column` is in `excluded` (a driver-side set)."""
        from pyspark.sql import functions as F

        if not excluded:
            return df
        excluded_df = self.spark.createDataFrame(
            [(value,) for value in excluded], f"{column}_excluded string"
        )
        return df.join(
            F.broadcast(excluded_df),
            df[column] == excluded_df[f"{column}_excluded"],
            "left_anti",
        )

    def _flag_tagged_clusters(self, df, tagged_clusters: Set[int]):
        """Add a `cluster_tagged` boolean from a driver-side set of cluster ids."""
        from pyspark.sql import functions as F

        if not tagged_clusters:
            return df.withColumn("cluster_tagged", F.lit(False))
        tagged_df = self.spark.createDataFrame(
            [(value,) for value in tagged_clusters], "cluster_id int"
        ).withColumn("cluster_tagged", F.lit(True))
        return df.join(F.broadcast(tagged_df), on="cluster_id", how="left").withColumn(
            "cluster_tagged", F.coalesce(F.col("cluster_tagged"), F.lit(False))
        )
