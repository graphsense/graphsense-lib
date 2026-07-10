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
"""

import csv
import logging
import os
from typing import Iterable, List, Sequence, Set
from urllib.parse import urlparse

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

# The candidate pool is collected to the driver. Guard against a
# --candidate-multiplier that would pull an unreasonable number of rows.
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
    """Is `path` on a filesystem the Spark executors can write to?"""
    return urlparse(path).scheme in REMOTE_SCHEMES


def local_path(path: str) -> str:
    """Strip a `file://` scheme, leaving a plain filesystem path."""
    parts = urlparse(path)
    return parts.path if parts.scheme == "file" else path


def _csv_cell(value):
    """Render one value the way Spark's CSV writer would."""
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    return value


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

    def _address_to_user_format(self, column):
        """Render the raw `address` column into the tagstore's identifier form."""
        from pyspark.sql import functions as F
        from pyspark.sql import types as T

        if self.is_utxo:
            return column

        currency = self.currency  # capture a plain str, not `self`, for pickling

        @F.udf(returnType=T.StringType())
        def render(address):
            from graphsenselib.utils.address import address_to_user_format

            if address is None:
                return None
            return address_to_user_format(currency, bytes(address))

        return render(column)

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
    ) -> None:
        from pyspark.sql import functions as F

        if out_format not in OUTPUT_FORMATS:
            raise ValueError(f"out_format must be one of {OUTPUT_FORMATS}")
        if sort_by not in SORT_COLUMNS:
            raise ValueError(f"sort_by must be one of {sorted(SORT_COLUMNS)}")
        sort_column = SORT_COLUMNS[sort_by]

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
        # narrowing to a candidate pool keeps the address-rendering UDF off all
        # but a few tens of thousands of them, and keeps the pool small enough
        # to probe the tagstore by index instead of scanning it.
        candidates = (
            self._metrics(min_txs, fiat_index)
            .orderBy(F.desc_nulls_last(sort_column))
            .limit(candidate_limit)
            .withColumn("address", self._address_to_user_format(F.col("raw_address")))
            .drop("raw_address")
            .cache()
        )

        tagged = self.probe_tagged_identifiers(
            self._distinct_column(candidates, "address")
        )
        untagged = self._exclude(candidates, "address", tagged)

        if self.is_utxo:
            tagged_clusters = self.probe_tagged_clusters(
                self._distinct_column(candidates, "cluster_id")
            )
            untagged = self._flag_tagged_clusters(untagged, tagged_clusters)
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
        )

        self._write(result, out_path, out_format)
        untagged.unpersist()
        candidates.unpersist()
        logger.info(
            "Wrote %d rows to %s (%s).", min(found, limit), out_path, out_format
        )

    def _write(self, df, out_path: str, out_format: str) -> None:
        """Write the (small) result: one file on the driver, or via Spark on s3 etc.

        The result is at most `limit` rows, so a local path is written straight
        from the driver as a single file. Handing it to Spark's writer instead
        would stage it through `_temporary` on whichever *executor* ran the
        write task — which for a scheme-less path means that node's own local
        disk, not the driver's.
        """
        if is_remote_path(out_path):
            writer = df.coalesce(1).write.mode("overwrite").format(out_format)
            if out_format == "csv":
                writer = writer.option("header", "true")
            writer.save(out_path)
            return

        path = local_path(out_path)
        parent = os.path.dirname(os.path.abspath(path))
        if parent:
            os.makedirs(parent, exist_ok=True)

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
