"""Model-driven Cassandra writes.

Every write is checked against the :class:`~graphsense_v3.schema.model.Table` it
targets *before* Spark is asked to do anything. A missing key column or a stray
column is then a failure at job start, not six hours into a run -- which is the
concrete reason the schema is a model and not just ``.sql`` text.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from graphsense_v3.schema.model import Table

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

#: Cassandra ignores a column absent from an INSERT, so a DataFrame may omit
#: columns -- but never a key column, and never carry one the table lacks.
_WRITE_MODE = "append"


def conformance_errors(columns: list[str], table: Table) -> list[str]:
    """Return why ``columns`` cannot be written to ``table``; empty means it can."""
    out: list[str] = []
    present = set(columns)
    declared = table.column_names()

    unknown = sorted(present - declared)
    if unknown:
        out.append(f"{table.name}: columns not in the table: {', '.join(unknown)}")

    key_columns = (*table.key.partition, *table.key.clustering)
    missing = [c for c in key_columns if c not in present]
    if missing:
        out.append(f"{table.name}: missing key columns: {', '.join(missing)}")

    duplicates = sorted({c for c in columns if columns.count(c) > 1})
    if duplicates:
        out.append(f"{table.name}: duplicated columns: {', '.join(duplicates)}")

    return out


def check(df: DataFrame, table: Table) -> None:
    """Raise if ``df`` cannot be written to ``table``."""
    errors = conformance_errors(list(df.columns), table)
    if errors:
        raise ValueError("; ".join(errors))


def write(
    df: DataFrame, table: Table, keyspace: str, sidecar: dict | None = None
) -> None:
    """Write ``df`` to ``keyspace.table``, after checking it conforms.

    The keyspace name is checked against the v3 pattern first. Nothing in this
    package can write outside it, which is the point: a v3 backfill must not be
    able to touch a live keyspace by typo, by a stale argument, or by a caller
    that skipped the driver.

    ``sidecar`` routes the write through the Cassandra Sidecar bulk path
    instead of the connector's CQL path; its keys are
    :func:`graphsense_v3.spark.sidecar.write`'s keyword arguments.
    """
    from graphsense_v3.settings import assert_v3_keyspace

    assert_v3_keyspace(keyspace)
    check(df, table)
    if sidecar is not None:
        from graphsense_v3.spark import sidecar as bulk

        bulk.write(df, keyspace, table.name, **sidecar)
        return
    (
        df.write.format("org.apache.spark.sql.cassandra")
        .options(table=table.name, keyspace=keyspace)
        .mode(_WRITE_MODE)
        .save()
    )
