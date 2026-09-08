"""Column expressions shared by the raw loaders.

Native Spark expressions wherever one exists; a Python UDF only where the
alternative is reimplementing something that must not diverge (see
:mod:`graphsense_v3.spark.udf`).
"""

# NOTE: no `from __future__ import annotations` -- this module defines pandas
# UDFs, whose annotations pyspark inspects at decoration time.

import datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame

_EPOCH = datetime.date(1970, 1, 1)

#: Cassandra `varint` arrives over the connector as a Spark decimal. 38 digits is
#: Spark's maximum decimal precision, so a value wider than that cannot be
#: represented and must fail loudly rather than wrap.
VARINT_PRECISION = 38


def _script_types() -> dict:
    """The ingest-time script-type classification.

    Imported inside the function, not at module level: this module defines
    pandas UDFs, so the EXECUTORS import it -- and `graphsenselib.ingest.utxo`
    pulls in `graphsenselib.config`, hence pydantic and goodconf, which the
    baked spark-env archive does not carry. Only :func:`address_type` needs the
    table, and that builds a Spark expression on the driver.

    Imported rather than copied: a second copy of this table is exactly the
    drift the v3 schema work exists to remove.
    """
    from graphsenselib.ingest.utxo import _address_types

    return _address_types


#: Script types ingest deliberately stores with no address (`address_as_string`).
ADDRESSLESS_TYPES = (
    "null",
    "nulldata",
    "nonstandard",
    "witness_unknown",
    "shielded",
)


def id_group(column: "Column", bucket_size: int) -> "Column":
    """``id // bucket_size``, the block/tx partition key."""
    from pyspark.sql import functions as F

    if bucket_size <= 0:
        raise ValueError(f"bucket_size must be positive, got {bucket_size}")
    return (F.floor(column / F.lit(bucket_size))).cast("int")


def day_from_timestamp(column: "Column") -> "Column":
    """Unix seconds -> a UTC ``date``.

    Pure arithmetic on the epoch, so it does not depend on
    ``spark.sql.session.timeZone`` -- which a backfill run on a differently
    configured cluster would otherwise silently shift by a day.
    """
    from pyspark.sql import functions as F

    return F.date_add(F.lit(_EPOCH), F.floor(column / F.lit(86400)).cast("int"))


def day_key_from_timestamp(column: "Column") -> "Column":
    """Unix seconds -> the ``block_by_date`` partition key, ``yyyymmdd``.

    An integer rather than a CQL ``date``, per design rule 5. It sorts
    correctly, reads in cqlsh, and the only access pattern is equality on the
    day followed by a range on the timestamp within it. Formatting a DateType
    is timezone-free, and the DateType itself came from epoch arithmetic.
    """
    from pyspark.sql import functions as F

    return F.date_format(day_from_timestamp(column), "yyyyMMdd").cast("int")


def address_type(column: "Column") -> "Column":
    """Script-type string -> the stored smallint classification.

    Unknown types map to NULL rather than to a sentinel; ingest raises on them,
    and :func:`unknown_address_types` turns that into a pre-run check instead of
    a silent zero.
    """
    from pyspark.sql import functions as F

    pairs: list[Column] = []
    for name, value in sorted(_script_types().items()):
        pairs.extend([F.lit(name), F.lit(value)])
    return F.create_map(*pairs)[column].cast("smallint")


def hex_to_bytes(column: "Column") -> "Column":
    """Hex string -> blob. NULL-safe; ``unhex`` on NULL is NULL."""
    from pyspark.sql import functions as F

    return F.unhex(column)


def as_varint(column: "Column") -> "Column":
    """A Spark integer column in the shape a Cassandra ``varint`` takes.

    Counts are aggregated as `bigint` and stored as `varint` (design rule 8),
    and the gap has to be closed on the FRAME rather than left to the writer:
    both write paths dispatch on the frame's Spark type, not on the CQL type,
    so an uncast long takes the wrong branch in `sidecar.cast_varints_to_string`
    and reaches the converter as a Long where a BigInteger is expected.
    """
    from pyspark.sql.types import DecimalType

    return column.cast(DecimalType(VARINT_PRECISION, 0))


def bytes_to_varint_udf(column: str = "?", *, null_when_too_wide: bool = False):
    """Big-endian bytes -> the decimal a Cassandra ``varint`` column takes.

    The lake stores wide integers (``value``, ``difficulty``, ``gas_price``) as
    big-endian bytes; ``from_bytes_df`` does the same conversion on the ingest
    side. Raises on a value too wide for Spark's decimal, because a backfill that
    silently wrapped a balance would be discovered by a customer, not by us.

    ``column`` names the column in that error. Which one it is decides whether
    the value is a data bug or a fact about the chain: a native-currency amount
    over 38 digits cannot happen (ETH's whole supply is ~1.2e26 wei), while an
    ERC-20 transfer value is a full uint256 and a scam token minting 2^255 units
    is ordinary. Cassandra's ``varint`` is arbitrary precision and holds either;
    the 38-digit ceiling is Arrow's decimal128, imposed by this UDF's return
    type, and nothing downstream of it.

    ``null_when_too_wide`` is therefore set ONLY for the token transfer value,
    where too wide is a fact rather than a fault: the value becomes NULL, which
    says "not representable" instead of a wrapped number, and
    :func:`graphsense_v3.spark.derived_account.unrepresentable_values` counts
    them so a run reports what it could not store. Everywhere else the default
    stands and the run stops, because a native amount that wide means the bytes
    were misread.
    """
    import decimal

    import pandas as pd
    from pyspark.sql.functions import pandas_udf
    from pyspark.sql.types import DecimalType

    limit = 10**VARINT_PRECISION

    def _one(raw):
        if raw is None:
            return None
        value = int.from_bytes(raw, byteorder="big")
        if value >= limit:
            if null_when_too_wide:
                return None
            raise ValueError(
                f"{column}: integer of {len(raw)} bytes exceeds "
                f"{VARINT_PRECISION} decimal digits, which is the widest Arrow "
                f"decimal a pandas UDF can return (Cassandra's varint itself "
                f"has no such limit): {value}"
            )
        return decimal.Decimal(value)

    @pandas_udf(DecimalType(VARINT_PRECISION, 0))  # ty: ignore[no-matching-overload]
    def _convert(values: pd.Series) -> pd.Series:
        return values.map(_one)

    return _convert


def unknown_address_types(df: "DataFrame", column: str = "type") -> list[str]:
    """Script types the classification table does not know. Empty means safe.

    A pre-run check: ingest raises ``UnknownAddressType`` on these, so a lake
    written by an older ingest cannot contain one -- but a lake written by a
    *newer* one can, and the loader would store NULL for it.
    """
    from pyspark.sql import functions as F

    known = list(_script_types())
    rows = (
        df.select(F.col(column).alias("t"))
        # `NULL IN (...)` is NULL, so nulls are dropped by the filter anyway.
        .where(~F.col("t").isin(known))
        .distinct()
        .collect()
    )
    return sorted(row["t"] for row in rows)


def hex_prefix(column: "Column", length: int) -> "Column":
    """First ``length`` characters of a hash's lowercase hex form.

    The prefix partition key for the transaction tables. Ingest slices the hex
    *string* before it becomes a blob (``ingest/utxo.py:703``, ``get_tx_refs``),
    so the same characters have to come back out of the stored bytes here.
    """
    from pyspark.sql import functions as F

    return F.substring(F.lower(F.hex(column)), 1, length)
