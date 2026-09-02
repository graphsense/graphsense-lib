"""Spark-side mirrors of :mod:`graphsense_v3.codec`.

Each function here must agree with its Python twin on every input, because the
backfill writes keys the DAL later has to look up. ``tests/v3/test_v3_codec.py``
asserts that against a real SparkSession rather than trusting it.
"""

# NOTE: deliberately no `from __future__ import annotations` here. pyspark
# inspects a pandas UDF's annotations at decoration time and rejects string
# forms with UNSUPPORTED_SIGNATURE, so the hints below must be real objects.
# Type-checker-only names are quoted individually instead.

from typing import TYPE_CHECKING

from graphsense_v3.codec import (
    DEFAULT_PREFIX_LENGTH,
    TX_INDEX_BITS,
    decode_address,
    encode_address,
    search_prefix,
)

if TYPE_CHECKING:
    from pyspark.sql import Column


def bucket_expr(address: "Column", buckets: int) -> "Column":
    """``crc32(address) % buckets`` -- native, so no Python round trip per row.

    Mirrors :func:`graphsense_v3.codec.bucket`. ``F.crc32`` is
    ``java.util.zip.CRC32``; :func:`zlib.crc32` is the same IEEE CRC-32.
    """
    from pyspark.sql import functions as F

    if buckets <= 0:
        raise ValueError(f"buckets must be positive, got {buckets}")
    return (F.crc32(address) % F.lit(buckets)).cast("int")


def encode_address_udf(network: str):
    """Vectorised UDF for user-format address string -> stored bytes.

    A Python UDF on a hot column is a real cost -- BTC has ~9e9 transaction_io
    rows -- but the alternative is reimplementing the bit-packing codec as a
    Spark expression, where a divergence would corrupt keys irreversibly and
    silently. Correctness first; if this dominates a backfill, the fix is a
    native implementation *validated against this one*, not instead of it.
    """
    import pandas as pd
    from pyspark.sql.functions import pandas_udf
    from pyspark.sql.types import BinaryType

    net = network.lower()

    # The type-hint form is the current API; `functionType=PandasUDFType.SCALAR`
    # would satisfy ty's stubs but is deprecated at runtime and warns. ty models
    # no one-argument `pandas_udf(returnType)` decorator overload, hence the
    # suppression -- the stub is wrong, not the call.
    @pandas_udf(BinaryType())  # ty: ignore[no-matching-overload]
    def _encode(addresses: pd.Series) -> pd.Series:
        return addresses.map(lambda a: None if a is None else encode_address(net, a))

    return _encode


def search_prefix_udf(network: str, length: int = DEFAULT_PREFIX_LENGTH):
    """Vectorised UDF for the ``address_by_prefix`` partition key."""
    import pandas as pd
    from pyspark.sql.functions import pandas_udf
    from pyspark.sql.types import StringType

    net = network.lower()

    @pandas_udf(StringType())  # ty: ignore[no-matching-overload]
    def _prefix(addresses: pd.Series) -> pd.Series:
        return addresses.map(
            lambda a: None if a is None else search_prefix(net, a, length)
        )

    return _prefix


def encode_address_list_udf(network: str):
    """Vectorised UDF for a list of address strings -> a list of stored bytes.

    ``transaction_io.address`` is a list because a multisig output names several
    addresses. Encoding the whole array in one UDF rather than calling the scalar
    one under ``F.transform`` keeps this to a single Python round trip per row.
    """
    import pandas as pd
    from pyspark.sql.functions import pandas_udf
    from pyspark.sql.types import ArrayType, BinaryType

    net = network.lower()

    def _one(addresses):
        if addresses is None:
            return None
        return [None if a is None else encode_address(net, a) for a in addresses]

    @pandas_udf(ArrayType(BinaryType()))  # ty: ignore[no-matching-overload]
    def _encode(values: pd.Series) -> pd.Series:
        return values.map(_one)

    return _encode


def tx_id_expr(block_id: "Column", index: "Column") -> "Column":
    """``(block_id << 32) + index`` -- native, mirroring :func:`codec.tx_id`.

    Both operands are cast to long first: shifting an int column overflows at
    block 0 in Spark, silently.
    """
    from pyspark.sql import functions as F

    return F.shiftleft(block_id.cast("long"), TX_INDEX_BITS) + index.cast("long")


def block_of_tx_id_expr(tx_id: "Column") -> "Column":
    """``tx_id >> 32`` -- the block, mirroring :func:`codec.block_of_tx_id`.

    Worth having natively: it means a table keyed by ``tx_id`` never has to join
    anything to find out which block, and therefore which exchange rate, a row
    belongs to.
    """
    from pyspark.sql import functions as F

    return F.shiftright(tx_id, TX_INDEX_BITS).cast("int")


def search_prefix_bytes_udf(network: str, length: int = DEFAULT_PREFIX_LENGTH):
    """Stored address bytes -> the ``address_by_prefix`` partition key.

    The transformed side only ever holds encoded addresses, so the prefix has to
    come back out of the bytes. Applied to DISTINCT addresses, not to every row.
    """
    import pandas as pd
    from pyspark.sql.functions import pandas_udf
    from pyspark.sql.types import StringType

    net = network.lower()

    def _one(raw):
        if raw is None:
            return None
        return search_prefix(net, decode_address(net, bytes(raw)), length)

    @pandas_udf(StringType())  # ty: ignore[no-matching-overload]
    def _prefix(values: pd.Series) -> pd.Series:
        return values.map(_one)

    return _prefix
