"""The Spark expressions and their Python twins must agree exactly.

Two producers disagreeing about a key is the failure mode this migration is most
exposed to, and it fails silently: a wrong bucket writes a row nobody can read.
These run against a real local SparkSession for that reason.
"""

import zlib

import pytest

from graphsense_v3.codec import (
    block_of_tx_id,
    bucket,
    decode_address,
    encode_address,
    index_of_tx_id,
    search_prefix,
    tx_id,
    tx_id_range,
)

pyspark = pytest.importorskip("pyspark")

ADDRESSES = {
    "btc": [
        "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa",
        "3J98t1WpEZ73CNmQviecrnyiWrnqRhWNLy",
        "bc1qar0srrr7xfkvy5l643lydnw9re59gtzzwf5mdq",
        "bc1p5cyxnuxmeuwuvkwfem96lqzszd02n6xdcjrs20cac6yqjjwudpxqkedrcr",
    ],
    "ltc": [
        "LNE5crMWZ1CzBHiF9wUmVQCjjXTuFsHzGP",
        "ltc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3t4",
    ],
}


@pytest.mark.parametrize("network", sorted(ADDRESSES))
def test_encoding_round_trips(network: str) -> None:
    for address in ADDRESSES[network]:
        assert decode_address(network, encode_address(network, address)) == address


def test_spark_crc32_matches_zlib(spark) -> None:
    """The bucket function rests on this identity: F.crc32 is java.util.zip.CRC32
    and zlib.crc32 is the same IEEE CRC-32. If it ever stops holding, every
    bucket silently moves."""
    from pyspark.sql import functions as F

    encoded = [(encode_address(net, a),) for net in ADDRESSES for a in ADDRESSES[net]]
    df = spark.createDataFrame(encoded, "address binary")
    rows = df.withColumn("crc", F.crc32(F.col("address"))).collect()
    for row in rows:
        assert row["crc"] == zlib.crc32(bytes(row["address"]))


@pytest.mark.parametrize("buckets", [1, 16, 100, 300_000])
def test_bucket_expr_matches_python(spark, buckets: int) -> None:
    from graphsense_v3.spark.udf import bucket_expr

    encoded = [(encode_address(net, a),) for net in ADDRESSES for a in ADDRESSES[net]]
    df = spark.createDataFrame(encoded, "address binary")
    rows = df.withColumn("b", bucket_expr(df["address"], buckets)).collect()
    for row in rows:
        assert row["b"] == bucket(bytes(row["address"]), buckets)


def test_encode_udf_matches_python(spark) -> None:
    from graphsense_v3.spark.udf import encode_address_udf

    df = spark.createDataFrame([(a,) for a in ADDRESSES["btc"]], "address string")
    rows = df.withColumn("b", encode_address_udf("btc")(df["address"])).collect()
    for row in rows:
        assert bytes(row["b"]) == encode_address("btc", row["address"])


def test_prefix_udf_matches_python(spark) -> None:
    from graphsense_v3.spark.udf import search_prefix_udf

    df = spark.createDataFrame([(a,) for a in ADDRESSES["btc"]], "address string")
    rows = df.withColumn("p", search_prefix_udf("btc")(df["address"])).collect()
    for row in rows:
        assert row["p"] == search_prefix("btc", row["address"])


def test_bech32_prefix_carries_four_varying_characters() -> None:
    """v2 strips only 'bc', leaving '1q...'/'1p...' -- two of four characters
    constant, so the whole segwit space lands in 32^2 = 1024 partitions."""
    v3 = {search_prefix("btc", a) for a in ADDRESSES["btc"] if a.startswith("bc1")}
    v2 = {a[len("bc") :][:4] for a in ADDRESSES["btc"] if a.startswith("bc1")}
    assert all(not p.startswith(("1q", "1p")) for p in v3)
    assert all(p.startswith(("1q", "1p")) for p in v2)


def test_bucket_rejects_zero() -> None:
    with pytest.raises(ValueError, match="positive"):
        bucket(b"x", 0)


# --------------------------------------------------------------------------- #
# transaction ids                                                              #
# --------------------------------------------------------------------------- #


def test_tx_id_round_trips_through_its_parts() -> None:
    for block_id, index in ((0, 0), (1, 0), (1, 4095), (3_400_000, 7), (964_902, 2499)):
        value = tx_id(block_id, index)
        assert block_of_tx_id(value) == block_id
        assert index_of_tx_id(value) == index


def test_tx_id_orders_by_block_then_position() -> None:
    """The one property the running counter it replaces actually provided.
    Everything downstream uses tx_id for ORDER BY, min and max -- never as a
    count -- so preserving the order preserves the meaning."""
    ordered = [(0, 0), (0, 1), (1, 0), (1, 500), (2, 0)]
    ids = [tx_id(b, i) for b, i in ordered]
    assert ids == sorted(ids)


def test_tx_id_fits_a_signed_64_bit_column() -> None:
    """Worst case in production is ZEC's 3.47M blocks."""
    assert tx_id(3_467_088, (1 << 32) - 1) < 2**63 - 1


def test_tx_id_range_covers_a_block_range_exactly() -> None:
    """A height filter becomes a tx_id range with no lookup. v2 read the
    previous block's block_transactions and took max(tx_id) (db/utxo.py:109)."""
    lo, hi = tx_id_range(10, 12)
    assert lo == tx_id(10, 0)
    assert hi == tx_id(13, 0) - 1
    assert lo <= tx_id(12, (1 << 32) - 1) <= hi
    assert tx_id(9, (1 << 32) - 1) < lo
    assert tx_id(13, 0) > hi


def test_tx_id_rejects_an_index_that_would_carry() -> None:
    with pytest.raises(ValueError, match="does not fit"):
        tx_id(1, 1 << 32)
    with pytest.raises(ValueError, match="non-negative"):
        tx_id(-1, 0)
    with pytest.raises(ValueError, match="empty block range"):
        tx_id_range(5, 4)


def test_tx_id_expr_matches_python(spark) -> None:
    """The Spark mirror and the Python function must agree: the backfill writes
    ids the DAL later has to decode."""
    from pyspark.sql import functions as F

    from graphsense_v3.spark.udf import tx_id_expr

    pairs = [(0, 0), (1, 3), (964_902, 2499), (3_467_088, 12)]
    df = spark.createDataFrame(pairs, "block_id int, idx int")
    got = df.select(
        tx_id_expr(F.col("block_id"), F.col("idx")).alias("tx_id")
    ).collect()
    assert [r["tx_id"] for r in got] == [tx_id(b, i) for b, i in pairs]


def test_a_nonstandard_bch_address_survives_a_decode_encode_round_trip() -> None:
    """gslib's BCH converter strips the "nonstandard" prefix in to_str but
    re-adds it in to_bytes only for bc1 addresses, so decode->encode was not a
    round trip. v2 never noticed -- it stores address STRINGS; v3 keys on bytes
    and round-trips everything it reads. Three sampled BCH addresses failed
    every call with "'0' not in alphabet" because of this."""
    stored = encode_address(
        "bch", "nonstandard51460314450f69a042f55ab1f2e7a35e93415f39"
    )
    decoded = decode_address("bch", stored)
    assert decoded == "51460314450f69a042f55ab1f2e7a35e93415f39"
    # The whole point: what came out must go back in, to the same bytes.
    assert encode_address("bch", decoded) == stored


def test_a_genuinely_invalid_address_still_raises() -> None:
    """The repair is self-validating for a reason: prefixing blindly would
    manufacture bytes for any garbage string on every network, which is worse
    than the failure it fixes."""
    from graphsenselib.utils.address import InvalidAddress

    for network, junk in (("btc", "not an address at all"), ("bch", "!!!!")):
        with pytest.raises((InvalidAddress, ValueError)):
            encode_address(network, junk)


def test_standard_addresses_are_unaffected() -> None:
    for network, address in (
        ("bch", "1BvBMSEYstWetqTFn5Au4m4GFg7xJaNVN2"),
        ("btc", "bc1qar0srrr7xfkvy5l643lydnw9re59gtzzwf5mdq"),
        ("ltc", "LLcHNPNWE7s6FfLzkt4fD8kJPbsK1V8pyT"),
    ):
        assert decode_address(network, encode_address(network, address)) == address
